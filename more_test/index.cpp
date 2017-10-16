#include "postgres-connection.h"
#include "postgres-exceptions.h"

#include <iostream>

using namespace db::postgres;

namespace test {
  std::string toJsonb(const std::string& input) noexcept {
    std::vector<char> v(input.length() + 1);
    std::copy(input.c_str(), input.c_str() + input.length(), v.begin() + 1);
    v[0] = char(1);
    std::string copy_s{ v.begin(), v.end() };
    return std::move(copy_s);
  }

  std::string fromJsonb(const std::string& s) noexcept {
    if (s.length() > 0 && int(s.at(0)) == 1) {
      std::vector<char> v(s.length());
      std::copy(s.c_str() + 1, s.c_str() + s.length(), v.begin());
      std::string copy_s{ v.begin(), v.end() };
      return std::move(copy_s);
    }
  }
}

int main() {

  Connection cnx;
  try {
    cnx.connect("application_name=jsonb_test host=127.0.0.1 port=5432 dbname=pccrms connect_timeout=30");

    cnx.execute(R"SQL(
      drop table if exists rogueone;
      create table rogueone (
        id varchar(50),
        name varchar(500),
        ts timestamp without time zone DEFAULT now(),
        current jsonb,
        history jsonb,
        constraint rogueone_pkey primary key (id)
      );
    )SQL");

    std::string json_string = R"(
    {
      "id": "RO0001",
      "name": "Rogue One"
    }
    )";

    auto modified_json_str = toJsonb(json_string);
    std::cout << modified_json_str << std::endl;
    std::cout << "before: " << json_string.size() << " after: " << modified_json_str.size() << std::endl;

    auto revert = test::fromJsonb(modified_json_str);
    std::cout << revert << std::endl;

    // Test std::string.
    int64_t inserted = cnx.execute(R"SQL(
      insert into rogueone (id, name, current) values ($1, $2, $3)
      on conflict (id) do update set name = EXCLUDED.name, ts = now(), current = EXCLUDED.current, history = EXCLUDED.history;
    )SQL", "RO0001", "Rogue One", modified_json_str).count();

    std::cout << inserted << " have been added." << std::endl;

    // Test const char*.
    int64_t inserted1 = cnx.execute(R"SQL(
      insert into rogueone (id, name, current) values ($1, $2, $3)
      on conflict (id) do update set name = EXCLUDED.name, ts = now(), current = EXCLUDED.current, history = EXCLUDED.history;
    )SQL", "RO0001", "Rogue One", modified_json_str.c_str()).count();

    std::cout << inserted1 << " have been added." << std::endl;

  } catch (ConnectionException e) {
    std::cerr << "Oops... Cannot connect...";
  } catch (ExecutionException e) {
    std::cerr << "Oops... " << e.what();
  }

  return -1;
}
