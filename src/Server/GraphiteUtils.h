#pragma once

#include <vector>
#include <any>
#include <string>
#include <fmt/format.h>

namespace DB
{


void replace_all(std::string &s, const std::string &search, const std::string &replace);

bool has_wildcard(std::string &target);

size_t index_last_wildcard(const std::string &target);

size_t index_wildcard(const std::string &target);

std::string like(const std::string& field, const std::string& s);

std::string reverse_string(const std::string &target);

bool str_has_prefix(const std::string &fullString, const std::string &prefix);

bool str_has_suffix(const std::string &fullString, const std::string &prefix);

std::vector<std::string> split(const std::string &target, const std::string& delimiter = ".");

std::string glob_to_regexp(const std::string& g);

std::string join(const std::vector<std::string> &elems, size_t start, size_t end, const std::string &sep);

std::string has_prefix(const std::string &field, const std::string &prefix);

std::string in_table(const std::string &field,const std::string &table);

std::string date_between (const std::string &field, int from, int until);

std::string timestamp_between(const std::string &field, long long from, long long until);

int IntervalStrings(std::string &s, int sign);






template<class T>
requires std::numeric_limits<T>::is_integer
std::string quote(T value) {
    return fmt::format("{}", value);
}







std::string escape(const std::string& g);


std::string like_escape(const std::string& g);



template<class T>
std::string quote(T value) {
  return fmt::format("'{}'", escape(value));
}


template<typename T>
std::string eq(const std::string &field, T value) {
  return fmt::format("{}={}", field, quote(value));
}




std::string clear_glob(std::string &query);

std::string in(std::string &field, std::vector<std::string> &list);

std::string Glob(const std::string &field,const std::string &query);

std::string TreeGlob(const std::string &field, const std::string &query);

int IntervalStrings(std::string & s, int sign = 1);




class Where{
 public:
  Where() = default;

  template <typename... T>
  void andf(fmt::format_string<T...> fmt, T&&... args);


  void And(const std::string& exp);
  void Or(const std::string& exp);
  std::string string();
  std::string sql();
  std::string preWhereSql();
 private:
  std::string where;
};

}
