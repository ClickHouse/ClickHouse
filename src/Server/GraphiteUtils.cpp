
#include <iostream>
#include <fmt/format.h>
#include <string>
#include <utility>
#include <vector>
#include <string_view>
#include "GraphiteUtils.h"
#include <numeric>
#include <type_traits>

namespace DB
{


const std::string op_eq = "=";

std::string join(const std::vector<std::string> &elems, size_t start, size_t end, const std::string &sep){

  if (elems.empty()) {
    return "";
  }
  if (elems.size() == 1){
    return elems[0];
  }
  if (end > elems.size()){
    return "";
  }
  std::string res;
  res.reserve(100);
  for (size_t i = start; i < end - 1; i++) {
    res += elems[i] + sep;
  }
  res += elems[end - 1];
  return res;
}

bool str_has_prefix(const std::string &fullString, const std::string &prefix) {
  if (fullString.length() >= prefix.length()) {
    return (0 == fullString.compare (0, prefix.length(), prefix));
  } else {
    return false;
  }
}

bool str_has_suffix (std::string const &fullString, std::string const &ending) {
  if (fullString.length() >= ending.length()) {
    return (0 == fullString.compare (fullString.length() - ending.length(), ending.length(), ending));
  } else {
    return false;
  }
}


size_t indexAny(std::string_view s, const std::string& chars) {
  size_t el;
  size_t m = std::string::npos;
  for (char i : chars) {
    el = s.find(i);
    if (el != std::string::npos) {
      m = std::min(m, el);
    }
  }
  return m;
}

size_t lastIndexAny(std::string_view s, const std::string& chars){
  bool flag = false;
  size_t el;
  size_t m = 0;
  for (char i : chars) {
    el = s.find(i);
    if (el != std::string::npos) {
      flag = true;
      m = std::max(m, el);
    }
  }
  if (flag){
    return m;
  } else {
    return std::string::npos;
  }
}


void Where::And(const std::string &exp) {
  if (exp.empty()) {
    return;
  }
  if (!where.empty()) {
    where = fmt::format("({}) AND ({})", where, exp);
  } else {
    where = exp;
  }
}

void Where::Or(const std::string &exp) {
  if (exp.empty()) {
    return;
  }
  if (!where.empty()) {
    where = fmt::format("({}) OR ({})", where, exp);
  } else {
    where = exp;
  }
}
std::string Where::string() {
  return where;
}

template <typename... T>
void Where::andf(fmt::format_string<T...> fmt, T&&... args) {
  And(fmt::format(fmt, args...));
}

std::string Where::sql() {
  if (where.empty()) {
    return "";
  }
  return "WHERE " + where;
}

std::string Where::preWhereSql() {
  if (where.empty()) {
    return "";
  }
  return "PREWHERE " + where;
}


///////////////////////

void replace_all(std::string &s, const std::string &search, const std::string &replace) {
  for (size_t pos = 0;; pos += replace.length()) {
    // Locate the substring to replace
    pos = s.find(search, pos);
    if (pos == std::string::npos) break;
    // Replace by erasing and inserting
    s.erase(pos, search.length());
    s.insert(pos, replace);
  }
}
std::string glob_to_regexp(const std::string& g) {
  std::string s = g;
  replace_all(s, ".", "[.]");
  replace_all(s, "$", "[$]");
  replace_all(s, "{", "(");
  replace_all(s, "}", ")");
  replace_all(s, "?", "[^.]");
  replace_all(s, ",", "|");
  replace_all(s, "*", "([^.]*?)");
  return s;
}
std::string escape(const std::string& g) {
  std::string s = g;
  replace_all(s, "\\", "\\\\");
  replace_all(s, "`", "\'");
  return s;
}

std::string like_escape(const std::string& g) {
  std::string s = g;
  replace_all(s, "_", "\\_");
  replace_all(s, "%", "\\%");
  replace_all(s, "\\", "\\\\");
  replace_all(s, "'", "\\'");
  return s;
}

bool has_wildcard(std::string &target) {
  return indexAny(target, "[]{}*?") != std::string::npos;
}
size_t index_last_wildcard(const std::string &target) {
  return lastIndexAny(target, "[]{}*?");
}
size_t index_wildcard(const std::string &target) {
  return indexAny(target, "[]{}*?");
}
std::string like(const std::string &field, const std::string &s) {
  return fmt::format("{} LIKE '{}'", field, s);
}

std::string has_prefix(const std::string &field, const std::string &prefix) {
  return fmt::format("{} LIKE '{}%'", field, like_escape(prefix));
}

std::string has_prefix_and_not_eq(std::string &field, std::string &prefix) {
  return fmt::format("{} LIKE '{}_%'", field, like_escape(prefix));
}





std::string in_table(const std::string &field, const std::string &table) {
  return fmt::format("{} in ({})", field, table);
}

std::string date_between (const std::string &field, int from, int until) {
  return fmt::format("{} >= toDate({}) AND {} <= toDate({})", field, from, field, until);
}

std::string timestamp_between(const std::string &field, long long from, long long until) {
  return fmt::format("{} >= {} AND {} <= {}", field, from, field, until);
}



// glob

std::string clear_glob(const std::string &q) {
  std::string_view query(q);
  size_t p = 0;
  size_t s = indexAny(query, "{[");
  if (s == std::string::npos) {
    return std::string(query);
  }
  bool found = false;
  std::string buf;
  while(1) {
    size_t e;
    if (query[s] == '{'){
      e = indexAny(query.substr(s), "}.");
      if (e == std::string::npos || query[s + e] == '.'){
        break;
      }
      e += s + 1;
      size_t delim = query.substr(0, e).find(',', s+1);
      if (delim == std::string::npos) {
        if (!found){
          found = true;
        }
        buf += std::string(query.substr(p, s - p)) + std::string(query.substr(s+1, e-s-2));
        p = e;
      }
    } else {
      e = indexAny(query.substr(s+1), "].");
      if (e == std::string::npos || query[s + e] == '.'){
        break;
      } else {
        if (e < 2) {
          found = true;
          buf += std::string(query.substr(p, s - p)) + std::string(query.substr(s+1, e));
          p = e + s + 2;
        }

      }
      e += s + 2;
    }
    if (e >= query.size()) {
      break;
    }
    s = indexAny(query.substr(e), "{[");
    if (s == std::string::npos) {
      break;
    }
    s += e;
  }

  if (found) {
    if (p < query.size()) {
      buf += query.substr(p);
    }
    return buf;
  }
  return std::string(query);
}

std::string in(const std::string &field,const std::vector<std::string> &list) {
  if (list.size() == 1){
    return eq(field, list[0]);
  }
  std::string buf;
  buf += field + " IN (";
  for (size_t i = 0; i < list.size(); i++) {
    if (i > 0) {
      buf += ",";
    }
    buf += quote(list[i]);
  }
  buf += ")";
  return buf;

}

// const StringPiece& unquoted
//std::string non_regexp_prefix(const std::string &expr){
//  pcrecpp::StringPiece e(expr);
//  std::string s = pcrecpp::RE::QuoteMeta(e);
//  for (size_t i = 0; i < expr.size(); ++i) {
//    if (expr[i] != s[i] || expr[i] == '\\'){
//      if (expr.size() > i + 1 && expr[i] == '|'){
//        size_t eq = lastIndexAny(expr.substr(0, i), "=~");
//        if (eq > 0){
//          return expr.substr(0, eq + 1);
//        }
//      }
//      return expr.substr(0, i);
//    }
//  }
//  return expr;
//}

std::string quote_regexp(std::string &key, std::string &value){
  bool start_line = value[0] == '^';
  bool end_line = value[value.size() - 1] == '$';
  if (start_line && end_line) {
    return fmt::format("'^{}{}{}'", key, op_eq, escape(value.substr(1)));
  } else if (start_line) {
    return fmt::format("'^{}{}{}'", key, op_eq, escape(value.substr(1)));
  } else if (end_line) {
    return fmt::format("'^{}{}.*{}'", key, op_eq, escape(value.substr(0)));
  }
  return fmt::format("'^{}{}.*{}'", key, op_eq, escape(value.substr()));
}

std::string glob(const std::string &field, const std::string &q, bool optionalDoAtEnd) {
  if (q == "*") {
    return "";
  }
  std::string query = clear_glob(q);

  if (!has_wildcard(query)) {
    if (optionalDoAtEnd) {
      std::vector<std::string> list = {query, query + '.'};
      return in(field, list);
    } else {
      return eq(field, query);
    }
  }
  Where w;
  size_t simple_prefix_pos =indexAny(query, "[]{}*?");
  std::string simple_prefix;
  if (simple_prefix_pos != std::string::npos) {
    simple_prefix = query.substr(0, simple_prefix_pos);
    w.And(has_prefix(field, simple_prefix));
  }

  if (simple_prefix.size() == query.size() - 1 && query[query.size() - 1] == '*'){
    return has_prefix(field, simple_prefix);
  }

  std::string postfix = "$";
  if (optionalDoAtEnd) {
    postfix = "[.]?$";
  }

  if (simple_prefix.empty()){
    return fmt::format("match({}, {})", field, quote("^" + glob_to_regexp(query) + postfix));
  }
  return fmt::format("{} AND match({}, {})", has_prefix(field, simple_prefix), field, quote("^" + glob_to_regexp(query) + postfix));
}

std::string Glob(const std::string &field, const std::string &query){
  return glob(field, query, false);
}

std::string TreeGlob(const std::string &field, const std::string &query){
  return glob(field, query, true);
}

std::string concat_match_kv(std::string &key, std::string &value) {
  bool start_line = value[0] == '^';
  bool end_line = value[value.size() - 1] == '$';
  if (start_line){
    return key + op_eq + value.substr(1);
  } else if (end_line) {
    return key + op_eq + value + "\\\\%";
  }
  return key + op_eq + "\\\\%" + value;
}


std::string reverse_string(const std::string &target){
  std::string s = target;
  std::string delimiter = ".";
  size_t pos = 0;
  std::string token;
  std::string result;
  while ((pos = s.find_last_of(delimiter)) != std::string::npos) {
    token = s.substr(pos + delimiter.size());
    result += token + delimiter;
    s.erase(pos);
  }
  result += s;
  return result;
}


std::vector<std::string> split(const std::string &target, const std::string& delimiter){
  std::string s = target;
  size_t pos = 0;
  std::string token;
  std::vector<std::string> result;
  while ((pos = s.find_first_of(delimiter)) != std::string::npos) {
    token = s.substr(0, pos);
    result.push_back(token);
    s.erase(0, pos + delimiter.size());
  }
  result.push_back(s);
  return result;
}

//std::string match(std::string &field, std::string &key, std::string &value){
//  if (value.empty() || value == "*"){
//    return like(field, key+"=%");
//  }
//  std::string expr = concat_match_kv(key, value);
//  std::string simple_prefix = non_regexp_prefix(expr);
//  if (simple_prefix.size() == expr.size()) {
//    return eq(field, expr);
//  } else if (simple_prefix.size() == expr.size() -1 && expr[expr.size() -1] == '$') {
//    return eq(field, simple_prefix);
//  }
//
//  if (simple_prefix.empty()) {
//    return fmt::format("match({}, {})", field, quote_regexp(key, value));
//  }
//
//  return fmt::format("%s AND match(%s, %s)",
//                     has_prefix(field, simple_prefix),
//                     field, quote_regexp(key, value));
//}


int IntervalStrings(std::string &s, int sign) {
  if (s[0] == '-'){
    sign = -1;
    s = s.substr(1);
  } else if (s[1] == '+'){
    sign = 1;
    s = s.substr(1);
  }
  int totalInterval = 0;
  while(!s.empty()){
    size_t j = 0;
    while(j < s.size() && std::isdigit(s[j])){
      j++;
    }
    std::string offsetStr;
    offsetStr = s.substr(0, j);
    s = s.substr(j);
    j = 0;
    while(j < s.size() && std::isalpha(s[j])){
      j++;
    }
    std::string unitStr;
    unitStr = s.substr(0, j);
    s = s.substr(j);
    int units = 0;
    if (unitStr == "s" || unitStr == "sec" || unitStr == "second" || unitStr == "seconds") {
      units = 1;
    } else if (unitStr == "m" || unitStr == "min" || unitStr == "mins" || unitStr == "minute" || unitStr == "minutes"){
      units = 60;
    } else if (unitStr == "h" || unitStr == "hour" || unitStr == "hours") {
      units = 60 * 60;
    } else if (unitStr == "d" || unitStr == "day" || unitStr == "days"){
      units = 24 * 60 * 60;
    } else if (unitStr == "w" || unitStr == "week" || unitStr == "weeks"){
      units = 7 * 24 * 60 * 60;
    } else if (unitStr == "mon" || unitStr == "month" || unitStr == "months"){
      units = 30 * 24 * 60 * 60;
    } else if (unitStr == "y" || unitStr == "year" || unitStr == "years"){
      units = 365 * 24 * 60 * 60;
    }
    int offset = stoi(offsetStr);
    totalInterval += sign * offset * units;
  }
  return totalInterval;
}



}
