

#include "GraphiteFinder.h"
#include "GraphiteUtils.h"
#include "fmt/format.h"
#include <chrono>
#include <iostream>
#include <iomanip>
#include <ctime>
#include <chrono>
#include <ctime>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/Serializations/SerializationDateTime.h>

namespace DB
{


int IndexDirect = 1;
int IndexAuto = 2;
int IndexReversed = 3;
int ReverseLevelOffset = 10000;
int TreeLevelOffset = 20000;
int ReverseTreeLevelOffset = 30000;
std::string DefaultTreeDate = "1970-02-12";
int PrefixNotMatched = 1;
int PrefixMatched = 2;
int PrefixPartialMathed = 3;

std::string graphite_index = "graphite_index";

std::unordered_map<std::string, size_t> index_reverse =  {
    {"direct", IndexDirect},
    {"auto", IndexAuto},
    {"reversed", IndexReversed},
};
std::shared_ptr<GraphiteFinder> new_plain_finder(const std::string &table_name_)
{
  if (table_name_ == graphite_index)
    return std::make_shared<IndexFinder>(table_name_, false, 1, false, 1);
  return std::make_shared<IndexFinder>(table_name_, false, 1, false, 1);
}
std::string MetricsFind(const std::string &table_name,
                        const std::string &query,
                        int from,
                        int until,
                        const std::string &format) {
  auto f = new_plain_finder(table_name);
  return f->generate_query(query, from, until, format);
}

Where BaseFinder::where(const std::string &query)
{
  std::string::difference_type level = std::count(query.begin(), query.end(), '.') + 1;
  Where w;
  w.And(eq("Level", level));
  w.And(TreeGlob("Path", query));
  return w;
}

std::string BaseFinder::generate_query(const std::string &query, int from = 0, int until = 0, const std::string &format = "TabSeparatedRaw") 
{
  Where w = where(query);
  bool use_daily;
  if (from > 0 && until > 0) {
    use_daily = true;
  } else {
    use_daily = false;
  }

  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  std::time_t time_from = std::chrono::system_clock::to_time_t(
      now + std::chrono::seconds(from));
  std::time_t time_until = std::chrono::system_clock::to_time_t(
      now + std::chrono::seconds(until));
  WriteBufferFromOwnString date_from;
  WriteBufferFromOwnString date_until;
  writeDateTimeText(time_from, date_from);
  writeDateTimeText(time_until, date_until);

  if (use_daily) {
      w.And(fmt::format("Date >='{}' AND Date <= '{}'",
                        date_from.str().substr(0, 10), date_until.str().substr(0, 10)));
  } else {
      w.And(eq(("Date"), DefaultTreeDate));
  }

  std::string q = fmt::format("SELECT Path FROM {} WHERE {} GROUP BY Path FORMAT {}", table, w.string(), format);
  return q;
}
BaseFinder::BaseFinder(const std::string &table_) : table(table_) {}

std::string DateFinder::generate_query(const std::string &query, int from = 0, int until = 0, const std::string &format = "TabSeparatedRaw")
{
  Where w = finder.where(query);
  Where date_where;


  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  std::time_t time_from = std::chrono::system_clock::to_time_t(
      now + std::chrono::seconds(from));
  std::time_t time_until = std::chrono::system_clock::to_time_t(
      now + std::chrono::seconds(until));
  WriteBufferFromOwnString date_from;
  WriteBufferFromOwnString date_until;
  writeDateTimeText(time_from, date_from);
  writeDateTimeText(time_until, date_until);


  date_where.And(fmt::format("Date >='{}' AND Date <= '{}'",
                             date_from.str().substr(0, 10), date_until.str().substr(0, 10)));

  std::string q = fmt::format("SELECT Path FROM {} WHERE ({}) AND ({}) GROUP BY Path FORMAT {}", finder.table, date_where.string(), w.string(), format);
  return q;

}
std::string DateFinderV3::generate_query(const std::string &query, int from = 0, int until = 0, const std::string &format = "TabSeparatedRaw") 
{

  Where w = finder.where(reverse_string(query));
  Where date_where;

  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  std::time_t time_from = std::chrono::system_clock::to_time_t(
      now + std::chrono::seconds(from));
  std::time_t time_until = std::chrono::system_clock::to_time_t(
      now + std::chrono::seconds(until));
  WriteBufferFromOwnString date_from;
  WriteBufferFromOwnString date_until;
  writeDateTimeText(time_from, date_from);
  writeDateTimeText(time_until, date_until);


  date_where.And(fmt::format("Date >='{}' AND Date <= '{}'",
                            date_from.str().substr(0, 10), date_until.str().substr(0, 10)));

  std::string q = fmt::format("SELECT Path FROM {} WHERE ({}) AND ({}) GROUP BY Path FORMAT {}", finder.table, date_where.string(), w.string(), format);
  return q;

}
Where IndexFinder::where(const std::string &query, int levelOffset) {
  std::string::difference_type level = std::count(query.begin(), query.end(), '.') + 1;
  Where w;
  w.And(eq("Level", level+levelOffset));
  w.And(TreeGlob("Path", query));
  return w;
}
uint8_t IndexFinder::check_reverses(const std::string &query) 
{
  for (const auto& rule : conf_reverses) {
    if (!rule.prefix.empty() && !str_has_prefix(query, rule.prefix)) {
      continue;
    }
    if (!rule.suffix.empty() && !str_has_suffix(query, rule.suffix)) {
      continue;
    }
    std::smatch m;
    if (!std::regex_match(query, m, rule.regex)){
      continue;
    }
    return index_reverse[rule.reverse];
  }
  return conf_reverse;
}
bool IndexFinder::use_reverse(const std::string &query) 
{
  if (reverse == IndexDirect) {
    return false;
  } else if (reverse == IndexReversed) {
    return true;
  }
  reverse = check_reverses(query);
  if (reverse != IndexAuto) {
    return use_reverse(query);
  }
  size_t w = index_wildcard(query);
  if (w == std::string::npos) {
    reverse = IndexDirect;
    return use_reverse(query);
  }
  std::string before_wildcard = query.substr(0, w);

  std::string::difference_type firstWildcardNode = std::count(before_wildcard.begin(), before_wildcard.end(), '.') + 1;

  w = index_last_wildcard(query);
  std::string after_wildcard = query.substr(w);
  std::string::difference_type lastWildcardNode = std::count(before_wildcard.begin(), before_wildcard.end(), '.') + 1;
  if (firstWildcardNode < lastWildcardNode) {
    reverse = IndexReversed;
    return use_reverse(query);
  }
  reverse = IndexDirect;
  return use_reverse(query);
}

std::string IndexFinder::generate_query(const std::string &query, int from = 0, int until = 0, const std::string &format = "TabSeparatedRaw") 
{

  std::string new_query = query;
  if (daily_enabled) {
    use_daily = true;
  } else {
    use_daily = false;
  }

  int levelOffset = 0;
  if (use_daily) {
    if (use_reverse(new_query)) {
      levelOffset = ReverseLevelOffset;
    }
  } else {
    if (use_reverse(new_query)) {
      levelOffset = ReverseTreeLevelOffset;
    } else {
      levelOffset = TreeLevelOffset;
    }
  }

  if (use_reverse(new_query)) {
    new_query = reverse_string(new_query);
  }

  Where w = where(new_query, levelOffset);


  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  std::time_t time_from = std::chrono::system_clock::to_time_t(
      now + std::chrono::seconds(from));
  std::time_t time_until = std::chrono::system_clock::to_time_t(
      now + std::chrono::seconds(until));
  WriteBufferFromOwnString date_from;
  WriteBufferFromOwnString date_until;
  writeDateTimeText(time_from, date_from);
  writeDateTimeText(time_until, date_until);

  if (use_daily) {
    w.And(fmt::format("Date >='{}' AND Date <= '{}'",
                      date_from.str().substr(0, 10), date_until.str().substr(0, 10)));
  } else {
    w.And(eq(("Date"), DefaultTreeDate));
  }

  std::string q = fmt::format("SELECT Path FROM {} WHERE {} GROUP BY Path FORMAT {}", table, w.string(), format);
  return q;
}

std::string PrefixFinder::generate_query(const std::string &query, int from = 0, int until = 0, const std::string &format = "TabSeparatedRaw") 
{
  std::vector<std::string> qs = split(query, ".");
  std::vector<std::string> ps = split(prefix, ".");
  if (qs.size() < ps.size()) {
    part = join(qs, 0, qs.size(), ".") + ".";
  }
  return wrapped->generate_query(join(qs, ps.size(), qs.size(), "."), from, until, format);

}
std::string ReverseFinder::generate_query(const std::string &query, int from = 0, int until = 0, const std::string &format = "TabSeparatedRaw") 
{
  size_t p = std::count(query.begin(), query.end(), '.') + 1;
  if (p == 0 || p >= query.size() - 1) {
    return wrapped->generate_query(query, from, until, format);
  }

  std::string new_query = query.substr(p+1);
  if (has_wildcard(new_query)) {
    wrapped->generate_query(query, from, until, format);
  }
  is_used = true;
  return base_finder.generate_query(reverse_string(query), from, until);

}

std::string GraphiteFinder::generate_query(const std::string &query_,
                                           int from_,
                                           int until_,
                                           const std::string &format_) {
  return fmt::format("error while creating query {} from {} untill {} format",
                     query_,
                     from_,
                     until_,
                     format_);
}
}
