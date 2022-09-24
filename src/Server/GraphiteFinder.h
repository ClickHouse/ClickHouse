#pragma once

#include <string>
#include <regex>
#include <unordered_map>
#include "GraphiteUtils.h"
#include <iostream>
#include "fmt/format.h"

namespace DB {

class Result {
 public:
  std::string GetList();
  std::vector<std::string> GetSeries();
  std::string GetAbs(std::string &v);
};

struct IndexReverseRule {
  std::string suffix;
  std::string prefix;
  std::string regex_str;
  std::regex regex;
  std::string reverse;
};

extern int IndexDirect;
extern int IndexAuto;
extern int IndexReversed;

extern int ReverseLevelOffset;
extern int TreeLevelOffset;
extern int ReverseTreeLevelOffset;
extern std::string DefaultTreeDate;

extern int PrefixNotMatched;
extern int PrefixMatched;
extern int PrefixPartialMathed;

extern std::unordered_map<std::string, size_t> index_reverse;

class GraphiteFinder {
 public:
  virtual std::string generate_query(const std::string &query_,
                                     int from_ = 0,
                                     int until_ = 0,
                                     const std::string &format_ = "TabSeparatedRaw") {
    return fmt::format("error while creating query {} from {} untill {} format",
                       query_,
                       from_,
                       until_,
                       format_);
  }
  virtual ~GraphiteFinder() = default;
};

class BaseFinder : virtual public GraphiteFinder {
 public:
  BaseFinder(const std::string &table_) : table(table_) {}
  std::string generate_query(const std::string &query_,
                             int from_,
                             int until_,
                             const std::string &format_) override;
  Where where(const std::string &query_);
  std::string table;
  ~BaseFinder() override = default;

};

class BlackListFinder : public GraphiteFinder {
 public:
  BlackListFinder(std::shared_ptr<GraphiteFinder> &wrapped_, std::vector<std::regex> &blacklist_)
      : wrapped(wrapped_), blacklist(blacklist_), matched(false) {}
  std::string generate_query(const std::string &query_,
                             int from_,
                             int until_,
                             const std::string &format_) override;
 private:
  std::shared_ptr<GraphiteFinder> wrapped;
  std::vector<std::regex> blacklist;
  bool matched;
};

class DateFinder : public GraphiteFinder {
 public:
  DateFinder(const std::string &table_) : finder(table_) {}
  std::string generate_query(const std::string &query_,
                             int from_,
                             int until_,
                             const std::string &format_) override;
  BaseFinder finder;
};

class DateFinderV3 : public GraphiteFinder {
 public:
  DateFinderV3(const std::string &table_) : finder(table_) {}
  std::string generate_query(const std::string &query_,
                             int from_,
                             int until_,
                             const std::string &format_) override;
  BaseFinder finder;
};

class IndexFinder : public GraphiteFinder {
 public:
  IndexFinder(const std::string &table_,
              bool daily_enabled_,
              uint8_t conf_reverse_,
              bool use_daily_,
              uint8_t reverse_)
      : table(table_),
        daily_enabled(daily_enabled_),
        conf_reverse(conf_reverse_),
        conf_reverses(1),
        reverse(reverse_),
        use_daily(use_daily_) {}
  Where where(const std::string &query_, int levelOffset_);
  uint8_t check_reverses(const std::string &query);
  bool use_reverse(const std::string &query);
  std::string generate_query(const std::string &query_,
                             int from_,
                             int until_,
                             const std::string &format_) override;
  const std::string table;
  bool daily_enabled;
  size_t conf_reverse;
  std::vector<IndexReverseRule> conf_reverses;
  uint8_t reverse;
  std::string body;
  bool use_daily;
};

class PrefixFinder : public GraphiteFinder {
 public:
  PrefixFinder(std::shared_ptr<GraphiteFinder> &wrapped_,
               std::string &prefix_,
               char *prefixBytes_,
               int matched_,
               std::string &part_) :
      wrapped(wrapped_), prefix(prefix_), matched(matched_), part(part_) {
    strcat(prefixBytes_, ".");
    prefix_bytes = prefixBytes_;
  }
  std::string generate_query(const std::string &query_,
                             int from_,
                             int until_,
                             const std::string &format_) override;

  std::shared_ptr<GraphiteFinder> wrapped;
  std::string prefix;
  char *prefix_bytes;
  int matched;
  std::string &part;
};

class ReverseFinder : public GraphiteFinder {
 public:
  ReverseFinder(std::shared_ptr<GraphiteFinder> &wrapped_, std::string &table_, bool is_used_)
      :
      wrapped(wrapped_), base_finder(table_), table(table_), is_used(is_used_) {}
  std::string generate_query(const std::string &query_,
                             int from_,
                             int until_,
                             const std::string &format_) override;

  std::shared_ptr<GraphiteFinder> wrapped;
  BaseFinder base_finder;
  std::string &table;
  bool is_used;
};

std::shared_ptr<GraphiteFinder> new_plain_finder(const std::string &table_name_);

std::string MetricsFind(const std::string &table_name,
                 const std::string &query,
                 int from,
                 int until,
                 const std::string &format);

}
