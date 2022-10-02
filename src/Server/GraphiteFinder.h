#pragma once

#include <iostream>
#include <regex>
#include <string>
#include <unordered_map>
#include <utility>
#include "GraphiteUtils.h"
#include "fmt/format.h"

namespace GraphiteCarbon
{


extern int IndexDirect;
extern int IndexAuto;
extern int IndexReversed;

extern std::string DefaultTreeDate;


extern std::unordered_map<std::string, size_t> index_reverse;

class GraphiteFinder
{
public:
    virtual std::string
    generate_query(const std::string & query_, int from_ = 0, int until_ = 0, const std::string & format_ = "TabSeparatedRaw");
    virtual ~GraphiteFinder() = default;
};

class BaseFinder : virtual public GraphiteFinder
{
public:
    explicit BaseFinder(const std::string & table_);
    std::string generate_query(const std::string & query_, int from_, int until_, const std::string & format_) override;
    Where where(const std::string & query_);
    std::string table;
    ~BaseFinder() override = default;
};

class DateFinder : public GraphiteFinder
{
public:
    DateFinder(std::string & table_) : finder(table_) { }
    std::string generate_query(const std::string & query_, int from_, int until_, const std::string & format_) override;
    BaseFinder finder;
};

class DateFinderV3 : public GraphiteFinder
{
public:
    DateFinderV3(const std::string & table_) : finder(table_) { }
    std::string generate_query(const std::string & query_, int from_, int until_, const std::string & format_) override;
    BaseFinder finder;
};

class IndexFinder : public GraphiteFinder
{
public:
    IndexFinder(const std::string & table_, bool daily_enabled_, uint8_t conf_reverse_, bool use_daily_, bool reverse_)
        : table(table_), daily_enabled(daily_enabled_), conf_reverse(conf_reverse_), use_reverse(reverse_), use_daily(use_daily_)
    {
    }
    Where where(const std::string & query_, int levelOffset_);
    std::string generate_query(const std::string & query_, int from_, int until_, const std::string & format_) override;
    const std::string table;
    bool daily_enabled;
    size_t conf_reverse;
    bool use_reverse;
    std::string body;
    bool use_daily;
};

class PrefixFinder : public GraphiteFinder
{
public:
    PrefixFinder(std::shared_ptr<GraphiteFinder> wrapped_, const std::string & prefix_) : wrapped(std::move(wrapped_)), prefix(prefix_) { }
    std::string generate_query(const std::string & query_, int from_, int until_, const std::string & format_) override;

    std::shared_ptr<GraphiteFinder> wrapped;
    std::string prefix;
    std::string part{};
};

class ReverseFinder : public GraphiteFinder
{
public:
    ReverseFinder(std::shared_ptr<GraphiteFinder> wrapped_, const std::string & table_)
        : wrapped(std::move(wrapped_)), base_finder(table_), table(table_)
    {
    }
    std::string generate_query(const std::string & query_, int from_, int until_, const std::string & format_) override;

    std::shared_ptr<GraphiteFinder> wrapped;
    BaseFinder base_finder;
    std::string table;
    bool is_used{};
};

std::shared_ptr<GraphiteFinder> new_plain_finder(const std::string & table_name_);

std::string MetricsFind(const std::string & table_name, const std::string & query, int from, int until, const std::string & format);

}
