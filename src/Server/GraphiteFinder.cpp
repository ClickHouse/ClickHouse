

#include "GraphiteFinder.h"
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <DataTypes/Serializations/SerializationDateTime.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Server/IServer.h>
#include <Poco/Util/LayeredConfiguration.h>
#include "GraphiteUtils.h"
#include "fmt/format.h"


namespace GraphiteCarbon
{


const int ReverseLevelOffset = 10000;
const int TreeLevelOffset = 20000;
const int ReverseTreeLevelOffset = 30000;
const std::string DefaultTreeDate = "1970-02-12";

const std::string graphite_index = "graphite_index";
const std::string graphite_reversed = "graphite_reversed";
const std::string graphite_prefix = "graphite_prefix";

std::shared_ptr<GraphiteFinder> new_plain_finder(DB::IServer & server, const std::string & table_name_)
{
    auto f = std::make_shared<IndexFinder>(table_name_, false, 1, false, false);
    if (table_name_ == graphite_index)
        return f;
    if (table_name_ == graphite_prefix)
    {
        const auto & prefix = server.config().getString("graphite_carbon.graphite_prefix.prefix", "prefix");
        return std::make_shared<PrefixFinder>(f, prefix);
    }
    if (table_name_ == graphite_reversed)
    {
        return std::make_shared<ReverseFinder>(f, table_name_);
    }
    return f;

}
std::string MetricsFind(DB::IServer & server, const std::string & table_name, const std::string & query, int from, int until, const std::string & format)
{
    auto f = new_plain_finder(server, table_name);
    return f->generate_query(query, from, until, format);
}

Where BaseFinder::where(const std::string & query)
{
    std::string::difference_type level = std::count(query.begin(), query.end(), '.') + 1;
    Where w;
    w.And(eq("Level", level));
    w.And(TreeGlob("Path", query));
    return w;
}

std::string
BaseFinder::generate_query(const std::string & query, int from = 0, int until = 0, const std::string & format = "TabSeparatedRaw")
{
    Where w = where(query);
    bool use_daily;
    if (from > 0 && until > 0)
    {
        use_daily = true;
    }
    else
    {
        use_daily = false;
    }

    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    std::time_t time_from = std::chrono::system_clock::to_time_t(now + std::chrono::seconds(from));
    std::time_t time_until = std::chrono::system_clock::to_time_t(now + std::chrono::seconds(until));
    DB::WriteBufferFromOwnString date_from;
    DB::WriteBufferFromOwnString date_until;
    DB::writeDateTimeText(time_from, date_from);
    DB::writeDateTimeText(time_until, date_until);

    if (use_daily)
    {
        w.And(fmt::format("Date >='{}' AND Date <= '{}'", date_from.str().substr(0, 10), date_until.str().substr(0, 10)));
    }
    else
    {
        w.And(eq(("Date"), DefaultTreeDate));
    }

    std::string q = fmt::format("SELECT Path FROM {} WHERE {} GROUP BY Path FORMAT {}", table, w.string(), format);
    return q;
}
BaseFinder::BaseFinder(const std::string & table_) : table(table_)
{
}

std::string
DateFinder::generate_query(const std::string & query, int from = 0, int until = 0, const std::string & format = "TabSeparatedRaw")
{
    Where w = finder.where(query);
    Where date_where;


    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    std::time_t time_from = std::chrono::system_clock::to_time_t(now + std::chrono::seconds(from));
    std::time_t time_until = std::chrono::system_clock::to_time_t(now + std::chrono::seconds(until));
    DB::WriteBufferFromOwnString date_from;
    DB::WriteBufferFromOwnString date_until;
    DB::writeDateTimeText(time_from, date_from);
    DB::writeDateTimeText(time_until, date_until);


    date_where.And(fmt::format("Date >='{}' AND Date <= '{}'", date_from.str().substr(0, 10), date_until.str().substr(0, 10)));

    std::string q = fmt::format(
        "SELECT Path FROM {} WHERE ({}) AND ({}) GROUP BY Path FORMAT {}", finder.table, date_where.string(), w.string(), format);
    return q;
}
std::string
DateFinderV3::generate_query(const std::string & query, int from = 0, int until = 0, const std::string & format = "TabSeparatedRaw")
{
    Where w = finder.where(reverse_string(query));
    Where date_where;

    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    std::time_t time_from = std::chrono::system_clock::to_time_t(now + std::chrono::seconds(from));
    std::time_t time_until = std::chrono::system_clock::to_time_t(now + std::chrono::seconds(until));
    DB::WriteBufferFromOwnString date_from;
    DB::WriteBufferFromOwnString date_until;
    DB::writeDateTimeText(time_from, date_from);
    DB::writeDateTimeText(time_until, date_until);


    date_where.And(fmt::format("Date >='{}' AND Date <= '{}'", date_from.str().substr(0, 10), date_until.str().substr(0, 10)));

    std::string q = fmt::format(
        "SELECT Path FROM {} WHERE ({}) AND ({}) GROUP BY Path FORMAT {}", finder.table, date_where.string(), w.string(), format);
    return q;
}
Where IndexFinder::where(const std::string & query, int levelOffset)
{
    std::string::difference_type level = std::count(query.begin(), query.end(), '.') + 1;
    Where w;
    w.And(eq("Level", level + levelOffset));
    w.And(TreeGlob("Path", query));
    return w;
}
std::string
IndexFinder::generate_query(const std::string & query, int from = 0, int until = 0, const std::string & format = "TabSeparatedRaw")
{
    std::string new_query = query;
    if (daily_enabled)
    {
        use_daily = true;
    }
    else
    {
        use_daily = false;
    }

    int levelOffset = 0;
    if (use_daily)
    {
        if (use_reverse)
        {
            levelOffset = ReverseLevelOffset;
        }
    }
    else
    {
        if (use_reverse)
        {
            levelOffset = ReverseTreeLevelOffset;
        }
        else
        {
            levelOffset = TreeLevelOffset;
        }
    }

    Where w = where(new_query, levelOffset);


    std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
    std::time_t time_from = std::chrono::system_clock::to_time_t(now + std::chrono::seconds(from));
    std::time_t time_until = std::chrono::system_clock::to_time_t(now + std::chrono::seconds(until));
    DB::WriteBufferFromOwnString date_from;
    DB::WriteBufferFromOwnString date_until;
    DB::writeDateTimeText(time_from, date_from);
    DB::writeDateTimeText(time_until, date_until);

    if (use_daily)
    {
        w.And(fmt::format("Date >='{}' AND Date <= '{}'", date_from.str().substr(0, 10), date_until.str().substr(0, 10)));
    }
    else
    {
        w.And(eq(("Date"), DefaultTreeDate));
    }

    std::string q = fmt::format("SELECT Path FROM {} WHERE {} GROUP BY Path FORMAT {}", table, w.string(), format);
    return q;
}

std::string
PrefixFinder::generate_query(const std::string & query, int from = 0, int until = 0, const std::string & format = "TabSeparatedRaw")
{
    std::vector<std::string> qs = split(query, ".");
    std::vector<std::string> ps = split(prefix, ".");
    if (qs.size() < ps.size())
    {
        part = join(qs, 0, qs.size(), ".") + ".";
    }
    return wrapped->generate_query(join(qs, ps.size(), qs.size(), "."), from, until, format);
}
std::string
ReverseFinder::generate_query(const std::string & query, int from = 0, int until = 0, const std::string & format = "TabSeparatedRaw")
{
    size_t p = std::count(query.begin(), query.end(), '.') + 1;
    if (p == 0 || p >= query.size() - 1)
    {
        return wrapped->generate_query(query, from, until, format);
    }

    std::string new_query = query.substr(p + 1);
    if (has_wildcard(new_query))
    {
        wrapped->generate_query(query, from, until, format);
    }
    return base_finder.generate_query(reverse_string(query), from, until);
}

std::string GraphiteFinder::generate_query(const std::string & query_, int from_, int until_, const std::string & format_)
{
    return fmt::format("error while creating query {} from {} untill {} format", query_, from_, until_, format_);
}
}
