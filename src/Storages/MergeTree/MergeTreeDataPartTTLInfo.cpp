#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/quoteString.h>
#include <algorithm>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <base/JSON.h>

namespace DB
{

void MergeTreeDataPartTTLInfo::update(time_t time)
{
    if (time && (!min || time < min))
        min = time;

    max = std::max(time, max);
}

void MergeTreeDataPartTTLInfo::update(const MergeTreeDataPartTTLInfo & other_info)
{
    if (other_info.min && (!min || other_info.min < min))
        min = other_info.min;

    max = std::max(other_info.max, max);
    if (ttl_finished.has_value())
        ttl_finished = ttl_finished.value() && other_info.finished();
    else
        ttl_finished = other_info.finished();
}

void MergeTreeDataPartTTLInfos::update(const MergeTreeDataPartTTLInfos & other_infos)
{
    for (const auto & [name, ttl_info] : other_infos.columns_ttl)
    {
        columns_ttl[name].update(ttl_info);
        updatePartMinMaxTTL(ttl_info.min, ttl_info.max);
    }

    for (const auto & [name, ttl_info] : other_infos.rows_where_ttl)
    {
        rows_where_ttl[name].update(ttl_info);
        updatePartMinMaxTTL(ttl_info.min, ttl_info.max);
    }

    for (const auto & [name, ttl_info] : other_infos.group_by_ttl)
    {
        group_by_ttl[name].update(ttl_info);
        updatePartMinMaxTTL(ttl_info.min, ttl_info.max);
    }

    for (const auto & [name, ttl_info] : other_infos.recompression_ttl)
        recompression_ttl[name].update(ttl_info);

    for (const auto & [expression, ttl_info] : other_infos.moves_ttl)
        moves_ttl[expression].update(ttl_info);

    table_ttl.update(other_infos.table_ttl);
    updatePartMinMaxTTL(table_ttl.min, table_ttl.max);
}


void MergeTreeDataPartTTLInfos::read(ReadBuffer & in)
{
    String json_str;
    readString(json_str, in);
    assertEOF(in);

    JSON json(json_str);
    if (json.has("columns"))
    {
        const JSON & columns = json["columns"];
        for (auto col : columns) // NOLINT
        {
            MergeTreeDataPartTTLInfo ttl_info;
            ttl_info.min = col["min"].getUInt();
            ttl_info.max = col["max"].getUInt();

            if (col.has("finished"))
                ttl_info.ttl_finished = col["finished"].getUInt();

            String name = col["name"].getString();
            columns_ttl.emplace(name, ttl_info);

            updatePartMinMaxTTL(ttl_info.min, ttl_info.max);
        }
    }
    if (json.has("table"))
    {
        const JSON & table = json["table"];
        table_ttl.min = table["min"].getUInt();
        table_ttl.max = table["max"].getUInt();

        if (table.has("finished"))
            table_ttl.ttl_finished = table["finished"].getUInt();

        updatePartMinMaxTTL(table_ttl.min, table_ttl.max);
    }

    auto fill_ttl_info_map = [this](const JSON & json_part, TTLInfoMap & ttl_info_map, bool update_min_max)
    {
        for (auto elem : json_part) // NOLINT
        {
            MergeTreeDataPartTTLInfo ttl_info;
            ttl_info.min = elem["min"].getUInt();
            ttl_info.max = elem["max"].getUInt();

            if (elem.has("finished"))
                ttl_info.ttl_finished = elem["finished"].getUInt();

            String expression = elem["expression"].getString();
            ttl_info_map.emplace(expression, ttl_info);

            if (update_min_max)
                updatePartMinMaxTTL(ttl_info.min, ttl_info.max);
        }
    };

    if (json.has("moves"))
    {
        const JSON & moves = json["moves"];
        fill_ttl_info_map(moves, moves_ttl, false);
    }
    if (json.has("recompression"))
    {
        const JSON & recompressions = json["recompression"];
        fill_ttl_info_map(recompressions, recompression_ttl, false);
    }
    if (json.has("group_by"))
    {
        const JSON & group_by = json["group_by"];
        fill_ttl_info_map(group_by, group_by_ttl, true);
    }
    if (json.has("rows_where"))
    {
        const JSON & rows_where = json["rows_where"];
        fill_ttl_info_map(rows_where, rows_where_ttl, true);
    }
}


void MergeTreeDataPartTTLInfos::write(WriteBuffer & out) const
{
    writeString("ttl format version: 1\n", out);
    writeString("{", out);
    if (!columns_ttl.empty())
    {
        writeString("\"columns\":[", out);
        for (auto it = columns_ttl.begin(); it != columns_ttl.end(); ++it)
        {
            if (it != columns_ttl.begin())
                writeString(",", out);

            writeString("{\"name\":", out);
            writeString(doubleQuoteString(it->first), out);
            writeString(",\"min\":", out);
            writeIntText(it->second.min, out);
            writeString(",\"max\":", out);
            writeIntText(it->second.max, out);
            writeString(R"(,"finished":)", out);
            writeIntText(static_cast<uint8_t>(it->second.finished()), out);
            writeString("}", out);
        }
        writeString("]", out);
    }
    if (table_ttl.min)
    {
        if (!columns_ttl.empty())
            writeString(",", out);
        writeString(R"("table":{"min":)", out);
        writeIntText(table_ttl.min, out);
        writeString(R"(,"max":)", out);
        writeIntText(table_ttl.max, out);
        writeString(R"(,"finished":)", out);
        writeIntText(static_cast<uint8_t>(table_ttl.finished()), out);
        writeString("}", out);
    }

    auto write_infos = [&out](const TTLInfoMap & infos, const String & type, bool is_first)
    {
        if (!is_first)
            writeString(",", out);

        writeDoubleQuotedString(type, out);
        writeString(":[", out);
        for (auto it = infos.begin(); it != infos.end(); ++it)
        {
            if (it != infos.begin())
                writeString(",", out);

            writeString(R"({"expression":)", out);
            writeString(doubleQuoteString(it->first), out);
            writeString(R"(,"min":)", out);
            writeIntText(it->second.min, out);
            writeString(R"(,"max":)", out);
            writeIntText(it->second.max, out);
            writeString(R"(,"finished":)", out);
            writeIntText(static_cast<uint8_t>(it->second.finished()), out);
            writeString("}", out);
        }
        writeString("]", out);
    };

    bool is_first = columns_ttl.empty() && !table_ttl.min;
    if (!moves_ttl.empty())
    {
        write_infos(moves_ttl, "moves", is_first);
        is_first = false;
    }

    if (!recompression_ttl.empty())
    {
        write_infos(recompression_ttl, "recompression", is_first);
        is_first = false;
    }

    if (!group_by_ttl.empty())
    {
        write_infos(group_by_ttl, "group_by", is_first);
        is_first = false;
    }

    if (!rows_where_ttl.empty())
        write_infos(rows_where_ttl, "rows_where", is_first);

    writeString("}", out);
}

time_t MergeTreeDataPartTTLInfos::getMinimalMaxRecompressionTTL() const
{
    time_t max = std::numeric_limits<time_t>::max();
    for (const auto & [name, info] : recompression_ttl)
        if (info.max != 0)
            max = std::min(info.max, max);

    if (max == std::numeric_limits<time_t>::max())
        return 0;

    return max;
}

bool MergeTreeDataPartTTLInfos::hasAnyNonFinishedTTLs() const
{
    auto has_non_finished_ttl = [] (const TTLInfoMap & map) -> bool
    {
        for (const auto & [name, info] : map)
        {
            if (!info.finished())
                return true;
        }
        return false;
    };

    if (!table_ttl.finished())
        return true;

    if (has_non_finished_ttl(columns_ttl))
        return true;

    if (has_non_finished_ttl(rows_where_ttl))
        return true;

    if (has_non_finished_ttl(moves_ttl))
        return true;

    if (has_non_finished_ttl(recompression_ttl))
        return true;

    if (has_non_finished_ttl(group_by_ttl))
        return true;

    return false;
}

namespace
{
    /// We had backward incompatibility in representation of serialized expressions, example:
    ///
    /// `expired + toIntervalSecond(20)` vs `plus(expired, toIntervalSecond(20))`
    /// Since they are stored as strings we cannot compare them directly as strings
    /// To avoid backward incompatibility we parse them and check AST hashes.
    /// This O(N^2), but amount of TTLs should be small, so it should be Ok.
    auto tryToFindTTLExpressionInMapByASTMatching(const TTLInfoMap & ttl_info_map, const std::string & result_column)
    {
        ParserExpression parser;
        auto ast_needle = parseQuery(parser, result_column.data(), result_column.data() + result_column.size(), "", 0, 0, 0);
        for (auto it = ttl_info_map.begin(); it != ttl_info_map.end(); ++it)
        {
            const std::string & stored_expression = it->first;
            auto ast_candidate = parseQuery(parser, stored_expression.data(), stored_expression.data() + stored_expression.size(), "", 0, 0, 0);
            if (ast_candidate->getTreeHash(false) == ast_needle->getTreeHash(false))
                return it;
        }
        return ttl_info_map.end();
    }
}

std::optional<TTLDescription> selectTTLDescriptionForTTLInfos(const TTLDescriptions & descriptions, const TTLInfoMap & ttl_info_map, time_t current_time, bool use_max)
{
    time_t best_ttl_time = 0;
    TTLDescriptions::const_iterator best_entry_it;
    for (auto ttl_entry_it = descriptions.begin(); ttl_entry_it != descriptions.end(); ++ttl_entry_it)
    {
        auto ttl_info_it = ttl_info_map.find(ttl_entry_it->result_column);

        if (ttl_info_it == ttl_info_map.end())
        {
            ttl_info_it = tryToFindTTLExpressionInMapByASTMatching(ttl_info_map, ttl_entry_it->result_column);
            if (ttl_info_it == ttl_info_map.end())
                continue;
        }

        time_t ttl_time;

        if (use_max)
            ttl_time = ttl_info_it->second.max;
        else
            ttl_time = ttl_info_it->second.min;

        /// Prefer TTL rule which went into action last.
        if (ttl_time <= current_time
                && best_ttl_time <= ttl_time)
        {
            best_entry_it = ttl_entry_it;
            best_ttl_time = ttl_time;
        }
    }

    return best_ttl_time ? *best_entry_it : std::optional<TTLDescription>();
}


}
