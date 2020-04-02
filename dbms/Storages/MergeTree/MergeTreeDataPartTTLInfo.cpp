#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/quoteString.h>

#include <common/JSON.h>

namespace DB
{

void MergeTreeDataPartTTLInfos::update(const MergeTreeDataPartTTLInfos & other_infos)
{
    for (const auto & [name, ttl_info] : other_infos.columns_ttl)
    {
        columns_ttl[name].update(ttl_info);
        updatePartMinMaxTTL(ttl_info.min, ttl_info.max);
    }

    for (const auto & [expression, ttl_info] : other_infos.moves_ttl)
    {
        moves_ttl[expression].update(ttl_info);
    }

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

        updatePartMinMaxTTL(table_ttl.min, table_ttl.max);
    }
    if (json.has("moves"))
    {
        const JSON & moves = json["moves"];
        for (auto move : moves) // NOLINT
        {
            MergeTreeDataPartTTLInfo ttl_info;
            ttl_info.min = move["min"].getUInt();
            ttl_info.max = move["max"].getUInt();
            String expression = move["expression"].getString();
            moves_ttl.emplace(expression, ttl_info);
        }
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
        writeString("}", out);
    }
    if (!moves_ttl.empty())
    {
        if (!columns_ttl.empty() || table_ttl.min)
            writeString(",", out);
        writeString(R"("moves":[)", out);
        for (auto it = moves_ttl.begin(); it != moves_ttl.end(); ++it)
        {
            if (it != moves_ttl.begin())
                writeString(",", out);

            writeString(R"({"expression":)", out);
            writeString(doubleQuoteString(it->first), out);
            writeString(R"(,"min":)", out);
            writeIntText(it->second.min, out);
            writeString(R"(,"max":)", out);
            writeIntText(it->second.max, out);
            writeString("}", out);
        }
        writeString("]", out);
    }
    writeString("}", out);
}

}
