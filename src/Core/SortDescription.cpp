#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include "Core/Field.h"

namespace DB
{

void dumpSortDescription(const SortDescription & description, const Block &, WriteBuffer & out)
{
    bool first = true;

    for (const auto & desc : description)
    {
        if (!first)
            out << ", ";
        first = false;

        if (!desc.column_name.empty())
            out << desc.column_name;
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty column name.");

        if (desc.direction > 0)
            out << " ASC";
        else
            out << " DESC";

        if (desc.with_fill)
            out << " WITH FILL";
    }
}

void SortColumnDescription::explain(JSONBuilder::JSONMap & map, const Block &) const
{
    if (!column_name.empty())
        map.add("Column", column_name);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty column name.");

    map.add("Ascending", direction > 0);
    map.add("With Fill", with_fill);
}

std::string dumpSortDescription(const SortDescription & description)
{
    WriteBufferFromOwnString wb;
    dumpSortDescription(description, Block{}, wb);
    return wb.str();
}

JSONBuilder::ItemPtr explainSortDescription(const SortDescription & description, const Block & header)
{
    auto json_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & descr : description)
    {
        auto json_map = std::make_unique<JSONBuilder::JSONMap>();
        descr.explain(*json_map, header);
        json_array->add(std::move(json_map));
    }

    return json_array;
}

}
