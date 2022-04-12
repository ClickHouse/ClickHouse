#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace DB
{

void dumpSortDescription(const SortDescription & description, WriteBuffer & out)
{
    bool first = true;

    for (const auto & desc : description)
    {
        if (!first)
            out << ", ";
        first = false;

        out << desc.column_name;

        if (desc.direction > 0)
            out << " ASC";
        else
            out << " DESC";

        if (desc.with_fill)
            out << " WITH FILL";
    }
}

void SortColumnDescription::explain(JSONBuilder::JSONMap & map) const
{
    map.add("Column", column_name);
    map.add("Ascending", direction > 0);
    map.add("With Fill", with_fill);
}

std::string dumpSortDescription(const SortDescription & description)
{
    WriteBufferFromOwnString wb;
    dumpSortDescription(description, wb);
    return wb.str();
}

JSONBuilder::ItemPtr explainSortDescription(const SortDescription & description)
{
    auto json_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & descr : description)
    {
        auto json_map = std::make_unique<JSONBuilder::JSONMap>();
        descr.explain(*json_map);
        json_array->add(std::move(json_map));
    }

    return json_array;
}

}
