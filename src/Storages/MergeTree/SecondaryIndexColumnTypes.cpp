#include <Storages/MergeTree/SecondaryIndexColumnTypes.h>

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/ColumnsDescription.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>

#include <sstream>

#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

std::optional<String> SecondaryIndexColumnTypes::tryGetBuiltType(const String & index_name, const String & column_name) const
{
    auto index_it = index_to_column_types.find(index_name);
    if (index_it == index_to_column_types.end())
        return {};

    auto column_it = index_it->second.find(column_name);
    if (column_it == index_it->second.end())
        return {};

    return column_it->second;
}

void SecondaryIndexColumnTypes::writeJSON(WriteBuffer & out) const
{
    Poco::JSON::Object root;
    for (const auto & [index_name, columns] : index_to_column_types)
    {
        Poco::JSON::Object::Ptr index_object = new Poco::JSON::Object();
        for (const auto & [column_name, type_name] : columns)
            index_object->set(column_name, type_name);
        root.set(index_name, index_object);
    }

    std::ostringstream oss; // NOLINT(*-dynamic-static-initializers)
    oss.exceptions(std::ios::failbit);
    root.stringify(oss);
    writeString(oss.str(), out);
}

SecondaryIndexColumnTypes SecondaryIndexColumnTypes::readJSON(ReadBuffer & in)
{
    String json_str;
    {
        WriteBufferFromString buf(json_str);
        copyData(in, buf);
        buf.finalize();
    }

    SecondaryIndexColumnTypes result;
    if (json_str.empty())
        return result;

    Poco::JSON::Parser parser;
    auto root = parser.parse(json_str).extract<Poco::JSON::Object::Ptr>();

    for (const auto & index_entry : *root)
    {
        auto index_object = index_entry.second.extract<Poco::JSON::Object::Ptr>();
        auto & columns = result.index_to_column_types[index_entry.first];
        for (const auto & column_entry : *index_object)
            columns[column_entry.first] = column_entry.second.convert<String>();
    }

    return result;
}

SecondaryIndexColumnTypes SecondaryIndexColumnTypes::compute(
    const std::vector<MergeTreeIndexPtr> & rebuilt_indices,
    const std::vector<String> & hardlinked_index_names,
    const ColumnsDescription & new_part_columns,
    const SecondaryIndexColumnTypes & source_part_types)
{
    SecondaryIndexColumnTypes result;

    auto options = GetColumnsOptions(GetColumnsOptions::All).withSubcolumns();

    /// Rebuilt indices were written against the type the index helper carries. Record a required
    /// column when that type differs from the part's own declared type. A column the part does not
    /// carry does not resolve here; leave it unrecorded so the read path refuses its index instead of
    /// certifying a granule whose data the part is missing.
    for (const auto & index : rebuilt_indices)
    {
        ColumnTypes columns;
        for (const auto & required_column : index->getColumnsWithTypesRequiredForIndexCalc())
        {
            const String built_type_name = required_column.type->getName();
            auto part_column = new_part_columns.tryGetColumn(options, required_column.name);
            if (part_column && part_column->type->getName() != built_type_name)
                columns.emplace(required_column.name, built_type_name);
        }

        if (!columns.empty())
            result.index_to_column_types.emplace(index->index.name, std::move(columns));
    }

    /// Hardlinked indices inherit their granules and columns unchanged, so the source record still holds.
    for (const auto & index_name : hardlinked_index_names)
    {
        auto it = source_part_types.index_to_column_types.find(index_name);
        if (it != source_part_types.index_to_column_types.end())
            result.index_to_column_types.emplace(index_name, it->second);
    }

    return result;
}

}
