#include "ArrowFormatUtil.h"
#if USE_PARQUET || USE_ORC
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
/// Count the number of indices for types.
/// For orc format, nested_type_has_index is true, a complex type takes one index.
size_t ArrowFormatUtil::countIndicesForType(std::shared_ptr<arrow::DataType> type)
{
    if (type->id() == arrow::Type::LIST)
    {
        auto ret = countIndicesForType(static_cast<arrow::ListType *>(type.get())->value_type());
        if (nested_type_has_index)
            ret = ret + 1;
        return ret;
    }

    if (type->id() == arrow::Type::STRUCT)
    {
        int indices = nested_type_has_index ? 1 : 0;
        auto * struct_type = static_cast<arrow::StructType *>(type.get());
        for (int i = 0; i != struct_type->num_fields(); ++i)
            indices += countIndicesForType(struct_type->field(i)->type());
        return indices;
    }

    if (type->id() == arrow::Type::MAP)
    {
        auto * map_type = static_cast<arrow::MapType *>(type.get());
        auto ret = countIndicesForType(map_type->key_type()) + countIndicesForType(map_type->item_type());
        if (nested_type_has_index)
            ret += 1;
        return ret;
    }

    return 1;
}

/// Recursively count every field indices. Return a map
///   - key: field name with full path. eg. a struct field's name is like a.x.i
///   - value: a pair, first value refers to this field's start index, second value refers to how many
///   indices this field take. eg.
/// For a parquet schema {x: int , y: {i: int, j: int}}, the return will be
/// - x: (0, 1)
/// - y: (1, 2)
/// - y.i: (1, 1)
/// - y.j: (2, 1)
std::map<std::string, std::pair<int, int>>
ArrowFormatUtil::calculateFieldIndices(const arrow::Schema & schema)
{
    std::map<std::string, std::pair<int, int>> result;
    int index_start = nested_type_has_index ? 1 : 0;
    for (int i = 0; i < schema.num_fields(); ++i)
    {
        const auto & field = schema.field(i);
        calculateFieldIndices(*field, index_start, result);
    }
    return result;
}

void ArrowFormatUtil::calculateFieldIndices(const arrow::Field & field,
    int & current_start_index,
    std::map<std::string, std::pair<int, int>> & result,
    const std::string & name_prefix)
{
    std::string field_name = field.name();
    const auto & field_type = field.type();
    if (field_name.empty())
    {
        current_start_index += countIndicesForType(field_type);
        return;
    }
    if (ignore_case)
    {
        boost::to_lower(field_name);
    }

    std::string full_path_name = name_prefix.empty() ? field_name : name_prefix + "." + field_name;
    auto & index_info = result[full_path_name];
    index_info.first = current_start_index;
    if (field_type->id() == arrow::Type::STRUCT)
    {
        if (nested_type_has_index)
            current_start_index += 1;

        auto * struct_type = static_cast<arrow::StructType *>(field_type.get());
        for (int i = 0, n = struct_type->num_fields(); i < n ; ++i)
        {
            const auto & sub_field = struct_type->field(i);
            calculateFieldIndices(*sub_field, current_start_index, result, full_path_name);
        }
    }
    else
    {
        current_start_index += countIndicesForType(field_type);
    }
    index_info.second = current_start_index - index_info.first;
}

/// Only collect the required fields' indices. Eg. when just read a field of a struct,
/// don't need to collect the whole indices in this struct.
std::vector<int> ArrowFormatUtil::findRequiredIndices(const Block & header,
    const arrow::Schema & schema)
{
    std::vector<int> required_indices;
    std::set<std::string> added_nested_table;
    std::set<int> added_indices;
    /// Flat all named fields' index information into a map.
    auto fields_indices = calculateFieldIndices(schema);
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & named_col = header.getByPosition(i);
        std::string col_name = named_col.name;
        if (ignore_case)
            boost::to_lower(col_name);
        /// Since all named fields are flatten into a map, we should found the column by name
        /// in this map.
        auto it = fields_indices.find(col_name);
        /// It may be a nested table, not a named struct but a list field.
        if (it == fields_indices.end() && import_nested)
        {
            col_name = Nested::splitName(col_name).first;
            if (added_nested_table.contains(col_name))
                continue;
            added_nested_table.insert(col_name);
            it = fields_indices.find(col_name);
        }

        if (it == fields_indices.end())
        {
            if (!allow_missing_columns)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found field({}) in arrow schema:{}", named_col.name, schema.ToString());
            else
                continue;
        }
        for (int j = 0; j < it->second.second; ++j)
        {
            auto index = it->second.first + j;
            if (!added_indices.contains(index))
            {
                required_indices.emplace_back(index);
                added_indices.insert(index);
            }
        }
    }
    return required_indices;
}
}
#endif

