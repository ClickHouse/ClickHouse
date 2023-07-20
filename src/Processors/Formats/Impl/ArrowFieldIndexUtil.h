#pragma once
#include "config.h"
#if USE_PARQUET || USE_ORC
#include <unordered_map>
#include <Core/Block.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/NestedUtils.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/Exception.h>
namespace arrow
{
class Schema;
class DataType;
class Field;
}
namespace DB
{
namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
}

class ArrowFieldIndexUtil
{
public:
    explicit ArrowFieldIndexUtil(bool ignore_case_, bool allow_missing_columns_)
        : ignore_case(ignore_case_)
        , allow_missing_columns(allow_missing_columns_)
    {
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
    std::unordered_map<std::string, std::pair<int, int>>
        calculateFieldIndices(const arrow::Schema & schema)
    {
        std::unordered_map<std::string, std::pair<int, int>> result;
        int index_start = 0;
        for (int i = 0; i < schema.num_fields(); ++i)
        {
            const auto & field = schema.field(i);
            calculateFieldIndices(*field, field->name(), index_start, result);
        }
        return result;
    }

    /// Only collect the required fields' indices. Eg. when just read a field of a struct,
    /// don't need to collect the whole indices in this struct.
    std::vector<int> findRequiredIndices(const Block & header, const arrow::Schema & schema)
    {
        std::vector<int> required_indices;
        std::unordered_set<int> added_indices;
        /// Flat all named fields' index information into a map.
        auto fields_indices = calculateFieldIndices(schema);
        for (size_t i = 0; i < header.columns(); ++i)
        {
            const auto & named_col = header.getByPosition(i);
            std::string col_name = named_col.name;
            if (ignore_case)
                boost::to_lower(col_name);
            /// Since all named fields are flatten into a map, we should find the column by name
            /// in this map.
            auto it = fields_indices.find(col_name);

            if (it == fields_indices.end())
            {
                if (!allow_missing_columns)
                    throw Exception(
                        ErrorCodes::THERE_IS_NO_COLUMN, "Not found field ({}) in the following Arrow schema:\n{}\n", named_col.name, schema.ToString());
                else
                    continue;
            }
            for (int j = 0; j < it->second.second; ++j)
            {
                auto index = it->second.first + j;
                if (added_indices.insert(index).second)
                    required_indices.emplace_back(index);
            }
        }
        return required_indices;
    }

    /// Count the number of indices for types.
    size_t countIndicesForType(std::shared_ptr<arrow::DataType> type)
    {
        if (type->id() == arrow::Type::LIST)
        {
            return countIndicesForType(static_cast<arrow::ListType *>(type.get())->value_type());
        }

        if (type->id() == arrow::Type::STRUCT)
        {
            int indices = 0;
            auto * struct_type = static_cast<arrow::StructType *>(type.get());
            for (int i = 0; i != struct_type->num_fields(); ++i)
                indices += countIndicesForType(struct_type->field(i)->type());
            return indices;
        }

        if (type->id() == arrow::Type::MAP)
        {
            auto * map_type = static_cast<arrow::MapType *>(type.get());
            return countIndicesForType(map_type->key_type()) + countIndicesForType(map_type->item_type()) ;
        }

        return 1;
    }

private:
    bool ignore_case;
    bool allow_missing_columns;
    void calculateFieldIndices(const arrow::Field & field,
        std::string field_name,
        int & current_start_index,
        std::unordered_map<std::string, std::pair<int, int>> & result, const std::string & name_prefix = "")
    {
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
            auto * struct_type = static_cast<arrow::StructType *>(field_type.get());
            for (int i = 0, n = struct_type->num_fields(); i < n; ++i)
            {
                const auto & sub_field = struct_type->field(i);
                calculateFieldIndices(*sub_field, sub_field->name(), current_start_index, result, full_path_name);
            }
        }
        else if (
            field_type->id() == arrow::Type::LIST
            && static_cast<arrow::ListType *>(field_type.get())->value_type()->id() == arrow::Type::STRUCT)
        {
            // It is a nested table.
            const auto * list_type = static_cast<arrow::ListType *>(field_type.get());
            const auto value_field = list_type->value_field();
            auto index_snapshot = current_start_index;
            calculateFieldIndices(*value_field, field_name, current_start_index, result, name_prefix);
            // The nested struct field has the same name as this list field.
            // rewrite it back to the original value.
            index_info.first = index_snapshot;
        }
        else
        {
            current_start_index += countIndicesForType(field_type);
        }
        index_info.second = current_start_index - index_info.first;
    }
};
}
#endif
