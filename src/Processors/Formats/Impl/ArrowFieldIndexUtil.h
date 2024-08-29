#pragma once

#include "config.h"

#if USE_PARQUET || USE_ORC

#include <unordered_map>
#include <Core/Block.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/NestedUtils.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <Common/Exception.h>
#include <parquet/metadata.h>


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

// This is only used for parquet now.
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
    /// For a parquet schema {x: int, y: {i: int, j: int}}, the return will be
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

    // For a parquet schema {x: {i: int, j: int}}, this should be populated as follows
    // clickhouse_index = 0, parquet_indexes = {0, 1}
    struct ClickHouseIndexToParquetIndex
    {
        std::size_t clickhouse_index;
        std::vector<int> parquet_indexes;
    };

    /// Only collect the required fields' indices. Eg. when just read a field of a struct,
    /// don't need to collect the whole indices in this struct.
    std::vector<ClickHouseIndexToParquetIndex> findRequiredIndices(
        const Block & header,
        const arrow::Schema & schema,
        const parquet::FileMetaData & file)
    {
        std::vector<ClickHouseIndexToParquetIndex> required_indices;
        std::unordered_set<int> added_indices;
        /// Flat all named fields' index information into a map.
        auto fields_indices = calculateFieldIndices(schema);
        for (size_t i = 0, n = header.columns(); i < n; ++i)
        {
            const auto & named_col = header.getByPosition(i);
            std::string col_name = named_col.name;
            if (ignore_case)
                boost::to_lower(col_name);
            findRequiredIndices(col_name, i, named_col.type, fields_indices, added_indices, required_indices, file);
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
        else if (field_type->id() == arrow::Type::LIST)
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
        else if (field_type->id() == arrow::Type::MAP)
        {
            const auto * map_type = static_cast<arrow::MapType *>(field_type.get());
            auto index_snapshot = current_start_index;
            current_start_index += countIndicesForType(map_type->key_type());
            calculateFieldIndices(*map_type->item_field(), field_name, current_start_index, result, name_prefix);
            index_info.first = index_snapshot;
        }
        else
        {
            current_start_index += countIndicesForType(field_type);
        }
        index_info.second = current_start_index - index_info.first;
    }

    void findRequiredIndices(
        const String & name,
        std::size_t header_index,
        DataTypePtr data_type,
        const std::unordered_map<std::string, std::pair<int, int>> & field_indices,
        std::unordered_set<int> & added_indices,
        std::vector<ClickHouseIndexToParquetIndex> & required_indices,
        const parquet::FileMetaData & file)
    {
        auto nested_type = removeNullable(data_type);
        if (const DB::DataTypeTuple * type_tuple = typeid_cast<const DB::DataTypeTuple *>(nested_type.get()))
        {
            if (type_tuple->haveExplicitNames())
            {
                auto field_names = type_tuple->getElementNames();
                auto field_types = type_tuple->getElements();
                for (size_t i = 0, n = field_names.size(); i < n; ++i)
                {
                    auto field_name = field_names[i];
                    if (ignore_case)
                        boost::to_lower(field_name);
                    const auto & field_type = field_types[i];
                    findRequiredIndices(Nested::concatenateName(name, field_name), header_index, field_type, field_indices, added_indices, required_indices, file);
                }
                return;
            }
        }
        else if (const auto * type_array = typeid_cast<const DB::DataTypeArray *>(nested_type.get()))
        {
            findRequiredIndices(name, header_index, type_array->getNestedType(), field_indices, added_indices, required_indices, file);
            return;
        }
        else if (const auto * type_map = typeid_cast<const DB::DataTypeMap *>(nested_type.get()))
        {
            findRequiredIndices(name, header_index, type_map->getKeyType(), field_indices, added_indices, required_indices, file);
            findRequiredIndices(name, header_index, type_map->getValueType(), field_indices, added_indices, required_indices, file);
            return;
        }
        auto it = field_indices.find(name);
        if (it == field_indices.end())
        {
            if (!allow_missing_columns)
                throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Not found field ({})", name);
        }
        else
        {
            ClickHouseIndexToParquetIndex index_mapping;
            index_mapping.clickhouse_index = header_index;
            for (int j = 0; j < it->second.second; ++j)
            {
                auto index = it->second.first + j;
                if (added_indices.insert(index).second)
                {
                    index_mapping.parquet_indexes.emplace_back(index);
                }
            }

            required_indices.emplace_back(index_mapping);
        }
    }
};

}

#endif
