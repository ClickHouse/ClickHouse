#pragma once

#include <DataTypes/DataTypeString.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/Arena.h>

namespace DB
{
struct PrimaryKeysEncoder
{
    static ColumnPtr encode(const ColumnRawPtrs & columns, size_t size, size_t keys_size)
    {
        auto res = DataTypeString{}.createColumn();
        for (size_t i = 0; i < size; ++i)
        {
            Arena pool;
            auto encoded_value = serializeKeysToPoolContiguous(i, keys_size, columns, pool);
            res->insertData(encoded_value.data, encoded_value.size);
        }
        return res;
    }

    static void encode(const ColumnRawPtrs & columns, size_t size, size_t keys_size, MutableColumnPtr & res_column)
    {
        for (size_t i = 0; i < size; ++i)
        {
            Arena pool;
            auto encoded_value = serializeKeysToPoolContiguous(i, keys_size, columns, pool);
            res_column->insertData(encoded_value.data, encoded_value.size);
        }
    }
};
}
