#pragma once

#include <config.h>

#if USE_PARQUET

#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

class ParquetFilterCondition
{
    struct ConditionElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            /// Can take any value.
            FUNCTION_UNKNOWN,
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        using ColumnPtr = IColumn::Ptr;
        using HashesForColumns = std::vector<std::vector<uint64_t>>;
        using KeyColumns = std::vector<std::size_t>;

        Function function;
        // each entry represents a list of hashes per column
        // suppose there are three columns with 2 rows each
        // hashes_per_column.size() == 3 and hashes_per_column[0].size() == 2
        HashesForColumns hashes_per_column;
        KeyColumns key_columns;
    };
};

}

#endif
