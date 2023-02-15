#pragma once

#include <Columns/IColumn.h>

#include <Storages/MergeTree/MergeTreePartition.h>

namespace DB
{
// Need to keep the min max value of unique key column
struct UniqueMergeTreeWriteState
{
    MergeTreePartition partition;

    ColumnPtr key_column = nullptr;
    ColumnPtr version_column = nullptr;
    ColumnPtr delete_key_column = nullptr;

    ContextPtr context;

    std::vector<Field> max_key_values;
    std::vector<Field> min_key_values;
};
}
