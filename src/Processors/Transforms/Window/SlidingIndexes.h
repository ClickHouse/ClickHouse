#pragma once

#include <Processors/Transforms/Window/WindowTransformParams.h>

#include <Columns/IColumn_fwd.h>

#include <Common/VectorWithMemoryTracking.h>

#include <cstdint>
#include <optional>

namespace DB
{

struct SlidingIndex
{
    VectorWithMemoryTracking<bool> partition_starts;
    VectorWithMemoryTracking<int64_t> peer_group_indexes;
    VectorWithMemoryTracking<bool> missing_keys;
};

class SlidingIndexes
{
public:
    explicit SlidingIndexes(const WindowTransformParams & params_);

    SlidingIndex calculate(const Columns & materialized_columns, int64_t rows_count);

private:
    const WindowTransformParams & params;
    std::optional<Columns> last_partition_key;
    std::optional<Columns> last_order_key;
    int64_t last_peer_group_index = -1;
};

}
