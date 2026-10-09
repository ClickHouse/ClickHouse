#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Processors/Transforms/Window/WindowTransformParams.h>

#include <cstdint>
#include <optional>
#include <vector>

namespace DB
{

struct SlidingIndex
{
    VectorWithMemoryTracking<bool> partition_starts;
    VectorWithMemoryTracking<bool> peer_group_starts;
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
};

}
