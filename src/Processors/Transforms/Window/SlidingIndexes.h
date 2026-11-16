#pragma once

#include <Columns/IColumn_fwd.h>
#include <Processors/Transforms/Window/WindowTransformParams.h>

#include <cstdint>
#include <optional>
#include <vector>

namespace DB
{

struct SlidingIndex
{
    std::vector<bool> partition_starts;
    std::vector<int64_t> peer_group_indexes;
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
