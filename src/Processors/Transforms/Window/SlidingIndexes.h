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

    /// Fills `order_by_peer_group_starts` with the peer group starts by the ORDER BY values whatever the frame type,
    /// only if `WindowTransformParams::separate_order_by_peer_groups` is set.
    SlidingIndex calculate(const Columns & materialized_columns, int64_t rows_count, std::vector<bool> & order_by_peer_group_starts);

private:
    const WindowTransformParams & params;
    const bool has_order_by_collation;
    std::optional<Columns> last_partition_key;
    std::optional<Columns> last_order_key;
    int64_t last_peer_group_index = -1;
    int64_t last_order_by_peer_group_index = -1;
};

}
