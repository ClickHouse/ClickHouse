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
};

class SlidingIndexes
{
public:
    explicit SlidingIndexes(const WindowTransformParams & params_);

    SlidingIndex calculate(const Columns & materialized_columns, int64_t rows_count);

private:
    const WindowTransformParams & params;
    std::optional<Columns> last_partition_key;
};

}
