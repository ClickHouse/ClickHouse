#pragma once

#include <functional>
#include <optional>

#include <Columns/IColumn.h>
#include <Common/PODArray.h>

namespace DB
{

/// Where one input chunk's rows go, as decided by a selector: either the whole chunk to a single
/// output, or a per-row destination for sharding.
struct ChunkRouting
{
    /// When set, the whole chunk goes to this single output and `selector` is ignored. Lets a selector that
    /// already knows the destination (e.g. "route everything to the hot port") skip building and scanning a
    /// per-row selector entirely.
    std::optional<size_t> whole_chunk_output;

    /// Per-row destination: size num_rows, every value in [0, num_outputs). Used when `whole_chunk_output`
    /// is nullopt.
    IColumn::Selector selector;
};

/// Builds the routing for one input chunk; `columns` are the input chunk's columns.
using ChunkRoutingSelector = std::function<ChunkRouting(const Columns & columns)>;

}
