#pragma once

#include <Core/ColumnsWithTypeAndName.h>
#include <Common/VectorWithMemoryTracking.h>

#include <functional>
#include <optional>

namespace DB
{

/// How a candidate batch is priced: the exact bytes one call carrying `[start, start + length)` of
/// the arguments puts in guest memory. An ABI that builds its own buffer passes its layout pass.
using WasmBatchMeasurer = std::function<size_t(
    const ColumnsWithTypeAndName &, size_t, size_t, const VectorWithMemoryTracking<size_t> &)>;

/// How many rows the call starting at `start_idx` should carry, out of `remaining`.
///
/// The cost of a batch is monotone in its row count - adding a row can only grow the payload,
/// and can only grow a per-batch dictionary - so "the rows that fit the budget" is a prefix and
/// can be bracketed. Each probe measures a candidate exactly, keeps the largest candidate known
/// to fit and the smallest known to overflow, and picks the next candidate inside that bracket,
/// so the bracket shrinks on every step and the walk ends on the real boundary rather than on
/// the first prefix that looked full enough.
///
/// The next candidate follows the marginal cost of a row, taken as the slope between the last
/// two measurements, not the average bytes per row of the candidate. The average carries the
/// batch-wide part of the payload - framing, a `LowCardinality` dictionary, `Dynamic` and
/// `Variant` structure prefixes, and any single wide row already in the prefix - which is paid
/// once and does not grow with the rows added next. Dividing by it prices every further row at
/// the cost of the whole prefix, so a block whose first row is far wider than the rest would be
/// handed to the guest one row per call while hundreds of its rows still fit.
///
/// Every candidate is bounded by rows already measured: the walk starts at one row and grows by
/// a bounded factor per probe. Nothing is carried over from a previous batch or block, because a
/// row count only means something for rows of a known width - a count fitted by narrow rows
/// would have the next batch materialize that many wide rows before any measurement justified
/// it, recreating the oversized call the split exists to avoid.
///
/// `memory_ceiling` is the most the guest memory can grow to, empty when nothing bounds it.
size_t chooseWasmBatchRows(
    const ColumnsWithTypeAndName & arguments,
    size_t start_idx,
    size_t remaining,
    size_t budget,
    std::optional<size_t> memory_ceiling,
    const WasmBatchMeasurer & measure);

}
