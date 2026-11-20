#pragma once

#include <Core/Names.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <base/types.h>

#include <memory>
#include <optional>

namespace DB
{

/// Reorders the PREWHERE steps of JOIN runtime filters (`__applyFilter`) by measurements.
///
/// The planned order among runtime filters is arbitrary, and a weak filter that runs first probes every row. The planner can't
/// estimate a filter well: its selectivity depends on the build side's own filters, which correlate with the join key. So the
/// reader measures each filter's cost and benefit and orders the filters adaptively.
///
/// Where: `MergeTreeWhereOptimizer` gives runtime filters PREWHERE steps of their own, after the ordinary conditions of their
/// rank.
/// - MergeTree finds the last range of adjacent runtime filter steps once per source and orders it per read task, when the
///   task's readers chain is built.
/// - Parquet finds and orders it per file, over the PREWHERE conjuncts.
///
/// Rule: adjacent steps swap, as in a bounded bubble sort, when the later one is clearly better by what was measured for both.
/// - Measurements: selectivity, rejected rows (rows in blocks rejected whole count extra: later steps skip reading them), and
///   probe time per row.
/// - If both steps have a probe time: the one with the lower cost per rejected row goes first (5% margin).
/// - Otherwise: the one that rejects more goes first (10% margin). That move is blind to cost, so an expensive filter (a bloom
///   filter or a large exact set) may not jump ahead of a cheap-to-probe one (a fixed hash table or a small exact set) whose
///   probe time is known.
/// - Probe time is measured on large blocks only. A probe call has a fixed cost (about 10 µs), so on the small blocks that
///   reach a step behind a selective one, the call, not the filter, sets the time per row. In practice only the step that runs
///   first gets large blocks, so often only one step of a pair has a probe time.
/// - The margins keep measurement noise from flipping the order.
///
/// Why results can't change: runtime filters only drop rows the join drops anyway, so their order doesn't change the result.
/// A move only changes which rows each step sees, so only a step that does nothing but look up a stored column moves:
///
///     SELECT ... FROM t JOIN A ON t.a = A.k JOIN B ON t.b = B.k
///     PREWHERE steps: [__applyFilter(A, a)] [__applyFilter(B, b)]      the range: adjacent steps that may swap
///
/// - The key, the probe side of the join condition, is a column, a safe cast of one or a tuple of those, so the lookup can't
///   throw. A computed key such as `intDiv(100, d)` could divide by a zero the filter ahead removes.
/// - MergeTree: each step reads exactly its own key columns, and the part stores them as is. So a moved step neither needs a
///   column another step reads (a key `a + b`) nor one the reader fills from others (`ALTER ADD COLUMN b DEFAULT a * 2`).
/// - MergeTree: no reorder with predicate statistics, which are counted per step position.
///
/// A step after the range may see other rows, as a filter that passes most rows it sees is switched off for some blocks and a
/// reorder changes what each filter sees. That needs no guard: the switch-off already does the same in the planned order.

class ActionsDAG;
struct IRuntimeFilterLookup;
class RuntimeFilter;
using RuntimeFilterConstPtr = std::shared_ptr<const RuntimeFilter>;

/// The PREWHERE steps (or conjuncts) that may run in any order among themselves: adjacent, each holding only join runtime
/// filters that can run on rows the others would remove. Only the last such range counts, and only with at least two steps;
/// the steps outside it keep their place.
struct ReorderableRuntimeFilterSteps
{
    size_t first_step = 0;
    /// The ids of each step's filters, so the measurements can be looked up per task; empty if there are no such steps.
    VectorWithMemoryTracking<Names> filter_ids_per_step;
    /// The columns each step looks up, all it may read.
    VectorWithMemoryTracking<NameSet> key_columns_per_step;
};

/// The filter ids of one step or conjunct and the columns their keys look up; no ids if it is not only such lookups.
struct RuntimeFilterKeys
{
    Names filter_ids;
    NameSet key_columns;
};

/// What the runtime filters of one PREWHERE step have shown so far in the query.
struct RuntimeFilterStepMeasurement
{
    /// Fraction of the checked rows that pass, set once trusted; 1 for a filter whose key set was dropped.
    std::optional<Float64> selectivity;
    /// Probe time per row, measured on large blocks only; set once measured on enough of their rows.
    std::optional<Float64> probe_nanoseconds_per_row;
    /// Share of the checked rows in blocks rejected whole: over the large blocks once `probe_nanoseconds_per_row` is set, else
    /// over all.
    Float64 rows_in_rejected_blocks_ratio = 0;
    bool cheap_to_probe = false;
};

/// Measures one step from its filters; a null filter (not registered yet) leaves the step unmeasured.
RuntimeFilterStepMeasurement measureRuntimeFilterStep(const VectorWithMemoryTracking<RuntimeFilterConstPtr> & filters);

/// The order in which to run adjacent PREWHERE steps of runtime filters, as indices into `steps`, planned order first.
PrewhereStepOrder orderRuntimeFilterSteps(const VectorWithMemoryTracking<RuntimeFilterStepMeasurement> & steps);

/// `orderRuntimeFilterSteps` of the steps whose filters `lookup` holds under `filter_ids_per_step`; empty if it is the planned order.
PrewhereStepOrder
orderRuntimeFilterStepsByMeasurement(const IRuntimeFilterLookup & lookup, const VectorWithMemoryTracking<Names> & filter_ids_per_step);

/// A copy of the PREWHERE DAG whose `ReorderableRuntimeFilterSteps` of conjuncts run in the order `orderRuntimeFilterSteps` picks
/// from the filters' measurements; nullopt if the order stays.
std::optional<ActionsDAG> reorderRuntimeFilterConjuncts(
    const ActionsDAG & dag, const String & filter_column_name, const IRuntimeFilterLookup & lookup);

/// The `ReorderableRuntimeFilterSteps` of `prewhere_actions`.
ReorderableRuntimeFilterSteps findReorderableRuntimeFilterSteps(const PrewhereExprInfo & prewhere_actions);

}
