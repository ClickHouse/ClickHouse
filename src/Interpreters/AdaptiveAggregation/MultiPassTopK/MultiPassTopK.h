#pragma once

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopK_fwd.h>
#include <base/types.h>

#include <memory>
#include <optional>
#include <vector>

namespace DB
{

class ActionsDAG;
class AggregatingStep;
class IQueryPlanStep;
class QueryPipelineBuilder;
class ReadFromMergeTree;

/// The expression of a step that may lie between the read and the aggregation of a `MultiPassTopKInput`: an
/// `ExpressionStep` or a `FilterStep`. Null for another step.
const ActionsDAG * expressionOfInputStep(const IQueryPlanStep & step);

/// The input of a multi-pass top-K aggregation of `aggregating`, which takes the `top` best groups by its aggregate at
/// `rank_index`, over `read` and `input_steps`, the expressions and filters between them, from the bottom up. The
/// caller checks that reading the input again gives the same rows, and gives the read the analysis of its ranges. Null
/// when the query takes more groups than the candidates may hold, when the read selects too few granules for the
/// aggregation to pay off, when the pilot cannot identify the granules of the read, when the limits of the query would
/// not hold for the passes, or when a copy of the read would output other columns.
MultiPassTopKInputPtr makeMultiPassTopKInput(
    const AggregatingStep & aggregating,
    const ReadFromMergeTree & read,
    const std::vector<const IQueryPlanStep *> & input_steps,
    size_t top,
    size_t rank_index);

/// The position of the aggregate that the top ranks by.
size_t rankIndexOf(const MultiPassTopKInput & input);

/// The key of the aggregation whose values bound the ranks of the groups in the bounding mode, if the input has one.
std::optional<String> boundingKeyOf(const MultiPassTopKInput & input);

/// The multi-pass top-K aggregation of `GROUP BY ... ORDER BY <rank> DESC LIMIT n` over a `MergeTree` table, where the
/// rank is a lone `count`, `uniqExact` or `uniqExactIf`: an aggregate that never exceeds the rows of its group. The
/// pipeline of the original aggregation stays the path of the queries it declines. Otherwise passes of its own run,
/// each over the whole input: a pilot over a sample, and then the passes of the mode that the pilot chooses, one of
/// two. Both modes aggregate exactly the rows that may hold a group of the top, and prove that the other rows hold
/// none.
///
/// The pilot aggregates a sample of every k-th granule exactly, by the keys, into the rank, the rows and the number of
/// the sampled granules of every key. The keys whose sampled ranks may place them among the best n are the candidates,
/// with which the candidate mode runs. When the best keys of the sample appear in few of its granules, the values of
/// the keys come in runs rather than spread over the table (as for a column of the sorting key), so the sample may miss
/// the heaviest ones. Then, or when the sample shows no clear top, the bounding mode runs where it applies, and the
/// original aggregation otherwise.
///
/// The candidate mode:
/// 1. The candidate pass aggregates the rows of the candidates exactly, and counts the rows of every key in hash cells
///    (see `TopKCandidates`). It is certified when the n-th best candidate ranks at least as high as the most rows
///    that the other keys of a cell hold, or for `uniqExact` the most runs of repeated rows: no other key can then
///    outrank the n best candidates.
/// 2. Otherwise the fallback aggregates every key of the cells whose other keys may outrank the n-th best candidate,
///    exactly, over all the rows of those cells. The candidates of the other cells keep the values of the pass.
///
/// The bounding mode serves the queries whose sample cannot show the best keys: their values come in runs, or the
/// sample holds too few of their rows. It needs several keys, one of them an integer column of the table whose values
/// each hold all the rows of their groups (see `boundingKeyOf`):
/// 1. The sketch pass summarizes the rows of the values of that key in hash cells, with the value that holds most rows
///    of each cell.
/// 2. The first restricted pass aggregates the values of the cells of the most rows, which `PREWHERE` and the primary
///    key analysis select, and finds the n-th best rank among their groups. It is exact when the other values of no
///    cell hold more rows.
/// 3. Otherwise the hot pass aggregates the other values of the finer cells that may hold more.
///
/// The output holds the groups the top may take with their exact aggregates; the `ORDER BY` and `LIMIT` above select
/// the top among them.
///
/// A repeated query, identified by the key of the hash table statistics of its aggregation, starts from what an
/// earlier run found instead of a pilot. While `adaptive_aggregator_top_k_cache_candidates` allows, a query whose
/// candidates proved the top runs its candidate pass with them, and a query whose fallback found the keys that its
/// candidates missed runs it with both. A query of the bounding mode starts with its sketch pass. A query whose pilot
/// declined runs the original aggregation for a number of runs, as does a query where the strategy cost more than it
/// saved: its candidates needed the fallback and could not learn from it, or the passes over some values of its
/// bounding key aggregated most rows. Every run proves its own result, so what an earlier run found never changes the
/// result.
///
/// Adds the passes to `pipeline`, the pipeline of the original aggregation. `aggregating` is a copy of its step, as
/// the plan left it, which the passes clone with parameters of their own.
void addMultiPassTopK(QueryPipelineBuilder & pipeline, std::unique_ptr<AggregatingStep> aggregating, MultiPassTopKInputPtr input);

}
