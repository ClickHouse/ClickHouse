#pragma once

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopKImpl.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <algorithm>

/// What the modes of a multi-pass top-K aggregation plan their passes with: their parameters, the copies of the input
/// they read, the ranges they read and the groups they output; and what the modes read the outputs of the passes with.

namespace DB::MultiPassTopKImpl
{

/// Whether the rank of the top is the `count()` of the rows of a group (see `MultiPassTopKTemplate::rows_index`).
bool rankIsRows(const MultiPassTopKTemplate & templ);

/// The column of the rank in the output of an aggregation by the aggregates of the template.
const String & rankColumnOf(const MultiPassTopKTemplate & templ);

/// The aggregate `function_name` with `parameters` of the columns `argument_names` of the types `argument_types`, which
/// outputs `column_name`.
AggregateDescription makeAggregate(
    const String & function_name, const DataTypes & argument_types, Names argument_names, String column_name, Array parameters = {});

/// The parameters of a pass with an aggregation of its own, the pilot or the candidate pass: those of the original
/// aggregation without the optimizations that the plan derived for it, and without the hash table statistics, whose
/// sizes from a pass would mislead the next run of the query.
Aggregator::Params paramsOfOwnPass(const AggregatingStep & aggregating);

/// The parameters of a pass that runs the original aggregation over some of the rows, the fallback or a restricted
/// pass: those of the original aggregation without the hash table statistics, as for `paramsOfOwnPass`. Its
/// optimizations for the top stay valid: the best groups of the rows include every group of them that the top takes.
Aggregator::Params paramsOfSubsetPass(const AggregatingStep & aggregating);

/// Whether every aggregate of `params` keeps its state in place, in a fixed size: no state needs a destructor or
/// allocates in the arena. The tables of a pass then grow only with their keys.
bool hasFixedSizeStates(const Aggregator::Params & params);

/// A copy of the read of the template for a pass, reading with the settings of `context`. The copy takes the
/// filters the ranges of the analysis were selected by, which the reader applies as well: the skip indexes that
/// it applies while reading and the conditions it records in the query condition cache.
std::unique_ptr<ReadFromMergeTree> cloneRead(const MultiPassTopKTemplate & templ, const ContextPtr & context);

/// The input of a pass over `read`, a copy of the read of the template: the read, and the expressions and filters of
/// the input over it. The copy may output other columns than the original read, or in another order, which the steps
/// over it find by name.
QueryPlan makeInputPlan(const MultiPassTopKTemplate & templ, std::unique_ptr<ReadFromMergeTree> read);

/// Keeps of the groups of `plan`, a pass that aggregates by the template, the `limit` of the best ranks: the mode reads
/// at most these. Nothing else bounds the groups that a pass outputs, as the aggregation selects the best groups of its
/// top only for some keys (see `Aggregator::Params::bucket_top_k`).
void keepBestGroups(QueryPlan & plan, const MultiPassTopKTemplate & templ, size_t limit);

/// Replaces the ranges of every part of `analysis` by `select(part)`, which may take the ranges of the part, drops the
/// parts left without ranges, and updates the totals. The exact ranges, whose rows the primary key counts without
/// reading them, no longer apply.
template <typename Select>
void selectRanges(ReadFromMergeTree::AnalysisResult & analysis, Select && select)
{
    RangesInDataParts kept;
    analysis.selected_ranges = 0;
    analysis.selected_marks = 0;
    analysis.selected_rows = 0;
    for (auto & part : analysis.parts_with_ranges)
    {
        part.ranges = select(part);
        if (part.ranges.empty())
            continue;
        part.exact_ranges.clear();
        analysis.selected_ranges += part.ranges.size();
        analysis.selected_marks += part.getMarksCount();
        analysis.selected_rows += part.getRowsCount();
        kept.push_back(std::move(part));
    }
    analysis.parts_with_ranges = std::move(kept);
    analysis.selected_parts = analysis.parts_with_ranges.size();
    analysis.has_exact_ranges = false;
    analysis.exact_ranges_excluded = false;
}

/// Keeps every `period`-th granule of the analysis, counted over all its parts.
void keepEveryNthGranule(ReadFromMergeTree::AnalysisResult & analysis, size_t period);

/// A copy of `analysis` whose parts split their ranges into up to `entries` entries each, which the analysis of the
/// primary key of a filter runs in parallel. `restrictRanges` takes the ranges that it selects back to the parts.
std::shared_ptr<ReadFromMergeTree::AnalysisResult> splitForAnalysis(const ReadFromMergeTree::AnalysisResult & analysis, size_t entries);

/// Keeps of the ranges of `analysis` those that `restriction` selects as well. A part may have several entries in
/// `restriction` (see `splitForAnalysis`).
void restrictRanges(ReadFromMergeTree::AnalysisResult & analysis, const ReadFromMergeTree::AnalysisResult & restriction);

/// The values of the `UInt64` column `name` of `block`, such as a rank or the rows of the groups.
const PaddedPODArray<UInt64> & countsOf(const Block & block, const String & name);

/// The n-th best of `ranks`, or 0 when there are fewer.
UInt64 nthBest(const PaddedPODArray<UInt64> & ranks, size_t n);

/// Keeps in `positions` at most `max_candidates` of them, those of the best `ranks`.
template <typename Ranks>
void keepBestCandidates(IColumn::Permutation & positions, const Ranks & ranks)
{
    if (positions.size() <= max_candidates)
        return;
    std::nth_element(positions.begin(), positions.begin() + max_candidates, positions.end(),
        [&](size_t lhs, size_t rhs) { return ranks[lhs] > ranks[rhs]; });
    positions.resize(max_candidates);
}

}
