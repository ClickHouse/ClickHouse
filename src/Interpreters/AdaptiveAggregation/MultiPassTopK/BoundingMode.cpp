#include <Interpreters/AdaptiveAggregation/MultiPassTopK/BoundingMode.h>

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/BoundingKey.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/KeySketch.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Passes.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/TopKCandidates.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationMultiPassTopKSketchPasses;
    extern const Event AdaptiveAggregationMultiPassTopKRestrictedPasses;
}

namespace DB::MultiPassTopKImpl
{

namespace
{

/// The first restricted pass takes the values of the bounding key with the most rows, this many for every group of
/// the top.
constexpr size_t first_restricted_values_per_top = 4;
/// The bounding mode pays off while its restricted passes aggregate at most this share of the rows. The next runs of
/// a query whose passes aggregated more run the original aggregation, as after a decline.
constexpr double max_restricted_rows_share = 0.5;

/// The rows of every value of the bounding key that a restricted pass aggregates, counted before its aggregation: the
/// pass outputs only its best groups (see `keepBestGroups`), whose rows miss those of the other groups.
class TopKValueRows
{
public:
    void add(const std::vector<std::pair<UInt64, UInt64>> & runs)
    {
        std::lock_guard lock(mutex);
        for (const auto & [value, count] : runs)
            rows[value] += count;
    }

    /// The rows of every value, once the pass finished.
    const std::unordered_map<UInt64, UInt64> & get() const { return rows; }

private:
    std::mutex mutex;
    std::unordered_map<UInt64, UInt64> rows;
};

using TopKValueRowsPtr = std::shared_ptr<TopKValueRows>;

constexpr auto rank_filter_column = "__top_k_rank_filter";
constexpr auto min_rank_column = "__top_k_min_rank";

/// Counts the rows of every value of the bounding key in its input (see `TopKValueRows`), and passes the input on.
class TopKValueRowsTransform final : public ISimpleTransform
{
public:
    TopKValueRowsTransform(SharedHeader header, const String & key, TopKValueRowsPtr value_rows_)
        : ISimpleTransform(header, header, /*skip_empty_chunks_=*/true)
        , key_position(header->getPositionByName(key))
        , value_rows(std::move(value_rows_))
    {
    }

    String getName() const override { return "TopKValueRowsTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        runs.clear();
        forEachKeyRun(chunk.getColumns()[key_position], [&](UInt64 value, UInt64 rows) { runs.emplace_back(value, rows); });
        value_rows->add(runs);
    }

private:
    const size_t key_position;
    const TopKValueRowsPtr value_rows;
    std::vector<std::pair<UInt64, UInt64>> runs;
};

class TopKValueRowsStep final : public ITransformingStep
{
public:
    TopKValueRowsStep(const SharedHeader & input_header, String key_, TopKValueRowsPtr value_rows_)
        : ITransformingStep(input_header, input_header, row_preserving_traits)
        , key(std::move(key_))
        , value_rows(std::move(value_rows_))
    {
    }

    String getName() const override { return "TopKValueRows"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.addSimpleTransform([&](const SharedHeader & header)
            { return std::make_shared<TopKValueRowsTransform>(header, key, value_rows); });
    }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    const String key;
    const TopKValueRowsPtr value_rows;
};

/// A copy of the read of the template that reads only the rows that `filter` keeps, which merges into `PREWHERE` (see
/// `findBoundingKey`). The reader would record the granules that the filter rejects as rejected by the conditions of
/// the query, so the copy does not use the query condition cache.
std::unique_ptr<ReadFromMergeTree> cloneReadWithKeyFilter(const MultiPassTopKTemplate & templ, TopKKeyFilter filter)
{
    auto read = cloneRead(templ, templ.input->context);
    read->disableQueryConditionCache();
    read->updatePrewhereInfo(addToPrewhere(
        read->getPrewhereInfo(), std::move(filter), *templ.input->bounding_key, *read->getOutputHeader(), templ.input->context));
    return read;
}

/// Keeps the groups of `plan`, an aggregation of the template, whose rank reaches `min_rank`.
void filterByMinRank(QueryPlan & plan, const MultiPassTopKTemplate & templ, UInt64 min_rank)
{
    const auto & header = plan.getCurrentHeader();
    ActionsDAG dag(header->getColumnsWithTypeAndName());
    const auto & rank = dag.findInOutputs(rankColumnOf(templ));
    const auto & bound = dag.addColumn(
        ColumnConst::create(ColumnUInt64::create(1, min_rank), 1), std::make_shared<DataTypeUInt64>(), min_rank_column);
    const auto & filter = dag.addFunction(FunctionFactory::instance().get("greaterOrEquals", templ.input->context), {&rank, &bound}, rank_filter_column);
    dag.getOutputs().push_back(&filter);
    plan.addStep(std::make_unique<FilterStep>(header, std::move(dag), rank_filter_column, /*remove_filter_column=*/true));
}

/// The ranges of the read of the template that may hold the rows of `set`, values of the bounding key. For a column of
/// the primary or the partition key, the analysis of the filter by the set selects their granules.
std::shared_ptr<ReadFromMergeTree::AnalysisResult> rangesOfValues(const MultiPassTopKTemplate & templ, const FutureSetPtr & set)
{
    const auto & key = *templ.input->bounding_key;
    const auto & analysis = *templ.input->read->getAnalyzedResult();
    auto ranges = std::make_shared<ReadFromMergeTree::AnalysisResult>(analysis);
    if (!key.prunes_granules)
        return ranges;

    /// The analysis of the original read enforced the settings that require an index, and the analysis of the filter
    /// only narrows its ranges.
    auto analysis_context = Context::createCopy(templ.input->context);
    analysis_context->resetSettingsToDefaultValue({"force_primary_key", "force_index_by_date", "force_data_skipping_indexes"});
    auto analysis_read = templ.input->read->cloneWithContext(analysis_context);
    analysis_read->setAnalyzedResult(splitForAnalysis(analysis, templ.input->read->getNumStreams()));
    analysis_read->disableQueryConditionCache();
    auto filter = makeInFilter(key, set, templ.input->context);
    analysis_read->addFilter(std::move(filter.dag), filter.column);
    analysis_read->SourceStepWithFilterBase::applyFilters();
    const auto restriction = analysis_read->selectRangesToRead();
    /// An analysis is unusable when it exceeds a row limit, and this one selects some of the ranges of the original
    /// analysis, which the plan required to be usable.
    chassert(restriction->isUsable());
    restrictRanges(*ranges, *restriction);
    return ranges;
}

/// The hot pass of the bounding mode: the original aggregation of the rows of the values of the hot fine cells, which the
/// first restricted pass did not take, for the groups of at least `min_rank`. `PREWHERE` filters the rows (see
/// `FunctionTopKHotFineCells`), and the streams filter the groups, so only those that the top may take come out. The hot
/// cells hold `hot_rows` rows.
QueryPlan makeHotFineCellsPlan(
    const MultiPassTopKTemplate & templ,
    std::shared_ptr<const TopKCellSet> hot_cells,
    std::shared_ptr<const std::unordered_set<UInt64>> excluded,
    UInt64 min_rank,
    UInt64 hot_rows)
{
    auto read = cloneReadWithKeyFilter(templ, makeHotFineCellsFilter(*templ.input->bounding_key, std::move(hot_cells), std::move(excluded)));
    QueryPlan plan = makeInputPlan(templ, std::move(read));
    /// When the streams hold fewer rows of the hot cells than make a table two-level, most of their tables stay small
    /// and single-level, and a single two-level table would make the merge convert all of them, which allocates the
    /// buckets of every table. Single-level tables merge in parallel by partitions instead. States that grow, as those
    /// of `uniqExact` do, keep the threshold of bytes, so that their tables may become two-level and spill as in the
    /// original aggregation.
    Aggregator::Params params = paramsOfSubsetPass(*templ.aggregating);
    if (hot_rows / std::max<size_t>(1, params.max_threads) < params.group_by_two_level_threshold)
    {
        params.group_by_two_level_threshold = 0;
        if (hasFixedSizeStates(params))
            params.group_by_two_level_threshold_bytes = 0;
    }
    plan.addStep(templ.aggregating->cloneWithParams(plan.getCurrentHeader(), std::move(params)));
    filterByMinRank(plan, templ, min_rank);
    /// Its groups compete for the places of the top with the groups of the first restricted pass.
    keepBestGroups(plan, templ, templ.top);
    return plan;
}

/// A restricted pass of the bounding mode: the original aggregation of the rows of `values` of the bounding key, whose
/// rows `value_rows` counts. `PREWHERE` filters the rows by the values, in the ranges that `rangesOfValues` selects.
QueryPlan makeRestrictedPlan(const MultiPassTopKTemplate & templ, ColumnPtr values, const TopKValueRowsPtr & value_rows)
{
    const auto & key = *templ.input->bounding_key;
    const auto set = makeSet(key, std::move(values));
    auto read = cloneReadWithKeyFilter(templ, makeInFilter(key, set, templ.input->context));
    read->setAnalyzedResult(rangesOfValues(templ, set));

    QueryPlan plan = makeInputPlan(templ, std::move(read));
    plan.addStep(std::make_unique<TopKValueRowsStep>(plan.getCurrentHeader(), key.name, value_rows));
    plan.addStep(templ.aggregating->cloneWithParams(plan.getCurrentHeader(), paramsOfSubsetPass(*templ.aggregating)));
    /// The top takes at most `top` groups of the pass, and the mode reads the n-th best rank of these (see
    /// `afterFirstRestricted`).
    keepBestGroups(plan, templ, templ.top);
    return plan;
}

/// The bounding mode (see `makeBoundingMode`).
class TopKBoundingMode final : public TopKMode
{
public:
    TopKBoundingMode(MultiPassTopKTemplatePtr templ_, TopKRunMemoPtr memo_, UInt64 min_key_rows_)
        : TopKMode(std::move(templ_), std::move(memo_))
        , min_key_rows(min_key_rows_)
    {
    }

    /// The sketch pass.
    QueryPlan firstPass() override
    {
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationMultiPassTopKSketchPasses);
        sketch = std::make_shared<TopKKeySketch>();
        return makeKeySketchPlan(*templ, sketch);
    }

    TopKNext afterPass(Block output) override
    {
        switch (pass)
        {
            case Pass::Sketch:
                return afterSketch();
            case Pass::FirstRestricted:
                return afterFirstRestricted(output);
            case Pass::HotFineCells:
                return afterHotPass(output);
        }
        UNREACHABLE();
    }

private:
    enum class Pass : uint8_t
    {
        Sketch,
        FirstRestricted,
        HotFineCells,
    };

    Pass pass = Pass::Sketch;

    /// `min_key_rows` is the n-th best rank of the sample of the pilot, or 0 without a pilot. The sketch pass
    /// summarizes the values of the bounding key of the `input_rows` rows of the input (see `TopKKeySketch`). The first
    /// restricted pass takes `first_values`, the candidates of the cells of the most rows, whose rows `first_rows`
    /// counts. The hot pass aggregates the other values of the fine cells that may hold a group of the top.
    /// `restricted_rows` counts the rows of the passes after the sketch.
    const UInt64 min_key_rows;
    TopKKeySketchPtr sketch;
    UInt64 input_rows = 0;
    std::unordered_set<UInt64> first_values;
    TopKValueRowsPtr first_rows;
    UInt64 restricted_rows = 0;

    /// Whether the mode pays off with restricted passes that aggregate `rows` rows (see `max_restricted_rows_share`).
    bool paysOff(UInt64 rows) const
    {
        return static_cast<double>(rows) <= max_restricted_rows_share * static_cast<double>(input_rows);
    }

    /// After the sketch pass: the first restricted pass takes the candidates of the cells of the most rows.
    TopKNext afterSketch()
    {
        input_rows = sketch->rows();
        first_values = sketch->candidatesOfHeaviestCells(first_restricted_values_per_top * templ->top);
        /// The first restricted pass aggregates at least the rows that the votes of its values show. If the mode does
        /// not pay off with these alone, that pass would repeat most of the original aggregation, so the run declines.
        if (!paysOff(sketch->minRowsOf(first_values)))
            return {.decline = "the heaviest values of the bounding key hold most rows"};
        auto values = templ->input->bounding_key->type->createColumn();
        for (const UInt64 value : first_values)
            insertKeyValue(*values, value);
        LOG_DEBUG(log, "Sketch of {}: {} input rows, the first pass takes {} values", templ->input->bounding_key->name, input_rows, values->size());
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationMultiPassTopKRestrictedPasses);
        first_rows = std::make_shared<TopKValueRows>();
        pass = Pass::FirstRestricted;
        return {.next_pass = makeRestrictedPlan(*templ, std::move(values), first_rows)};
    }

    /// After the first restricted pass: emits its groups, the best `top` groups of its values, which prove the top
    /// when no other value of the bounding key may hold more rows than their n-th best rank (see
    /// `TopKKeySketch::boundsOfOtherValues`). Otherwise the hot pass aggregates the other values of the fine cells
    /// that may hold more (see `TopKKeySketch::hotFineCells`).
    TopKNext afterFirstRestricted(const Block & result)
    {
        const UInt64 nth_rank = nthBest(countsOf(result, rankColumnOf(*templ)), templ->top);
        Chunks found;
        found.emplace_back(result.getColumns(), result.rows());

        const auto bounds = sketch->boundsOfOtherValues(first_values, first_rows->get());
        restricted_rows += bounds.rows_of_taken;
        const UInt64 bound = *std::ranges::max_element(bounds.cells);
        if (bound <= nth_rank)
        {
            LOG_DEBUG(log, "Certified {} values of the bounding key: rank {} of the {}-th best group, at most {} rows of another",
                first_values.size(), nth_rank, templ->top, bound);
            return finish(std::move(found));
        }

        /// The value of a group that outranks the n-th best group of the pass holds more rows than that rank, and at
        /// least the n-th best rank of the sample.
        const UInt64 min_rows = std::max(nth_rank + 1, min_key_rows);
        auto [hot_cells, hot_rows] = sketch->hotFineCells(bounds, min_rows);
        restricted_rows += hot_rows;
        LOG_DEBUG(log, "First pass over {} values of the bounding key: rank {} of the {}-th best group, up to {} rows of another, {} fine cells of at least {} rows with {} rows",
            first_values.size(), nth_rank, templ->top, bound, hot_cells->count(), min_rows, hot_rows);
        if (hot_cells->none())
            return finish(std::move(found));
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationMultiPassTopKRestrictedPasses);
        pass = Pass::HotFineCells;
        return {
            .result = std::move(found),
            .next_pass = makeHotFineCellsPlan(
                *templ, std::move(hot_cells), std::make_shared<const std::unordered_set<UInt64>>(first_values), nth_rank, hot_rows)};
    }

    /// After the hot pass: emits its groups, which its filter keeps to those that may outrank the n-th best group of
    /// the first restricted pass.
    TopKNext afterHotPass(const Block & output)
    {
        Chunks result;
        result.emplace_back(output.getColumns(), output.rows());
        return finish(std::move(result));
    }

    /// Ends the bounding mode with `result`. The next runs start with the sketch while the restricted passes aggregate
    /// a small share of the rows, or else run the original aggregation.
    TopKNext finish(Chunks result)
    {
        if (!paysOff(restricted_rows))
        {
            LOG_DEBUG(log, "The restricted passes aggregated {} of {} rows", restricted_rows, input_rows);
            memo->keepOriginal();
        }
        else if (!memo->startedFromMemo())
        {
            memo->keepBounding();
        }
        return {.result = std::move(result)};
    }
};

}

TopKModePtr makeBoundingMode(MultiPassTopKTemplatePtr templ, TopKRunMemoPtr memo, UInt64 min_key_rows)
{
    return std::make_unique<TopKBoundingMode>(std::move(templ), std::move(memo), min_key_rows);
}

}
