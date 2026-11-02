#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Pilot.h>

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/BoundingMode.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/CandidateMode.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Passes.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <atomic>
#include <numeric>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationMultiPassTopKPilotPasses;
}

namespace DB::MultiPassTopKImpl
{

namespace
{

/// The n-th best rank of the sample must reach this, or the sample ranks keys too coarsely for candidates.
constexpr UInt64 min_pilot_rank = 32;

/// The fewest sampled rows of a candidate of a pilot that does not decline, as the rank of a key never
/// exceeds its rows. The aggregation of the pilot skips the keys with fewer, which are most keys of a sample.
constexpr UInt64 min_candidate_rows = min_pilot_rank / candidate_rank_divisor;

/// The best n keys of the sample must appear, on average, in at least this many of its chunks, which the
/// pilot reads one granule each. A key whose rows come in a run appears in one or two granules of the sample,
/// the best keys of spread values in several.
constexpr double min_spread_chunks = 3;

/// The candidate pass saves the aggregation of the rows of the other keys only, so the pilot declines when
/// the candidates hold more than this share of the rows of its sample.
constexpr double max_candidate_rows_share = 0.5;

/// The columns of the output of the pilot.
constexpr auto pilot_rank_column = "__top_k_pilot_rank";
constexpr auto pilot_rows_column = "__top_k_pilot_rows";
constexpr auto pilot_spread_column = "__top_k_pilot_spread";

/// The number of the chunks and the rows of the sample of a pilot.
struct TopKPilotCounters
{
    std::atomic<UInt32> chunks = 0;
    std::atomic<UInt64> rows = 0;
};

using TopKPilotCountersPtr = std::shared_ptr<TopKPilotCounters>;

/// The pilot reads every k-th granule, with k chosen for about this many sampled granules, and at most every
/// 32nd one.
constexpr size_t pilot_granules = 128;
constexpr size_t min_pilot_period = 32;

/// The pilot counts the chunks of a key up to this many, which `min_spread_chunks` never needs to exceed.
constexpr UInt64 max_spread_chunks = 8;

constexpr auto pilot_chunk_column = "__top_k_pilot_chunk";

/// Appends to every chunk a column with a number of its own, so that the pilot counts the chunks each key
/// appears in, and counts the rows of the sample.
class TopKPilotChunkTransform final : public ISimpleTransform
{
public:
    TopKPilotChunkTransform(SharedHeader input_header, SharedHeader output_header, TopKPilotCountersPtr counters_)
        : ISimpleTransform(std::move(input_header), std::move(output_header), /*skip_empty_chunks_=*/true)
        , counters(std::move(counters_))
    {
    }

    String getName() const override { return "TopKPilotChunkTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        counters->rows += chunk.getNumRows();
        chunk.addColumn(ColumnUInt32::create(chunk.getNumRows(), counters->chunks.fetch_add(1)));
    }

private:
    TopKPilotCountersPtr counters;
};

SharedHeader appendPilotChunkColumn(const Block & header)
{
    Block result = header;
    result.insert({ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), pilot_chunk_column});
    return std::make_shared<const Block>(std::move(result));
}

class TopKPilotChunkStep final : public ITransformingStep
{
public:
    TopKPilotChunkStep(const SharedHeader & input_header, TopKPilotCountersPtr counters_)
        : ITransformingStep(input_header, appendPilotChunkColumn(*input_header), row_preserving_traits)
        , counters(std::move(counters_))
    {
    }

    String getName() const override { return "TopKPilotChunk"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.addSimpleTransform([&](const SharedHeader & header)
            { return std::make_shared<TopKPilotChunkTransform>(header, output_header, counters); });
    }

private:
    void updateOutputHeader() override { output_header = appendPilotChunkColumn(*input_headers.front()); }

    TopKPilotCountersPtr counters;
};

/// The parameters of the pilot: the rows, the rank and the number of the sampled chunks of every key of the sample (see
/// `TopKPilotChunkTransform`). The conversion skips the keys of fewer than `min_candidate_rows` rows by the `HAVING`
/// prefilter on the `count` at position 0, which works on the buckets of two-level tables of the classic aggregator. A
/// sample is small, so its tables are two-level from the start, and the merge of the adaptive aggregator would only add
/// work.
Aggregator::Params pilotParams(const MultiPassTopKTemplate & templ)
{
    Aggregator::Params params = paramsOfOwnPass(*templ.aggregating);
    AggregateDescriptions aggregates;
    aggregates.push_back(makeAggregate("count", {}, {}, pilot_rows_column));
    if (!rankIsRows(templ))
    {
        aggregates.push_back(params.aggregates[templ.rank_index]);
        aggregates.back().column_name = pilot_rank_column;
    }
    aggregates.push_back(makeAggregate(
        "uniqUpTo", {std::make_shared<DataTypeUInt32>()}, {pilot_chunk_column}, pilot_spread_column, {Field(UInt64{max_spread_chunks})}));
    params.aggregates = std::move(aggregates);
    params.aggregates_size = params.aggregates.size();
    params.enable_adaptive_aggregator = false;
    params.group_by_two_level_threshold = 1;
    params.group_by_two_level_threshold_bytes = 0;
    params.having_prefilter_op = Aggregator::Params::HavingPrefilterOp::GreaterOrEqual;
    params.having_prefilter_threshold = min_candidate_rows;
    params.having_prefilter_count_index = 0;
    return params;
}

/// A context that reads with tasks of one granule with all the threads: the threads that finish early take the remaining
/// granules, which the passes that read some granules only, or few rows of each, would otherwise leave to a few threads.
ContextPtr fineTasksContext(const ContextPtr & context)
{
    auto result = Context::createCopy(context);
    for (const auto * setting : {
             "merge_tree_min_rows_for_concurrent_read",
             "merge_tree_min_bytes_for_concurrent_read",
             "merge_tree_min_rows_for_concurrent_read_for_remote_filesystem",
             "merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem",
             "merge_tree_min_read_task_size"})
        result->setSetting(setting, Field(UInt64{1}));
    result->setSetting("merge_tree_min_bytes_per_read_stream", Field(UInt64{0}));
    return result;
}

/// The plan of the pilot, which counts its chunks and rows in `counters`.
QueryPlan makePilotPlan(const MultiPassTopKTemplate & templ, const TopKPilotCountersPtr & counters)
{
    /// The sample is a few granules spread over the table, of uneven sizes. Read tasks of one granule let
    /// every thread of the query read a part of it, let the threads that finish early take the remaining
    /// granules, and make every chunk of the sample one granule. The pilot reads with all the threads,
    /// however few bytes the sample holds, so that they are ready for the pass after it, the candidate pass
    /// or the original aggregation: the executor spawns the threads for the tasks at the start of a query at
    /// once, but for later tasks one at a time.
    auto read = cloneRead(templ, fineTasksContext(templ.input->context));
    auto analysis = std::make_shared<ReadFromMergeTree::AnalysisResult>(*templ.input->read->getAnalyzedResult());
    keepEveryNthGranule(*analysis, std::max<size_t>(min_pilot_period, analysis->selected_marks / pilot_granules));
    read->setAnalyzedResult(std::move(analysis));
    /// A sample proves nothing about the granules it skips.
    read->disableQueryConditionCache();

    QueryPlan plan = makeInputPlan(templ, std::move(read));
    plan.addStep(std::make_unique<TopKPilotChunkStep>(plan.getCurrentHeader(), counters));
    plan.addStep(templ.aggregating->cloneWithParams(plan.getCurrentHeader(), pilotParams(templ)));
    return plan;
}

/// The pilot (see `makePilot`).
class TopKPilot final : public TopKMode
{
public:
    using TopKMode::TopKMode;

    QueryPlan firstPass() override
    {
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationMultiPassTopKPilotPasses);
        counters = std::make_shared<TopKPilotCounters>();
        return makePilotPlan(*templ, counters);
    }

    /// Starts the candidate mode with the keys of the best ranks of the sample when they stand out and spread over the
    /// table. Otherwise starts the bounding mode where it may bound the top, or declines.
    TopKNext afterPass(Block sample) override
    {
        const size_t num_keys = sample.rows();
        const size_t top = templ->top;
        /// The aggregation of the pilot skips the keys of fewer than `min_candidate_rows` rows, so with fewer
        /// than `top` keys left the n-th best rank is below `min_pilot_rank`.
        if (num_keys < top)
            return {.decline = "the sample shows no clear top"};

        const auto & rows = countsOf(sample, pilot_rows_column);
        const auto & ranks = rankIsRows(*templ) ? rows : countsOf(sample, pilot_rank_column);
        const auto & spreads = countsOf(sample, pilot_spread_column);

        std::vector<size_t> best(num_keys);
        std::iota(best.begin(), best.end(), 0);
        std::nth_element(best.begin(), best.begin() + (top - 1), best.end(), [&](size_t lhs, size_t rhs) { return ranks[lhs] > ranks[rhs]; });
        const UInt64 nth_rank = ranks[best[top - 1]];

        double spread = 0;
        for (size_t i = 0; i < top; ++i)
            spread += static_cast<double>(spreads[best[i]]);
        spread /= static_cast<double>(top);

        /// The candidates are the keys whose rank reaches a quarter of the n-th best one, at most
        /// `max_candidates` of those with the best ranks. A rank that counts distinct values may be far below
        /// the rows of a key, whose rows then stay in the bound of its cell.
        IColumn::Permutation candidate_order;
        for (size_t i = 0; i < num_keys; ++i)
            if (ranks[i] >= nth_rank / candidate_rank_divisor)
                candidate_order.push_back(i);
        keepBestCandidates(candidate_order, ranks);
        UInt64 candidate_rows = 0;
        for (const size_t i : candidate_order)
            candidate_rows += rows[i];
        const size_t num_candidates = candidate_order.size();
        const double candidate_rows_share
            = static_cast<double>(candidate_rows) / static_cast<double>(std::max<UInt64>(1, counters->rows.load()));

        LOG_DEBUG(log, "Pilot: {} keys in {} chunks, rank {} of the {}-th best key, its best keys in {:.1f} chunks on average, {} candidates with {:.3f} of the rows",
            num_keys, counters->chunks.load(), nth_rank, top, spread, num_candidates, candidate_rows_share);

        /// The sample cannot show the best keys, but the n-th best rank of the sample bounds the values of the
        /// bounding key that may hold a group of the top.
        const bool may_bound = templ->input->bounding_key && nth_rank >= min_bounding_rank;
        if (nth_rank < min_pilot_rank)
        {
            if (may_bound)
                return {.next_mode = makeBoundingMode(templ, memo, nth_rank)};
            return {.decline = "the sample shows no clear top"};
        }
        if (spread < min_spread_chunks)
        {
            if (may_bound)
                return {.next_mode = makeBoundingMode(templ, memo, nth_rank)};
            return {.decline = "the best keys of the sample come in runs"};
        }
        if (candidate_rows_share > max_candidate_rows_share)
            return {.decline = "the candidates hold most rows of the sample"};

        Block keys;
        for (size_t i = 0; i < templ->aggregating->getParams().keys_size; ++i)
        {
            const auto & key = sample.getByPosition(i);
            keys.insert({key.column->permute(candidate_order, num_candidates), key.type, key.name});
        }
        return {.next_mode = makeCandidateMode(templ, memo, std::move(keys))};
    }

private:
    TopKPilotCountersPtr counters;
};

}

TopKModePtr makePilot(MultiPassTopKTemplatePtr templ, TopKRunMemoPtr memo)
{
    return std::make_unique<TopKPilot>(std::move(templ), std::move(memo));
}

}
