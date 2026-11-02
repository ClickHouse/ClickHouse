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
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/HashTable/Hash.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <array>
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

/// The best n keys of the sample must appear, on average, in at least this many of its granules. A key whose
/// rows come in a run appears in one or two granules of the sample, the best keys of spread values in several.
constexpr double min_spread_granules = 3;

/// A run that keeps no candidates for the next runs pays a fallback, which reads the whole input again, on every run.
/// Its pilot then also treats the best keys of the sample as runs when at least this many of the best n keys, and this
/// share of them, appear in a single sampled granule. Their values come in runs shorter than the gaps between the
/// sampled granules, which then hide other heavy keys from the sample. A few such keys are common in a sample that
/// shows the top, as the heaviest keys span many granules.
constexpr size_t min_single_granule_keys = 10;
constexpr double min_single_granule_keys_share = 0.25;

/// The candidate pass saves the aggregation of the rows of the other keys only, so the pilot declines when
/// the candidates hold more than this share of the rows of its sample.
constexpr double max_candidate_rows_share = 0.5;

/// The columns of the output of the pilot.
constexpr auto pilot_rank_column = "__top_k_pilot_rank";
constexpr auto pilot_rows_column = "__top_k_pilot_rows";
constexpr auto pilot_spread_column = "__top_k_pilot_spread";

/// The size of the sample of a pilot: its granules, set when its plan is built, and its rows, counted while it reads.
struct TopKPilotSampleSize
{
    size_t granules = 0;
    std::atomic<UInt64> rows = 0;
};

using TopKPilotSampleSizePtr = std::shared_ptr<TopKPilotSampleSize>;

/// The pilot reads every k-th granule, with k chosen for about this many sampled granules, and at most every
/// 32nd one.
constexpr size_t pilot_granules = 128;
constexpr size_t min_pilot_period = 32;

/// The pilot counts the granules of a key up to this many, which `min_spread_granules` never needs to exceed.
constexpr UInt64 max_spread_granules = 8;

/// The virtual columns that identify the granule of a row: its part and its offset in the part.
constexpr std::array<const char *, 2> granule_virtual_columns = {"_part_index", "_part_granule_offset"};

/// The column of the hash of the granule of every row, whose values the aggregation of the pilot counts for every key.
constexpr auto pilot_granule_hash_column = "__top_k_pilot_granule_hash";

/// Appends to every chunk the granule of every row, a hash of 32 bits of its part and of its offset in the part (see
/// `granule_virtual_columns`), so that the pilot counts the granules each key appears in, and counts the rows of the
/// sample. The state of the pilot holds the granules of every key, which values of 32 bits keep small. A collision
/// only lowers the spread of a key, so it never makes keys in runs look spread.
class TopKPilotGranuleTransform final : public ISimpleTransform
{
public:
    TopKPilotGranuleTransform(SharedHeader input_header, SharedHeader output_header, TopKPilotSampleSizePtr sample_size_)
        : ISimpleTransform(input_header, std::move(output_header), /*skip_empty_chunks_=*/true)
        , part_position(input_header->getPositionByName(granule_virtual_columns[0]))
        , offset_position(input_header->getPositionByName(granule_virtual_columns[1]))
        , sample_size(std::move(sample_size_))
    {
    }

    String getName() const override { return "TopKPilotGranuleTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        const size_t num_rows = chunk.getNumRows();
        sample_size->rows += num_rows;
        const auto & parts = assert_cast<const ColumnUInt64 &>(*chunk.getColumns()[part_position]).getData();
        const auto & offsets = assert_cast<const ColumnUInt64 &>(*chunk.getColumns()[offset_position]).getData();
        auto granules = ColumnUInt32::create(num_rows);
        auto & data = granules->getData();
        for (size_t i = 0; i < num_rows; ++i)
            data[i] = intHash32<0>((parts[i] << 32) ^ offsets[i]);
        chunk.addColumn(std::move(granules));
    }

private:
    const size_t part_position;
    const size_t offset_position;
    TopKPilotSampleSizePtr sample_size;
};

SharedHeader appendPilotGranuleHashColumn(const Block & header)
{
    Block result = header;
    result.insert({ColumnUInt32::create(), std::make_shared<DataTypeUInt32>(), pilot_granule_hash_column});
    return std::make_shared<const Block>(std::move(result));
}

class TopKPilotGranuleStep final : public ITransformingStep
{
public:
    TopKPilotGranuleStep(const SharedHeader & input_header, TopKPilotSampleSizePtr sample_size_)
        : ITransformingStep(input_header, appendPilotGranuleHashColumn(*input_header), row_preserving_traits)
        , sample_size(std::move(sample_size_))
    {
    }

    String getName() const override { return "TopKPilotGranule"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.addSimpleTransform([&](const SharedHeader & header)
            { return std::make_shared<TopKPilotGranuleTransform>(header, output_header, sample_size); });
    }

private:
    void updateOutputHeader() override { output_header = appendPilotGranuleHashColumn(*input_headers.front()); }

    TopKPilotSampleSizePtr sample_size;
};

/// The parameters of the pilot: the rows, the rank and the number of the sampled granules of every key of the sample
/// (see `TopKPilotGranuleTransform`). The conversion skips the keys of fewer than `min_candidate_rows` rows by the
/// `HAVING` prefilter on the `count` at position 0, which works on the buckets of two-level tables of the classic
/// aggregator. A sample is small, so its tables are two-level from the start, and the merge of the adaptive aggregator
/// would only add work.
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
        "uniqUpTo", {std::make_shared<DataTypeUInt32>()}, {pilot_granule_hash_column}, pilot_spread_column,
        {Field(UInt64{max_spread_granules})}));
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

/// The plan of the pilot, which records the size of its sample in `sample_size`.
QueryPlan makePilotPlan(const MultiPassTopKTemplate & templ, const TopKPilotSampleSizePtr & sample_size)
{
    /// The sample is a few granules spread over the table, of uneven sizes. Read tasks of one granule let
    /// every thread of the query read a part of it, and let the threads that finish early take the remaining
    /// granules. The pilot reads with all the threads, however few bytes the sample holds, so that they are
    /// ready for the pass after it, the candidate pass or the original aggregation: the executor spawns the
    /// threads for the tasks at the start of a query at once, but for later tasks one at a time.
    auto read = cloneRead(templ, fineTasksContext(templ.input->context));
    auto analysis = std::make_shared<ReadFromMergeTree::AnalysisResult>(*templ.input->read->getAnalyzedResult());
    keepEveryNthGranule(*analysis, std::max<size_t>(min_pilot_period, analysis->selected_marks / pilot_granules));
    sample_size->granules = analysis->selected_marks;
    read->setAnalyzedResult(std::move(analysis));
    /// A sample proves nothing about the granules it skips.
    read->disableQueryConditionCache();
    /// The expressions and the filters of the input pass the virtual columns of the granules through, as they do not
    /// read them (see `pilotCanIdentifyGranules`).
    chassert(pilotCanIdentifyGranules(*templ.input->read));
    for (const auto * column : granule_virtual_columns)
        read->addReadColumn(column);

    QueryPlan plan = makeInputPlan(templ, std::move(read));
    plan.addStep(std::make_unique<TopKPilotGranuleStep>(plan.getCurrentHeader(), sample_size));
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
        sample_size = std::make_shared<TopKPilotSampleSize>();
        return makePilotPlan(*templ, sample_size);
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
        size_t single_granule_keys = 0;
        for (size_t i = 0; i < top; ++i)
        {
            spread += static_cast<double>(spreads[best[i]]);
            single_granule_keys += spreads[best[i]] == 1;
        }
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
            = static_cast<double>(candidate_rows) / static_cast<double>(std::max<UInt64>(1, sample_size->rows.load()));

        LOG_DEBUG(log, "Pilot: {} keys in {} granules, rank {} of the {}-th best key, its best keys in {:.1f} granules on average and {} of them in one granule, {} candidates with {:.3f} of the rows",
            num_keys, sample_size->granules, nth_rank, top, spread, single_granule_keys, num_candidates, candidate_rows_share);

        /// The sample cannot show the best keys, but the n-th best rank of the sample bounds the values of the
        /// bounding key that may hold a group of the top.
        const bool may_bound = templ->input->bounding_key && nth_rank >= min_bounding_rank;
        if (nth_rank < min_pilot_rank)
        {
            if (may_bound)
                return {.next_mode = makeBoundingMode(templ, memo, nth_rank)};
            return {.decline = "the sample shows no clear top"};
        }
        const bool many_keys_in_runs = single_granule_keys >= min_single_granule_keys
            && static_cast<double>(single_granule_keys) >= min_single_granule_keys_share * static_cast<double>(top);
        if (spread < min_spread_granules || (many_keys_in_runs && !memo->cachesCandidates()))
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
    TopKPilotSampleSizePtr sample_size;
};

}

bool pilotCanIdentifyGranules(const ReadFromMergeTree & read)
{
    const auto & columns = read.getStorageMetadata()->getColumns();
    return std::ranges::none_of(granule_virtual_columns, [&](const char * name)
        { return columns.has(name) || std::ranges::contains(read.getAllColumnNames(), name); });
}

TopKModePtr makePilot(MultiPassTopKTemplatePtr templ, TopKRunMemoPtr memo)
{
    return std::make_unique<TopKPilot>(std::move(templ), std::move(memo));
}

}
