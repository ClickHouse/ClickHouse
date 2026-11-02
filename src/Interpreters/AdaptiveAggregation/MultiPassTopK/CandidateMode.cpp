#include <Interpreters/AdaptiveAggregation/MultiPassTopK/CandidateMode.h>

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Passes.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/TopKCandidates.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <numeric>

namespace ProfileEvents
{
    extern const Event AdaptiveAggregationMultiPassTopKCandidatePasses;
    extern const Event AdaptiveAggregationMultiPassTopKFallbackPasses;
}

namespace DB::MultiPassTopKImpl
{

namespace
{

/// The column of the rows that the candidate pass adds to an aggregation without `count()`.
constexpr auto candidate_rows_column = "__top_k_candidate_rows";

/// The column of the rows of a group in the output of the candidate pass.
String rowsColumnOf(const MultiPassTopKTemplate & templ)
{
    return templ.rows_index ? templ.aggregating->getParams().aggregates[*templ.rows_index].column_name : candidate_rows_column;
}

std::vector<size_t> keyPositions(const Block & header, const Names & keys)
{
    std::vector<size_t> positions;
    positions.reserve(keys.size());
    for (const auto & key : keys)
        positions.push_back(header.getPositionByName(key));
    return positions;
}

/// Keeps the rows whose keys fall into the hot cells of the fallback.
class TopKHotCellsTransform final : public ISimpleTransform
{
public:
    TopKHotCellsTransform(SharedHeader header, const Names & keys, std::shared_ptr<const TopKCellSet> hot_cells_)
        : ISimpleTransform(header, header, /*skip_empty_chunks_=*/true)
        , key_positions(keyPositions(*header, keys))
        , hot_cells(std::move(hot_cells_))
    {
    }

    String getName() const override { return "TopKHotCellsTransform"; }

protected:
    void transform(Chunk & chunk) override
    {
        const size_t num_rows = chunk.getNumRows();
        auto columns = chunk.detachColumns();

        ColumnRawPtrs keys;
        keys.reserve(key_positions.size());
        for (const size_t position : key_positions)
            keys.push_back(columns[position].get());
        TopKCandidates::hashKeys(keys, num_rows, hashes);

        filter.resize(num_rows);
        size_t kept = 0;
        for (size_t i = 0; i < num_rows; ++i)
        {
            const UInt8 keep = hot_cells->test(TopKCandidates::cell(hashes[i]));
            filter[i] = keep;
            kept += keep;
        }

        if (kept < num_rows)
            for (auto & column : columns)
                column = column->filter(filter, kept);
        chunk.setColumns(std::move(columns), kept);
    }

private:
    const std::vector<size_t> key_positions;
    const std::shared_ptr<const TopKCellSet> hot_cells;
    PaddedPODArray<UInt32> hashes;
    IColumn::Filter filter;
};

class TopKHotCellsStep final : public ITransformingStep
{
public:
    TopKHotCellsStep(const SharedHeader & input_header, Names keys_, std::shared_ptr<const TopKCellSet> hot_cells_)
        : ITransformingStep(input_header, input_header, filtering_traits)
        , keys(std::move(keys_))
        , hot_cells(std::move(hot_cells_))
    {
    }

    String getName() const override { return "TopKHotCells"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.addSimpleTransform([&](const SharedHeader & header)
            { return std::make_shared<TopKHotCellsTransform>(header, keys, hot_cells); });
    }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    const Names keys;
    const std::shared_ptr<const TopKCellSet> hot_cells;
};

/// The parameters of the candidate pass. The bound of a cell needs the rows of its candidates, which a `count()` of the
/// pass gives when the aggregation has none (see `rowsColumnOf`). Its tables hold the candidates only, a few thousand
/// keys at most, so they stay single-level and in memory. A conversion to two-level, which the memory of a thread can
/// trigger, would only split each of them into many tiny tables for the merge.
Aggregator::Params candidatePassParams(const MultiPassTopKTemplate & templ)
{
    Aggregator::Params params = paramsOfOwnPass(*templ.aggregating);
    if (!templ.rows_index)
    {
        params.aggregates.push_back(makeAggregate("count", {}, {}, candidate_rows_column));
        params.aggregates_size = params.aggregates.size();
    }
    params.group_by_two_level_threshold = 0;
    params.group_by_two_level_threshold_bytes = 0;
    params.max_bytes_before_external_group_by = 0;
    params.min_bytes_before_external_group_by = 0;
    return params;
}

/// The values that the rank counts when it counts distinct values (see `TopKDistinctRank`).
std::optional<TopKDistinctRank> distinctRankOf(const MultiPassTopKTemplate & templ)
{
    const auto & rank = templ.aggregating->getParams().aggregates[templ.rank_index];
    const String name = rank.function->getName();
    if (name == "uniqExact")
        return TopKDistinctRank{.values = rank.argument_names, .condition = {}};
    if (name != "uniqExactIf")
        return {};

    /// A condition of another type than `UInt8`, such as a `Nullable` one, stays among the values: a row then
    /// starts a run when its condition changes, which loosens the bound but keeps it.
    TopKDistinctRank distinct{.values = rank.argument_names, .condition = {}};
    if (WhichDataType(templ.aggregating->getInputHeaders().front()->getByName(distinct.values.back()).type).isUInt8())
    {
        distinct.condition = distinct.values.back();
        distinct.values.pop_back();
    }
    return distinct;
}

/// The candidates of the keys `keys`, whose first columns are the keys.
TopKCandidatesPtr makeCandidates(const MultiPassTopKTemplate & templ, const Block & keys)
{
    const size_t num_candidates = keys.rows();
    const Aggregator::Params params = candidatePassParams(templ);
    const size_t keys_size = params.keys_size;
    const Block mergeable = params.getHeader(*templ.aggregating->getInputHeaders().front(), /*final=*/false);
    MutableColumns seed = mergeable.cloneEmptyColumns();
    for (size_t i = 0; i < keys_size; ++i)
        seed[i]->insertRangeFrom(*keys.getByPosition(i).column, 0, num_candidates);
    /// `insertManyDefaults` of `ColumnAggregateFunction` creates empty states.
    for (size_t i = keys_size; i < seed.size(); ++i)
        seed[i]->insertManyDefaults(num_candidates);
    return std::make_shared<TopKCandidates>(
        std::make_shared<const Block>(mergeable.cloneWithColumns(std::move(seed))), keys_size, distinctRankOf(templ));
}

/// The candidate pass with `candidates`.
QueryPlan makeCandidatePlan(const MultiPassTopKTemplate & templ, const TopKCandidatesPtr & candidates)
{
    QueryPlan plan = makeInputPlan(templ, cloneRead(templ, templ.input->context));
    auto aggregating = templ.aggregating->cloneWithParams(plan.getCurrentHeader(), candidatePassParams(templ));
    aggregating->setTopKCandidates(candidates);
    plan.addStep(std::move(aggregating));
    return plan;
}

/// The fallback: the original aggregation of the rows whose keys fall into `hot_cells` (see `TopKCandidates`).
QueryPlan makeFallbackPlan(const MultiPassTopKTemplate & templ, std::shared_ptr<const TopKCellSet> hot_cells)
{
    QueryPlan plan = makeInputPlan(templ, cloneRead(templ, templ.input->context));
    plan.addStep(std::make_unique<TopKHotCellsStep>(plan.getCurrentHeader(), templ.aggregating->getParams().keys, std::move(hot_cells)));
    plan.addStep(templ.aggregating->cloneWithParams(plan.getCurrentHeader(), paramsOfSubsetPass(*templ.aggregating)));
    return plan;
}

/// The candidate mode (see `makeCandidateMode`).
class TopKCandidateMode final : public TopKMode
{
public:
    TopKCandidateMode(MultiPassTopKTemplatePtr templ_, TopKRunMemoPtr memo_, Block candidate_keys_)
        : TopKMode(std::move(templ_), std::move(memo_))
        , candidate_keys(std::move(candidate_keys_))
    {
    }

    QueryPlan firstPass() override
    {
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationMultiPassTopKCandidatePasses);
        candidates = makeCandidates(*templ, candidate_keys);
        return makeCandidatePlan(*templ, candidates);
    }

    TopKNext afterPass(Block output) override
    {
        switch (pass)
        {
            case Pass::Candidates:
                return afterCandidatePass(std::move(output));
            case Pass::Fallback:
                return afterFallback(std::move(output));
        }
        UNREACHABLE();
    }

private:
    enum class Pass : uint8_t
    {
        Candidates,
        Fallback,
    };

    /// What a run that learns from its fallback keeps until the fallback finishes (see `learnCandidates`): the proven
    /// candidates with their aggregates, and the least rank of a group of the fallback that becomes a candidate.
    struct Learning
    {
        Block proven_candidates;
        UInt64 min_rank = 0;
    };

    Pass pass = Pass::Candidates;
    /// The keys of the candidates, which a run that the candidates certify keeps for the next runs, and the candidates.
    Block candidate_keys;
    TopKCandidatesPtr candidates;
    std::optional<Learning> learning;

    /// After the candidate pass: emits the proven candidates, which are groups of the result (see
    /// `TopKCandidates::prove`). The candidates prove the top when no cell is hot; otherwise the fallback aggregates
    /// all the keys of the hot cells.
    TopKNext afterCandidatePass(Block result)
    {
        const size_t num_candidates = result.rows();

        /// The candidates include the best `top` keys of the sample by their rank, whose rows reach it.
        chassert(num_candidates >= templ->top);
        const UInt64 nth_rank = nthBest(countsOf(result, rankColumnOf(*templ)), templ->top);

        ColumnRawPtrs keys;
        for (size_t i = 0; i < templ->aggregating->getParams().keys_size; ++i)
            keys.push_back(result.getByPosition(i).column.get());
        auto proof = candidates->prove(keys, countsOf(result, rowsColumnOf(*templ)), nth_rank);

        Columns columns = result.getColumns();
        if (proof.num_proven < num_candidates)
            for (auto & column : columns)
                column = column->filter(proof.proven, proof.num_proven);
        Block proven_candidates = result.cloneWithColumns(columns);
        /// The output has no column of the rows that the candidate pass adds to an aggregation without `count()`.
        if (!templ->rows_index)
            columns.pop_back();
        TopKNext next;
        next.result.emplace_back(std::move(columns), proof.num_proven);

        if (proof.hot_cells->none())
        {
            LOG_DEBUG(log, "Certified {} candidates: rank {} of the {}-th best, at most {} of another key",
                num_candidates, nth_rank, templ->top, proof.bound);
            if (!memo->startedFromMemo() && memo->cachesCandidates())
                memo->keepCandidates(std::move(candidate_keys), /*learned=*/false);
            return next;
        }

        LOG_DEBUG(log, "Falling back for {} hot cells: rank {} of the {}-th best candidate, up to {} of another key",
            proof.hot_cells->count(), nth_rank, templ->top, proof.bound);
        ProfileEvents::increment(ProfileEvents::AdaptiveAggregationMultiPassTopKFallbackPasses);
        /// The fallback finds the keys that the candidates miss, and the next runs start with them (see
        /// `learnCandidates`). Where the run may not learn them, or a fallback completed the candidates already, the
        /// fallback would read the input once more on every run, which costs more than the candidates save. The next
        /// runs then run the original aggregation, as after a decline.
        if (memo->cachesCandidates() && !memo->startedFromLearned())
            learning = Learning{.proven_candidates = std::move(proven_candidates), .min_rank = nth_rank / candidate_rank_divisor};
        else
            memo->keepOriginal();
        pass = Pass::Fallback;
        next.next_pass = makeFallbackPlan(*templ, std::move(proof.hot_cells));
        return next;
    }

    /// After the fallback: emits its groups after the proven candidates.
    TopKNext afterFallback(Block fallback)
    {
        if (learning)
            learnCandidates(fallback);
        const size_t num_rows = fallback.rows();
        TopKNext next;
        next.result.emplace_back(fallback.getColumns(), num_rows);
        return next;
    }

    /// Keeps the candidates for the next runs (see `learning`): the proven ones, and the groups of `fallback` whose
    /// rank reaches a quarter of the n-th best candidate, as the pilot picks them. These hold the keys that the sample
    /// missed. Keeps at most `max_candidates` of them, those of the best ranks. Fewer than `top` of them mean that the
    /// input holds fewer heavy groups than the top takes, as after the data changed: the next runs then run the
    /// original aggregation, as after a decline.
    void learnCandidates(const Block & fallback)
    {
        const auto & fallback_ranks = countsOf(fallback, rankColumnOf(*templ));
        IColumn::Filter heavy(fallback.rows());
        size_t num_heavy = 0;
        for (size_t i = 0; i < fallback.rows(); ++i)
        {
            heavy[i] = fallback_ranks[i] >= learning->min_rank;
            num_heavy += heavy[i];
        }

        const Block & proven = learning->proven_candidates;
        const auto & proven_ranks = countsOf(proven, rankColumnOf(*templ));
        std::vector<UInt64> ranks(proven_ranks.begin(), proven_ranks.end());
        for (size_t i = 0; i < fallback.rows(); ++i)
            if (heavy[i])
                ranks.push_back(fallback_ranks[i]);

        Block keys;
        for (size_t i = 0; i < templ->aggregating->getParams().keys_size; ++i)
        {
            const auto & proven_key = proven.getByPosition(i);
            auto column = proven_key.column->cloneEmpty();
            column->insertRangeFrom(*proven_key.column, 0, proven.rows());
            column->insertRangeFrom(*fallback.getByPosition(i).column->filter(heavy, num_heavy), 0, num_heavy);
            keys.insert({std::move(column), proven_key.type, proven_key.name});
        }

        if (ranks.size() > max_candidates)
        {
            IColumn::Permutation best(ranks.size());
            std::iota(best.begin(), best.end(), 0);
            keepBestCandidates(best, ranks);
            for (auto & key : keys)
                key.column = key.column->permute(best, best.size());
        }
        LOG_DEBUG(log, "Learned {} candidates: {} proven and {} of the fallback", keys.rows(), proven.rows(), num_heavy);
        if (keys.rows() < templ->top)
            memo->keepOriginal();
        else
            memo->keepCandidates(std::move(keys), /*learned=*/true);
    }
};

}

TopKModePtr makeCandidateMode(MultiPassTopKTemplatePtr templ, TopKRunMemoPtr memo, Block candidate_keys)
{
    return std::make_unique<TopKCandidateMode>(std::move(templ), std::move(memo), std::move(candidate_keys));
}

}
