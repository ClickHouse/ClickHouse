#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Passes.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/assert_cast.h>

#include <algorithm>
#include <unordered_map>

namespace DB::MultiPassTopKImpl
{

namespace
{

MarkRanges intersectRanges(MarkRanges lhs, MarkRanges rhs)
{
    const auto by_begin = [](const MarkRange & a, const MarkRange & b) { return a.begin < b.begin; };
    std::ranges::sort(lhs, by_begin);
    std::ranges::sort(rhs, by_begin);
    MarkRanges result;
    size_t i = 0;
    size_t j = 0;
    while (i < lhs.size() && j < rhs.size())
    {
        const size_t begin = std::max(lhs[i].begin, rhs[j].begin);
        const size_t end = std::min(lhs[i].end, rhs[j].end);
        if (begin < end)
        {
            if (!result.empty() && result.back().end == begin)
                result.back().end = end;
            else
                result.emplace_back(begin, end);
        }
        if (lhs[i].end < rhs[j].end)
            ++i;
        else
            ++j;
    }
    return result;
}

/// The analysis of the primary key runs the entries of `parts_with_ranges` in parallel, so the ranges of every part are
/// split into entries of at least `min_marks_per_entry` marks, up to `entries` of them, which lets the analysis of a few
/// large parts use as many threads. An entry searches its own ranges only, so the entries find the ranges of the part.
constexpr size_t min_marks_per_entry = 512;

const ITransformingStep::Traits best_groups_traits{
    .data_stream_traits = {.returns_single_stream = true, .preserves_number_of_streams = false, .preserves_sorting = false},
    .transform_traits = {.preserves_number_of_rows = false}};

/// Keeps of its input the `limit` groups of the best ranks, the values of `rank_column`, in one stream (see
/// `keepBestGroups`).
class TopKBestGroupsStep final : public ITransformingStep
{
public:
    TopKBestGroupsStep(const SharedHeader & input_header, const String & rank_column, size_t limit_, SortingStep::Settings sort_settings_)
        : ITransformingStep(input_header, input_header, best_groups_traits)
        , limit(limit_)
        , sort_settings(std::move(sort_settings_))
    {
        best_first.emplace_back(rank_column, /*direction_=*/-1);
    }

    String getName() const override { return "TopKBestGroups"; }

    /// One stream sorts all the groups of the pass. It keeps at most `limit` rows of every block, a few thousand at
    /// most, which costs little. A sort of every stream would add two processors for every stream to the running
    /// pipeline, which costs more than the sorting itself.
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        pipeline.resize(1);
        SortingStep::fullSortStreams(pipeline, sort_settings, best_first, limit);
    }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }

    SortDescription best_first;
    const size_t limit;
    const SortingStep::Settings sort_settings;
};

}

bool rankIsRows(const MultiPassTopKTemplate & templ)
{
    return templ.rows_index == templ.rank_index;
}

const String & rankColumnOf(const MultiPassTopKTemplate & templ)
{
    return templ.aggregating->getParams().aggregates[templ.rank_index].column_name;
}

AggregateDescription makeAggregate(
    const String & function_name, const DataTypes & argument_types, Names argument_names, String column_name, Array parameters)
{
    AggregateFunctionProperties properties;
    AggregateDescription description;
    description.function = AggregateFunctionFactory::instance().get(function_name, NullsAction::EMPTY, argument_types, parameters, properties);
    description.parameters = std::move(parameters);
    description.argument_names = std::move(argument_names);
    description.column_name = std::move(column_name);
    return description;
}

Aggregator::Params paramsOfOwnPass(const AggregatingStep & aggregating)
{
    Aggregator::Params params = aggregating.getParams();
    params.bucket_top_k = 0;
    params.threshold_top_k.reset();
    params.top_k.reset();
    params.having_prefilter_op = Aggregator::Params::HavingPrefilterOp::Disabled;
    params.stats_collecting_params.disable();
    return params;
}

Aggregator::Params paramsOfSubsetPass(const AggregatingStep & aggregating)
{
    Aggregator::Params params = aggregating.getParams();
    params.stats_collecting_params.disable();
    return params;
}

bool hasFixedSizeStates(const Aggregator::Params & params)
{
    return std::ranges::all_of(params.aggregates, [](const AggregateDescription & aggregate)
        { return aggregate.function->hasTrivialDestructor() && !aggregate.function->allocatesMemoryInArena(); });
}

std::unique_ptr<ReadFromMergeTree> cloneRead(const MultiPassTopKTemplate & templ, const ContextPtr & context)
{
    auto read = templ.input->read->cloneWithContext(context);
    read->adoptFiltersFrom(*templ.input->read);
    return read;
}

QueryPlan makeInputPlan(const MultiPassTopKTemplate & templ, std::unique_ptr<ReadFromMergeTree> read)
{
    QueryPlan plan;
    plan.addStep(std::move(read));
    /// A `PREWHERE` that a pass extends outputs the columns of the read in another order, and the pilot reads columns
    /// for itself. The expressions and the filters of the input find their columns by name and pass the others through.
    for (const auto & step : templ.input->input_steps)
    {
        auto clone = step->clone();
        clone->updateInputHeader(plan.getCurrentHeader());
        plan.addStep(std::move(clone));
    }
    return plan;
}

void keepBestGroups(QueryPlan & plan, const MultiPassTopKTemplate & templ, size_t limit)
{
    plan.addStep(std::make_unique<TopKBestGroupsStep>(
        plan.getCurrentHeader(), rankColumnOf(templ), limit, SortingStep::Settings(templ.input->context->getSettingsRef())));
}

void keepEveryNthGranule(ReadFromMergeTree::AnalysisResult & analysis, size_t period)
{
    size_t granule = 0;
    selectRanges(analysis, [&](const RangesInDataPart & part)
    {
        MarkRanges sampled;
        for (const auto & range : part.ranges)
            for (size_t mark = range.begin; mark < range.end; ++mark, ++granule)
                if (granule % period == 0)
                    sampled.emplace_back(mark, mark + 1);
        return sampled;
    });
}

std::shared_ptr<ReadFromMergeTree::AnalysisResult> splitForAnalysis(const ReadFromMergeTree::AnalysisResult & analysis, size_t entries)
{
    auto result = std::make_shared<ReadFromMergeTree::AnalysisResult>(analysis);
    RangesInDataParts split;
    for (const auto & part : analysis.parts_with_ranges)
    {
        const size_t marks = part.getMarksCount();
        const size_t marks_per_entry = std::max(min_marks_per_entry, (marks + entries - 1) / std::max<size_t>(1, entries));
        RangesInDataPart entry = part;
        entry.ranges.clear();
        entry.exact_ranges.clear();
        size_t entry_marks = 0;
        for (const auto & range : part.ranges)
        {
            for (size_t begin = range.begin; begin < range.end;)
            {
                const size_t end = std::min(range.end, begin + (marks_per_entry - entry_marks));
                entry.ranges.emplace_back(begin, end);
                entry_marks += end - begin;
                begin = end;
                if (entry_marks == marks_per_entry)
                {
                    split.push_back(entry);
                    entry.ranges.clear();
                    entry_marks = 0;
                }
            }
        }
        if (!entry.ranges.empty())
            split.push_back(std::move(entry));
    }
    result->parts_with_ranges = std::move(split);
    return result;
}

void restrictRanges(ReadFromMergeTree::AnalysisResult & analysis, const ReadFromMergeTree::AnalysisResult & restriction)
{
    std::unordered_map<const IMergeTreeDataPart *, MarkRanges> restricted;
    for (const auto & part : restriction.parts_with_ranges)
    {
        auto & ranges = restricted[part.data_part.get()];
        ranges.insert(ranges.end(), part.ranges.begin(), part.ranges.end());
    }
    selectRanges(analysis, [&](RangesInDataPart & part)
    {
        const auto it = restricted.find(part.data_part.get());
        return it == restricted.end() ? MarkRanges{} : intersectRanges(std::move(part.ranges), it->second);
    });
}

const PaddedPODArray<UInt64> & countsOf(const Block & block, const String & name)
{
    return assert_cast<const ColumnUInt64 &>(*block.getByName(name).column).getData();
}

UInt64 nthBest(const PaddedPODArray<UInt64> & ranks, size_t n)
{
    if (ranks.size() < n)
        return 0;
    std::vector<UInt64> best(ranks.begin(), ranks.end());
    std::nth_element(best.begin(), best.begin() + (n - 1), best.end(), std::greater<>());
    return best[n - 1];
}

}
