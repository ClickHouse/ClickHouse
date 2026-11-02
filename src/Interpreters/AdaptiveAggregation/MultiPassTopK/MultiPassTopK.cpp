#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopK.h>

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/BoundingKey.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/MultiPassTopKImpl.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/Pilot.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace MultiPassTopKImpl
{

namespace
{

/// The fewest granules that the read must select. The pilot and the proof cost about as much on a small input as on a
/// large one, while the aggregation they save grows with the input: below about a thousand granules, 8M rows at the
/// default granule size, the pilot of most queries finds no clear top and only adds its cost.
constexpr size_t min_selected_granules = 1024;

}

}

using namespace MultiPassTopKImpl;

const ActionsDAG * expressionOfInputStep(const IQueryPlanStep & step)
{
    if (const auto * expression = typeid_cast<const ExpressionStep *>(&step))
        return &expression->getExpression();
    if (const auto * filter = typeid_cast<const FilterStep *>(&step))
        return &filter->getExpression();
    return nullptr;
}

MultiPassTopKInputPtr makeMultiPassTopKInput(
    const AggregatingStep & aggregating,
    const ReadFromMergeTree & read,
    const std::vector<const IQueryPlanStep *> & input_steps,
    size_t top,
    size_t rank_index)
{
    /// The candidate pass proves the n-th best of its candidates, so they must hold the top.
    if (top > max_candidates || read.getAnalyzedResult()->selected_marks < min_selected_granules)
        return nullptr;
    if (!pilotCanIdentifyGranules(read))
        return nullptr;

    auto input = std::make_shared<MultiPassTopKInput>();
    input->top = top;
    input->rank_index = rank_index;
    input->read = read.cloneWithContext(read.getContext());
    if (!blocksHaveEqualStructure(*input->read->getOutputHeader(), *read.getOutputHeader()))
        return nullptr;
    input->read->adoptFiltersFrom(read);
    for (const auto * step : input_steps)
        input->input_steps.push_back(step->clone());
    input->context = read.getContext();
    input->bounding_key = findBoundingKey(aggregating, *input);
    return input;
}

std::optional<String> boundingKeyOf(const MultiPassTopKInput & input)
{
    if (!input.bounding_key)
        return {};
    return input.bounding_key->name;
}

size_t rankIndexOf(const MultiPassTopKInput & input)
{
    return input.rank_index;
}

}
