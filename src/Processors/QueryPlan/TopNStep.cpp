#include <Processors/QueryPlan/TopNStep.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
        {
            {
                .returns_single_stream = true,
                .preserves_number_of_streams = false,
                .preserves_sorting = false,
            },
            {
                .preserves_number_of_rows = false,
            }
        };
}

TopNStep::TopNStep(QueryPlanStepPtr sorting_step_, QueryPlanStepPtr limit_step_)
    : ITransformingStep(sorting_step_->getInputStreams()[0], sorting_step_->getInputStreams()[0].header, getTraits())
    , sorting_step(sorting_step_)
    , limit_step(limit_step_)
{
    output_stream = limit_step->getOutputStream();
    output_stream->sort_description = sorting_step->getOutputStream().sort_description;
    output_stream->sort_scope = sorting_step->getOutputStream().sort_scope;
}

void TopNStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto * limit = typeid_cast<LimitStep *>(limit_step.get());
    assert(limit != nullptr);
    auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
    assert(sorting != nullptr);

    sorting->transformPipeline(pipeline, {});
    limit->transformPipeline(pipeline, {});
}

void TopNStep::updateOutputStream()
{
    output_stream = limit_step->getOutputStream();
    output_stream->sort_description = sorting_step->getOutputStream().sort_description;
    output_stream->sort_scope = sorting_step->getOutputStream().sort_scope;
}


std::shared_ptr<TopNStep> TopNStep::makePreliminary(bool exact_rows_before_limit)
{
    auto * limit = typeid_cast<LimitStep *>(limit_step.get());
    assert(limit != nullptr);
    auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
    assert(sorting != nullptr);

    auto pre_sort = sorting->clone();
    auto pre_limit = std::make_shared<LimitStep>(
        limit_step->getInputStreams().front(),
        limit->getLimitForSorting(),
        0,
        exact_rows_before_limit,
        limit->withTies(),
        limit->getSortDescription());

    auto pre_topn = std::make_shared<TopNStep>(pre_sort, pre_limit);
    pre_topn->phase = TopNStep::Phase::Preliminary;
    return pre_topn;
}

std::shared_ptr<TopNStep> TopNStep::makeFinal(size_t max_block_size, bool exact_rows_before_limit)
{
    auto * limit = typeid_cast<LimitStep *>(limit_step.get());
    assert(limit != nullptr);
    auto * sorting = typeid_cast<SortingStep *>(sorting_step.get());
    assert(sorting != nullptr);

    auto merging_sorted = std::make_shared<SortingStep>(
        sorting_step->getInputStreams()[0], sorting->getSortDescription(), max_block_size, sorting->getLimit(), exact_rows_before_limit);

    auto final_limit = std::make_shared<LimitStep>(
        limit_step->getInputStreams().front(),
        limit->getLimit(),
        limit->getOffset(),
        exact_rows_before_limit,
        limit->withTies(),
        limit->getSortDescription());

    auto final_topn = std::make_shared<TopNStep>(merging_sorted, final_limit);
    final_topn->phase = TopNStep::Phase::Final;
    return final_topn;
}

}
