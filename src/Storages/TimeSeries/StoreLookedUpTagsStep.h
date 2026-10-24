#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>


namespace DB
{

class ContextTimeSeriesTagsCollector;

/// Stores the tags of the time series selected by the tags lookup query of `timeSeriesSelector` into the tags collector of the
/// query context, in every stream of the query, and passes on only the columns before the tags: the id and the kinds of samples.
/// The input columns starting at `tags_start_index` are the arguments of `timeSeriesStoreTags` after the id:
/// `tags, '__name__', metric_name, 'tag_name1', tag_column1, ...`.
/// The step is not serializable, so a distributed plan keeps it on the initiator.
/// The goal of this step is to make tags storing parallelizable, while keeping it on the initiator at the same time.
class StoreLookedUpTagsStep final : public ITransformingStep
{
public:
    /// The name the errors about the columns of the lookup query refer to.
    static constexpr std::string_view FUNCTION_NAME = "lookUpTags";

    StoreLookedUpTagsStep(
        const SharedHeader & input_header_,
        size_t tags_start_index_,
        const std::shared_ptr<ContextTimeSeriesTagsCollector> & tags_collector_);

    String getName() const override { return "StoreLookedUpTags"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    QueryPlanStepPtr clone() const override;

private:
    void updateOutputHeader() override;

    const size_t tags_start_index;
    const std::shared_ptr<ContextTimeSeriesTagsCollector> tags_collector;
};

}
