#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>

#include <Processors/QueryPlan/ReadFromSubscriptionStep.h>
#include <Processors/Sources/SubscriptionSource.h>
#include <Processors/Transforms/ExpressionTransform.h>

#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

ReadFromSubscriptionStep::ReadFromSubscriptionStep(Block storage_sample_, Block desired_header_, SubscriberPtr subscriber_)
    : ISourceStep(DataStream{.header = desired_header_})
    , storage_sample(std::move(storage_sample_))
    , desired_header(std::move(desired_header_))
    , subscriber(std::move(subscriber_))
{
}

void ReadFromSubscriptionStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    Pipe pipe(std::make_shared<SubscriptionSource>(storage_sample, std::move(subscriber)));

    if (!isCompatibleHeader(storage_sample, desired_header))
    {
        auto converting_dag = ActionsDAG::makeConvertingActions(
            storage_sample.getColumnsWithTypeAndName(), desired_header.getColumnsWithTypeAndName(), ActionsDAG::MatchColumnsMode::Name);

        auto converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));

        pipe.addSimpleTransform([&](const Block & cur_header)
                                { return std::make_shared<ExpressionTransform>(cur_header, converting_actions); });
    }

    pipeline.init(std::move(pipe));
}

}
