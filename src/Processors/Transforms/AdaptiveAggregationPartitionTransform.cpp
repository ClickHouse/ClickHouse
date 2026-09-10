#include <Processors/Transforms/AdaptiveAggregationPartitionTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace DB
{

AdaptiveAggregationPartitionTransform::AdaptiveAggregationPartitionTransform(
    std::shared_ptr<AggregatingTransformParams> params_, AdaptiveAggregationSessionPtr session_)
    : ISimpleTransform(params_->aggregator.getAdaptiveArgumentHeader(), params_->aggregator.getAdaptiveStagedHeader(), false)
    , params(std::move(params_))
    , session(std::move(session_))
{
}

IProcessor::Status AdaptiveAggregationPartitionTransform::prepare()
{
    if (isCancelled() || output.isFinished())
    {
        session->cancel();
        input.close();
        if (input.hasData())
            input.pullData(/*set_not_needed=*/true);
        input_data = {};
        output_data = {};
        converter = {};
        return Status::Finished;
    }
    return ISimpleTransform::prepare();
}

void AdaptiveAggregationPartitionTransform::transform(Chunk & chunk)
{
    chunk = params->aggregator.partitionAdaptiveBlock(*session, converter, std::move(chunk));
}

void AdaptiveAggregationPartitionTransform::onCancel() noexcept
{
    session->cancel();
}

}
