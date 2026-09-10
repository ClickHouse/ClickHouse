#include <Processors/Transforms/AdaptiveAggregationPublishTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace DB
{

AdaptiveAggregationPublishTransform::AdaptiveAggregationPublishTransform(
    std::shared_ptr<AggregatingTransformParams> params_, AdaptiveAggregationSessionPtr session_)
    : IProcessor({params_->aggregator.getAdaptiveStagedHeader()}, {Block()})
    , params(std::move(params_))
    , session(std::move(session_))
{
}

IProcessor::Status AdaptiveAggregationPublishTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();
    if (isCancelled() || output.isFinished())
    {
        session->cancel();
        input.close();
        if (input.hasData())
            input.pullData(/*set_not_needed=*/true);
        current_chunk.clear();
        return Status::Finished;
    }
    if (has_input)
        return Status::Ready;
    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    /// Completion needs no output demand. Renewed input demand acknowledges the preceding publication.
    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    current_chunk = input.pull(/*set_not_needed=*/true);
    has_input = true;
    return Status::Ready;
}

void AdaptiveAggregationPublishTransform::work()
{
    params->aggregator.publishAdaptiveChunk(*session, std::move(current_chunk));
    has_input = false;
}

void AdaptiveAggregationPublishTransform::onCancel() noexcept
{
    session->cancel();
}

}
