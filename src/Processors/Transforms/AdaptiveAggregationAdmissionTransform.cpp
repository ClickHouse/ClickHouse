#include <Processors/Transforms/AdaptiveAggregationAdmissionTransform.h>
#include <Common/Exception.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

AdaptiveAggregationAdmissionTransform::AdaptiveAggregationAdmissionTransform(
    SharedHeader header, std::shared_ptr<AggregatingTransformParams> params_, AdaptiveAggregationSessionPtr session_)
    : IProcessor({header}, {header})
    , params(std::move(params_))
    , session(std::move(session_))
{
}

IProcessor::Status AdaptiveAggregationAdmissionTransform::prepare()
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
        has_current_chunk = false;
        return Status::Finished;
    }

    if (has_current_chunk)
        return Status::Ready;

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    /// Renewed demand acknowledges registration and release of the previous envelope in `work`.
    /// Completion carries no data and needs no output demand.
    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    current_chunk = input.pull(/*set_not_needed=*/true);
    has_current_chunk = true;
    return Status::Ready;
}

void AdaptiveAggregationAdmissionTransform::work()
{
    const auto info = current_chunk.getChunkInfos().get<StagedChunkInfo>();
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk should have StagedChunkInfo.");
    params->aggregator.admitStagedChunk(*session, info->chunk, info->use_own_memory_tracker);
    current_chunk.clear();
    has_current_chunk = false;
}

void AdaptiveAggregationAdmissionTransform::onCancel() noexcept
{
    session->cancel();
}

}
