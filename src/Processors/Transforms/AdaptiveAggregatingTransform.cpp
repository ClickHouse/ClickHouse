#include <Processors/Transforms/AdaptiveAggregatingTransform.h>
#include <Interpreters/AdaptiveAggregationChunkInfo.h>
#include <Interpreters/AdaptiveAggregationExecution.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace DB
{

AdaptiveAggregatingTransform::AdaptiveAggregatingTransform(
    SharedHeader header, AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_, size_t current_variant)
    : AggregatingTransformBase(header, params_->aggregator.getAdaptiveArgumentHeader(), params_, std::move(many_data_), current_variant)
    , adaptive_context(std::make_unique<AdaptiveAggregationProducer>(many_data->adaptive_session))
    , adaptive_execution(std::make_unique<AdaptiveAggregationExecution>(*adaptive_context))
{
    chassert(params->aggregator.getParams().enable_adaptive_aggregator && many_data->adaptive_session);
}

AdaptiveAggregatingTransform::~AdaptiveAggregatingTransform() = default;

void AdaptiveAggregatingTransform::onCancel() noexcept
{
    adaptive_context->session->cancel();
}

IProcessor::Status AdaptiveAggregatingTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();
    if (isCancelled() || output.isFinished())
    {
        adaptive_context->session->cancel();
        input.close();
        if (input.hasData())
            input.pullData(/*set_not_needed=*/true);
        current_chunk.clear();
        adaptive_execution.reset();
        many_data.reset();
        return Status::Finished;
    }

    /// Renewed demand follows staging, required publications, and release of the input arguments.
    if (!output.canPush())
        return Status::PortFull;
    if (has_output_chunk)
    {
        output.push(std::move(current_chunk));
        has_output_chunk = false;
        return Status::PortFull;
    }
    if (adaptive_execution->hasPendingBlock())
        return Status::Ready;

    if (is_consume_finished)
    {
        input.close();
        if (!local_aggregation_finished)
            return Status::Ready;
        output.finish();
        return Status::Finished;
    }
    if (read_current_chunk)
        return Status::Ready;
    if (input.isFinished())
    {
        is_consume_finished = true;
        return Status::Ready;
    }
    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    current_chunk = input.pull(/*set_not_needed=*/true);
    read_current_chunk = true;
    return Status::Ready;
}

void AdaptiveAggregatingTransform::work()
{
    if (adaptive_execution->hasPendingBlock())
    {
        if (!params->aggregator.resumeAdaptiveBlock(*adaptive_execution, variants, no_more_keys))
            is_consume_finished = true;
    }
    else if (is_consume_finished)
        finishAggregation();
    else
    {
        consume(current_chunk, adaptive_execution.get());
        read_current_chunk = false;
        if (adaptive_execution->hasPendingBlock())
        {
            current_chunk.getChunkInfos().add(std::move(adaptive_execution->misses));
            params->aggregator.extractAdaptiveArguments(current_chunk, std::move(adaptive_execution->key_column));
            has_output_chunk = true;
        }
        else
            current_chunk.clear();
    }
}

void AdaptiveAggregatingTransform::finishAggregation()
{
    finishLocalAggregation();
    if (adaptive_context->session->initialized.load(std::memory_order_acquire) && variants.isConvertibleToTwoLevel())
        variants.convertToTwoLevel();
    local_aggregation_finished = true;
    many_data.reset();
}

}
