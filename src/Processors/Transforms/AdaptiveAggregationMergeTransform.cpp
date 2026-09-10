#include <Processors/Transforms/AdaptiveAggregationMergeTransform.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace DB
{

AdaptiveAggregationMergeTransform::AdaptiveAggregationMergeTransform(
    AggregatingTransformParamsPtr params_, ManyAggregatedDataPtr many_data_,
    size_t max_threads_, size_t temporary_data_merge_threads_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : IProcessor(InputPorts(many_data_->num_producers, Block()), {params_->getHeader()})
    , params(std::move(params_))
    , many_data(std::move(many_data_))
    , session(many_data->adaptive_session)
    , max_threads(std::min(many_data->num_producers, max_threads_))
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , updater(std::move(updater_))
{
}

IProcessor::Status AdaptiveAggregationMergeTransform::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts &)
{
    auto & output = outputs.front();
    if (isCancelled() || output.isFinished())
    {
        session->cancel();
        for (auto & input : inputs)
        {
            input.close();
            if (input.hasData())
                input.pullData(/*set_not_needed=*/true);
        }
        many_data.reset();
        return Status::Finished;
    }

    if (stage == Stage::WaitingForInputs)
    {
        if (!inputs_initialized)
        {
            for (auto & input : inputs)
            {
                chassert(!input.hasData());
                if (!input.isFinished())
                {
                    input.setNeeded();
                    unfinished_inputs.insert(&input);
                }
            }
            inputs_initialized = true;
        }
        else
        {
            for (const auto * input : updated_inputs)
            {
                chassert(!input->hasData());
                if (input->isFinished())
                    unfinished_inputs.erase(input);
            }
        }
        return unfinished_inputs.empty() ? Status::Ready : Status::NeedData;
    }

    if (stage == Stage::ExpandingPipeline)
        return Status::UpdatePipeline;

    auto & input = inputs.back();
    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }
    if (input.isFinished())
    {
        output.finish();
        many_data.reset();
        return Status::Finished;
    }
    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    output.push(input.pull());
    return Status::PortFull;
}

void AdaptiveAggregationMergeTransform::work()
{
    chassert(stage == Stage::WaitingForInputs && inputs_initialized && unfinished_inputs.empty());
    processors = createAggregationMergePipeline(
        params, many_data, max_threads, temporary_data_merge_threads,
        /*should_produce_results_in_order_of_bucket_number=*/false, /*skip_merging=*/false, updater);
    chassert(!processors.empty());
    stage = Stage::ExpandingPipeline;
}

IProcessor::PipelineUpdate AdaptiveAggregationMergeTransform::updatePipeline()
{
    chassert(stage == Stage::ExpandingPipeline);
    auto & output = processors.back()->getOutputs().front();
    inputs.emplace_back(output.getHeader(), this);
    connect(output, inputs.back());
    stage = Stage::ReadingMerge;
    for (auto & processor : processors)
        processor->inheritQueryPlanStepFromParent(*this, getQueryPlanStepGroup());
    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

void AdaptiveAggregationMergeTransform::onCancel() noexcept
{
    session->cancel();
}

}
