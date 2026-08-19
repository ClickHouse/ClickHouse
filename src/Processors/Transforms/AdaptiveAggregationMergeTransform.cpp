#include <Processors/Transforms/AdaptiveAggregationMergeTransform.h>

#include <Interpreters/AdaptiveAggregationImpl.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

AdaptiveAggregationMergeTransform::AdaptiveAggregationMergeTransform(
    SharedHeader header,
    size_t num_inputs,
    AggregatingTransformParamsPtr params_,
    ManyAggregatedDataPtr many_data_,
    size_t max_threads_,
    size_t temporary_data_merge_threads_,
    RuntimeDataflowStatisticsCacheUpdaterPtr updater_)
    : IProcessor(InputPorts(num_inputs, header), {header})
    , params(std::move(params_))
    , many_data(std::move(many_data_))
    , num_producers(num_inputs)
    , max_threads(max_threads_)
    , temporary_data_merge_threads(temporary_data_merge_threads_)
    , updater(std::move(updater_))
{
}

IProcessor::Status AdaptiveAggregationMergeTransform::prepare()
{
    auto & output = outputs.front();

    if (output.isFinished())
    {
        for (auto & in : inputs)
            in.close();
        return Status::Finished;
    }

    if (!merge_assembled)
    {
        if (has_chunk_to_absorb)
            return Status::Ready;

        /// Absorption is not gated on the output: nothing can leave before the merge exists,
        /// and the producers must never wait on the downstream to hand their chunks over.
        bool all_finished = true;
        auto in = inputs.begin();
        for (size_t i = 0; i < num_producers; ++i, ++in)
        {
            if (in->isFinished())
                continue;
            all_finished = false;
            in->setNeeded();
            if (in->hasData())
            {
                chunk_to_absorb = in->pull(/*set_not_needed = */ true);
                has_chunk_to_absorb = true;
                return Status::Ready;
            }
        }

        if (!all_finished)
            return Status::NeedData;

        /// Every producer closed its port: the aggregation is complete, assemble the merge.
        return Status::Ready;
    }

    if (!is_pipeline_created)
        return Status::UpdatePipeline;

    /// Forward the merged output (the input `updatePipeline` added).
    if (!output.canPush())
        return Status::PortFull;

    auto & merged = inputs.back();
    if (merged.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    merged.setNeeded();
    if (!merged.hasData())
        return Status::NeedData;

    output.push(merged.pull(/*set_not_needed = */ false));
    return Status::PortFull;
}

void AdaptiveAggregationMergeTransform::work()
{
    if (has_chunk_to_absorb)
    {
        auto info = chunk_to_absorb.getChunkInfos().get<StagedChunkInfo>();
        if (!info || !info->chunk)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The adaptive staged-chunk store received a chunk without a staged payload.");

        MutableStagedChunkPtr staged = info->chunk;
        info.reset();
        chunk_to_absorb = {};
        has_chunk_to_absorb = false;

        /// The store is the backlog's only writer: publication finishes the chunk (builds its
        /// instruction preparation) and registers it with the buckets it touches.
        StagedChunkBacklogSink(params->aggregator, *many_data->adaptive_session).consume(std::move(staged));
        return;
    }

    assembleMerge();
}

void AdaptiveAggregationMergeTransform::assembleMerge()
{
    merge_assembled = true;

    const auto & session = many_data->adaptive_session;
    const bool engaged = session->initialized.load(std::memory_order_acquire);
    if (engaged)
        LOG_TRACE(
            log,
            "Adaptive aggregation: {} delayed records queued for the merge-time drain",
            session->backlog.undrainedRecords());

    /// The adaptive admission rejects bucket-ordered output and partitioned (skip-merging)
    /// aggregation, so the assembly runs the plain parallel-merge shape.
    processors = assembleAggregatedMerge(
        params,
        many_data,
        engaged ? session : nullptr,
        updater,
        max_threads,
        temporary_data_merge_threads,
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*skip_merging=*/false,
        log,
        tmp_files);
}

IProcessor::PipelineUpdate AdaptiveAggregationMergeTransform::updatePipeline()
{
    if (processors.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not updatePipeline in AdaptiveAggregationMergeTransform. This is a bug.");

    auto & out = processors.back()->getOutputs().front();
    inputs.emplace_back(out.getHeader(), this);
    connect(out, inputs.back());
    is_pipeline_created = true;

    for (auto & proc : processors)
        proc->inheritQueryPlanStepFromParent(*this, generatingStepGroupOf(*this));

    return PipelineUpdate{.to_add = std::move(processors), .to_remove = {}};
}

}
