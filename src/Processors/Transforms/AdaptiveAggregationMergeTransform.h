#pragma once

#include <Processors/Transforms/AggregatingTransform.h>

namespace DB
{

/// Carries one sealed staged chunk of the adaptive aggregation through the pipeline: the
/// producing AggregatingTransform emits it as a rowless chunk (the header's columns, all
/// empty) and the staged-chunk store absorbs it into the session backlog. The chunk is
/// mutable because the store finishes it (builds its instruction preparation) at absorption;
/// in flight it is exclusively owned.
class StagedChunkInfo final : public ChunkInfoCloneable<StagedChunkInfo>
{
public:
    explicit StagedChunkInfo(MutableStagedChunkPtr chunk_) : chunk(std::move(chunk_)) { }

    MutableStagedChunkPtr chunk;
};


/// The staged-chunk store and finish barrier of the adaptive aggregation. Every producing
/// AggregatingTransform emits its sealed staged chunks through its output port; this
/// processor absorbs them into the session backlog (it is the backlog's only writer, so
/// publication needs no cross-producer coordination). A producer closes its port only after
/// it flushed its staging and finished its local table, so once every input is finished the
/// aggregation is complete: the store then assembles the merge - exactly the work the
/// last-finishing AggregatingTransform does on the non-adaptive path - and forwards the
/// merged output through its own port.
class AdaptiveAggregationMergeTransform final : public IProcessor
{
public:
    AdaptiveAggregationMergeTransform(
        SharedHeader header,
        size_t num_inputs,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data_,
        size_t max_threads_,
        size_t temporary_data_merge_threads_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    String getName() const override { return "AdaptiveAggregationMerge"; }
    Status prepare() override;
    void work() override;
    PipelineUpdate updatePipeline() override;

private:
    void assembleMerge();

    /// The processors of the assembled merge, handed to the executor via `updatePipeline`.
    Processors processors;

    AggregatingTransformParamsPtr params;
    LoggerPtr log = getLogger("AdaptiveAggregationMergeTransform");

    ManyAggregatedDataPtr many_data;
    const size_t num_producers;
    size_t max_threads;
    size_t temporary_data_merge_threads;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    /// Keeps the spilled files of an external merge alive for the sources reading them.
    std::list<TemporaryBlockStreamHolder> tmp_files;

    Chunk chunk_to_absorb;
    bool has_chunk_to_absorb = false;
    bool merge_assembled = false;
    bool is_pipeline_created = false;
};


}
