#pragma once

#include <Interpreters/AdaptiveAggregationStaging.h>
#include <Processors/IProcessor.h>

namespace DB
{

struct AggregatingTransformParams;

/// Buffers small partitioned chunks and emits coalesced payloads. Under pressure, it waits for
/// publication of its buffered tail before renewing demand. End of input also flushes the tail.
class AdaptiveAggregationCoalescingTransform final : public IProcessor
{
public:
    AdaptiveAggregationCoalescingTransform(
        std::shared_ptr<AggregatingTransformParams> params_, AdaptiveAggregationSessionPtr session_);

    String getName() const override { return "AdaptiveAggregationCoalescingTransform"; }
    Status prepare() override;
    void work() override;
    void onCancel() noexcept override;

private:
    std::shared_ptr<AggregatingTransformParams> params;
    AdaptiveAggregationSessionPtr session;
    StagedChunkCoalescer coalescer;
    Chunk current_chunk;
    Chunk ready_chunk;
    bool has_input = false;
    bool flush_pending = false;
    bool input_finished = false;
    bool use_own_memory_tracker = false;
};

}
