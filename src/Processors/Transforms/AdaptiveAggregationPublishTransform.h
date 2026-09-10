#pragma once

#include <Interpreters/AdaptiveAggregation.h>
#include <Processors/IProcessor.h>

namespace DB
{

struct AggregatingTransformParams;

/// Sizes and prepares coalesced records, then registers immutable chunks in the shared backlog.
/// Renewed input demand acknowledges publication. The output carries stream completion only.
class AdaptiveAggregationPublishTransform final : public IProcessor
{
public:
    AdaptiveAggregationPublishTransform(
        std::shared_ptr<AggregatingTransformParams> params_, AdaptiveAggregationSessionPtr session_);

    String getName() const override { return "AdaptiveAggregationPublishTransform"; }
    Status prepare() override;
    void work() override;
    void onCancel() noexcept override;

private:
    std::shared_ptr<AggregatingTransformParams> params;
    AdaptiveAggregationSessionPtr session;
    Chunk current_chunk;
    bool has_input = false;
};

}
