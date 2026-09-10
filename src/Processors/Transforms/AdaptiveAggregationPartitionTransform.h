#pragma once

#include <Interpreters/AdaptiveAggregationStaging.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

struct AggregatingTransformParams;

/// Gathers miss arguments and groups records by bucket. Downstream demand acknowledges the
/// partitioned output before this transform requests the producer's next block.
class AdaptiveAggregationPartitionTransform final : public ISimpleTransform
{
public:
    AdaptiveAggregationPartitionTransform(
        std::shared_ptr<AggregatingTransformParams> params_, AdaptiveAggregationSessionPtr session_);

    String getName() const override { return "AdaptiveAggregationPartitionTransform"; }
    Status prepare() override;
    void transform(Chunk & chunk) override;
    void onCancel() noexcept override;

private:
    std::shared_ptr<AggregatingTransformParams> params;
    AdaptiveAggregationSessionPtr session;
    StagedChunkConverter converter;
};

}
