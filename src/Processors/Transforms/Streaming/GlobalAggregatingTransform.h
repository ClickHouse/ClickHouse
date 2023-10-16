#include <Processors/Transforms/Streaming/AggregatingTransform.h>

namespace DB
{
namespace Streaming
{
class GlobalAggregatingTransform final : public AggregatingTransform
{
public:
    GlobalAggregatingTransform(Block header, AggregatingTransformParamsPtr params_);

    /// For Parallel aggregating.
    GlobalAggregatingTransform(
        Block header,
        AggregatingTransformParamsPtr params_,
        ManyAggregatedDataPtr many_data,
        size_t current_variant_,
        size_t max_threads);

    ~GlobalAggregatingTransform() override = default;

    String getName() const override { return "GlobalAggregatingTransform"; }

private:
    bool needFinalization(Int64 min_watermark) const override;
    bool prepareFinalization(Int64 min_watermark) override;

    void finalize(const ChunkContextPtr & chunk_ctx) override;

    inline bool initialize(ManyAggregatedDataVariantsPtr & data, const ChunkContextPtr & chunk_ctx);

    void convertSingleLevel(ManyAggregatedDataVariantsPtr & data, const ChunkContextPtr & chunk_ctx);

    void convertTwoLevel(ManyAggregatedDataVariantsPtr & data, const ChunkContextPtr & chunk_ctx);
};

}
}
