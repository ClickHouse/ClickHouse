#include <Processors/QueryPlan/BlocksMarshallingStep.h>

#include <Processors/ISimpleTransform.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/SortChunksBySequenceNumber.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

struct MarshallBlocksTransform : ISimpleTransform
{
public:
    explicit MarshallBlocksTransform(SharedHeader header_, BlockMarshallingCallback callback_)
        : ISimpleTransform(header_, header_, false)
        , callback(std::move(callback_))
    {
    }

    String getName() const override { return "MarshallBlocks"; }

    void transform(Chunk & chunk) override
    {
        const size_t num_rows = chunk.getNumRows();
        auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
        block = callback(block);
        chunk.setColumns(block.getColumns(), num_rows);
    }

private:
    BlockMarshallingCallback callback;
};

BlocksMarshallingStep::BlocksMarshallingStep(const SharedHeader & input_header_)
    : ITransformingStep(input_header_, input_header_, getTraits())
{
}

void BlocksMarshallingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    // The getNumStreams() == 1 is special, because it may indicate that pipeline ended with a sorting or aggregation, i.e. we should preserve chunks order.
    const bool single_stream = pipeline.getNumStreams() == 1;
    if (single_stream)
        pipeline.addTransform(std::make_shared<AddSequenceNumber>(pipeline.getSharedHeader()));
    const size_t num_threads = pipeline.getNumThreads();
    pipeline.resize(num_threads);
    pipeline.addSimpleTransform([&](const SharedHeader & header)
                                { return std::make_shared<MarshallBlocksTransform>(header, settings.block_marshalling_callback); });
    if (single_stream)
        pipeline.addTransform(std::make_shared<SortChunksBySequenceNumber>(pipeline.getHeader(), num_threads));
}

std::unique_ptr<IQueryPlanStep> BlocksMarshallingStep::deserialize(Deserialization & ctx)
{
    chassert(ctx.input_headers.size() == 1);
    return std::make_unique<BlocksMarshallingStep>(ctx.input_headers.front());
}

void BlocksMarshallingStep::updateOutputHeader()
{
}

}
