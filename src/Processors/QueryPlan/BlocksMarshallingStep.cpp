#include <Processors/QueryPlan/BlocksMarshallingStep.h>

#include <Processors/ISimpleTransform.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/SortChunksBySequenceNumber.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

struct MarshallBlocks : ISimpleTransform
{
public:
    explicit MarshallBlocks(const Block & header_, BlockMarshallingCallback callback_)
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

BlocksMarshallingStep::BlocksMarshallingStep(const Header & input_header_)
    : ITransformingStep(input_header_, input_header_, getTraits())
{
}

void BlocksMarshallingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    if (pipeline.getNumStreams() > 1)
    {
        pipeline.addSimpleTransform([&](const Block & header)
                                    { return std::make_shared<MarshallBlocks>(header, settings.block_marshalling_callback); });
    }
    else
    {
        const auto num_threads = pipeline.getNumThreads();
        pipeline.addTransform(std::make_shared<AddSequenceNumber>(pipeline.getHeader()));
        pipeline.resize(num_threads);
        pipeline.addSimpleTransform([&](const Block & header)
                                    { return std::make_shared<MarshallBlocks>(header, settings.block_marshalling_callback); });
        pipeline.addTransform(std::make_shared<SortChunksBySequenceNumber>(pipeline.getHeader(), num_threads));
    }
}

std::unique_ptr<IQueryPlanStep> BlocksMarshallingStep::deserialize(Deserialization & ctx)
{
    return std::make_unique<BlocksMarshallingStep>(ctx.input_headers.front());
}

void BlocksMarshallingStep::updateOutputHeader()
{
}

}
