#include <Processors/QueryPlan/ShuffleStep.h>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Operators.h>
#include <Processors/Port.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>
#include <Processors/Transforms/ShuffleTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/JSONBuilder.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

ITransformingStep::Traits getTraits(UInt64 limit)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = true,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = limit == 0,
        }
    };
}

SharedHeader makeOutputHeader(const SharedHeader & input_header, const std::optional<ShuffleStep::Key> & key)
{
    if (!key || key->replace)
        return input_header;

    auto header = *input_header;
    if (header.has(key->column_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The shuffle key column {} already exists", key->column_name);
    auto type = std::make_shared<DataTypeUInt64>();
    header.insert({type->createColumn(), type, key->column_name});
    return std::make_shared<const Block>(std::move(header));
}

/// Scatters every row of every stream into a uniformly random one of `num_parts` streams.
void scatterRandomly(QueryPipelineBuilder & pipeline, size_t num_parts)
{
    const size_t num_streams = pipeline.getNumStreams();
    pipeline.transform([&](OutputPortRawPtrs ports)
    {
        Processors result;
        VectorWithMemoryTracking<OutputPort *> part_outputs;
        part_outputs.reserve(num_streams * num_parts);
        for (auto * port : ports)
        {
            auto scatter = ScatterByPartitionTransform::createRandom(port->getSharedHeader(), num_parts);
            connect(*port, scatter->getInputs().front());
            for (auto & output : scatter->getOutputs())
                part_outputs.push_back(&output);
            result.push_back(std::move(scatter));
        }

        if (num_streams == 1)
            return result;

        for (size_t part = 0; part < num_parts; ++part)
        {
            auto resize = std::make_shared<ResizeProcessor>(part_outputs[part]->getSharedHeader(), num_streams, 1);
            auto input_it = resize->getInputs().begin();
            for (size_t stream = 0; stream < num_streams; ++stream, ++input_it)
                connect(*part_outputs[stream * num_parts + part], *input_it);
            result.push_back(std::move(resize));
        }
        return result;
    });
}

}

ShuffleStep::ShuffleStep(
    const SharedHeader & input_header,
    std::optional<Key> key_,
    UInt64 limit_,
    size_t max_block_size_)
    : ITransformingStep(input_header, makeOutputHeader(input_header, key_), getTraits(limit_))
    , key(std::move(key_))
    , limit(limit_)
    , max_block_size(max_block_size_)
{
}

void ShuffleStep::updateOutputHeader()
{
    output_header = makeOutputHeader(input_headers.front(), key);
}

void ShuffleStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    const auto input_header = pipeline.getSharedHeader();
    const auto output_header = getOutputHeader();

    std::optional<ShuffleKeyDescription> key_description;
    if (key)
        key_description = ShuffleKeyDescription{
            .position = output_header->getPositionByName(key->column_name),
            .replace = key->replace,
            .direction = key->direction};

    /// The totals and extremes are not shuffled, but they need the appended key column. It is added in the same call
    /// as the transform that adds the key column to the main stream, because the streams must have the same header.
    auto totals_transform = [&](const SharedHeader & header) -> ProcessorPtr
    {
        if (key && !key->replace)
            return std::make_shared<AppendShuffleKeyTransform>(header, output_header);
        return nullptr;
    };

    if (limit)
    {
        /// Reservoir sampling of every stream, then the merge of the samples.
        pipeline.addSimpleTransform(
            [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;
                return std::make_shared<ReservoirSampleTransform>(header, limit, max_block_size);
            });

        if (pipeline.getNumStreams() > 1)
            pipeline.addTransform(std::make_shared<MergingShuffledTransform>(input_header, pipeline.getNumStreams(), limit, max_block_size));

        if (key_description)
        {
            pipeline.addSimpleTransform(
                [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
                {
                    if (stream_type != QueryPipelineBuilder::StreamType::Main)
                        return totals_transform(header);
                    return std::make_shared<FillShuffleKeyTransform>(header, output_header, *key_description);
                });
        }
        return;
    }

    /// Without a limit: scatter the rows into parts at random, shuffle the parts in parallel, and concatenate them.
    /// The scatter creates (num_parts * num_streams) connections, which is bounded as for `SortingStep`.
    const size_t num_parts = pipeline.getNumThreads();
    const size_t max_scatter_connections = 1000000;
    if (num_parts > 1 && num_parts * pipeline.getNumStreams() <= max_scatter_connections)
        scatterRandomly(pipeline, num_parts);
    else
        pipeline.resize(1);

    const size_t num_ranges = pipeline.getNumStreams();
    size_t range_index = 0;
    pipeline.addSimpleTransform(
        [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return totals_transform(header);
            return std::make_shared<ShuffleTransform>(header, output_header, key_description, max_block_size, range_index++, num_ranges);
        });

    if (pipeline.getNumStreams() > 1)
        pipeline.addTransform(std::make_shared<ShuffleConcatTransform>(output_header, pipeline.getNumStreams()));
}

void ShuffleStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    if (key)
        settings.out << prefix << "Shuffle key: " << key->column_name << (key->direction < 0 ? " DESC" : " ASC") << '\n';
    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void ShuffleStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (key)
    {
        map.add("Shuffle Key", key->column_name);
        map.add("Direction", key->direction < 0 ? "DESC" : "ASC");
    }
    if (limit)
        map.add("Limit", limit);
}

QueryPlanStepPtr ShuffleStep::clone() const
{
    return std::make_unique<ShuffleStep>(*this);
}

}
