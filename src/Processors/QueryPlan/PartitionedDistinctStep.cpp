#include <IO/Operators.h>
#include <Processors/QueryPlan/PartitionedDistinctStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/PartitionedDistinctTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/BitHelpers.h>
#include <Common/JSONBuilder.h>

namespace DB
{

namespace QueryPlanSerializationSetting
{
extern const QueryPlanSerializationSettingsOverflowMode distinct_overflow_mode;
extern const QueryPlanSerializationSettingsUInt64 max_bytes_in_distinct;
extern const QueryPlanSerializationSettingsUInt64 max_rows_in_distinct;
}

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits{
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = false,
        }};
}

PartitionedDistinctStep::PartitionedDistinctStep(
    const SharedHeader & input_header_,
    const Names & columns_,
    const SizeLimits & set_size_limits_,
    UInt64 limit_hint_,
    size_t partitions_num_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , set_size_limits(set_size_limits_)
    , limit_hint(limit_hint_)
    , columns(columns_)
    , partitions_num(roundUpToPowerOfTwoOrZero(partitions_num_))
{
}

void PartitionedDistinctStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto prev_streams_num = pipeline.getNumStreams();
    size_t streams = partitions_num;
    pipeline.resize(streams);

    /// PartitionDistinctMapTransform hash one chunk into N parts.
    /// PartitionDistinctReduceTransform does distinct on each part.
    auto pipeline_transform = [&](OutputPortRawPtrs ports)
    {
        Processors processors;
        Processors map_processors;
        std::vector<OutputPort *> map_output_ports;
        Processors reduce_processors;
        std::vector<InputPort *> reduce_input_ports;
        // size_t num_ports = ports.size();
        // assert(num_ports == streams);
        auto header = ports[0]->getSharedHeader();
        for (size_t i = 0; i < streams; ++i)
        {
            map_processors.push_back(std::make_shared<PartitionDistinctMapTransform>(header, columns, streams));
            auto & map_outputs = map_processors.back()->getOutputs();
            for (auto & port : map_outputs)
            {
                map_output_ports.push_back(&port);
            }

            reduce_processors.push_back(std::make_shared<PartitionDistinctReduceTransform>(header, columns, streams));
            auto & reduce_inputs = reduce_processors.back()->getInputs();
            for (auto & port : reduce_inputs)
            {
                reduce_input_ports.push_back(&port);
            }

            processors.push_back(map_processors.back());
            processors.push_back(reduce_processors.back());
        }
        for (size_t i = 0; i < streams; ++i)
        {
            auto & output_port = *ports[i];
            auto & input_port = map_processors[i]->getInputs().front();
            connect(output_port, input_port);
        }
        for (size_t i = 0; i < streams; ++i)
        {
            for (size_t j = 0; j < streams; ++j)
            {
                auto & map_output_port = *map_output_ports[i * streams + j];
                auto & reduce_input_port = *reduce_input_ports[j * streams + i];
                connect(map_output_port, reduce_input_port);
            }
        }
        return processors;
    };
    pipeline.transform(pipeline_transform);

    pipeline.resize(prev_streams_num);
}


void PartitionedDistinctStep::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    settings[QueryPlanSerializationSetting::max_rows_in_distinct] = set_size_limits.max_rows;
    settings[QueryPlanSerializationSetting::max_bytes_in_distinct] = set_size_limits.max_bytes;
    settings[QueryPlanSerializationSetting::distinct_overflow_mode] = set_size_limits.overflow_mode;
}

void PartitionedDistinctStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    settings.out << prefix << "Columns: ";

    if (columns.empty())
        settings.out << "none";
    else
    {
        bool first = true;
        for (const auto & column : columns)
        {
            if (!first)
                settings.out << ", ";
            first = false;

            settings.out << column;
        }
    }

    settings.out << '\n';
}

void PartitionedDistinctStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
}

void PartitionedDistinctStep::updateLimitHint(UInt64 hint)
{
    if (hint && limit_hint)
        /// Both limits are set - take the min
        limit_hint = std::min(hint, limit_hint);
    else
        /// Some limit is not set - take the other one
        limit_hint = std::max(hint, limit_hint);
}

void PartitionedDistinctStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

void PartitionedDistinctStep::serialize(Serialization & ctx) const
{
    writeVarUInt(columns.size(), ctx.out);
    for (const auto & column : columns)
        writeStringBinary(column, ctx.out);
    writeVarUInt(partitions_num, ctx.out);
}

std::unique_ptr<IQueryPlanStep> PartitionedDistinctStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "DistinctStep must have one input stream");

    size_t columns_size;
    readVarUInt(columns_size, ctx.in);
    Names column_names(columns_size);
    for (size_t i = 0; i < columns_size; ++i)
        readStringBinary(column_names[i], ctx.in);
    size_t partitions_num_ = 0;
    readVarUInt(partitions_num_, ctx.in);

    SizeLimits size_limits;
    size_limits.max_rows = ctx.settings[QueryPlanSerializationSetting::max_rows_in_distinct];
    size_limits.max_bytes = ctx.settings[QueryPlanSerializationSetting::max_bytes_in_distinct];
    size_limits.overflow_mode = ctx.settings[QueryPlanSerializationSetting::distinct_overflow_mode];

    return std::make_unique<PartitionedDistinctStep>(ctx.input_headers.front(), column_names, size_limits, 0, partitions_num_);
}

void registerPartitionedDistinctStep(QueryPlanStepRegistry & factory)
{
    factory.registerStep("PartitionedDistinct", PartitionedDistinctStep::deserialize);
}
}
