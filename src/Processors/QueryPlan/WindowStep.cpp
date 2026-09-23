#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/WindowFunction.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/IDataType.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/NullsAction.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/keyTypeBreaksHashSharding.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/PartitionAggregateTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <Processors/Transforms/WindowTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/scatterByPartition.h>
#include <Common/JSONBuilder.h>
#include <base/unit.h>

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForSort;
}

namespace ProfileEvents
{
    extern const Event ExternalSortCompressedBytes;
    extern const Event ExternalSortUncompressedBytes;
    extern const Event ExternalSortWritePart;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

static ITransformingStep::Traits getTraits(bool preserves_sorting)
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = preserves_sorting,
        },
        {
            .preserves_number_of_rows = true
        }
    };
}

static Block addWindowFunctionResultColumns(const Block & block,
    std::vector<WindowFunctionDescription> window_functions)
{
    auto result = block;

    for (const auto & f : window_functions)
    {
        ColumnWithTypeAndName column_with_type;
        column_with_type.name = f.column_name;
        column_with_type.type = f.aggregate_function->getResultType();
        column_with_type.column = column_with_type.type->createColumn();

        result.insert(column_with_type);
    }

    return result;
}

WindowStep::WindowStep(
    const SharedHeader & input_header_,
    const WindowDescription & window_description_,
    const std::vector<WindowFunctionDescription> & window_functions_,
    bool streams_fan_out_,
    std::optional<SortingStep::Settings> hash_partitioning_settings_)
    : ITransformingStep(
          input_header_,
          std::make_shared<const Block>(addWindowFunctionResultColumns(*input_header_, window_functions_)),
          getTraits(!streams_fan_out_ && !hash_partitioning_settings_))
    , window_description(window_description_)
    , window_functions(window_functions_)
    , streams_fan_out(streams_fan_out_)
    , hash_partitioning_settings(std::move(hash_partitioning_settings_))
{
    // We don't remove any columns, only add, so probably we don't have to update
    // the output DataStream::distinct_columns.

    window_description.checkValid();

}

bool WindowStep::canUseHashPartitioning(const WindowDescription & window_description, const Block & input_header)
{
    if (window_description.partition_by.empty() || !window_description.order_by.empty())
        return false;

    /// Without ORDER BY all rows of a partition are peers, so a RANGE or GROUPS frame ending at the current
    /// row ends at the end of the partition.
    const auto & frame = window_description.frame;
    const bool from_start = frame.begin_type == WindowFrame::BoundaryType::Unbounded
        || (frame.begin_type == WindowFrame::BoundaryType::Current && frame.type != WindowFrame::FrameType::ROWS);
    const bool to_end = frame.end_type == WindowFrame::BoundaryType::Unbounded
        || (frame.end_type == WindowFrame::BoundaryType::Current && frame.type != WindowFrame::FrameType::ROWS);
    if (!from_start || !to_end)
        return false;

    for (const auto & function : window_description.window_functions)
        if (dynamic_cast<const IWindowFunction *>(function.aggregate_function.get()))
            return false;

    for (const auto & column : window_description.partition_by)
        if (QueryPlanOptimizations::keyTypeBreaksHashSharding(*input_header.getByName(column.column_name).type))
            return false;

    return true;
}

void WindowStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & build_settings)
{
    auto num_threads = pipeline.getNumThreads();

    if (hash_partitioning_settings)
    {
        const auto & input_header = pipeline.getHeader();
        ColumnNumbers key_positions;
        for (const auto & column : window_description.partition_by)
            key_positions.push_back(input_header.getPositionByName(column.column_name));

        /// Every stream must hold whole partitions.
        if (!skip_scatter_by_partition)
        {
            if (num_threads > 1)
            {
                SortingStep::checkScatterConnectionLimit(num_threads, pipeline.getNumStreams());
                scatterByPartition(pipeline, num_threads, key_positions);
            }
            else
            {
                pipeline.resize(1);
            }
        }

        /// The input can be many small chunks, e.g. after a selective PREWHERE.
        pipeline.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<SimpleSquashingChunksTransform>(header, hash_partitioning_settings->max_block_size, 1_MiB);
        });

        /// `max_rows_to_sort` / `max_bytes_to_sort` per stream, as for the sorting it replaces.
        StreamLocalLimits limits;
        limits.mode = LimitsMode::LIMITS_CURRENT;
        limits.size_limits = hash_partitioning_settings->size_limits;
        limits.size_limits.overflow_mode = OverflowMode::THROW;
        pipeline.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<LimitsCheckingTransform>(header, limits);
        });

        const auto & sort_settings = *hash_partitioning_settings;
        TemporaryDataOnDiskScopePtr tmp_data;
        if (build_settings.temp_data_on_disk)
            tmp_data = build_settings.temp_data_on_disk->childScope(
                {.current_metric = CurrentMetrics::TemporaryFilesForSort,
                 .bytes_compressed = ProfileEvents::ExternalSortCompressedBytes,
                 .bytes_uncompressed = ProfileEvents::ExternalSortUncompressedBytes,
                 .num_files = ProfileEvents::ExternalSortWritePart,
                 .spilled_to_disk_operator = "window"},
                sort_settings.temporary_files_buffer_size,
                sort_settings.temporary_files_codec);
        if (sort_settings.max_bytes_in_block_before_external_sort && !tmp_data)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary data storage for spilling is not provided");

        const PartitionAggregateTransform::SpillSettings spill_settings{
            .max_bytes_before_external = sort_settings.max_bytes_in_block_before_external_sort
                ? std::max<size_t>(1, sort_settings.max_bytes_in_block_before_external_sort / pipeline.getNumStreams())
                : 0,
            .max_query_bytes_before_external = sort_settings.max_bytes_in_query_before_external_sort,
            .min_free_disk_space = sort_settings.min_free_disk_space,
            .tmp_data = std::move(tmp_data),
        };

        pipeline.addSimpleTransform([&](const SharedHeader & header)
        {
            return std::make_shared<PartitionAggregateTransform>(header, output_header, key_positions, window_functions, spill_settings);
        });

        assertBlocksHaveEqualStructure(pipeline.getHeader(), *output_header,
            "WindowStep transform for '" + window_description.window_name + "'");
        return;
    }

    // This resize is needed for cases such as `over ()` when we don't have a
    // sort node, and the input might have multiple streams. The sort node would
    // have resized it.
    if (window_description.full_sort_description.empty())
        pipeline.resize(1);

    pipeline.addSimpleTransform(
        [&](const SharedHeader & /*header*/)
        {
            return std::make_shared<WindowTransform>(
                input_headers.front(), output_header, window_description, window_functions);
        });

    if (streams_fan_out)
    {
        pipeline.resize(num_threads);
    }

    assertBlocksHaveEqualStructure(pipeline.getHeader(), *output_header,
        "WindowStep transform for '" + window_description.window_name + "'");

    /// Intentionally no `RuntimeDataflowStatisticsCollector` here: the window is computed on the
    /// initiator, so the columns it appends are never shipped by replicas. Collecting statistics at
    /// this point would count the window result as replica output and inflate the automatic
    /// parallel-replicas cost model. See `supportsDataflowStatisticsCollection` in the header.
}

void WindowStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;
    settings.out << prefix << "Window: (";
    if (!window_description.partition_by.empty())
    {
        settings.out << "PARTITION BY ";
        for (size_t i = 0; i < window_description.partition_by.size(); ++i)
        {
            if (i > 0)
            {
                settings.out << ", ";
            }
            const auto & column_name = window_description.partition_by[i].column_name;
            settings.out << (settings.pretty ? QueryPlanFormat::formatColumnPretty(column_name, settings.pretty_names) : column_name);
        }
    }
    if (!window_description.partition_by.empty()
        && !window_description.order_by.empty())
    {
        settings.out << " ";
    }
    if (!window_description.order_by.empty())
    {
        settings.out << "ORDER BY ";
        dumpSortDescription(window_description.order_by, settings);
    }
    settings.out << ")\n";
    if (hash_partitioning_settings)
        settings.out << prefix << "Hash partitioning: 1\n";
    if (skip_scatter_by_partition)
        settings.out << prefix << "Skip scatter by partition: 1\n";

    for (size_t i = 0; i < window_functions.size(); ++i)
    {
        settings.out << prefix << (i == 0 ? "Functions: "
                                          : "           ");
        const auto & column_name = window_functions[i].column_name;
        settings.out << (settings.pretty ? QueryPlanFormat::formatColumnPretty(column_name, settings.pretty_names) : column_name) << "\n";
    }
}

void WindowStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (!window_description.partition_by.empty())
    {
        auto partion_columns_array = std::make_unique<JSONBuilder::JSONArray>();
        for (const auto & descr : window_description.partition_by)
            partion_columns_array->add(descr.column_name);

        map.add("Partition By", std::move(partion_columns_array));
    }

    if (!window_description.order_by.empty())
        map.add("Sort Description", explainSortDescription(window_description.order_by));

    auto functions_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & func : window_functions)
        functions_array->add(func.column_name);

    map.add("Functions", std::move(functions_array));
    if (hash_partitioning_settings)
        map.add("Hash partitioning", true);
    if (skip_scatter_by_partition)
        map.add("Skip scatter by partition", true);
}

void WindowStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(addWindowFunctionResultColumns(*input_headers.front(), window_functions));

    window_description.checkValid();
}

Names WindowStep::getPartitionByColumnNames() const
{
    Names names;
    for (const auto & column : window_description.partition_by)
        names.push_back(column.column_name);
    return names;
}

const WindowDescription & WindowStep::getWindowDescription() const
{
    return window_description;
}

QueryPlanStepPtr WindowStep::clone() const
{
    return std::make_unique<WindowStep>(*this);
}

static void serializeWindowFrame(const WindowFrame & frame, WriteBuffer & out)
{
    UInt8 flags = 0;
    if (frame.is_default)
        flags |= 1;
    if (frame.begin_preceding)
        flags |= 2;
    if (frame.end_preceding)
        flags |= 4;
    writeIntBinary(flags, out);

    writeIntBinary(static_cast<UInt8>(frame.type), out);
    writeIntBinary(static_cast<UInt8>(frame.begin_type), out);
    writeIntBinary(static_cast<UInt8>(frame.end_type), out);

    writeFieldBinary(frame.begin_offset, out);
    writeFieldBinary(frame.end_offset, out);
}

static WindowFrame deserializeWindowFrame(ReadBuffer & in)
{
    WindowFrame frame;

    UInt8 flags = 0;
    readIntBinary(flags, in);
    frame.is_default = bool(flags & 1);
    frame.begin_preceding = bool(flags & 2);
    frame.end_preceding = bool(flags & 4);

    /// The plan may be client-supplied (TCPHandler::receiveQueryPlan), so reject out-of-range enum
    /// values instead of casting an arbitrary byte into the enum (which downstream switches would not
    /// handle).
    UInt8 type = 0;
    readIntBinary(type, in);
    if (type > static_cast<UInt8>(WindowFrame::FrameType::RANGE))
        throw Exception(ErrorCodes::INCORRECT_DATA, "WindowStep: invalid window frame type {}", static_cast<UInt16>(type));
    frame.type = static_cast<WindowFrame::FrameType>(type);

    UInt8 begin_type = 0;
    readIntBinary(begin_type, in);
    if (begin_type > static_cast<UInt8>(WindowFrame::BoundaryType::Offset))
        throw Exception(ErrorCodes::INCORRECT_DATA, "WindowStep: invalid window frame begin boundary type {}", static_cast<UInt16>(begin_type));
    frame.begin_type = static_cast<WindowFrame::BoundaryType>(begin_type);

    UInt8 end_type = 0;
    readIntBinary(end_type, in);
    if (end_type > static_cast<UInt8>(WindowFrame::BoundaryType::Offset))
        throw Exception(ErrorCodes::INCORRECT_DATA, "WindowStep: invalid window frame end boundary type {}", static_cast<UInt16>(end_type));
    frame.end_type = static_cast<WindowFrame::BoundaryType>(end_type);

    frame.begin_offset = readFieldBinary(in);
    frame.end_offset = readFieldBinary(in);

    return frame;
}

static void serializeWindowFunctions(const std::vector<WindowFunctionDescription> & window_functions, WriteBuffer & out)
{
    writeVarUInt(window_functions.size(), out);
    for (const auto & func : window_functions)
    {
        writeStringBinary(func.column_name, out);

        /// Argument types are not serialized: they are derived from the input columns on deserialize
        /// (see `deserializeWindowFunctions`), which both avoids trusting client-supplied types and
        /// rebuilds the aggregate exactly as the planner does.
        writeVarUInt(func.argument_names.size(), out);
        for (const auto & argument_name : func.argument_names)
            writeStringBinary(argument_name, out);

        writeStringBinary(func.aggregate_function->getName(), out);

        /// Serialize the parameters the planner preserved, not `aggregate_function->getParameters()`:
        /// some parametric aggregates (e.g. `groupArrayMovingSum`, `groupArrayMovingAvg`,
        /// `mannWhitneyUTest`, `kolmogorovSmirnovTest`) do not round-trip their parameters through
        /// `getParameters`, which would silently rebuild a different function on the worker. This mirrors
        /// `serializeAggregateDescriptions`.
        writeVarUInt(func.function_parameters.size(), out);
        for (const auto & param : func.function_parameters)
            writeFieldBinary(param, out);
    }
}

static std::vector<WindowFunctionDescription>
deserializeWindowFunctions(ReadBuffer & in, const Block & input_header)
{
    UInt64 num_functions = 0;
    readVarUInt(num_functions, in);

    std::vector<WindowFunctionDescription> window_functions(num_functions);
    for (auto & func : window_functions)
    {
        readStringBinary(func.column_name, in);

        UInt64 num_argument_names = 0;
        readVarUInt(num_argument_names, in);
        func.argument_names.resize(num_argument_names);
        for (auto & argument_name : func.argument_names)
            readStringBinary(argument_name, in);

        /// The plan may come from an untrusted client (TCPHandler::receiveQueryPlan). Derive the argument
        /// types from the actual input columns rather than trusting the wire: WindowTransform sizes its
        /// argument workspace from argument_names and the aggregate reads those columns by position and
        /// casts them to its argument types, so types that disagree with the real columns would cause an
        /// out-of-bounds read or an invalid cast. Deriving from the header also rebuilds the aggregate
        /// exactly as the planner did, which uses the argument column types.
        func.argument_types.resize(func.argument_names.size());
        for (size_t i = 0; i < func.argument_names.size(); ++i)
        {
            if (!input_header.has(func.argument_names[i]))
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "WindowStep: argument column '{}' is not present in the input header", func.argument_names[i]);

            func.argument_types[i] = input_header.getByName(func.argument_names[i]).type;
        }

        String function_name;
        readStringBinary(function_name, in);

        UInt64 num_parameters = 0;
        readVarUInt(num_parameters, in);
        func.function_parameters.resize(num_parameters);
        for (auto & param : func.function_parameters)
            param = readFieldBinary(in);

        AggregateFunctionProperties properties;
        func.aggregate_function = AggregateFunctionFactory::instance().get(
            function_name,
            NullsAction::EMPTY,
            func.argument_types,
            func.function_parameters,
            properties,
            AggregateFunctionStateVariant::Window);
    }

    return window_functions;
}

void WindowStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 /*version*/) const
{
    if (hash_partitioning_settings)
        hash_partitioning_settings->updatePlanSettings(settings);
}

void WindowStep::serialize(Serialization & ctx) const
{
    /// `WindowStep` is only registered under `QueryPlanStepRegistry` since query-plan serialization
    /// version 4; an older worker does not know the "Window" step name at all and would throw
    /// `UNKNOWN_IDENTIFIER` on it, or worse, misparse the stream if it ever did partially recognize the
    /// name. Fail closed here rather than write bytes an older peer cannot understand.
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_WINDOW_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan: serializing a WindowStep requires query plan serialization "
            "version >= {}; all nodes must run the same version", DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_WINDOW_STEP);

    if (hash_partitioning_settings && ctx.step_version < 1)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Serializing a hash-partitioned WindowStep requires query plan serialization version >= {}",
            DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_WINDOW_HASH_PARTITIONING);

    UInt8 flags = 0;
    if (streams_fan_out)
        flags |= 1;
    if (hash_partitioning_settings)
        flags |= 2;
    writeIntBinary(flags, ctx.out);

    writeStringBinary(window_description.window_name, ctx.out);

    serializeSortDescription(window_description.partition_by, ctx.out, ctx.version);
    serializeSortDescription(window_description.order_by, ctx.out, ctx.version);

    serializeWindowFrame(window_description.frame, ctx.out);

    serializeWindowFunctions(window_functions, ctx.out);
}

QueryPlanStepPtr WindowStep::deserialize(Deserialization & ctx)
{
    /// Mirrors the guard in `serialize`: a "Window" step never legitimately arrives from a stream
    /// written below this version, since a peer that old cannot have written one (see `serialize`).
    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_WINDOW_STEP)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "make_distributed_plan: deserializing a WindowStep requires query plan serialization "
            "version >= {}; all nodes must run the same version", DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_WINDOW_STEP);

    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "WindowStep must have one input stream");

    UInt8 flags = 0;
    readIntBinary(flags, ctx.in);
    bool streams_fan_out = bool(flags & 1);
    std::optional<SortingStep::Settings> hash_partitioning_settings;
    if (ctx.step_version >= 1 && (flags & 2))
        hash_partitioning_settings.emplace(ctx.settings);

    WindowDescription window_description;
    readStringBinary(window_description.window_name, ctx.in);

    deserializeSortDescription(window_description.partition_by, ctx.in, ctx.version, ctx.max_type_complexity);
    deserializeSortDescription(window_description.order_by, ctx.in, ctx.version, ctx.max_type_complexity);

    window_description.frame = deserializeWindowFrame(ctx.in);

    /// `full_sort_description` is not serialized: it is the concatenation of PARTITION BY and
    /// ORDER BY, reconstructed here exactly as the planner builds it (see PlannerWindowFunctions).
    window_description.full_sort_description = window_description.partition_by;
    window_description.full_sort_description.insert(
        window_description.full_sort_description.end(),
        window_description.order_by.begin(),
        window_description.order_by.end());

    window_description.window_functions = deserializeWindowFunctions(ctx.in, *ctx.input_headers.front());

    return std::make_unique<WindowStep>(
        ctx.input_headers.front(),
        window_description,
        window_description.window_functions,
        streams_fan_out,
        std::move(hash_partitioning_settings));
}

void registerWindowStep(QueryPlanStepRegistry & registry);
void registerWindowStep(QueryPlanStepRegistry & registry)
{
    const QueryPlanStepRegistry::StepVersions versions{{0, 0}, {1, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_WINDOW_HASH_PARTITIONING}};
    registry.registerStep("Window", WindowStep::deserialize, versions);
}

}
