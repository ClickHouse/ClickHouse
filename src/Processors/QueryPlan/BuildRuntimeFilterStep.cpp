#include <string_view>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>

#include <algorithm>
#include <mutex>
#include <Columns/ColumnString.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/ISink.h>
#include <Processors/QueryPlan/ExchangeLookup.h>
#include <Processors/QueryPlan/IParameterLookup.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/CopyTransform.h>
#include <Processors/Transforms/MergeRuntimeFiltersTransform.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>

namespace ProfileEvents
{
extern const Event RuntimeFilterStatesSent;
extern const Event RuntimeFilterStateBytesSent;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

namespace
{

/// The partial filter shared by all streams of one task: each stream merges its part in at end of
/// stream, and the stream that completes the merge serializes the result.
struct TaskPartialFilter
{
    std::mutex mutex;
    UniqueRuntimeFilterPtr filter;
    size_t streams_left;

    explicit TaskPartialFilter(size_t num_streams)
        : streams_left(num_streams)
    {
    }
};

/// Passes the build-side stream through while collecting the filter column into its own partial
/// filter. At end of stream the partial is merged into the shared per-task one; the last stream
/// serializes the merged result and emits it as a single row on the second output.
class BuildRuntimeFilterPartialTransform final : public IProcessor
{
public:
    BuildRuntimeFilterPartialTransform(
        SharedHeader header,
        SharedHeader partials_header,
        std::shared_ptr<TaskPartialFilter> task_filter_,
        String filter_column_name_,
        const DataTypePtr & filter_column_type_,
        size_t num_streams,
        size_t num_destinations_,
        const RuntimeFilterGeometry & geometry,
        String filter_key_,
        String filter_name_,
        ContextPtr query_context_)
        : IProcessor({header}, {header, std::move(partials_header)})
        , task_filter(std::move(task_filter_))
        , num_destinations(num_destinations_)
        , filter_column_position(header->getPositionByName(filter_column_name_))
        , filter_column_original_type(header->getByPosition(filter_column_position).type)
        , filter_column_target_type(filter_column_type_)
        , filter_key(std::move(filter_key_))
        , filter_name(std::move(filter_name_))
        , query_context(std::move(query_context_))
        , partial(
              std::make_unique<ApproximateRuntimeFilter>(
                  /*filters_to_merge_=*/num_streams - 1,
                  filter_column_target_type,
                  geometry,
                  /// No stats-sized bloom growth for a transported partial: its serialized state must
                  /// match the plan's geometry on every receiving task.
                  /*distinct_keys_hint_=*/std::nullopt))
    {
        if (!filter_column_target_type->equals(*filter_column_original_type))
            cast_to_target_type = createInternalCast(
                header->getByPosition(filter_column_position), filter_column_target_type, CastType::nonAccurate, {}, nullptr);
    }

    String getName() const override { return "BuildRuntimeFilterPartialTransform"; }

    Status prepare() override
    {
        auto & input = inputs.front();
        auto & data_output = outputs.front();
        auto & partial_output = outputs.back();

        if (data_output.isFinished())
        {
            input.close();
            partial_output.finish();
            return Status::Finished;
        }

        if (has_data_chunk)
        {
            if (!data_output.canPush())
                return Status::PortFull;
            data_output.push(std::move(data_chunk));
            has_data_chunk = false;
        }

        if (has_partial_chunk)
        {
            if (!partial_output.isFinished())
            {
                if (!partial_output.canPush())
                    return Status::PortFull;
                partial_output.push(std::move(partial_chunk));
            }
            has_partial_chunk = false;
            data_output.finish();
            partial_output.finish();
            return Status::Finished;
        }

        if (input.isFinished())
        {
            if (!finished_building)
                return Status::Ready;
            data_output.finish();
            partial_output.finish();
            return Status::Finished;
        }

        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        data_chunk = input.pull(/*set_not_needed=*/true);
        has_data_chunk = true;
        return Status::Ready;
    }

    void work() override
    {
        if (has_data_chunk)
        {
            ColumnPtr column = data_chunk.getColumns()[filter_column_position];
            if (cast_to_target_type)
                column = cast_to_target_type->execute(
                    {ColumnWithTypeAndName(column, filter_column_original_type, "")},
                    filter_column_target_type,
                    column->size(),
                    /*dry_run=*/false);
            partial->insert(column);
            return;
        }

        finished_building = true;

        std::lock_guard lock(task_filter->mutex);
        if (task_filter->filter)
            task_filter->filter->merge(partial.get());
        else
            task_filter->filter = std::move(partial);

        if (--task_filter->streams_left > 0)
            return;

        WriteBufferFromOwnString out;
        assert_cast<ApproximateRuntimeFilter &>(*task_filter->filter).serialize(out);
        /// Same-stage `__applyFilter` is not on the exchange (that edge would cycle the scheduler).
        /// Serialize before `add`: it takes ownership and `finishInsert`s.
        if (!filter_key.empty())
        {
            if (!query_context)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Query context is not available for BuildRuntimeFilterPartialTransform");
            query_context->getRuntimeFilterLookup()->add(filter_key, filter_name, std::move(task_filter->filter));
        }
        ProfileEvents::increment(ProfileEvents::RuntimeFilterStatesSent, num_destinations);
        ProfileEvents::increment(ProfileEvents::RuntimeFilterStateBytesSent, out.str().size() * num_destinations);
        auto column = ColumnString::create();
        column->insertData(out.str().data(), out.str().size());
        Columns columns;
        columns.emplace_back(std::move(column));
        partial_chunk = Chunk(std::move(columns), 1);
        has_partial_chunk = true;
    }

private:
    std::shared_ptr<TaskPartialFilter> task_filter;
    const size_t num_destinations;
    const size_t filter_column_position;
    const DataTypePtr filter_column_original_type;
    const DataTypePtr filter_column_target_type;
    FunctionBasePtr cast_to_target_type;

    const String filter_key;
    const String filter_name;
    ContextPtr query_context;
    UniqueRuntimeFilterPtr partial;
    Chunk data_chunk;
    Chunk partial_chunk;
    bool has_data_chunk = false;
    bool has_partial_chunk = false;
    bool finished_building = false;
};

}

BuildRuntimeFilterStep::BuildRuntimeFilterStep(
    const SharedHeader & input_header_,
    String filter_column_name_,
    const DataTypePtr & filter_column_type_,
    String filter_name_,
    String filter_key_,
    RuntimeFilterGeometry geometry_,
    bool allow_to_use_not_exact_filter_,
    bool track_key_range_,
    std::optional<UInt64> distinct_keys_hint_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , filter_column_name(std::move(filter_column_name_))
    , filter_column_type(filter_column_type_)
    , filter_name(filter_name_)
    , filter_key(std::move(filter_key_))
    , geometry(geometry_)
    , allow_to_use_not_exact_filter(allow_to_use_not_exact_filter_)
    , track_key_range(track_key_range_)
    , distinct_keys_hint(distinct_keys_hint_)
{
    if (!geometry.bloom_filter_bytes)
        geometry.bloom_filter_bytes = DEFAULT_RUNTIME_BLOOM_FILTER_BYTES;
    if (geometry.bloom_filter_bytes > MAX_RUNTIME_BLOOM_FILTER_BYTES)
        throw Exception(
            ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Specified runtime bloom filter size {} is too big, maximum: {}",
            geometry.bloom_filter_bytes,
            MAX_RUNTIME_BLOOM_FILTER_BYTES);

    if (!geometry.bloom_filter_hash_functions)
        geometry.bloom_filter_hash_functions = DEFAULT_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS;
    if (geometry.bloom_filter_hash_functions > MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS)
        throw Exception(
            ErrorCodes::PARAMETER_OUT_OF_BOUND,
            "Specified runtime bloom filter hash function count {} is too big, maximum: {}",
            geometry.bloom_filter_hash_functions,
            MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS);

    /// The exact phase is byte-bounded by the bloom size unless the plan raised it explicitly
    /// (runtime-filter transport does, from cardinality estimates).
    if (!geometry.exact_bytes_limit)
        geometry.exact_bytes_limit = geometry.bloom_filter_bytes;
}

void BuildRuntimeFilterStep::addExchange(String exchange_id_, Strings destination_buckets_)
{
    chassert(!tree_exchange);
    exchanges.push_back(FilterExchange{std::move(exchange_id_), std::move(destination_buckets_)});
}

void BuildRuntimeFilterStep::setTreeExchange(String exchange_id_, Strings source_buckets_, size_t fan_in_)
{
    chassert(exchanges.empty() && !tree_exchange);
    tree_exchange = TreeExchange{std::move(exchange_id_), std::move(source_buckets_), fan_in_};
}

void BuildRuntimeFilterStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    if (hasFilterExchanges())
    {
        transformPipelineForTransport(pipeline, settings);
        return;
    }

    if (filter_key.empty())
        return;

    auto streams = pipeline.getNumStreams();
    auto query_context = CurrentThread::get().tryGetQueryContext();
    pipeline.addSimpleTransform([&, query_context](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type)-> ProcessorPtr
    {
        /// Build the filter only from the main stream
        if (stream_type != QueryPipelineBuilder::StreamType::Main)
            return nullptr;

        return std::make_shared<BuildRuntimeFilterTransform>(
            header,
            filter_column_name,
            filter_column_type,
            filter_name,
            filter_key,
            /*filters_to_merge_=*/streams - 1,
            geometry,
            allow_to_use_not_exact_filter,
            track_key_range,
            distinct_keys_hint,
            query_context);
    });
}

void BuildRuntimeFilterStep::transformPipelineForTransport(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    const String bucket_id = settings.parameter_lookup->getParameter("bucket_id").safeGet<String>();
    auto partials_header = runtimeFilterPartialsHeader();

    /// Destination streams of this task's single serialized partial: through the merge tree the
    /// partial goes out exactly once, to the parent merge task; a single-task build stage is
    /// itself the tree root and broadcasts to every destination of every receiving stage.
    std::vector<ExchangeStreamId> destination_streams;
    if (tree_exchange)
    {
        const auto it = std::find(tree_exchange->source_buckets.begin(), tree_exchange->source_buckets.end(), bucket_id);
        if (it == tree_exchange->source_buckets.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "BuildRuntimeFilterStep: bucket {} is not among the build stage buckets", bucket_id);
        const size_t bucket_index = it - tree_exchange->source_buckets.begin();
        destination_streams.emplace_back(tree_exchange->exchange_id, bucket_id, toString(bucket_index / tree_exchange->fan_in));
    }
    else
    {
        for (const auto & exchange : exchanges)
            for (const String & destination_bucket : exchange.destination_buckets)
                destination_streams.emplace_back(exchange.exchange_id, bucket_id, destination_bucket);
    }

    pipeline.transform(
        [&](OutputPortRawPtrs ports)
        {
            Processors result;
            auto task_filter = std::make_shared<TaskPartialFilter>(ports.size());

            auto resize = std::make_shared<ResizeProcessor>(partials_header, ports.size(), 1);
            auto resize_input = resize->getInputs().begin();
            for (auto * port : ports)
            {
                auto builder = std::make_shared<BuildRuntimeFilterPartialTransform>(
                    port->getSharedHeader(),
                    partials_header,
                    task_filter,
                    filter_column_name,
                    filter_column_type,
                    ports.size(),
                    destination_streams.size(),
                    geometry,
                    filter_key,
                    filter_name,
                    CurrentThread::get().tryGetQueryContext());
                connect(*port, builder->getInputs().front());
                connect(builder->getOutputs().back(), *resize_input++);
                result.emplace_back(std::move(builder));
            }

            OutputPort * partial_output = &resize->getOutputs().front();
            result.emplace_back(std::move(resize));

            if (destination_streams.size() > 1)
            {
                auto copy = std::make_shared<CopyTransform>(partials_header, destination_streams.size());
                connect(*partial_output, copy->getInputs().front());
                auto output = copy->getOutputs().begin();
                for (const auto & stream : destination_streams)
                {
                    auto sink = settings.exchange_lookup->createSink(partials_header, stream, /*advisory*/ true);
                    connect(*output++, sink->getPort());
                    result.emplace_back(std::move(sink));
                }
                result.emplace_back(std::move(copy));
            }
            else
            {
                auto sink = settings.exchange_lookup->createSink(partials_header, destination_streams.front(), /*advisory*/ true);
                connect(*partial_output, sink->getPort());
                result.emplace_back(std::move(sink));
            }

            return result;
        },
        /*check_ports=*/false);
}

void BuildRuntimeFilterStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

void BuildRuntimeFilterStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const
{
    geometry.serializeSettings(settings, version);
}

void BuildRuntimeFilterStep::serialize(Serialization & ctx) const
{
    writeStringBinary(filter_column_name, ctx.out);
    encodeDataType(filter_column_type, ctx.out);
    writeStringBinary(filter_name, ctx.out);
    writeBinary(allow_to_use_not_exact_filter, ctx.out);

    if (ctx.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_RUNTIME_FILTER_EXCHANGES)
    {
        if (hasFilterExchanges())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "make_distributed_plan: serializing a BuildRuntimeFilterStep with filter exchanges requires "
                "query plan serialization version >= {}; all nodes must run the same version",
                DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_RUNTIME_FILTER_EXCHANGES);
        return;
    }

    writeBinary(UInt8(tree_exchange ? 1 : 0), ctx.out);
    if (tree_exchange)
    {
        writeStringBinary(tree_exchange->exchange_id, ctx.out);
        writeVectorBinary(tree_exchange->source_buckets, ctx.out);
        writeVarUInt(tree_exchange->fan_in, ctx.out);
    }
    writeVarUInt(exchanges.size(), ctx.out);
    for (const auto & exchange : exchanges)
    {
        writeStringBinary(exchange.exchange_id, ctx.out);
        writeVectorBinary(exchange.destination_buckets, ctx.out);
    }
}

QueryPlanStepPtr BuildRuntimeFilterStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "BuildRuntimeFilterStep must have one input stream");

    String filter_column_name;
    readStringBinary(filter_column_name, ctx.in);

    DataTypePtr filter_column_type = decodeDataType(ctx.in, ctx.max_type_complexity);

    String filter_name;
    readStringBinary(filter_name, ctx.in);

    bool allow_to_use_not_exact_filter = false;
    readBinary(allow_to_use_not_exact_filter, ctx.in);

    UInt8 has_tree_exchange = 0;
    String tree_exchange_id;
    Strings tree_source_buckets;
    size_t tree_fan_in = 0;
    size_t num_exchanges = 0;
    if (ctx.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_RUNTIME_FILTER_EXCHANGES)
    {
        readBinary(has_tree_exchange, ctx.in);
        if (has_tree_exchange > 1)
            throw Exception(ErrorCodes::INCORRECT_DATA, "BuildRuntimeFilterStep has a malformed tree exchange flag");
        if (has_tree_exchange)
        {
            readStringBinary(tree_exchange_id, ctx.in);
            readVectorBinary(tree_source_buckets, ctx.in);
            readVarUInt(tree_fan_in, ctx.in);
            if (tree_exchange_id.empty() || tree_source_buckets.empty() || tree_fan_in == 0)
                throw Exception(ErrorCodes::INCORRECT_DATA, "BuildRuntimeFilterStep has a malformed tree exchange");
        }

        readVarUInt(num_exchanges, ctx.in);
        if (has_tree_exchange && num_exchanges != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "BuildRuntimeFilterStep has both a tree exchange and broadcast exchanges");
    }

    auto geometry = RuntimeFilterGeometry::fromSettings(ctx.settings);
    if (has_tree_exchange || num_exchanges != 0)
        geometry.validateTransported();

    auto step = std::make_unique<BuildRuntimeFilterStep>(
        ctx.input_headers.front(),
        std::move(filter_column_name),
        filter_column_type,
        std::move(filter_name),
        /*filter_key_=*/String{},
        geometry,
        allow_to_use_not_exact_filter,
        /*track_key_range_=*/false);
    if (has_tree_exchange)
        step->setTreeExchange(std::move(tree_exchange_id), std::move(tree_source_buckets), tree_fan_in);
    for (size_t i = 0; i < num_exchanges; ++i)
    {
        String exchange_id;
        readStringBinary(exchange_id, ctx.in);
        Strings destination_buckets;
        readVectorBinary(destination_buckets, ctx.in);
        if (exchange_id.empty() || destination_buckets.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "BuildRuntimeFilterStep has an exchange without an id or destinations");
        step->addExchange(std::move(exchange_id), std::move(destination_buckets));
    }
    return step;
}

QueryPlanStepPtr BuildRuntimeFilterStep::clone() const
{
    return std::make_unique<BuildRuntimeFilterStep>(*this);
}

void BuildRuntimeFilterStep::describeActions(FormatSettings & format_settings) const
{
    const std::string & prefix = format_settings.detail_prefix;

    std::string_view filter_id_view = filter_name;
    if (format_settings.pretty)
    {
        if (auto it = format_settings.runtime_filter_names.find(filter_name); it != format_settings.runtime_filter_names.end())
            filter_id_view = it->second.pretty_name;
    }

    format_settings.out << prefix << "Filter id: " << filter_id_view << '\n';

    if (format_settings.pretty)
    {
        if (auto it = format_settings.runtime_filter_names.find(filter_name); it != format_settings.runtime_filter_names.end())
        {
            if (!it->second.build_table_name.empty())
                format_settings.out << prefix << "Source table: " << it->second.build_table_name << '\n';
        }
    }
    else
    {
        format_settings.out << prefix << "Allow not exact filter: " << allow_to_use_not_exact_filter << '\n';
    }
}

void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry);
void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("BuildRuntimeFilter", BuildRuntimeFilterStep::deserialize);
}

}
