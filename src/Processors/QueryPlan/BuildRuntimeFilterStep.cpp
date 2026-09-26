#include <string_view>
#include <Core/Settings.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/RuntimeFilterBloomSizing.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>

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

namespace ProfileEvents
{
extern const Event RuntimeFilterStatesSent;
extern const Event RuntimeFilterStateBytesSent;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_join_runtime_filters_index_analysis;
}

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
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

/// The partial filter shared by all streams of one task. It holds an `AdaptiveSetRuntimeFilter`, not a
/// `RuntimeFilter`: `streams_left` counts the streams still to merge in, and nothing evaluates the
/// partial before it is serialized. The last stream wraps it in a `RuntimeFilter` for the same-task lookup.
struct TaskPartialFilter
{
    std::mutex mutex;
    std::unique_ptr<AdaptiveSetRuntimeFilter> filter;
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
        , runtime_filter_config{geometry.pass_ratio_threshold_for_disabling, geometry.blocks_to_skip_before_reenabling}
        , query_context(std::move(query_context_))
        , partial(
              std::make_unique<AdaptiveSetRuntimeFilter>(
                  filter_column_target_type,
                  geometry,
                  /// No stats-sized bloom growth for a transported partial: its serialized state must
                  /// match the plan's geometry on every receiving task.
                  /*distinct_keys_hint_=*/std::nullopt,
                  /*distinct_keys_hint_matches_filter_key_=*/false))
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
            task_filter->filter->mergeFrom(*partial);
        else
            task_filter->filter = std::move(partial);

        if (--task_filter->streams_left > 0)
            return;

        /// Serialize first: the filter is moved into the lookup below, where `add` calls `finishInsert`.
        WriteBufferFromOwnString out;
        task_filter->filter->serialize(out);
        /// Same-stage `__applyFilter` sites read this task's lookup: an exchange edge back to this stage
        /// would cycle the scheduler. Every transported build registers here, whether or not its stage
        /// has such a site. The filter is already built, so this only keeps it until the task ends.
        /// Every stream has merged in, so the published filter expects no merges.
        if (!filter_key.empty())
        {
            if (!query_context)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Query context is not available for BuildRuntimeFilterPartialTransform");
            auto filter = std::make_unique<RuntimeFilter>(/*filters_to_merge_=*/0, runtime_filter_config, std::move(*task_filter->filter));
            if (query_context->getSettingsRef()[Setting::enable_join_runtime_filters_index_analysis])
                filter->enableIndexAnalysis();
            query_context->getRuntimeFilterLookup()->add(filter_key, filter_name, std::move(filter));
        }
        task_filter->filter.reset();
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
    const RuntimeFilterConfig runtime_filter_config;
    ContextPtr query_context;
    std::unique_ptr<AdaptiveSetRuntimeFilter> partial;
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
    RuntimeFilterBuildOptions build_options_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , filter_column_name(std::move(filter_column_name_))
    , filter_column_type(filter_column_type_)
    , filter_name(filter_name_)
    , filter_key(std::move(filter_key_))
    , build_options(std::move(build_options_))
{
    auto & geometry = build_options.geometry;
    const auto bloom_filter_parameters = resolveRuntimeBloomFilterDefaults(
        RuntimeBloomFilterParameters{geometry.bloom_filter_bytes, geometry.bloom_filter_hash_functions});
    geometry.bloom_filter_bytes = bloom_filter_parameters.bytes;
    geometry.bloom_filter_hash_functions = bloom_filter_parameters.hash_functions;
    validateRuntimeBloomFilterParameters(bloom_filter_parameters);

    /// The exact phase is byte-bounded by the bloom size unless the plan raised it explicitly
    /// (runtime-filter transport does, from build-side row estimates).
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

    /// Only a transported build serializes its key, so a deserialized local build has none. No lookup
    /// could find its filter. The join task derives its own local filter when it optimizes its fragment.
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
            build_options,
            query_context);
    });
}

void BuildRuntimeFilterStep::transformPipelineForTransport(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    const String bucket_id = settings.parameter_lookup->getParameter("bucket_id").safeGet<String>();
    auto partials_header = runtimeFilterPartialsHeader();

    /// Destination streams of this task's single serialized partial. With a merge tree it goes out
    /// once, to the parent merge task; with broadcast exchanges it goes to every destination bucket.
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
                    destination_streams.size(),
                    build_options.geometry,
                    filter_key,
                    filter_name,
                    CurrentThread::get().tryGetQueryContext());
                connect(*port, builder->getInputs().front());
                connect(builder->getOutputs().back(), *resize_input++);
                result.emplace_back(std::move(builder));
            }

            OutputPort * partial_output = &resize->getOutputs().front();
            result.emplace_back(std::move(resize));

            /// All legs of one filter use exchanges of the same kind, so one serializer (none for a
            /// persisted exchange) serves every destination and the copies share its packets.
            SharedHeader stream_header = partials_header;
            if (auto serializer = settings.exchange_lookup->createSerializer(partials_header, destination_streams.front().exchange_id))
            {
                connect(*partial_output, serializer->getInputs().front());
                partial_output = &serializer->getOutputs().front();
                stream_header = partial_output->getSharedHeader();
                result.emplace_back(std::move(serializer));
            }

            if (destination_streams.size() > 1)
            {
                auto copy = std::make_shared<CopyTransform>(stream_header, destination_streams.size());
                connect(*partial_output, copy->getInputs().front());
                auto output = copy->getOutputs().begin();
                for (const auto & stream : destination_streams)
                {
                    auto sink = settings.exchange_lookup->createSink(stream_header, stream, /*advisory*/ true);
                    connect(*output++, sink->getPort());
                    result.emplace_back(std::move(sink));
                }
                result.emplace_back(std::move(copy));
            }
            else
            {
                auto sink = settings.exchange_lookup->createSink(stream_header, destination_streams.front(), /*advisory*/ true);
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
    build_options.geometry.serializeSettings(settings, version);
}

void BuildRuntimeFilterStep::serialize(Serialization & ctx) const
{
    writeStringBinary(filter_column_name, ctx.out);
    encodeDataType(filter_column_type, ctx.out);
    writeStringBinary(filter_name, ctx.out);
    writeBinary(build_options.polarity == RuntimeFilterPolarity::Contains, ctx.out);

    /// Step version 1 carries the filter exchange topology and the key. The registry picks version 0
    /// for a peer whose release predates the transport. Such a peer would run the step as a local
    /// build, and the filter would silently never arrive. So the topology is refused, not dropped.
    if (ctx.step_version < 1)
    {
        if (hasFilterExchanges())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "make_distributed_plan: cannot serialize a BuildRuntimeFilterStep with filter exchanges "
                "at step version {}; all nodes must run the same version",
                ctx.step_version);
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

    /// The rendezvous key is random per plan build, so a cache key must not contain it. A local build
    /// sends no key and stays inert on the worker. The worker plans its own local filters.
    writeStringBinary(hasFilterExchanges() && !ctx.for_cache_key ? filter_key : String{}, ctx.out);
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
    std::vector<FilterExchange> exchanges;
    String filter_key;
    if (ctx.step_version >= 1)
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

        size_t num_exchanges = 0;
        readVarUInt(num_exchanges, ctx.in);
        if (has_tree_exchange && num_exchanges != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "BuildRuntimeFilterStep has both a tree exchange and broadcast exchanges");
        for (size_t i = 0; i < num_exchanges; ++i)
        {
            FilterExchange exchange;
            readStringBinary(exchange.exchange_id, ctx.in);
            readVectorBinary(exchange.destination_buckets, ctx.in);
            if (exchange.exchange_id.empty() || exchange.destination_buckets.empty())
                throw Exception(ErrorCodes::INCORRECT_DATA, "BuildRuntimeFilterStep has an exchange without an id or destinations");
            exchanges.push_back(std::move(exchange));
        }

        readStringBinary(filter_key, ctx.in);
        /// Only a build with filter exchanges carries its key. A local build must stay inert.
        if (!filter_key.empty() && !has_tree_exchange && exchanges.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "BuildRuntimeFilterStep without filter exchanges carries a rendezvous key");
    }

    auto geometry = RuntimeFilterGeometry::fromSettings(ctx.settings);
    if (has_tree_exchange || !exchanges.empty())
        geometry.validateTransported();

    auto step = std::make_unique<BuildRuntimeFilterStep>(
        ctx.input_headers.front(),
        std::move(filter_column_name),
        filter_column_type,
        std::move(filter_name),
        std::move(filter_key),
        RuntimeFilterBuildOptions{
            .geometry = geometry,
            .polarity = allow_to_use_not_exact_filter ? RuntimeFilterPolarity::Contains : RuntimeFilterPolarity::NotContains,
            /// Not serialized: a deserialized step builds without key-range tracking and without the
            /// statistics hint that sizes the bloom filter.
            .track_key_range = false,
            .distinct_keys_hint = std::nullopt,
            .distinct_keys_hint_matches_filter_key = false});
    if (has_tree_exchange)
        step->setTreeExchange(std::move(tree_exchange_id), std::move(tree_source_buckets), tree_fan_in);
    for (auto & exchange : exchanges)
        step->addExchange(std::move(exchange.exchange_id), std::move(exchange.destination_buckets));
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
        format_settings.out << prefix << "Allow not exact filter: " << (build_options.polarity == RuntimeFilterPolarity::Contains) << '\n';
    }
}

void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry);
void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry)
{
    /// Version 1 adds the filter exchange topology.
    registry.registerStep(
        "BuildRuntimeFilter",
        BuildRuntimeFilterStep::deserialize,
        {{0, 0}, {1, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_RUNTIME_FILTER_EXCHANGES}});
}

}
