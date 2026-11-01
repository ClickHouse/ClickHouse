#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/ExternalLimitByTransform.h>
#include <Processors/Transforms/LimitByTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <Core/SettingsQuirks.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>

namespace ProfileEvents
{
    extern const Event ExternalLimitByWritePart;
    extern const Event ExternalLimitByCompressedBytes;
    extern const Event ExternalLimitByUncompressedBytes;
}

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForLimitBy;
}

namespace DB
{

namespace Setting
{
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 prefer_external_sort_block_bytes;
    extern const SettingsUInt64 max_bytes_before_external_limit_by;
    extern const SettingsDouble max_bytes_ratio_before_external_limit_by;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsString temporary_files_codec;
    extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
}

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsNonZeroUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsUInt64 prefer_external_sort_block_bytes;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_limit_by;
    extern const QueryPlanSerializationSettingsDouble max_bytes_ratio_before_external_limit_by;
    extern const QueryPlanSerializationSettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const QueryPlanSerializationSettingsString temporary_files_codec;
    extern const QueryPlanSerializationSettingsNonZeroUInt64 temporary_files_buffer_size;
}

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = false,
            .preserves_sorting = false,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

LimitByStep::Settings::Settings(const DB::Settings & settings_)
{
    max_block_size = settings_[Setting::max_block_size];
    prefer_external_sort_block_bytes = settings_[Setting::prefer_external_sort_block_bytes];

    max_bytes_before_external_limit_by = settings_[Setting::max_bytes_before_external_limit_by];
    max_bytes_ratio_before_external_limit_by = settings_[Setting::max_bytes_ratio_before_external_limit_by];

    min_free_disk_space = settings_[Setting::min_free_disk_space_for_temporary_data];
    temporary_files_codec = settings_[Setting::temporary_files_codec];
    temporary_files_buffer_size = settings_[Setting::temporary_files_buffer_size];
}

LimitByStep::Settings::Settings(const QueryPlanSerializationSettings & settings_)
{
    max_block_size = settings_[QueryPlanSerializationSetting::max_block_size];
    prefer_external_sort_block_bytes = settings_[QueryPlanSerializationSetting::prefer_external_sort_block_bytes];

    max_bytes_before_external_limit_by = settings_[QueryPlanSerializationSetting::max_bytes_before_external_limit_by];
    max_bytes_ratio_before_external_limit_by = settings_[QueryPlanSerializationSetting::max_bytes_ratio_before_external_limit_by];

    min_free_disk_space = settings_[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data];
    temporary_files_codec = settings_[QueryPlanSerializationSetting::temporary_files_codec];
    temporary_files_buffer_size = clampTemporaryFilesBufferSize(settings_[QueryPlanSerializationSetting::temporary_files_buffer_size]);
}

void LimitByStep::Settings::updatePlanSettings(QueryPlanSerializationSettings & plan_settings, UInt64 version) const
{
    plan_settings[QueryPlanSerializationSetting::max_block_size] = max_block_size;
    plan_settings[QueryPlanSerializationSetting::prefer_external_sort_block_bytes] = prefer_external_sort_block_bytes;

    if (version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXTERNAL_LIMIT_BY)
    {
        plan_settings[QueryPlanSerializationSetting::max_bytes_before_external_limit_by] = max_bytes_before_external_limit_by;
        plan_settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_limit_by] = max_bytes_ratio_before_external_limit_by;
    }

    plan_settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data] = min_free_disk_space;
    plan_settings[QueryPlanSerializationSetting::temporary_files_codec] = temporary_files_codec;
    plan_settings[QueryPlanSerializationSetting::temporary_files_buffer_size] = temporary_files_buffer_size;
}

LimitByStep::LimitByStep(
    const SharedHeader & input_header_,
    Settings settings_,
    size_t group_length_, size_t group_offset_, Names columns_, bool always_read_till_end_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , settings(std::move(settings_))
    , group_length(group_length_)
    , group_offset(group_offset_)
    , columns(std::move(columns_))
    , always_read_till_end(always_read_till_end_)
{
}

void LimitByStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & build_settings)
{
    /// Here's what each variable means:
    /// `sorted_columns_descr` (non-empty): every stream is sorted by the LIMIT BY keys, in that key order. This can happen for two reasons
    ///                         1. Upstream ORDER BY compatible with LIMIT BY keys
    ///                         2. Reading from MergeTree InOrder if LIMIT BY keys are compatible with primary key
    /// `skip_stream_merging`: every stream has disjoint keys. This means that if a key appears in one stream, it doesn't appear in any other. Currently, this happens if the
    ///                        partition key is a function of the LIMIT BY keys and we read each partition through a different port.
    ///
    /// Importantly, if there is an upstream ORDER BY, then `pipeline.getNumStreams() == 1` for sure. Otherwise,
    /// we can output keys in any order.

    /// Per-partition reading: each partition is a disjoint, already-sorted stream. No merge needed and we can
    /// run the optimized `LimitBySortedStreamTransform` per stream. Again, in this case, it does not matter if `pipeline.getNumStreams()` is 1 or more.
    /// If it's 1, then we have one single global order in the output. If it's more than 1, that means there is no upstream ORDER BY and as a result, we can
    /// output results in any order. The result is correct (but orderless) because data is disjoint across streams.
    if (!sorted_columns_descr.empty() && skip_stream_merging)
    {
        pipeline.addSimpleTransform(
            [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<LimitBySortedStreamTransform>(
                    header, group_length, group_offset, sorted_columns_descr, always_read_till_end);
            });
        return;
    }

    /// The hash-based transforms below keep a count per group, which can spill to disk once query memory reaches
    /// the threshold or when a memory scheduler asks. The sorted-stream transform keeps one group at a time and
    /// never needs to.
    const UInt64 group_limit_end = computeGroupLimitEnd(group_length, group_offset);
    const size_t external_threshold = getMaxBytesBeforeExternalProcessing(
        settings.max_bytes_before_external_limit_by, settings.max_bytes_ratio_before_external_limit_by,
        "max_bytes_ratio_before_external_limit_by");
    const bool external = external_threshold || build_settings.isSchedulerSpillingEnabled();

    TemporaryDataOnDiskScopePtr tmp_data_on_disk;
    auto create_hash_transform = [&](const SharedHeader & header) -> ProcessorPtr
    {
        /// Constant keys form one group, and `LIMIT 0 BY` returns no rows, so neither needs spill storage.
        if (!external || group_length == 0 || calculateLimitByKeyColumnsPositions(*header, columns).empty())
            return std::make_shared<LimitByTransform>(header, group_length, group_offset, columns, always_read_till_end);

        /// Sorted-stream execution never invokes this factory. Create the scope only for hash-based execution,
        /// and share it among transforms when the input streams contain disjoint groups.
        if (!tmp_data_on_disk)
        {
            if (!build_settings.temp_data_on_disk)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary data storage for external LIMIT BY is not provided");

            tmp_data_on_disk = build_settings.temp_data_on_disk->childScope(
                {.current_metric = CurrentMetrics::TemporaryFilesForLimitBy,
                 .bytes_compressed = ProfileEvents::ExternalLimitByCompressedBytes,
                 .bytes_uncompressed = ProfileEvents::ExternalLimitByUncompressedBytes,
                 .num_files = ProfileEvents::ExternalLimitByWritePart,
                 .spilled_to_disk_operator = "limit_by"},
                settings.temporary_files_buffer_size,
                settings.temporary_files_codec);
        }

        return std::make_shared<ExternalLimitByTransform>(
            header,
            GroupLimitParams{.key_columns = columns, .group_offset = group_offset, .group_limit_end = group_limit_end},
            external_threshold,
            tmp_data_on_disk,
            settings.min_free_disk_space,
            settings.max_block_size,
            settings.prefer_external_sort_block_bytes,
            preserve_input_order);
    };

    /// Per stream sorted.
    if (!sorted_columns_descr.empty() && pipeline.getNumStreams() > 1)
    {
        chassert(!skip_stream_merging);

        /// In parallel, run the optimized `LimitBySortedStreamTransform` per stream to reduce the amount of data.
        pipeline.addSimpleTransform(
            [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<LimitBySortedStreamTransform>(
                    header, group_limit_end, 0, sorted_columns_descr, always_read_till_end);
            });

        /// Now we need to dedup. If LIMIT 1 BY ..., that means the same key can exist in multiple streams but in the final output that key can only appear once.
        /// So we combine the streams to make sure the final output has, in this case, only 1 instance of each key. Again, since `pipeline.getNumStreams() > 1`,
        /// the streams may be combined in arbitrary order so no global order is preserved. That's okay because `pipeline.getNumStreams() > 1` implies
        /// there is no ORDER BY so we can output final keys in any order.
        pipeline.resize(1);

        pipeline.addSimpleTransform(
            [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return create_hash_transform(header);
            });
        return;
    }

    /// Now three remaining cases can happen:
    ///     1) input sorted by keys and pipeline.getNumStreams() == 1
    ///     2) skip_stream_merging with any pipeline.getNumStreams()
    ///     3) !skip_stream_merging and input not sorted by keys
    /// In all of these, no key is ever split across the streams we run LIMIT BY on, so running it on each stream
    /// independently is correct (cases 1 and 2 already satisfy this; case 3 only after the `resize(1)` below).
    /// In case 1 we already have one sorted stream, so the `resize(1)` below does nothing and we run the optimized
    /// `LimitBySortedStreamTransform`. In case 2 we read each partition through its own stream and a key never repeats across streams,
    /// so again we keep the streams as they are (the `resize(1)` is skipped) and run `LimitByTransform` on each. Case 3 is the plain case
    /// with no optimization, so we combine everything into one stream with `resize(1)` and run `LimitByTransform`.
    /// For case 3, if `pipeline.getNumStreams()` is > 1, then it means there was no `ORDER BY` earlier and we can output results in any order.
    /// If `pipeline.getNumStreams()` is 1, that likely means there was an `ORDER BY` earlier but the LIMIT BY key columns are not compatible
    /// with the ORDER BY keys, so we can only use the hash-based `LimitByTransform` but it keeps the output order the same as the input.
    if (!skip_stream_merging)
        pipeline.resize(1);

    pipeline.addSimpleTransform(
        [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            if (!sorted_columns_descr.empty())
                return std::make_shared<LimitBySortedStreamTransform>(
                    header, group_length, group_offset, sorted_columns_descr, always_read_till_end);

            return create_hash_transform(header);
        });
}

void LimitByStep::describeActions(FormatSettings & format_settings) const
{
    const String & prefix = format_settings.detail_prefix;

    format_settings.out << prefix << "Columns: ";

    if (columns.empty())
        format_settings.out << "none\n";
    else
    {
        bool first = true;
        for (const auto & column : columns)
        {
            if (!first)
                format_settings.out << ", ";
            first = false;

            format_settings.out << (format_settings.pretty ? QueryPlanFormat::formatColumnPretty(column, format_settings.pretty_names) : column);
        }
        format_settings.out << '\n';
    }

    format_settings.out << prefix << "Length " << group_length << '\n';
    format_settings.out << prefix << "Offset " << group_offset << '\n';
    if (always_read_till_end)
        format_settings.out << prefix << "Reads all data\n";
    if (skip_stream_merging)
        format_settings.out << prefix << "Skip stream merging: 1\n";
    if (preserve_input_order)
        format_settings.out << prefix << "Preserve input order: 1\n";
}

void LimitByStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
    map.add("Length", group_length);
    map.add("Offset", group_offset);
    map.add("Reads All Data", always_read_till_end);
    if (skip_stream_merging)
        map.add("Skip stream merging", true);
    if (preserve_input_order)
        map.add("Preserve input order", true);
}

void LimitByStep::serializeSettings(QueryPlanSerializationSettings & plan_settings, UInt64 version) const
{
    settings.updatePlanSettings(plan_settings, version);
}

void LimitByStep::serialize(Serialization & ctx) const
{
    if (ctx.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LIMIT_BY_ALWAYS_READ_TILL_END)
    {
        UInt8 flags = always_read_till_end ? 1 : 0;
        /// The input-order flag is part of the plan's meaning but not of the hash-table cache key.
        if (ctx.step_version >= 1 && !ctx.for_cache_key && preserve_input_order)
            flags |= 2;
        writeIntBinary(flags, ctx.out);
    }

    writeVarUInt(group_length, ctx.out);
    writeVarUInt(group_offset, ctx.out);


    writeVarUInt(columns.size(), ctx.out);
    for (const auto & column : columns)
        ctx.writeColumnName(column);
}

QueryPlanStepPtr LimitByStep::deserialize(Deserialization & ctx)
{
    bool always_read_till_end = true;
    bool preserve_input_order = false;
    if (ctx.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_LIMIT_BY_ALWAYS_READ_TILL_END)
    {
        UInt8 flags = 0;
        readIntBinary(flags, ctx.in);
        const UInt8 known_flags = ctx.step_version >= 1 ? 3 : 1;
        if (flags & ~known_flags)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "LimitByStep: unsupported flags={} in this version", static_cast<size_t>(flags));

        always_read_till_end = flags & 1;
        preserve_input_order = flags & 2;
    }

    UInt64 group_length = 0;
    UInt64 group_offset = 0;

    readVarUInt(group_length, ctx.in);
    readVarUInt(group_offset, ctx.in);

    UInt64 num_columns = 0;
    readVarUInt(num_columns, ctx.in);
    Names columns(num_columns);
    for (auto & column : columns)
        readStringBinary(column, ctx.in);

    auto step = std::make_unique<LimitByStep>(
        ctx.input_headers.front(), Settings(ctx.settings), group_length, group_offset, std::move(columns), always_read_till_end);
    if (preserve_input_order)
        step->preserveInputOrder();
    return step;
}

void LimitByStep::applyOrder(const SortDescription & sort_description)
{
    sorted_columns_descr = sort_description;
}

QueryPlanStepPtr LimitByStep::clone() const
{
    return std::make_unique<LimitByStep>(*this);
}

void registerLimitByStep(QueryPlanStepRegistry & registry);
void registerLimitByStep(QueryPlanStepRegistry & registry)
{
    const QueryPlanStepRegistry::StepVersions versions{{0, 0}, {1, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXTERNAL_LIMIT_BY}};
    registry.registerStep("LimitBy", LimitByStep::deserialize, versions);
}

}
