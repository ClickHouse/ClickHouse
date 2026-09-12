#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/ExternalLimitByTransform.h>
#include <Processors/Transforms/LimitByTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <IO/Operators.h>
#include <Common/CurrentMetrics.h>
#include <Common/JSONBuilder.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <limits>

namespace ProfileEvents
{
    extern const Event ExternalLimitByCompressedBytes;
    extern const Event ExternalLimitByUncompressedBytes;
    extern const Event ExternalLimitByWritePart;
}

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForLimitBy;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_block_size;
    extern const SettingsUInt64 max_bytes_before_external_limit_by;
    extern const SettingsDouble max_bytes_ratio_before_external_limit_by;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsString temporary_files_codec;
    extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
}

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_limit_by;
    extern const QueryPlanSerializationSettingsDouble max_bytes_ratio_before_external_limit_by;
    extern const QueryPlanSerializationSettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const QueryPlanSerializationSettingsString temporary_files_codec;
    extern const QueryPlanSerializationSettingsNonZeroUInt64 temporary_files_buffer_size;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// The share of the available memory that a `LIMIT BY` may hold before it spills, in bytes. This is
/// what makes the spilling automatic: it applies without anyone setting an absolute threshold.
static size_t getMaxBytesInQueryBeforeExternalLimitBy(double max_bytes_ratio_before_external_limit_by)
{
    if (max_bytes_ratio_before_external_limit_by == 0.)
        return 0;

    const double ratio = max_bytes_ratio_before_external_limit_by;
    if (ratio < 0 || ratio >= 1.)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Setting max_bytes_ratio_before_external_limit_by should be >= 0 and < 1 ({:.3f})", ratio);

    auto available_system_memory = getMostStrictAvailableSystemMemory();
    if (!available_system_memory.has_value())
    {
        LOG_TRACE(getLogger("LimitByStep"), "No system memory limits configured. Ignoring max_bytes_ratio_before_external_limit_by");
        return 0;
    }

    const size_t ratio_in_bytes = static_cast<size_t>(static_cast<double>(*available_system_memory) * ratio);

    LOG_TRACE(
        getLogger("LimitByStep"),
        "Adjusting memory limit before external LIMIT BY with {} (ratio: {:.3f}, available system memory: {})",
        formatReadableSizeWithBinarySuffix(ratio_in_bytes),
        ratio,
        formatReadableSizeWithBinarySuffix(*available_system_memory));

    return ratio_in_bytes;
}

LimitByStep::ExternalSettings::ExternalSettings(const Settings & settings)
    : max_bytes_in_state_before_external_limit_by(settings[Setting::max_bytes_before_external_limit_by])
    , max_bytes_in_query_before_external_limit_by(
          getMaxBytesInQueryBeforeExternalLimitBy(settings[Setting::max_bytes_ratio_before_external_limit_by]))
    , max_bytes_ratio_before_external_limit_by(settings[Setting::max_bytes_ratio_before_external_limit_by])
    , max_block_size(settings[Setting::max_block_size])
    , min_free_disk_space(settings[Setting::min_free_disk_space_for_temporary_data])
    , temporary_files_codec(settings[Setting::temporary_files_codec])
    , temporary_files_buffer_size(settings[Setting::temporary_files_buffer_size])
{
}

LimitByStep::ExternalSettings::ExternalSettings(const QueryPlanSerializationSettings & settings)
    : max_bytes_in_state_before_external_limit_by(settings[QueryPlanSerializationSetting::max_bytes_before_external_limit_by])
    , max_bytes_in_query_before_external_limit_by(getMaxBytesInQueryBeforeExternalLimitBy(
          settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_limit_by]))
    , max_bytes_ratio_before_external_limit_by(settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_limit_by])
    , max_block_size(settings[QueryPlanSerializationSetting::max_block_size])
    , min_free_disk_space(settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data])
    , temporary_files_codec(settings[QueryPlanSerializationSetting::temporary_files_codec])
    , temporary_files_buffer_size(settings[QueryPlanSerializationSetting::temporary_files_buffer_size])
{
}

void LimitByStep::ExternalSettings::updatePlanSettings(QueryPlanSerializationSettings & settings) const
{
    settings[QueryPlanSerializationSetting::max_block_size] = max_block_size;
    settings[QueryPlanSerializationSetting::max_bytes_before_external_limit_by] = max_bytes_in_state_before_external_limit_by;
    settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_limit_by] = max_bytes_ratio_before_external_limit_by;
    settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data] = min_free_disk_space;
    settings[QueryPlanSerializationSetting::temporary_files_codec] = temporary_files_codec;
    settings[QueryPlanSerializationSetting::temporary_files_buffer_size] = temporary_files_buffer_size;
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

LimitByStep::LimitByStep(
    const SharedHeader & input_header_,
    size_t group_length_, size_t group_offset_, Names columns_,
    ExternalSettings external_settings_)
    : ITransformingStep(input_header_, input_header_, getTraits())
    , group_length(group_length_)
    , group_offset(group_offset_)
    , columns(std::move(columns_))
    , external_settings(std::move(external_settings_))
{
}

ProcessorPtr LimitByStep::makeHashTransform(const SharedHeader & header, size_t length, size_t offset, bool can_spill) const
{
    if (!can_spill)
        return std::make_shared<LimitByTransform>(header, length, offset, columns);

    TemporaryDataOnDiskScopePtr tmp_data_on_disk;
    if (auto data = Context::getGlobalContextInstance()->getSharedTempDataOnDisk())
        tmp_data_on_disk = data->childScope(
            {.current_metric = CurrentMetrics::TemporaryFilesForLimitBy,
             .bytes_compressed = ProfileEvents::ExternalLimitByCompressedBytes,
             .bytes_uncompressed = ProfileEvents::ExternalLimitByUncompressedBytes,
             .num_files = ProfileEvents::ExternalLimitByWritePart},
            external_settings.temporary_files_buffer_size,
            external_settings.temporary_files_codec);

    /// Without temporary storage there is nothing to spill into, so keep everything in memory rather
    /// than failing a query that the in-memory implementation can still answer.
    if (!tmp_data_on_disk)
        return std::make_shared<LimitByTransform>(header, length, offset, columns);

    return std::make_shared<ExternalLimitByTransform>(
        header,
        length,
        offset,
        columns,
        external_settings.max_bytes_in_state_before_external_limit_by,
        external_settings.max_bytes_in_query_before_external_limit_by,
        external_settings.max_block_size,
        std::move(tmp_data_on_disk),
        external_settings.min_free_disk_space);
}


void LimitByStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
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

    /// A spill emits the rows it did not get to before the spill in grouping key order rather than in
    /// input order, so it is only usable where the output order is free anyway. More than one stream
    /// here means no upstream `ORDER BY` established an order, which is exactly that case. This is read
    /// before the `resize(1)` calls below, which would otherwise hide it.
    const bool can_spill = external_settings.isEnabled() && pipeline.getNumStreams() > 1;

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

                return std::make_shared<LimitBySortedStreamTransform>(header, group_length, group_offset, sorted_columns_descr);
            });
        return;
    }

    /// Per stream sorted.
    if (!sorted_columns_descr.empty() && pipeline.getNumStreams() > 1)
    {
        chassert(!skip_stream_merging);

        /// In parallel, run the optimized `LimitBySortedStreamTransform` per stream to reduce the amount of data.
        const UInt64 prefilter_length = (group_offset > std::numeric_limits<UInt64>::max() - group_length)
            ? std::numeric_limits<UInt64>::max()
            : group_length + group_offset;

        pipeline.addSimpleTransform(
            [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<LimitBySortedStreamTransform>(header, prefilter_length, 0, sorted_columns_descr);
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

                return makeHashTransform(header, group_length, group_offset, can_spill);
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
                return std::make_shared<LimitBySortedStreamTransform>(header, group_length, group_offset, sorted_columns_descr);

            return makeHashTransform(header, group_length, group_offset, can_spill);
        });
}

void LimitByStep::describeActions(FormatSettings & settings) const
{
    const String & prefix = settings.detail_prefix;

    settings.out << prefix << "Columns: ";

    if (columns.empty())
        settings.out << "none\n";
    else
    {
        bool first = true;
        for (const auto & column : columns)
        {
            if (!first)
                settings.out << ", ";
            first = false;

            settings.out << (settings.pretty ? QueryPlanFormat::formatColumnPretty(column, settings.pretty_names) : column);
        }
        settings.out << '\n';
    }

    settings.out << prefix << "Length " << group_length << '\n';
    settings.out << prefix << "Offset " << group_offset << '\n';
    if (skip_stream_merging)
        settings.out << prefix << "Skip stream merging: 1\n";
}

void LimitByStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
    map.add("Length", group_length);
    map.add("Offset", group_offset);
    if (skip_stream_merging)
        map.add("Skip stream merging", true);
}

void LimitByStep::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 /*version*/) const
{
    external_settings.updatePlanSettings(settings);
}

void LimitByStep::serialize(Serialization & ctx) const
{
    writeVarUInt(group_length, ctx.out);
    writeVarUInt(group_offset, ctx.out);


    writeVarUInt(columns.size(), ctx.out);
    for (const auto & column : columns)
        writeStringBinary(column, ctx.out);
}

QueryPlanStepPtr LimitByStep::deserialize(Deserialization & ctx)
{
    UInt64 group_length = 0;
    UInt64 group_offset = 0;

    readVarUInt(group_length, ctx.in);
    readVarUInt(group_offset, ctx.in);

    UInt64 num_columns = 0;
    readVarUInt(num_columns, ctx.in);
    Names columns(num_columns);
    for (auto & column : columns)
        readStringBinary(column, ctx.in);

    return std::make_unique<LimitByStep>(
        ctx.input_headers.front(), group_length, group_offset, std::move(columns), LimitByStep::ExternalSettings(ctx.settings));
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
    registry.registerStep("LimitBy", LimitByStep::deserialize);
}

}
