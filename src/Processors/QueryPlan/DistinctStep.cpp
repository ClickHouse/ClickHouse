#include <Processors/QueryPlan/DistinctStep.h>
#include <Core/Settings.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/DistinctSortedStreamTransform.h>
#include <Processors/Transforms/DistinctSortedTransform.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <Processors/Transforms/ExternalDistinctTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Common/CurrentMetrics.h>
#include <Common/JSONBuilder.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Core/SortDescription.h>

namespace ProfileEvents
{
    extern const Event ExternalDistinctWritePart;
    extern const Event ExternalDistinctCompressedBytes;
    extern const Event ExternalDistinctUncompressedBytes;
}

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForDistinct;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_rows_in_distinct;
    extern const SettingsUInt64 max_bytes_in_distinct;
    extern const SettingsOverflowMode distinct_overflow_mode;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_bytes_before_external_distinct;
    extern const SettingsDouble max_bytes_ratio_before_external_distinct;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsString temporary_files_codec;
    extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
}

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsOverflowMode distinct_overflow_mode;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_in_distinct;
    extern const QueryPlanSerializationSettingsUInt64 max_rows_in_distinct;
    extern const QueryPlanSerializationSettingsUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_distinct;
    extern const QueryPlanSerializationSettingsDouble max_bytes_ratio_before_external_distinct;
    extern const QueryPlanSerializationSettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const QueryPlanSerializationSettingsString temporary_files_codec;
    extern const QueryPlanSerializationSettingsNonZeroUInt64 temporary_files_buffer_size;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

/// The min-combination of the absolute and the ratio thresholds, the same as for external GROUP BY
/// (Aggregator::Params::getMaxBytesBeforeExternalGroupBy).
static size_t getMaxBytesBeforeExternalDistinct(size_t max_bytes_before_external_distinct, double max_bytes_ratio_before_external_distinct)
{
    std::optional<size_t> threshold;
    if (max_bytes_before_external_distinct != 0)
        threshold = max_bytes_before_external_distinct;

    if (max_bytes_ratio_before_external_distinct != 0.)
    {
        double ratio = max_bytes_ratio_before_external_distinct;
        if (ratio < 0 || ratio >= 1.)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Setting max_bytes_ratio_before_external_distinct should be >= 0 and < 1 ({})", ratio);

        auto available_system_memory = getMostStrictAvailableSystemMemory();
        if (available_system_memory.has_value())
        {
            size_t ratio_in_bytes = static_cast<size_t>(static_cast<double>(*available_system_memory) * ratio);
            if (threshold)
                threshold = std::min(threshold.value(), ratio_in_bytes);
            else
                threshold = ratio_in_bytes;

            LOG_TRACE(
                getLogger("DistinctStep"),
                "Adjusting memory limit before external DISTINCT with {} (ratio: {}, available system memory: {})",
                formatReadableSizeWithBinarySuffix(ratio_in_bytes),
                ratio,
                formatReadableSizeWithBinarySuffix(*available_system_memory));
        }
        else
        {
            LOG_TRACE(getLogger("DistinctStep"), "No system memory limits configured. Ignoring max_bytes_ratio_before_external_distinct");
        }
    }

    return threshold.value_or(0);
}

DistinctStep::Settings::Settings(const DB::Settings & settings_)
{
    set_size_limits = SizeLimits(
        settings_[Setting::max_rows_in_distinct], settings_[Setting::max_bytes_in_distinct], settings_[Setting::distinct_overflow_mode]);
    max_block_size = settings_[Setting::max_block_size];

    max_bytes_before_external_distinct = settings_[Setting::max_bytes_before_external_distinct];
    max_bytes_ratio_before_external_distinct = settings_[Setting::max_bytes_ratio_before_external_distinct];

    min_free_disk_space = settings_[Setting::min_free_disk_space_for_temporary_data];
    temporary_files_codec = settings_[Setting::temporary_files_codec];
    temporary_files_buffer_size = settings_[Setting::temporary_files_buffer_size];
}

DistinctStep::Settings::Settings(const QueryPlanSerializationSettings & settings_)
{
    set_size_limits = SizeLimits(
        settings_[QueryPlanSerializationSetting::max_rows_in_distinct],
        settings_[QueryPlanSerializationSetting::max_bytes_in_distinct],
        settings_[QueryPlanSerializationSetting::distinct_overflow_mode]);
    max_block_size = settings_[QueryPlanSerializationSetting::max_block_size];

    max_bytes_before_external_distinct = settings_[QueryPlanSerializationSetting::max_bytes_before_external_distinct];
    max_bytes_ratio_before_external_distinct = settings_[QueryPlanSerializationSetting::max_bytes_ratio_before_external_distinct];

    min_free_disk_space = settings_[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data];
    temporary_files_codec = settings_[QueryPlanSerializationSetting::temporary_files_codec];
    temporary_files_buffer_size = settings_[QueryPlanSerializationSetting::temporary_files_buffer_size];
}

void DistinctStep::Settings::updatePlanSettings(QueryPlanSerializationSettings & plan_settings) const
{
    plan_settings[QueryPlanSerializationSetting::max_rows_in_distinct] = set_size_limits.max_rows;
    plan_settings[QueryPlanSerializationSetting::max_bytes_in_distinct] = set_size_limits.max_bytes;
    plan_settings[QueryPlanSerializationSetting::distinct_overflow_mode] = set_size_limits.overflow_mode;
    plan_settings[QueryPlanSerializationSetting::max_block_size] = max_block_size;

    plan_settings[QueryPlanSerializationSetting::max_bytes_before_external_distinct] = max_bytes_before_external_distinct;
    plan_settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_distinct] = max_bytes_ratio_before_external_distinct;

    plan_settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data] = min_free_disk_space;
    plan_settings[QueryPlanSerializationSetting::temporary_files_codec] = temporary_files_codec;
    plan_settings[QueryPlanSerializationSetting::temporary_files_buffer_size] = temporary_files_buffer_size;
}

/// External DISTINCT writes sorted runs to disk, so all the key columns must support comparison
/// (a few types support only equality, which is enough for the hash-based DISTINCT). Constant columns
/// are not part of the DISTINCT key; if there are only constant columns, the result is a single row and
/// spilling makes no sense.
static bool canUseExternalDistinct(const Block & header, const Names & columns)
{
    const auto key_columns_pos = calculateDistinctKeyColumnsPositions(header, columns);
    if (key_columns_pos.empty())
        return false;

    for (const auto pos : key_columns_pos)
        if (!header.getByPosition(pos).type->isComparable())
            return false;

    return true;
}

static ITransformingStep::Traits getTraits(bool pre_distinct)
{
    const bool preserves_number_of_streams = pre_distinct;
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = !pre_distinct,
            .preserves_number_of_streams = preserves_number_of_streams,
            .preserves_sorting = preserves_number_of_streams,
        },
        {
            .preserves_number_of_rows = false,
        }
    };
}

DistinctStep::DistinctStep(
    const SharedHeader & input_header_,
    Settings settings_,
    UInt64 limit_hint_,
    const Names & columns_,
    bool pre_distinct_)
    : ITransformingStep(
            input_header_,
            input_header_,
            getTraits(pre_distinct_))
    , settings(std::move(settings_))
    , limit_hint(limit_hint_)
    , columns(columns_)
    , pre_distinct(pre_distinct_)
{
}

void DistinctStep::updateLimitHint(UInt64 hint)
{
    if (hint && limit_hint)
        /// Both limits are set - take the min
        limit_hint = std::min(hint, limit_hint);
    else
        /// Some limit is not set - take the other one
        limit_hint = std::max(hint, limit_hint);
}

void DistinctStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (!pre_distinct)
        pipeline.resize(1);

    {
        if (!distinct_sort_desc.empty())
        {
            /// pre-distinct for sorted chunks
            if (pre_distinct)
            {
                pipeline.addSimpleTransform(
                    [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
                    {
                        if (stream_type != QueryPipelineBuilder::StreamType::Main)
                            return nullptr;

                        return std::make_shared<DistinctSortedStreamTransform>(
                            header,
                            settings.set_size_limits,
                            limit_hint,
                            distinct_sort_desc,
                            columns);
                    });
                return;
            }

            /// final distinct for sorted stream (sorting inside and among chunks)
            if (pipeline.getNumStreams() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "DistinctStep with in-order expects single input");

            if (distinct_sort_desc.size() < columns.size())
            {
                if (DistinctSortedTransform::isApplicable(pipeline.getHeader(), distinct_sort_desc, columns))
                {
                    pipeline.addSimpleTransform(
                        [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
                        {
                            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                                return nullptr;

                            return std::make_shared<DistinctSortedTransform>(
                                header, distinct_sort_desc, settings.set_size_limits, limit_hint, columns);
                        });
                    return;
                }
            }
            else
            {
                pipeline.addSimpleTransform(
                    [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
                    {
                        if (stream_type != QueryPipelineBuilder::StreamType::Main)
                            return nullptr;

                        return std::make_shared<DistinctSortedStreamTransform>(
                            header, settings.set_size_limits, limit_hint, distinct_sort_desc, columns);
                    });
                return;
            }
        }
    }

    const size_t external_threshold = getMaxBytesBeforeExternalDistinct(
        settings.max_bytes_before_external_distinct, settings.max_bytes_ratio_before_external_distinct);

    /// The final DISTINCT can spill to disk. The preliminary DISTINCT never spills: it is best-effort, so
    /// under memory pressure it just clears its set and lets the duplicates through to the final DISTINCT
    /// (see the pass-through threshold below).
    if (!pre_distinct && external_threshold && canUseExternalDistinct(*pipeline.getSharedHeader(), columns))
    {
        TemporaryDataOnDiskScopePtr tmp_data_on_disk;
        if (auto tmp_data = Context::getGlobalContextInstance()->getSharedTempDataOnDisk())
            tmp_data_on_disk = tmp_data->childScope(
                {.current_metric = CurrentMetrics::TemporaryFilesForDistinct,
                 .bytes_compressed = ProfileEvents::ExternalDistinctCompressedBytes,
                 .bytes_uncompressed = ProfileEvents::ExternalDistinctUncompressedBytes,
                 .num_files = ProfileEvents::ExternalDistinctWritePart},
                settings.temporary_files_buffer_size,
                settings.temporary_files_codec);

        if (tmp_data_on_disk)
        {
            pipeline.addSimpleTransform(
                [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
                {
                    if (stream_type != QueryPipelineBuilder::StreamType::Main)
                        return nullptr;

                    return std::make_shared<ExternalDistinctTransform>(
                        header,
                        settings.set_size_limits,
                        limit_hint,
                        columns,
                        external_threshold,
                        tmp_data_on_disk,
                        settings.min_free_disk_space,
                        settings.max_block_size);
                });
            return;
        }

        /// External DISTINCT is armed by default via the ratio setting, and that default must not break
        /// environments without temporary data storage. Only an explicit request for external DISTINCT
        /// is an error here; otherwise fall through to the in-memory transform.
        if (settings.max_bytes_before_external_distinct)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary data storage for external DISTINCT is not provided");
    }

    if (external_threshold && !pre_distinct && settings.max_bytes_before_external_distinct)
        LOG_DEBUG(
            getLogger("DistinctStep"),
            "External DISTINCT is not used: the DISTINCT columns do not support it "
            "(all key columns must be comparable and at least one must be non-constant)");

    /// A preliminary DISTINCT may shed its set under memory pressure only when the final DISTINCT is
    /// able to spill for the same columns (otherwise shedding just moves the memory to an in-memory
    /// final DISTINCT).
    const UInt64 pass_through_threshold
        = (pre_distinct && canUseExternalDistinct(*pipeline.getSharedHeader(), columns)) ? external_threshold : 0;

    pipeline.addSimpleTransform(
        [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
        {
            if (stream_type != QueryPipelineBuilder::StreamType::Main)
                return nullptr;

            return std::make_shared<DistinctTransform>(header, settings.set_size_limits, limit_hint, columns, pass_through_threshold);
        });
}

void DistinctStep::describeActions(FormatSettings & format_settings) const
{
    const String & prefix = format_settings.detail_prefix;
    format_settings.out << prefix << "Columns: ";

    if (columns.empty())
        format_settings.out << "none";
    else
    {
        bool first = true;
        for (const auto & column : columns)
        {
            if (!first)
                format_settings.out << ", ";
            first = false;

            format_settings.out
                << (format_settings.pretty ? QueryPlanFormat::formatColumnPretty(column, format_settings.pretty_names) : column);
        }
    }

    format_settings.out << '\n';
}

void DistinctStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto columns_array = std::make_unique<JSONBuilder::JSONArray>();
    for (const auto & column : columns)
        columns_array->add(column);

    map.add("Columns", std::move(columns_array));
}

void DistinctStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

void DistinctStep::serializeSettings(QueryPlanSerializationSettings & plan_settings) const
{
    settings.updatePlanSettings(plan_settings);
}

void DistinctStep::serialize(Serialization & ctx) const
{
    /// Let's not serialize limit_hint.
    /// Ideally, we can get if from a query plan optimization on the follower.

    writeVarUInt(columns.size(), ctx.out);
    for (const auto & column : columns)
        writeStringBinary(column, ctx.out);
}

QueryPlanStepPtr DistinctStep::deserialize(Deserialization & ctx, bool pre_distinct_)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "DistinctStep must have one input stream");

    size_t columns_size = 0;
    readVarUInt(columns_size, ctx.in);
    Names column_names(columns_size);
    for (size_t i = 0; i < columns_size; ++i)
        readStringBinary(column_names[i], ctx.in);

    return std::make_unique<DistinctStep>(
        ctx.input_headers.front(), Settings(ctx.settings), 0, column_names, pre_distinct_);
}

QueryPlanStepPtr DistinctStep::deserializeNormal(Deserialization & ctx)
{
    return DistinctStep::deserialize(ctx, false);
}
QueryPlanStepPtr DistinctStep::deserializePre(Deserialization & ctx)
{
    return DistinctStep::deserialize(ctx, true);
}

void registerDistinctStep(QueryPlanStepRegistry & registry);
void registerDistinctStep(QueryPlanStepRegistry & registry)
{
    /// Preliminary distinct probably can be a query plan optimization.
    /// It's easier to serialize it using different names, so that pre-distinct can be potentially removed later.
    registry.registerStep("Distinct", DistinctStep::deserializeNormal);
    registry.registerStep("PreDistinct", DistinctStep::deserializePre);
}

}
