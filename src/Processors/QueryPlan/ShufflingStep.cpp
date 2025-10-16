#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Processors/Merges/MergingShuffledTransform.h>
#include <Processors/QueryPlan/ShufflingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MergeShufflingTransform.h>
#include <Processors/Transforms/PartialShufflingTransform.h>
#include <Processors/QueryPlan/BufferChunksTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/JSONBuilder.h>
#include <Core/Settings.h>

#include <Processors/ResizeProcessor.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>

#include <memory>
#include <optional>

#include <Common/logger_useful.h>

namespace CurrentMetrics
{
    extern const Metric TemporaryFilesForSort;
}

namespace DB
{
namespace Setting
{
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_bytes_before_external_sort;
    extern const SettingsDouble max_bytes_ratio_before_external_sort;
    extern const SettingsUInt64 max_bytes_before_remerge_sort;
    extern const SettingsUInt64 max_bytes_to_sort;
    extern const SettingsUInt64 max_rows_to_sort;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsUInt64 prefer_external_sort_block_bytes;
    extern const SettingsBool read_in_order_use_buffering;
    extern const SettingsFloat remerge_sort_lowered_memory_bytes_ratio;
    extern const SettingsOverflowMode sort_overflow_mode;
}

namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_block_size;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_sort;
    extern const QueryPlanSerializationSettingsDouble max_bytes_ratio_before_external_sort;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_remerge_sort;
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_to_sort;
    extern const QueryPlanSerializationSettingsUInt64 max_rows_to_sort;
    extern const QueryPlanSerializationSettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const QueryPlanSerializationSettingsUInt64 prefer_external_sort_block_bytes;
    extern const QueryPlanSerializationSettingsFloat remerge_sort_lowered_memory_bytes_ratio;
    extern const QueryPlanSerializationSettingsOverflowMode sort_overflow_mode;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int LIMIT_EXCEEDED;
}

size_t getMaxBytesInQueryBeforeExternalShuffle(double max_bytes_ratio_before_external_sort)
{

    if (max_bytes_ratio_before_external_sort == 0.)
        return 0;

    double ratio = max_bytes_ratio_before_external_sort;
    if (ratio < 0 || ratio >= 1.)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting max_bytes_ratio_before_external_sort should be >= 0 and < 1 ({})", ratio);

    auto available_system_memory = getMostStrictAvailableSystemMemory();
    if (available_system_memory.has_value())
    {
        size_t ratio_in_bytes = static_cast<size_t>(*available_system_memory * ratio);

        LOG_TRACE(getLogger("ShufflingStep"), "Adjusting memory limit before external sort with");
        //  {} (ratio: {}, available system memory: {})",
            // formatReadableSizeWithBinarySuffix(ratio_in_bytes),
            // ratio,
            // formatReadableSizeWithBinarySuffix(*available_system_memory)
            // );

        return ratio_in_bytes;
    }
    else
    {
        LOG_TRACE(getLogger("ShufflingStep"), "No system memory limits configured. Ignoring max_bytes_ratio_before_external_sort");
        return 0;
    }
}

ShufflingStep::Settings::Settings(const DB::Settings & settings)
{
    max_block_size = settings[Setting::max_block_size];

    size_limits = SizeLimits(settings[Setting::max_rows_to_sort], settings[Setting::max_bytes_to_sort], settings[Setting::sort_overflow_mode]);
    LOG_TRACE(getLogger("ShufflingStep"), "max_block_size {}, size_limits.max_rows: {}", max_block_size, size_limits.max_rows);


    max_bytes_before_remerge = settings[Setting::max_bytes_before_remerge_sort];
    remerge_lowered_memory_bytes_ratio = settings[Setting::remerge_sort_lowered_memory_bytes_ratio];

    max_bytes_ratio_before_external_sort = settings[Setting::max_bytes_ratio_before_external_sort];
    max_bytes_in_block_before_external_sort = settings[Setting::max_bytes_before_external_sort];
    max_bytes_in_query_before_external_sort = getMaxBytesInQueryBeforeExternalShuffle(settings[Setting::max_bytes_ratio_before_external_sort]);

    min_free_disk_space = settings[Setting::min_free_disk_space_for_temporary_data];
    max_block_bytes = settings[Setting::prefer_external_sort_block_bytes];
    read_in_order_use_buffering = settings[Setting::read_in_order_use_buffering];
}

ShufflingStep::Settings::Settings(size_t max_block_size_)
{
    max_block_size = max_block_size_;
    // LOG_TRACE(getLogger("ShufflingStep"), "max_block_size: {}", max_block_size);

}

ShufflingStep::Settings::Settings(const QueryPlanSerializationSettings & settings)
{
    LOG_TRACE(getLogger("ShufflingStep"), "Settings::Settings(const QueryPlanSerializationSettings & settings)");

    max_block_size = settings[QueryPlanSerializationSetting::max_block_size];
    size_limits = SizeLimits(settings[QueryPlanSerializationSetting::max_rows_to_sort], settings[QueryPlanSerializationSetting::max_bytes_to_sort], settings[QueryPlanSerializationSetting::sort_overflow_mode]);
    LOG_TRACE(getLogger("ShufflingStep"), "max_block_size {}, size_limits.max_rows: {}", max_block_size, size_limits.max_rows);
    max_bytes_before_remerge = settings[QueryPlanSerializationSetting::max_bytes_before_remerge_sort];
    remerge_lowered_memory_bytes_ratio = settings[QueryPlanSerializationSetting::remerge_sort_lowered_memory_bytes_ratio];

    max_bytes_ratio_before_external_sort = settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_sort];
    max_bytes_in_block_before_external_sort = settings[QueryPlanSerializationSetting::max_bytes_before_external_sort];
    max_bytes_in_query_before_external_sort = getMaxBytesInQueryBeforeExternalShuffle(settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_sort]);

    min_free_disk_space = settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data];
    max_block_bytes = settings[QueryPlanSerializationSetting::prefer_external_sort_block_bytes];
    read_in_order_use_buffering = false; //settings.read_in_order_use_buffering;
}

void ShufflingStep::Settings::updatePlanSettings(QueryPlanSerializationSettings & settings) const
{
    LOG_TRACE(getLogger("ShufflingStep"), "updatePlanSettings");

    settings[QueryPlanSerializationSetting::max_block_size] = max_block_size;
    settings[QueryPlanSerializationSetting::max_rows_to_sort] = size_limits.max_rows;
    settings[QueryPlanSerializationSetting::max_bytes_to_sort] = size_limits.max_bytes;
    settings[QueryPlanSerializationSetting::sort_overflow_mode] = size_limits.overflow_mode;

    settings[QueryPlanSerializationSetting::max_bytes_before_remerge_sort] = max_bytes_before_remerge;
    settings[QueryPlanSerializationSetting::remerge_sort_lowered_memory_bytes_ratio] = remerge_lowered_memory_bytes_ratio;
    settings[QueryPlanSerializationSetting::max_bytes_before_external_sort] = max_bytes_in_block_before_external_sort;
    settings[QueryPlanSerializationSetting::max_bytes_ratio_before_external_sort] = max_bytes_ratio_before_external_sort;
    settings[QueryPlanSerializationSetting::min_free_disk_space_for_temporary_data] = min_free_disk_space;
    settings[QueryPlanSerializationSetting::prefer_external_sort_block_bytes] = max_block_bytes;
}

static ITransformingStep::Traits getTraits(size_t limit)
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

ShufflingStep::ShufflingStep(
    const SharedHeader & input_header, UInt64 limit_, const Settings & settings_)
    : ITransformingStep(input_header, input_header, getTraits(limit_))
    , type(Type::Full)
    , limit(limit_)
    , sort_settings(settings_)
{

    LOG_TRACE(getLogger("ShufflingStep"), "ShufflingStep type(Type::Full)");
}

ShufflingStep::ShufflingStep(
    const SharedHeader & input_header,
    size_t max_block_size_,
    UInt64 limit_,
    bool)
    : ITransformingStep(input_header, input_header, getTraits(limit_))
    , type(Type::MergingShuffled)
    , limit(limit_)
    // , always_read_till_end(always_read_till_end_)
    , sort_settings(max_block_size_)
{
    LOG_TRACE(getLogger("ShufflingStep"), "ShufflingStep type(Type::MergingShuffled), max_block_size: {}", max_block_size_);
}

void ShufflingStep::updateOutputHeader()
{
    output_header = input_headers.front();
}

// void ShufflingStep::updateLimit(size_t limit_)
// {
//     LOG_TRACE(getLogger("ShufflingStep"), "updateLimit");

//     if (limit_ && (limit == 0 || limit_ < limit))
//     {
//         limit = limit_;
//         transform_traits.preserves_number_of_rows = false;
//     }
// }

void ShufflingStep::mergingShuffled(QueryPipelineBuilder & pipeline, const UInt64)
{
    LOG_TRACE(getLogger("ShufflingStep"), "mergingShuffled");

    /// If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1)
    {
        // TODO
        // if (use_buffering && sort_settings.read_in_order_use_buffering)
        // {
        //     pipeline.addSimpleTransform([&](const SharedHeader & header)
        //     {
        //         return std::make_shared<BufferChunksTransform>(header, sort_settings.max_block_size, sort_settings.max_block_bytes, limit_);
        //     });
        // }

        // TODO: finish MergingShuffledTransform
    //     auto transform = std::make_shared<MergingShuffledTransform>(
    //         pipeline.getSharedHeader(),
    //         pipeline.getNumStreams(),
    //         sort_settings.max_block_size,
    //         /*max_block_size_bytes=*/0,
    //         limit_,
    //         always_read_till_end,
    //         nullptr,
    //         false,
    //         apply_virtual_row_conversions);

    //     pipeline.addTransform(std::move(transform));
    }
}

void ShufflingStep::mergeShuffling(
    QueryPipelineBuilder & pipeline, const Settings & sort_settings, UInt64 limit_)
{
    LOG_TRACE(getLogger("ShufflingStep"), "ShufflingStep::mergeShuffling");

    bool increase_sort_description_compile_attempts = true;

    TemporaryDataOnDiskScopePtr tmp_data_on_disk = nullptr;
    if (auto data = Context::getGlobalContextInstance()->getSharedTempDataOnDisk())
        tmp_data_on_disk = data->childScope(CurrentMetrics::TemporaryFilesForSort);

    if (sort_settings.max_bytes_in_block_before_external_sort && tmp_data_on_disk == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary data storage for external shuffling is not provided");

    pipeline.addSimpleTransform(
        [&, increase_sort_description_compile_attempts](
            const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) mutable -> ProcessorPtr
        {
            if (stream_type == QueryPipelineBuilder::StreamType::Totals)
                return nullptr;

            // For multiple FinishShufflingTransform we need to count identical comparators only once per QueryPlan.
            // To property support min_count_to_compile_sort_description.
            bool increase_sort_description_compile_attempts_current = increase_sort_description_compile_attempts;

            if (increase_sort_description_compile_attempts)
                increase_sort_description_compile_attempts = false;

            return std::make_shared<MergeShufflingTransform>(
                header,
                sort_settings.max_block_size,
                sort_settings.max_block_bytes,
                limit_,
                increase_sort_description_compile_attempts_current,
                sort_settings.max_bytes_before_remerge / pipeline.getNumStreams(),
                sort_settings.remerge_lowered_memory_bytes_ratio,
                sort_settings.max_bytes_in_block_before_external_sort / pipeline.getNumStreams(),
                sort_settings.max_bytes_in_query_before_external_sort,
                tmp_data_on_disk,
                sort_settings.min_free_disk_space);
        });
}

void ShufflingStep::fullShuffleStreams(
    QueryPipelineBuilder & pipeline,
    const Settings & sort_settings,
    const UInt64 limit_,
    const bool skip_partial_shuffle)
{
    LOG_TRACE(getLogger("ShufflingStep"), "ShufflingStep::fullShuffleStreams, {}", sort_settings.max_block_size);

    if (!skip_partial_shuffle || limit_)
    {
        pipeline.addSimpleTransform(
            [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<PartialShufflingTransform>(header, limit_);
            });

        StreamLocalLimits limits;
        limits.mode = LimitsMode::LIMITS_CURRENT;
        limits.size_limits = sort_settings.size_limits;

        pipeline.addSimpleTransform(
            [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                return std::make_shared<LimitsCheckingTransform>(header, limits);
            });
    }

    mergeShuffling(pipeline, sort_settings, limit_);
}

void ShufflingStep::fullShuffle(
    QueryPipelineBuilder & pipeline, const UInt64 limit_, const bool skip_partial_shuffle)
{
    LOG_TRACE(getLogger("ShufflingStep"), "ShufflingStep::fullShuffle, pipeline.getNumStreams(): {}", pipeline.getNumStreams());

    // scatterByPartitionIfNeeded(pipeline);

    fullShuffleStreams(pipeline, sort_settings, limit_, skip_partial_shuffle);

    // TODO
    // If there are several streams, then we merge them into one
    if (pipeline.getNumStreams() > 1 )
    // && (partition_by_description.empty() || pipeline.getNumThreads() == 1))
    {
        auto transform = std::make_shared<MergingShuffledTransform>(
            pipeline.getSharedHeader(),
            pipeline.getNumStreams(),
            sort_settings.max_block_size,
            /*max_block_size_bytes=*/0,
            limit_,
            always_read_till_end);

        pipeline.addTransform(std::move(transform));
    }
}

void ShufflingStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// We consider that a caller has more information what type of shuffling to apply.
    /// The type depends on constructor used to create shuffling step.
    /// So we'll try to infer shuffling to use only in case of Full shuffling

    // LOG_TRACE(getLogger("ShufflingStep"), "transformPipeline, mbs: {}", max_block_size);

    if (type == Type::MergingShuffled)
    {
        mergingShuffled(pipeline, limit);
        return;
    }

    fullShuffle(pipeline, limit);
}

void ShufflingStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');

    if (limit)
        settings.out << prefix << "Limit " << limit << '\n';
}

void ShufflingStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (limit)
        map.add("Limit", limit);
}

void ShufflingStep::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    sort_settings.updatePlanSettings(settings);
}

void ShufflingStep::serialize(Serialization &) const
{
    if (type != Type::Full)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of ShufflingStep is implemented only for Full shuffling");

    /// Do not serialize type here; Later we can use different names if needed.\

    /// Do not serialize limit for now; it is expected to be pushed down from plan optimization.
}

std::unique_ptr<IQueryPlanStep> ShufflingStep::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 1)
        throw Exception(ErrorCodes::INCORRECT_DATA, "ShufflingStep must have one input stream");

    ShufflingStep::Settings sort_settings(ctx.settings);

    return std::make_unique<ShufflingStep>(
        ctx.input_headers.front(), 0, std::move(sort_settings));
}

void registerShufflingStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Shuffling", ShufflingStep::deserialize);
}

}
