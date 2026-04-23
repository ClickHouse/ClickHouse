#include <Storages/MergeTree/Streaming/CommitOrderStrategy.h>

#include <Storages/MergeTree/MergeTreeIndexGranularity.h>

#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/MemoryTrackerUtils.h>
#include <Common/logger_useful.h>
#include <Core/SortDescription.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Processors/Transforms/MergeSortingTransform.h>
#include <Processors/Transforms/PartialSortingTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/defines.h>

namespace DB
{

namespace Setting
{
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 prefer_external_sort_block_bytes;
    extern const SettingsUInt64 max_bytes_before_remerge_sort;
    extern const SettingsFloat remerge_sort_lowered_memory_bytes_ratio;
    extern const SettingsUInt64 max_bytes_before_external_sort;
    extern const SettingsFloat max_bytes_ratio_before_external_sort;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_STREAM;
}

namespace
{

/// Copy of the helper from SortingStep.cpp.
size_t getMaxBytesInQueryBeforeExternalSort(double ratio)
{
    if (ratio == 0.)
        return 0;

    if (ratio < 0 || ratio >= 1.)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Setting max_bytes_ratio_before_external_sort should be >= 0 and < 1 ({})", ratio);

    auto available_system_memory = getMostStrictAvailableSystemMemory();
    if (!available_system_memory.has_value())
        return 0;

    return static_cast<size_t>(static_cast<double>(*available_system_memory) * ratio);
}

bool sortingKeyStartsWithBlockNumberAndOffset(const Names & sorting_key_column_names)
{
    return sorting_key_column_names.size() >= 2
        && sorting_key_column_names[0] == BlockNumberColumn::name
        && sorting_key_column_names[1] == BlockOffsetColumn::name;
}

SortDescription commitOrderSortDescription()
{
    SortDescription description;
    description.push_back(SortColumnDescription(BlockNumberColumn::name, /*direction=*/1, /*nulls_direction=*/1));
    description.push_back(SortColumnDescription(BlockOffsetColumn::name, /*direction=*/1, /*nulls_direction=*/1));
    return description;
}

}

CommitOrderReadStrategy chooseCommitOrderReadStrategy(
    const RangesInDataPart & ranges,
    const StorageMetadataPtr & metadata)
{
    const auto & part = ranges.data_part;

    /// 1 If part is already a projection try read it directly.
    if (part->isProjectionPart())
    {
        const std::string & projection_name = part->name;
        if (!metadata->projections.has(projection_name))
            throw Exception(ErrorCodes::ILLEGAL_STREAM, "Trying to read projection part '{}' which projection does not exist in metadata", projection_name);

        const ProjectionDescription & projection_description = metadata->projections.get(projection_name);
        if (!projection_description.with_block_number || !projection_description.with_block_offset)
            throw Exception(ErrorCodes::ILLEGAL_STREAM, "Trying to read in commit order projection part '{}' which is not a commit order projection", projection_name);

        /// Read this part as is
        return {CommitOrderReadStrategy::Native, ranges};
    }

    /// 2 If partial read was requested try read it directly.
    const size_t granule_count = part->index_granularity->getMarksCountWithoutFinal();
    const bool is_partial_read = !(ranges.ranges.size() == 1 && ranges.ranges.front().begin == 0 && ranges.ranges.front().end == granule_count);
    if (is_partial_read)
    {
        if (!sortingKeyStartsWithBlockNumberAndOffset(metadata->sorting_key.column_names))
            throw Exception(ErrorCodes::ILLEGAL_STREAM, "Partial-range read of a part whose sort key is not (_block_number, _block_offset) cannot produce commit-order rows");

        return {CommitOrderReadStrategy::Native, ranges};
    }

    /// 3 Try to find commit-order projection to read from.
    for (const auto & projection : metadata->projections)
    {
        if (!projection.with_block_number || !projection.with_block_offset)
            continue;

        if (!part->getProjectionParts().contains(projection.name))
            continue;

        const auto projection_part_to_read = part->getProjectionParts().at(projection.name);
        return {CommitOrderReadStrategy::Native, RangesInDataPart(projection_part_to_read, part)};
    }

    /// 4 If part contain single block - disk layout already matches commit-order.
    if (part->isZeroLevel())
        return {CommitOrderReadStrategy::Native, ranges};

    /// 5 If part was sorted in commit-order try read it directly.
    if (sortingKeyStartsWithBlockNumberAndOffset(metadata->sorting_key.column_names))
        return {CommitOrderReadStrategy::Native, ranges};

    /// 6 Fallback to commit-order sorting.
    return {CommitOrderReadStrategy::Sort, ranges};
}

Pipe createCommitOrderReadStream(
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    AlterConversionsPtr alter_conversions,
    Names columns_to_read,
    const CommitOrderReadStrategy & strategy,
    ContextPtr context)
{
    RangesInDataPart ranges = strategy.ranges_to_read;
    MarkRanges mark_ranges = ranges.ranges;

    switch (strategy.kind)
    {
        case CommitOrderReadStrategy::Native:
        {
            return createMergeTreeSequentialSource(
                MergeTreeSequentialSourceType::Merge,
                storage,
                storage_snapshot,
                std::move(ranges),
                std::move(alter_conversions),
                /*merged_part_offsets=*/ nullptr,
                std::move(columns_to_read),
                std::move(mark_ranges),
                /*filtered_rows_count=*/ nullptr,
                /*apply_deleted_mask=*/ true,
                /*read_with_direct_io=*/ false,
                /*prefetch=*/ false);
        }

        case CommitOrderReadStrategy::Sort:
        {
            auto pipe = createMergeTreeSequentialSource(
                MergeTreeSequentialSourceType::Merge,
                storage,
                storage_snapshot,
                std::move(ranges),
                std::move(alter_conversions),
                /*merged_part_offsets=*/ nullptr,
                std::move(columns_to_read),
                std::move(mark_ranges),
                /*filtered_rows_count=*/ nullptr,
                /*apply_deleted_mask=*/ true,
                /*read_with_direct_io=*/ false,
                /*prefetch=*/ false);

            const auto & settings = context->getSettingsRef();
            const auto sort_description = commitOrderSortDescription();

            pipe.addSimpleTransform([&](const SharedHeader & header) -> ProcessorPtr
            {
                return std::make_shared<PartialSortingTransform>(header, sort_description);
            });

            const size_t max_block_bytes = settings[Setting::prefer_external_sort_block_bytes];
            const UInt64 max_block_size = settings[Setting::max_block_size].value;
            const size_t max_bytes_before_remerge = settings[Setting::max_bytes_before_remerge_sort];
            const double remerge_ratio = static_cast<double>(settings[Setting::remerge_sort_lowered_memory_bytes_ratio]);
            const size_t max_bytes_in_block_before_external_sort = settings[Setting::max_bytes_before_external_sort];
            const size_t max_bytes_in_query_before_external_sort = getMaxBytesInQueryBeforeExternalSort(static_cast<double>(settings[Setting::max_bytes_ratio_before_external_sort]));
            const size_t min_free_disk_space = settings[Setting::min_free_disk_space_for_temporary_data];

            auto tmp_data = context->getTempDataOnDisk();

            pipe.addSimpleTransform([&](const SharedHeader & header) -> ProcessorPtr
            {
                return std::make_shared<MergeSortingTransform>(
                    header,
                    sort_description,
                    max_block_size,
                    max_block_bytes,
                    /*limit_=*/ 0,
                    /*increase_sort_description_compile_attempts=*/ false,
                    max_bytes_before_remerge,
                    remerge_ratio,
                    max_bytes_in_block_before_external_sort,
                    max_bytes_in_query_before_external_sort,
                    tmp_data,
                    min_free_disk_space);
            });

            return pipe;
        }
    }

    UNREACHABLE();
}

}
