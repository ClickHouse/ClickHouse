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
#include <Storages/MergeTree/MergeTreeReadPoolInOrder.h>
#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>
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
    extern const SettingsUInt64 preferred_block_size_bytes;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_STREAM;
}

namespace
{

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

CommitOrderReadStrategy::Decision CommitOrderReadStrategy::chooseKind(const RangesInDataPart & ranges) const
{
    const auto & metadata = storage_snapshot->metadata;
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
        return {Kind::Native, ranges, projection_description.metadata};
    }

    /// 2 If partial read was requested try read it directly.
    const size_t granule_count = part->index_granularity->getMarksCountWithoutFinal();
    const bool is_partial_read = !(ranges.ranges.size() == 1 && ranges.ranges.front().begin == 0 && ranges.ranges.front().end == granule_count);
    if (is_partial_read)
    {
        if (!sortingKeyStartsWithBlockNumberAndOffset(metadata->sorting_key.column_names))
            throw Exception(ErrorCodes::ILLEGAL_STREAM, "Partial-range read of a part whose sort key is not (_block_number, _block_offset) cannot produce commit-order rows");

        return {Kind::Native, ranges, metadata};
    }

    /// 3 Try to find commit-order projection to read from.
    for (const auto & projection : metadata->projections)
    {
        if (!projection.with_block_number || !projection.with_block_offset)
            continue;

        if (!part->getProjectionParts().contains(projection.name))
            continue;

        const auto projection_part_to_read = part->getProjectionParts().at(projection.name);
        return {Kind::Native, RangesInDataPart(projection_part_to_read, part), projection.metadata};
    }

    /// 4 If part contain single block - disk layout already matches commit-order.
    if (part->isZeroLevel())
        return {Kind::Native, ranges, metadata};

    /// 5 If part was sorted in commit-order try read it directly.
    if (sortingKeyStartsWithBlockNumberAndOffset(metadata->sorting_key.column_names))
        return {Kind::Native, ranges, metadata};

    /// 6 Fallback to commit-order sorting.
    return {Kind::Sort, ranges, metadata};
}

Pipe CommitOrderReadStrategy::readSinglePartInOrder(const RangesInDataPart & ranges, const StorageMetadataPtr & metadata) const
{
    const auto & settings = context->getSettingsRef();

    RangesInDataParts parts_with_ranges{ranges};
    const size_t sum_marks = ranges.getMarksCount();

    MergeTreeReadPoolBase::PoolSettings pool_settings{
        .threads = 1,
        .sum_marks = sum_marks,
        .min_marks_for_concurrent_read = 1,
        .preferred_block_size_bytes = settings[Setting::preferred_block_size_bytes],
        .use_uncompressed_cache = false,
        .total_query_nodes = 1,
    };

    auto pool = std::make_shared<MergeTreeReadPoolInOrder>(
        /*has_limit_below_one_block=*/ false,
        MergeTreeReadType::InOrder,
        std::move(parts_with_ranges),
        mutations_snapshot,
        shared_virtual_fields,
        index_read_tasks,
        std::make_shared<StorageSnapshot>(storage, metadata), /// TODO: rewrite pools to use only metadata
        row_level_filter,
        prewhere_info,
        actions_settings,
        reader_settings,
        columns_to_read,
        pool_settings,
        block_size_params,
        context,
        /*updater=*/ nullptr);

    auto algorithm = std::make_unique<MergeTreeInOrderSelectAlgorithm>(/*part_idx=*/ 0);

    auto processor = std::make_unique<MergeTreeSelectProcessor>(
        pool,
        std::move(algorithm),
        row_level_filter,
        prewhere_info,
        index_read_tasks,
        actions_settings,
        reader_settings,
        /*merge_tree_index_build_context=*/ nullptr,
        /*lazy_materializing_rows=*/ nullptr);

    auto source = std::make_shared<MergeTreeSource>(std::move(processor), storage.getLogName());
    return Pipe(std::move(source));
}

void CommitOrderReadStrategy::addSortTransforms(Pipe & pipe) const
{
    static const auto sort_description = commitOrderSortDescription();
    const auto & settings = context->getSettingsRef();

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
}

CommitOrderReadStrategy::CommitOrderReadStrategy(
    const MergeTreeData & storage_,
    StorageSnapshotPtr storage_snapshot_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    Names columns_to_read_,
    MergeTreeReaderSettings reader_settings_,
    ExpressionActionsSettings actions_settings_,
    MergeTreeReadTask::BlockSizeParams block_size_params_,
    VirtualFields shared_virtual_fields_,
    IndexReadTasks index_read_tasks_,
    PrewhereInfoPtr prewhere_info_,
    FilterDAGInfoPtr row_level_filter_,
    ContextPtr context_)
    : storage(storage_)
    , storage_snapshot(std::move(storage_snapshot_))
    , mutations_snapshot(std::move(mutations_snapshot_))
    , columns_to_read(std::move(columns_to_read_))
    , reader_settings(std::move(reader_settings_))
    , actions_settings(std::move(actions_settings_))
    , block_size_params(block_size_params_)
    , shared_virtual_fields(std::move(shared_virtual_fields_))
    , index_read_tasks(std::move(index_read_tasks_))
    , prewhere_info(std::move(prewhere_info_))
    , row_level_filter(std::move(row_level_filter_))
    , context(std::move(context_))
{
}

Pipe CommitOrderReadStrategy::createReadStream(const RangesInDataPart & ranges) const
{
    const auto [kind, ranges_to_read, metadata_to_use] = chooseKind(ranges);

    auto pipe = readSinglePartInOrder(ranges_to_read, metadata_to_use);

    if (kind == Kind::Sort)
        addSortTransforms(pipe);

    return pipe;
}

}
