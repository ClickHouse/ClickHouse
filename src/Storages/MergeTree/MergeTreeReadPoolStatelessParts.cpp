#include <Storages/MergeTree/MergeTreeReadPoolStatelessParts.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Disks/IDisk.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/BorrowedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/DeserializationPrefixesCache.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace Setting
{
    extern const SettingsNonZeroUInt64 merge_tree_min_read_task_size;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool share_nested_offsets;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

MergeTreeReadPoolStatelessParts::MergeTreeReadPoolStatelessParts(
    ReadFromPartsInfo read_from_parts_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const FilterDAGInfoPtr & row_level_filter_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & column_names_,
    const PoolSettings & settings_,
    const MergeTreeReadTask::BlockSizeParams & params_,
    const ContextPtr & context_)
    : MergeTreeReadPool(
          /*parts_=*/ {},
          /*mutations_snapshot_=*/ nullptr,
          /*shared_virtual_fields_=*/ {},
          /*index_read_tasks_=*/ {},
          storage_snapshot_,
          row_level_filter_,
          prewhere_info_,
          actions_settings_,
          reader_settings_,
          column_names_,
          settings_,
          params_,
          context_,
          /*updater_=*/ nullptr)
    , read_from_parts_info(std::move(read_from_parts_info_))
    , storage_columns(storage_snapshot_->metadata->getColumns().getAllPhysical())
    , requested_columns(storage_snapshot_->getSampleBlockForColumns(column_names_).getNamesAndTypesList())
    , storage_settings(read_from_parts_info.buildStorageSettings())
    , min_marks_per_task(
          std::max<size_t>(context_->getSettingsRef()[Setting::merge_tree_min_read_task_size], settings_.min_marks_for_concurrent_read))
    , part_info_built(read_from_parts_info.parts.size())
    , per_part_task_infos(read_from_parts_info.parts.size())
{
    is_part_on_remote_disk.assign(read_from_parts_info.parts.size(), read_from_parts_info.disk->isRemote());
    fillPerThreadInfoForBorrowedParts(pool_settings.threads);
}

/// Right now the same as MergeTreeReadPool::fillPerThreadInfo, except that the marks of a part come
/// from the description instead of from a RangesInDataPart.
void MergeTreeReadPoolStatelessParts::fillPerThreadInfoForBorrowedParts(size_t threads)
{
    std::lock_guard lock(mutex);

    /// threads_tasks is in the base class
    threads_tasks.resize(threads);
    if (!threads)
        return;

    /// Ranges still to be handed out, per part, consumed back to front.
    struct PartInfo
    {
        MarkRanges ranges;
        size_t sum_marks;
        size_t part_idx;
    };

    std::vector<PartInfo> parts_queue;
    parts_queue.reserve(read_from_parts_info.parts.size());

    size_t sum_marks = 0;
    for (size_t i = 0; i < read_from_parts_info.parts.size(); ++i)
    {
        const size_t marks_in_part = read_from_parts_info.parts[i].ranges.getNumberOfMarks();
        if (!marks_in_part)
            continue;

        parts_queue.push_back(PartInfo{read_from_parts_info.parts[i].ranges, marks_in_part, i});
        sum_marks += marks_in_part;
    }

    if (parts_queue.empty())
        return;

    const size_t parts_with_marks = parts_queue.size();
    const size_t min_marks_per_thread = (sum_marks - 1) / threads + 1;

    for (size_t i = 0; i < threads && !parts_queue.empty(); ++i)
    {
        size_t need_marks = min_marks_per_thread;

        while (need_marks > 0 && !parts_queue.empty())
        {
            auto & current_part = parts_queue.back();
            size_t & marks_in_part = current_part.sum_marks;
            const size_t part_idx = current_part.part_idx;
            /// This runs from the constructor, so it must not go through the virtual
            /// getMinMarksPerTask; the override returns exactly this value for every part.
            const size_t min_marks_for_part = min_marks_per_task;

            if (!min_marks_for_part)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Chosen number of marks to read is zero");

            /// Do not get too few marks from a part.
            if (marks_in_part >= min_marks_for_part && need_marks < min_marks_for_part)
                need_marks = min_marks_for_part;

            /// Do not leave too few marks in a part for next time.
            if (marks_in_part > need_marks && marks_in_part - need_marks < min_marks_for_part)
                need_marks = marks_in_part;

            MarkRanges ranges_to_get_from_part;
            size_t marks_in_ranges = need_marks;

            /// Take the whole remainder of the part if it is small enough.
            if (marks_in_part <= need_marks)
            {
                ranges_to_get_from_part = std::move(current_part.ranges);
                marks_in_ranges = marks_in_part;

                need_marks -= marks_in_part;
                parts_queue.pop_back();
            }
            else
            {
                while (need_marks > 0)
                {
                    if (current_part.ranges.empty())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected end of ranges while spreading marks among threads");

                    MarkRange & range = current_part.ranges.front();

                    const size_t marks_in_range = range.end - range.begin;
                    const size_t marks_to_get_from_range = std::min(marks_in_range, need_marks);

                    ranges_to_get_from_part.emplace_back(range.begin, range.begin + marks_to_get_from_range);
                    range.begin += marks_to_get_from_range;
                    marks_in_part -= marks_to_get_from_range;
                    need_marks -= marks_to_get_from_range;
                    if (range.begin == range.end)
                        current_part.ranges.pop_front();
                }
            }

            threads_tasks[i].parts_and_ranges.push_back({part_idx, std::move(ranges_to_get_from_part)});
            threads_tasks[i].sum_marks_in_parts.push_back(marks_in_ranges);
            if (marks_in_ranges != 0)
                remaining_thread_tasks.insert(i);
        }
    }

    size_t queue_entries = 0;
    size_t max_thread_marks = 0;
    for (const auto & thread_tasks : threads_tasks)
    {
        queue_entries += thread_tasks.parts_and_ranges.size();

        size_t marks_for_thread = 0;
        for (size_t marks : thread_tasks.sum_marks_in_parts)
            marks_for_thread += marks;
        max_thread_marks = std::max(max_thread_marks, marks_for_thread);
    }

    LOG_DEBUG(
        logger,
        "Spread {} marks from {} parts over {} threads: {} queue items, {} threads used, "
        "{} marks targeted per thread, {} is highest marks assigned",
        sum_marks,
        parts_with_marks,
        threads,
        queue_entries,
        remaining_thread_tasks.size(),
        min_marks_per_thread,
        max_thread_marks);
}

RangesInDataPartsDescription MergeTreeReadPoolStatelessParts::buildAnnouncementDescriptions() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} cannot announce ranges: borrowed parts have no data part", getName());
}

MergeTreeReadTaskInfoPtr MergeTreeReadPoolStatelessParts::getOrBuildTaskInfo(size_t part_index) const
{
    std::call_once(
        part_info_built[part_index],
        [&] { per_part_task_infos[part_index] = buildTaskInfoForPart(read_from_parts_info.parts[part_index], part_index); });

    return per_part_task_infos[part_index];
}

MergeTreeReadTaskPtr MergeTreeReadPoolStatelessParts::getTask(size_t task_idx, MergeTreeReadTask * previous_task)
{
    /// Nothing sets a refiner on this path yet (only ReadFromMergeTree does, for projection indexes).
    if (ranges_refiner)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} does not support a read ranges refiner", getName());

    while (true)
    {
        size_t part_idx = 0;
        size_t thread_idx = 0;
        size_t need_marks = 0;
        MarkRanges task_ranges;

        if (!cutRangesToRead(task_idx, part_idx, thread_idx, need_marks, task_ranges))
            return nullptr;

        /// A portion can only come back empty if a queue entry held no marks; take the next one.
        if (task_ranges.empty())
            continue;

        return createTask(
            getOrBuildTaskInfo(part_idx), std::move(task_ranges), /*patches_ranges=*/ {}, previous_task, /*updater=*/ nullptr);
    }
}

MergeTreeDataPartInfoForReaderPtr MergeTreeReadPoolStatelessParts::buildReaderInfoFromDisk(
    const ReadFromPart & part, const VolumePtr & volume, const std::string & part_root, const std::string & part_name) const
{
    auto all_columns = storage_columns;
    if (part.has_lightweight_delete)
        all_columns.emplace_back(RowExistsColumn::name, RowExistsColumn::type);

    auto [data_part_storage, mark_type] = MergeTreeDataPartBuilder::getPartStorageAndMarkType(volume, part_root, part_name, getReadSettings());
    if (!data_part_storage || !mark_type)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot determine part storage type and mark type from disk");

    auto metadata_read_settings = getContext()->getReadSettings().adjustBufferSize(4096);

    /// Part's physical columns; columns absent from the part fill from defaults. Fresh list (readText appends).
    NamesAndTypesList serialization_columns;
    if (auto columns_buf = data_part_storage->readFileIfExists("columns.txt", metadata_read_settings, {}))
        serialization_columns.readText(*columns_buf);
    else
        serialization_columns = all_columns;

    SerializationInfoByName serialization_infos({});
    if (data_part_storage->existsFile(IMergeTreeDataPart::SERIALIZATION_FILE_NAME))
        serialization_infos = SerializationInfoByName::readJSON(
            serialization_columns, *data_part_storage->readFile(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, metadata_read_settings, {}));

    MergeTreeIndexGranularityInfo index_granularity_info(
        *mark_type, read_from_parts_info.index_granularity, read_from_parts_info.index_granularity_bytes);

    MergeTreeIndexGranularityPtr index_granularity;

    /// Compact parts need getTotalSubstreams() (below) when mark_type.with_substreams is true.
    ColumnsSubstreams columns_substreams;
    if (auto buf = data_part_storage->readFileIfExists("columns_substreams.txt", metadata_read_settings, {}))
        columns_substreams.readText(*buf);

    NameSet invalidated_system_columns;
    if (auto invalidated_buf = data_part_storage->readFileIfExists(IMergeTreeDataPart::INVALIDATED_SYSTEM_COLUMNS_FILE_NAME, metadata_read_settings, {}))
        invalidated_system_columns = IMergeTreeDataPart::readInvalidatedSystemColumns(*invalidated_buf);

    switch (part.type.getValue())
    {
        case MergeTreeDataPartType::Wide:
        {
            index_granularity_info.changeGranularityIfRequired(*data_part_storage);

            /// Granularity is shared across columns; load it from any present column (or its parent).
            if (serialization_columns.empty())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "No columns in wide part {} with path {}", part_name, part_root);
            NameAndTypePair column = requested_columns.empty() ? serialization_columns.front() : requested_columns.front();
            if (!serialization_columns.contains(column.name))
            {
                String storage_name = column.name;
                if (auto resolved = storage_snapshot->metadata->getColumns().tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column.name))
                    storage_name = resolved->getNameInStorage();
                if (auto storage_column = serialization_columns.tryGetByName(storage_name))
                    column = *storage_column;
                else if (!serialization_columns.empty())
                    column = serialization_columns.front();
            }
            auto it = serialization_infos.find(column.getNameInStorage());
            auto serialization = it == serialization_infos.end()
                ? IDataType::getSerialization(column)
                : IDataType::getSerialization(column, *it->second);

            String filename;
            serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
            {
                if (filename.empty())
                {
                    /// Storage-based overload resolves hash-based stream names by checking which .bin exists.
                    auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
                        column, substream_path, IMergeTreeDataPart::DATA_FILE_EXTENSION, *data_part_storage, storage_settings);
                    if (stream_name)
                        filename = *stream_name;
                    else
                        throw Exception(ErrorCodes::CORRUPTED_DATA, "Could not get filename for column {} in part {} with path {}",
                                        column.name, part_name, part_root);
                }
            });

            index_granularity = std::make_shared<MergeTreeIndexGranularityAdaptive>();
            MergeTreeDataPartWide::loadIndexGranularityImpl(index_granularity, index_granularity_info, *data_part_storage, filename, *storage_settings);
            break;
        }
        case MergeTreeDataPartType::Compact:
        {
            /// with_substreams marks store one entry per substream, so pass the substream count.
            size_t marks_per_granule = index_granularity_info.mark_type.with_substreams
                ? columns_substreams.getTotalSubstreams() : serialization_columns.size();
            if (!marks_per_granule)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Zero marks per granule in part {} with path {}", part_name, part_root);
            index_granularity = std::make_shared<MergeTreeIndexGranularityAdaptive>();
            MergeTreeDataPartCompact::loadIndexGranularityImpl(index_granularity, index_granularity_info, marks_per_granule, *data_part_storage, *storage_settings);
            break;
        }
        case MergeTreeDataPartType::Unknown:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Data parts of type `{}` are not allowed", part.type.toString());
    }

    MergeTreeDataPartChecksums checksums;
    auto buf = data_part_storage->readFile("checksums.txt", metadata_read_settings, {});
    checksums.read(*buf);

    auto table_name = storage_snapshot->storage.getStorageID().getNameForLogs();

    return std::make_shared<BorrowedMergeTreeDataPartInfoForReader>(
        part.type, data_part_storage, serialization_columns, std::move(columns_substreams), std::move(invalidated_system_columns),
        index_granularity_info, index_granularity, checksums, serialization_infos,
        table_name, part.marks_count, storage_settings, getContext(),
        (*storage_settings)[MergeTreeSetting::share_nested_offsets]);
}

MergeTreeReadTaskInfoPtr MergeTreeReadPoolStatelessParts::buildTaskInfoForPart(const ReadFromPart & part, size_t part_index) const
{
    /// e.g. store/707/<uuid>/20000101_1_1_0  ->  root "store/707/<uuid>", name "20000101_1_1_0"
    fs::path part_relative_path = part.relative_path;
    if (part.relative_path.ends_with("/"))
        part_relative_path = part_relative_path.parent_path();
    std::string part_name = part_relative_path.filename();
    std::string part_relative_root_path = part_relative_path.parent_path();

    LOG_DEBUG(
        logger,
        "Initializing reader for {} part {} ({}): {} ranges, {} marks of {}, min_marks_per_task {}, {} streams",
        part.type.toString(),
        part_name,
        part.relative_path,
        part.ranges.size(),
        part.ranges.getNumberOfMarks(),
        part.marks_count,
        min_marks_per_task,
        pool_settings.threads);

    auto disk = read_from_parts_info.disk;
    auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);

    auto info_for_reader = buildReaderInfoFromDisk(part, single_disk_volume, part_relative_root_path, part_name);

    auto read_task_info = std::make_shared<MergeTreeReadTaskInfo>();
    read_task_info->data_part_info = info_for_reader;
    read_task_info->part_index_in_query = part_index;
    read_task_info->part_starting_offset_in_query = 0;
    read_task_info->alter_conversions = std::make_shared<AlterConversions>();
    read_task_info->min_marks_per_task = min_marks_per_task;

    /// Build the lightweight delete mutation step for this part if needed. Per-part because
    /// different parts may or may not have lightweight deletes.
    if (reader_settings.apply_deleted_mask && part.has_lightweight_delete)
    {
        bool remove_filter_column = std::ranges::find(column_names, RowExistsColumn::name) == column_names.end();
        read_task_info->mutation_steps.push_back(createLightweightDeleteStep(remove_filter_column));
    }

    read_task_info->task_columns = getReadTaskColumns(
        *info_for_reader,
        storage_snapshot,
        column_names,
        row_level_filter,
        prewhere_info,
        read_task_info->mutation_steps,
        /*index_read_tasks=*/ {},
        actions_settings,
        reader_settings,
        /// MergeTree parts support subcolumns (read from the parent column's streams); resolve
        /// them like the regular read pool, which also passes true.
        /*with_subcolumns=*/ true);

    read_task_info->const_virtual_fields = shared_virtual_fields;
    read_task_info->const_virtual_fields.emplace("_part_index", read_task_info->part_index_in_query);
    read_task_info->const_virtual_fields.emplace("_part_starting_offset", read_task_info->part_starting_offset_in_query);
    read_task_info->deserialization_prefixes_cache = std::make_shared<DeserializationPrefixesCache>();

    return read_task_info;
}

}
