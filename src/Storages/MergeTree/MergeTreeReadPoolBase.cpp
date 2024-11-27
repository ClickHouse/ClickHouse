#include <Storages/MergeTree/MergeTreeReadPoolBase.h>

#include <Core/Settings.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool merge_tree_determine_task_size_by_prewhere_columns;
    extern const SettingsUInt64 merge_tree_min_bytes_per_task_for_remote_reading;
    extern const SettingsUInt64 merge_tree_min_read_task_size;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReadPoolBase::MergeTreeReadPoolBase(
    RangesInDataParts && parts_,
    MutationsSnapshotPtr mutations_snapshot_,
    VirtualFields shared_virtual_fields_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & column_names_,
    const PoolSettings & pool_settings_,
    const MergeTreeReadTask::BlockSizeParams & block_size_params_,
    const ContextPtr & context_)
    : WithContext(context_)
    , parts_ranges(std::move(parts_))
    , mutations_snapshot(std::move(mutations_snapshot_))
    , shared_virtual_fields(std::move(shared_virtual_fields_))
    , storage_snapshot(storage_snapshot_)
    , prewhere_info(prewhere_info_)
    , actions_settings(actions_settings_)
    , reader_settings(reader_settings_)
    , column_names(column_names_)
    , pool_settings(pool_settings_)
    , block_size_params(block_size_params_)
    , owned_mark_cache(context_->getGlobalContext()->getMarkCache())
    , owned_uncompressed_cache(pool_settings_.use_uncompressed_cache ? context_->getGlobalContext()->getUncompressedCache() : nullptr)
    , header(storage_snapshot->getSampleBlockForColumns(column_names))
    , profile_callback([this](ReadBufferFromFileBase::ProfileInfo info_) { profileFeedback(info_); })
{
    fillPerPartInfos(context_->getSettingsRef());
}

static size_t getSizeOfColumns(const IMergeTreeDataPart & part, const Names & columns_to_read)
{
    ColumnSize columns_size{};
    for (const auto & col_name : columns_to_read)
        columns_size.add(part.getColumnSize(col_name));
    /// For compact parts we don't know individual column sizes, let's use whole part size as approximation
    return columns_size.data_compressed ? columns_size.data_compressed : part.getBytesOnDisk();
}

/// Columns from different prewhere steps are read independently, so it makes sense to use the heaviest set of columns among them as an estimation.
static Names
getHeaviestSetOfColumnsAmongPrewhereSteps(const IMergeTreeDataPart & part, const std::vector<NamesAndTypesList> & prewhere_steps_columns)
{
    const auto it = std::ranges::max_element(
        prewhere_steps_columns,
        [&](const auto & lhs, const auto & rhs)
        { return getSizeOfColumns(part, lhs.getNames()) < getSizeOfColumns(part, rhs.getNames()); });
    return it->getNames();
}

static std::pair<size_t, size_t> // (min_marks_per_task, avg_mark_bytes)
calculateMinMarksPerTask(
    const RangesInDataPart & part,
    const Names & columns_to_read,
    const std::vector<NamesAndTypesList> & prewhere_steps_columns,
    const MergeTreeReadPoolBase::PoolSettings & pool_settings,
    const Settings & settings)
{
    size_t min_marks_per_task
        = std::max<size_t>(settings[Setting::merge_tree_min_read_task_size], pool_settings.min_marks_for_concurrent_read);
    size_t avg_mark_bytes = 0;
    /// It is important to obtain marks count from the part itself instead of calling `part.getMarksCount()`,
    /// because `part` will report number of marks selected from this part by the query.
    const size_t part_marks_count = part.data_part->getMarksCount();
    if (part_marks_count)
    {
        if (part.data_part->isStoredOnRemoteDisk())
        {
            /// We assume that most of the time prewhere does it's job good meaning that lion's share of the rows is filtered out.
            /// Which means in turn that for most of the rows we will read only the columns from prewhere clause.
            /// So it makes sense to use only them for the estimation.
            const auto & columns = settings[Setting::merge_tree_determine_task_size_by_prewhere_columns] && !prewhere_steps_columns.empty()
                ? getHeaviestSetOfColumnsAmongPrewhereSteps(*part.data_part, prewhere_steps_columns)
                : columns_to_read;
            const size_t part_compressed_bytes = getSizeOfColumns(*part.data_part, columns);

            avg_mark_bytes = std::max<size_t>(part_compressed_bytes / part_marks_count, 1);
            const auto min_bytes_per_task = settings[Setting::merge_tree_min_bytes_per_task_for_remote_reading];
            /// We're taking min here because number of tasks shouldn't be too low - it will make task stealing impossible.
            /// We also create at least two tasks per thread to have something to steal from a slow thread.
            const auto heuristic_min_marks
                = std::min<size_t>(pool_settings.sum_marks / pool_settings.threads / 2, min_bytes_per_task / avg_mark_bytes);
            if (heuristic_min_marks > min_marks_per_task)
            {
                LOG_TEST(
                    &Poco::Logger::get("MergeTreeReadPoolBase"),
                    "Increasing min_marks_per_task from {} to {} based on columns size heuristic",
                    min_marks_per_task,
                    heuristic_min_marks);
                min_marks_per_task = heuristic_min_marks;
            }
        }
        else
        {
            avg_mark_bytes = std::max<size_t>(getSizeOfColumns(*part.data_part, columns_to_read) / part_marks_count, 1);
        }
    }

    LOG_TEST(&Poco::Logger::get("MergeTreeReadPoolBase"), "Will use min_marks_per_task={}", min_marks_per_task);
    return {min_marks_per_task, avg_mark_bytes};
}

void MergeTreeReadPoolBase::fillPerPartInfos(const Settings & settings)
{
    per_part_infos.reserve(parts_ranges.size());
    is_part_on_remote_disk.reserve(parts_ranges.size());

    auto sample_block = storage_snapshot->metadata->getSampleBlock();

    for (const auto & part_with_ranges : parts_ranges)
    {
#ifndef NDEBUG
        assertSortedAndNonIntersecting(part_with_ranges.ranges);
#endif

        MergeTreeReadTaskInfo read_task_info;

        read_task_info.data_part = part_with_ranges.data_part;

        const auto & data_part = read_task_info.data_part;
        if (data_part->isProjectionPart())
        {
            read_task_info.parent_part = data_part->storage.getPartIfExists(
                data_part->getParentPartName(),
                {MergeTreeDataPartState::PreActive, MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});

            if (!read_task_info.parent_part)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Did not find parent part {} for projection part {}",
                            data_part->getParentPartName(), data_part->getDataPartStorage().getFullPath());
        }

        read_task_info.part_index_in_query = part_with_ranges.part_index_in_query;
        read_task_info.alter_conversions = MergeTreeData::getAlterConversionsForPart(part_with_ranges.data_part, mutations_snapshot, storage_snapshot->metadata, getContext());

        LoadedMergeTreeDataPartInfoForReader part_info(part_with_ranges.data_part, read_task_info.alter_conversions);

        read_task_info.task_columns = getReadTaskColumns(
            part_info,
            storage_snapshot,
            column_names,
            prewhere_info,
            actions_settings,
            reader_settings,
            /*with_subcolumns=*/true);

        read_task_info.const_virtual_fields = shared_virtual_fields;
        read_task_info.const_virtual_fields.emplace("_part_index", read_task_info.part_index_in_query);

        if (pool_settings.preferred_block_size_bytes > 0)
        {
            const auto & result_column_names = read_task_info.task_columns.columns.getNames();
            NameSet all_column_names(result_column_names.begin(), result_column_names.end());

            for (const auto & pre_columns_per_step : read_task_info.task_columns.pre_columns)
            {
                const auto & pre_column_names = pre_columns_per_step.getNames();
                all_column_names.insert(pre_column_names.begin(), pre_column_names.end());
            }

            read_task_info.shared_size_predictor = std::make_unique<MergeTreeBlockSizePredictor>(
                read_task_info.data_part,
                Names(all_column_names.begin(), all_column_names.end()),
                sample_block);
        }

        is_part_on_remote_disk.push_back(part_with_ranges.data_part->isStoredOnRemoteDisk());
        std::tie(read_task_info.min_marks_per_task, read_task_info.approx_size_of_mark)
            = calculateMinMarksPerTask(part_with_ranges, column_names, read_task_info.task_columns.pre_columns, pool_settings, settings);
        per_part_infos.push_back(std::make_shared<MergeTreeReadTaskInfo>(std::move(read_task_info)));
    }
}

std::vector<size_t> MergeTreeReadPoolBase::getPerPartSumMarks() const
{
    std::vector<size_t> per_part_sum_marks;
    per_part_sum_marks.reserve(parts_ranges.size());

    for (const auto & part_with_ranges : parts_ranges)
    {
        size_t sum_marks = 0;
        for (const auto & range : part_with_ranges.ranges)
            sum_marks += range.end - range.begin;

        per_part_sum_marks.push_back(sum_marks);
    }

    return per_part_sum_marks;
}

MergeTreeReadTaskPtr
MergeTreeReadPoolBase::createTask(MergeTreeReadTaskInfoPtr read_info, MergeTreeReadTask::Readers task_readers, MarkRanges ranges) const
{
    auto task_size_predictor = read_info->shared_size_predictor
        ? std::make_unique<MergeTreeBlockSizePredictor>(*read_info->shared_size_predictor)
        : nullptr; /// make a copy

    return std::make_unique<MergeTreeReadTask>(
        read_info, std::move(task_readers), std::move(ranges), block_size_params, std::move(task_size_predictor));
}

MergeTreeReadTaskPtr
MergeTreeReadPoolBase::createTask(MergeTreeReadTaskInfoPtr read_info, MarkRanges ranges, MergeTreeReadTask * previous_task) const
{
    auto get_part_name = [](const auto & task_info) -> String
    {
        const auto & data_part = task_info.data_part;

        if (data_part->isProjectionPart())
        {
            auto parent_part_name = data_part->getParentPartName();

            auto parent_part = data_part->storage.getPartIfExists(
                parent_part_name, {MergeTreeDataPartState::PreActive, MergeTreeDataPartState::Active, MergeTreeDataPartState::Outdated});

            if (!parent_part)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Did not find parent part {} for projection part {}",
                            parent_part_name, data_part->getDataPartStorage().getFullPath());

            return parent_part_name;
        }

        return data_part->name;
    };

    auto extras = getExtras();
    MergeTreeReadTask::Readers task_readers;

    if (!previous_task)
    {
        task_readers = MergeTreeReadTask::createReaders(read_info, extras, ranges);
    }
    else if (get_part_name(previous_task->getInfo()) != get_part_name(*read_info))
    {
        extras.value_size_map = previous_task->getMainReader().getAvgValueSizeHints();
        task_readers = MergeTreeReadTask::createReaders(read_info, extras, ranges);
    }
    else
    {
        task_readers = previous_task->releaseReaders();
    }

    return createTask(read_info, std::move(task_readers), std::move(ranges));
}

MergeTreeReadTask::Extras MergeTreeReadPoolBase::getExtras() const
{
    return
    {
        .uncompressed_cache = owned_uncompressed_cache.get(),
        .mark_cache = owned_mark_cache.get(),
        .reader_settings = reader_settings,
        .storage_snapshot = storage_snapshot,
        .profile_callback = profile_callback,
    };
}

}
