#include <Storages/MergeTree/MergeTreeReadPoolBase.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReadPoolBase::MergeTreeReadPoolBase(
    RangesInDataParts && parts_,
    VirtualFields shared_virtual_fields_,
    const StorageSnapshotPtr & storage_snapshot_,
    const PrewhereInfoPtr & prewhere_info_,
    const ExpressionActionsSettings & actions_settings_,
    const MergeTreeReaderSettings & reader_settings_,
    const Names & column_names_,
    const PoolSettings & pool_settings_,
    const ContextPtr & context_)
    : parts_ranges(std::move(parts_))
    , shared_virtual_fields(std::move(shared_virtual_fields_))
    , storage_snapshot(storage_snapshot_)
    , prewhere_info(prewhere_info_)
    , actions_settings(actions_settings_)
    , reader_settings(reader_settings_)
    , column_names(column_names_)
    , pool_settings(pool_settings_)
    , owned_mark_cache(context_->getGlobalContext()->getMarkCache())
    , owned_uncompressed_cache(pool_settings_.use_uncompressed_cache ? context_->getGlobalContext()->getUncompressedCache() : nullptr)
    , header(storage_snapshot->getSampleBlockForColumns(column_names))
    , profile_callback([this](ReadBufferFromFileBase::ProfileInfo info_) { profileFeedback(info_); })
{
    fillPerPartInfos();
}

void MergeTreeReadPoolBase::fillPerPartInfos()
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
        read_task_info.alter_conversions = part_with_ranges.alter_conversions;

        LoadedMergeTreeDataPartInfoForReader part_info(part_with_ranges.data_part, part_with_ranges.alter_conversions);

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

MergeTreeReadTaskPtr MergeTreeReadPoolBase::createTask(
    MergeTreeReadTaskInfoPtr read_info,
    MarkRanges ranges,
    MergeTreeReadTask * previous_task) const
{
    auto task_size_predictor = read_info->shared_size_predictor
        ? std::make_unique<MergeTreeBlockSizePredictor>(*read_info->shared_size_predictor)
        : nullptr; /// make a copy

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

    return std::make_unique<MergeTreeReadTask>(
        read_info,
        std::move(task_readers),
        std::move(ranges),
        std::move(task_size_predictor));
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
