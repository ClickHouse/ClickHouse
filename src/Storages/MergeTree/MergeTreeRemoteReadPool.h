#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/SelectQueryInfo.h>
#include <mutex>


namespace DB
{

class MergeTreeRemoteReadPool : public IMergeTreeReadPool
{
public:
    MergeTreeRemoteReadPool(
        size_t threads_,
        size_t sum_marks_,
        size_t min_marks_for_concurrent_read_,
        RangesInDataParts && parts_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const Names & column_names_,
        const Names & virtual_column_names_,
        size_t preferred_block_size_bytes_);

    MergeTreeReadTaskPtr getTask(size_t min_marks_to_read, size_t thread, const Names & ordered_names) override;

    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

    Block getHeader() const override;

private:
    struct PartInfo
    {
        MergeTreeData::DataPartPtr data_part;
        size_t part_index_in_query;
        size_t sum_marks = 0;

        NameSet column_name_set;
        MergeTreeReadTaskColumns task_columns;
        MergeTreeBlockSizePredictorPtr size_predictor;
    };
    using PartsInfos = std::queue<MergeTreeRemoteReadPool::PartInfo>;

    static PartsInfos getPartsInfosGroupedByDiskName(
        const RangesInDataParts & parts,
        const PrewhereInfoPtr & prewhere_info,
        const Names & column_names,
        const Names & virtual_column_names,
        size_t preferred_block_size_bytes);

    void fillPerThreadInfo(
        size_t threads,
        size_t sum_marks,
        const RangesInDataParts & parts,
        size_t min_marks_for_concurrent_read,
        const PrewhereInfoPtr & prewhere_info,
        const Names & column_names,
        const Names & virtual_column_names,
        size_t preferred_block_size_bytes);

    mutable std::mutex mutex;
    Poco::Logger * log = &Poco::Logger::get("MergeTreeRemoteReadPool");

    const StorageSnapshotPtr storage_snapshot;
    const Names column_names;

    const bool predict_block_size_bytes;
    const PrewhereInfoPtr prewhere_info;
    const RangesInDataParts parts_ranges;

    using ThreadTask = std::vector<MergeTreeReadTaskPtr>;
    std::vector<ThreadTask> threads_tasks;

    std::set<size_t> remaining_thread_tasks;
};

}
