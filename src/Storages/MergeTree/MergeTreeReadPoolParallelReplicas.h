#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/SelectQueryInfo.h>
#include "Storages/MergeTree/RequestResponse.h"
#include "Storages/StorageSnapshot.h"

#include <condition_variable>
#include <mutex>
#include <memory>

namespace DB
{


/// Weird class with copy-paste from original read pool
class MergeTreeReadPoolParallelReplicas : public IMergeTreeReadPool, private boost::noncopyable
{
public:

    MergeTreeReadPoolParallelReplicas(
        StorageSnapshotPtr storage_snapshot_,
        size_t threads_,
        ParallelReadingExtension extension_,
        const RangesInDataParts & parts_,
        const PrewhereInfoPtr & prewhere_info_,
        const Names & column_names_,
        const Names & virtual_column_names_,
        size_t min_marks_for_concurrent_read_
    )
    : storage_snapshot(storage_snapshot_)
    , extension(extension_)
    , column_names{column_names_}
    , virtual_column_names{virtual_column_names_}
    , prewhere_info(prewhere_info_)
    , parts_ranges(parts_)
    , threads(threads_)
    , min_marks_for_concurrent_read(min_marks_for_concurrent_read_)
    {
        fillPerPartInfo(parts_ranges);
    }

    ~MergeTreeReadPoolParallelReplicas() override;

    /// Sends all the data about selected parts to the initiator
    void initialize();
    MergeTreeReadTaskPtr getTask(size_t thread) override;
    Block getHeader() const override;
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

private:
    StorageSnapshotPtr storage_snapshot;
    ParallelReadingExtension extension;

    const Names column_names;
    const Names virtual_column_names;

    PrewhereInfoPtr prewhere_info;

    struct PartWithParams
    {
        MergeTreeReadTaskColumns task_columns;
        NameSet column_name_set;
        MergeTreeBlockSizePredictorPtr size_predictor;
        RangesInDataPart data_part;
    };

    std::vector<PartWithParams> parts_ranges_with_params;
    RangesInDataPartsDescription buffered_ranges;
    RangesInDataParts parts_ranges;
    size_t threads;
    size_t min_marks_for_concurrent_read;
    bool no_more_tasks_available{false};
    std::shared_future<std::optional<ParallelReadResponse>> future_response;
    mutable std::mutex mutex;
    Poco::Logger * log = &Poco::Logger::get("MergeTreeReadPoolParallelReplicas");

    std::vector<size_t> fillPerPartInfo(const RangesInDataParts & parts);
    void sendRequest();
};

using MergeTreeReadPoolParallelReplicasPtr = std::shared_ptr<MergeTreeReadPoolParallelReplicas>;


class MergeTreeInOrderReadPoolParallelReplicas : private boost::noncopyable
{
public:
    MergeTreeInOrderReadPoolParallelReplicas(
        RangesInDataParts parts_,
        ParallelReadingExtension extension_,
        CoordinationMode mode_,
        size_t min_marks_for_concurrent_read_)
    : parts_ranges(parts_)
    , extension(extension_)
    , mode(mode_)
    , min_marks_for_concurrent_read(min_marks_for_concurrent_read_)
    {
        for (const auto & part : parts_ranges)
            request.push_back({part.data_part->info, MarkRanges{}});

        for (const auto & part : parts_ranges)
            buffered_tasks.push_back({part.data_part->info, MarkRanges{}});
    }

    void initialize();
    MarkRanges getNewTask(RangesInDataPartDescription description);

    RangesInDataParts parts_ranges;
    ParallelReadingExtension extension;
    CoordinationMode mode;
    size_t min_marks_for_concurrent_read{0};

    bool no_more_tasks{false};
    RangesInDataPartsDescription request;
    RangesInDataPartsDescription buffered_tasks;

    std::mutex mutex;
    std::condition_variable can_go;
};

using MergeTreeInOrderReadPoolParallelReplicasPtr = std::shared_ptr<MergeTreeInOrderReadPoolParallelReplicas>;


}
