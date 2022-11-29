#pragma once

#include <Common/ThreadPool.h>
#include <Storages/MergeTree/IMergeTreeReadPool.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Core/BackgroundSchedulePool.h>
#include <queue>

namespace Poco { class Logger; }

namespace DB
{

struct RangesInDataPart;
using RangesInDataParts = std::vector<RangesInDataPart>;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;
struct PrewhereInfo;
using PrewhereInfoPtr = std::shared_ptr<PrewhereInfo>;
struct MergeTreeReaderSettings;
class MarkCache;
class UncompressedCache;
class IMergeTreeReader;
using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
struct MarkRange;
using MarkRanges = std::deque<MarkRange>;


class MergeTreePrefetchedReadPool : public IMergeTreeReadPool, private WithContext
{
public:
    MergeTreePrefetchedReadPool(
        size_t threads,
        size_t sum_marks_,
        size_t min_marks_for_concurrent_read_,
        RangesInDataParts && parts_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const Names & column_names_,
        const Names & virtual_column_names_,
        size_t preferred_block_size_bytes_,
        const MergeTreeReaderSettings & reader_settings_,
        ContextPtr context_,
        bool use_uncompressed_cache_);

    MergeTreeReadTaskPtr getTask(size_t min_marks_to_read, size_t thread) override;

    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

    Block getHeader() const override { return header; }

private:
    struct PartInfo;
    using PartInfoPtr = std::shared_ptr<PartInfo>;
    using PartsInfos = std::vector<PartInfoPtr>;
    using ThreadsTasks = std::map<size_t, std::deque<MergeTreeReadTaskPtr>>;

    std::future<MergeTreeReaderPtr> createReader(
        const PartInfo & part,
        const NamesAndTypesList & columns,
        const MarkRanges & required_ranges) const;

    PartsInfos getPartsInfos(
        const RangesInDataParts & parts,
        const PrewhereInfoPtr & prewhere_info,
        const Names & virtual_column_names,
        size_t preferred_block_size_bytes) const;

    ThreadsTasks createThreadsTasks(
        size_t threads,
        size_t sum_marks,
        size_t min_marks_for_concurrent_read,
        const PrewhereInfoPtr & prewhere_info) const;

    static MarkRanges getMarkRangesToRead(size_t need_marks, PartInfo & part);

    mutable std::mutex mutex;
    Poco::Logger * log;

    Block header;
    StorageSnapshotPtr storage_snapshot;
    MarkCache * mark_cache;
    UncompressedCache * uncompressed_cache;
    MergeTreeReaderSettings reader_settings;
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    ThreadPool & prefetch_threadpool;

    /// Saved here, because MergeTreeReadTask in threads_tasks
    /// holds reference to its values.
    Names column_names;
    PartsInfos parts_infos;

    ThreadsTasks threads_tasks;
};

}
