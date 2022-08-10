#pragma once

#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/RequestResponse.h>

#include <Processors/ISource.h>


namespace DB
{

class IMergeTreeReader;
class UncompressedCache;
class MarkCache;
struct PrewhereExprInfo;


struct ParallelReadingExtension
{
    MergeTreeReadTaskCallback callback;
    size_t count_participating_replicas{0};
    size_t number_of_current_replica{0};
    /// This is needed to estimate the number of bytes
    /// between a pair of marks to perform one request
    /// over the network for a 1Gb of data.
    Names colums_to_read;
};

/// Base class for MergeTreeThreadSelectProcessor and MergeTreeSelectProcessor
class MergeTreeBaseSelectProcessor : public ISource
{
public:
    MergeTreeBaseSelectProcessor(
        Block header,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        ExpressionActionsSettings actions_settings,
        UInt64 max_block_size_rows_,
        UInt64 preferred_block_size_bytes_,
        UInt64 preferred_max_column_in_block_size_bytes_,
        const MergeTreeReaderSettings & reader_settings_,
        bool use_uncompressed_cache_,
        const Names & virt_column_names_ = {},
        std::optional<ParallelReadingExtension> extension = {});

    ~MergeTreeBaseSelectProcessor() override;

    static Block transformHeader(
        Block block, const PrewhereInfoPtr & prewhere_info, const DataTypePtr & partition_value_type, const Names & virtual_columns);

    static std::unique_ptr<MergeTreeBlockSizePredictor> getSizePredictor(
        const MergeTreeData::DataPartPtr & data_part,
        const MergeTreeReadTaskColumns & task_columns,
        const Block & sample_block);

protected:

    Chunk generate() final;

    /// Creates new this->task and return a flag whether it was successful or not
    virtual bool getNewTaskImpl() = 0;
    /// Creates new readers for a task it is needed. These methods are separate, because
    /// in case of parallel reading from replicas the whole task could be denied by a coodinator
    /// or it could modified somehow.
    virtual void finalizeNewTask() = 0;

    size_t estimateMaxBatchSizeForHugeRanges();

    virtual bool canUseConsistentHashingForParallelReading() { return false; }

    /// Closes readers and unlock part locks
    virtual void finish() = 0;

    virtual Chunk readFromPart();

    Chunk readFromPartImpl();

    /// Two versions for header and chunk.
    static void
    injectVirtualColumns(Block & block, MergeTreeReadTask * task, const DataTypePtr & partition_value_type, const Names & virtual_columns);
    static void
    injectVirtualColumns(Chunk & chunk, MergeTreeReadTask * task, const DataTypePtr & partition_value_type, const Names & virtual_columns);

    void initializeRangeReaders(MergeTreeReadTask & task);

    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;

    PrewhereInfoPtr prewhere_info;
    std::unique_ptr<PrewhereExprInfo> prewhere_actions;

    UInt64 max_block_size_rows;
    UInt64 preferred_block_size_bytes;
    UInt64 preferred_max_column_in_block_size_bytes;

    MergeTreeReaderSettings reader_settings;

    bool use_uncompressed_cache;

    Names virt_column_names;

    /// These columns will be filled by the merge tree range reader
    Names non_const_virtual_column_names;

    DataTypePtr partition_value_type;

    /// This header is used for chunks from readFromPart().
    Block header_without_virtual_columns;

    std::shared_ptr<UncompressedCache> owned_uncompressed_cache;
    std::shared_ptr<MarkCache> owned_mark_cache;

    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
    MergeTreeReaderPtr reader;
    MergeTreeReaderPtr pre_reader;

    MergeTreeReadTaskPtr task;

    std::optional<ParallelReadingExtension> extension;
    bool no_more_tasks{false};
    std::deque<MergeTreeReadTaskPtr> delayed_tasks;
    std::deque<MarkRanges> buffered_ranges;

private:
    Poco::Logger * log = &Poco::Logger::get("MergeTreeBaseSelectProcessor");

    enum class Status
    {
        Accepted,
        Cancelled,
        Denied
    };

    /// Calls getNewTaskImpl() to get new task, then performs a request to a coordinator
    /// The coordinator may modify the set of ranges to read from a part or could
    /// deny the whole request. In the latter case it creates new task and retries.
    /// Then it calls finalizeNewTask() to create readers for a task if it is needed.
    bool getNewTask();
    bool getNewTaskParallelReading();

    /// After PK analysis the range of marks could be extremely big
    /// We divide this range to a set smaller consecutive ranges
    /// Then, depending on the type of reading (concurrent, in order or in reverse order)
    /// we can calculate a consistent hash function with the number of buckets equal to
    /// the number of replicas involved. And after that we can throw away some ranges with
    /// hash not equals to the number of the current replica.
    bool getTaskFromBuffer();

    /// But we can't throw that ranges completely, because if we have different sets of parts
    /// on replicas (have merged part on one, but not on another), then such a situation is possible
    /// - Coordinator allows to read from a big merged part, but this part is present only on one replica.
    ///   And that replica calculates consistent hash and throws away some ranges
    /// - Coordinator denies other replicas to read from another parts (source parts for that big one)
    /// At the end, the result of the query is wrong, because we didn't read all the data.
    /// So, we have to remember parts and mark ranges with hash different then current replica number.
    /// An we have to ask the coordinator about its permission to read from that "delayed" parts.
    /// It won't work with reading in order or reading in reverse order, because we can possibly seek back.
    bool getDelayedTasks();

    /// It will form a request a request to coordinator and
    /// then reinitialize the mark ranges of this->task object
    Status performRequestToCoordinator(MarkRanges requested_ranges, bool delayed);

    void splitCurrentTaskRangesAndFillBuffer();

};

}
