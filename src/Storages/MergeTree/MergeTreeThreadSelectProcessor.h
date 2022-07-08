#pragma once
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>


namespace DB
{

class MergeTreeReadPool;


/** Used in conjunction with MergeTreeReadPool, asking it for more work to do and performing whatever reads it is asked
  * to perform.
  */
class MergeTreeThreadSelectProcessor final : public MergeTreeBaseSelectProcessor
{
public:
    MergeTreeThreadSelectProcessor(
        size_t thread_,
        const std::shared_ptr<MergeTreeReadPool> & pool_,
        size_t min_marks_to_read_,
        UInt64 max_block_size_,
        size_t preferred_block_size_bytes_,
        size_t preferred_max_column_in_block_size_bytes_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        bool use_uncompressed_cache_,
        const PrewhereInfoPtr & prewhere_info_,
        ExpressionActionsSettings actions_settings,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & virt_column_names_,
        std::optional<ParallelReadingExtension> extension_);

    String getName() const override { return "MergeTreeThread"; }

    ~MergeTreeThreadSelectProcessor() override;

protected:
    /// Requests read task from MergeTreeReadPool and signals whether it got one
    bool getNewTaskImpl() override;

    void finalizeNewTask() override;

    void finish() override;

    bool canUseConsistentHashingForParallelReading() override { return true; }

private:
    /// "thread" index (there are N threads and each thread is assigned index in interval [0..N-1])
    size_t thread;

    std::shared_ptr<MergeTreeReadPool> pool;
    size_t min_marks_to_read;

    /// Last part read in this thread
    std::string last_readed_part_name;
    /// Names from header. Used in order to order columns in read blocks.
    Names ordered_names;
};

}
