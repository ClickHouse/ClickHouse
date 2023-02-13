#pragma once
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>


namespace DB
{

class IMergeTreeReadPool;
using IMergeTreeReadPoolPtr = std::shared_ptr<IMergeTreeReadPool>;

/** Used in conjunction with MergeTreeReadPool, asking it for more work to do and performing whatever reads it is asked
  * to perform.
  */
class MergeTreeThreadSelectAlgorithm final : public IMergeTreeSelectAlgorithm
{
public:
    MergeTreeThreadSelectAlgorithm(
        size_t thread_,
        IMergeTreeReadPoolPtr pool_,
        size_t min_marks_for_concurrent_read,
        size_t max_block_size_,
        size_t preferred_block_size_bytes_,
        size_t preferred_max_column_in_block_size_bytes_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        bool use_uncompressed_cache_,
        const PrewhereInfoPtr & prewhere_info_,
        ExpressionActionsSettings actions_settings,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & virt_column_names_);

    String getName() const override { return "MergeTreeThread"; }

    ~MergeTreeThreadSelectAlgorithm() override;

protected:
    /// Requests read task from MergeTreeReadPool and signals whether it got one
    bool getNewTaskImpl() override;

    void finalizeNewTask() override;

    void finish() override;

private:
    /// "thread" index (there are N threads and each thread is assigned index in interval [0..N-1])
    size_t thread;

    IMergeTreeReadPoolPtr pool;

    /// Last part read in this thread
    std::string last_read_part_name;
};

}
