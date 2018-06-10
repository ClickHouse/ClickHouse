#pragma once
#include <Storages/MergeTree/MergeTreeBaseBlockInputStream.h>


namespace DB
{

class MergeTreeReadPool;


/** Used in conjunction with MergeTreeReadPool, asking it for more work to do and performing whatever reads it is asked
  * to perform.
  */
class MergeTreeThreadBlockInputStream : public MergeTreeBaseBlockInputStream
{
public:
    MergeTreeThreadBlockInputStream(
        const size_t thread,
        const std::shared_ptr<MergeTreeReadPool> & pool,
        const size_t min_marks_to_read,
        const size_t max_block_size,
        size_t preferred_block_size_bytes,
        size_t preferred_max_column_in_block_size_bytes,
        MergeTreeData & storage,
        const bool use_uncompressed_cache,
        const ExpressionActionsPtr & prewhere_actions,
        const String & prewhere_column,
        const Settings & settings,
        const Names & virt_column_names);

    String getName() const override { return "MergeTreeThread"; }

    ~MergeTreeThreadBlockInputStream() override;

    Block getHeader() const override;

protected:
    /// Requests read task from MergeTreeReadPool and signals whether it got one
    bool getNewTask() override;

    /// "thread" index (there are N threads and each thread is assigned index in interval [0..N-1])
    size_t thread;

    std::shared_ptr<MergeTreeReadPool> pool;
    size_t min_marks_to_read;
};

}
