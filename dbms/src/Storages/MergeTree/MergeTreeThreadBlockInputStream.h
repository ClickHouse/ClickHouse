#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class MergeTreeReader;
class MergeTreeReadPool;
struct MergeTreeReadTask;
class UncompressedCache;
class MarkCache;


struct MergeTreeBaseBlockInputStream : public IProfilingBlockInputStream
{
    MergeTreeBaseBlockInputStream(
        MergeTreeData & storage,
        const ExpressionActionsPtr & prewhere_actions,
        const String & prewhere_column,
        size_t max_block_size_rows,
        size_t preferred_block_size_bytes,
        size_t min_bytes_to_use_direct_io,
        size_t max_read_buffer_size,
        bool use_uncompressed_cache,
        bool save_marks_in_cache = true,
        const Names & virt_column_names = {});

    ~MergeTreeBaseBlockInputStream() override;

protected:

    Block readFromPart(MergeTreeReadTask * task);

    void injectVirtualColumns(Block & block, const MergeTreeReadTask * task);

    /// We will call progressImpl manually.
    void progress(const Progress & value) override {}

protected:

    MergeTreeData & storage;

    ExpressionActionsPtr prewhere_actions;
    String prewhere_column;

    size_t max_block_size_rows;
    size_t preferred_block_size_bytes;

    size_t min_bytes_to_use_direct_io;
    size_t max_read_buffer_size;

    bool use_uncompressed_cache;
    bool save_marks_in_cache;

    std::shared_ptr<UncompressedCache> owned_uncompressed_cache;
    std::shared_ptr<MarkCache> owned_mark_cache;

    using MergeTreeReaderPtr = std::unique_ptr<MergeTreeReader>;
    MergeTreeReaderPtr reader;
    MergeTreeReaderPtr pre_reader;

    size_t max_block_size_marks;

    Names virt_column_names;

    Logger * log;
};


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
        MergeTreeData & storage,
        const bool use_uncompressed_cache,
        const ExpressionActionsPtr & prewhere_actions,
        const String & prewhere_column,
        const Settings & settings,
        const Names & virt_column_names);

    String getName() const override { return "MergeTreeThread"; }

    String getID() const override;

    ~MergeTreeThreadBlockInputStream() override;

protected:

    Block readImpl() override;

private:
    /// Requests read task from MergeTreeReadPool and signals whether it got one
    bool getNewTask();

    /// "thread" index (there are N threads and each thread is assigned index in interval [0..N-1])
    size_t thread;

    std::shared_ptr<MergeTreeReadPool> pool;
    std::shared_ptr<MergeTreeReadTask> task;
    size_t min_marks_to_read;
};


}
