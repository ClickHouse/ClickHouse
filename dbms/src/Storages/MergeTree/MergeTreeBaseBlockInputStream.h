#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class MergeTreeReader;
class UncompressedCache;
class MarkCache;


/// Base class for MergeTreeThreadBlockInputStream and MergeTreeBlockInputStream
class MergeTreeBaseBlockInputStream : public IProfilingBlockInputStream
{
public:
    MergeTreeBaseBlockInputStream(
        MergeTreeData & storage,
        const ExpressionActionsPtr & prewhere_actions,
        const String & prewhere_column,
        size_t max_block_size_rows,
        size_t preferred_block_size_bytes,
        size_t preferred_max_column_in_block_size_bytes,
        size_t min_bytes_to_use_direct_io,
        size_t max_read_buffer_size,
        bool use_uncompressed_cache,
        bool save_marks_in_cache = true,
        const Names & virt_column_names = {});

    ~MergeTreeBaseBlockInputStream() override;

protected:

    Block readImpl() override final;

    /// Creates new this->task, and initilizes readers
    virtual bool getNewTask() = 0;

    /// We will call progressImpl manually.
    void progress(const Progress &) override {}

    Block readFromPart();

    void injectVirtualColumns(Block & block);

protected:

    MergeTreeData & storage;

    ExpressionActionsPtr prewhere_actions;
    String prewhere_column_name;

    size_t max_block_size_rows;
    size_t preferred_block_size_bytes;
    size_t preferred_max_column_in_block_size_bytes;

    size_t min_bytes_to_use_direct_io;
    size_t max_read_buffer_size;

    bool use_uncompressed_cache;
    bool save_marks_in_cache;

    Names virt_column_names;

    std::unique_ptr<MergeTreeReadTask> task;

    std::shared_ptr<UncompressedCache> owned_uncompressed_cache;
    std::shared_ptr<MarkCache> owned_mark_cache;

    using MergeTreeReaderPtr = std::unique_ptr<MergeTreeReader>;
    MergeTreeReaderPtr reader;
    MergeTreeReaderPtr pre_reader;

    size_t max_block_size_marks;
};

}
