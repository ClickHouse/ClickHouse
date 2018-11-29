#pragma once
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Storages/MergeTree/MarkRange.h>
#include <memory>

namespace DB
{

/// Lightweight (in terms of logic) stream for reading single part from MergeTree
class MergeTreeSequentialBlockInputStream : public IProfilingBlockInputStream
{
public:
    MergeTreeSequentialBlockInputStream(
        const MergeTreeData & storage_,
        const MergeTreeData::DataPartPtr & data_part_,
        Names columns_to_read_,
        bool read_with_direct_io_,
        bool take_column_types_from_storage,
        bool quiet = false
    );

    ~MergeTreeSequentialBlockInputStream() override;

    String getName() const override { return "MergeTreeSequentialBlockInputStream"; }

    Block getHeader() const override;

    /// Closes readers and unlock part locks
    void finish();

    size_t getCurrentMark() const { return current_mark; }

    size_t getCurrentRow() const { return current_row; }

protected:
    Block readImpl() override;

private:

    const MergeTreeData & storage;

    Block header;

    /// Data part will not be removed if the pointer owns it
    MergeTreeData::DataPartPtr data_part;

    /// Forbids to change columns list of the part during reading
    std::shared_lock<std::shared_mutex> part_columns_lock;

    /// Columns we have to read (each Block from read will contain them)
    Names columns_to_read;

    /// Should read using direct IO
    bool read_with_direct_io;

    Logger * log = &Logger::get("MergeTreeSequentialBlockInputStream");

    std::shared_ptr<MarkCache> mark_cache;
    using MergeTreeReaderPtr = std::unique_ptr<MergeTreeReader>;
    MergeTreeReaderPtr reader;

    /// current mark at which we stop reading
    size_t current_mark = 0;

    /// current row at which we stop reading
    size_t current_row = 0;

private:
    void fixHeader(Block & header_block) const;

};

}
