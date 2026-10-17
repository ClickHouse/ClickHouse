#pragma once

#include <Core/StreamingHandleErrorMode.h>
#include <Processors/ISource.h>
#include <Storages/FileLog/FileLogConsumer.h>
#include <Storages/FileLog/StorageFileLog.h>

namespace Poco
{
    class Logger;
}
namespace DB
{
class FileLogSource final : public ISource
{
public:
    FileLogSource(
        StorageFileLog & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Names & columns,
        size_t max_block_size_,
        size_t poll_time_out_,
        size_t stream_number_,
        size_t max_streams_number_,
        StreamingHandleErrorMode handle_error_mode_,
        bool skip_broken_records_);

    String getName() const override { return "FileLog"; }

    bool noRecords() { return !consumer || consumer->noRecords(); }

    void close();

    ~FileLogSource() override;

protected:
    Chunk generate() override;

private:
    Chunk generateImpl();

    struct ReadPosition
    {
        UInt64 offset;
        StorageFileLog::FileStatus status;
    };

    /// Closes the files [start, end) without storing their read positions, and moves them back to `positions`.
    void closeAndRestoreReadPositions(const std::vector<ReadPosition> & positions);

    StorageFileLog & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    Names column_names;
    UInt64 max_block_size;

    size_t poll_time_out;

    size_t stream_number;
    size_t max_streams_number;
    StreamingHandleErrorMode handle_error_mode;
    bool skip_broken_records;

    std::unique_ptr<FileLogConsumer> consumer;

    Block non_virtual_header;
    Block virtual_header;

    /// The start pos and end pos of files responsible by this stream,
    /// does not include end.
    size_t start;
    size_t end;

    bool closed = false;
};

}
