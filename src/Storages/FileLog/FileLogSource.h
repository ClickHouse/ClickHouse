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
class FileLogSource : public ISource
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
        StreamingHandleErrorMode handle_error_mode_);

    String getName() const override { return "FileLog"; }

    bool noRecords() { return !consumer || consumer->noRecords(); }

    void close();

    ~FileLogSource() override;

protected:
    Chunk generate() override;

private:
    StorageFileLog & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    Names column_names;
    UInt64 max_block_size;

    size_t poll_time_out;

    size_t stream_number;
    size_t max_streams_number;
    StreamingHandleErrorMode handle_error_mode;

    std::unique_ptr<FileLogConsumer> consumer;

    Block non_virtual_header;
    Block virtual_header;

    /// The start pos and end pos of files responsible by this stream,
    /// does not include end.
    size_t start;
    size_t end;
};

}
