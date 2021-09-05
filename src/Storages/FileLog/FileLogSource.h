#pragma once

#include <Processors/Sources/SourceWithProgress.h>

#include <Storages/FileLog/ReadBufferFromFileLog.h>
#include <Storages/FileLog/StorageFileLog.h>


namespace Poco
{
    class Logger;
}
namespace DB
{
class FileLogSource : public SourceWithProgress
{
public:
    FileLogSource(
        StorageFileLog & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const ContextPtr & context_,
        const Names & columns,
        size_t max_block_size_,
        size_t poll_time_out_,
        size_t stream_number_,
        size_t max_streams_number_);

    String getName() const override { return "FileLog"; }

    bool noRecords() { return !buffer || buffer->noRecords(); }

protected:
    Chunk generate() override;

private:
    StorageFileLog & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    Names column_names;
    UInt64 max_block_size;

    size_t poll_time_out;

    std::unique_ptr<ReadBufferFromFileLog> buffer;

    Block non_virtual_header;
    const NamesAndTypesList column_names_and_types;
};

}
