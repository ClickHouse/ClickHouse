#pragma once

#include <DataStreams/IBlockInputStream.h>

#include <Storages/FileLog/ReadBufferFromFileLog.h>
#include <Storages/FileLog/StorageFileLog.h>


namespace Poco
{
    class Logger;
}
namespace DB
{
class FileLogBlockInputStream : public IBlockInputStream
{
public:
    FileLogBlockInputStream(
        StorageFileLog & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::shared_ptr<Context> & context_,
        const Names & columns,
        Poco::Logger * log_,
        size_t max_block_size_);
    ~FileLogBlockInputStream() override = default;

    String getName() const override { return storage.getName(); }
    Block getHeader() const override;

    void readPrefixImpl() override;
    Block readImpl() override;
    void readSuffixImpl() override;

private:
    StorageFileLog & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    Names column_names;
    Poco::Logger * log;
    UInt64 max_block_size;

    ReadBufferFromFileLogPtr buffer;
    bool finished = false;

    const Block non_virtual_header;
    const Block virtual_header;
};

}
