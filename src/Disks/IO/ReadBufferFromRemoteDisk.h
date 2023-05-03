#pragma once

#include <Client/RemoteFSConnectionPool.h>
#include <IO/ReadBufferFromFileBase.h>

namespace DB
{

class ReadBufferFromRemoteDisk : public ReadBufferFromFileBase
{
public:
    explicit ReadBufferFromRemoteDisk(
        RemoteFSConnectionPool::Entry & conn_,
        const String & file_name_,
        const ReadSettings & settings_);

    bool nextImpl() override;

    off_t seek(off_t offset, int whence) override;

    off_t getPosition() override;

    String getFileName() const override { return file_name; }

    size_t getFileOffsetOfBufferEnd() const override { return offset_of_buffer_end; }

    Range getRemainingReadRange() const override { return Range{ .left = offset_of_buffer_end, .right = std::nullopt }; }

private:
    Poco::Logger * log;

    size_t buf_size;
    String file_name;

    RemoteFSConnectionPool::Entry conn;

    size_t offset_of_buffer_end = 0;
};

}
