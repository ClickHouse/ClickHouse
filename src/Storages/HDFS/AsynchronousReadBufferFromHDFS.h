#pragma once

#include <Common/config.h>

#if USE_HDFS
#include <string>
#include <memory>

#include <hdfs/hdfs.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <base/types.h>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/AsynchronousReader.h>
#include <IO/SeekableReadBuffer.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Interpreters/Context.h>

namespace DB
{

class AsynchronousReadBufferFromHDFS : public BufferWithOwnMemory<SeekableReadBufferWithSize>
{
public:
    AsynchronousReadBufferFromHDFS(
        AsynchronousReaderPtr reader_,
        const ReadSettings & settings_,
        std::shared_ptr<ReadBufferFromHDFS> impl_);
        // size_t min_bytes_for_seek_ = DBMS_DEFAULT_BUFFER_SIZE);

    ~AsynchronousReadBufferFromHDFS() override;

    off_t seek(off_t offset_, int whence) override;

    void prefetch() override;

    std::optional<size_t> getTotalSize() override;

    off_t getPosition() override;

    size_t getFileOffsetOfBufferEnd() const override;

    // void setReadUntilPosition(size_t position) override; /// [..., position).
    // void setReadUntilEnd() override;
    // String getInfoForLog() override;

private:
    class StatusGuard
    {
    public:
        StatusGuard(AsynchronousReadBufferFromHDFS * buf_, const String & action_) : buf(buf_), action(action_)
        {
            std::cout << "before " << buf->getStatus(action) << std::endl;
        }

        ~StatusGuard() { std::cout << "after " << buf->getStatus(action) << std::endl; }

    private:
        AsynchronousReadBufferFromHDFS * buf;
        String action;
    };

    String getId() const { return std::to_string(reinterpret_cast<std::uintptr_t>(this)); }
    String getStatus(const String & action);

    bool nextImpl() override;

    void finalize();

    bool hasPendingDataToRead();

    std::future<IAsynchronousReader::Result> readInto(char * data, size_t size);

    AsynchronousReaderPtr reader;
    Int32 priority;
    std::shared_ptr<ReadBufferFromHDFS> impl;
    std::future<IAsynchronousReader::Result> prefetch_future;
    Memory<> prefetch_buffer;

    size_t file_offset_of_buffer_end = 0;
    // size_t min_bytes_for_seek;
    // size_t bytes_to_ignore = 0;
    std::optional<size_t> read_until_position;

    Poco::Logger * log;
};

}
#endif
