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

class AsynchronousReadBufferFromHDFS : public BufferWithOwnMemory<SeekableReadBuffer>, public WithFileSize
{
public:
    AsynchronousReadBufferFromHDFS(
        AsynchronousReaderPtr reader_,
        const ReadSettings & settings_,
        std::shared_ptr<ReadBufferFromHDFS> impl_);

    ~AsynchronousReadBufferFromHDFS() override;

    off_t seek(off_t offset_, int whence) override;

    void prefetch() override;

    std::optional<size_t> getFileSize() override;

    off_t getPosition() override;

    size_t getFileOffsetOfBufferEnd() const override;

private:
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
    std::optional<size_t> read_until_position;

    Poco::Logger * log;
};

}
#endif
