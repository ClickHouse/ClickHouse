#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <IO/ReadBufferFromFile.h>
#include <IO/AsynchronousReader.h>
#include <Disks/ReadBufferFromRemoteFSGather.h>
#include <Disks/IDiskRemote.h>
#include <utility>


namespace DB
{

/**
* Reads data from S3/HDFS/Web using stored paths in metadata.
* This class is an asynchronous version of ReadIndirectBufferFromRemoteFS.
*
* Buffers chain for diskS3:
* AsynchronousIndirectReadBufferFromRemoteFS -> ReadBufferFromRemoteFS ->
* -> ReadBufferFromS3 -> ReadBufferFromIStream.
*
* Buffers chain for diskWeb:
* AsynchronousIndirectReadBufferFromRemoteFS -> ReadBufferFromRemoteFS ->
* -> ReadIndirectBufferFromWebServer -> ReadBufferFromHttp -> ReadBufferFromIStream.
*
* We pass either `memory` or `prefetch_buffer` through all this chain and return it back.
*/
class AsynchronousReadIndirectBufferFromRemoteFS : public ReadBufferFromFileBase
{
public:
    explicit AsynchronousReadIndirectBufferFromRemoteFS(
        AsynchronousReaderPtr reader_, Int32 priority_,
        std::shared_ptr<ReadBufferFromRemoteFSGather> impl_,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        size_t min_bytes_for_seek = 1024 * 1024);

    ~AsynchronousReadIndirectBufferFromRemoteFS() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override { return absolute_position - available(); }

    String getFileName() const override { return impl->getFileName(); }

    void prefetch() override;

    void setRightOffset(size_t offset);

private:
    bool nextImpl() override;

    void finalize();

    std::future<IAsynchronousReader::Result> readInto(char * data, size_t size);

    AsynchronousReaderPtr reader;

    Int32 priority;

    std::shared_ptr<ReadBufferFromRemoteFSGather> impl;

    std::future<IAsynchronousReader::Result> prefetch_future;

    size_t absolute_position = 0;

    Memory<> prefetch_buffer;

    String buffer_events;

    size_t min_bytes_for_seek;

    size_t bytes_to_ignore = 0;

    size_t last_offset = 0;
};

}
