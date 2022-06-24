#pragma once

#include <Common/config.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/AsynchronousReader.h>
#include <utility>

namespace Poco { class Logger; }

namespace DB
{

class ReadBufferFromRemoteFSGather;
struct ReadSettings;

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
        AsynchronousReaderPtr reader_, const ReadSettings & settings_,
        std::shared_ptr<ReadBufferFromRemoteFSGather> impl_,
        size_t min_bytes_for_seek = DBMS_DEFAULT_BUFFER_SIZE);

    ~AsynchronousReadIndirectBufferFromRemoteFS() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override { return file_offset_of_buffer_end - available(); }

    String getFileName() const override;

    void prefetch() override;

    void setReadUntilPosition(size_t position) override; /// [..., position).

    void setReadUntilEnd() override;

    String getInfoForLog() override;

    size_t getFileSize() override;

private:
    bool nextImpl() override;

    void finalize();

    bool hasPendingDataToRead();

    std::future<IAsynchronousReader::Result> asyncReadInto(char * data, size_t size);

    AsynchronousReaderPtr reader;

    Int32 priority;

    std::shared_ptr<ReadBufferFromRemoteFSGather> impl;

    std::future<IAsynchronousReader::Result> prefetch_future;

    size_t file_offset_of_buffer_end = 0;

    Memory<> prefetch_buffer;

    size_t min_bytes_for_seek;

    size_t bytes_to_ignore = 0;

    std::optional<size_t> read_until_position;

    Poco::Logger * log;
};

}
