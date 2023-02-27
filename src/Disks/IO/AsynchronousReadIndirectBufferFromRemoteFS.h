#pragma once

#include "config.h"
#include <IO/ReadBufferFromFile.h>
#include <IO/AsynchronousReader.h>
#include <IO/ReadSettings.h>
#include <Interpreters/FilesystemReadPrefetchesLog.h>
#include <utility>

namespace Poco { class Logger; }

namespace DB
{

class ReadBufferFromRemoteFSGather;

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
* -> ReadIndirectBufferFromWebServer -> ReadBufferFromHTTP -> ReadBufferFromIStream.
*
* We pass either `memory` or `prefetch_buffer` through all this chain and return it back.
*/
class AsynchronousReadIndirectBufferFromRemoteFS : public ReadBufferFromFileBase
{
public:
    explicit AsynchronousReadIndirectBufferFromRemoteFS(
        IAsynchronousReader & reader_, const ReadSettings & settings_,
        std::shared_ptr<ReadBufferFromRemoteFSGather> impl_,
        size_t min_bytes_for_seek = DBMS_DEFAULT_BUFFER_SIZE);

    ~AsynchronousReadIndirectBufferFromRemoteFS() override;

    off_t seek(off_t offset_, int whence) override;

    off_t getPosition() override { return file_offset_of_buffer_end - available(); }

    String getFileName() const override;

    void prefetch(int64_t priority) override;

    void setReadUntilPosition(size_t position) override; /// [..., position).

    void setReadUntilEnd() override;

    String getInfoForLog() override;

    size_t getFileSize() override;

    bool isIntegratedWithFilesystemCache() const override { return true; }

private:
    bool nextImpl() override;

    void finalize();

    bool hasPendingDataToRead();

    void appendToPrefetchLog(FilesystemPrefetchState state, int64_t size, const std::unique_ptr<Stopwatch> & execution_watch);

    std::future<IAsynchronousReader::Result> asyncReadInto(char * data, size_t size, int64_t priority);

    void resetPrefetch(FilesystemPrefetchState state);

    ReadSettings read_settings;

    IAsynchronousReader & reader;

    int64_t base_priority;

    std::shared_ptr<ReadBufferFromRemoteFSGather> impl;

    std::future<IAsynchronousReader::Result> prefetch_future;

    size_t file_offset_of_buffer_end = 0;

    Memory<> prefetch_buffer;

    size_t min_bytes_for_seek;

    std::string query_id;

    std::string current_reader_id;

    size_t bytes_to_ignore = 0;

    std::optional<size_t> read_until_position;

    Poco::Logger * log;

    struct LastPrefetchInfo
    {
        UInt64 submit_time = 0;
        size_t priority = 0;
    };
    LastPrefetchInfo last_prefetch_info;
};

}
