#pragma once

#include <Common/config.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/AsynchronousReader.h>
#include <Disks/ObjectStorages/IObjectStorage.h>

namespace Poco { class Logger; }

namespace DB
{

/**
 * Remote disk might need to split one clickhouse file into multiple files in remote fs.
 * This class works like a proxy to allow transition from one file into multiple.
 */
class ReadBufferFromRemoteFSGather final : public ReadBuffer
{
friend class ReadIndirectBufferFromRemoteFS;

public:
    using ReadBufferCreator = std::function<std::shared_ptr<ReadBufferFromFileBase>(const std::string & path, size_t read_until_position)>;

    ReadBufferFromRemoteFSGather(
        ReadBufferCreator && read_buffer_creator_,
        const StoredObjects & blobs_to_read_,
        const ReadSettings & settings_);

    ~ReadBufferFromRemoteFSGather() override;

    String getFileName() const;

    void reset();

    void setReadUntilPosition(size_t position) override;

    IAsynchronousReader::Result readInto(char * data, size_t size, size_t offset, size_t ignore) override;

    size_t getFileSize() const;

    size_t getFileOffsetOfBufferEnd() const;

    bool initialized() const { return current_buf != nullptr; }

    String getInfoForLog();

    size_t getImplementationBufferOffset() const;

private:
    SeekableReadBufferPtr createImplementationBuffer(const String & path, size_t file_size);

    bool nextImpl() override;

    void initialize();

    bool readImpl();

    bool moveToNextBuffer();

    void appendFilesystemCacheLog();

    ReadBufferCreator read_buffer_creator;

    StoredObjects blobs_to_read;

    ReadSettings settings;

    size_t read_until_position = 0;

    String current_file_path;
    size_t current_file_size = 0;

    bool with_cache;

    String query_id;

    Poco::Logger * log;

    SeekableReadBufferPtr current_buf;

    size_t current_buf_idx = 0;

    size_t file_offset_of_buffer_end = 0;

    /**
     * File:                        |___________________|
     * Buffer:                            |~~~~~~~|
     * file_offset_of_buffer_end:                 ^
     */
    size_t bytes_to_ignore = 0;

    size_t total_bytes_read_from_current_file = 0;

    bool enable_cache_log = false;
};

}
