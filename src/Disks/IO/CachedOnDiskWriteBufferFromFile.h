#pragma once

#include <IO/WriteBufferFromFileDecorator.h>
#include <IO/WriteSettings.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/FilesystemCacheLog.h>

namespace Poco
{
class Logger;
}

namespace DB
{

/**
* We want to write eventually some size, which is not known until the very end.
* Therefore we allocate file segments lazily. Each file segment is assigned capacity
* of max_file_segment_size, but reserved_size remains 0, until call to tryReserve().
* Once current file segment is full (reached max_file_segment_size), we allocate a
* new file segment. All allocated file segments resize in file segments holder.
* If at the end of all writes, the last file segment is not full, then it is resized.
*/
class FileSegmentRangeWriter
{
public:
    FileSegmentRangeWriter(
        FileCache * cache_,
        const FileSegment::Key & key_,
        const FileCacheUserInfo & user_,
        std::shared_ptr<FilesystemCacheLog> cache_log_,
        const String & query_id_,
        const String & source_path_);

    /**
    * Write a range of file segments. Allocate file segment of `max_file_segment_size` and write to
    * it until it is full and then allocate next file segment.
    */
    bool write(const char * data, size_t size, size_t offset, FileSegmentKind segment_kind);

    void finalize();

    ~FileSegmentRangeWriter();

private:
    FileSegment & allocateFileSegment(size_t offset, FileSegmentKind segment_kind);

    void appendFilesystemCacheLog(const FileSegment & file_segment);

    void completeFileSegment();

    FileCache * cache;
    FileSegment::Key key;

    LoggerPtr log;
    std::shared_ptr<FilesystemCacheLog> cache_log;
    String query_id;
    String source_path;
    FileCacheUserInfo user;

    FileSegmentsHolderPtr file_segments;

    size_t expected_write_offset = 0;

    bool finalized = false;
};


/**
 *  Write buffer for filesystem caching on write operations.
 */
class CachedOnDiskWriteBufferFromFile final : public WriteBufferFromFileDecorator
{
public:
    CachedOnDiskWriteBufferFromFile(
        std::unique_ptr<WriteBuffer> impl_,
        FileCachePtr cache_,
        const String & source_path_,
        const FileCacheKey & key_,
        const String & query_id_,
        const WriteSettings & settings_,
        const FileCacheUserInfo & user_,
        std::shared_ptr<FilesystemCacheLog> cache_log_);

    void nextImpl() override;

    void finalizeImpl() override;

    bool cachingStopped() const { return cache_in_error_state_or_disabled; }

private:
    void cacheData(char * data, size_t size, bool throw_on_error);

    LoggerPtr log;

    FileCachePtr cache;
    String source_path;
    FileCacheKey key;

    size_t current_download_offset = 0;
    const String query_id;
    const FileCacheUserInfo user;

    bool throw_on_error_from_cache;
    bool cache_in_error_state_or_disabled = false;

    std::unique_ptr<FileSegmentRangeWriter> cache_writer;
    std::shared_ptr<FilesystemCacheLog> cache_log;
};

}
