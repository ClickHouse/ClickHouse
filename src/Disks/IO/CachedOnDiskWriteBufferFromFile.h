#pragma once

#include <IO/WriteBufferFromFileDecorator.h>
#include <IO/WriteSettings.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Common/filesystemHelpers.h>

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
        FileCache * cache_, const FileSegment::Key & key_,
        std::shared_ptr<FilesystemCacheLog> cache_log_, const String & query_id_, const String & source_path_);

    /* Write a range of file segments.
     * Allocate file segment of `max_file_segment_size` and write to it until it is full and then allocate next file segment.
     * If it's impossible to allocate new file segment and reserve space to write all data, then returns false.
     *
     * Note: the data that was written to file segments before the error occurred is not rolled back.
     */
    bool write(const char * data, size_t size, size_t offset, FileSegmentKind segment_kind);

    /* Tries to write data to current file segment.
     * Size of written data may be less than requested_size, because it may not be enough space.
     *
     * Returns size of written data.
     */
    size_t tryWrite(const char * data, size_t size, size_t offset, FileSegmentKind segment_kind = FileSegmentKind::Regular, bool strict = false);

    /// Same as `write/tryWrite`, but doesn't write anything, just reserves some space in cache
    bool reserve(size_t size, size_t offset);
    size_t tryReserve(size_t size, size_t offset);

    void finalize();

    size_t currentOffset() const { return current_file_segment_write_offset; }

    ~FileSegmentRangeWriter();

private:
    FileSegments::iterator allocateFileSegment(size_t offset, FileSegmentKind segment_kind);

    void appendFilesystemCacheLog(const FileSegment & file_segment);

    void completeFileSegment(FileSegment & file_segment, std::optional<FileSegment::State> state = {});

    /* Writes data to current file segment as much as possible and returns size of written data, do not allocate new file segments
     * In `strict` mode it will write all data or nothing, otherwise it will write as much as possible
     * If returned non zero value, then we can try to write again to next file segment.
     * If no space is available, returns zero.
     */
    size_t tryWriteImpl(const char * data, size_t size, size_t offset, FileSegmentKind segment_kind, bool strict);

    FileCache * cache;
    FileSegment::Key key;

    std::shared_ptr<FilesystemCacheLog> cache_log;
    String query_id;
    String source_path;

    FileSegmentsHolder file_segments_holder{};
    FileSegments::iterator current_file_segment_it;

    size_t current_file_segment_write_offset = 0;

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
        const FileCache::Key & key_,
        bool is_persistent_cache_file_,
        const String & query_id_,
        const WriteSettings & settings_);

    void nextImpl() override;

    void finalizeImpl() override;

private:
    void cacheData(char * data, size_t size);

    Poco::Logger * log;

    FileCachePtr cache;
    String source_path;
    FileCache::Key key;

    bool is_persistent_cache_file;
    size_t current_download_offset = 0;
    const String query_id;

    bool enable_cache_log;

    bool cache_in_error_state_or_disabled = false;

    std::unique_ptr<FileSegmentRangeWriter> cache_writer;
};

}
