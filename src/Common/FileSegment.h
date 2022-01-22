#pragma once

#include <boost/noncopyable.hpp>
#include <Core/Types.h>
#include <IO/WriteBufferFromFile.h>
#include <list>


namespace DB
{

class FileCache;
using FileCacheKey = UInt128;

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentPtr>;


class FileSegment : boost::noncopyable
{
friend class LRUFileCache;

public:
    enum class State
    {
        DOWNLOADED,
        /**
         * When file segment is first created and returned to user, it has state EMPTY.
         * EMPTY state can become DOWNLOADING when getOrSetDownaloder is called successfully
         * by any owner of EMPTY state file segment.
         */
        EMPTY,
        /**
         * A newly created file segment never has DOWNLOADING state until call to getOrSetDownloader
         * because each cache user might acquire multiple file segments and reads them one by one,
         * so only user which actually needs to read this segment earlier than others - becomes a downloader.
         */
        DOWNLOADING,
        /**
         * Space reservation for a file segment is incremental, i.e. downaloder reads buffer_size bytes
         * from remote fs -> tries to reserve buffer_size bytes to put them to cache -> writes to cache
         * on successfull reservation and stops cache write otherwise. If some users were waiting for
         * current segment will read partially downloaded part from cache and remaining part directly from remote fs.
         */
        PARTIALLY_DOWNLOADED_NO_CONTINUATION,
        /**
         * If downloader did not finish download if current file segment for any reason apart from running
         * out of cache space, then download can be continued by other owners of this file segment.
         */
        PARTIALLY_DOWNLOADED,
        /**
         * If file segment cannot possibly be downloaded (first space reservation attempt failed), mark
         * this file segment as out of cache scope.
         */
        SKIP_CACHE,
    };

    FileSegment(size_t offset_, size_t size_, const FileCacheKey & key_, FileCache * cache_, State download_state_);

    State state() const
    {
        std::lock_guard lock(mutex);
        return download_state;
    }

    static String toString(FileSegment::State state);

    /// Represents an interval [left, right] including both boundaries.
    struct Range
    {
        size_t left;
        size_t right;

        Range(size_t left_, size_t right_) : left(left_), right(right_) {}

        size_t size() const { return right - left + 1; }

        String toString() const { return '[' + std::to_string(left) + ',' + std::to_string(right) + ']'; }
    };

    const Range & range() const { return segment_range; }

    const FileCacheKey & key() const { return file_key; }

    size_t downloadedSize() const;

    size_t reservedSize() const;

    State wait();

    bool reserve(size_t size);

    void write(const char * from, size_t size);

    void complete();

    String getOrSetDownloader();

    bool isDownloader() const;

    static String getCallerId();

private:
    size_t available() const { return reserved_size - downloaded_size; }

    Range segment_range;

    State download_state; /// Protected by mutex and cache->mutex
    String downloader_id;

    std::unique_ptr<WriteBufferFromFile> download_buffer;

    size_t downloaded_size = 0;
    size_t reserved_size = 0;

    mutable std::mutex mutex;
    std::condition_variable cv;

    FileCacheKey file_key;
    FileCache * cache;
};

struct FileSegmentsHolder : boost::noncopyable
{
    explicit FileSegmentsHolder(FileSegments && file_segments_) : file_segments(file_segments_) {}
    FileSegmentsHolder(FileSegmentsHolder && other) : file_segments(std::move(other.file_segments)) {}

    ~FileSegmentsHolder()
    {
        /// CacheableReadBufferFromRemoteFS removes completed file segments from FileSegmentsHolder, so
        /// in destruction here remain only uncompleted file segments.

        for (auto & segment : file_segments)
        {
            /// In general file segment is completed by downloader by calling segment->complete()
            /// for each segment once it has been downloaded or failed to download.
            /// But if not done by downloader, downloader's holder will do that.

            if (segment && segment->isDownloader())
                segment->complete();
        }
    }

    FileSegments file_segments;
};

}
