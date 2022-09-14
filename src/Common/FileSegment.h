#pragma once

#include <boost/noncopyable.hpp>
#include <IO/WriteBufferFromFile.h>
#include <Core/Types.h>
#include <IO/SeekableReadBuffer.h>
#include <list>

namespace Poco { class Logger; }

namespace DB
{

class IFileCache;

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentPtr>;


class FileSegment : boost::noncopyable
{
friend class LRUFileCache;
friend struct FileSegmentsHolder;

public:
    using Key = UInt128;
    using RemoteFileReaderPtr = std::shared_ptr<SeekableReadBuffer>;
    using LocalCacheWriterPtr = std::unique_ptr<WriteBufferFromFile>;

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
         * on successful reservation and stops cache write otherwise. Those, who waited for the same file
         * file segment, will read downloaded part from cache and remaining part directly from remote fs.
         */
        PARTIALLY_DOWNLOADED_NO_CONTINUATION,
        /**
         * If downloader did not finish download of current file segment for any reason apart from running
         * out of cache space, then download can be continued by other owners of this file segment.
         */
        PARTIALLY_DOWNLOADED,
        /**
         * If file segment cannot possibly be downloaded (first space reservation attempt failed), mark
         * this file segment as out of cache scope.
         */
        SKIP_CACHE,
    };

    FileSegment(
        size_t offset_, size_t size_, const Key & key_,
        IFileCache * cache_, State download_state_);

    State state() const;

    static String stateToString(FileSegment::State state);

    /// Represents an interval [left, right] including both boundaries.
    struct Range
    {
        size_t left;
        size_t right;

        Range(size_t left_, size_t right_) : left(left_), right(right_) {}

        bool operator==(const Range & other) const { return left == other.left && right == other.right; }

        size_t size() const { return right - left + 1; }

        String toString() const { return fmt::format("[{}, {}]", std::to_string(left), std::to_string(right)); }
    };

    const Range & range() const { return segment_range; }

    const Key & key() const { return file_key; }

    size_t offset() const { return range().left; }

    State wait();

    bool reserve(size_t size);

    void write(const char * from, size_t size);

    RemoteFileReaderPtr getRemoteFileReader();

    void setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_);

    String getOrSetDownloader();

    String getDownloader() const;

    void resetDownloader();

    bool isDownloader() const;

    bool isDownloaded() const { return is_downloaded.load(); }

    static String getCallerId();

    size_t getDownloadOffset() const;

    void completeBatchAndResetDownloader();

    void complete(State state);

    String getInfoForLog() const;

private:
    size_t availableSize() const { return reserved_size - downloaded_size; }
    bool lastFileSegmentHolder() const;
    void complete();
    void completeImpl(bool allow_non_strict_checking = false);
    void setDownloaded(std::lock_guard<std::mutex> & segment_lock);
    static String getCallerIdImpl(bool allow_non_strict_checking = false);
    void resetDownloaderImpl(std::lock_guard<std::mutex> & segment_lock);
    size_t getDownloadedSize(std::lock_guard<std::mutex> & segment_lock) const;
    String getInfoForLogImpl(std::lock_guard<std::mutex> & segment_lock) const;

    const Range segment_range;

    State download_state;
    String downloader_id;

    RemoteFileReaderPtr remote_file_reader;
    LocalCacheWriterPtr cache_writer;

    size_t downloaded_size = 0;
    size_t reserved_size = 0;

    mutable std::mutex mutex;
    std::condition_variable cv;

    /// Protects downloaded_size access with actual write into fs.
    /// downloaded_size is not protected by download_mutex in methods which
    /// can never be run in parallel to FileSegment::write() method
    /// as downloaded_size is updated only in FileSegment::write() method.
    /// Such methods are identified by isDownloader() check at their start,
    /// e.g. they are executed strictly by the same thread, sequentially.
    mutable std::mutex download_mutex;

    Key file_key;
    IFileCache * cache;

    Poco::Logger * log;

    bool detached = false;

    std::atomic<bool> is_downloaded{false};
};

struct FileSegmentsHolder : private boost::noncopyable
{
    explicit FileSegmentsHolder(FileSegments && file_segments_) : file_segments(std::move(file_segments_)) {}
    FileSegmentsHolder(FileSegmentsHolder && other) : file_segments(std::move(other.file_segments)) {}

    ~FileSegmentsHolder()
    {
        /// In CacheableReadBufferFromRemoteFS file segment's downloader removes file segments from
        /// FileSegmentsHolder right after calling file_segment->complete(), so on destruction here
        /// remain only uncompleted file segments.

        for (auto & segment : file_segments)
        {
            try
            {
                segment->complete();
            }
            catch (...)
            {
#ifndef NDEBUG
                throw;
#else
                tryLogCurrentException(__PRETTY_FUNCTION__);
#endif
            }
        }
    }

    FileSegments file_segments{};

    String toString();
};

}
