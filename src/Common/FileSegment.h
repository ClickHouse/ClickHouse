#pragma once

#include <boost/noncopyable.hpp>
#include <Common/FileCacheKey.h>
#include <Core/Types.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <list>
#include <queue>


namespace Poco { class Logger; }

namespace CurrentMetrics
{
extern const Metric CacheFileSegments;
}

namespace DB
{

class FileCache;

class FileSegment;
using FileSegmentPtr = std::shared_ptr<FileSegment>;
using FileSegments = std::list<FileSegmentPtr>;


struct CreateFileSegmentSettings
{
    bool is_persistent = false;
    bool is_async_download = false;
};

class FileSegment : private boost::noncopyable, public std::enable_shared_from_this<FileSegment>
{

friend class FileCache;
friend struct FileSegmentsHolder;
friend class FileSegmentRangeWriter;

public:
    using Key = FileCacheKey;
    using RemoteFileReaderPtr = std::shared_ptr<ReadBufferFromFileBase>;
    using LocalCacheWriterPtr = std::unique_ptr<WriteBufferFromFile>;
    using Downloader = std::string;
    using DownloaderId = std::string;

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
        size_t offset_,
        size_t size_,
        const Key & key_,
        FileCache * cache_,
        State download_state_,
        const CreateFileSegmentSettings & create_settings);

    ~FileSegment();

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

    static String getCallerId();

    String getInfoForLog() const;

    /**
     * ========== Methods to get file segment's constant state ==================
     */

    const Range & range() const { return segment_range; }

    const Key & key() const { return file_key; }

    size_t offset() const { return range().left; }

    bool isPersistent() const { return is_persistent; }

    using UniqueId = std::pair<FileCacheKey, size_t>;
    UniqueId getUniqueId() const { return std::pair(key(), offset()); }

    String getPathInLocalCache() const;

    /**
     * ========== Methods for _any_ file segment's owner ========================
     */

    String getOrSetDownloader();

    bool isDownloader() const;

    DownloaderId getDownloader() const;

    /// Wait for the change of state from DOWNLOADING to any other.
    State wait();

    bool isDownloaded() const { return is_downloaded.load(); }

    size_t getHitsCount() const { return hits_count; }

    size_t getRefCount() const { return ref_count; }

    void incrementHitsCount() { ++hits_count; }

    size_t getDownloadOffset() const;

    size_t getDownloadedOffset() const;

    size_t getDownloadedSize() const;

    void detach(std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock);

    static FileSegmentPtr getSnapshot(const FileSegmentPtr & file_segment, std::lock_guard<std::mutex> & cache_lock);

    /**
     * ========== Methods for _only_ file segment's `writer` ======================
     */

    void synchronousWrite(const char * from, size_t size, size_t offset, bool is_internal);

    void asynchronousWrite(const char * from, size_t size, size_t offset);

    /**
     * writeInMemory and finalizeWrite are used together to write a single file with delay.
     * Both can be called only once, one after another. Used for writing cache via threadpool
     * on wrote operations. TODO: this solution is temporary, until adding a separate cache layer.
     */
    void writeInMemory(const char * from, size_t size);

    size_t finalizeWrite();

    /**
     * ========== Methods for _only_ file segment's `downloader` ==================
     */

    /// Try to reserve exactly `size` bytes.
    bool reserve(size_t size);

    /// Write data into reserved space.
    void write(const char * from, size_t size, size_t offset);

    /// Complete file segment with a certain state.
    void completeWithState(State state);

    /// Complete file segment's part which was last written.
    void completePartAndResetDownloader();

    void resetDownloader();

    RemoteFileReaderPtr getRemoteFileReader();

    void setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_);

    void resetRemoteFileReader();

private:
    size_t availableSizeUnlocked(std::lock_guard<std::mutex> & /* segment_lock */) const { return reserved_size - downloaded_size; }
    String getInfoForLogUnlocked(std::lock_guard<std::mutex> & segment_lock) const;

    void setDownloadedUnlocked(std::lock_guard<std::mutex> & segment_lock);
    void setDownloadFailedUnlocked(std::lock_guard<std::mutex> & segment_lock);
    void setInternalDownloaderUnlocked(const DownloaderId & new_downloader_id, std::lock_guard<std::mutex> & /* download_lock */);

    bool lastFileSegmentHolder() const;
    void resetDownloaderUnlocked(bool is_internal, std::lock_guard<std::mutex> & segment_lock);

    bool hasFinalizedStateUnlocked(std::lock_guard<std::mutex> & segment_lock) const;

    bool isDetached(std::lock_guard<std::mutex> & /* segment_lock */) const { return is_detached; }
    void markAsDetached(std::lock_guard<std::mutex> & segment_lock);
    [[noreturn]] void throwIfDetachedUnlocked(std::lock_guard<std::mutex> & segment_lock) const;

    void assertDetachedStatus(std::lock_guard<std::mutex> & segment_lock) const;
    void assertNotDetached() const;
    void assertNotDetachedUnlocked(std::lock_guard<std::mutex> & segment_lock) const;
    void assertIsDownloaderUnlocked(bool is_internal, const std::string & operation, std::lock_guard<std::mutex> & segment_lock) const;
    void assertCorrectnessUnlocked(std::lock_guard<std::mutex> & segment_lock) const;

    size_t getDownloadedSizeUnlocked(std::lock_guard<std::mutex> & segment_lock) const;
    bool isDownloaderUnlocked(bool is_internal, std::lock_guard<std::mutex> & segment_lock) const;
    String getDownloaderUnlocked(bool is_internal, std::lock_guard<std::mutex> & segment_lock) const;
    size_t getDownloadOffsetUnlocked(std::lock_guard<std::mutex> & segment_lock) const;
    size_t getDownloadedOffsetUnlocked(std::lock_guard<std::mutex> & segment_lock) const;

    /// complete() without any completion state is called from destructor of
    /// FileSegmentsHolder. complete() might check if the caller of the method
    /// is the last alive holder of the segment. Therefore, complete() and destruction
    /// of the file segment pointer must be done under the same cache mutex.
    void completeWithoutState(std::lock_guard<std::mutex> & cache_lock);
    void completeBasedOnCurrentState(std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock);

    void completePartAndResetDownloaderUnlocked(bool is_internal, std::lock_guard<std::mutex> & segment_lock);

    void wrapWithCacheInfo(Exception & e, const String & message, std::lock_guard<std::mutex> & segment_lock) const;

    const Range segment_range;

    State download_state;

    /// The one who prepares the download
    DownloaderId downloader_id;
    DownloaderId background_downloader_id;

    RemoteFileReaderPtr remote_file_reader;
    LocalCacheWriterPtr cache_writer;

    size_t downloaded_size = 0;
    size_t reserved_size = 0;

    /// global locking order rule:
    /// 1. cache lock
    /// 2. segment lock

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
    FileCache * cache;

    Poco::Logger * log;

    /// "detached" file segment means that it is not owned by cache ("detached" from cache).
    /// In general case, all file segments are owned by cache.
    bool is_detached = false;

    std::atomic<bool> is_downloaded{false};
    std::atomic<size_t> hits_count = 0; /// cache hits.
    std::atomic<size_t> ref_count = 0; /// Used for getting snapshot state

    /// Currently no-op. (will be added in PR 36171)
    /// Defined if a file comply by the eviction policy.
    bool is_persistent;
    /// Should the file segment be downloaded into cache synchronously or asynchronously?
    bool is_async_download;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::CacheFileSegments};

    class WriteState
    {
    public:
        struct Buffer
        {
            Buffer() = default;

            explicit Buffer(size_t size, size_t offset_) : memory(size), offset(offset_) {}

            char * data() { return memory.data(); }

            size_t size() const { return memory.size(); }

            /// Buffer data.
            Memory<> memory;

            /// Offset within a file segment
            /// where current buffer needs to be written.
            size_t offset;
        };

        using Buffers = std::queue<Buffer>;

        size_t getDownloadOffset() const { return download_offset; }

        size_t getDownloadQueueEndOffset() const { return download_queue_offset; }

        void throwIfException()
        {
            std::lock_guard lock(exception_mutex);
            if (exception)
                std::rethrow_exception(exception);
        }

        void setExceptionIfEmpty()
        {
            std::lock_guard lock(exception_mutex);
            if (!exception)
                exception = std::current_exception();
        }

        /// A list of buffers which are waiting to be written
        /// into cache within current write state.
        /// Each buffer can be written one after another in direct order.
        std::queue<Buffer> buffers;

        int64_t last_added_offset = -1;
        std::atomic<size_t> download_offset = 0;
        std::atomic<size_t> download_queue_offset = 0;

    private:
        /// Contains the first exception happened when writing data
        /// from `buffers`. No further buffer can be written, if there
        /// was exception while writing previous buffer.
        std::exception_ptr exception TSA_GUARDED_BY(exception_mutex);
        std::mutex exception_mutex;

        // std::string toString() const
        // {
        //     WriteBufferFromOwnString wb;
        //     wb << "buffers number: " << buffers.size() << ", ";
        //     wb << "download offset: " << download_offset.load() << ",";
        //     wb << "last added offset: " << last_added_offset;
        //     return wb.str();
        // }
    };

    mutable std::optional<WriteState> async_write_state;
    bool isBackgroundDownloader(std::lock_guard<std::mutex> & segment_lock) const;
};

struct FileSegmentsHolder : private boost::noncopyable
{
    explicit FileSegmentsHolder(FileSegments && file_segments_) : file_segments(std::move(file_segments_)) {}

    FileSegmentsHolder(FileSegmentsHolder && other) noexcept : file_segments(std::move(other.file_segments)) {}

    ~FileSegmentsHolder();

    FileSegments file_segments{};

    String toString();
};

}
