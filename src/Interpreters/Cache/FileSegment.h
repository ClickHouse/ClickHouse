#pragma once

#include <boost/noncopyable.hpp>
#include <Interpreters/Cache/FileCacheKey.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <base/getThreadId.h>
#include <list>
#include <queue>


namespace Poco { class Logger; }

namespace CurrentMetrics
{
extern const Metric CacheFileSegments;
extern const Metric FilesystemCacheBackgroundDownloadBuffers;
}

namespace DB
{

class FileCache;
class ReadBufferFromFileBase;

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
friend class StorageSystemFilesystemCache;

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

    bool isDownloaded() const;

    void waitBackgroundDownloadIfExists(size_t offset, size_t max_wait_seconds) const;

    size_t getHitsCount() const { return hits_count; }

    size_t getRefCount() const { return ref_count; }

    void incrementHitsCount() { ++hits_count; }

    size_t getCurrentWriteOffset() const;

    size_t getFirstNonDownloadedOffset() const;

    size_t getDownloadedSize() const;

    /// Now detached status can be used in the following cases:
    /// 1. there is only 1 remaining file segment holder
    ///    && it does not need this segment anymore
    ///    && this file segment was in cache and needs to be removed
    /// 2. in read_from_cache_if_exists_otherwise_bypass_cache case to create NOOP file segments.
    /// 3. removeIfExists - method which removes file segments from cache even though
    ///    it might be used at the moment.

    /// If file segment is detached it means the following:
    /// 1. It is not present in FileCache, e.g. will not be visible to any cache user apart from
    /// those who acquired shared pointer to this file segment before it was detached.
    /// 2. Detached file segment can still be hold by some cache users, but it's state became
    /// immutable at the point it was detached, any non-const / stateful method will throw an
    /// exception.
    void detach(std::lock_guard<std::mutex> & cache_lock, std::unique_lock<std::mutex> & segment_lock);

    static FileSegmentPtr getSnapshot(const FileSegmentPtr & file_segment, std::lock_guard<std::mutex> & cache_lock);

    bool isDetached() const;

    void assertCorrectness() const;

    bool isBackgroundDownloadFailedOrCancelled() const;

    /**
     * ========== Methods for _only_ file segment's `writer` ======================
     */

    void synchronousWrite(const char * from, size_t size, size_t offset);

    void asynchronousWrite(const char * from, size_t size, size_t offset);

    /**
     * ========== Methods for _only_ file segment's `downloader` ==================
     */

    /// Try to reserve exactly `size` bytes.
    bool reserve(size_t size_to_reserve);

    /// Write data into reserved space.
    void write(const char * from, size_t size, size_t offset);

    /// Complete file segment with a certain state.
    void completeWithState(State state);

    void completeWithoutState();

    /// Complete file segment's part which was last written.
    void completePartAndResetDownloader();

    void resetDownloader();

    RemoteFileReaderPtr getRemoteFileReader();

    RemoteFileReaderPtr extractRemoteFileReader();

    void setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_);

    void resetRemoteFileReader();

private:
    size_t getFirstNonDownloadedOffsetUnlocked(std::unique_lock<std::mutex> & segment_lock) const;
    size_t getCurrentWriteOffsetUnlocked(std::unique_lock<std::mutex> & segment_lock) const;
    size_t getDownloadedSizeUnlocked(std::unique_lock<std::mutex> & segment_lock) const;

    String getInfoForLogUnlocked(std::unique_lock<std::mutex> & segment_lock) const;

    String getDownloaderUnlocked(std::unique_lock<std::mutex> & segment_lock) const;
    void resetDownloaderUnlocked(std::unique_lock<std::mutex> & segment_lock);
    void resetDownloadingStateUnlocked(std::unique_lock<std::mutex> & segment_lock);

    void setDownloadState(State state);

    void setDownloadedUnlocked(std::unique_lock<std::mutex> & segment_lock);

    bool hasFinalizedStateUnlocked(std::unique_lock<std::mutex> & segment_lock) const;

    bool isDownloaderUnlocked(std::unique_lock<std::mutex> & segment_lock) const;

    bool isDetached(std::unique_lock<std::mutex> & /* segment_lock */) const { return is_detached; }
    void detachAssumeStateFinalized(std::unique_lock<std::mutex> & segment_lock);
    [[noreturn]] void throwIfDetachedUnlocked(std::unique_lock<std::mutex> & segment_lock) const;

    void assertDetachedStatus(std::unique_lock<std::mutex> & segment_lock) const;
    void assertNotDetached() const;
    void assertNotDetachedUnlocked(std::unique_lock<std::mutex> & segment_lock) const;
    void assertIsDownloaderUnlocked(const std::string & operation, std::unique_lock<std::mutex> & segment_lock) const;
    void assertCorrectnessUnlocked(std::unique_lock<std::mutex> & segment_lock) const;

    void completeWithoutStateUnlocked(std::lock_guard<std::mutex> & cache_lock);
    void completeBasedOnCurrentState(std::lock_guard<std::mutex> & cache_lock, std::unique_lock<std::mutex> & segment_lock);

    void completePartAndResetDownloaderUnlocked(std::unique_lock<std::mutex> & segment_lock);

    void wrapWithCacheInfo(Exception & e, const String & message, std::unique_lock<std::mutex> & segment_lock) const;

    static bool isInternal();

    Range segment_range;

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

    bool is_downloaded{false};

    std::atomic<size_t> hits_count = 0; /// cache hits.
    std::atomic<size_t> ref_count = 0; /// Used for getting snapshot state

    bool is_persistent;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::CacheFileSegments};

    class BackgroundDownload;
    mutable std::unique_ptr<BackgroundDownload> background_download;

    void assertAsyncWriteStateInitialized() const;

    void cancelBackgroundDownloadIfExists(std::unique_lock<std::mutex> & segment_lock);

    bool isBackgroundDownloader(std::unique_lock<std::mutex> & segment_lock) const;
};

class FileSegment::BackgroundDownload : private boost::noncopyable
{
friend class FileCache;
friend class FileSegment;

public:
    explicit BackgroundDownload(FileCache * cache_) : cache(cache_) {}

    /// If some range is being downloaded, return it. It is guaranteed that
    /// at the moment of time there is only one range which can be downloaded
    /// because execution of entries from a single BackgroundDownload can
    /// be done only in a single thread (because BackgroundDownload is a
    /// property of a file segment, which is a single file).
    std::optional<FileSegment::Range>
    getCurrentlyDownloadingRange(std::unique_lock<std::mutex> & segment_lock) const;

    /// Get a list of ranges, which are waiting to be downloaded.
    std::vector<FileSegment::Range>
    getDownloadQueueRanges(std::unique_lock<std::mutex> & segment_lock) const;

private:
    bool hasError(std::unique_lock<std::mutex> & /* segment_lock */) const { return exception != nullptr; }

    /// Get size of the file segment as it would be if all entries
    /// in this queue would be downloaded successfully.
    size_t getFutureDownloadedSize(std::unique_lock<std::mutex> & /* segment_lock */) const { return future_downloaded_size; }

    /// Reserve space for background download. Background download has a limited size,
    /// so if we reach the limit, new downloads will be discarded.
    bool reserve(size_t size, std::unique_lock<std::mutex> & segment_lock);

    struct BackgroundDownloadResult
    {
        std::shared_future<void> shared_future;
        size_t expected_size;

        BackgroundDownloadResult(std::shared_future<void> && shared_future_, size_t expected_size_)
            : shared_future(std::move(shared_future_)), expected_size(expected_size_) {}
    };

    /// Order is important.
    std::map<size_t, BackgroundDownloadResult> currently_downloading;

    struct Buffer : private boost::noncopyable
    {
        explicit Buffer(size_t size_, FileCache * cache_, std::lock_guard<std::mutex> & background_download_lock);

        ~Buffer();

        char * data() { return memory.data(); }

        size_t size() const { return buf_size; }

        /// Buffer data.
        Memory<> memory;

        /// Offset within a file segment where current buffer needs to be written.
        size_t offset;
        /// Size of current buffer. size == memory.size().
        size_t buf_size;

        FileCache * cache;

        CurrentMetrics::Increment metric_increment{CurrentMetrics::FilesystemCacheBackgroundDownloadBuffers};
    };

    using BufferPtr = std::unique_ptr<Buffer>;
    using Buffers = std::queue<BufferPtr>;

    /// A list of buffers which are waiting to be written into cache within current
    /// write state. Each buffer can be written one after another in direct order.
    std::deque<BufferPtr> buffers;

    /// Contains the first exception happened when writing data
    /// from `buffers`. No further buffer can be written, if there
    /// was exception while writing previous buffer.
    std::exception_ptr exception;

    /// Whether the download was cancelled. For example because file segment was removed from cache.
    bool is_cancelled = false;

    struct OffsetAndSize
    {
        size_t offset;
        size_t size;
    };

    /// Keep track of the last added buffer info to ba able to assert correctness of
    /// the newly added buffer. (It is not enough to check just last buffer within the
    /// wait list because previous buffer could be popped any time by the download thread.)
    std::optional<OffsetAndSize> last_added_buffer_range;

    std::optional<OffsetAndSize> currently_executing_range;

    /// Downloaded size as it would be at the point when all currently submitted download
    /// tasks would be finished.
    std::atomic<size_t> future_downloaded_size = 0;

    FileCache * cache;

    /// Buffer placeholder is put here in file_segment.reserve() method. It will be moved from here
    /// to Buffers queue when write() method is called.
    BufferPtr reserved_buffer;
};

struct FileSegmentsHolder : private boost::noncopyable
{
    FileSegmentsHolder() = default;

    explicit FileSegmentsHolder(FileSegments && file_segments_) : file_segments(std::move(file_segments_)) {}

    FileSegmentsHolder(FileSegmentsHolder && other) noexcept : file_segments(std::move(other.file_segments)) {}

    ~FileSegmentsHolder();

    String toString();

    FileSegments::iterator add(FileSegmentPtr && file_segment)
    {
        return file_segments.insert(file_segments.end(), file_segment);
    }

    FileSegments file_segments{};
};

}
