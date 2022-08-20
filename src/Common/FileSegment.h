#pragma once

#include <boost/noncopyable.hpp>
#include <Common/FileCacheKey.h>

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

    void waitBackgroundDownloadIfExists(size_t offset) const;

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

    /**
     * ========== Methods for _only_ file segment's `writer` ======================
     */

    void synchronousWrite(const char * from, size_t size, size_t offset, bool is_internal);

    void asynchronousWrite(const char * from, size_t size, size_t offset);

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

    RemoteFileReaderPtr extractRemoteFileReader();

    void setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_);

    void resetRemoteFileReader();

    size_t getRemainingSizeToDownload() const;

    /// [[noreturn]] void throwIfDetached() const;

private:
    size_t availableSizeUnlocked(std::unique_lock<std::mutex> & /* segment_lock */) const { return reserved_size - downloaded_size; }
    String getInfoForLogUnlocked(std::unique_lock<std::mutex> & segment_lock) const;

    void setDownloadedUnlocked(std::unique_lock<std::mutex> & segment_lock);
    void setDownloadFailedUnlocked(std::unique_lock<std::mutex> & segment_lock);
    void setInternalDownloaderUnlocked(const DownloaderId & new_downloader_id, std::unique_lock<std::mutex> & /* download_lock */);

    bool lastFileSegmentHolder() const;
    void resetDownloaderUnlocked(bool is_internal, std::unique_lock<std::mutex> & segment_lock);

    bool hasFinalizedStateUnlocked(std::unique_lock<std::mutex> & segment_lock) const;

    bool isDetached(std::unique_lock<std::mutex> & /* segment_lock */) const { return is_detached; }
    void detachAssumeStateFinalized(std::unique_lock<std::mutex> & segment_lock);
    [[noreturn]] void throwIfDetachedUnlocked(std::unique_lock<std::mutex> & segment_lock) const;

    void assertDetachedStatus(std::unique_lock<std::mutex> & segment_lock) const;
    void assertNotDetached() const;
    void assertNotDetachedUnlocked(std::unique_lock<std::mutex> & segment_lock) const;
    void assertIsDownloaderUnlocked(bool is_internal, const std::string & operation, std::unique_lock<std::mutex> & segment_lock) const;
    void assertCorrectnessUnlocked(std::unique_lock<std::mutex> & segment_lock) const;

    size_t getDownloadedSizeUnlocked(std::unique_lock<std::mutex> & segment_lock) const;
    bool isDownloaderUnlocked(bool is_internal, std::unique_lock<std::mutex> & segment_lock) const;
    String getDownloaderUnlocked(bool is_internal, std::unique_lock<std::mutex> & segment_lock) const;
    size_t getCurrentWriteOffsetUnlocked(std::unique_lock<std::mutex> & segment_lock) const;
    size_t getFirstNonDownloadedOffsetUnlocked(std::unique_lock<std::mutex> & segment_lock) const;

    /// complete() without any completion state is called from destructor of
    /// FileSegmentsHolder. complete() might check if the caller of the method
    /// is the last alive holder of the segment. Therefore, complete() and destruction
    /// of the file segment pointer must be done under the same cache mutex.
    void completeWithoutState(std::lock_guard<std::mutex> & cache_lock);
    void completeBasedOnCurrentState(std::lock_guard<std::mutex> & cache_lock, std::unique_lock<std::mutex> & segment_lock);

    void completePartAndResetDownloaderUnlocked(bool is_internal, std::unique_lock<std::mutex> & segment_lock);

    void wrapWithCacheInfo(Exception & e, const String & message, std::unique_lock<std::mutex> & segment_lock) const;

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

    std::atomic<bool> is_downloaded{false};
    std::atomic<size_t> hits_count = 0; /// cache hits.
    std::atomic<size_t> ref_count = 0; /// Used for getting snapshot state

    bool is_persistent;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::CacheFileSegments};

    class AsynchronousWriteState
    {
    public:

        /// Size in bytes - total size of buffers (contiguous file segment ranges of buffers)
        /// which were submitted for download.
        size_t getFutureDownloadedSize() const { return future_downloaded_size; }

        struct Buffer
        {
            Buffer() = default;

            explicit Buffer(size_t size, size_t offset_) : memory(size), offset(offset_) {}

            char * data() { return memory.data(); }

            size_t size() const { return memory.size(); }

            /// Buffer data.
            Memory<> memory;

            /// Offset within a file segment where current buffer needs to be written.
            size_t offset;
        };

        using Buffers = std::queue<Buffer>;

        /// A list of buffers which are waiting to be written into cache within current
        /// write state. Each buffer can be written one after another in direct order.
        std::queue<Buffer> buffers;

        struct SizeAndOffset
        {
            size_t size;
            size_t offset;
        };
        /// Keep track of the last added buffer info to ba able to assert correctness of
        /// the newly added buffer. (It is not enough to check just last buffer within the
        /// wait list because previous buffer could be popped any time by the download thread.)
        std::optional<SizeAndOffset> last_added_buffer_info;

        /// Downloaded size as it would be at the point when all currently submitted download
        /// tasks would be finished.
        std::atomic<size_t> future_downloaded_size = 0;

        struct BackgroundDownloadResult
        {
            std::shared_future<void> shared_future;
            size_t expected_size;

            BackgroundDownloadResult(std::shared_future<void> && shared_future_, size_t expected_size_)
                : shared_future(std::move(shared_future_)), expected_size(expected_size_) {}
        };
        /// Order is important.
        std::map<size_t, BackgroundDownloadResult> currently_downloading;

        /// Contains the first exception happened when writing data
        /// from `buffers`. No further buffer can be written, if there
        /// was exception while writing previous buffer.
        std::exception_ptr exception;

        /// Whether the download was cancelled. For example because file segment was removed from cache.
        size_t is_cancelled = false;

        /// The following is_executing, execution_end_cv, ExecutinoHolder are needed for
        /// only one purpose: to be able to cancel background download.
        bool is_executing = false;

        struct ExecutionHolder
        {
            explicit ExecutionHolder(AsynchronousWriteState & state_, std::mutex & file_segment_mutex)
                : state(state_), execution_end_lock(file_segment_mutex, std::defer_lock)
            {
                state.is_executing = true;
            }

            ~ExecutionHolder()
            {
                state.is_executing = false;
                /// state.execution_end_cv.notify_all();
            }

            AsynchronousWriteState & state;
            std::unique_lock<std::mutex> execution_end_lock;
        };
        std::optional<ExecutionHolder> execution_holder;
    };

    mutable std::optional<AsynchronousWriteState> async_write_state;

    void assertAsyncWriteStateInitialized() const;

    void cancelBackgroundDownloadIfExists(std::unique_lock<std::mutex> & segment_lock);

    bool isBackgroundDownloader(std::unique_lock<std::mutex> & segment_lock) const;
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
    using OnCompleteFileSegmentCallback = std::function<void(const FileSegment & file_segment)>;

    FileSegmentRangeWriter(
        FileCache * cache_,
        const FileSegment::Key & key_,
        /// A callback which is called right after each file segment is completed.
        /// It is used to write into filesystem cache log.
        OnCompleteFileSegmentCallback && on_complete_file_segment_func_);

    ~FileSegmentRangeWriter();

    bool write(const char * data, size_t size, size_t offset, bool is_persistent);

    void finalize();

private:
    FileSegments::iterator allocateFileSegment(size_t offset, bool is_persistent);
    void completeFileSegment(FileSegment & file_segment);

    FileCache * cache;
    FileSegment::Key key;

    FileSegmentsHolder file_segments_holder;
    FileSegments::iterator current_file_segment_it;

    size_t current_file_segment_write_offset = 0;

    bool finalized = false;

    OnCompleteFileSegmentCallback on_complete_file_segment_func;
};

}
