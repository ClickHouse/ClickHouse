#pragma once

#include <Core/Types.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <list>
#include <Common/FileCacheType.h>

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


class FileSegment : boost::noncopyable
{

friend class FileCache;
friend struct FileSegmentsHolder;
friend class FileSegmentRangeWriter;

public:
    using Key = FileCacheKey;
    using RemoteFileReaderPtr = std::shared_ptr<ReadBufferFromFileBase>;
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
        size_t offset_,
        size_t size_,
        const Key & key_,
        FileCache * cache_,
        State download_state_,
        bool is_persistent_ = false);

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

    const Range & range() const { return segment_range; }

    const Key & key() const { return file_key; }

    size_t offset() const { return range().left; }

    bool isPersistent() const { return is_persistent; }

    State wait();

    bool reserve(size_t size);

    void write(const char * from, size_t size, size_t offset_);

    RemoteFileReaderPtr getRemoteFileReader();

    void setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_);

    void resetRemoteFileReader();

    String getOrSetDownloader();

    String getDownloader() const;

    void resetDownloader();

    bool isDownloader() const;

    bool isDownloaded() const { return is_downloaded.load(); }

    static String getCallerId();

    size_t getDownloadOffset() const;

    size_t getDownloadedSize() const;

    size_t getRemainingSizeToDownload() const;

    void completeBatchAndResetDownloader();

    void completeWithState(State state, bool auto_resize = false);

    String getInfoForLog() const;

    size_t getHitsCount() const { return hits_count; }

    size_t getRefCount() const { return ref_count; }

    void incrementHitsCount() { ++hits_count; }

    void assertCorrectness() const;

    static FileSegmentPtr getSnapshot(
        const FileSegmentPtr & file_segment,
        std::lock_guard<std::mutex> & cache_lock);

    void detach(
        std::lock_guard<std::mutex> & cache_lock,
        std::lock_guard<std::mutex> & segment_lock);

    [[noreturn]] void throwIfDetached() const;

    bool isDetached() const;

    String getPathInLocalCache() const;

private:
    size_t availableSize() const { return reserved_size - downloaded_size; }

    size_t getDownloadedSize(std::lock_guard<std::mutex> & segment_lock) const;
    String getInfoForLogImpl(std::lock_guard<std::mutex> & segment_lock) const;
    void assertCorrectnessImpl(std::lock_guard<std::mutex> & segment_lock) const;
    bool hasFinalizedState() const;

    bool isDetached(std::lock_guard<std::mutex> & /* segment_lock */) const { return is_detached; }
    void markAsDetached(std::lock_guard<std::mutex> & segment_lock);
    [[noreturn]] void throwIfDetachedUnlocked(std::lock_guard<std::mutex> & segment_lock) const;

    void assertDetachedStatus(std::lock_guard<std::mutex> & segment_lock) const;
    void assertNotDetached(std::lock_guard<std::mutex> & segment_lock) const;

    void setDownloaded(std::lock_guard<std::mutex> & segment_lock);
    void setDownloadFailed(std::lock_guard<std::mutex> & segment_lock);
    bool isDownloaderImpl(std::lock_guard<std::mutex> & segment_lock) const;

    void wrapWithCacheInfo(Exception & e, const String & message, std::lock_guard<std::mutex> & segment_lock) const;

    bool lastFileSegmentHolder() const;

    /// complete() without any completion state is called from destructor of
    /// FileSegmentsHolder. complete() might check if the caller of the method
    /// is the last alive holder of the segment. Therefore, complete() and destruction
    /// of the file segment pointer must be done under the same cache mutex.
    void completeBasedOnCurrentState(std::lock_guard<std::mutex> & cache_lock);
    void completeBasedOnCurrentStateUnlocked(std::lock_guard<std::mutex> & cache_lock, std::lock_guard<std::mutex> & segment_lock);

    void completeImpl(
        std::lock_guard<std::mutex> & cache_lock,
        std::lock_guard<std::mutex> & segment_lock);

    void resetDownloaderImpl(std::lock_guard<std::mutex> & segment_lock);

    Range segment_range;

    State download_state;

    String downloader_id;

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
    using onCompleteFileSegmentCallback = std::function<void(const FileSegment & file_segment)>;

    FileSegmentRangeWriter(
        FileCache * cache_,
        const FileSegment::Key & key_,
        /// A callback which is called right after each file segment is completed.
        /// It is used to write into filesystem cache log.
        onCompleteFileSegmentCallback && on_complete_file_segment_func_);

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

    onCompleteFileSegmentCallback on_complete_file_segment_func;
};

}
