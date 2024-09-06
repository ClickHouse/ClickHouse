#pragma once

#include <boost/noncopyable.hpp>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/Guards.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <IO/OpenedFileCache.h>
#include <base/getThreadId.h>
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/FileSegmentInfo.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>


namespace Poco { class Logger; }

namespace CurrentMetrics
{
extern const Metric CacheFileSegments;
}

namespace DB
{

class ReadBufferFromFileBase;
struct FileCacheReserveStat;


struct CreateFileSegmentSettings
{
    FileSegmentKind kind = FileSegmentKind::Regular;
    bool unbounded = false;

    CreateFileSegmentSettings() = default;

    explicit CreateFileSegmentSettings(FileSegmentKind kind_)
        : kind(kind_), unbounded(kind == FileSegmentKind::Ephemeral) {}
};

class FileSegment : private boost::noncopyable
{
friend struct LockedKey;
friend class FileCache; /// Because of reserved_size in tryReserve().

public:
    using Key = FileCacheKey;
    using RemoteFileReaderPtr = std::shared_ptr<ReadBufferFromFileBase>;
    using LocalCacheWriterPtr = std::shared_ptr<WriteBufferFromFile>;
    using Downloader = std::string;
    using DownloaderId = std::string;
    using Priority = IFileCachePriority;
    using State = FileSegmentState;
    using Info = FileSegmentInfo;
    using QueueEntryType = FileCacheQueueEntryType;

    FileSegment(
        const Key & key_,
        size_t offset_,
        size_t size_,
        State download_state_,
        const CreateFileSegmentSettings & create_settings = {},
        bool background_download_enabled_ = false,
        FileCache * cache_ = nullptr,
        std::weak_ptr<KeyMetadata> key_metadata_ = std::weak_ptr<KeyMetadata>(),
        Priority::IteratorPtr queue_iterator_ = nullptr);

    ~FileSegment() = default;

    State state() const;

    static String stateToString(FileSegment::State state);

    /// Represents an interval [left, right] including both boundaries.
    struct Range
    {
        size_t left;
        size_t right;

        Range(size_t left_, size_t right_);

        bool operator==(const Range & other) const { return left == other.left && right == other.right; }

        bool operator<(const Range & other) const { return right < other.left; }

        size_t size() const { return right - left + 1; }

        bool contains(size_t point) const { return left <= point && point <= right; }

        bool contains(const Range & other) const { return contains(other.left) && contains(other.right); }

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

    FileSegmentKind getKind() const { return segment_kind; }

    bool isUnbound() const { return is_unbound; }

    String getPath() const;

    int getFlagsForLocalRead() const { return O_RDONLY | O_CLOEXEC; }

    /**
     * ========== Methods for _any_ file segment's owner ========================
     */

    String getOrSetDownloader();

    bool isDownloader() const;

    DownloaderId getDownloader() const;

    /// Wait for the change of state from DOWNLOADING to any other.
    State wait(size_t offset);

    bool isDownloaded() const;

    size_t getHitsCount() const { return hits_count; }

    size_t getRefCount() const { return ref_count; }

    size_t getCurrentWriteOffset() const;

    size_t getDownloadedSize() const;

    size_t getReservedSize() const;

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
    void detach(const FileSegmentGuard::Lock &, const LockedKey &);

    static FileSegmentInfo getInfo(const FileSegmentPtr & file_segment);

    bool isDetached() const;

    /// File segment has a completed state, if this state is final and
    /// is not going to be changed. Completed states: DOWNALODED, DETACHED.
    bool isCompleted(bool sync = false) const;

    void increasePriority();

    /**
     * ========== Methods used by `cache` ========================
     */

    FileSegmentGuard::Lock lock() const;

    Priority::IteratorPtr getQueueIterator() const;

    void setQueueIterator(Priority::IteratorPtr iterator);

    void resetQueueIterator();

    KeyMetadataPtr tryGetKeyMetadata() const;

    KeyMetadataPtr getKeyMetadata() const;

    bool assertCorrectness() const;

    /**
     * ========== Methods that must do cv.notify() ==================
     */

    void complete();

    void completePartAndResetDownloader();

    void resetDownloader();

    /**
     * ========== Methods for _only_ file segment's `downloader` ==================
     */

    /// Try to reserve exactly `size` bytes (in addition to the getDownloadedSize() bytes already downloaded).
    /// Returns true if reservation was successful, false otherwise.
    bool reserve(
        size_t size_to_reserve,
        size_t lock_wait_timeout_milliseconds,
        std::string & failure_reason,
        FileCacheReserveStat * reserve_stat = nullptr);

    /// Write data into reserved space.
    void write(char * from, size_t size, size_t offset_in_file);

    // Invariant: if state() != DOWNLOADING and remote file reader is present, the reader's
    // available() == 0, and getFileOffsetOfBufferEnd() == our getCurrentWriteOffset().
    //
    // The reader typically requires its internal_buffer to be assigned from the outside before
    // calling next().
    RemoteFileReaderPtr getRemoteFileReader();
    LocalCacheWriterPtr getLocalCacheWriter();

    RemoteFileReaderPtr extractRemoteFileReader();

    void resetRemoteFileReader();

    void setRemoteFileReader(RemoteFileReaderPtr remote_file_reader_);

    void setDownloadFailed();

private:
    String getDownloaderUnlocked(const FileSegmentGuard::Lock &) const;
    bool isDownloaderUnlocked(const FileSegmentGuard::Lock & segment_lock) const;
    void resetDownloaderUnlocked(const FileSegmentGuard::Lock &);

    void setDownloadState(State state, const FileSegmentGuard::Lock &);
    void resetDownloadingStateUnlocked(const FileSegmentGuard::Lock &);
    void setDetachedState(const FileSegmentGuard::Lock &);

    String getInfoForLogUnlocked(const FileSegmentGuard::Lock &) const;

    void setDownloadedUnlocked(const FileSegmentGuard::Lock &);
    void setDownloadFailedUnlocked(const FileSegmentGuard::Lock &);

    void assertNotDetached() const;
    void assertNotDetachedUnlocked(const FileSegmentGuard::Lock &) const;
    void assertIsDownloaderUnlocked(const std::string & operation, const FileSegmentGuard::Lock &) const;
    bool assertCorrectnessUnlocked(const FileSegmentGuard::Lock &) const;

    LockedKeyPtr lockKeyMetadata(bool assert_exists = true) const;

    String tryGetPath() const;

    Key file_key;
    Range segment_range;
    const FileSegmentKind segment_kind;
    /// Size of the segment is not known until it is downloaded and
    /// can be bigger than max_file_segment_size.
    const bool is_unbound = false;
    const bool background_download_enabled;

    std::atomic<State> download_state;
    DownloaderId downloader_id; /// The one who prepares the download

    RemoteFileReaderPtr remote_file_reader;
    LocalCacheWriterPtr cache_writer;

    /// downloaded_size should always be less or equal to reserved_size
    std::atomic<size_t> downloaded_size = 0;
    std::atomic<size_t> reserved_size = 0;
    mutable std::mutex write_mutex;

    mutable FileSegmentGuard segment_guard;
    std::weak_ptr<KeyMetadata> key_metadata;
    mutable Priority::IteratorPtr queue_iterator; /// Iterator is put here on first reservation attempt, if successful.
    FileCache * cache;
    std::condition_variable cv;

    LoggerPtr log;

    std::atomic<size_t> hits_count = 0; /// cache hits.
    std::atomic<size_t> ref_count = 0; /// Used for getting snapshot state

    CurrentMetrics::Increment metric_increment{CurrentMetrics::CacheFileSegments};
};


struct FileSegmentsHolder final : private boost::noncopyable
{
    FileSegmentsHolder() = default;

    explicit FileSegmentsHolder(FileSegments && file_segments_);

    ~FileSegmentsHolder();

    bool empty() const { return file_segments.empty(); }

    size_t size() const { return file_segments.size(); }

    String toString(bool with_state = false) const;

    void popFront() { completeAndPopFrontImpl(); }

    FileSegment & front() { return *file_segments.front(); }
    const FileSegment & front() const { return *file_segments.front(); }

    FileSegment & back() { return *file_segments.back(); }
    const FileSegment & back() const { return *file_segments.back(); }

    FileSegment & add(FileSegmentPtr && file_segment);

    FileSegments::iterator begin() { return file_segments.begin(); }
    FileSegments::iterator end() { return file_segments.end(); }

    FileSegments::const_iterator begin() const { return file_segments.begin(); }
    FileSegments::const_iterator end() const { return file_segments.end(); }
    FileSegmentPtr getSingleFileSegment() const;

    void reset();

private:
    FileSegments file_segments{};

    FileSegments::iterator completeAndPopFrontImpl();
};

using FileSegmentsHolderPtr = std::unique_ptr<FileSegmentsHolder>;

String toString(const FileSegments & file_segments, bool with_state = false);

}
