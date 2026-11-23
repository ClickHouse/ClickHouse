#pragma once
#include <atomic>
#include <cctype>
#include <optional>
#include <vector>
#include <Interpreters/FileCache/FileCacheKey.h>
#include <Interpreters/FileCache/FileCacheOriginInfo.h>
#include <Interpreters/FileCache/FileCache_fwd.h>
#include <Interpreters/FileCache/IFileCachePriority.h>

namespace DB
{
    enum class FileSegmentState : uint8_t
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
         * because each cache user might acquire multiple file segments and read them one by one,
         * so only user which actually needs to read this segment earlier than others - becomes a downloader.
         */
        DOWNLOADING,
        /**
         * Space reservation for a file segment is incremental, i.e. downloader reads buffer_size bytes
         * from remote fs -> tries to reserve buffer_size bytes to put them to cache -> writes to cache
         * on successful reservation and stops cache write otherwise. Those, who waited for the same file
         * segment, will read downloaded part from cache and remaining part directly from remote fs.
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
        DETACHED,
    };

    enum class FileSegmentKind : uint8_t
    {
        /**
         * Represents data cached from S3 or other backing storage.
         * It is kept in the cache after usage and can be evicted on demand, unless there are some holders.
         */
        Regular,

        /**
         * Represents temporary data without backing storage, but written to the cache from outside.
         * Ephemeral file segments are kept while they are in use, but then can be removed immediately after releasing.
         * Also, corresponding files are removed during cache loading.
         * Ephemeral file segments have no bound, and a single segment can have an arbitrary size.
         */
        Ephemeral,
    };

    std::string toString(FileSegmentKind kind);

    struct FileSegmentInfo
    {
        FileCacheKey key;
        size_t offset;
        std::string path;
        uint64_t range_left;
        uint64_t range_right;
        FileSegmentKind kind;
        FileSegmentState state;
        uint64_t size;
        uint64_t downloaded_size;
        time_t download_finished_time;
        uint64_t cache_hits;
        uint64_t references;
        bool is_unbound;
        IFileCachePriority::QueueEntryType queue_entry_type;
        FileCacheOriginInfo origin;
    };

    struct FileSegmentSummaryInfo
    {
        /// 1-based position of the segment in the output for 'hottest' and 'coldest', i.e. the priority order.
        /// For 'random': distinct, but in no particular order.
        UInt64 priority_rank{0};
        /// Real on-disk file, including the _<size> suffix of downloaded segments
        String path;
        /// Cache key
        String key;
        /// Segment offset in the source file
        UInt64 offset;
        /// Segment size as accounted by the cache
        UInt64 size;
        String state;
        /// See `IFileCachePriority::ObservedQueue`.
        IFileCachePriority::ObservedQueue::SplitQueue split_queue;
        IFileCachePriority::ObservedQueue::SLRUQueue slru_queue;
        String user_id;
    };

    using FileSegmentSummaryInfoList = std::vector<FileSegmentSummaryInfo>;

    /// The state of the walk over the queues in `IFileCachePriority::observeFilesystemCache`, shared by all sub-queues.
    /// Under the priority lock, the walk only copies queue entries; their file segments are read after it is released.
    struct FileSegmentSummaryInfos
    {
        struct ObservedEntry
        {
            IFileCachePriority::EntryPtr entry;
            IFileCachePriority::ObservedQueue observed_queue;
        };

        /// `HOTTEST` and `COLDEST`: the copied entries in the walk order. `RANDOM`: the reservoir.
        std::vector<ObservedEntry> entries;
        /// `HOTTEST` and `COLDEST`: the sum of the sizes of the copied entries, as the queue entries see them.
        UInt64 copied_size = 0;
        /// `HOTTEST` and `COLDEST`: set when `query_max_elements` entries are copied, or `copied_size` passes `query_max_total_size`.
        /// The next sub-queue (SLRU's second one, Split's second one) is then not walked at all.
        bool limit_reached{false};
        /// Set by the query to stop the walk early, e.g. on `KILL QUERY` or `max_execution_time`, so that the priority lock
        /// is released without walking the rest of the queue. The result is incomplete then. `nullptr` if the walk cannot be cancelled.
        const std::atomic<bool> * cancellation_flag = nullptr;

        [[nodiscard]] bool isCancelled() const { return cancellation_flag && cancellation_flag->load(std::memory_order_relaxed); }
        /// `RANDOM` only: the number of entries walked so far, which all sub-queues sample from as one reservoir.
        UInt64 seen = 0;
    };
}
