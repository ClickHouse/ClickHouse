#pragma once
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/FileCacheKey.h>

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

    enum class FileCacheQueueEntryType : uint8_t
    {
        None,
        LRU,
        SLRU_Protected,
        SLRU_Probationary,
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
        uint64_t cache_hits;
        uint64_t references;
        bool is_unbound;
        FileCacheQueueEntryType queue_entry_type;
        std::string user_id;
        uint64_t user_weight;
    };
}
