#pragma once

#include <Common/Logger.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/FileCache/FileCache_fwd.h>

namespace DB
{

struct FileSegmentsHolder;
class ReadBuffer;
struct Settings;

/// Stores query result cache entries in a preconfigured filesystem cache (an entry of the `filesystem_caches` section of the server
/// configuration), so that cached query results get more space than in memory, survive server restarts, and (with a distributed
/// filesystem cache) can be shared between servers. The filesystem cache is selected per query with setting
/// `query_cache_on_disk_cache_name`.
///
/// The entries are ordinary filesystem cache entries, indistinguishable from other data in the same filesystem cache: they are not
/// held from deletion, they are evicted by the same rules, and there are no specific limits on their total size or number other than
/// the limits of the filesystem cache itself. The keys are hashed with a salt so that they cannot intersect with other keys in the
/// filesystem cache. All metadata needed to validate an entry (TTL, access control) is serialized into the entry itself; no in-memory
/// state is kept.
///
/// The on-disk query result cache works independently of the in-memory query result cache, see settings
/// `enable_reads_from_query_cache_on_disk` and `enable_writes_to_query_cache_on_disk`. If both caches are enabled for reads, the
/// lookup is attempted first from memory and only on a miss from disk. If both caches are enabled for writes, the result is written
/// to both.
class QueryResultCacheOnDisk
{
public:
    QueryResultCacheOnDisk(FileCachePtr file_cache_, bool enable_reads_, bool enable_writes_, const String & codec_name_, size_t reserve_space_lock_wait_timeout_milliseconds_);

    /// Resolves settings `query_cache_on_disk_cache_name` and co. Returns nullptr if the on-disk query result cache is not used for
    /// the query: no filesystem cache name is given, or both reads and writes are disabled, or the filesystem cache exists but is not
    /// initialized yet. Throws if the given name does not resolve to a configured filesystem cache.
    static std::shared_ptr<const QueryResultCacheOnDisk> getFromSettings(const Settings & settings);

    bool readsEnabled() const { return enable_reads; }
    bool writesEnabled() const { return enable_writes; }

    /// Look up the query result for the key. The returned reader is empty (no cache entry) if the entry does not exist, is stale,
    /// was partially evicted, or is not accessible to the user in the key.
    QueryResultCacheReader createReader(const QueryResultCache::Key & key) const;

    /// Store the query result. Best-effort: an entry which cannot be written (no space, a concurrent writer, a fresh entry already
    /// exists) is skipped and the reason is logged, no exception is thrown.
    void write(const QueryResultCache::Key & key, const QueryResultCache::Entry & entry) const;

private:
    /// The fixed-size prefix of a serialized entry. Small enough to be probed cheaply, sufficient to decide whether the entry is
    /// usable (staleness) and how large it is (to look up the rest of the entry in the filesystem cache).
    struct FixedHeader
    {
        UInt32 format_version = 0;
        UInt32 protocol_revision = 0; /// TCP protocol revision the Native blocks are serialized with
        UInt64 total_size = 0; /// entire entry size in bytes, including this header
        UInt64 created_at = 0; /// seconds since epoch
        UInt64 expires_at = 0; /// seconds since epoch
        UInt128 body_checksum = 0; /// SipHash-128 of everything after the fixed header

        bool isStale() const;
    };

    enum class ProbeResult
    {
        None, /// no (readable) entry for the key
        Fresh, /// a non-stale, fully downloaded entry exists
        StaleOrUnreadable /// an entry exists but it is stale, corrupt or was written by an incompatible version
    };

    /// Parses and validates the fixed header. Returns std::nullopt if the data does not look like an entry of the on-disk query
    /// result cache (wrong magic), or the entry was written in an incompatible format or by a newer server.
    static std::optional<FixedHeader> parseFixedHeader(ReadBuffer & in);

    /// Reads everything after the fixed header and verifies it against the checksum in the header. Returns std::nullopt if the
    /// body is corrupt. The caller must have verified that all `header.total_size` bytes of the entry are downloaded.
    static std::optional<String> readCheckedBody(const FileSegmentsHolder & holder, const FixedHeader & header);

    ProbeResult probeExistingEntry(const FileCacheKey & cache_key) const;

    /// Reads the entry stored under `cache_key`, if there is a usable one. Returns std::nullopt if the entry does not exist, is stale,
    /// was partially evicted, is unreadable, or is not accessible to the user in `key`, so that the caller can try another key.
    std::optional<QueryResultCacheReader> tryCreateReader(const QueryResultCache::Key & key, const FileCacheKey & cache_key) const;

    FileCachePtr file_cache;
    const bool enable_reads;
    const bool enable_writes;
    const String codec_name;
    const size_t reserve_space_lock_wait_timeout_milliseconds;
    LoggerPtr logger = getLogger("QueryResultCacheOnDisk");
};

using QueryResultCacheOnDiskPtr = std::shared_ptr<const QueryResultCacheOnDisk>;

}
