#pragma once

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/Logger.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Common/SharedMutex.h>

namespace DB
{

/// An implementation of predicate caching a la https://doi.org/10.1145/3626246.3653395
///
/// Given the table, part name and a hash of a predicate as key, caches which marks definitely don't match the predicate and which marks may
/// match the predicate. This allows to skip the scan if the same predicate is evaluated on the same data again. Note that this doesn't work
/// the other way round: we can't tell if _all_ rows in the mark match the predicate.
///
/// Note: The cache may store more than the minimal number of matching marks. For example, assume a very selective predicate that matches
/// just a single row in a single mark. One would expect that the cache records just a single mark as potentially matching:
///     000000010000000000000000000
/// But it is equally correct for the cache to store this.
///     000001111111110000000000000
/// It is just less efficient for pruning (false positives).
class QueryConditionCache
{
public:
    /// False means none of the rows in the mark match the predicate. We can skip such marks.
    /// True means at least one row in the mark matches the predicate. We need to read such marks.
    using MatchingMarks = std::vector<bool>;

private:
    /// A hash of the table id, part name and condition id.
    /// CityHash128 is enough to use for practical applications as the probability of collisions is very low.
    /// https://github.com/ClickHouse/ClickHouse/issues/9506
    using Key = UInt128;

    struct Entry
    {
#if defined(DEBUG_OR_SANITIZER_BUILD)
        /// Store extended information only in Debug builds.
        /// Having them in release builds is too costly.
        const UUID table_id;
        const String part_name;
        const UInt64 condition_hash = 42;
        const String condition;
#endif

        MatchingMarks matching_marks;

        /// Digest of the file-level metadata (e.g. the Parquet footer) the marks were computed
        /// from, for entries written by file-backed storages (`File`, object storage). The marks
        /// name row groups of that exact footer, so a later read may only apply them to a file
        /// whose footer produces the same digest - see `ParquetFileBucketInfo::footer_digest` for
        /// the fail-close contract. 0 means "unknown" (MergeTree entries, or a format that does
        /// not report a digest) and disables the guard. Set once at entry creation and immutable
        /// afterwards: concurrent writers of the same key hold the same version token, so they
        /// describe the same file generation and the same digest.
        const UInt64 file_metadata_digest = 0;

        SharedMutex mutex; /// (*)

        explicit Entry(size_t mark_count, UInt64 file_metadata_digest_); /// (**)

#if defined(DEBUG_OR_SANITIZER_BUILD)
        Entry(size_t mark_count_, UInt64 file_metadata_digest_, const UUID & table_id_, const String & part_name_, UInt64 condition_hash_, const String & condition_);
#endif

        /// (*) You might wonder why Entry has its own mutex considering that CacheBase locks internally already. The reason is that
        ///     ClickHouse scans ranges within the same part in parallel. The first scan creates and inserts a new Key + Entry into the cache,
        ///     the 2nd ... Nth scans find the existing Key and update its Entry for the new ranges. This can only be done safely in a
        ///     synchronized fashion.

        /// (**) About error handling: There could be an exception after the i-th scan and cache entries could (theoretically) be left in a
        ///     corrupt state. If we are not careful, future scans queries could then skip too many ranges. To prevent this, it is important to
        ///     initialize all marks of each entry as non-matching. In case of an exception, future scans will then not skip them.
    };

    struct EntryWeight
    {
        size_t operator()(const Entry & entry) const;
    };


public:
    using Cache = CacheBase<Key, Entry, UInt128TrivialHash, EntryWeight>;

    /// Compute cache key from table UUID, part name and condition hash
    static Key makeKey(const UUID & table_id, const String & part_name, UInt64 condition_hash);

    /// Compose the `part_name` component of a cache key for a file-backed table (e.g. `File`, `S3`,
    /// object storage). Uses the full path (not just the base name) so files that share a name in
    /// different directories do not collide, and folds in a content-version token so an in-place
    /// rewrite of the file yields a different key rather than a stale hit. The token is the ETag for
    /// remote objects, or a local identity (modification time + inode + size) for local files. For
    /// immutable files (e.g. data-lake data files) the path alone is a stable identity and the token
    /// may be left empty. The path and the token are separated by a NUL byte, which cannot occur in
    /// either, so the mapping is unambiguous.
    static String makeFilePartName(const String & path, std::string_view version_token);

    QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Add an entry to the cache. The passed marks represent ranges of the column with matches of the predicate.
    /// `file_metadata_digest` ties the marks to the exact file metadata they were computed from
    /// (see `Entry::file_metadata_digest`); pass 0 when no such digest exists (MergeTree parts).
    void write(
        const UUID & table_id, const String & part_name, UInt64 condition_hash, const String & condition,
        const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark, UInt64 file_metadata_digest = 0);

    /// Check the cache if it contains an entry for the given table + part id and predicate hash.
    /// A single logical consultation may probe more than one key (e.g. the bare condition hash and
    /// a skip-index-profiled hash); pass increment_profile_events = false on the extra probes so the
    /// QueryConditionCacheHits/Misses events count consultations, not internal key lookups.
    /// On a hit, `file_metadata_digest` (when non-null) receives the digest stored with the entry
    /// (see `Entry::file_metadata_digest`; 0 = unknown).
    std::optional<MatchingMarks> read(
        const UUID & table_id, const String & part_name, UInt64 condition_hash,
        bool increment_profile_events = true, UInt64 * file_metadata_digest = nullptr);

    /// For debugging and system tables
    std::vector<QueryConditionCache::Cache::KeyMapped> dump() const;

    void clear();

    void setMaxSizeInBytes(size_t max_size_in_bytes);
    size_t maxSizeInBytes() const;

private:
    Cache cache;
    LoggerPtr logger = getLogger("QueryConditionCache");

    friend class StorageSystemQueryConditionCache;
};

using QueryConditionCachePtr = std::shared_ptr<QueryConditionCache>;

}
