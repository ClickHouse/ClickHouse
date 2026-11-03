#pragma once

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>
#include <Common/Logger.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Common/SharedMutex.h>

#include <limits>
#include <mutex>

namespace DB
{

struct Settings;
class ColumnsDescription;
class ColumnMapper;
struct FormatSettings;

/// Settings that change how a function inside a condition evaluates without leaving any trace in the
/// condition's `ActionsDAG` (the `formatDateTime`/`parseDateTime` family, `locate`, `least`/`greatest` and a few more
/// read them while they run; the registration rule is in the definition).
/// Two queries whose conditions differ only in those settings must not share a cache entry: a mark verdict
/// computed under one value is wrong under the other. Fold the returned salt into the condition hash.
UInt64 queryConditionCacheSettingsSalt(const Settings & settings);

/// Combines the hash of a condition's `ActionsDAG` with that salt. Every producer of a query condition
/// cache key has to use it, or a verdict written by one query is never found by the next one.
UInt64 queryConditionCacheHash(UInt64 condition_dag_hash, UInt64 settings_salt);


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

    /// The exact number of rows of each mark that match the predicate, or `UNKNOWN_MATCH_COUNT`.
    /// Unlike `MatchingMarks`, these are exact, so a `count()` filtered by the predicate can add them up instead of
    /// reading the marks again (see `optimizeUseAggregateProjections`). Only reads that need them record them.
    /// Two bytes per mark: a mark with `UNKNOWN_MATCH_COUNT` or more matching rows is not counted (a granule has
    /// at most `index_granularity` rows, 8192 by default). They are kept apart from the `MatchingMarks`, see `counts_cache`.
    using MatchCounts = std::vector<UInt16>;
    static constexpr UInt16 UNKNOWN_MATCH_COUNT = std::numeric_limits<UInt16>::max();
    /// Pairs of (mark, number of rows of the mark that match the predicate).
    using MarkMatchCounts = std::vector<std::pair<size_t, size_t>>;

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
        SharedMutex mutex; /// (*)

        explicit Entry(size_t mark_count); /// (**)

#if defined(DEBUG_OR_SANITIZER_BUILD)
        Entry(size_t mark_count_, const UUID & table_id_, const String & part_name_, UInt64 condition_hash_, const String & condition_);
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

    struct CountsEntry
    {
        MatchCounts match_counts;
        SharedMutex mutex;

        explicit CountsEntry(size_t mark_count) : match_counts(mark_count, UNKNOWN_MATCH_COUNT) {}
    };

    struct CountsEntryWeight
    {
        /// The weight of the match counts of `marks_count` marks, or the maximum `size_t` if it overflows.
        static size_t forMarks(size_t marks_count);
        size_t operator()(const CountsEntry & entry) const;
    };

    using CountsCache = CacheBase<Key, CountsEntry, UInt128TrivialHash, CountsEntryWeight>;

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

    /// The `table_id` for the entries of a file-backed table. A `part_name` from `makeFilePartName` that
    /// identifies the data by itself - an absolute local path with its version token, or the full
    /// location of a remote object, including the storage it is in, with its ETag (or of an immutable
    /// data lake file) - needs no table to be unambiguous. So a table without a UUID (the one behind a
    /// table function like `file` or `s3`, or a table in a database without UUIDs) keeps such entries
    /// without a table, while a `Nil` `table_id` disables the cache (see `write` and `read`).
    /// The `part_name` identifies only the bytes of the file, not how they are read: the same file can be
    /// read through another format, compression or structure (e.g. a column missing in the file with
    /// another default), and a verdict of one such reader is wrong for another. A table fixes all of them,
    /// so a table without a UUID gets an id derived from them instead. A data lake also maps its columns to
    /// the fields of a file by field ids (`column_mapper`, may be null), and the same file and columns can
    /// map to different fields (e.g. a column dropped and added again under the same name, read at two
    /// snapshots), so the mapping is part of the id too. A table can also fix the format settings of its
    /// reader at `CREATE` (`format_settings`, null when the reader takes them from the query context, which
    /// the condition hash covers already), and those that change the verdict are part of the id as well.
    static UUID getTableIdForFileEntries(
        const UUID & table_uuid,
        std::string_view format,
        std::string_view compression_method,
        const ColumnsDescription & columns,
        const ColumnMapper * column_mapper,
        const FormatSettings * format_settings);

    QueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio);

    /// Add an entry to the cache. The passed marks represent ranges of the column with matches of the predicate.
    void write(
        const UUID & table_id, const String & part_name, UInt64 condition_hash, const String & condition,
        const MarkRanges & mark_ranges, size_t marks_count, bool has_final_mark);

    /// Check the cache if it contains an entry for the given table + part id and predicate hash.
    /// A single logical consultation may probe more than one key (e.g. the bare condition hash and
    /// a skip-index-profiled hash); pass increment_profile_events = false on the extra probes so the
    /// QueryConditionCacheHits/Misses events count consultations, not internal key lookups.
    std::optional<MatchingMarks> read(const UUID & table_id, const String & part_name, UInt64 condition_hash, bool increment_profile_events = true);

    /// Record the exact number of rows matching the predicate in the given marks of the part. Only a read that evaluates
    /// exactly the predicate of `condition_hash` on every row of these marks may call this: no filter that the hash does
    /// not cover may run before it, and the predicate must not be rounded (time conditions) or salted (TopK). See
    /// `MergeTreeSelectProcessor`.
    void writeMatchCounts(
        const UUID & table_id, const String & part_name, UInt64 condition_hash, const String & condition,
        const MarkMatchCounts & mark_match_counts, size_t marks_count, bool has_final_mark);

    /// The exact number of rows matching the predicate per mark of the part, if any were recorded (`UNKNOWN_MATCH_COUNT`
    /// for the marks that were not counted).
    std::optional<MatchCounts> readMatchCounts(const UUID & table_id, const String & part_name, UInt64 condition_hash);

    /// For debugging and system tables
    std::vector<QueryConditionCache::Cache::KeyMapped> dump() const;

    void clear();

    void setMaxSizeInBytes(size_t max_size_in_bytes);
    size_t maxSizeInBytes() const;

private:
    Cache cache;
    /// The match counts take 16 times the memory of the `MatchingMarks` of the same marks, and they only save reading the
    /// marks that match, while the `MatchingMarks` save reading all the others. So the match counts only use the space the
    /// `MatchingMarks` leave free: `cache` keeps the whole size, and a new `MatchingMarks` entry evicts the least recently
    /// used match counts if both do not fit together. Caching match counts never evicts `MatchingMarks`.
    CountsCache counts_cache;

    /// Serializes the changes of the size of `counts_cache`.
    std::mutex counts_size_mutex;

    /// Limits `counts_cache` to the space `cache` leaves free (evicting the least recently used match counts if they do not
    /// fit) and returns that space.
    size_t fitMatchCountsIntoFreeSpace() TSA_REQUIRES(counts_size_mutex);

    LoggerPtr logger = getLogger("QueryConditionCache");

    friend class StorageSystemQueryConditionCache;
};

using QueryConditionCachePtr = std::shared_ptr<QueryConditionCache>;

}
