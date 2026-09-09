#pragma once

#include <cstdint>
#include <optional>

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>


namespace DB
{

class MergeTreeDataPartWide;
using DataPartWidePtr = std::shared_ptr<const MergeTreeDataPartWide>;

/// Reader for Wide parts.
class MergeTreeReaderWide : public IMergeTreeReader
{
public:
    MergeTreeReaderWide(
        MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
        NamesAndTypesList columns_,
        const VirtualFields & virtual_fields_,
        const StorageSnapshotPtr & storage_snapshot_,
        const MergeTreeSettingsPtr & storage_settings_,
        UncompressedCache * uncompressed_cache_,
        ColumnsCache * columns_cache_,
        MarkCache * mark_cache_,
        DeserializationPrefixesCache * deserialization_prefixes_cache_,
        MarkRanges mark_ranges_,
        MergeTreeReaderSettings settings_,
        ValueSizeMap avg_value_size_hints_ = {},
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_ = {},
        clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE);

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    size_t readRows(
        size_t from_mark,
        size_t current_range_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        MutableColumns & res_columns) override;

    bool canReadIncompleteGranules() const override { return true; }

    void prefetchBeginOfRange(Priority priority) override;

    using FileStreams = std::map<std::string, std::unique_ptr<MergeTreeReaderStream>>;

    /// Return map (column to read) -> (list of all streams required to read this column).
    std::unordered_map<String, std::vector<String>> getAllColumnsSubstreams();

private:
    FileStreams streams;

    void prefetchForAllColumns(
        Priority priority,
        size_t num_columns,
        size_t from_mark,
        bool continue_reading,
        bool deserialize_prefixes);

    void addStreams(
        const NameAndTypePair & name_and_type,
        const SerializationPtr & serialization);

    ReadBuffer * getStream(
        bool seek_to_start,
        const ISerialization::SubstreamPath & substream_path,
        const MergeTreeDataPartChecksums & checksums,
        const NameAndTypePair & name_and_type,
        size_t from_mark,
        bool seek_to_mark,
        ISerialization::SubstreamsCache & cache);

    FileStreams::iterator addStream(const ISerialization::SubstreamPath & substream_path, const String & stream_name);

    void readData(
        const NameAndTypePair & name_and_type,
        const SerializationPtr & serialization,
        IColumn & column,
        size_t from_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        ISerialization::SubstreamsCache & cache,
        ISerialization::SubstreamsDeserializeStatesCache & deserialize_states_cache);

    /// Make next readData more simple by calling 'prefetch' of all related ReadBuffers (column streams).
    void prefetchForColumn(
        Priority priority,
        const NameAndTypePair & name_and_type,
        const SerializationPtr & serialization,
        size_t from_mark,
        bool continue_reading,
        ISerialization::SubstreamsCache & cache);

    void deserializePrefix(
        const SerializationPtr & serialization,
        const NameAndTypePair & name_and_type,
        size_t from_mark,
        DeserializeBinaryBulkStateMap & deserialize_state_map,
        ISerialization::SubstreamsCache & cache,
        ISerialization::SubstreamsDeserializeStatesCache & deserialize_states_cache,
        ISerialization::StreamCallback prefixes_prefetch_callback);

    void deserializePrefixForAllColumns(size_t num_columns, size_t from_mark);
    void deserializePrefixForAllColumnsWithPrefetch(size_t num_columns, size_t from_mark, Priority priority);

    using StreamCallbackGetter = std::function<ISerialization::StreamCallback(const NameAndTypePair &)>;
    void deserializePrefixForAllColumnsImpl(size_t num_columns, size_t from_mark, StreamCallbackGetter prefixes_prefetch_callback_getter);

    std::unordered_map<String, ISerialization::SubstreamsCache> caches;
    std::unordered_map<String, ISerialization::SubstreamsDeserializeStatesCache> deserialize_states_caches;
    DeserializationPrefixesCache * deserialization_prefixes_cache;
    std::unordered_set<std::string> prefetched_streams;
    ssize_t prefetched_from_mark = -1;
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;
    bool read_without_marks = false;
    LoggerPtr log;

    /// Names of the result columns as the query requests them, position by position with
    /// `columns_to_read`, which holds the names in the part: for a column renamed after the part
    /// was written, the part still knows it by its old name. Cache entries are identified by the
    /// requested name, so `system.columns_cache` and its access checks see the column as the
    /// current schema names it. The renames themselves invalidate the table's entries, so a
    /// name never refers to two columns within one schema identity.
    Names requested_column_names;

    /// State of the deferred columns cache write for the contiguous mark range being read.
    ///
    /// The range reader hands out the rows of one range over several `readRows` calls - one per
    /// output block, plus the calls that skip rows within the range - each into result columns of
    /// its own, while the cache stores one entry per column for the whole range. The rows read
    /// from disk are therefore copied into `cache_accumulated_columns`, in order, across the calls
    /// of the range, and the entries are written once the accumulated rows reach the end of the
    /// range. Deferring the write until then also keeps the cache from ever sharing column data
    /// with a read still in progress. A range that is not read to its end (the query was
    /// cancelled, or stopped early by `LIMIT`) is not cached.
    bool cache_write_pending = false;
    size_t cache_row_begin = 0;
    size_t cache_row_end_max = 0;
    /// One column per result column: the rows read so far; nullptr for columns that are not read.
    MutableColumns cache_accumulated_columns;
    /// The size the accumulated columns of the range are expected to reach, estimated from the
    /// size of a row of every column as it is first copied. A range that cannot stay resident in
    /// the cache is dropped as soon as this estimate says so, before its rows are copied and
    /// before memory is reserved for them.
    size_t cache_estimated_range_bytes = 0;
    /// Invalidation generation captured when the read of the range started.
    /// Passed to ColumnsCache::set so a deferred write is dropped if the table was
    /// invalidated (e.g. RENAME COLUMN), or the whole cache dropped by `SYSTEM DROP
    /// COLUMNS CACHE`, after the read began. See getInvalidationGeneration.
    UInt64 cache_table_generation = 0;

    /// State of a contiguous mark range that is served from the columns cache.
    ///
    /// A range is either served from the cache as a whole or read from disk as a whole:
    /// serving from the cache does not move the file streams, so a range that began from the
    /// cache could not be continued from disk. The decision is made by the first call of the
    /// range, which requires cached entries covering the whole range for every column; the
    /// following calls of the range are served from the columns held here, so the range stays
    /// consistent even if the entries are evicted from the cache meanwhile.
    bool cache_serving = false;
    /// The row the cached entries start at (the same for all columns).
    size_t cache_serving_cached_row_begin = 0;
    /// The next row to serve and the end of the range being served.
    size_t cache_serving_row = 0;
    size_t cache_serving_row_end = 0;
    /// One column per result column; nullptr for columns that are not read.
    Columns cache_serving_columns;

    /// Forget the state of the range being read: called when a new range begins.
    void resetColumnsCacheState();

    /// The cached columns of a range that can be served from the cache as a whole, and the row
    /// the entries they came from start at.
    struct ColumnsCacheRangeHit
    {
        Columns columns;
        size_t cached_row_begin = 0;
    };

    /// Look the whole range [row_begin, row_end) up in the cache for every column that is read.
    /// Returns the cached columns when every one of them is covered by a single entry containing
    /// the range, nothing otherwise. Changes no state, so it can be asked before the task is
    /// handed to a reading thread; `lookupColumnsCache` and `canServeFirstRangeFromCache` are the
    /// two callers, which is what keeps the read decision and the prefetch decision identical.
    std::optional<ColumnsCacheRangeHit> findColumnsCacheEntriesForRange(size_t row_begin, size_t row_end, size_t num_columns);

    /// Look the whole range [row_begin, row_end) up in the cache for every column. On success,
    /// arms `cache_serving` for the range and returns true.
    bool lookupColumnsCache(size_t row_begin, size_t row_end, size_t num_columns);

    /// Whether the column at `pos` is not produced by reading the part but synthesized by
    /// `fillMissingColumns` afterwards: it is absent from the part altogether (added by an
    /// `ALTER` after the part was written), or only some of its streams are there (a `Nested`
    /// member whose offsets come from a sibling). Such a column never gets a cache entry, so a
    /// lookup must not require one - otherwise a table holding one could never be served from
    /// the cache - and the serve path must leave it as the disk path does, for
    /// `fillMissingColumns` to build.
    bool isColumnFilledAfterReading(size_t pos) const;

    /// Columns without a single stream in the part, the counterpart of `partially_read_columns`.
    /// Filled by `addStreams`.
    NameSet columns_absent_from_part;

    /// Whether the first mark range of this reader can be served from the cache as a whole, and
    /// so its streams need not be prefetched. See `prefetchBeginOfRange`.
    bool canServeFirstRangeFromCache();

    /// Serve the next rows of the range from the columns held by `lookupColumnsCache`.
    size_t serveRowsFromColumnsCache(MutableColumns & res_columns, size_t max_rows_to_read);

    /// Whether the deferred write of the current range may go on: the query-wide budgets may
    /// have run out while the range was being read, and a range larger than the whole cache
    /// could never stay resident.
    bool canContinueColumnsCacheWrite() const;

    /// Copy `rows` rows read from disk, starting at `offset` of `column`, into the accumulator of
    /// the result column at `pos`.
    void accumulateRowsForColumnsCache(size_t pos, const IColumn & column, size_t offset, size_t rows);

    /// Write the accumulated columns to the cache if the range has been read to its end.
    void writeToColumnsCacheIfRangeComplete();
};

}
