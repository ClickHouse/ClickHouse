#pragma once

#include <cstdint>
#include <functional>
#include <optional>

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/ColumnsCache.h>
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

    /// Return map (column to read) -> (list of all streams required to read this column).
    std::unordered_map<String, std::vector<String>> getAllColumnsSubstreams();

private:
    /// Prefixes or data of a single column can be deserialized in parallel from different streams, so
    /// these containers are mutated concurrently and all of the reader's synchronization lives here.
    /// Only the containers are guarded: `MergeTreeReaderStream` is not thread safe and is used with the
    /// mutex released, which is safe because parallel tasks own disjoint stream names, so a stream and
    /// any `ReadBuffer` taken from it stay valid until the owning task calls `release`. The mutex is
    /// never held across stream construction or marks loading, both of which block.
    class FileStreams
    {
    public:
        using StreamFactory = std::function<std::unique_ptr<MergeTreeReaderStream>()>;

        /// `factory` runs only when the stream is missing, and with the mutex released: creating a
        /// stream schedules its marks load, which blocks when the marks pool queue is full.
        MergeTreeReaderStream * getOrCreate(const String & stream_name, const StreamFactory & factory);
        MergeTreeReaderStream * find(const String & stream_name) const;
        void release(const String & stream_name);

        bool isPrefetched(const String & stream_name) const;
        void markPrefetched(const String & stream_name);
        void unmarkPrefetched(const String & stream_name);
        void clearPrefetched();

    private:
        mutable std::mutex mutex;
        std::map<String, std::unique_ptr<MergeTreeReaderStream>> streams;
        std::unordered_set<String> prefetched;
    };

    FileStreams streams;

    /// Which columns a read of the part touches, and whether their streams continue from where the
    /// previous read left them or seek to the mark. Returns nothing for a column that is not read.
    using ColumnReadSelector = std::function<std::optional<bool>(size_t pos)>;

    void prefetchForAllColumns(
        Priority priority,
        size_t num_columns,
        size_t from_mark,
        bool continue_reading,
        bool deserialize_prefixes);

    void prefetchForColumns(
        Priority priority,
        size_t num_columns,
        size_t from_mark,
        bool deserialize_prefixes,
        const ColumnReadSelector & selector);

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

    MergeTreeReaderStream * getOrAddStream(const ISerialization::SubstreamPath & substream_path, const String & stream_name);

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

    /// Read up to `max_rows_to_read` rows of the selected columns from the part, appending them to
    /// `res_columns`, and return how many rows were read (zero if no selected column is in the part).
    /// `sizes_before_reading`, if given, receives the number of rows every column held before.
    size_t readSegmentFromDisk(
        size_t from_mark,
        size_t max_rows_to_read,
        MutableColumns & res_columns,
        const ColumnReadSelector & selector,
        std::vector<size_t> * sizes_before_reading = nullptr);

    std::unordered_map<String, ISerialization::SubstreamsCache> caches;
    std::unordered_map<String, ISerialization::SubstreamsDeserializeStatesCache> deserialize_states_caches;
    DeserializationPrefixesCache * deserialization_prefixes_cache;
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

    /// Columns without a single stream in the part, the counterpart of `partially_read_columns`.
    /// Filled by `addStreams`.
    NameSet columns_absent_from_part;

    /// Columns whose rows are not a function of the row range they are read for, so an entry
    /// written by one read would be wrong for another: the distinct-paths subcolumn of a `JSON`
    /// column (`ObjectDistinctPaths`) emits the path names of the part once per reader and the
    /// shared data paths per call, rather than a value per row. Such a column is read from the
    /// part by every read, like a partially read column. Filled by `addStreams`.
    NameSet columns_not_cacheable;

    /// Whether the column at `pos` is read from the part even when the rest of the granule is
    /// served from the cache: a partially read column or a column that is not cacheable.
    bool isColumnReadOnlyFromPart(size_t pos) const;
    bool hasColumnsReadOnlyFromPart() const;

    /// Whether the column at `pos` is not produced by reading the part but synthesized by
    /// `fillMissingColumns` afterwards: it is absent from the part altogether (added by an
    /// `ALTER` after the part was written), or only some of its streams are there (a `Nested`
    /// member whose offsets come from a sibling). Such a column never gets a cache entry, so a
    /// lookup must not require one - otherwise a table holding one could never be served from
    /// the cache - and it is never accumulated for a write.
    bool isColumnFilledAfterReading(size_t pos) const;

    /// Not a single stream of the column at `pos` is in the part, so reading it produces nothing
    /// and both paths leave it null for `fillMissingColumns`.
    bool isColumnAbsentFromPart(size_t pos) const;

    /// Only some of the streams of the column at `pos` are in the part - in practice a `Nested`
    /// member added by an `ALTER`, whose offsets are read from the shared stream of its group
    /// while its elements stay empty. Reading it does produce data: `fillMissingColumns`
    /// discards its values but takes those offsets to size every re-added member of the group,
    /// so it has to be read from the part even when the rest of the granule is served from cache.
    bool isColumnPartiallyRead(size_t pos) const;

    /// The column at `pos` is not read at all: dropped by a pending mutation, or an invalidated
    /// system column. Both paths leave it null.
    bool isColumnSkipped(size_t pos) const;

    /// The columns cache.
    ///
    /// The cache holds one entry per stripe of granules of a column of a part (see `ColumnsCacheKey`
    /// and `ColumnsCacheStripes`), covering a contiguous run of the granules of the stripe, and a
    /// read goes granule by granule and column by column: a column whose entry covers the granule
    /// is served from it, the others are read from the part, whatever the ranges of the read task
    /// are. The rows read from the part are copied into an entry of their own, which is written
    /// once the run of granules has been read to its end - so the cache never shares column data
    /// with a read still in progress, and a granule that is not read in full (skipped by `PREWHERE`,
    /// or cut short by `LIMIT` or a cancelled query) is not cached.
    ///
    /// Serving a granule from the cache does not move the file streams, so before the next
    /// granule is read from the part its streams are positioned at that granule again. Streams
    /// can only be positioned at marks, which is why the unit is a granule.

    /// Whether this reader can use the cache at all: the cache is on, and the part is a wide part
    /// of a table with a UUID (the key of the cache) and not a projection part - projection parts
    /// share the projection name as their part name, which is not unique across parent parts.
    bool columns_cache_reads_possible = false;
    bool columns_cache_writes_possible = false;

    /// How the marks of the part are cut into stripes, and the identity of every result column
    /// for the cache. See `ColumnsCacheKey`.
    ColumnsCacheStripes stripes;
    std::vector<UInt128> column_identities;

    /// Invalidation generation captured when the read of the range started.
    /// Passed to ColumnsCache::setMany so a deferred write is dropped if the table was
    /// invalidated (e.g. RENAME COLUMN), or the whole cache dropped by `SYSTEM DROP
    /// COLUMNS CACHE`, after the read began. See getInvalidationGeneration.
    UInt64 cache_table_generation = 0;

    /// The contiguous mark range being read, [range_first_mark, range_end_mark), the stripe it
    /// starts in, and the position of the next row to produce: the granule and the offset in it.
    size_t range_first_mark = 0;
    size_t range_end_mark = 0;
    size_t range_first_stripe = 0;
    size_t cursor_mark = 0;
    size_t cursor_offset = 0;

    /// Per result column, whether its streams are positioned at the cursor: true right after a
    /// segment of it was read from the part, false after one was served from the cache. A column
    /// is served or read granule by granule independently of the other columns.
    std::vector<bool> disk_positioned;
    /// Whether the columns read only from the part have been read in this range yet. They are
    /// read from the part for every segment, served or not, so their streams always continue.
    bool partial_columns_started = false;

    /// The entries of the range, per result column, per stripe of the range (nullptr when the
    /// stripe has no entry), and per granule whether every regular column of it is cached.
    std::vector<std::vector<ColumnsCache::MappedPtr>> cached_entries;
    std::vector<bool> granule_served_from_cache;

    const ColumnsCache::MappedPtr & cachedEntryFor(size_t pos, size_t mark) const;
    bool isGranuleColumnCached(size_t pos, size_t mark) const;

    /// The run of granules being copied for the cache, if any: consecutive granules of one
    /// stripe, read from the part for the same columns, from the first row of the first one.
    /// `accumulated_next_mark` is the granule whose rows are being appended and
    /// `accumulated_offset` how many of them are there; the columns hold the rows so far
    /// (nullptr for the columns that are not read from the part).
    bool accumulating = false;
    size_t accumulated_stripe = 0;
    size_t accumulated_first_mark = 0;
    size_t accumulated_next_mark = 0;
    size_t accumulated_offset = 0;
    std::vector<bool> accumulated_from_disk;
    MutableColumns accumulated_columns;

    /// Entries ready to be written, written together at the end of the call.
    std::vector<ColumnsCache::MappedPtr> pending_entries;

    /// Begin a new contiguous mark range at `from_mark`: write what the previous range left,
    /// look the stripes of the new one up in the cache and reset the state.
    void startColumnsCacheRange(size_t from_mark, size_t end_mark, size_t num_columns);

    /// Look the granules [first_mark, end_mark) up in the cache for every regular column. Returns
    /// the entries per column and per stripe, and per granule whether it can be served as a whole.
    void lookupColumnsCache(
        size_t first_mark, size_t end_mark, size_t num_columns,
        std::vector<std::vector<ColumnsCache::MappedPtr>> & entries, std::vector<bool> & servable);

    /// Whether the first mark range of this reader can be served from the cache as a whole
    /// *without touching a single stream of it*, and so need not be prefetched.
    /// See `prefetchBeginOfRange`.
    bool canServeFirstRangeFromCache();

    /// Read up to `max_rows_to_read` rows granule by granule, from the cache or from the part.
    size_t readRowsWithColumnsCache(size_t max_rows_to_read, MutableColumns & res_columns);

    /// Append the `rows` rows starting at row `row_begin` of the part - which lie in the stripe of
    /// granule `first_mark` - to the regular result columns whose entry covers them.
    void serveRowsFromColumnsCache(size_t first_mark, size_t row_begin, size_t rows, MutableColumns & res_columns);

    /// Whether a deferred write may begin or go on: the query-wide budgets may have run out.
    bool canWriteToColumnsCache() const;

    void resetAccumulation();

    /// Copy the rows [offset, offset + rows) of granule `mark`, just read from the part, for the
    /// cache: they are at `sizes_before_reading[pos] + row_in_run` of every result column read
    /// from the part (`from_disk`). A run of granules read to their end becomes an entry when
    /// its stripe ends, when the range ends, or when the run is broken.
    void accumulateRowsForColumnsCache(
        size_t mark,
        size_t offset,
        size_t rows,
        size_t granule_rows,
        size_t row_in_run,
        const std::vector<bool> & from_disk,
        const std::vector<size_t> & sizes_before_reading,
        const MutableColumns & res_columns);

    /// Turn the granules of the accumulated run that were read to their end into entries.
    void finishAccumulatedGranules();

    /// Queue an entry for the granules [first_mark, end_mark) of the result column at `pos`.
    void addPendingColumnsCacheEntry(size_t pos, size_t first_mark, size_t end_mark, MutableColumnPtr column);

    /// Write the pending entries to the cache.
    void flushPendingColumnsCacheWrites();
};

}
