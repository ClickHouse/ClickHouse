#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Storages/MergeTree/ColumnsCache.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNested.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/DeserializationPrefixesCache.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/escapeForFileName.h>
#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>
#include <Core/UUID.h>
#include <IO/SharedThreadPools.h>
#include <Compression/CachedCompressedReadBuffer.h>

#include <algorithm>

namespace ProfileEvents
{
    extern const Event ColumnsCacheHits;
    extern const Event ColumnsCacheMisses;
}

namespace DB
{

namespace
{
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeReaderWide::MergeTreeReaderWide(
    MergeTreeDataPartInfoForReaderPtr data_part_info_,
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
    ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : IMergeTreeReader(
        data_part_info_,
        columns_,
        virtual_fields_,
        storage_snapshot_,
        storage_settings_,
        uncompressed_cache_,
        columns_cache_,
        mark_cache_,
        mark_ranges_,
        settings_,
        avg_value_size_hints_)
    , deserialization_prefixes_cache(deserialization_prefixes_cache_)
    , profile_callback(profile_callback_)
    , clock_type(clock_type_)
    , read_without_marks(
        settings.can_read_part_without_marks
        && all_mark_ranges.isOneRangeForWholePart(data_part_info_for_read->getMarksCount()))
    , log(getLogger("MergeTreeReaderWide"))
{
    requested_column_names.reserve(getColumns().size());
    for (const auto & column : getColumns())
        requested_column_names.push_back(column.name);

    /// The columns cache requires:
    /// - columns_cache is available (it is null when the cache is disabled or sized zero, see `getColumnsCacheIfEnabled`)
    /// - table has a valid UUID (Atomic/Replicated databases only)
    /// Projection parts share the projection name (e.g. "ailog_rule_count") as their part name,
    /// which is not unique across parent parts. This would cause cache key collisions,
    /// so we disable caching for projection parts.
    const bool cache_possible = columns_cache
        && data_part_info_for_read->getTableUUID() != UUIDHelpers::Nil
        && !data_part_info_for_read->isProjectionPart();

    /// A part read without marks has streams that cannot seek, so a granule served from the
    /// cache could not be followed by one read from the part. Such a read still populates the
    /// cache: it reads every granule from its first row to its last.
    columns_cache_reads_possible = cache_possible && settings.enable_columns_cache_reads && !read_without_marks;
    columns_cache_writes_possible = cache_possible && settings.enable_columns_cache_writes;

    try
    {
        for (size_t i = 0; i < columns_to_read.size(); ++i)
        {
            /// Column was dropped by a pending mutation or invalidated. Don't read stale data;
            if (!isColumnDroppedByPendingMutation(i) && !isSystemColumnInvalidated(i))
                addStreams(columns_to_read[i], serializations[i]);
        }
    }
    catch (...)
    {
        if (!isRetryableException(std::current_exception()))
            data_part_info_for_read->reportBroken();
        throw;
    }
}

void MergeTreeReaderWide::prefetchBeginOfRange(Priority priority)
{
    prefetched_streams.clear();

    if (all_mark_ranges.getNumberOfMarks() == 0)
        return;

    /// `readRows` serves a whole range that is in the columns cache from the cached columns,
    /// block by block, and never touches the streams of that range - so prefetching them here
    /// would spend the IO, and on remote storage the object-storage requests, that the cache
    /// exists to avoid. This runs before the task is handed to a reading thread, which is why
    /// the decision has to be probed here rather than read off the reader's state.
    /// The entries can be evicted between this probe and the read, in which case the range is
    /// read from disk without a prefetch: correct, and no worse than a cache miss.
    if (canServeFirstRangeFromCache())
    {
        LOG_TEST(log, "Not prefetching the beginning of the range: it can be served from the columns cache");
        return;
    }

    try
    {
        /// Start prefetches for all columns. But don't deserialize prefixes, because it can be a heavy operation
        /// (for example for JSON column) and starting prefetches for all subcolumns here can consume a lot of memory.
        prefetchForAllColumns(priority, columns_to_read.size(), all_mark_ranges.front().begin, false, /*deserialize_prefixes=*/false);
        prefetched_from_mark = all_mark_ranges.front().begin;
        /// Arguments explanation:
        /// Current prefetch is done for read tasks before they can be picked by reading threads in IMergeTreeReadPool::getTask method.
        /// 1. columns_to_read.size() == requested_columns.size() == readRows::res_columns.size().
        /// 2. continue_reading == false, as we haven't read anything yet.
    }
    catch (...)
    {
        if (!isRetryableException(std::current_exception()))
            data_part_info_for_read->reportBroken();
        throw;
    }
}

void MergeTreeReaderWide::prefetchForAllColumns(
    Priority priority,
    size_t num_columns,
    size_t from_mark,
    bool continue_reading,
    bool deserialize_prefixes)
{
    prefetchForColumns(priority, num_columns, from_mark, deserialize_prefixes,
        [&](size_t) -> std::optional<bool> { return continue_reading; });
}

void MergeTreeReaderWide::prefetchForColumns(
    Priority priority,
    size_t num_columns,
    size_t from_mark,
    bool deserialize_prefixes,
    const ColumnReadSelector & selector)
{
    bool do_prefetch = data_part_info_for_read->getDataPartStorage()->isStoredOnRemoteDisk()
        ? settings.read_settings.remote_fs_settings.prefetch
        : settings.read_settings.local_fs_settings.prefetch;

    if (!do_prefetch || all_mark_ranges.getNumberOfMarks() == 0)
        return;
    if (settings.filesystem_prefetches_limit && num_columns > settings.filesystem_prefetches_limit)
        return;

    if (deserialize_prefixes)
        deserializePrefixForAllColumnsWithPrefetch(num_columns, from_mark, priority);

    /// Request reading of data in advance,
    /// so if reading can be asynchronous, it will also be performed in parallel for all columns.
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        if (isColumnSkipped(pos))
            continue;

        const auto continue_reading = selector(pos);
        if (!continue_reading)
            continue;

        try
        {
            auto & cache = caches[columns_to_read[pos].getNameInStorage()];
            prefetchForColumn(
                priority, columns_to_read[pos], serializations[pos], from_mark, *continue_reading, cache);
        }
        catch (Exception & e)
        {
            /// Better diagnostics.
            e.addMessage("(while reading column " + columns_to_read[pos].name + ")");
            throw;
        }
    }
}

size_t MergeTreeReaderWide::readRows(
    size_t from_mark, size_t current_range_last_mark, bool continue_reading, size_t max_rows_to_read,
    MutableColumns & res_columns)
{
    size_t read_rows = 0;
    if (prefetched_from_mark != -1 && static_cast<size_t>(prefetched_from_mark) != from_mark)
    {
        prefetched_streams.clear();
        prefetched_from_mark = -1;
    }

    try
    {
        size_t num_columns = res_columns.size();
        checkNumberOfColumns(num_columns);

        if (num_columns == 0)
            return max_rows_to_read;

        if (!columns_cache_reads_possible && !columns_cache_writes_possible)
        {
            /// Read from disk: every column that is in the part, continuing or seeking as asked.
            read_rows = readSegmentFromDisk(from_mark, max_rows_to_read, res_columns,
                [&](size_t) -> std::optional<bool> { return continue_reading; });
        }
        else
        {
            /// The rows of one contiguous mark range come out of several calls: one per output
            /// block, plus the calls that skip rows within the range. A call with
            /// `continue_reading == false` begins a new range, or re-enters the current one at
            /// another position after rows were skipped, and in both cases the range read so far
            /// is over: a granule whose copy has not reached its end is dropped.
            ///
            /// Cache lookups are bounded by the contiguous mark range being read, not by the last
            /// mark of the whole task: the granules between the ranges of a multi-range task are
            /// not read. Fall back to the task's last mark when the caller did not provide the
            /// range end.
            if (!continue_reading)
                startColumnsCacheRange(from_mark, current_range_last_mark ? current_range_last_mark : last_mark_to_read, num_columns);

            read_rows = readRowsWithColumnsCache(max_rows_to_read, res_columns);
        }

        /// NOTE: positions for all streams must be kept in sync.
        /// In particular, even if for some streams there are no rows to be read,
        /// you must ensure that no seeks are skipped and at this point they all point to to_mark.
    }
    catch (...)
    {
        if (!isRetryableException(std::current_exception()))
            data_part_info_for_read->reportBroken();

        /// Better diagnostics.
        try
        {
            rethrow_exception(std::current_exception());
        }
        catch (Exception & e)
        {
            e.addMessage(getMessageForDiagnosticOfBrokenPart(from_mark, max_rows_to_read));
        }

        throw;
    }

    return read_rows;
}

size_t MergeTreeReaderWide::readSegmentFromDisk(
    size_t from_mark,
    size_t max_rows_to_read,
    MutableColumns & res_columns,
    const ColumnReadSelector & selector)
{
    const size_t num_columns = res_columns.size();

    /// The streams prefetched by `prefetchBeginOfRange` stand at the first mark of the first
    /// range. A read that starts elsewhere - the granules before it were served from the cache -
    /// has to seek, so those streams do not count as prefetched for it.
    if (prefetched_from_mark != -1 && static_cast<size_t>(prefetched_from_mark) != from_mark)
    {
        prefetched_streams.clear();
        prefetched_from_mark = -1;
    }

    prefetchForColumns(Priority{}, num_columns, from_mark, /*deserialize_prefixes=*/ true, selector);
    deserializePrefixForAllColumns(num_columns, from_mark);

    size_t read_rows = 0;
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        /// Column was dropped by a pending mutation or invalidated.
        /// Don't read stale data; let defaults be used.
        if (isColumnSkipped(pos))
        {
            res_columns[pos] = nullptr;
            continue;
        }

        const auto continue_reading = selector(pos);
        if (!continue_reading)
            continue;

        const auto & column_to_read = columns_to_read[pos];

        /// The column may already be present (we append the values to the end) or empty; either way it is
        /// uniquely owned here, so we read into it directly without cloning.
        auto & column = res_columns[pos];
        if (!column)
            column = column_to_read.type->createColumn(*serializations[pos]);

        size_t column_size_before_reading = column->size();

        try
        {
            auto & cache = caches[column_to_read.getNameInStorage()];
            auto & deserialize_states_cache = deserialize_states_caches[column_to_read.getNameInStorage()];

            readData(
                column_to_read,
                serializations[pos],
                *column,
                from_mark,
                *continue_reading,
                max_rows_to_read,
                cache,
                deserialize_states_cache);

            /// For elements of Nested, column_size_before_reading may be greater than column size
            ///  if offsets are not empty and were already read, but elements are empty.
            if (!column->empty())
                read_rows = std::max(read_rows, column->size() - column_size_before_reading);
        }
        catch (Exception & e)
        {
            /// Better diagnostics.
            e.addMessage("(while reading column " + column_to_read.name + ")");
            throw;
        }

        if (accumulating && !isColumnFilledAfterReading(pos) && column->size() > column_size_before_reading)
            accumulateRowsForColumnsCache(pos, *column, column_size_before_reading, column->size() - column_size_before_reading);

        if (column->empty() && max_rows_to_read > 0)
            res_columns[pos] = nullptr;
    }

    prefetched_streams.clear();
    caches.clear();

    return read_rows;
}

bool MergeTreeReaderWide::isColumnFilledAfterReading(size_t pos) const
{
    return isColumnAbsentFromPart(pos) || isColumnPartiallyRead(pos);
}

bool MergeTreeReaderWide::isColumnAbsentFromPart(size_t pos) const
{
    return columns_absent_from_part.contains(columns_to_read[pos].name);
}

bool MergeTreeReaderWide::isColumnPartiallyRead(size_t pos) const
{
    return partially_read_columns.contains(columns_to_read[pos].name);
}

bool MergeTreeReaderWide::isColumnSkipped(size_t pos) const
{
    return isColumnDroppedByPendingMutation(pos) || isSystemColumnInvalidated(pos);
}

void MergeTreeReaderWide::lookupColumnsCache(
    size_t first_mark, size_t end_mark, size_t num_columns,
    std::vector<std::vector<ColumnsCache::MappedPtr>> & entries, std::vector<bool> & servable)
{
    const size_t num_granules = end_mark - first_mark;
    entries.assign(num_columns, {});
    servable.assign(num_granules, true);

    bool any_regular_column = false;
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        /// Columns dropped by pending mutations, invalidated system columns, and columns that
        /// `fillMissingColumns` synthesizes after the read don't need cache entries: the write
        /// path never produces one for them, so requiring one would make every read of a table
        /// with such a column miss forever. A partially read column is still read from the part
        /// while the rest of the granule is served.
        if (isColumnSkipped(pos) || isColumnFilledAfterReading(pos))
            continue;

        any_regular_column = true;
        entries[pos] = columns_cache->getMany(
            data_part_info_for_read->getTableUUID(),
            data_part_info_for_read->getPartName(),
            requested_column_names[pos],
            settings.columns_cache_schema_identity,
            first_mark,
            end_mark);

        for (size_t i = 0; i < num_granules; ++i)
            if (!entries[pos][i])
                servable[i] = false;
    }

    /// Nothing is served if there is nothing to serve: without a regular column the read produces
    /// no cached rows, and the granules have to be read from the part for whatever they contain.
    if (!any_regular_column)
        servable.assign(num_granules, false);
}

void MergeTreeReaderWide::startColumnsCacheRange(size_t from_mark, size_t end_mark, size_t num_columns)
{
    range_first_mark = from_mark;
    range_end_mark = std::max(from_mark, std::min(end_mark, data_part_info_for_read->getIndexGranularity().getMarksCount()));
    cursor_mark = from_mark;
    cursor_offset = 0;
    disk_positioned.assign(num_columns, false);
    partial_columns_started = false;

    cached_entries.clear();
    granule_served_from_cache.clear();
    resetAccumulatedGranule();

    /// Capture the invalidation generation before anything is read, so that any
    /// invalidation racing with this read is observed. It is passed to `set()` by the
    /// deferred write below: the write is dropped if the table was invalidated or the
    /// whole cache dropped after this point. The schema token of the cache keys is not
    /// taken from here but from the metadata snapshot of the query
    /// (`settings.columns_cache_schema_identity`), so that it cannot disagree with the
    /// schema this read actually uses, see `ColumnsCacheKey::schema_identity`.
    cache_table_generation = columns_cache->getInvalidationGeneration(data_part_info_for_read->getTableUUID());

    if (columns_cache_reads_possible)
    {
        lookupColumnsCache(range_first_mark, range_end_mark, num_columns, cached_entries, granule_served_from_cache);
        LOG_TEST(log, "Range of marks [{}, {}) of part {}: {} of {} granules are in the columns cache",
            range_first_mark, range_end_mark, data_part_info_for_read->getPartName(),
            std::count(granule_served_from_cache.begin(), granule_served_from_cache.end(), true), granule_served_from_cache.size());
    }
}

bool MergeTreeReaderWide::canServeFirstRangeFromCache()
{
    if (!columns_cache_reads_possible || all_mark_ranges.getNumberOfMarks() == 0)
        return false;

    /// A hit is not the same as a read without IO. The lookup ignores the partially read columns,
    /// because the write path can never produce an entry for one, so a range whose serve path
    /// still reads those columns from the part - and deserializes the prefix of every column on
    /// the way - would be left exactly as the repeated read that still goes to object storage
    /// without read-ahead. So the prefetch is skipped only when no stream of the range will be
    /// touched at all.
    if (!partially_read_columns.empty())
        return false;

    /// Only the first mark range matters: that is the only one this prefetch covers. Entries
    /// never span the gaps between the ranges of a task, so `readRows` decides range by range,
    /// and the ranges after the first one are prefetched by `readRows` itself when it reads them
    /// from disk.
    const auto & mark_range = all_mark_ranges.front();
    const size_t end_mark = std::min(mark_range.end, data_part_info_for_read->getIndexGranularity().getMarksCount());

    std::vector<std::vector<ColumnsCache::MappedPtr>> entries;
    std::vector<bool> servable;
    lookupColumnsCache(mark_range.begin, end_mark, columns_to_read.size(), entries, servable);

    return !servable.empty() && std::all_of(servable.begin(), servable.end(), [](bool s) { return s; });
}

size_t MergeTreeReaderWide::readRowsWithColumnsCache(size_t max_rows_to_read, MutableColumns & res_columns)
{
    const auto & index_granularity = data_part_info_for_read->getIndexGranularity();
    const size_t num_columns = res_columns.size();
    const bool has_partial_columns = !partially_read_columns.empty();

    size_t read_rows = 0;
    size_t rows_left = max_rows_to_read;

    while (rows_left > 0 && cursor_mark < range_end_mark)
    {
        const size_t granule_rows = index_granularity.getMarkRows(cursor_mark);
        if (cursor_offset >= granule_rows)
        {
            ++cursor_mark;
            cursor_offset = 0;
            continue;
        }

        const size_t rows_to_read = std::min(rows_left, granule_rows - cursor_offset);
        const size_t granule_index = cursor_mark - range_first_mark;
        const bool reaches_granule_end = cursor_offset + rows_to_read == granule_rows;

        /// Every regular column whose entry for this granule is in the cache is served from it,
        /// the others are read from the part - together with the partially read columns, which
        /// have no entries but are not a no-op to read: their offsets are in the part, and
        /// `fillMissingColumns` sizes every re-added member of their `Nested` group from them.
        size_t served_columns = 0;
        size_t disk_columns = 0;
        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            if (isColumnSkipped(pos) || isColumnFilledAfterReading(pos))
                continue;
            if (isGranuleColumnCached(pos, granule_index))
                ++served_columns;
            else
                ++disk_columns;
        }

        if (served_columns)
            serveRowsFromColumnsCache(granule_index, cursor_offset, rows_to_read, res_columns);

        size_t rows = rows_to_read;
        if (disk_columns || has_partial_columns)
        {
            /// A granule is copied for the cache only when its read starts at its first row, and
            /// only the columns that are read from the part are copied: the others are there already.
            if (accumulating && accumulated_mark != cursor_mark)
                resetAccumulatedGranule();
            if (disk_columns && !accumulating && cursor_offset == 0 && columns_cache_writes_possible
                && canWriteToColumnsCache() && columns_cache->shouldAdmit())
                startAccumulatingGranule(cursor_mark, granule_rows, num_columns);

            const bool continue_partial = partial_columns_started;
            const size_t rows_from_disk = readSegmentFromDisk(cursor_mark, rows_to_read, res_columns,
                [&](size_t pos) -> std::optional<bool>
                {
                    if (isColumnAbsentFromPart(pos))
                        return std::nullopt;
                    if (isColumnPartiallyRead(pos))
                        return continue_partial;
                    if (isGranuleColumnCached(pos, granule_index))
                        return std::nullopt;
                    return static_cast<bool>(disk_positioned[pos]);
                });

            for (size_t pos = 0; pos < num_columns; ++pos)
            {
                if (isColumnSkipped(pos) || isColumnFilledAfterReading(pos))
                    continue;
                /// The streams of a served column stay where they were; the others are at the cursor.
                disk_positioned[pos] = !isGranuleColumnCached(pos, granule_index);
            }
            partial_columns_started = true;

            if (disk_columns)
            {
                /// Zero rows from the part when a regular column was to be read means that
                /// nothing is left; the caller finishes the range by its own count of rows.
                if (rows_from_disk == 0 && served_columns == 0)
                    break;
                rows = std::min(rows, std::max(rows_from_disk, served_columns ? rows_to_read : 0));
            }

            if (reaches_granule_end && rows == rows_to_read)
            {
                if (columns_cache_reads_possible)
                    ProfileEvents::increment(ProfileEvents::ColumnsCacheMisses, disk_columns);
                if (accumulating)
                    finishAccumulatedGranule();
            }
        }
        else
        {
            for (size_t pos = 0; pos < num_columns; ++pos)
                if (!isColumnSkipped(pos) && !isColumnFilledAfterReading(pos))
                    disk_positioned[pos] = false;
        }

        if (reaches_granule_end && rows == rows_to_read)
            ProfileEvents::increment(ProfileEvents::ColumnsCacheHits, served_columns);

        cursor_offset += rows;
        if (cursor_offset >= granule_rows)
        {
            ++cursor_mark;
            cursor_offset = 0;
        }

        rows_left -= rows;
        read_rows += rows;

        /// Fewer rows than asked for: the data of the part ended.
        if (rows < rows_to_read)
            break;
    }

    flushPendingColumnsCacheWrites();
    return read_rows;
}

bool MergeTreeReaderWide::isGranuleColumnCached(size_t pos, size_t granule_index) const
{
    return pos < cached_entries.size() && granule_index < cached_entries[pos].size() && cached_entries[pos][granule_index] != nullptr;
}

void MergeTreeReaderWide::serveRowsFromColumnsCache(size_t granule_index, size_t offset, size_t rows, MutableColumns & res_columns)
{
    const size_t num_columns = res_columns.size();
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        /// Column was dropped by a pending mutation or invalidated - don't serve stale data from
        /// the cache - or not a single one of its streams is in the part, so it has no entry to
        /// serve. Leaving it null is what the disk path does with both (see the `column->empty()`
        /// case of the read loop), and `fillMissingColumns` runs after every read, whether its
        /// rows came from the cache or from disk.
        if (isColumnSkipped(pos) || isColumnAbsentFromPart(pos))
        {
            res_columns[pos] = nullptr;
            continue;
        }

        /// Read from the part by the caller, see `readRowsWithColumnsCache`.
        if (isColumnPartiallyRead(pos) || !isGranuleColumnCached(pos, granule_index))
            continue;

        const auto & entry = cached_entries[pos][granule_index];
        const IColumn & cached_column = *entry->column;
        if (cached_column.size() < offset + rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Columns cache entry of granule {} of column {} of part {} has {} rows, but rows [{}, {}) are requested",
                range_first_mark + granule_index, requested_column_names[pos], data_part_info_for_read->getPartName(),
                cached_column.size(), offset, offset + rows);

        auto & column = res_columns[pos];
        if (!column)
            column = columns_to_read[pos].type->createColumn(*serializations[pos]);

        /// The result column has the concrete type the current serialization produces. If the
        /// entry was written under different settings and holds a `ColumnSparse` while the result
        /// is a full column, insert from a full copy of the range so that `insertRangeFrom` is
        /// type-compatible. The disk-read path returns a `ColumnSparse` for parts with sparse
        /// serialization, and the cache-hit path must do the same to keep downstream behavior
        /// consistent: some aggregate functions (for example `groupConcat`) have a sparse fast
        /// path that processes non-defaults first and all defaults at the end, while the full
        /// path preserves the natural row order.
        const bool cached_is_sparse = typeid_cast<const ColumnSparse *>(&cached_column) != nullptr;
        const bool dst_is_sparse = typeid_cast<const ColumnSparse *>(column.get()) != nullptr;
        if (cached_is_sparse && !dst_is_sparse)
        {
            auto full = cached_column.cut(offset, rows)->convertToFullColumnIfSparse();
            column->insertRangeFrom(*full, 0, rows);
        }
        else
        {
            /// The copy is what keeps the cached column immutable: the result column is mutated
            /// by the read in progress.
            column->insertRangeFrom(cached_column, offset, rows);
        }
    }
}

bool MergeTreeReaderWide::canWriteToColumnsCache() const
{
    /// The query-wide estimate gate can disable cache writes while reads
    /// are already in flight: the estimate is accumulated as read
    /// pools build their tasks, and another pool of the same query may
    /// exceed the budget after this reader started. Consult the shared flag
    /// before writing so the estimate budget applies to the whole query.
    if (settings.columns_cache_writes_disabled
        && settings.columns_cache_writes_disabled->load(std::memory_order_relaxed))
        return false;

    /// The per-query runtime budget only grows during the query, so once it is exhausted the
    /// rest of the read is not worth copying either.
    const auto & bytes_written = settings.columns_cache_bytes_written_so_far;
    const size_t max_bytes = settings.columns_cache_max_bytes_to_write_to_cache;
    if (max_bytes > 0 && bytes_written && bytes_written->load(std::memory_order_relaxed) >= max_bytes)
        return false;

    return true;
}

void MergeTreeReaderWide::startAccumulatingGranule(size_t mark, size_t granule_rows, size_t num_columns)
{
    accumulating = true;
    accumulated_mark = mark;
    accumulated_granule_rows = granule_rows;
    accumulated_columns.clear();
    accumulated_columns.resize(num_columns);
}

void MergeTreeReaderWide::resetAccumulatedGranule()
{
    accumulating = false;
    accumulated_columns.clear();
}

void MergeTreeReaderWide::accumulateRowsForColumnsCache(size_t pos, const IColumn & column, size_t offset, size_t rows)
{
    auto & accumulated = accumulated_columns[pos];
    if (!accumulated)
    {
        accumulated = column.cloneEmpty();
        accumulated->reserve(accumulated_granule_rows);
    }

    /// This copy is the one the cache entry is made of: the accumulated column is moved into
    /// the entry when the granule is complete, and the result column stays uniquely owned by the
    /// read in progress.
    accumulated->insertRangeFrom(column, offset, rows);
}

void MergeTreeReaderWide::finishAccumulatedGranule()
{
    const size_t row_begin = data_part_info_for_read->getIndexGranularity().getMarkStartingRow(accumulated_mark);

    for (size_t pos = 0; pos < accumulated_columns.size(); ++pos)
    {
        auto & column = accumulated_columns[pos];
        if (!column || column->empty())
            continue;

        /// Give back the capacity the accumulation reserved beyond the rows it ended up holding.
        /// `PODArray` rounds a reservation up to a power of two elements, and doubles the element
        /// storage of `String` and `Array` columns on growth, so without this an entry would
        /// occupy - and, since the cache is bounded by the memory an entry retains, be charged
        /// for - up to twice the memory of its rows for the whole time it stays cached.
        column->shrinkToFit();

        const size_t rows = column->size();
        auto entry = std::make_shared<ColumnsCacheEntry>();
        entry->key = ColumnsCacheKey{
            data_part_info_for_read->getTableUUID(),
            data_part_info_for_read->getPartName(),
            requested_column_names[pos],
            accumulated_mark,
            settings.columns_cache_schema_identity};
        entry->row_begin = row_begin;
        entry->rows = rows;
        entry->column = std::move(column);

        pending_entries.push_back(std::move(entry));
    }

    resetAccumulatedGranule();
}

void MergeTreeReaderWide::flushPendingColumnsCacheWrites()
{
    if (pending_entries.empty())
        return;

    std::vector<ColumnsCache::MappedPtr> entries;
    entries.swap(pending_entries);

    /// The budget is an advisory soft threshold, not a hard cap: the entries that cross it are
    /// stored in full and charged afterwards, so the total written may overshoot by up to the
    /// entries of one call (and slightly more under concurrency).
    /// This matches columns_cache_max_bytes_to_write_to_cache's docs.
    if (!canWriteToColumnsCache())
        return;

    LOG_TEST(log, "Writing {} entries of part {} to the columns cache", entries.size(), data_part_info_for_read->getPartName());

    /// Charge the budget only for entries that were actually inserted: a write that is rejected
    /// as stale or does not stay resident must not consume the budget, otherwise a query could
    /// exhaust the cap and skip later real inserts even though those bytes were never written.
    const size_t bytes_admitted = columns_cache->setMany(entries, cache_table_generation);
    if (bytes_admitted && settings.columns_cache_bytes_written_so_far)
        settings.columns_cache_bytes_written_so_far->fetch_add(bytes_admitted, std::memory_order_relaxed);
}

void MergeTreeReaderWide::addStreams(
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization)
{
    bool has_any_stream = false;
    bool has_all_streams = true;

    ISerialization::StreamCallback callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        /// Don't create streams for ephemeral subcolumns that don't store any real data.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
            return;

        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);

        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if (!stream_name)
        {
            has_all_streams = false;
            return;
        }

        if (streams.contains(*stream_name))
        {
            has_any_stream = true;
            return;
        }

        addStream(substream_path, *stream_name);
        has_any_stream = true;
    };

    serialization->enumerateStreams(callback);

    if (has_any_stream && !has_all_streams)
        partially_read_columns.insert(name_and_type.name);

    /// Not a single stream of the column is in the part: it was added by an `ALTER` after the
    /// part was written, and the read produces nothing for it. `partially_read_columns` records
    /// the other half of the same situation, so `isColumnFilledAfterReading` can ask about both.
    if (!has_any_stream)
        columns_absent_from_part.insert(name_and_type.name);
}

MergeTreeReaderWide::FileStreams::iterator MergeTreeReaderWide::addStream(const ISerialization::SubstreamPath & substream_path, const String & stream_name)
{
    auto context = data_part_info_for_read->getContext();
    auto * load_marks_threadpool = settings.load_marks_asynchronously ? &context->getLoadMarksThreadpool() : nullptr;
    size_t num_marks_in_part = data_part_info_for_read->getMarksCount();

    auto marks_loader = std::make_shared<MergeTreeMarksLoader>(
        data_part_info_for_read,
        mark_cache,
        data_part_info_for_read->getIndexGranularityInfo().getMarksFilePath(stream_name),
        num_marks_in_part,
        data_part_info_for_read->getIndexGranularityInfo(),
        settings.save_marks_in_cache,
        settings.read_settings,
        load_marks_threadpool,
        /*num_columns_in_mark=*/ 1,
        settings.use_streaming_marks_compression);

    auto stream_settings = settings;
    stream_settings.is_low_cardinality_dictionary = ISerialization::isLowCardinalityDictionarySubcolumn(substream_path);
    stream_settings.is_metadata_file = ISerialization::isMetadataStream(substream_path);
    stream_settings.is_single_value_per_part = ISerialization::isSingleValuePerPartStream(substream_path);

    size_t data_file_size = data_part_info_for_read->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION);

    auto create_stream = [&]<typename Stream>()
    {
        return std::make_unique<Stream>(
            data_part_info_for_read->getDataPartStorage(), stream_name, DATA_FILE_EXTENSION,
            num_marks_in_part, all_mark_ranges, stream_settings,
            uncompressed_cache, data_file_size,
            std::move(marks_loader), profile_callback, clock_type);
    };

    if (read_without_marks)
        return streams.emplace(stream_name, create_stream.operator()<MergeTreeReaderStreamSingleColumnWholePart>()).first;

    /// Nothing is read from an empty data file (for example, a variant of a `Dynamic` column that has no values
    /// in this part), and all its marks point to the beginning of the file, so its marks file is not needed.
    if (data_file_size != 0)
        marks_loader->startAsyncLoad();

    return streams.emplace(stream_name, create_stream.operator()<MergeTreeReaderStreamSingleColumn>()).first;
}

ReadBuffer * MergeTreeReaderWide::getStream(
    bool seek_to_start,
    const ISerialization::SubstreamPath & substream_path,
    const MergeTreeDataPartChecksums & checksums,
    const NameAndTypePair & name_and_type,
    size_t from_mark,
    bool seek_to_mark,
    ISerialization::SubstreamsCache & cache)
{
    /// If substream have already been read.
    if (cache.contains(ISerialization::getSubcolumnNameForStream(substream_path)))
        return nullptr;

    auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", checksums, storage_settings);
    if (!stream_name)
    {
        /// We allow missing streams only for columns/subcolumns that are not present in this part.
        auto column = data_part_info_for_read->getColumnsDescription().tryGetColumn(GetColumnsOptions::AllPhysical, name_and_type.getNameInStorage());
        if (column && (!name_and_type.isSubcolumn() || column->type->hasSubcolumn(name_and_type.getSubcolumnName())))
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Stream {} for column {} with type {} is not found",
                ISerialization::getFileNameForStream(
                    name_and_type.name, substream_path, ISerialization::StreamFileNameSettings(*storage_settings)),
                    name_and_type.name,
                    name_and_type.type->getName());
        }

        return nullptr;
    }

    auto it = streams.find(*stream_name);
    if (it == streams.end())
    {
        /// If we didn't create requested stream, but file with this path exists, create a stream for it.
        /// It may happen during reading of columns with dynamic subcolumns, because all streams are known
        /// only after deserializing of binary bulk prefix.

        it = addStream(substream_path, *stream_name);
    }

    MergeTreeReaderStream & stream = *it->second;
    stream.adjustRightMark(last_mark_to_read);

    if (seek_to_start)
        stream.seekToStart();
    else if (seek_to_mark)
        stream.seekToMark(from_mark);

    return stream.getDataBuffer();
}

void MergeTreeReaderWide::deserializePrefix(
    const SerializationPtr & serialization,
    const NameAndTypePair & name_and_type,
    size_t from_mark,
    DeserializeBinaryBulkStateMap & deserialize_state_map,
    ISerialization::SubstreamsCache & cache,
    ISerialization::SubstreamsDeserializeStatesCache & deserialize_states_cache,
    ISerialization::StreamCallback prefixes_prefetch_callback)
{
    const auto & name = name_and_type.name;
    if (!deserialize_state_map.contains(name))
    {
        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        deserialize_settings.object_and_dynamic_read_statistics = true;
        deserialize_settings.prefixes_prefetch_callback = prefixes_prefetch_callback;
        deserialize_settings.data_part_type = MergeTreeDataPartType::Wide;
        deserialize_settings.prefixes_deserialization_thread_pool = settings.use_prefixes_deserialization_thread_pool ? &getMergeTreePrefixesDeserializationThreadPool().get() : nullptr;
        deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            /// This stream could be prefetched in prefetchBeginOfRange, but here we
            /// have to seek the stream to the start of file to deserialize the prefix.
            /// If we do not read from the first mark, we should remove this stream from
            /// prefetched_streams to prefetch it again starting from the current mark
            /// after prefix is deserialized.
            if (stream_name && from_mark != 0)
                prefetched_streams.erase(*stream_name);

            return getStream(/* seek_to_start = */true, substream_path, data_part_info_for_read->getChecksums(), name_and_type, 0, /* seek_to_mark = */false, cache);
        };
        deserialize_settings.seek_to_start_callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (!stream_name)
                return;

            if (from_mark != 0)
                prefetched_streams.erase(*stream_name);

            auto it = streams.find(*stream_name);
            if (it == streams.end())
                it = addStream(substream_path, *stream_name);

            it->second->adjustRightMark(last_mark_to_read);
            it->second->seekToStart();
        };
        /// Add streams for newly discovered dynamic subcolumns to start async marks loading beforehand if needed.
        deserialize_settings.dynamic_subcolumns_callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            /// Don't create streams for ephemeral subcolumns that don't store any real data.
            if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                return;

            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (stream_name && !streams.contains(*stream_name))
                addStream(substream_path, *stream_name);
        };
        deserialize_settings.release_stream_callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (stream_name)
                streams.erase(*stream_name);
        };
        deserialize_settings.check_stream_exists_callback = [&](const ISerialization::SubstreamPath & substream_path) -> bool
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
                name_and_type, substream_path, ".bin",
                data_part_info_for_read->getChecksums(), storage_settings);
            return stream_name.has_value();
        };
        deserialize_settings.release_all_prefixes_streams = settings.read_only_column_sample;
        deserialize_settings.has_uniform_marks_callback =
            [&](const ISerialization::SubstreamPath & substream_path,
                size_t max_transitions) -> bool
        {
            /// Wide parts with a final mark have a trailing position after the
            /// suffix, so a single per-part dictionary shows up as <= 2 distinct
            /// positions in the dictionary stream (data mark + final mark).
            /// Without a final mark, the same check must be stricter: a true
            /// single-dictionary part has only one distinct position, while a part
            /// with one dictionary in the main stream and another in the suffix can
            /// still look like "2 positions".
            /// This is only a necessary condition; `SerializationLowCardinality`
            /// still checks the `DictionaryKeys` stream reaches EOF after the
            /// first dictionary.
            const bool has_final_mark = data_part_info_for_read->getIndexGranularity().hasFinalMark();
            const size_t allowed_distinct_marks = has_final_mark || max_transitions == 0
                ? max_transitions
                : max_transitions - 1;

            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
                name_and_type, substream_path, ".bin",
                data_part_info_for_read->getChecksums(), storage_settings);
            if (!stream_name)
                return false;

            auto it = streams.find(*stream_name);
            if (it == streams.end())
                return false;

            return it->second->hasAtMostNDistinctMarks(allowed_distinct_marks);
        };
        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, deserialize_state_map[name], &deserialize_states_cache);
    }
}

void MergeTreeReaderWide::deserializePrefixForAllColumnsImpl(size_t num_columns, size_t from_mark, StreamCallbackGetter prefixes_prefetch_callback_getter)
{
    /// Check if we already deserialized prefixes.
    if (!deserialize_binary_bulk_state_map.empty())
        return;

    auto deserialize = [&]()
    {
        DeserializeBinaryBulkStateMap deserialize_state_map;
        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            if (isColumnDroppedByPendingMutation(pos) || isSystemColumnInvalidated(pos))
                continue;

            try
            {
                auto & cache = caches[columns_to_read[pos].getNameInStorage()];
                auto & deserialize_states_cache = deserialize_states_caches[columns_to_read[pos].getNameInStorage()];
                deserializePrefix(
                    serializations[pos],
                    columns_to_read[pos],
                    from_mark,
                    deserialize_state_map,
                    cache,
                    deserialize_states_cache,
                    prefixes_prefetch_callback_getter ? prefixes_prefetch_callback_getter(columns_to_read[pos]) : ISerialization::StreamCallback{});
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage("(while reading prefix of column " + columns_to_read[pos].name + ")");
                throw;
            }
        }

        return deserialize_state_map;
    };

    /// If we have cache, deserialize through it.
    if (deserialization_prefixes_cache && settings.use_deserialization_prefixes_cache)
        deserialize_binary_bulk_state_map = deserialization_prefixes_cache->getOrSet(deserialize);
    else
        deserialize_binary_bulk_state_map = deserialize();
}

void MergeTreeReaderWide::deserializePrefixForAllColumns(size_t num_columns, size_t from_mark)
{
    deserializePrefixForAllColumnsImpl(num_columns, from_mark, {});
}

void MergeTreeReaderWide::deserializePrefixForAllColumnsWithPrefetch(size_t num_columns, size_t from_mark, Priority priority)
{
    auto prefixes_prefetch_callback_getter = [&](const NameAndTypePair & name_and_type)
    {
        return [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (stream_name && !prefetched_streams.contains(*stream_name))
            {
                if (ReadBuffer * buf = getStream(/* seek_to_start = */true, substream_path, data_part_info_for_read->getChecksums(), name_and_type, 0, /* seek_to_mark = */false, caches[name_and_type.getNameInStorage()]))
                {
                    buf->prefetch(priority);
                    prefetched_streams.insert(*stream_name);
                }
            }
        };
    };

    deserializePrefixForAllColumnsImpl(num_columns, from_mark, prefixes_prefetch_callback_getter);
}

void MergeTreeReaderWide::prefetchForColumn(
    Priority priority,
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization,
    size_t from_mark,
    bool continue_reading,
    ISerialization::SubstreamsCache & cache)
{
    const bool prefix_deserialized = deserialize_binary_bulk_state_map.contains(name_and_type.name);

    auto callback = [&](const ISerialization::SubstreamPath & substream_path)
    {
        /// Skip ephemeral subcolumns that don't store any real data.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
            return;

        /// Skip substreams that don't need to be prefetched.
        if (!ISerialization::isPrefetchNeededForSubstream(substream_path, substream_path.size(), settings.prefetch_json_shared_data_substreams))
            return;

        /// Metadata streams (for example, the structure of `Dynamic` or `JSON`) are read only while deserializing
        /// the prefix, which always reads from the beginning of the file, and are released right after that.
        /// Prefetching such a stream pays off only in `prefetchBeginOfRange` for a range starting at mark 0:
        /// there the prefix is not deserialized yet, and it will reuse exactly this prefetch. Otherwise the
        /// prefetch is either at a wrong offset or the prefix is already deserialized, and the only effect is
        /// creating the stream again and reading the file a second time.
        if (ISerialization::isMetadataStream(substream_path) && (prefix_deserialized || from_mark != 0))
            return;

        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);

        if (stream_name && !prefetched_streams.contains(*stream_name))
        {
            /// There is nothing to prefetch from an empty data file, and its marks are not needed.
            if (data_part_info_for_read->getFileSizeOrZero(*stream_name + DATA_FILE_EXTENSION) == 0)
                return;

            bool seek_to_mark = !continue_reading && !read_without_marks;
            if (ReadBuffer * buf = getStream(false, substream_path, data_part_info_for_read->getChecksums(), name_and_type, from_mark, seek_to_mark, cache))
            {
                buf->prefetch(priority);
                prefetched_streams.insert(*stream_name);
            }
        }
    };

    /// If we already deserialized prefixes, we can use deserialization state during streams enumeration to enumerate dynamic subcolumns.
    if (!deserialize_binary_bulk_state_map.empty())
    {
        auto data = ISerialization::SubstreamData(serialization).withType(name_and_type.type).withDeserializeState(deserialize_binary_bulk_state_map[name_and_type.name]);
        ISerialization::EnumerateStreamsSettings settings;
        serialization->enumerateStreams(settings, callback, data);
    }
    else
    {
        serialization->enumerateStreams(callback);
    }
}


void MergeTreeReaderWide::readData(
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization,
    IColumn & column,
    size_t from_mark,
    bool continue_reading,
    size_t max_rows_to_read,
    ISerialization::SubstreamsCache & cache,
    ISerialization::SubstreamsDeserializeStatesCache & deserialize_states_cache)
{
    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.data_part_type = MergeTreeDataPartType::Wide;
    /// Only the columns whose data files are partly there (see `addStreams`) are refilled by
    /// `IMergeTreeReader::fillMissingColumns`; any other column has to be read in full.
    deserialize_settings.partially_read_columns_are_refilled = partially_read_columns.contains(name_and_type.name);

    deserializePrefix(serialization, name_and_type, from_mark, deserialize_binary_bulk_state_map, cache, deserialize_states_cache, {});

    deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        bool was_prefetched = stream_name && prefetched_streams.contains(*stream_name);
        bool seek_to_mark = !was_prefetched && !continue_reading && !read_without_marks;

        return getStream(
            /* seek_to_start = */false, substream_path,
            data_part_info_for_read->getChecksums(),
            name_and_type, from_mark, seek_to_mark, cache);
    };

    deserialize_settings.seek_stream_to_mark_callback = [&](const ISerialization::SubstreamPath & substream_path, const MarkInCompressedFile & mark)
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        if (!stream_name)
            return;

        streams[*stream_name]->seekToMark(mark);
    };

    /// Seek a substream's stream to the current granule's mark. Needed by serializations that read a
    /// value not implied by the ongoing range position (e.g. a per-part value broadcast to every
    /// granule): the data getter above seeks only conditionally (skipped on prefetch/continue_reading),
    /// so such a stream can be left at a sibling substream's offset. With `read_without_marks` the
    /// whole part is one range read from the start and the stream has no marks to seek to, so skip it.
    deserialize_settings.seek_stream_to_current_mark_callback = [&](const ISerialization::SubstreamPath & substream_path)
    {
        if (read_without_marks)
            return;

        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        if (!stream_name)
            return;

        auto it = streams.find(*stream_name);
        if (it != streams.end())
            it->second->seekToMark(from_mark);
    };

    deserialize_settings.get_avg_value_size_hint_callback
        = [&](const ISerialization::SubstreamPath & substream_path) -> double
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        if (!stream_name)
            return 0.0;

        return avg_value_size_hints[*stream_name];
    };

    deserialize_settings.update_avg_value_size_hint_callback
        = [&](const ISerialization::SubstreamPath & substream_path, const IColumn & column_)
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        if (!stream_name)
            return;

        IDataType::updateAvgValueSizeHint(column_, avg_value_size_hints[*stream_name]);
    };

    deserialize_settings.continuous_reading = continue_reading;
    auto & deserialize_state = deserialize_binary_bulk_state_map[name_and_type.name];

    serialization->deserializeBinaryBulkWithMultipleStreams(
        column, max_rows_to_read, deserialize_settings, deserialize_state, &cache);
}

std::unordered_map<String, std::vector<String>> MergeTreeReaderWide::getAllColumnsSubstreams()
{
    /// We need to read prefixes to be able to collect all streams (because of dynamic structure of some columns).
    deserializePrefixForAllColumns(columns_to_read.size(), 0);
    std::unordered_map<String, std::vector<String>> column_to_streams;
    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        const auto & name_and_type = columns_to_read[i];
        const auto & serialization = serializations[i];

        ISerialization::StreamCallback callback = [&] (const ISerialization::SubstreamPath & substream_path)
        {
            if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                return;

            if (auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings))
                column_to_streams[name_and_type.name].push_back(*stream_name);
        };

        auto data = ISerialization::SubstreamData(serialization).withType(name_and_type.type).withDeserializeState(deserialize_binary_bulk_state_map[name_and_type.name]);
        ISerialization::EnumerateStreamsSettings settings;
        serialization->enumerateStreams(settings, callback, data);
    }

    return column_to_streams;
}

}
