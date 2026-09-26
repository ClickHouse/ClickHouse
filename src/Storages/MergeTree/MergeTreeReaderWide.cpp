#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Storages/MergeTree/ColumnsCache.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeObject.h>
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

    if (columns_cache_reads_possible || columns_cache_writes_possible)
    {
        stripes = ColumnsCacheStripes::forPart(data_part_info_for_read->getIndexGranularity());

        column_identities.reserve(requested_column_names.size());
        for (const auto & name : requested_column_names)
            column_identities.push_back(getColumnsCacheColumnIdentity(
                data_part_info_for_read->getTableUUID(), data_part_info_for_read->getPartName(), name, settings.columns_cache_schema_identity));
    }

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
    streams.clearPrefetched();

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
        streams.clearPrefetched();
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
    const ColumnReadSelector & selector,
    std::vector<size_t> * sizes_before_reading)
{
    const size_t num_columns = res_columns.size();
    if (sizes_before_reading)
        sizes_before_reading->assign(num_columns, 0);

    /// The streams prefetched by `prefetchBeginOfRange` stand at the first mark of the first
    /// range. A read that starts elsewhere - the granules before it were served from the cache -
    /// has to seek, so those streams do not count as prefetched for it.
    if (prefetched_from_mark != -1 && static_cast<size_t>(prefetched_from_mark) != from_mark)
    {
        streams.clearPrefetched();
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
        if (sizes_before_reading)
            (*sizes_before_reading)[pos] = column_size_before_reading;

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

        if (column->empty() && max_rows_to_read > 0)
            res_columns[pos] = nullptr;
    }

    streams.clearPrefetched();
    caches.clear();

    return read_rows;
}

bool MergeTreeReaderWide::isColumnFilledAfterReading(size_t pos) const
{
    return isColumnAbsentFromPart(pos) || isColumnReadOnlyFromPart(pos);
}

bool MergeTreeReaderWide::isColumnReadOnlyFromPart(size_t pos) const
{
    return isColumnPartiallyRead(pos) || columns_not_cacheable.contains(columns_to_read[pos].name);
}

bool MergeTreeReaderWide::hasColumnsReadOnlyFromPart() const
{
    return !partially_read_columns.empty() || !columns_not_cacheable.empty();
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
    const size_t first_stripe = stripes.stripeOf(first_mark);
    const size_t end_stripe = num_granules ? stripes.stripeOf(end_mark - 1) + 1 : first_stripe;
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
        entries[pos] = columns_cache->getMany(column_identities[pos], first_stripe, end_stripe);

        for (size_t i = 0; i < num_granules; ++i)
        {
            const auto & entry = entries[pos][stripes.stripeOf(first_mark + i) - first_stripe];
            if (!entry || !entry->coversMarks(first_mark + i, first_mark + i + 1))
                servable[i] = false;
        }
    }

    /// Nothing is served if there is nothing to serve: without a regular column the read produces
    /// no cached rows, and the granules have to be read from the part for whatever they contain.
    if (!any_regular_column)
        servable.assign(num_granules, false);
}

void MergeTreeReaderWide::startColumnsCacheRange(size_t from_mark, size_t end_mark, size_t num_columns)
{
    /// The previous range is over. The granules of it that were read to their end but not
    /// written yet - the range ended inside a stripe - are written now.
    finishAccumulatedGranules();
    flushPendingColumnsCacheWrites();

    range_first_mark = from_mark;
    range_end_mark = std::max(from_mark, std::min(end_mark, data_part_info_for_read->getIndexGranularity().getMarksCount()));
    range_first_stripe = stripes.stripeOf(from_mark);
    cursor_mark = from_mark;
    cursor_offset = 0;
    disk_positioned.assign(num_columns, false);
    partial_columns_started = false;

    cached_entries.clear();
    granule_served_from_cache.clear();

    /// Capture the invalidation generation before anything is read, so that any
    /// invalidation racing with this read is observed. It is passed to `setMany` by the
    /// deferred write below: the write is dropped if the table was invalidated or the
    /// whole cache dropped after this point. The schema token of the cache keys is not
    /// taken from here but from the metadata snapshot of the query
    /// (`settings.columns_cache_schema_identity`), so that it cannot disagree with the
    /// schema this read actually uses, see `getColumnsCacheColumnIdentity`.
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

    /// A hit is not the same as a read without IO. The lookup ignores the columns that are read
    /// only from the part, because the write path never produces an entry for one, so a range
    /// whose serve path still reads those columns from the part - and deserializes the prefix of
    /// every column on the way - would be left exactly as the repeated read that still goes to
    /// object storage without read-ahead. So the prefetch is skipped only when no stream of the
    /// range will be touched at all.
    if (hasColumnsReadOnlyFromPart())
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

const ColumnsCache::MappedPtr & MergeTreeReaderWide::cachedEntryFor(size_t pos, size_t mark) const
{
    static const ColumnsCache::MappedPtr no_entry;
    if (pos >= cached_entries.size())
        return no_entry;
    const size_t index = stripes.stripeOf(mark) - range_first_stripe;
    if (index >= cached_entries[pos].size())
        return no_entry;
    return cached_entries[pos][index];
}

bool MergeTreeReaderWide::isGranuleColumnCached(size_t pos, size_t mark) const
{
    const auto & entry = cachedEntryFor(pos, mark);
    return entry && entry->coversMarks(mark, mark + 1);
}

size_t MergeTreeReaderWide::readRowsWithColumnsCache(size_t max_rows_to_read, MutableColumns & res_columns)
{
    const auto & index_granularity = data_part_info_for_read->getIndexGranularity();
    const size_t num_columns = res_columns.size();

    /// The block: the pieces of the granules that hold the next `max_rows_to_read` rows of the range.
    struct Segment
    {
        size_t mark;
        size_t offset;
        size_t rows;
        size_t granule_rows;
    };
    std::vector<Segment> segments;
    {
        size_t mark = cursor_mark;
        size_t offset = cursor_offset;
        size_t left = max_rows_to_read;
        while (left > 0 && mark < range_end_mark)
        {
            const size_t granule_rows = index_granularity.getMarkRows(mark);
            if (offset >= granule_rows)
            {
                ++mark;
                offset = 0;
                continue;
            }
            const size_t rows = std::min(left, granule_rows - offset);
            segments.push_back({mark, offset, rows, granule_rows});
            left -= rows;
            offset += rows;
        }
    }

    if (segments.empty())
    {
        finishAccumulatedGranules();
        flushPendingColumnsCacheWrites();
        return 0;
    }

    size_t block_rows = 0;
    for (const auto & segment : segments)
        block_rows += segment.rows;

    /// The regular columns, granule by granule: a column whose entry for the granule is in the
    /// cache is served from it, the others are read from the part. Consecutive granules that read
    /// the same columns from the part are read in one call, so a range that is not cached at all
    /// is read exactly as it is without the cache, and the granules served from one entry are
    /// copied out of it in one piece.
    ///
    /// The columns that are read only from the part are read in the same calls, so that a
    /// partially read `Nested` member shares the call - and the offsets stream, read once per call
    /// for the group - with the members of its group that are read from the part; it has no entry
    /// but is not a no-op to read, because `fillMissingColumns` sizes every re-added member of the
    /// group from its offsets. A column that is not cacheable is read the same way: in one call
    /// per block unless the block mixes served and read granules.
    const bool has_columns_read_only_from_part = hasColumnsReadOnlyFromPart();

    auto columns_from_disk = [&](size_t mark)
    {
        std::vector<bool> from_disk(num_columns, false);
        for (size_t pos = 0; pos < num_columns; ++pos)
            if (!isColumnSkipped(pos) && !isColumnFilledAfterReading(pos) && !isGranuleColumnCached(pos, mark))
                from_disk[pos] = true;
        return from_disk;
    };

    std::vector<size_t> sizes_before_reading;
    size_t read_rows = 0;
    size_t run_begin = 0;
    while (run_begin < segments.size())
    {
        const auto from_disk = columns_from_disk(segments[run_begin].mark);
        size_t run_end = run_begin + 1;
        while (run_end < segments.size() && columns_from_disk(segments[run_end].mark) == from_disk)
            ++run_end;

        size_t run_rows = 0;
        for (size_t i = run_begin; i < run_end; ++i)
            run_rows += segments[i].rows;

        size_t disk_columns = 0;
        size_t served_columns = 0;
        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            if (isColumnSkipped(pos) || isColumnFilledAfterReading(pos))
                continue;
            if (from_disk[pos])
                ++disk_columns;
            else
                ++served_columns;
        }

        if (served_columns)
        {
            /// The segments of one stripe are served from the same entries: one copy per stripe.
            size_t i = run_begin;
            while (i < run_end)
            {
                const size_t stripe = stripes.stripeOf(segments[i].mark);
                const size_t row_begin = index_granularity.getMarkStartingRow(segments[i].mark) + segments[i].offset;
                size_t rows = 0;
                size_t j = i;
                while (j < run_end && stripes.stripeOf(segments[j].mark) == stripe)
                    rows += segments[j++].rows;
                serveRowsFromColumnsCache(segments[i].mark, row_begin, rows, res_columns);
                i = j;
            }
        }

        size_t rows = run_rows;
        if (disk_columns || has_columns_read_only_from_part)
        {
            const bool continue_reading_from_part_only = partial_columns_started;
            const size_t rows_from_disk = readSegmentFromDisk(segments[run_begin].mark, run_rows, res_columns,
                [&](size_t pos) -> std::optional<bool>
                {
                    if (isColumnAbsentFromPart(pos))
                        return std::nullopt;
                    if (isColumnReadOnlyFromPart(pos))
                        return continue_reading_from_part_only;
                    if (!from_disk[pos])
                        return std::nullopt;
                    return static_cast<bool>(disk_positioned[pos]);
                },
                &sizes_before_reading);
            partial_columns_started = true;

            /// The rows of the run are what the columns served from the cache hold, or, without
            /// any, what the read from the part produced - which can be less than asked for at the
            /// end of the part, and nothing at all for a column whose data file is empty (a variant
            /// of a `Dynamic` column without values in the part), just as without the cache: the
            /// range reader then counts the rows from its marks.
            if (served_columns == 0)
                rows = rows_from_disk;

            /// The granules of the run read from the part are copied for the cache.
            size_t row_in_run = 0;
            for (size_t i = run_begin; disk_columns && i < run_end && row_in_run + segments[i].rows <= rows; ++i)
            {
                const auto & segment = segments[i];
                if (columns_cache_writes_possible)
                    accumulateRowsForColumnsCache(segment.mark, segment.offset, segment.rows, segment.granule_rows, row_in_run, from_disk, sizes_before_reading, res_columns);
                if (columns_cache_reads_possible && segment.offset + segment.rows == segment.granule_rows)
                    ProfileEvents::increment(ProfileEvents::ColumnsCacheMisses, disk_columns);
                row_in_run += segment.rows;
            }
        }

        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            if (isColumnSkipped(pos) || isColumnFilledAfterReading(pos))
                continue;
            /// The streams of a served column stay where they were; the others are at the cursor.
            disk_positioned[pos] = from_disk[pos];
        }

        if (served_columns)
        {
            for (size_t i = run_begin; i < run_end; ++i)
                if (segments[i].offset + segments[i].rows == segments[i].granule_rows)
                    ProfileEvents::increment(ProfileEvents::ColumnsCacheHits, served_columns);
        }

        read_rows += rows;

        /// Fewer rows than asked for: the data of the part ended.
        if (rows < run_rows)
            break;

        run_begin = run_end;
    }

    /// Move the cursor over the rows that were produced.
    for (size_t left = std::min(read_rows, block_rows); left > 0;)
    {
        const size_t granule_rows = index_granularity.getMarkRows(cursor_mark);
        if (cursor_offset >= granule_rows)
        {
            ++cursor_mark;
            cursor_offset = 0;
            continue;
        }
        const size_t rows = std::min(left, granule_rows - cursor_offset);
        cursor_offset += rows;
        left -= rows;
    }
    if (cursor_mark < range_end_mark && cursor_offset >= index_granularity.getMarkRows(cursor_mark))
    {
        ++cursor_mark;
        cursor_offset = 0;
    }

    /// The range is read to its end: the granules accumulated for a stripe the range ends in are
    /// written now, as far as they go.
    if (cursor_mark >= range_end_mark)
        finishAccumulatedGranules();

    flushPendingColumnsCacheWrites();
    return read_rows;
}

void MergeTreeReaderWide::serveRowsFromColumnsCache(size_t first_mark, size_t row_begin, size_t rows, MutableColumns & res_columns)
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
        if (isColumnReadOnlyFromPart(pos) || !isGranuleColumnCached(pos, first_mark))
            continue;

        const auto & entry = cachedEntryFor(pos, first_mark);
        const IColumn & cached_column = *entry->column;
        if (row_begin < entry->row_begin || entry->row_begin + cached_column.size() < row_begin + rows)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Columns cache entry of column {} of part {} holds rows [{}, {}), but rows [{}, {}) are requested",
                requested_column_names[pos], data_part_info_for_read->getPartName(),
                entry->row_begin, entry->row_begin + cached_column.size(), row_begin, row_begin + rows);
        const size_t offset = row_begin - entry->row_begin;

        auto & column = res_columns[pos];
        if (!column)
        {
            /// The first rows of the result column are a copy of the cached rows, so that the
            /// column keeps the structure the part gives it and a freshly created column would
            /// not have: the dynamic paths of a `JSON` column and the variants of a `Dynamic`
            /// column come from the prefix of the part, the value of a column stored as a single
            /// value per part is in the data, and a part with sparse serialization produces a
            /// `ColumnSparse`. Downstream behavior depends on all of these: some aggregate
            /// functions (for example `groupConcat`) have a sparse fast path that processes
            /// non-defaults first and all defaults at the end, while the full path preserves the
            /// natural row order. `cut` creates a fresh copy, so mutating it cannot touch the
            /// column stored in the cache.
            column = IColumn::mutate(cached_column.cut(offset, rows));
            continue;
        }

        /// The result column already has its structure, from the granules before this one - read
        /// from the part or served from the cache, both of the same part. If the entry was written
        /// under different settings and holds a `ColumnSparse` while the result is a full column,
        /// insert from a full copy of the range so that `insertRangeFrom` is type-compatible.
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

void MergeTreeReaderWide::resetAccumulation()
{
    accumulating = false;
    accumulated_columns.clear();
}

void MergeTreeReaderWide::addPendingColumnsCacheEntry(size_t pos, size_t first_mark, size_t end_mark, MutableColumnPtr column)
{
    /// Give back the capacity the column holds beyond its rows. `PODArray` rounds an allocation
    /// up to a power of two elements, and the accumulated copy grew as its rows came, so without
    /// this an entry would occupy - and, since the cache is bounded by the memory an entry
    /// retains, be charged for - up to twice the memory of its rows for the whole time it stays
    /// cached.
    column->shrinkToFit();

    auto entry = std::make_shared<ColumnsCacheEntry>();
    entry->table_uuid = data_part_info_for_read->getTableUUID();
    entry->part_name = data_part_info_for_read->getPartName();
    entry->column_name = requested_column_names[pos];
    entry->schema_identity = settings.columns_cache_schema_identity;
    entry->first_mark = first_mark;
    entry->end_mark = end_mark;
    entry->row_begin = data_part_info_for_read->getIndexGranularity().getMarkStartingRow(first_mark);
    entry->rows = column->size();
    entry->key = ColumnsCacheKey{column_identities[pos], stripes.stripeOf(first_mark)};
    entry->column = std::move(column);

    pending_entries.push_back(std::move(entry));
}

void MergeTreeReaderWide::accumulateRowsForColumnsCache(
    size_t mark,
    size_t offset,
    size_t rows,
    size_t granule_rows,
    size_t row_in_run,
    const std::vector<bool> & from_disk,
    const std::vector<size_t> & sizes_before_reading,
    const MutableColumns & res_columns)
{
    /// The rows of the segment that column `pos` holds after the read, or nullptr.
    auto rows_of = [&](size_t pos) -> const IColumn *
    {
        if (!from_disk[pos] || !res_columns[pos])
            return nullptr;
        const auto & column = *res_columns[pos];
        if (column.size() < sizes_before_reading[pos] + row_in_run + rows)
            return nullptr;
        return &column;
    };

    /// The segment continues the accumulated run when it is the next rows of the same stripe,
    /// read from the part for the same columns. Anything else ends the run: the granules of it
    /// read to their end become an entry, and a new run begins with this segment if it begins
    /// at the first row of its granule.
    const bool continues = accumulating
        && stripes.stripeOf(mark) == accumulated_stripe
        && mark == accumulated_next_mark
        && offset == accumulated_offset
        && from_disk == accumulated_from_disk;

    if (!continues)
    {
        finishAccumulatedGranules();

        if (offset != 0 || !canWriteToColumnsCache() || !columns_cache->shouldAdmit())
            return;

        accumulating = true;
        accumulated_stripe = stripes.stripeOf(mark);
        accumulated_first_mark = mark;
        accumulated_next_mark = mark;
        accumulated_offset = 0;
        accumulated_from_disk = from_disk;
        accumulated_columns.clear();
        accumulated_columns.resize(res_columns.size());
    }

    for (size_t pos = 0; pos < res_columns.size(); ++pos)
    {
        const auto * column = rows_of(pos);
        if (!column)
            continue;

        auto & accumulated = accumulated_columns[pos];
        if (!accumulated)
        {
            accumulated = column->cloneEmpty();
            /// The stripe at most; the copy is shrunk to its rows before it is admitted.
            accumulated->reserve(std::min(ColumnsCacheStripes::TARGET_ROWS * 2, stripes.stripe_marks * std::max<size_t>(granule_rows, 1)));
        }
        accumulated->insertRangeFrom(*column, sizes_before_reading[pos] + row_in_run, rows);
    }

    accumulated_offset += rows;
    if (accumulated_offset >= granule_rows)
    {
        ++accumulated_next_mark;
        accumulated_offset = 0;
    }

    /// The stripe is read to its end.
    if (accumulated_next_mark >= stripes.endMark(accumulated_stripe, data_part_info_for_read->getIndexGranularity().getMarksCountWithoutFinal()))
        finishAccumulatedGranules();
}

void MergeTreeReaderWide::finishAccumulatedGranules()
{
    if (!accumulating)
        return;

    /// Only the granules read to their end become an entry; the rows of a granule the block
    /// ended in are dropped.
    if (accumulated_next_mark > accumulated_first_mark)
    {
        const auto & index_granularity = data_part_info_for_read->getIndexGranularity();
        const size_t complete_rows = index_granularity.getMarkStartingRow(accumulated_next_mark) - index_granularity.getMarkStartingRow(accumulated_first_mark);

        for (size_t pos = 0; pos < accumulated_columns.size(); ++pos)
        {
            auto & column = accumulated_columns[pos];
            if (!column || column->empty())
                continue;
            if (column->size() > complete_rows)
                column = IColumn::mutate(column->cut(0, complete_rows));
            if (column->size() == complete_rows)
                addPendingColumnsCacheEntry(pos, accumulated_first_mark, accumulated_next_mark, std::move(column));
        }
    }

    resetAccumulation();
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
    bool has_derived_stream = false;

    /// See `columns_not_cacheable`. The streams of the distinct-paths subcolumn are the structure
    /// and shared data streams of the `JSON` column, so it is recognized by its name: a typed path
    /// spelled the same way is a regular column, and not caching it costs nothing but the caching.
    if (name_and_type.isSubcolumn())
    {
        const auto & subcolumn_name = name_and_type.getSubcolumnName();
        const std::string_view special_name = DataTypeObject::SPECIAL_SUBCOLUMN_NAME_FOR_DISTINCT_PATHS_CALCULATION;
        if (subcolumn_name == special_name || subcolumn_name.ends_with(fmt::format(".{}", special_name)))
            columns_not_cacheable.insert(name_and_type.name);
    }

    ISerialization::StreamCallback callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        /// Don't create streams for ephemeral subcolumns that don't store any real data.
        /// Their rows are derived from the streams of the parent column - the null map of a
        /// sparse `Nullable` from the sparse offsets, for example - which `getStream` opens on
        /// demand while reading, so a column made of them alone is read like any other.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
        {
            has_derived_stream = true;
            return;
        }

        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);

        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if (!stream_name)
        {
            has_all_streams = false;
            return;
        }

        getOrAddStream(substream_path, *stream_name);
        has_any_stream = true;
    };

    serialization->enumerateStreams(callback);

    if (has_any_stream && !has_all_streams)
        partially_read_columns.insert(name_and_type.name);

    /// Not a single stream of the column is in the part: it was added by an `ALTER` after the
    /// part was written, and the read produces nothing for it. `partially_read_columns` records
    /// the other half of the same situation, so `isColumnFilledAfterReading` can ask about both.
    /// A column made of derived substreams alone - the null map of a sparse `Nullable` - has no
    /// stream of its own and is not absent; a column whose real streams are missing is, whatever
    /// derived substreams its type has besides (the element null maps of a `Variant`).
    if (!has_any_stream && (!has_derived_stream || !has_all_streams))
        columns_absent_from_part.insert(name_and_type.name);
}

MergeTreeReaderStream * MergeTreeReaderWide::FileStreams::getOrCreate(const String & stream_name, const StreamFactory & factory)
{
    {
        std::lock_guard lock(mutex);
        if (auto it = streams.find(stream_name); it != streams.end())
            return it->second.get();
    }

    auto stream = factory();

    std::lock_guard lock(mutex);
    auto [it, inserted] = streams.try_emplace(stream_name, std::move(stream));
    chassert(inserted);
    return it->second.get();
}

MergeTreeReaderStream * MergeTreeReaderWide::FileStreams::find(const String & stream_name) const
{
    std::lock_guard lock(mutex);
    auto it = streams.find(stream_name);
    return it == streams.end() ? nullptr : it->second.get();
}

void MergeTreeReaderWide::FileStreams::release(const String & stream_name)
{
    std::unique_ptr<MergeTreeReaderStream> stream;
    {
        std::lock_guard lock(mutex);
        if (auto it = streams.find(stream_name); it != streams.end())
        {
            stream = std::move(it->second);
            streams.erase(it);
        }
    }
    /// Dropped outside the mutex: `~MergeTreeMarksLoader` waits for an in-flight marks load.
}

bool MergeTreeReaderWide::FileStreams::isPrefetched(const String & stream_name) const
{
    std::lock_guard lock(mutex);
    return prefetched.contains(stream_name);
}

void MergeTreeReaderWide::FileStreams::markPrefetched(const String & stream_name)
{
    std::lock_guard lock(mutex);
    prefetched.insert(stream_name);
}

void MergeTreeReaderWide::FileStreams::unmarkPrefetched(const String & stream_name)
{
    std::lock_guard lock(mutex);
    prefetched.erase(stream_name);
}

void MergeTreeReaderWide::FileStreams::clearPrefetched()
{
    std::lock_guard lock(mutex);
    prefetched.clear();
}

MergeTreeReaderStream * MergeTreeReaderWide::getOrAddStream(const ISerialization::SubstreamPath & substream_path, const String & stream_name)
{
    return streams.getOrCreate(stream_name, [&]() -> std::unique_ptr<MergeTreeReaderStream>
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
            return create_stream.operator()<MergeTreeReaderStreamSingleColumnWholePart>();

        /// Nothing is read from an empty data file (for example, a variant of a `Dynamic` column that has no values
        /// in this part), and all its marks point to the beginning of the file, so its marks file is not needed.
        if (data_file_size != 0)
            marks_loader->startAsyncLoad();

        return create_stream.operator()<MergeTreeReaderStreamSingleColumn>();
    });
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
    if (cache.contains(ISerialization::getSubstreamsCacheKeyForStream(substream_path)))
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

    /// If we didn't create requested stream, but file with this path exists, create a stream for it.
    /// It may happen during reading of columns with dynamic subcolumns, because all streams are known
    /// only after deserializing of binary bulk prefix.
    auto * stream = getOrAddStream(substream_path, *stream_name);
    stream->adjustRightMark(last_mark_to_read);

    if (seek_to_start)
        stream->seekToStart();
    else if (seek_to_mark)
        stream->seekToMark(from_mark);

    return stream->getDataBuffer();
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
            /// If we do not read from the first mark, we should unmark this stream as prefetched
            /// to prefetch it again starting from the current mark after prefix is deserialized.
            if (stream_name && from_mark != 0)
                streams.unmarkPrefetched(*stream_name);

            return getStream(/* seek_to_start = */true, substream_path, data_part_info_for_read->getChecksums(), name_and_type, 0, /* seek_to_mark = */false, cache);
        };
        deserialize_settings.seek_to_start_callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (!stream_name)
                return;

            if (from_mark != 0)
                streams.unmarkPrefetched(*stream_name);

            auto * stream = getOrAddStream(substream_path, *stream_name);
            stream->adjustRightMark(last_mark_to_read);
            stream->seekToStart();
        };
        /// Add streams for newly discovered dynamic subcolumns to start async marks loading beforehand if needed.
        deserialize_settings.dynamic_subcolumns_callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            /// Don't create streams for ephemeral subcolumns that don't store any real data.
            if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                return;

            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (stream_name)
                getOrAddStream(substream_path, *stream_name);
        };
        deserialize_settings.release_stream_callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (stream_name)
                streams.release(*stream_name);
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

            auto * stream = streams.find(*stream_name);
            if (!stream)
                return false;

            return stream->hasAtMostNDistinctMarks(allowed_distinct_marks);
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
        /// Resolved here, on the serial thread: the callback below runs on sibling prefix tasks, and
        /// a non-const lookup in `caches` can insert, so several of them looking the same key up at
        /// once would modify the map while the others read it.
        auto * column_cache = &caches[name_and_type.getNameInStorage()];
        return [&, column_cache](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (!stream_name || streams.isPrefetched(*stream_name))
                return;

            if (ReadBuffer * buf = getStream(/* seek_to_start = */true, substream_path, data_part_info_for_read->getChecksums(), name_and_type, 0, /* seek_to_mark = */false, *column_cache))
            {
                buf->prefetch(priority);
                streams.markPrefetched(*stream_name);
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

        if (stream_name && !streams.isPrefetched(*stream_name))
        {
            /// There is nothing to prefetch from an empty data file, and its marks are not needed.
            if (data_part_info_for_read->getFileSizeOrZero(*stream_name + DATA_FILE_EXTENSION) == 0)
                return;

            bool seek_to_mark = !continue_reading && !read_without_marks;
            if (ReadBuffer * buf = getStream(false, substream_path, data_part_info_for_read->getChecksums(), name_and_type, from_mark, seek_to_mark, cache))
            {
                buf->prefetch(priority);
                streams.markPrefetched(*stream_name);
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
        bool was_prefetched = stream_name && streams.isPrefetched(*stream_name);
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

        auto * stream = streams.find(*stream_name);
        if (!stream)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Stream {} for column {} is not found", *stream_name, name_and_type.name);

        stream->seekToMark(mark);
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

        if (auto * stream = streams.find(*stream_name))
            stream->seekToMark(from_mark);
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
