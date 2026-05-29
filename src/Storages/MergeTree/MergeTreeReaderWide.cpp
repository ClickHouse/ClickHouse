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
    try
    {
        for (size_t i = 0; i < columns_to_read.size(); ++i)
        {
            if (!isColumnDroppedByPendingMutation(i))
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

    try
    {
        /// Start prefetches for all columns. But don't deserialize prefixes, because it can be a heavy operation
        /// (for example for JSON column) and starting prefetches for all subcolumns here can consume a lot of memory.
        prefetchForAllColumns(priority, columns_to_read.size(), all_mark_ranges.front().begin, all_mark_ranges.back().end, false, /*deserialize_prefixes=*/false);
        prefetched_from_mark = all_mark_ranges.front().begin;
        /// Arguments explanation:
        /// Current prefetch is done for read tasks before they can be picked by reading threads in IMergeTreeReadPool::getTask method.
        /// 1. columns_to_read.size() == requested_columns.size() == readRows::res_columns.size().
        /// 3. current_task_last_mark argument in readRows() (which is used only for reading from remote fs to make precise
        /// ranged read requests) is different from current reader's IMergeTreeReader::all_mark_ranges.back().end because
        /// the same reader can be reused between read tasks - if the new task mark ranges correspond to the same part we last
        /// read, so we cannot rely on all_mark_ranges and pass actual current_task_last_mark. But here we can do prefetch for begin
        /// of range only once so there is no such problem.
        /// 4. continue_reading == false, as we haven't read anything yet.
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
    size_t current_task_last_mark,
    bool continue_reading,
    bool deserialize_prefixes)
{
    bool do_prefetch = data_part_info_for_read->getDataPartStorage()->isStoredOnRemoteDisk()
        ? settings.read_settings.remote_fs_settings.prefetch
        : settings.read_settings.local_fs_settings.prefetch;

    if (!do_prefetch || all_mark_ranges.getNumberOfMarks() == 0)
        return;
    if (settings.filesystem_prefetches_limit && num_columns > settings.filesystem_prefetches_limit)
        return;

    if (deserialize_prefixes)
        deserializePrefixForAllColumnsWithPrefetch(num_columns, from_mark, current_task_last_mark, priority);

    /// Request reading of data in advance,
    /// so if reading can be asynchronous, it will also be performed in parallel for all columns.
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
        if (isColumnDroppedByPendingMutation(pos))
            continue;

        try
        {
            auto & cache = caches[columns_to_read[pos].getNameInStorage()];
            prefetchForColumn(
                priority, columns_to_read[pos], serializations[pos], from_mark, continue_reading,
                current_task_last_mark, cache);
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
    size_t from_mark, size_t current_task_last_mark, bool continue_reading, size_t max_rows_to_read,
    size_t rows_offset, Columns & res_columns)
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

        /// Check if we can serve from cache
        /// Cache requires:
        /// - columns_cache is available
        /// - table has a valid UUID (Atomic/Replicated databases only)
        /// - this is not a continuing read (continue_reading == false)
        /// Projection parts share the projection name (e.g. "ailog_rule_count") as their part name,
        /// which is not unique across parent parts. This would cause cache key collisions,
        /// so we disable caching for projection parts.
        const bool cache_possible = columns_cache
            && data_part_info_for_read->getTableUUID() != UUIDHelpers::Nil
            && !data_part_info_for_read->isProjectionPart();
        const bool cache_enabled = cache_possible && !continue_reading;

        /// New task starting - reset deferred cache write state from previous task.
        if (!continue_reading)
            cache_write_pending = false;

        bool serving_from_cache = false;
        std::vector<std::pair<ColumnsCacheKey, ColumnPtr>> cached_columns;

        /// Compute the row range we're trying to read.
        /// When rows_offset > 0, the first rows_offset rows are skipped, so the actual
        /// data starts at row_begin + rows_offset.
        const auto & index_granularity = data_part_info_for_read->getIndexGranularity();
        const size_t mark_row_begin = index_granularity.getMarkStartingRow(from_mark);
        const size_t row_begin = mark_row_begin + rows_offset;
        const size_t row_end_max = (current_task_last_mark < index_granularity.getMarksCount())
            ? index_granularity.getMarkStartingRow(current_task_last_mark)
            : data_part_info_for_read->getRowCount();
        const size_t row_end_query = std::min(row_begin + max_rows_to_read, row_end_max);

        /// When `serving_from_cache` is true, these hold the row range of the cached blocks
        /// and are guaranteed to be valid (they refer to a non-dropped column's cache key).
        size_t cached_row_begin = 0;
        size_t cached_row_end = 0;

        if (cache_enabled && settings.enable_columns_cache_reads)
        {
            LOG_TEST(log, "Checking cache: row_begin={}, row_end_query={}, max_rows_to_read={}",
                row_begin, row_end_query, max_rows_to_read);

            /// Query cache for intersecting blocks for all columns
            bool all_columns_have_cache = true;
            cached_columns.reserve(num_columns);

            for (size_t pos = 0; pos < num_columns; ++pos)
            {
                /// Columns dropped by pending mutations don't need cache entries.
                /// Push a placeholder to keep the vector aligned with column positions.
                if (isColumnDroppedByPendingMutation(pos))
                {
                    cached_columns.emplace_back(ColumnsCacheKey{}, nullptr);
                    continue;
                }

                const auto & column_name = columns_to_read[pos].name;
                auto intersecting = columns_cache->getIntersecting(
                    data_part_info_for_read->getTableUUID(),
                    data_part_info_for_read->getPartName(),
                    column_name,
                    row_begin,
                    row_end_query);

                /// We can serve from cache if we find exactly one block that fully contains
                /// the requested range [row_begin, row_end_query).
                /// This allows us to serve subset reads: if cached [a, b), we can serve [a', b')
                /// where a' >= a and b' <= b.
                if (intersecting.size() == 1
                    && intersecting[0].first.row_begin <= row_begin
                    && intersecting[0].first.row_end >= row_end_query)
                {
                    cached_columns.emplace_back(intersecting[0].first, intersecting[0].second->column);
                    LOG_TEST(log, "Found cached block for column {}: cached=[{}, {}), requested=[{}, {})",
                        column_name, intersecting[0].first.row_begin, intersecting[0].first.row_end, row_begin, row_end_query);
                }
                else
                {
                    all_columns_have_cache = false;
                    LOG_TEST(log, "No suitable cached block for column {}: intersecting.size()={}, need full containment",
                        column_name, intersecting.size());
                    break;
                }
            }

            if (all_columns_have_cache)
            {
                /// Verify all cached blocks have the same row range.
                /// Different columns may have been cached by different queries
                /// with different task boundaries, resulting in different row ranges.
                /// We must check BOTH row_begin and row_end to ensure the offset
                /// calculation is correct for all columns.
                /// Find the first non-dropped column to use as the reference row range.
                size_t ref_pos = 0;
                while (ref_pos < num_columns && isColumnDroppedByPendingMutation(ref_pos))
                    ++ref_pos;

                /// If all columns are dropped, there's nothing to serve from cache.
                bool consistent = (ref_pos < num_columns);
                size_t cached_row_begin_0 = consistent ? cached_columns[ref_pos].first.row_begin : 0;
                size_t cached_row_end_0 = consistent ? cached_columns[ref_pos].first.row_end : 0;

                for (size_t i = ref_pos + 1; i < num_columns && consistent; ++i)
                {
                    if (isColumnDroppedByPendingMutation(i))
                        continue;

                    const auto & key = cached_columns[i].first;
                    if (key.row_begin != cached_row_begin_0 || key.row_end != cached_row_end_0)
                    {
                        consistent = false;
                        LOG_TEST(log, "Inconsistent cached block range: expected=[{}, {}), got=[{}, {})",
                            cached_row_begin_0, cached_row_end_0, key.row_begin, key.row_end);
                    }
                }

                if (consistent)
                {
                    /// Only serve from cache if the cached block covers the ENTIRE task range.
                    /// When readRows is called with max_rows_to_read < total rows in task,
                    /// a subsequent continuation read (continue_reading=true) will follow.
                    /// Since serving from cache does not advance the file stream position,
                    /// the continuation read would start from the wrong position.
                    /// To avoid this, only serve from cache when no continuation is needed.
                    if (row_end_query >= row_end_max)
                    {
                        serving_from_cache = true;
                        cached_row_begin = cached_row_begin_0;
                        cached_row_end = cached_row_end_0;
                        LOG_TEST(log, "Serving from cache: all columns have consistent cached blocks");
                    }
                    else
                    {
                        LOG_TEST(log, "Skipping cache: max_rows_to_read ({}) < task rows ({}), "
                            "continuation reads would have wrong stream position",
                            max_rows_to_read, row_end_max - row_begin);
                    }
                }
            }
        }

        /// Calculate offset within the cached block and how many rows to extract.
        /// These are only meaningful when `serving_from_cache` is true.
        const size_t offset_in_cache = serving_from_cache ? (row_begin - cached_row_begin) : 0;
        const size_t rows_to_serve = serving_from_cache
            ? std::min(row_end_query - row_begin, cached_row_end - row_begin)
            : 0;

        if (serving_from_cache)
        {
            /// Validate all cached columns have sufficient data before modifying res_columns.
            /// The cache key's row range may not match the actual column size
            /// if columns had different row counts when cached.
            for (size_t pos = 0; pos < num_columns; ++pos)
            {
                if (isColumnDroppedByPendingMutation(pos))
                    continue;

                const auto & cached_col = cached_columns[pos].second;
                if (offset_in_cache + rows_to_serve > cached_col->size())
                {
                    LOG_WARNING(log, "Cache entry size mismatch for column {}: "
                        "need offset {} + rows {} = {} but cached column has {} rows, "
                        "falling back to disk read",
                        columns_to_read[pos].name, offset_in_cache, rows_to_serve,
                        offset_in_cache + rows_to_serve, cached_col->size());
                    serving_from_cache = false;
                    break;
                }
            }
        }

        /// Count cache hits/misses at request level, not per-entry.
        /// Must happen after validation above, which may flip `serving_from_cache` to false
        /// on a size mismatch and fall back to a disk read.
        if (cache_enabled && settings.enable_columns_cache_reads)
        {
            if (serving_from_cache)
                ProfileEvents::increment(ProfileEvents::ColumnsCacheHits);
            else
                ProfileEvents::increment(ProfileEvents::ColumnsCacheMisses);
        }

        if (serving_from_cache)
        {
            /// All cached columns validated - serve from cache
            for (size_t pos = 0; pos < num_columns; ++pos)
            {
                /// Column was dropped by a pending mutation. Don't serve stale data from cache.
                if (isColumnDroppedByPendingMutation(pos))
                {
                    res_columns[pos] = nullptr;
                    continue;
                }

                const auto & column_to_read = columns_to_read[pos];
                bool append = res_columns[pos] != nullptr;
                if (!append)
                    res_columns[pos] = column_to_read.type->createColumn(*serializations[pos]);

                /// Extract the needed subset from the cached block.
                const auto & cached_col = cached_columns[pos].second;
                auto cut_column = cached_col->cut(offset_in_cache, rows_to_serve);

                /// `ColumnConst` should never appear in the cache; convert defensively.
                cut_column = cut_column->convertToFullColumnIfConst();

                /// Do NOT convert `ColumnSparse` to a full column unconditionally.
                /// The disk-read path returns a `ColumnSparse` for parts with sparse
                /// serialization, so the cache-hit path must do the same to keep
                /// downstream behavior consistent. Some aggregate functions
                /// (for example `groupConcat`) have a sparse fast path
                /// (`addBatchSparseSinglePlace`) that processes non-defaults first
                /// and all defaults at the end, while the full path preserves the
                /// natural row order. If one query in a session gets sparse and
                /// another gets a full equivalent, results may differ.

                if (!append)
                {
                    res_columns[pos] = std::move(cut_column);
                }
                else
                {
                    /// In the append case, `res_columns[pos]` was created by
                    /// `createColumn(*serializations[pos])` so its type matches the
                    /// current serialization. If the cached column was written under
                    /// different settings and has a different concrete type, fall back
                    /// to a full column so that `insertRangeFrom` is type-compatible.
                    const bool cut_is_sparse = typeid_cast<const ColumnSparse *>(cut_column.get()) != nullptr;
                    const bool dst_is_sparse = typeid_cast<const ColumnSparse *>(res_columns[pos].get()) != nullptr;
                    if (cut_is_sparse && !dst_is_sparse)
                        cut_column = cut_column->convertToFullColumnIfSparse();

                    auto mutable_col = IColumn::mutate(std::move(res_columns[pos]));
                    mutable_col->insertRangeFrom(*cut_column, 0, cut_column->size());
                    res_columns[pos] = std::move(mutable_col);
                }
            }

            read_rows = rows_to_serve;
            LOG_TEST(log, "Served {} rows from cache (offset={}, requested=[{}, {}), cached=[{}, {}))",
                read_rows, offset_in_cache, row_begin, row_end_query, cached_row_begin, cached_row_end);
        }
        else
        {
            /// Read from disk
            prefetchForAllColumns(Priority{}, num_columns, from_mark, current_task_last_mark, continue_reading, /*deserialize_prefixes=*/ true);
            deserializePrefixForAllColumns(num_columns, from_mark, current_task_last_mark);

            /// On the first read of a task, record column sizes for deferred caching.
            /// We defer cache writes until all continuation reads are done,
            /// so we cache the full task range and avoid sharing column pointers
            /// with in-progress reads (which would corrupt cached data via assumeMutable).
            if (!continue_reading && cache_possible && settings.enable_columns_cache_writes)
            {
                cache_write_pending = true;
                cache_row_begin = row_begin;
                cache_task_last_mark = current_task_last_mark;
                cache_column_sizes_at_task_start.resize(num_columns);
            }

            for (size_t pos = 0; pos < num_columns; ++pos)
            {
                /// Column was dropped by a pending mutation. Don't read stale data; let defaults be used.
                if (isColumnDroppedByPendingMutation(pos))
                {
                    res_columns[pos] = nullptr;
                    continue;
                }

                const auto & column_to_read = columns_to_read[pos];

                /// The column is already present in the block so we will append the values to the end.
                bool append = res_columns[pos] != nullptr;
                if (!append)
                    res_columns[pos] = column_to_read.type->createColumn(*serializations[pos]);

                auto & column = res_columns[pos];
                size_t column_size_before_reading = column->size();

                /// Record column sizes at task start for deferred caching.
                if (!continue_reading && cache_write_pending)
                    cache_column_sizes_at_task_start[pos] = column_size_before_reading;

                try
                {
                    auto & cache = caches[column_to_read.getNameInStorage()];
                    auto & deserialize_states_cache = deserialize_states_caches[column_to_read.getNameInStorage()];

                    readData(
                        column_to_read,
                        serializations[pos],
                        column,
                        from_mark,
                        continue_reading,
                        current_task_last_mark,
                        max_rows_to_read,
                        rows_offset,
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

            /// Check if deferred cache write should execute.
            /// We cache only when the full task range has been read (all continuation reads are done).
            if (cache_write_pending && read_rows > 0)
            {
                size_t cache_row_end_max = (cache_task_last_mark < index_granularity.getMarksCount())
                    ? index_granularity.getMarkStartingRow(cache_task_last_mark)
                    : data_part_info_for_read->getRowCount();

                /// Compute total rows read across all calls for this task.
                size_t total_rows_for_task = 0;
                for (size_t pos = 0; pos < num_columns; ++pos)
                {
                    if (res_columns[pos] && !res_columns[pos]->empty()
                        && res_columns[pos]->size() > cache_column_sizes_at_task_start[pos])
                    {
                        total_rows_for_task = std::max(
                            total_rows_for_task,
                            res_columns[pos]->size() - cache_column_sizes_at_task_start[pos]);
                    }
                }

                if (cache_row_begin + total_rows_for_task >= cache_row_end_max)
                {
                    /// All continuation reads are done - cache the full task range.
                    size_t row_end = cache_row_begin + total_rows_for_task;

                    LOG_TEST(log, "Caching columns (deferred): row_begin={}, row_end={}, rows={}",
                        cache_row_begin, row_end, total_rows_for_task);

                    for (size_t pos = 0; pos < num_columns; ++pos)
                    {
                        if (res_columns[pos] && !res_columns[pos]->empty()
                            && !partially_read_columns.contains(columns_to_read[pos].name)
                            && res_columns[pos]->size() > cache_column_sizes_at_task_start[pos])
                        {
                            size_t rows_to_cache = res_columns[pos]->size() - cache_column_sizes_at_task_start[pos];

                            /// Use cut to create an independent copy for the cache.
                            ColumnPtr column_to_cache = res_columns[pos]->cut(
                                cache_column_sizes_at_task_start[pos], rows_to_cache);

                            /// Use per-column row_end based on actual rows in this column,
                            /// not the shared total_rows_for_task which may be larger if
                            /// other columns had more rows (e.g. for Nested or complex types).
                            size_t column_row_end = cache_row_begin + rows_to_cache;

                            ColumnsCacheKey cache_key{
                                data_part_info_for_read->getTableUUID(),
                                data_part_info_for_read->getPartName(),
                                columns_to_read[pos].name,
                                cache_row_begin,
                                column_row_end};

                            auto entry = std::make_shared<ColumnsCacheEntry>(
                                ColumnsCacheEntry{std::move(column_to_cache), rows_to_cache});

                            /// Per-query runtime cap: stop writing once the pool has spent its budget.
                            /// Slight overshoot under concurrency is acceptable — the budget is advisory.
                            const auto & bytes_written = settings.columns_cache_bytes_written_so_far;
                            const size_t max_bytes = settings.columns_cache_max_bytes_to_write_to_cache;
                            if (max_bytes > 0 && bytes_written && bytes_written->load(std::memory_order_relaxed) >= max_bytes)
                            {
                                LOG_TEST(log, "Skipping cache write: per-query budget exhausted ({} >= {})",
                                    bytes_written->load(std::memory_order_relaxed), max_bytes);
                                continue;
                            }

                            const size_t entry_weight = ColumnsCacheWeightFunction{}(*entry);
                            columns_cache->set(cache_key, entry);
                            if (bytes_written)
                                bytes_written->fetch_add(entry_weight, std::memory_order_relaxed);

                            LOG_TEST(log, "Cached column: {}, row_begin={}, row_end={}, rows={}",
                                columns_to_read[pos].name, cache_row_begin, column_row_end, rows_to_cache);
                        }
                    }

                    cache_write_pending = false;
                }
            }

            prefetched_streams.clear();
            caches.clear();
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
            e.addMessage(getMessageForDiagnosticOfBrokenPart(from_mark, max_rows_to_read, rows_offset));
        }

        throw;
    }

    return read_rows;
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
        /*num_columns_in_mark=*/ 1);

    auto stream_settings = settings;
    stream_settings.is_low_cardinality_dictionary = ISerialization::isLowCardinalityDictionarySubcolumn(substream_path);
    stream_settings.is_metadata_file = ISerialization::isMetadataStream(substream_path);

    auto create_stream = [&]<typename Stream>()
    {
        return std::make_unique<Stream>(
            data_part_info_for_read->getDataPartStorage(), stream_name, DATA_FILE_EXTENSION,
            num_marks_in_part, all_mark_ranges, stream_settings,
            uncompressed_cache, data_part_info_for_read->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
            std::move(marks_loader), profile_callback, clock_type);
    };

    if (read_without_marks)
        return streams.emplace(stream_name, create_stream.operator()<MergeTreeReaderStreamSingleColumnWholePart>()).first;

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
    size_t current_task_last_mark,
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
    stream.adjustRightMark(current_task_last_mark);

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
    size_t current_task_last_mark,
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

            return getStream(/* seek_to_start = */true, substream_path, data_part_info_for_read->getChecksums(), name_and_type, 0, /* seek_to_mark = */false, current_task_last_mark, cache);
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

            it->second->adjustRightMark(current_task_last_mark);
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

void MergeTreeReaderWide::deserializePrefixForAllColumnsImpl(size_t num_columns, size_t from_mark, size_t current_task_last_mark, StreamCallbackGetter prefixes_prefetch_callback_getter)
{
    /// Check if we already deserialized prefixes.
    if (!deserialize_binary_bulk_state_map.empty())
        return;

    auto deserialize = [&]()
    {
        DeserializeBinaryBulkStateMap deserialize_state_map;
        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            if (isColumnDroppedByPendingMutation(pos))
                continue;

            try
            {
                auto & cache = caches[columns_to_read[pos].getNameInStorage()];
                auto & deserialize_states_cache = deserialize_states_caches[columns_to_read[pos].getNameInStorage()];
                deserializePrefix(
                    serializations[pos],
                    columns_to_read[pos],
                    from_mark,
                    current_task_last_mark,
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

void MergeTreeReaderWide::deserializePrefixForAllColumns(size_t num_columns, size_t from_mark, size_t current_task_last_mark)
{
    deserializePrefixForAllColumnsImpl(num_columns, from_mark, current_task_last_mark, {});
}

void MergeTreeReaderWide::deserializePrefixForAllColumnsWithPrefetch(size_t num_columns, size_t from_mark, size_t current_task_last_mark, Priority priority)
{
    auto prefixes_prefetch_callback_getter = [&](const NameAndTypePair & name_and_type)
    {
        return [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
            if (stream_name && !prefetched_streams.contains(*stream_name))
            {
                if (ReadBuffer * buf = getStream(/* seek_to_start = */true, substream_path, data_part_info_for_read->getChecksums(), name_and_type, 0, /* seek_to_mark = */false, current_task_last_mark, caches[name_and_type.getNameInStorage()]))
                {
                    buf->prefetch(priority);
                    prefetched_streams.insert(*stream_name);
                }
            }
        };
    };

    deserializePrefixForAllColumnsImpl(num_columns, from_mark, current_task_last_mark, prefixes_prefetch_callback_getter);
}

void MergeTreeReaderWide::prefetchForColumn(
    Priority priority,
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization,
    size_t from_mark,
    bool continue_reading,
    size_t current_task_last_mark,
    ISerialization::SubstreamsCache & cache)
{
    auto callback = [&](const ISerialization::SubstreamPath & substream_path)
    {
        /// Skip ephemeral subcolumns that don't store any real data.
        if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
            return;

        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);

        if (stream_name && !prefetched_streams.contains(*stream_name))
        {
            bool seek_to_mark = !continue_reading && !read_without_marks;
            if (ReadBuffer * buf = getStream(false, substream_path, data_part_info_for_read->getChecksums(), name_and_type, from_mark, seek_to_mark, current_task_last_mark, cache))
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
    ColumnPtr & column,
    size_t from_mark,
    bool continue_reading,
    size_t current_task_last_mark,
    size_t max_rows_to_read,
    size_t rows_offset,
    ISerialization::SubstreamsCache & cache,
    ISerialization::SubstreamsDeserializeStatesCache & deserialize_states_cache)
{
    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.data_part_type = MergeTreeDataPartType::Wide;

    deserializePrefix(serialization, name_and_type, from_mark, current_task_last_mark, deserialize_binary_bulk_state_map, cache, deserialize_states_cache, {});

    deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        bool was_prefetched = stream_name && prefetched_streams.contains(*stream_name);
        bool seek_to_mark = !was_prefetched && !continue_reading && !read_without_marks;

        return getStream(
            /* seek_to_start = */false, substream_path,
            data_part_info_for_read->getChecksums(),
            name_and_type, from_mark, seek_to_mark, current_task_last_mark, cache);
    };

    deserialize_settings.seek_stream_to_mark_callback = [&](const ISerialization::SubstreamPath & substream_path, const MarkInCompressedFile & mark)
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, ".bin", data_part_info_for_read->getChecksums(), storage_settings);
        if (!stream_name)
            return;

        streams[*stream_name]->seekToMark(mark);
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
        column, rows_offset, max_rows_to_read, deserialize_settings, deserialize_state, &cache);
}

std::unordered_map<String, std::vector<String>> MergeTreeReaderWide::getAllColumnsSubstreams()
{
    /// We need to read prefixes to be able to collect all streams (because of dynamic structure of some columns).
    deserializePrefixForAllColumns(columns_to_read.size(), 0, getLastMark(all_mark_ranges));
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
