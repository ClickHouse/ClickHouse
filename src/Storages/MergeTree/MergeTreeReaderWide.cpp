#include <Storages/MergeTree/MergeTreeReaderWide.h>

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
#include <Storages/MergeTree/ProjectionIndex/PostingListState.h>
#include <Storages/MergeTree/ProjectionIndex/ProjectionIndexSerializationContext.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <IO/SharedThreadPools.h>
#include <Compression/CachedCompressedReadBuffer.h>

namespace DB
{

namespace
{
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
}

MergeTreeReaderWide::MergeTreeReaderWide(
    MergeTreeDataPartInfoForReaderPtr data_part_info_,
    NamesAndTypesList columns_,
    const VirtualFields & virtual_fields_,
    const StorageSnapshotPtr & storage_snapshot_,
    const MergeTreeSettingsPtr & storage_settings_,
    UncompressedCache * uncompressed_cache_,
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

        prefetchForAllColumns(Priority{}, num_columns, from_mark, current_task_last_mark, continue_reading, /*deserialize_prefixes=*/ true);
        deserializePrefixForAllColumns(num_columns, from_mark, current_task_last_mark);

        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            /// Column was dropped by a pending mutation. Don't read stale data; let defaults be used.
            if (isColumnDroppedByPendingMutation(pos))
            {
                res_columns[pos] = nullptr;
                continue;
            }

            /// Projection text index optimization: after reading the term column (pos 0),
            /// invoke the filter callback to compute which posting rows to deserialize.
            ///
            /// The previous implementation took a "fast path" that skipped `readData`
            /// entirely when nothing matched; on the next adjacent mark with
            /// `continue_reading = true`, the posting stream would still be parked at the
            /// previous mark's start, causing the next mark to deserialize postings from
            /// the wrong byte offset and silently return wrong results. We now always
            /// call `readData` — its deserializer skip path advances the stream past every
            /// row it doesn't materialise.
            if (posting_filter_callback && pos > 0 && res_columns[0])
            {
                auto matched = posting_filter_callback(*res_columns[0]);
                if (matched.size() == res_columns[0]->size())
                {
                    /// All rows matched — no filter needed; skip the per-row dispatch and
                    /// take the cheaper full-deserialize path.
                    matched_row_indices_for_posting.reset();
                }
                else
                {
                    /// Partial or zero matches — engage the filter. An empty vector here
                    /// means "skip every row but advance the stream"; see the optional's
                    /// documentation in IMergeTreeReader.h.
                    matched_row_indices_for_posting = std::move(matched);
                }
            }

            const auto & column_to_read = columns_to_read[pos];

            /// The column is already present in the block so we will append the values to the end.
            bool append = res_columns[pos] != nullptr;
            if (!append)
                res_columns[pos] = column_to_read.type->createColumn(*serializations[pos]);

            auto & column = res_columns[pos];
            try
            {
                size_t column_size_before_reading = column->size();
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

            /// Keep an empty (non-null) posting column when the projection text index filter
            /// is active and returned no matches: downstream `MergeTreeProjectionIndexText`
            /// dereferences `result[1]`, so a nullptr here would crash even though the
            /// empty-match path is the explicitly correct outcome.
            const bool is_empty_match_posting
                = posting_filter_callback && pos > 0
                && matched_row_indices_for_posting.has_value()
                && matched_row_indices_for_posting->empty();
            if (column->empty() && max_rows_to_read > 0 && !is_empty_match_posting)
                res_columns[pos] = nullptr;
        }

        prefetched_streams.clear();
        caches.clear();

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

        if (dynamic_cast<const DataTypePostingList *>(name_and_type.type->getCustomName()))
        {
            large_posting_streams.emplace(*stream_name, createLargePostingStream(*stream_name, profile_callback, clock_type));

            auto pos_stream_name = IMergeTreeDataPart::getStreamNameForColumn(
                name_and_type, substream_path, PROJECTION_INDEX_POSITION_SUFFIX,
                data_part_info_for_read->getChecksums(), storage_settings);
            if (pos_stream_name
                && data_part_info_for_read->getFileSizeOrZero(*pos_stream_name + PROJECTION_INDEX_POSITION_SUFFIX) > 0)
                position_streams.emplace(*pos_stream_name, createPositionStream(*pos_stream_name, profile_callback, clock_type));

            auto idx_stream_name = IMergeTreeDataPart::getStreamNameForColumn(
                name_and_type, substream_path, PROJECTION_INDEX_INDEX_SUFFIX,
                data_part_info_for_read->getChecksums(), storage_settings);
            if (idx_stream_name
                && data_part_info_for_read->getFileSizeOrZero(*idx_stream_name + PROJECTION_INDEX_INDEX_SUFFIX) > 0)
                lidx_streams.emplace(*idx_stream_name, createIndexStream(*idx_stream_name, profile_callback, clock_type));
        }

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

    ProjectionIndexDeserializationContext projection_index_context;
    if (dynamic_cast<const DataTypePostingList *>(name_and_type.type->getCustomName()))
    {
        projection_index_context.large_posting_getter
            = [&](const ISerialization::SubstreamPath & substream_path) -> LargePostingListReaderStreamPtr
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
                name_and_type,
                substream_path,
                PROJECTION_INDEX_LARGE_POSTING_SUFFIX,
                data_part_info_for_read->getChecksums(),
                storage_settings);
            if (!stream_name)
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Projection text index for column {} is missing required stream {}",
                    name_and_type.name, PROJECTION_INDEX_LARGE_POSTING_SUFFIX);
            auto it = large_posting_streams.find(*stream_name);
            if (it == large_posting_streams.end())
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "Projection text index stream {} is referenced but not registered for column {}",
                    *stream_name, name_and_type.name);
            auto posting_reader = it->second;
            posting_reader->getDataBuffer(); /// Call init()
            return posting_reader;
        };

        auto pos_stream_name = IMergeTreeDataPart::getStreamNameForColumn(
            name_and_type, {}, PROJECTION_INDEX_POSITION_SUFFIX,
            data_part_info_for_read->getChecksums(), storage_settings);
        if (pos_stream_name && position_streams.contains(*pos_stream_name))
        {
            projection_index_context.position_getter
                = [&](const ISerialization::SubstreamPath & substream_path) -> LargePostingListReaderStreamPtr
            {
                auto sn = IMergeTreeDataPart::getStreamNameForColumn(
                    name_and_type, substream_path,
                    PROJECTION_INDEX_POSITION_SUFFIX,
                    data_part_info_for_read->getChecksums(), storage_settings);
                if (!sn)
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Projection text index for column {} is missing required stream {}",
                        name_and_type.name, PROJECTION_INDEX_POSITION_SUFFIX);
                auto it = position_streams.find(*sn);
                if (it == position_streams.end())
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Projection text index position stream {} is referenced but not registered for column {}",
                        *sn, name_and_type.name);
                auto pos_reader = it->second;
                pos_reader->getDataBuffer();
                return pos_reader;
            };
        }

        auto idx_stream_name = IMergeTreeDataPart::getStreamNameForColumn(
            name_and_type, {}, PROJECTION_INDEX_INDEX_SUFFIX,
            data_part_info_for_read->getChecksums(), storage_settings);
        if (idx_stream_name && lidx_streams.contains(*idx_stream_name))
        {
            projection_index_context.index_getter
                = [&](const ISerialization::SubstreamPath & substream_path) -> LargePostingListReaderStreamPtr
            {
                auto sn = IMergeTreeDataPart::getStreamNameForColumn(
                    name_and_type, substream_path,
                    PROJECTION_INDEX_INDEX_SUFFIX,
                    data_part_info_for_read->getChecksums(), storage_settings);
                if (!sn)
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Projection text index for column {} is missing required stream {}",
                        name_and_type.name, PROJECTION_INDEX_INDEX_SUFFIX);
                auto it = lidx_streams.find(*sn);
                if (it == lidx_streams.end())
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Projection text index large-index stream {} is referenced but not registered for column {}",
                        *sn, name_and_type.name);
                auto idx_reader = it->second;
                idx_reader->getDataBuffer();
                return idx_reader;
            };
        }

        /// Wire `matched_row_indices` whenever the filter is active (`has_value()`), including
        /// the explicit zero-match case. The reader's `matched_row_indices_for_posting` optional
        /// is `std::nullopt` only when no filter should be applied (no callback OR all rows
        /// matched); in those cases we leave `matched_row_indices` as nullptr so the
        /// deserializer takes the unfiltered read-all path.
        if (matched_row_indices_for_posting.has_value())
            projection_index_context.matched_row_indices = &*matched_row_indices_for_posting;

        deserialize_settings.projection_index_context = &projection_index_context;
    }

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

LargePostingListReaderStreamPtr MergeTreeReaderWide::getProjectionIndexPostingStreamPtr() const
{
    auto it = large_posting_streams.find("posting");
    if (it == large_posting_streams.end())
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
            "posting", {}, PROJECTION_INDEX_LARGE_POSTING_SUFFIX, data_part_info_for_read->getChecksums(), storage_settings);
        if (!stream_name)
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Projection text index posting stream is missing (checksums report no {} file for column 'posting')",
                PROJECTION_INDEX_LARGE_POSTING_SUFFIX);
        return createLargePostingStream(*stream_name, profile_callback, clock_type);
    }
    return it->second;
}

LargePostingListReaderStreamPtr MergeTreeReaderWide::getProjectionIndexPostingIndexStreamPtr() const
{
    auto it = lidx_streams.find("posting");
    if (it == lidx_streams.end())
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
            "posting", {}, PROJECTION_INDEX_INDEX_SUFFIX, data_part_info_for_read->getChecksums(), storage_settings);
        if (!stream_name)
            return nullptr;
        return createIndexStream(*stream_name, profile_callback, clock_type);
    }
    return it->second;
}

}
