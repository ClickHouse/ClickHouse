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
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <IO/SharedThreadPools.h>

namespace DB
{

namespace
{
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}

MergeTreeReaderWide::MergeTreeReaderWide(
    MergeTreeDataPartInfoForReaderPtr data_part_info_,
    NamesAndTypesList columns_,
    const VirtualFields & virtual_fields_,
    const StorageSnapshotPtr & storage_snapshot_,
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
            addStreams(columns_to_read[i], serializations[i]);
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
        ? settings.read_settings.remote_fs_prefetch
        : settings.read_settings.local_fs_prefetch;

    if (!do_prefetch)
        return;

    if (deserialize_prefixes)
        deserializePrefixForAllColumnsWithPrefetch(num_columns, from_mark, current_task_last_mark, priority);

    /// Request reading of data in advance,
    /// so if reading can be asynchronous, it will also be performed in parallel for all columns.
    for (size_t pos = 0; pos < num_columns; ++pos)
    {
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

            if (column->empty() && max_rows_to_read > 0)
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

        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, data_part_info_for_read->getChecksums());

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
    auto * load_marks_threadpool = settings.read_settings.load_marks_asynchronously ? &context->getLoadMarksThreadpool() : nullptr;
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
    stream_settings.is_dynamic_or_object_structure = ISerialization::isDynamicOrObjectStructureSubcolumn(substream_path);

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

    auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, checksums);
    if (!stream_name)
        return nullptr;

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
    if (!deserialize_binary_bulk_state_map.contains(name))
    {
        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        deserialize_settings.object_and_dynamic_read_statistics = true;
        deserialize_settings.prefixes_prefetch_callback = prefixes_prefetch_callback;
        deserialize_settings.prefixes_deserialization_thread_pool = settings.use_prefixes_deserialization_thread_pool ? &getMergeTreePrefixesDeserializationThreadPool().get() : nullptr;
        deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
        {
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, data_part_info_for_read->getChecksums());
            /// This stream could be prefetched in prefetchBeginOfRange, but here we
            /// have to seek the stream to the start of file to deserialize the prefix.
            /// If we do not read from the first mark, we should remove this stream from
            /// prefetched_streams to prefetch it again starting from the current mark
            /// after prefix is deserialized.
            if (stream_name && from_mark != 0)
                prefetched_streams.erase(*stream_name);

            return getStream(/* seek_to_start = */true, substream_path, data_part_info_for_read->getChecksums(), name_and_type, 0, /* seek_to_mark = */false, current_task_last_mark, cache);
        };
        /// Add streams for newly discovered dynamic subcolumns to start async marks loading beforehand if needed.
        deserialize_settings.dynamic_subcolumns_callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            /// Don't create streams for ephemeral subcolumns that don't store any real data.
            if (ISerialization::isEphemeralSubcolumn(substream_path, substream_path.size()))
                return;

            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, data_part_info_for_read->getChecksums());
            if (!streams.contains(*stream_name))
                addStream(substream_path, *stream_name);
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
            auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, data_part_info_for_read->getChecksums());
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

        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, data_part_info_for_read->getChecksums());

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
    double & avg_value_size_hint = avg_value_size_hints[name_and_type.name];
    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.avg_value_size_hint = avg_value_size_hint;

    deserializePrefix(serialization, name_and_type, from_mark, current_task_last_mark, deserialize_binary_bulk_state_map, cache, deserialize_states_cache, {});

    deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
    {
        auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(name_and_type, substream_path, data_part_info_for_read->getChecksums());
        bool was_prefetched = stream_name && prefetched_streams.contains(*stream_name);
        bool seek_to_mark = !was_prefetched && !continue_reading && !read_without_marks;

        return getStream(
            /* seek_to_start = */false, substream_path,
            data_part_info_for_read->getChecksums(),
            name_and_type, from_mark, seek_to_mark, current_task_last_mark, cache);
    };

    deserialize_settings.continuous_reading = continue_reading;
    auto & deserialize_state = deserialize_binary_bulk_state_map[name_and_type.name];

    serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_offset, max_rows_to_read, deserialize_settings, deserialize_state, &cache);

    IDataType::updateAvgValueSizeHint(*column, avg_value_size_hint);
}

}
