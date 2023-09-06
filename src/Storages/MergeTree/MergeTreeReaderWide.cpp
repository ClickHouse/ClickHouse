#include <Storages/MergeTree/MergeTreeReaderWide.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNested.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace
{
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}

MergeTreeReaderWide::MergeTreeReaderWide(
    MergeTreeDataPartInfoForReaderPtr data_part_info_,
    NamesAndTypesList columns_,
    const StorageSnapshotPtr & storage_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_,
    IMergeTreeDataPart::ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : IMergeTreeReader(
        data_part_info_,
        columns_,
        storage_snapshot_,
        uncompressed_cache_,
        mark_cache_,
        mark_ranges_,
        settings_,
        avg_value_size_hints_)
{
    try
    {
        for (size_t i = 0; i < columns_to_read.size(); ++i)
            addStreams(columns_to_read[i], serializations[i], profile_callback_, clock_type_);
    }
    catch (const Exception & e)
    {
        if (!isRetryableException(e))
            data_part_info_for_read->reportBroken();
        throw;
    }
    catch (...)
    {
        data_part_info_for_read->reportBroken();
        throw;
    }
}

void MergeTreeReaderWide::prefetchBeginOfRange(Priority priority)
{
    prefetched_streams.clear();

    try
    {
        prefetchForAllColumns(priority, columns_to_read.size(), all_mark_ranges.front().begin, all_mark_ranges.back().end, false);
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
    catch (const Exception & e)
    {
        if (!isRetryableException(e))
            data_part_info_for_read->reportBroken();
        throw;
    }
    catch (...)
    {
        data_part_info_for_read->reportBroken();
        throw;
    }
}

void MergeTreeReaderWide::prefetchForAllColumns(
    Priority priority, size_t num_columns, size_t from_mark, size_t current_task_last_mark, bool continue_reading)
{
    bool do_prefetch = data_part_info_for_read->getDataPartStorage()->isStoredOnRemoteDisk()
        ? settings.read_settings.remote_fs_prefetch
        : settings.read_settings.local_fs_prefetch;

    if (!do_prefetch)
        return;

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
    size_t from_mark, size_t current_task_last_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
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

        prefetchForAllColumns(Priority{}, num_columns, from_mark, current_task_last_mark, continue_reading);

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

                readData(
                    column_to_read, serializations[pos], column,
                    from_mark, continue_reading, current_task_last_mark,
                    max_rows_to_read, cache, /* was_prefetched =*/ !prefetched_streams.empty());

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

            if (column->empty())
                res_columns[pos] = nullptr;
        }

        prefetched_streams.clear();
        caches.clear();

        /// NOTE: positions for all streams must be kept in sync.
        /// In particular, even if for some streams there are no rows to be read,
        /// you must ensure that no seeks are skipped and at this point they all point to to_mark.
    }
    catch (Exception & e)
    {
        if (!isRetryableException(e))
            data_part_info_for_read->reportBroken();

        /// Better diagnostics.
        e.addMessage(getMessageForDiagnosticOfBrokenPart(from_mark, max_rows_to_read));
        throw;
    }
    catch (...)
    {
        data_part_info_for_read->reportBroken();
        throw;
    }

    return read_rows;
}

void MergeTreeReaderWide::addStreams(
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback,
    clockid_t clock_type)
{
    bool has_any_stream = false;
    bool has_all_streams = true;

    ISerialization::StreamCallback callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (streams.contains(stream_name))
        {
            has_any_stream = true;
            return;
        }

        bool data_file_exists = data_part_info_for_read->getChecksums().files.contains(stream_name + DATA_FILE_EXTENSION);

        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if (!data_file_exists)
        {
            has_all_streams = false;
            return;
        }

        has_any_stream = true;
        bool is_lc_dict = substream_path.size() > 1 && substream_path[substream_path.size() - 2].type == ISerialization::Substream::Type::DictionaryKeys;

        auto context = data_part_info_for_read->getContext();
        auto * load_marks_threadpool = settings.read_settings.load_marks_asynchronously ? &context->getLoadMarksThreadpool() : nullptr;

        streams.emplace(stream_name, std::make_unique<MergeTreeReaderStream>(
            data_part_info_for_read, stream_name, DATA_FILE_EXTENSION,
            data_part_info_for_read->getMarksCount(), all_mark_ranges, settings, mark_cache,
            uncompressed_cache, data_part_info_for_read->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
            &data_part_info_for_read->getIndexGranularityInfo(),
            profile_callback, clock_type, is_lc_dict, load_marks_threadpool));
    };

    serialization->enumerateStreams(callback);

    if (has_any_stream && !has_all_streams)
        partially_read_columns.insert(name_and_type.name);
}


static ReadBuffer * getStream(
    bool seek_to_start,
    const ISerialization::SubstreamPath & substream_path,
    MergeTreeReaderWide::FileStreams & streams,
    const NameAndTypePair & name_and_type,
    size_t from_mark, bool seek_to_mark,
    size_t current_task_last_mark,
    ISerialization::SubstreamsCache & cache)
{
    /// If substream have already been read.
    if (cache.contains(ISerialization::getSubcolumnNameForStream(substream_path)))
        return nullptr;

    String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

    auto it = streams.find(stream_name);
    if (it == streams.end())
        return nullptr;

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
    size_t current_task_last_mark,
    ISerialization::SubstreamsCache & cache)
{
    const auto & name = name_and_type.name;
    if (!deserialize_binary_bulk_state_map.contains(name))
    {
        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
        {
            return getStream(/* seek_to_start = */true, substream_path, streams, name_and_type, 0, /* seek_to_mark = */false, current_task_last_mark, cache);
        };
        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, deserialize_binary_bulk_state_map[name]);
    }
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
    deserializePrefix(serialization, name_and_type, current_task_last_mark, cache);

    serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (!prefetched_streams.contains(stream_name))
        {
            bool seek_to_mark = !continue_reading;
            if (ReadBuffer * buf = getStream(false, substream_path, streams, name_and_type, from_mark, seek_to_mark, current_task_last_mark, cache))
            {
                buf->prefetch(priority);
                prefetched_streams.insert(stream_name);
            }
        }
    });
}


void MergeTreeReaderWide::readData(
    const NameAndTypePair & name_and_type, const SerializationPtr & serialization, ColumnPtr & column,
    size_t from_mark, bool continue_reading, size_t current_task_last_mark,
    size_t max_rows_to_read, ISerialization::SubstreamsCache & cache, bool was_prefetched)
{
    double & avg_value_size_hint = avg_value_size_hints[name_and_type.name];
    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.avg_value_size_hint = avg_value_size_hint;

    deserializePrefix(serialization, name_and_type, current_task_last_mark, cache);

    deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
    {
        bool seek_to_mark = !was_prefetched && !continue_reading;

        return getStream(
            /* seek_to_start = */false, substream_path, streams, name_and_type, from_mark,
            seek_to_mark, current_task_last_mark, cache);
    };

    deserialize_settings.continuous_reading = continue_reading;
    auto & deserialize_state = deserialize_binary_bulk_state_map[name_and_type.name];

    serialization->deserializeBinaryBulkWithMultipleStreams(column, max_rows_to_read, deserialize_settings, deserialize_state, &cache);
    IDataType::updateAvgValueSizeHint(*column, avg_value_size_hint);
}

}
