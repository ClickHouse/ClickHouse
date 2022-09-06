#include <Storages/MergeTree/MergeTreeReaderWide.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeNested.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace
{
    using OffsetColumns = std::map<std::string, ColumnPtr>;
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeReaderWide::MergeTreeReaderWide(
    DataPartWidePtr data_part_,
    NamesAndTypesList columns_,
    const StorageMetadataPtr & metadata_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_,
    IMergeTreeDataPart::ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : IMergeTreeReader(
        data_part_,
        columns_,
        metadata_snapshot_,
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
    catch (...)
    {
        storage.reportBrokenPart(data_part);
        throw;
    }
}

size_t MergeTreeReaderWide::readRows(
    size_t from_mark, size_t current_task_last_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    size_t read_rows = 0;
    try
    {
        size_t num_columns = res_columns.size();
        checkNumberOfColumns(num_columns);

        if (num_columns == 0)
            return max_rows_to_read;

        std::unordered_map<String, ISerialization::SubstreamsCache> caches;

        std::unordered_set<std::string> prefetched_streams;
        if (data_part->data_part_storage->isStoredOnRemoteDisk() ? settings.read_settings.remote_fs_prefetch : settings.read_settings.local_fs_prefetch)
        {
            /// Request reading of data in advance,
            /// so if reading can be asynchronous, it will also be performed in parallel for all columns.
            for (size_t pos = 0; pos < num_columns; ++pos)
            {
                try
                {
                    auto & cache = caches[columns_to_read[pos].getNameInStorage()];
                    prefetch(columns_to_read[pos], serializations[pos], from_mark, continue_reading, current_task_last_mark, cache, prefetched_streams);
                }
                catch (Exception & e)
                {
                    /// Better diagnostics.
                    e.addMessage("(while reading column " + columns_to_read[pos].name + ")");
                    throw;
                }
            }
        }

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

        /// NOTE: positions for all streams must be kept in sync.
        /// In particular, even if for some streams there are no rows to be read,
        /// you must ensure that no seeks are skipped and at this point they all point to to_mark.
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            storage.reportBrokenPart(data_part);

        /// Better diagnostics.
        e.addMessage("(while reading from part " + data_part->data_part_storage->getFullPath() + " "
                     "from mark " + toString(from_mark) + " "
                     "with max_rows_to_read = " + toString(max_rows_to_read) + ")");
        throw;
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part);

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
    ISerialization::StreamCallback callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (streams.contains(stream_name))
            return;

        bool data_file_exists = data_part->checksums.files.contains(stream_name + DATA_FILE_EXTENSION);

        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if (!data_file_exists)
            return;

        bool is_lc_dict = substream_path.size() > 1 && substream_path[substream_path.size() - 2].type == ISerialization::Substream::Type::DictionaryKeys;

        streams.emplace(stream_name, std::make_unique<MergeTreeReaderStream>(
            data_part->data_part_storage, stream_name, DATA_FILE_EXTENSION,
            data_part->getMarksCount(), all_mark_ranges, settings, mark_cache,
            uncompressed_cache, data_part->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
            &data_part->index_granularity_info,
            profile_callback, clock_type, is_lc_dict));
    };

    serialization->enumerateStreams(callback);
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

void MergeTreeReaderWide::prefetch(
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization,
    size_t from_mark,
    bool continue_reading,
    size_t current_task_last_mark,
    ISerialization::SubstreamsCache & cache,
    std::unordered_set<std::string> & prefetched_streams)
{
    deserializePrefix(serialization, name_and_type, current_task_last_mark, cache);

    serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (!prefetched_streams.contains(stream_name))
        {
            bool seek_to_mark = !continue_reading;
            if (ReadBuffer * buf = getStream(false, substream_path, streams, name_and_type, from_mark, seek_to_mark, current_task_last_mark, cache))
                buf->prefetch();

            prefetched_streams.insert(stream_name);
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
