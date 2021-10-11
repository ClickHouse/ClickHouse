#include <Storages/MergeTree/MergeTreeReaderWide.h>

#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
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
        std::move(data_part_),
        std::move(columns_),
        metadata_snapshot_,
        uncompressed_cache_,
        std::move(mark_cache_),
        std::move(mark_ranges_),
        std::move(settings_),
        std::move(avg_value_size_hints_))
{
    try
    {
        disk = data_part->volume->getDisk();
        for (const NameAndTypePair & column : columns)
        {
            auto column_from_part = getColumnFromPart(column);
            addStreams(column_from_part, profile_callback_, clock_type_);
        }
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);
        throw;
    }
}


size_t MergeTreeReaderWide::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    size_t read_rows = 0;
    try
    {
        size_t num_columns = columns.size();
        checkNumberOfColumns(num_columns);

        /// Pointers to offset columns that are common to the nested data structure columns.
        /// If append is true, then the value will be equal to nullptr and will be used only to
        /// check that the offsets column has been already read.
        OffsetColumns offset_columns;
        std::unordered_map<String, ISerialization::SubstreamsCache> caches;

        std::unordered_set<std::string> prefetched_streams;
        if (disk->isRemote() ? settings.read_settings.remote_fs_prefetch : settings.read_settings.local_fs_prefetch)
        {
            /// Request reading of data in advance,
            /// so if reading can be asynchronous, it will also be performed in parallel for all columns.
            auto name_and_type = columns.begin();
            for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
            {
                auto column_from_part = getColumnFromPart(*name_and_type);
                try
                {
                    auto & cache = caches[column_from_part.getNameInStorage()];
                    prefetch(column_from_part, from_mark, continue_reading, cache, prefetched_streams);
                }
                catch (Exception & e)
                {
                    /// Better diagnostics.
                    e.addMessage("(while reading column " + column_from_part.name + ")");
                    throw;
                }
            }
        }

        auto name_and_type = columns.begin();

        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            auto column_from_part = getColumnFromPart(*name_and_type);
            const auto & [name, type] = column_from_part;

            /// The column is already present in the block so we will append the values to the end.
            bool append = res_columns[pos] != nullptr;
            if (!append)
                res_columns[pos] = type->createColumn();

            auto & column = res_columns[pos];
            try
            {
                size_t column_size_before_reading = column->size();
                auto & cache = caches[column_from_part.getNameInStorage()];

                readData(
                    column_from_part, column, from_mark, continue_reading,
                    max_rows_to_read, cache, /* was_prefetched =*/ !prefetched_streams.empty());

                /// For elements of Nested, column_size_before_reading may be greater than column size
                ///  if offsets are not empty and were already read, but elements are empty.
                if (!column->empty())
                    read_rows = std::max(read_rows, column->size() - column_size_before_reading);
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage("(while reading column " + name + ")");
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
            storage.reportBrokenPart(data_part->name);

        /// Better diagnostics.
        e.addMessage("(while reading from part " + data_part->getFullPath() + " "
                     "from mark " + toString(from_mark) + " "
                     "with max_rows_to_read = " + toString(max_rows_to_read) + ")");
        throw;
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);

        throw;
    }

    return read_rows;
}

void MergeTreeReaderWide::addStreams(const NameAndTypePair & name_and_type,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type)
{
    ISerialization::StreamCallback callback = [&] (const ISerialization::SubstreamPath & substream_path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (streams.count(stream_name))
            return;

        bool data_file_exists = data_part->checksums.files.count(stream_name + DATA_FILE_EXTENSION);

        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if (!data_file_exists)
            return;

        streams.emplace(stream_name, std::make_unique<MergeTreeReaderStream>(
            disk, data_part->getFullRelativePath() + stream_name, DATA_FILE_EXTENSION,
            data_part->getMarksCount(), all_mark_ranges, settings, mark_cache,
            uncompressed_cache, data_part->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
            &data_part->index_granularity_info,
            profile_callback, clock_type));
    };

    auto serialization = data_part->getSerializationForColumn(name_and_type);
    serialization->enumerateStreams(callback);
    serializations.emplace(name_and_type.name, std::move(serialization));
}


static ReadBuffer * getStream(
    bool seek_to_start,
    const ISerialization::SubstreamPath & substream_path,
    MergeTreeReaderWide::FileStreams & streams,
    const NameAndTypePair & name_and_type,
    size_t from_mark, bool seek_to_mark,
    ISerialization::SubstreamsCache & cache)
{
    /// If substream have already been read.
    if (cache.count(ISerialization::getSubcolumnNameForStream(substream_path)))
        return nullptr;

    String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

    auto it = streams.find(stream_name);
    if (it == streams.end())
        return nullptr;

    MergeTreeReaderStream & stream = *it->second;

    if (seek_to_start)
        stream.seekToStart();
    else if (seek_to_mark)
        stream.seekToMark(from_mark);

    return stream.data_buffer;
}


void MergeTreeReaderWide::prefetch(
    const NameAndTypePair & name_and_type,
    size_t from_mark,
    bool continue_reading,
    ISerialization::SubstreamsCache & cache,
    std::unordered_set<std::string> & prefetched_streams)
{
    const auto & name = name_and_type.name;
    auto & serialization = serializations[name];

    serialization->enumerateStreams([&](const ISerialization::SubstreamPath & substream_path)
    {
        String stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);

        if (!prefetched_streams.count(stream_name))
        {
            bool seek_to_mark = !continue_reading;
            if (ReadBuffer * buf = getStream(false, substream_path, streams, name_and_type, from_mark, seek_to_mark, cache))
                buf->prefetch();

            prefetched_streams.insert(stream_name);
        }
    });
}


void MergeTreeReaderWide::readData(
    const NameAndTypePair & name_and_type, ColumnPtr & column,
    size_t from_mark, bool continue_reading, size_t max_rows_to_read,
    ISerialization::SubstreamsCache & cache, bool was_prefetched)
{
    double & avg_value_size_hint = avg_value_size_hints[name_and_type.name];
    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.avg_value_size_hint = avg_value_size_hint;

    const auto & name = name_and_type.name;
    auto & serialization = serializations[name];

    if (deserialize_binary_bulk_state_map.count(name) == 0)
    {
        deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
        {
            return getStream(/* seek_to_start = */true, substream_path, streams, name_and_type, from_mark, /* seek_to_mark = */false, cache);
        };
        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, deserialize_binary_bulk_state_map[name]);
    }

    deserialize_settings.getter = [&](const ISerialization::SubstreamPath & substream_path)
    {
        bool seek_to_mark = !was_prefetched && !continue_reading;

        return getStream(
            /* seek_to_start = */false, substream_path, streams, name_and_type, from_mark,
            seek_to_mark, cache);
    };
    deserialize_settings.continuous_reading = continue_reading;
    auto & deserialize_state = deserialize_binary_bulk_state_map[name];

    serialization->deserializeBinaryBulkWithMultipleStreams(column, max_rows_to_read, deserialize_settings, deserialize_state, &cache);
    IDataType::updateAvgValueSizeHint(*column, avg_value_size_hint);
}

}
