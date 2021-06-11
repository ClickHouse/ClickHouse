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
        for (const NameAndTypePair & column : columns)
        {
            auto column_from_part = getColumnFromPart(column);
            addStreams(column_from_part.name, *column_from_part.type, profile_callback_, clock_type_);
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

        auto name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            auto [name, type] = getColumnFromPart(*name_and_type);

            /// The column is already present in the block so we will append the values to the end.
            bool append = res_columns[pos] != nullptr;
            if (!append)
                res_columns[pos] = type->createColumn();

            /// To keep offsets shared. TODO Very dangerous. Get rid of this.
            MutableColumnPtr column = res_columns[pos]->assumeMutable();

            bool read_offsets = true;

            /// For nested data structures collect pointers to offset columns.
            if (const auto * type_arr = typeid_cast<const DataTypeArray *>(type.get()))
            {
                String table_name = Nested::extractTableName(name);

                auto it_inserted = offset_columns.emplace(table_name, nullptr);

                /// offsets have already been read on the previous iteration and we don't need to read it again
                if (!it_inserted.second)
                    read_offsets = false;

                /// need to create new offsets
                if (it_inserted.second && !append)
                    it_inserted.first->second = ColumnArray::ColumnOffsets::create();

                /// share offsets in all elements of nested structure
                if (!append)
                    column = ColumnArray::create(type_arr->getNestedType()->createColumn(),
                                                 it_inserted.first->second)->assumeMutable();
            }

            try
            {
                size_t column_size_before_reading = column->size();

                readData(name, *type, *column, from_mark, continue_reading, max_rows_to_read, read_offsets);

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
            else
                res_columns[pos] = std::move(column);
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

void MergeTreeReaderWide::addStreams(const String & name, const IDataType & type,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type)
{
    IDataType::StreamCallback callback = [&] (const IDataType::SubstreamPath & substream_path)
    {
        String stream_name = IDataType::getFileNameForStream(name, substream_path);

        if (streams.count(stream_name))
            return;

        bool data_file_exists = data_part->checksums.files.count(stream_name + DATA_FILE_EXTENSION);

        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if (!data_file_exists)
            return;

        streams.emplace(stream_name, std::make_unique<MergeTreeReaderStream>(
            data_part->volume->getDisk(), data_part->getFullRelativePath() + stream_name, DATA_FILE_EXTENSION,
            data_part->getMarksCount(), all_mark_ranges, settings, mark_cache,
            uncompressed_cache, data_part->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
            &data_part->index_granularity_info,
            profile_callback, clock_type));
    };

    IDataType::SubstreamPath substream_path;
    type.enumerateStreams(callback, substream_path);
}


void MergeTreeReaderWide::readData(
    const String & name, const IDataType & type, IColumn & column,
    size_t from_mark, bool continue_reading, size_t max_rows_to_read,
    bool with_offsets)
{
    auto get_stream_getter = [&](bool stream_for_prefix) -> IDataType::InputStreamGetter
    {
        return [&, stream_for_prefix](const IDataType::SubstreamPath & substream_path) -> ReadBuffer *
        {
            /// If offsets for arrays have already been read.
            if (!with_offsets && substream_path.size() == 1 && substream_path[0].type == IDataType::Substream::ArraySizes)
                return nullptr;

            String stream_name = IDataType::getFileNameForStream(name, substream_path);

            auto it = streams.find(stream_name);
            if (it == streams.end())
                return nullptr;

            MergeTreeReaderStream & stream = *it->second;

            if (stream_for_prefix)
            {
                stream.seekToStart();
                continue_reading = false;
            }
            else if (!continue_reading)
                stream.seekToMark(from_mark);

            return stream.data_buffer;
        };
    };

    double & avg_value_size_hint = avg_value_size_hints[name];
    IDataType::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.avg_value_size_hint = avg_value_size_hint;

    if (deserialize_binary_bulk_state_map.count(name) == 0)
    {
        deserialize_settings.getter = get_stream_getter(true);
        type.deserializeBinaryBulkStatePrefix(deserialize_settings, deserialize_binary_bulk_state_map[name]);
    }

    deserialize_settings.getter = get_stream_getter(false);
    deserialize_settings.continuous_reading = continue_reading;
    auto & deserialize_state = deserialize_binary_bulk_state_map[name];
    type.deserializeBinaryBulkWithMultipleStreams(column, max_rows_to_read, deserialize_settings, deserialize_state);
    IDataType::updateAvgValueSizeHint(column, avg_value_size_hint);
}

}
