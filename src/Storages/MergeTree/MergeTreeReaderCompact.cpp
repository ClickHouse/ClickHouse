#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}

MergeTreeReaderCompact::MergeTreeReaderCompact(
    MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
    NamesAndTypesList columns_,
    const VirtualFields & virtual_fields_,
    const StorageSnapshotPtr & storage_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    DeserializationPrefixesCache *,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_,
    ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : IMergeTreeReader(
        data_part_info_for_read_,
        columns_,
        virtual_fields_,
        storage_snapshot_,
        uncompressed_cache_,
        mark_cache_,
        mark_ranges_,
        settings_,
        avg_value_size_hints_)
    , columns_substreams(data_part_info_for_read_->getColumnsSubstreams())
    , marks_loader(std::make_shared<MergeTreeMarksLoader>(
        data_part_info_for_read_,
        mark_cache,
        data_part_info_for_read_->getIndexGranularityInfo().getMarksFilePath(MergeTreeDataPartCompact::DATA_FILE_NAME),
        data_part_info_for_read_->getMarksCount(),
        data_part_info_for_read_->getIndexGranularityInfo(),
        settings.save_marks_in_cache,
        settings.read_settings,
        settings_.read_settings.load_marks_asynchronously
            ? &data_part_info_for_read_->getContext()->getLoadMarksThreadpool() : nullptr,
        data_part_info_for_read_->getIndexGranularityInfo().mark_type.with_substreams
            ? columns_substreams.getTotalSubstreams() : data_part_info_for_read_->getColumns().size()))
    , profile_callback(profile_callback_)
    , clock_type(clock_type_)
    , have_substream_marks(data_part_info_for_read_->getIndexGranularityInfo().mark_type.with_substreams)
{
    marks_loader->startAsyncLoad();
}

void MergeTreeReaderCompact::fillColumnPositions()
{
    size_t columns_num = columns_to_read.size();

    column_positions.resize(columns_num);
    columns_for_offsets.resize(columns_num);

    for (size_t i = 0; i < columns_num; ++i)
    {
        auto & column_to_read = columns_to_read[i];
        auto position = data_part_info_for_read->getColumnPosition(column_to_read.getNameInStorage());

        if (position.has_value() && column_to_read.isSubcolumn())
        {
            auto name_in_storage = column_to_read.getNameInStorage();
            auto subcolumn_name = column_to_read.getSubcolumnName();
            auto storage_column_from_part = part_columns.getColumn(GetColumnsOptions::All, name_in_storage);

            if (!storage_column_from_part.type->hasSubcolumn(subcolumn_name))
                position.reset();
        }

        column_positions[i] = std::move(position);

        /// If array of Nested column is missing in part,
        /// we have to read its offsets if they exist.
        if (!column_positions[i])
            findPositionForMissedNested(i);
    }
}

NameAndTypePair MergeTreeReaderCompact::getColumnConvertedToSubcolumnOfNested(const NameAndTypePair & column)
{
    if (!isArray(column.type))
        return column;

    /// If it is a part of Nested, we need to get the column from
    /// storage metadata which is converted to Nested type with subcolumns.
    /// It is required for proper counting of shared streams.
    auto [name_in_storage, subcolumn_name] = Nested::splitName(column.name);

    if (subcolumn_name.empty())
        return column;

    if (!storage_columns_with_collected_nested)
    {
        auto options = GetColumnsOptions(GetColumnsOptions::All).withExtendedObjects();
        auto storage_columns_list = Nested::collect(storage_snapshot->getColumns(options));
        storage_columns_with_collected_nested = ColumnsDescription(std::move(storage_columns_list));
    }

    return storage_columns_with_collected_nested->getColumnOrSubcolumn(
        GetColumnsOptions::All,
        Nested::concatenateName(name_in_storage, subcolumn_name));
}

void MergeTreeReaderCompact::findPositionForMissedNested(size_t pos)
{
    auto & column = columns_to_read[pos];

    bool is_array = isArray(column.type);
    bool is_offsets_subcolumn = isArray(column.getTypeInStorage()) && column.getSubcolumnName() == "size0";

    if (!is_array && !is_offsets_subcolumn)
        return;

    NameAndTypePair column_in_storage{column.getNameInStorage(), column.getTypeInStorage()};

    auto column_to_read_with_subcolumns = getColumnConvertedToSubcolumnOfNested(column_in_storage);
    auto column_for_offsets = findColumnForOffsets(column_to_read_with_subcolumns);

    if (!column_for_offsets)
        return;

    if (is_offsets_subcolumn)
    {
        /// Read offsets from antoher array from the same Nested column.
        column = {column_for_offsets->column.name, column.getSubcolumnName(), column.getTypeInStorage(), column.type};
    }
    else
    {
        columns_for_offsets[pos] = column_for_offsets;
        partially_read_columns.insert(column.name);
    }

    column_positions[pos] = data_part_info_for_read->getColumnPosition(column_for_offsets->column.name);
    serializations_of_full_columns[column.getNameInStorage()] = column_for_offsets->serialization;
}

static ColumnPtr getFullColumnFromCache(std::unordered_map<String, ColumnPtr> * columns_cache_for_subcolumns, const String & column_name)
{
    if (!columns_cache_for_subcolumns)
        return nullptr;

    auto it = columns_cache_for_subcolumns->find(column_name);
    if (it == columns_cache_for_subcolumns->end())
        return nullptr;

    return it->second;
}

void MergeTreeReaderCompact::readData(
    size_t column_idx,
    ColumnPtr & column,
    size_t rows_to_read,
    size_t rows_offset,
    size_t from_mark,
    MergeTreeReaderStream & stream,
    ISerialization::SubstreamsCache & columns_cache,
    std::unordered_map<String, ColumnPtr> * columns_cache_for_subcolumns)
{
    const auto & name_and_type = columns_to_read[column_idx];
    const auto [name, type] = name_and_type;

    bool seek_to_substream_mark = name_and_type.isSubcolumn() && have_substream_marks;
    auto buffer_getter = [&](const ISerialization::SubstreamPath & substream_path) -> ReadBuffer *
    {
        if (needSkipStream(column_idx, substream_path))
            return nullptr;

        if (seek_to_substream_mark)
        {
            auto stream_name = ISerialization::getFileNameForStream(name_and_type, substream_path);
            size_t substream_position = columns_substreams.getSubstreamPosition(*column_positions[column_idx], stream_name);
            stream.seekToMarkAndColumn(from_mark, substream_position);
        }

        return stream.getDataBuffer();
    };

    try
    {
        size_t column_size_before_reading = column->size();

        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        deserialize_settings.getter = buffer_getter;
        deserialize_settings.avg_value_size_hint = avg_value_size_hints[name];
        deserialize_settings.use_specialized_prefixes_substreams = true;

        auto it = columns_cache.find(name);
        if (it != columns_cache.end() && it->second != nullptr)
        {
            column = it->second;
            return;
        }

        if (name_and_type.isSubcolumn() && !have_substream_marks)
        {
            const auto & type_in_storage = name_and_type.getTypeInStorage();
            const auto & name_in_storage = name_and_type.getNameInStorage();
            const auto & serialization = serializations_of_full_columns.at(name_in_storage);

            ColumnPtr temp_full_column = getFullColumnFromCache(columns_cache_for_subcolumns, name_in_storage);

            if (!temp_full_column)
            {
                temp_full_column = type_in_storage->createColumn(*serialization);
                serialization->deserializeBinaryBulkWithMultipleStreams(temp_full_column, rows_offset, rows_to_read, deserialize_settings, deserialize_binary_bulk_state_map_for_subcolumns[name_in_storage], nullptr);

                if (columns_cache_for_subcolumns)
                    columns_cache_for_subcolumns->emplace(name_in_storage, temp_full_column);
            }

            auto subcolumn = type_in_storage->getSubcolumn(name_and_type.getSubcolumnName(), temp_full_column);

            /// TODO: Avoid extra copying.
            if (column->empty())
                column = IColumn::mutate(subcolumn);
            else
                column->assumeMutable()->insertRangeFrom(*subcolumn, 0, subcolumn->size());
        }
        else
        {
            const auto & serialization = serializations[column_idx];
            serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_offset, rows_to_read, deserialize_settings, deserialize_binary_bulk_state_map[name], nullptr);
        }

        columns_cache[name] = column;

        size_t read_rows_in_column = column->size() - column_size_before_reading;
        if (read_rows_in_column != rows_to_read)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all data in MergeTreeReaderCompact. Rows read: {}. Rows expected: {}.",
                read_rows_in_column, rows_to_read);
    }
    catch (Exception & e)
    {
        e.addMessage("(while reading column " + name_and_type.name + ")");
        throw;
    }
}

void MergeTreeReaderCompact::readPrefix(size_t column_idx, size_t from_mark, MergeTreeReaderStream & stream, ISerialization::SubstreamsDeserializeStatesCache * cache)
{
    if (columns_for_offsets[column_idx])
    {
        /// If we read only offsets we have to read prefix anyway
        /// to preserve correctness of serialization.
        auto buffer_getter = [&](const auto &) -> ReadBuffer *
        {
            return stream.getDataBuffer();
        };

        ISerialization::DeserializeBinaryBulkStatePtr state;
        readPrefix(columns_for_offsets[column_idx]->column, columns_for_offsets[column_idx]->serialization, state, buffer_getter, cache);
    }

    const auto & column = columns_to_read[column_idx];
    auto name_in_storage = column.getNameInStorage();

    bool seek_to_substream_mark = column.isSubcolumn() && have_substream_marks;
    auto buffer_getter = [&](const ISerialization::SubstreamPath & substream_path) -> ReadBuffer *
    {
        if (needSkipStream(column_idx, substream_path))
            return nullptr;

        if (seek_to_substream_mark)
        {
            auto stream_name = ISerialization::getFileNameForStream(column, substream_path);
            size_t substream_position = columns_substreams.getSubstreamPosition(*column_positions[column_idx], stream_name);
            stream.seekToMarkAndColumn(from_mark, substream_position);
        }

        return stream.getDataBuffer();
    };

    if (column.isSubcolumn() && !have_substream_marks)
    {
        if (deserialize_binary_bulk_state_map_for_subcolumns.contains(name_in_storage))
            return;

        const auto & serialization = serializations_of_full_columns.at(name_in_storage);
        auto & state = deserialize_binary_bulk_state_map_for_subcolumns[name_in_storage];
        readPrefix(column, serialization, state, buffer_getter, nullptr);
    }
    else
    {
        const auto & serialization = serializations[column_idx];
        auto & state = deserialize_binary_bulk_state_map[column.name];
        readPrefix(column, serialization, state, buffer_getter, seek_to_substream_mark ? cache : nullptr);
    }
}

void MergeTreeReaderCompact::readPrefix(
    const NameAndTypePair & name_and_type,
    const SerializationPtr & serialization,
    ISerialization::DeserializeBinaryBulkStatePtr & state,
    const InputStreamGetter & buffer_getter,
    ISerialization::SubstreamsDeserializeStatesCache * cache)
{
    try
    {
        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        deserialize_settings.getter = buffer_getter;
        deserialize_settings.object_and_dynamic_read_statistics = true;
        deserialize_settings.use_specialized_prefixes_substreams = true;

        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, state, cache);
    }
    catch (Exception & e)
    {
        e.addMessage("(while reading column " + name_and_type.name + ")");
        throw;
    }
}

void MergeTreeReaderCompact::createColumnsForReading(Columns & res_columns) const
{
    for (size_t i = 0; i < columns_to_read.size(); ++i)
    {
        if (column_positions[i] && res_columns[i] == nullptr)
            res_columns[i] = columns_to_read[i].type->createColumn(*serializations[i]);
    }
}

bool MergeTreeReaderCompact::needSkipStream(size_t column_pos, const ISerialization::SubstreamPath & substream) const
{
    /// Offset stream can be read only from columns of current level or
    /// below (since it is OK to read all parent streams from the
    /// alternative).
    ///
    /// Consider the following columns in nested "root":
    /// - root.array Array(UInt8) - exists
    /// - root.nested_array Array(Array(UInt8)) - does not exist (only_offsets_level=1)
    ///
    /// For root.nested_array it will try to read multiple streams:
    /// - offsets (substream_path = {ArraySizes})
    ///   OK
    /// - root.nested_array elements (substream_path = {ArrayElements, ArraySizes})
    ///   NOT OK - cannot use root.array offsets stream for this
    ///
    /// Here only_offsets_level is the level of the alternative stream,
    /// and substream_path.size() is the level of the current stream.

    if (!columns_for_offsets[column_pos])
        return false;

    bool is_offsets = !substream.empty() && substream.back().type == ISerialization::Substream::ArraySizes;
    return !is_offsets || columns_for_offsets[column_pos]->level < ISerialization::getArrayLevel(substream);
}

}
