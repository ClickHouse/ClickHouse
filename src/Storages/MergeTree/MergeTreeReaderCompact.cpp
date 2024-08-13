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
        data_part_info_for_read_->getColumns().size()))
    , profile_callback(profile_callback_)
    , clock_type(clock_type_)
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

        if (column_to_read.isSubcolumn())
        {
            NameAndTypePair column_in_storage{column_to_read.getNameInStorage(), column_to_read.getTypeInStorage()};
            auto storage_column_from_part = getColumnInPart(column_in_storage);

            auto subcolumn_name = column_to_read.getSubcolumnName();
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

    /// If column is renamed get the new name from storage metadata.
    if (alter_conversions->columnHasNewName(name_in_storage))
        name_in_storage = alter_conversions->getColumnNewName(name_in_storage);

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
    auto name_level_for_offsets = findColumnForOffsets(column_to_read_with_subcolumns);

    if (!name_level_for_offsets)
        return;

    column_positions[pos] = data_part_info_for_read->getColumnPosition(name_level_for_offsets->first);

    if (is_offsets_subcolumn)
    {
        /// Read offsets from antoher array from the same Nested column.
        column = {name_level_for_offsets->first, column.getSubcolumnName(), column.getTypeInStorage(), column.type};
    }
    else
    {
        columns_for_offsets[pos] = std::move(name_level_for_offsets);
        partially_read_columns.insert(column.name);
    }
}

void MergeTreeReaderCompact::readData(
    const NameAndTypePair & name_and_type,
    ColumnPtr & column,
    size_t rows_to_read,
    const InputStreamGetter & getter,
    ISerialization::SubstreamsCache & cache)
{
    try
    {
        const auto [name, type] = name_and_type;
        size_t column_size_before_reading = column->size();

        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        deserialize_settings.getter = getter;
        deserialize_settings.avg_value_size_hint = avg_value_size_hints[name];

        auto it = cache.find(name);
        if (it != cache.end() && it->second != nullptr)
        {
            column = it->second;
            return;
        }

        if (name_and_type.isSubcolumn())
        {
            const auto & type_in_storage = name_and_type.getTypeInStorage();
            const auto & name_in_storage = name_and_type.getNameInStorage();

            auto serialization = getSerializationInPart({name_in_storage, type_in_storage});
            ColumnPtr temp_column = type_in_storage->createColumn(*serialization);

            serialization->deserializeBinaryBulkWithMultipleStreams(temp_column, rows_to_read, deserialize_settings, deserialize_binary_bulk_state_map[name], nullptr);
            auto subcolumn = type_in_storage->getSubcolumn(name_and_type.getSubcolumnName(), temp_column);

            /// TODO: Avoid extra copying.
            if (column->empty())
                column = subcolumn;
            else
                column->assumeMutable()->insertRangeFrom(*subcolumn, 0, subcolumn->size());
        }
        else
        {
            auto serialization = getSerializationInPart(name_and_type);
            serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_to_read, deserialize_settings, deserialize_binary_bulk_state_map[name], nullptr);
        }

        cache[name] = column;

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


void MergeTreeReaderCompact::readPrefix(
    const NameAndTypePair & name_and_type,
    const InputStreamGetter & buffer_getter,
    const InputStreamGetter & buffer_getter_for_prefix,
    const ColumnNameLevel & name_level_for_offsets)
{
    try
    {
        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;

        if (name_level_for_offsets.has_value())
        {
            const auto & part_columns = data_part_info_for_read->getColumnsDescription();
            auto column_for_offsets = part_columns.getPhysical(name_level_for_offsets->first);

            auto serialization_for_prefix = getSerializationInPart(column_for_offsets);
            deserialize_settings.getter = buffer_getter_for_prefix;
            ISerialization::DeserializeBinaryBulkStatePtr state_for_prefix;

            serialization_for_prefix->deserializeBinaryBulkStatePrefix(deserialize_settings, state_for_prefix, nullptr);
        }

        SerializationPtr serialization;
        if (name_and_type.isSubcolumn())
            serialization = getSerializationInPart({name_and_type.getNameInStorage(), name_and_type.getTypeInStorage()});
        else
            serialization = getSerializationInPart(name_and_type);

        deserialize_settings.getter = buffer_getter;
        deserialize_settings.dynamic_read_statistics = true;
        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, deserialize_binary_bulk_state_map[name_and_type.name], nullptr);
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
    return !is_offsets || columns_for_offsets[column_pos]->second < ISerialization::getArrayLevel(substream);
}

}
