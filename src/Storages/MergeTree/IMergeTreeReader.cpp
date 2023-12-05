#include <Storages/MergeTree/IMergeTreeReader.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/escapeForFileName.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace
{
    using OffsetColumns = std::map<std::string, ColumnPtr>;
}
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


IMergeTreeReader::IMergeTreeReader(
    MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
    const NamesAndTypesList & columns_,
    const StorageSnapshotPtr & storage_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    const MarkRanges & all_mark_ranges_,
    const MergeTreeReaderSettings & settings_,
    const ValueSizeMap & avg_value_size_hints_)
    : data_part_info_for_read(data_part_info_for_read_)
    , avg_value_size_hints(avg_value_size_hints_)
    , uncompressed_cache(uncompressed_cache_)
    , mark_cache(mark_cache_)
    , settings(settings_)
    , storage_snapshot(storage_snapshot_)
    , all_mark_ranges(all_mark_ranges_)
    , alter_conversions(data_part_info_for_read->getAlterConversions())
    /// For wide parts convert plain arrays of Nested to subcolumns
    /// to allow to use shared offset column from cache.
    , requested_columns(data_part_info_for_read->isWidePart()
        ? Nested::convertToSubcolumns(columns_)
        : columns_)
    , part_columns(data_part_info_for_read->isWidePart()
        ? data_part_info_for_read->getColumnsDescriptionWithCollectedNested()
        : data_part_info_for_read->getColumnsDescription())
{
    columns_to_read.reserve(requested_columns.size());
    serializations.reserve(requested_columns.size());

    for (const auto & column : requested_columns)
    {
        columns_to_read.emplace_back(getColumnInPart(column));
        serializations.emplace_back(getSerializationInPart(column));
    }
}

const IMergeTreeReader::ValueSizeMap & IMergeTreeReader::getAvgValueSizeHints() const
{
    return avg_value_size_hints;
}

void IMergeTreeReader::fillMissingColumns(Columns & res_columns, bool & should_evaluate_missing_defaults, size_t num_rows) const
{
    try
    {
        NamesAndTypesList available_columns(columns_to_read.begin(), columns_to_read.end());
        DB::fillMissingColumns(
            res_columns, num_rows,
            Nested::convertToSubcolumns(requested_columns),
            Nested::convertToSubcolumns(available_columns),
            partially_read_columns, storage_snapshot->metadata);

        should_evaluate_missing_defaults = std::any_of(
            res_columns.begin(), res_columns.end(), [](const auto & column) { return column == nullptr; });
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        const auto & part_storage = data_part_info_for_read->getDataPartStorage();
        e.addMessage(
            "(while reading from part " + part_storage->getFullPath()
            + " located on disk " + part_storage->getDiskName()
            + " of type " + part_storage->getDiskType() + ")");
        throw;
    }
}

void IMergeTreeReader::evaluateMissingDefaults(Block additional_columns, Columns & res_columns) const
{
    try
    {
        size_t num_columns = requested_columns.size();

        if (res_columns.size() != num_columns)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "invalid number of columns passed to MergeTreeReader::fillMissingColumns. "
                            "Expected {}, got {}", num_columns, res_columns.size());

        /// Convert columns list to block.
        /// TODO: rewrite with columns interface. It will be possible after changes in ExpressionActions.
        auto name_and_type = requested_columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (res_columns[pos] == nullptr)
                continue;

            additional_columns.insert({res_columns[pos], name_and_type->type, name_and_type->name});
        }

        auto dag = DB::evaluateMissingDefaults(
            additional_columns, requested_columns,
            storage_snapshot->metadata->getColumns(),
            data_part_info_for_read->getContext());

        if (dag)
        {
            dag->addMaterializingOutputActions();
            auto actions = std::make_shared<
                ExpressionActions>(std::move(dag),
                ExpressionActionsSettings::fromSettings(data_part_info_for_read->getContext()->getSettingsRef()));
            actions->execute(additional_columns);
        }

        /// Move columns from block.
        name_and_type = requested_columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
            res_columns[pos] = std::move(additional_columns.getByName(name_and_type->name).column);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        const auto & part_storage = data_part_info_for_read->getDataPartStorage();
        e.addMessage(
            "(while reading from part " + part_storage->getFullPath()
            + " located on disk " + part_storage->getDiskName()
            + " of type " + part_storage->getDiskType() + ")");
        throw;
    }
}

String IMergeTreeReader::getColumnNameInPart(const NameAndTypePair & required_column) const
{
    auto name_in_storage = required_column.getNameInStorage();
    if (alter_conversions->isColumnRenamed(name_in_storage))
    {
        name_in_storage = alter_conversions->getColumnOldName(name_in_storage);
        return Nested::concatenateName(name_in_storage, required_column.getSubcolumnName());
    }

    return required_column.name;
}

NameAndTypePair IMergeTreeReader::getColumnInPart(const NameAndTypePair & required_column) const
{
    auto name_in_part = getColumnNameInPart(required_column);
    auto column_in_part = part_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name_in_part);
    if (column_in_part)
        return *column_in_part;

    return required_column;
}

SerializationPtr IMergeTreeReader::getSerializationInPart(const NameAndTypePair & required_column) const
{
    auto name_in_part = getColumnNameInPart(required_column);
    auto column_in_part = part_columns.tryGetColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name_in_part);
    if (!column_in_part)
        return IDataType::getSerialization(required_column);

    const auto & infos = data_part_info_for_read->getSerializationInfos();
    if (auto it = infos.find(column_in_part->getNameInStorage()); it != infos.end())
        return IDataType::getSerialization(*column_in_part, *it->second);

    return IDataType::getSerialization(*column_in_part);
}

void IMergeTreeReader::performRequiredConversions(Columns & res_columns) const
{
    try
    {
        size_t num_columns = requested_columns.size();

        if (res_columns.size() != num_columns)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Invalid number of columns passed to MergeTreeReader::performRequiredConversions. "
                            "Expected {}, got {}", num_columns, res_columns.size());
        }

        Block copy_block;
        auto name_and_type = requested_columns.begin();

        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (res_columns[pos] == nullptr)
                continue;

            copy_block.insert({res_columns[pos], getColumnInPart(*name_and_type).type, name_and_type->name});
        }

        DB::performRequiredConversions(copy_block, requested_columns, data_part_info_for_read->getContext());

        /// Move columns from block.
        name_and_type = requested_columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
            res_columns[pos] = std::move(copy_block.getByName(name_and_type->name).column);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        const auto & part_storage = data_part_info_for_read->getDataPartStorage();
        e.addMessage(
            "(while reading from part " + part_storage->getFullPath()
            + " located on disk " + part_storage->getDiskName()
            + " of type " + part_storage->getDiskType() + ")");
        throw;
    }
}

IMergeTreeReader::ColumnNameLevel IMergeTreeReader::findColumnForOffsets(const NameAndTypePair & required_column) const
{
    auto get_offsets_streams = [](const auto & serialization, const auto & name_in_storage)
    {
        std::vector<std::pair<String, size_t>> offsets_streams;
        serialization->enumerateStreams([&](const auto & subpath)
        {
            if (subpath.empty() || subpath.back().type != ISerialization::Substream::ArraySizes)
                return;

            auto subname = ISerialization::getSubcolumnNameForStream(subpath);
            auto full_name = Nested::concatenateName(name_in_storage, subname);
            offsets_streams.emplace_back(full_name, ISerialization::getArrayLevel(subpath));
        });

        return offsets_streams;
    };

    auto required_name_in_storage = Nested::extractTableName(required_column.getNameInStorage());
    auto required_offsets_streams = get_offsets_streams(getSerializationInPart(required_column), required_name_in_storage);

    size_t max_matched_streams = 0;
    ColumnNameLevel name_level;

    /// Find column that has maximal number of matching
    /// offsets columns with required_column.
    for (const auto & part_column : Nested::convertToSubcolumns(data_part_info_for_read->getColumns()))
    {
        auto name_in_storage = Nested::extractTableName(part_column.name);
        if (name_in_storage != required_name_in_storage)
            continue;

        auto offsets_streams = get_offsets_streams(data_part_info_for_read->getSerialization(part_column), name_in_storage);
        NameToIndexMap offsets_streams_map(offsets_streams.begin(), offsets_streams.end());

        size_t i = 0;
        auto it = offsets_streams_map.end();
        for (; i < required_offsets_streams.size(); ++i)
        {
            auto current_it = offsets_streams_map.find(required_offsets_streams[i].first);
            if (current_it == offsets_streams_map.end())
                break;
            it = current_it;
        }

        if (i && (!name_level || i > max_matched_streams))
        {
            max_matched_streams = i;
            name_level.emplace(part_column.name, it->second);
        }
    }

    return name_level;
}

void IMergeTreeReader::checkNumberOfColumns(size_t num_columns_to_read) const
{
    if (num_columns_to_read != requested_columns.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "invalid number of columns passed to MergeTreeReader::readRows. "
                        "Expected {}, got {}", requested_columns.size(), num_columns_to_read);
}

String IMergeTreeReader::getMessageForDiagnosticOfBrokenPart(size_t from_mark, size_t max_rows_to_read) const
{
    const auto & data_part_storage = data_part_info_for_read->getDataPartStorage();
    return fmt::format(
        "(while reading from part {} in table {} located on disk {} of type {}, from mark {} with max_rows_to_read = {})",
        data_part_storage->getFullPath(),
        data_part_info_for_read->getTableName(),
        data_part_storage->getDiskName(),
        data_part_storage->getDiskType(),
        from_mark,
        max_rows_to_read);
}

}
