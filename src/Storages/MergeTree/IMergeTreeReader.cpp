#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/escapeForFileName.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Common/typeid_cast.h>


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
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_,
    const StorageMetadataPtr & metadata_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    const MarkRanges & all_mark_ranges_,
    const MergeTreeReaderSettings & settings_,
    const ValueSizeMap & avg_value_size_hints_)
    : data_part(data_part_)
    , avg_value_size_hints(avg_value_size_hints_)
    , columns(columns_)
    , uncompressed_cache(uncompressed_cache_)
    , mark_cache(mark_cache_)
    , settings(settings_)
    , storage(data_part_->storage)
    , metadata_snapshot(metadata_snapshot_)
    , all_mark_ranges(all_mark_ranges_)
    , alter_conversions(storage.getAlterConversionsForPart(data_part))
{
    if (isWidePart(data_part))
    {
        /// For wide parts convert plain arrays of Nested to subcolumns
        /// to allow to use shared offset column from cache.
        columns = Nested::convertToSubcolumns(columns);
    }
}

IMergeTreeReader::~IMergeTreeReader() = default;


const IMergeTreeReader::ValueSizeMap & IMergeTreeReader::getAvgValueSizeHints() const
{
    return avg_value_size_hints;
}

void IMergeTreeReader::fillMissingColumns(Columns & res_columns, bool & should_evaluate_missing_defaults, size_t num_rows) const
{
    try
    {
        DB::fillMissingColumns(res_columns, num_rows, columns, metadata_snapshot);
        should_evaluate_missing_defaults = std::any_of(
            res_columns.begin(), res_columns.end(), [](const auto & column) { return column == nullptr; });
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + data_part->data_part_storage->getFullPath() + ")");
        throw;
    }
}

void IMergeTreeReader::evaluateMissingDefaults(Block additional_columns, Columns & res_columns) const
{
    try
    {
        size_t num_columns = columns.size();

        if (res_columns.size() != num_columns)
            throw Exception("invalid number of columns passed to MergeTreeReader::fillMissingColumns. "
                            "Expected " + toString(num_columns) + ", "
                            "got " + toString(res_columns.size()), ErrorCodes::LOGICAL_ERROR);

        /// Convert columns list to block.
        /// TODO: rewrite with columns interface. It will be possible after changes in ExpressionActions.
        auto name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (res_columns[pos] == nullptr)
                continue;

            additional_columns.insert({res_columns[pos], name_and_type->type, name_and_type->name});
        }

        auto dag = DB::evaluateMissingDefaults(
                additional_columns, columns, metadata_snapshot->getColumns(), storage.getContext());
        if (dag)
        {
            dag->addMaterializingOutputActions();
            auto actions = std::make_shared<
                ExpressionActions>(std::move(dag),
                ExpressionActionsSettings::fromSettings(storage.getContext()->getSettingsRef()));
            actions->execute(additional_columns);
        }

        /// Move columns from block.
        name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
            res_columns[pos] = std::move(additional_columns.getByName(name_and_type->name).column);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + data_part->data_part_storage->getFullPath() + ")");
        throw;
    }
}

String IMergeTreeReader::getColumnNameInPart(const NameAndTypePair & required_column) const
{
    auto name_in_storage = required_column.getNameInStorage();
    if (alter_conversions.isColumnRenamed(name_in_storage))
    {
        name_in_storage = alter_conversions.getColumnOldName(name_in_storage);
        return Nested::concatenateName(name_in_storage, required_column.getSubcolumnName());
    }

    return required_column.name;
}

NameAndTypePair IMergeTreeReader::getColumnInPart(const NameAndTypePair & required_column) const
{
    auto column_in_part = data_part->tryGetColumn(getColumnNameInPart(required_column));
    if (column_in_part)
        return *column_in_part;

    return required_column;
}

void IMergeTreeReader::performRequiredConversions(Columns & res_columns) const
{
    try
    {
        size_t num_columns = columns.size();

        if (res_columns.size() != num_columns)
        {
            throw Exception(
                "Invalid number of columns passed to MergeTreeReader::performRequiredConversions. "
                "Expected "
                    + toString(num_columns)
                    + ", "
                      "got "
                    + toString(res_columns.size()),
                ErrorCodes::LOGICAL_ERROR);
        }

        Block copy_block;
        auto name_and_type = columns.begin();

        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (res_columns[pos] == nullptr)
                continue;

            copy_block.insert({res_columns[pos], getColumnInPart(*name_and_type).type, name_and_type->name});
        }

        DB::performRequiredConversions(copy_block, columns, storage.getContext());

        /// Move columns from block.
        name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            res_columns[pos] = std::move(copy_block.getByName(name_and_type->name).column);
        }
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + data_part->data_part_storage->getFullPath() + ")");
        throw;
    }
}

IMergeTreeReader::ColumnPosition IMergeTreeReader::findColumnForOffsets(const String & column_name) const
{
    String table_name = Nested::extractTableName(column_name);
    for (const auto & part_column : data_part->getColumns())
    {
        if (typeid_cast<const DataTypeArray *>(part_column.type.get()))
        {
            auto position = data_part->getColumnPosition(part_column.getNameInStorage());
            if (position && Nested::extractTableName(part_column.name) == table_name)
                return position;
        }
    }

    return {};
}

void IMergeTreeReader::checkNumberOfColumns(size_t num_columns_to_read) const
{
    if (num_columns_to_read != columns.size())
        throw Exception("invalid number of columns passed to MergeTreeReader::readRows. "
                        "Expected " + toString(columns.size()) + ", "
                        "got " + toString(num_columns_to_read), ErrorCodes::LOGICAL_ERROR);
}

}
