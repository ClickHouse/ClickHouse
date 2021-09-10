#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/escapeForFileName.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/inplaceBlockConversions.h>
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
    , part_columns(data_part->getColumns())
    , uncompressed_cache(uncompressed_cache_)
    , mark_cache(mark_cache_)
    , settings(settings_)
    , storage(data_part_->storage)
    , metadata_snapshot(metadata_snapshot_)
    , all_mark_ranges(all_mark_ranges_)
    , alter_conversions(storage.getAlterConversionsForPart(data_part))
{
    if (settings.convert_nested_to_subcolumns)
    {
        columns = Nested::convertToSubcolumns(columns);
        part_columns = Nested::collect(part_columns);
    }

    columns_from_part.set_empty_key(StringRef());
    for (const auto & column_from_part : part_columns)
        columns_from_part.emplace(column_from_part.name, &column_from_part.type);
}

IMergeTreeReader::~IMergeTreeReader() = default;


const IMergeTreeReader::ValueSizeMap & IMergeTreeReader::getAvgValueSizeHints() const
{
    return avg_value_size_hints;
}


static bool arrayHasNoElementsRead(const IColumn & column)
{
    const auto * column_array = typeid_cast<const ColumnArray *>(&column);

    if (!column_array)
        return false;

    size_t size = column_array->size();
    if (!size)
        return false;

    size_t data_size = column_array->getData().size();
    if (data_size)
        return false;

    size_t last_offset = column_array->getOffsets()[size - 1];
    return last_offset != 0;
}

void IMergeTreeReader::fillMissingColumns(Columns & res_columns, bool & should_evaluate_missing_defaults, size_t num_rows)
{
    try
    {
        size_t num_columns = columns.size();

        if (res_columns.size() != num_columns)
            throw Exception("invalid number of columns passed to MergeTreeReader::fillMissingColumns. "
                            "Expected " + toString(num_columns) + ", "
                            "got " + toString(res_columns.size()), ErrorCodes::LOGICAL_ERROR);

        /// For a missing column of a nested data structure we must create not a column of empty
        /// arrays, but a column of arrays of correct length.

        /// First, collect offset columns for all arrays in the block.
        OffsetColumns offset_columns;
        auto requested_column = columns.begin();
        for (size_t i = 0; i < num_columns; ++i, ++requested_column)
        {
            if (res_columns[i] == nullptr)
                continue;

            if (const auto * array = typeid_cast<const ColumnArray *>(res_columns[i].get()))
            {
                String offsets_name = Nested::extractTableName(requested_column->name);
                auto & offsets_column = offset_columns[offsets_name];

                /// If for some reason multiple offsets columns are present for the same nested data structure,
                /// choose the one that is not empty.
                if (!offsets_column || offsets_column->empty())
                    offsets_column = array->getOffsetsPtr();
            }
        }

        should_evaluate_missing_defaults = false;

        /// insert default values only for columns without default expressions
        requested_column = columns.begin();
        for (size_t i = 0; i < num_columns; ++i, ++requested_column)
        {
            auto & [name, type] = *requested_column;

            if (res_columns[i] && arrayHasNoElementsRead(*res_columns[i]))
                res_columns[i] = nullptr;

            if (res_columns[i] == nullptr)
            {
                if (metadata_snapshot->getColumns().hasDefault(name))
                {
                    should_evaluate_missing_defaults = true;
                    continue;
                }

                String offsets_name = Nested::extractTableName(name);
                auto offset_it = offset_columns.find(offsets_name);
                const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
                if (offset_it != offset_columns.end() && array_type)
                {
                    const auto & nested_type = array_type->getNestedType();
                    ColumnPtr offsets_column = offset_it->second;
                    size_t nested_rows = typeid_cast<const ColumnUInt64 &>(*offsets_column).getData().back();

                    ColumnPtr nested_column =
                        nested_type->createColumnConstWithDefaultValue(nested_rows)->convertToFullColumnIfConst();

                    res_columns[i] = ColumnArray::create(nested_column, offsets_column);
                }
                else
                {
                    /// We must turn a constant column into a full column because the interpreter could infer
                    /// that it is constant everywhere but in some blocks (from other parts) it can be a full column.
                    res_columns[i] = type->createColumnConstWithDefaultValue(num_rows)->convertToFullColumnIfConst();
                }
            }
        }
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + data_part->getFullPath() + ")");
        throw;
    }
}

void IMergeTreeReader::evaluateMissingDefaults(Block additional_columns, Columns & res_columns)
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
        e.addMessage("(while reading from part " + data_part->getFullPath() + ")");
        throw;
    }
}

NameAndTypePair IMergeTreeReader::getColumnFromPart(const NameAndTypePair & required_column) const
{
    auto name_in_storage = required_column.getNameInStorage();

    decltype(columns_from_part.begin()) it;
    if (alter_conversions.isColumnRenamed(name_in_storage))
    {
        String old_name = alter_conversions.getColumnOldName(name_in_storage);
        it = columns_from_part.find(old_name);
    }
    else
    {
        it = columns_from_part.find(name_in_storage);
    }

    if (it == columns_from_part.end())
        return required_column;

    const auto & type = *it->second;
    if (required_column.isSubcolumn())
    {
        auto subcolumn_name = required_column.getSubcolumnName();
        auto subcolumn_type = type->tryGetSubcolumnType(subcolumn_name);

        if (!subcolumn_type)
            return required_column;

        return {String(it->first), subcolumn_name, type, subcolumn_type};
    }

    return {String(it->first), type};
}

void IMergeTreeReader::performRequiredConversions(Columns & res_columns)
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

            copy_block.insert({res_columns[pos], getColumnFromPart(*name_and_type).type, name_and_type->name});
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
        e.addMessage("(while reading from part " + data_part->getFullPath() + ")");
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
            auto position = data_part->getColumnPosition(part_column.name);
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
