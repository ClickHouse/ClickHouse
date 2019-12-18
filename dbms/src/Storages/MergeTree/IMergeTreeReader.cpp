#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/escapeForFileName.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Common/typeid_cast.h>
#include <Poco/File.h>


namespace DB
{

namespace
{
    using OffsetColumns = std::map<std::string, ColumnPtr>;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


IMergeTreeReader::IMergeTreeReader(const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_, UncompressedCache * uncompressed_cache_, MarkCache * mark_cache_,
    const MarkRanges & all_mark_ranges_, const MergeTreeReaderSettings & settings_,
    const ValueSizeMap & avg_value_size_hints_)
    : data_part(data_part_), avg_value_size_hints(avg_value_size_hints_), path(data_part_->getFullPath())
    , columns(columns_), uncompressed_cache(uncompressed_cache_), mark_cache(mark_cache_)
    , settings(settings_), storage(data_part_->storage)
    , all_mark_ranges(all_mark_ranges_)
{
}

IMergeTreeReader::~IMergeTreeReader() = default;


const IMergeTreeReader::ValueSizeMap & IMergeTreeReader::getAvgValueSizeHints() const
{
    return avg_value_size_hints;
}


static bool arrayHasNoElementsRead(const IColumn & column)
{
    const ColumnArray * column_array = typeid_cast<const ColumnArray *>(&column);

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


void IMergeTreeReader::fillMissingColumns(Block & res, bool & should_reorder, bool & should_evaluate_missing_defaults, size_t num_rows)
{
    try
    {
        /// For a missing column of a nested data structure we must create not a column of empty
        /// arrays, but a column of arrays of correct length.

        /// First, collect offset columns for all arrays in the block.
        OffsetColumns offset_columns;
        for (size_t i = 0; i < res.columns(); ++i)
        {
            const ColumnWithTypeAndName & column = res.safeGetByPosition(i);

            if (const ColumnArray * array = typeid_cast<const ColumnArray *>(column.column.get()))
            {
                String offsets_name = Nested::extractTableName(column.name);
                auto & offsets_column = offset_columns[offsets_name];

                /// If for some reason multiple offsets columns are present for the same nested data structure,
                /// choose the one that is not empty.
                if (!offsets_column || offsets_column->empty())
                    offsets_column = array->getOffsetsPtr();
            }
        }

        should_evaluate_missing_defaults = false;
        should_reorder = false;

        /// insert default values only for columns without default expressions
        for (const auto & requested_column : columns)
        {
            bool has_column = res.has(requested_column.name);
            if (has_column)
            {
                const auto & col = *res.getByName(requested_column.name).column;
                if (arrayHasNoElementsRead(col))
                {
                    res.erase(requested_column.name);
                    has_column = false;
                }
            }

            if (!has_column)
            {
                should_reorder = true;
                if (storage.getColumns().hasDefault(requested_column.name))
                {
                    should_evaluate_missing_defaults = true;
                    continue;
                }

                ColumnWithTypeAndName column_to_add;
                column_to_add.name = requested_column.name;
                column_to_add.type = requested_column.type;

                String offsets_name = Nested::extractTableName(column_to_add.name);
                if (offset_columns.count(offsets_name))
                {
                    ColumnPtr offsets_column = offset_columns[offsets_name];
                    DataTypePtr nested_type = typeid_cast<const DataTypeArray &>(*column_to_add.type).getNestedType();
                    size_t nested_rows = typeid_cast<const ColumnUInt64 &>(*offsets_column).getData().back();

                    ColumnPtr nested_column = nested_type->createColumnConstWithDefaultValue(nested_rows)->convertToFullColumnIfConst();

                    column_to_add.column = ColumnArray::create(nested_column, offsets_column);
                }
                else
                {
                    /// We must turn a constant column into a full column because the interpreter could infer that it is constant everywhere
                    /// but in some blocks (from other parts) it can be a full column.
                    column_to_add.column = column_to_add.type->createColumnConstWithDefaultValue(num_rows)->convertToFullColumnIfConst();
                }

                res.insert(std::move(column_to_add));
            }
        }
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + path + ")");
        throw;
    }
}

void IMergeTreeReader::reorderColumns(Block & res, const Names & ordered_names, const String * filter_name)
{
    try
    {
        Block ordered_block;

        for (const auto & name : ordered_names)
            if (res.has(name))
                ordered_block.insert(res.getByName(name));

        if (filter_name && !ordered_block.has(*filter_name) && res.has(*filter_name))
            ordered_block.insert(res.getByName(*filter_name));

        std::swap(res, ordered_block);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + path + ")");
        throw;
    }
}

void IMergeTreeReader::evaluateMissingDefaults(Block & res)
{
    try
    {
        DB::evaluateMissingDefaults(res, columns, storage.getColumns().getDefaults(), storage.global_context);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + path + ")");
        throw;
    }
}

}
