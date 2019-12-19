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
                if (storage.getColumns().hasDefault(name))
                {
                    should_evaluate_missing_defaults = true;
                    continue;
                }

                String offsets_name = Nested::extractTableName(name);
                auto offset_it = offset_columns.find(offsets_name);
                if (offset_it != offset_columns.end())
                {
                    ColumnPtr offsets_column = offset_it->second;
                    DataTypePtr nested_type = typeid_cast<const DataTypeArray &>(*type).getNestedType();
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
        e.addMessage("(while reading from part " + path + ")");
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
        /// TODO: rewrite with columns interface. It wll be possible after changes in ExpressionActions.
        auto name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (res_columns[pos] == nullptr)
                continue;

            additional_columns.insert({res_columns[pos], name_and_type->type, name_and_type->name});
        }

        DB::evaluateMissingDefaults(additional_columns, columns, storage.getColumns().getDefaults(), storage.global_context);

        /// Move columns from block.
        name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
            res_columns[pos] = std::move(additional_columns.getByName(name_and_type->name).column);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + path + ")");
        throw;
    }
}

}
