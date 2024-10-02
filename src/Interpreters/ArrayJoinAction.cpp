#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ArrayJoinAction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int TYPE_MISMATCH;
}

std::shared_ptr<const DataTypeArray> getArrayJoinDataType(DataTypePtr type)
{
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return std::shared_ptr<const DataTypeArray>{type, array_type};
    else if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get()))
    {
        const auto & nested_type = map_type->getNestedType();
        const auto * nested_array_type = typeid_cast<const DataTypeArray *>(nested_type.get());
        return std::shared_ptr<const DataTypeArray>{nested_type, nested_array_type};
    }
    else
        return nullptr;
}

ColumnPtr getArrayJoinColumn(const ColumnPtr & column)
{
    if (typeid_cast<const ColumnArray *>(column.get()))
        return column;
    else if (const auto * map = typeid_cast<const ColumnMap *>(column.get()))
        return map->getNestedColumnPtr();
    else
        return nullptr;
}

const ColumnArray * getArrayJoinColumnRawPtr(const ColumnPtr & column)
{
    if (const auto & col_arr = getArrayJoinColumn(column))
        return typeid_cast<const ColumnArray *>(col_arr.get());
    return nullptr;
}

ColumnWithTypeAndName convertArrayJoinColumn(const ColumnWithTypeAndName & src_col)
{
    ColumnWithTypeAndName array_col;
    array_col.name = src_col.name;
    array_col.type = getArrayJoinDataType(src_col.type);
    array_col.column = getArrayJoinColumn(src_col.column->convertToFullColumnIfConst());
    return array_col;
}

ArrayJoinAction::ArrayJoinAction(const NameSet & array_joined_columns_, bool array_join_is_left, ContextPtr context)
    : columns(array_joined_columns_)
    , is_left(array_join_is_left)
    , is_unaligned(context->getSettingsRef().enable_unaligned_array_join)
    , max_block_size(context->getSettingsRef().max_block_size)
{
    if (columns.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No arrays to join");

    if (is_unaligned)
    {
        function_length = FunctionFactory::instance().get("length", context);
        function_greatest = FunctionFactory::instance().get("greatest", context);
        function_array_resize = FunctionFactory::instance().get("arrayResize", context);
    }
    else if (is_left)
        function_builder = FunctionFactory::instance().get("emptyArrayToSingle", context);
}

void ArrayJoinAction::prepare(ColumnsWithTypeAndName & sample) const
{
    for (auto & current : sample)
    {
        if (!columns.contains(current.name))
            continue;

        if (const auto & type = getArrayJoinDataType(current.type))
        {
            current.column = nullptr;
            current.type = type->getNestedType();
        }
        else
            throw Exception(ErrorCodes::TYPE_MISMATCH, "ARRAY JOIN requires array or map argument");
    }
}

ArrayJoinResultIteratorPtr ArrayJoinAction::execute(Block block)
{
    if (columns.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No arrays to join");

    return std::make_unique<ArrayJoinResultIterator>(this, std::move(block));
}


ArrayJoinResultIterator::ArrayJoinResultIterator(const ArrayJoinAction * array_join_, Block block_)
    : array_join(array_join_), block(std::move(block_)), total_rows(block.rows()), current_row(0)
{
    const auto & columns = array_join->columns;
    bool is_unaligned = array_join->is_unaligned;
    bool is_left = array_join->is_left;
    const auto & function_length = array_join->function_length;
    const auto & function_greatest = array_join->function_greatest;
    const auto & function_array_resize = array_join->function_array_resize;
    const auto & function_builder = array_join->function_builder;

    any_array_map_ptr = block.getByName(*columns.begin()).column->convertToFullColumnIfConst();
    any_array = getArrayJoinColumnRawPtr(any_array_map_ptr);
    if (!any_array)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "ARRAY JOIN requires array or map argument");

    if (is_unaligned)
    {
        /// Resize all array joined columns to the longest one, (at least 1 if LEFT ARRAY JOIN), padded with default values.
        auto rows = block.rows();
        auto uint64 = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName column_of_max_length{{}, uint64, {}};
        if (is_left)
            column_of_max_length = ColumnWithTypeAndName(uint64->createColumnConst(rows, 1u), uint64, {});
        else
            column_of_max_length = ColumnWithTypeAndName(uint64->createColumnConst(rows, 0u), uint64, {});

        for (const auto & name : columns)
        {
            auto & src_col = block.getByName(name);

            ColumnWithTypeAndName array_col = convertArrayJoinColumn(src_col);
            ColumnsWithTypeAndName tmp_block{array_col}; //, {{}, uint64, {}}};
            auto len_col = function_length->build(tmp_block)->execute(tmp_block, uint64, rows);

            ColumnsWithTypeAndName tmp_block2{column_of_max_length, {len_col, uint64, {}}};
            column_of_max_length.column = function_greatest->build(tmp_block2)->execute(tmp_block2, uint64, rows);
        }

        for (const auto & name : columns)
        {
            auto & src_col = block.getByName(name);

            ColumnWithTypeAndName array_col = convertArrayJoinColumn(src_col);
            ColumnsWithTypeAndName tmp_block{array_col, column_of_max_length};
            array_col.column = function_array_resize->build(tmp_block)->execute(tmp_block, array_col.type, rows);

            src_col = std::move(array_col);
            any_array_map_ptr = src_col.column->convertToFullColumnIfConst();
        }

        any_array = getArrayJoinColumnRawPtr(any_array_map_ptr);
        if (!any_array)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "ARRAY JOIN requires array or map argument");
    }
    else if (is_left)
    {
        for (const auto & name : columns)
        {
            const auto & src_col = block.getByName(name);
            ColumnWithTypeAndName array_col = convertArrayJoinColumn(src_col);
            ColumnsWithTypeAndName tmp_block{array_col};
            non_empty_array_columns[name] = function_builder->build(tmp_block)->execute(tmp_block, array_col.type, array_col.column->size());
        }

        any_array_map_ptr = non_empty_array_columns.begin()->second->convertToFullColumnIfConst();
        any_array = getArrayJoinColumnRawPtr(any_array_map_ptr);
        if (!any_array)
            throw Exception(ErrorCodes::TYPE_MISMATCH, "ARRAY JOIN requires array or map argument");
    }
}

bool ArrayJoinResultIterator::hasNext() const
{
    return total_rows != 0 && current_row < total_rows;
}


Block ArrayJoinResultIterator::next()
{
    if (!hasNext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No more elements in ArrayJoinResultIterator.");

    size_t max_block_size = array_join->max_block_size;
    const auto & offsets = any_array->getOffsets();

    /// Make sure output block rows do not exceed max_block_size.
    size_t next_row = current_row;
    for (; next_row < total_rows; ++next_row)
    {
        if (offsets[next_row] - offsets[current_row - 1] >= max_block_size)
            break;
    }
    if (next_row == current_row)
        ++next_row;

    Block res;
    size_t num_columns = block.columns();
    const auto & columns = array_join->columns;
    bool is_unaligned = array_join->is_unaligned;
    bool is_left = array_join->is_left;
    auto cut_any_col = any_array->cut(current_row, next_row - current_row);
    const auto * cut_any_array = typeid_cast<const ColumnArray *>(cut_any_col.get());

    for (size_t i = 0; i < num_columns; ++i)
    {
        ColumnWithTypeAndName current = block.safeGetByPosition(i);

        /// Reuse cut_any_col if possible to avoid unnecessary cut.
        if (!is_unaligned && !is_left && current.name == *columns.begin())
        {
            current.column = cut_any_col;
            current.type = getArrayJoinDataType(current.type);
        }
        else
            current.column = current.column->cut(current_row, next_row - current_row);

        if (columns.contains(current.name))
        {
            if (const auto & type = getArrayJoinDataType(current.type))
            {
                ColumnPtr array_ptr;
                if (typeid_cast<const DataTypeArray *>(current.type.get()))
                {
                    array_ptr = (is_left && !is_unaligned) ? non_empty_array_columns[current.name]->cut(current_row, next_row - current_row)
                                                           : current.column;
                    array_ptr = array_ptr->convertToFullColumnIfConst();
                }
                else
                {
                    ColumnPtr map_ptr = current.column->convertToFullColumnIfConst();
                    const ColumnMap & map = typeid_cast<const ColumnMap &>(*map_ptr);
                    array_ptr = (is_left && !is_unaligned) ? non_empty_array_columns[current.name]->cut(current_row, next_row - current_row)
                                                           : map.getNestedColumnPtr();
                }

                const ColumnArray & array = typeid_cast<const ColumnArray &>(*array_ptr);
                if (!is_unaligned && !array.hasEqualOffsets(*cut_any_array))
                    throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Sizes of ARRAY-JOIN-ed arrays do not match");

                current.column = typeid_cast<const ColumnArray &>(*array_ptr).getDataPtr();
                current.type = type->getNestedType();
            }
            else
                throw Exception(ErrorCodes::TYPE_MISMATCH, "ARRAY JOIN of not array nor map: {}", current.name);
        }
        else
        {
            current.column = current.column->replicate(cut_any_array->getOffsets());
        }

        res.insert(std::move(current));
    }

    current_row = next_row;
    return res;
}

}
