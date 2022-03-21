#include <Common/typeid_cast.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ArrayJoinAction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
}

ArrayJoinAction::ArrayJoinAction(const NameSet & array_joined_columns_, bool array_join_is_left, ContextPtr context)
    : columns(array_joined_columns_)
    , is_left(array_join_is_left)
    , is_unaligned(context->getSettingsRef().enable_unaligned_array_join)
{
    if (columns.empty())
        throw Exception("No arrays to join", ErrorCodes::LOGICAL_ERROR);

    if (is_unaligned)
    {
        function_length = FunctionFactory::instance().get("length", context);
        function_greatest = FunctionFactory::instance().get("greatest", context);
        function_arrayResize = FunctionFactory::instance().get("arrayResize", context);
    }
    else if (is_left)
        function_builder = FunctionFactory::instance().get("emptyArrayToSingle", context);
}


void ArrayJoinAction::prepare(ColumnsWithTypeAndName & sample) const
{
    for (auto & current : sample)
    {
        if (columns.count(current.name) == 0)
            continue;

        const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(&*current.type);
        if (!array_type)
            throw Exception("ARRAY JOIN requires array argument", ErrorCodes::TYPE_MISMATCH);
        current.type = array_type->getNestedType();
        current.column = nullptr;
    }
}

void ArrayJoinAction::execute(Block & block)
{
    if (columns.empty())
        throw Exception("No arrays to join", ErrorCodes::LOGICAL_ERROR);

    ColumnPtr any_array_ptr = block.getByName(*columns.begin()).column->convertToFullColumnIfConst();
    const ColumnArray * any_array = typeid_cast<const ColumnArray *>(&*any_array_ptr);
    if (!any_array)
        throw Exception("ARRAY JOIN of not array: " + *columns.begin(), ErrorCodes::TYPE_MISMATCH);

    /// If LEFT ARRAY JOIN, then we create columns in which empty arrays are replaced by arrays with one element - the default value.
    std::map<String, ColumnPtr> non_empty_array_columns;

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

            ColumnsWithTypeAndName tmp_block{src_col}; //, {{}, uint64, {}}};
            auto len_col = function_length->build(tmp_block)->execute(tmp_block, uint64, rows);

            ColumnsWithTypeAndName tmp_block2{column_of_max_length, {len_col, uint64, {}}};
            column_of_max_length.column = function_greatest->build(tmp_block2)->execute(tmp_block2, uint64, rows);
        }

        for (const auto & name : columns)
        {
            auto & src_col = block.getByName(name);

            ColumnsWithTypeAndName tmp_block{src_col, column_of_max_length};
            src_col.column = function_arrayResize->build(tmp_block)->execute(tmp_block, src_col.type, rows);
            any_array_ptr = src_col.column->convertToFullColumnIfConst();
        }

        any_array = typeid_cast<const ColumnArray *>(&*any_array_ptr);
    }
    else if (is_left)
    {
        for (const auto & name : columns)
        {
            auto src_col = block.getByName(name);

            ColumnsWithTypeAndName tmp_block{src_col};

            non_empty_array_columns[name] = function_builder->build(tmp_block)->execute(tmp_block, src_col.type, src_col.column->size());
        }

        any_array_ptr = non_empty_array_columns.begin()->second->convertToFullColumnIfConst();
        any_array = &typeid_cast<const ColumnArray &>(*any_array_ptr);
    }

    size_t num_columns = block.columns();
    for (size_t i = 0; i < num_columns; ++i)
    {
        ColumnWithTypeAndName & current = block.safeGetByPosition(i);

        if (columns.count(current.name))
        {
            if (!typeid_cast<const DataTypeArray *>(&*current.type))
                throw Exception("ARRAY JOIN of not array: " + current.name, ErrorCodes::TYPE_MISMATCH);

            ColumnPtr array_ptr = (is_left && !is_unaligned) ? non_empty_array_columns[current.name] : current.column;
            array_ptr = array_ptr->convertToFullColumnIfConst();

            const ColumnArray & array = typeid_cast<const ColumnArray &>(*array_ptr);
            if (!is_unaligned && !array.hasEqualOffsets(typeid_cast<const ColumnArray &>(*any_array_ptr)))
                throw Exception("Sizes of ARRAY-JOIN-ed arrays do not match", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

            current.column = typeid_cast<const ColumnArray &>(*array_ptr).getDataPtr();
            current.type = typeid_cast<const DataTypeArray &>(*current.type).getNestedType();
        }
        else
        {
            current.column = current.column->replicate(any_array->getOffsets());
        }
    }
}

}
