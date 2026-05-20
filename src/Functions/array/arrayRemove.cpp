#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>

#include <Common/assert_cast.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/array/arrayRemove.h>

namespace DB
{

namespace
{

ColumnPtr BuildAndExecuteFunction(
    const FunctionOverloadResolverPtr & resolver, const ColumnsWithTypeAndName & func_args, size_t expected_column_count)
{
    auto func = resolver->build(func_args);
    return func->execute(func_args, func->getResultType(), expected_column_count, /* dry_run = */ false);
}

}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

FunctionPtr FunctionArrayRemove::create(ContextPtr context_)
{
    return std::make_shared<FunctionArrayRemove>(context_);
}

FunctionArrayRemove::FunctionArrayRemove(ContextPtr context_)
    : is_null_resolver(FunctionFactory::instance().get("isNull", context_))
    , not_resolver(FunctionFactory::instance().get("not", context_))
    , not_equals_resolver(FunctionFactory::instance().get("notEquals", context_))
    , equals_resolver(FunctionFactory::instance().get("equals", context_))
    , or_resolver(FunctionFactory::instance().get("or", context_))
    , and_resolver(FunctionFactory::instance().get("and", context_))
    , if_resolver(FunctionFactory::instance().get("if", context_))
{
}

DataTypePtr FunctionArrayRemove::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 2",
            getName(), arguments.size());

    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(arguments[0]).get());
    if (!array_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "First argument for function {} must be an array but it has type {}.",
                        getName(), arguments[0]->getName());

    return arguments[0];
}

ColumnPtr FunctionArrayRemove::executeImpl(
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & return_type,
    size_t input_rows_count) const
{
    const auto & arr_arg_column_full = arguments[0].column->convertToFullColumnIfConst();
    const bool argument_type_is_nullable = arguments[0].type->isNullable();
    if (const auto * nullable_array_column = checkAndGetColumn<ColumnNullable>(arr_arg_column_full.get()))
    {
        if (checkAndGetColumn<ColumnArray>(&nullable_array_column->getNestedColumn()))
        {
            ColumnsWithTypeAndName nested_arguments = arguments;
            nested_arguments[0].column = nullable_array_column->getNestedColumnPtr();
            nested_arguments[0].type = removeNullable(arguments[0].type);

            auto nested_result = executeImpl(nested_arguments, removeNullable(return_type), input_rows_count);

            if (return_type->isNullable())
            {
                auto mutable_nested_result = IColumn::mutate(nested_result->convertToFullColumnIfConst());
                const size_t num_rows = arguments[0].column->size();
                if (mutable_nested_result->size() == 1 && num_rows != 1)
                    mutable_nested_result = mutable_nested_result->cloneResized(num_rows);

                auto null_map = ColumnUInt8::create();
                auto & null_map_data = null_map->getData();
                null_map_data.assign(
                    nullable_array_column->getNullMapData().begin(), nullable_array_column->getNullMapData().end());
                if (null_map_data.size() == 1)
                    null_map_data.resize_fill(num_rows, nullable_array_column->getNullMapData()[0]);
                else if (null_map_data.size() != num_rows)
                    null_map_data.resize_fill(num_rows, 0);

                return ColumnNullable::create(std::move(mutable_nested_result), std::move(null_map));
            }

            return nested_result;
        }
    }
    else if (argument_type_is_nullable)
    {
        ColumnsWithTypeAndName nested_arguments = arguments;
        nested_arguments[0].type = removeNullable(arguments[0].type);

        auto nested_result = executeImpl(nested_arguments, removeNullable(return_type), input_rows_count);

        if (return_type->isNullable())
        {
            auto null_map = ColumnUInt8::create();
            null_map->getData().resize_fill(nested_result->size(), 0);
            return ColumnNullable::create(nested_result, std::move(null_map));
        }

        return nested_result;
    }

    const auto * return_type_array = checkAndGetDataType<DataTypeArray>(removeNullable(return_type).get());
    if (!return_type_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Return type for function {} must be Array, got {}", getName(), return_type->getName());

    if (typeid_cast<const DataTypeNothing *>(return_type_array->getNestedType().get()))
        return return_type->createColumnConstWithDefaultValue(input_rows_count);

    const auto & arr_arg_column = arr_arg_column_full;
    const auto * arr_col = checkAndGetColumn<ColumnArray>(arr_arg_column.get());
    if (!arr_col)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "First argument for function {} must be Array, got {}", getName(), arguments[0].column->getName());

    const auto & arr_data_col = arr_col->getDataPtr();
    const auto & arr_offsets = arr_col->getOffsets();
    size_t arr_elements_count = arr_data_col->size();

    const auto & arr_data_type = typeid_cast<const DataTypeArray &>(*removeNullable(arguments[0].type)).getNestedType();
    const auto & elem_type = arguments[1].type;

    ColumnPtr filter_col;
    if (elem_type->onlyNull())
    {
        if (!canContainNull(*arr_data_type))
        {
            return arguments[0].column;
        }

        // Filter = NOT isNull(arr)
        auto is_arr_null_col = BuildAndExecuteFunction(is_null_resolver, {{arr_data_col, arr_data_type, "arr"}}, arr_elements_count);
        filter_col = BuildAndExecuteFunction(
            not_resolver, {{is_arr_null_col, std::make_shared<DataTypeUInt8>(), "is_null_res"}}, arr_elements_count);
    }
    else if (!canContainNull(*arr_data_type) && !canContainNull(*elem_type))
    {
        // Filter = notEquals(arr, replicated_elem_col)
        filter_col = BuildAndExecuteFunction(not_equals_resolver,
            {{arr_data_col, arr_data_type, "arr"},
             {arguments[1].column->replicate(arr_offsets), elem_type, "elem"}}, arr_elements_count);
    }
    else
    {
        // Filter = NOT if(
        //      or(isNull(arr), isNull(elem)),       // cond
        //      and(isNull(arr), isNull(elem)),      // then (NULL == NULL)
        //      equals(arr, elem)                    // else (Value == Value)
        // )
        const auto & replicated_elem_col = arguments[1].column->replicate(arr_offsets);

        auto is_arr_null_col = BuildAndExecuteFunction(is_null_resolver, {{arr_data_col, arr_data_type, "arr"}}, arr_elements_count);
        auto is_elem_null_col = BuildAndExecuteFunction(is_null_resolver, {{replicated_elem_col, elem_type, "elem"}}, arr_elements_count);

        auto is_arr_null_arg = ColumnWithTypeAndName{is_arr_null_col, std::make_shared<DataTypeUInt8>(), "is_arr_null"};
        auto is_elem_null_arg = ColumnWithTypeAndName{is_elem_null_col, std::make_shared<DataTypeUInt8>(), "is_elem_null"};

        // cond: or(isNull(arr), isNull(elem))
        auto cond_col = BuildAndExecuteFunction(or_resolver, {is_arr_null_arg, is_elem_null_arg}, arr_elements_count);

        // then: and(isNull(arr), isNull(elem))
        auto then_col = BuildAndExecuteFunction(and_resolver, {is_arr_null_arg, is_elem_null_arg}, arr_elements_count);

        auto equals_result = BuildAndExecuteFunction(
            equals_resolver, {{arr_data_col, arr_data_type, "arr"}, {replicated_elem_col, elem_type, "elem"}}, arr_elements_count);
        /// equals() on tuples with nullable components may return ColumnConst(Nullable(UInt8)).
        /// convertToFullIfNeeded() unwraps ColumnConst so removeNullable can strip the Nullable layer.
        auto else_col = removeNullable(equals_result->convertToFullIfNeeded());

        auto cond_arg = ColumnWithTypeAndName{cond_col, std::make_shared<DataTypeUInt8>(), "cond"};
        auto then_arg = ColumnWithTypeAndName{then_col, std::make_shared<DataTypeUInt8>(), "then"};
        auto else_arg = ColumnWithTypeAndName{else_col, std::make_shared<DataTypeUInt8>(), "else"};

        auto equality_check_col = BuildAndExecuteFunction(if_resolver, {cond_arg, then_arg, else_arg}, arr_elements_count);
        auto equality_check_arg = ColumnWithTypeAndName{equality_check_col, std::make_shared<DataTypeUInt8>(), "eq_check"};

        filter_col = BuildAndExecuteFunction(not_resolver, {equality_check_arg}, arr_elements_count);
    }

    /// The filter can end up as ColumnConst or ColumnNullable when comparing tuples
    /// with NULL components during constant folding. Normalize to a plain ColumnUInt8.
    filter_col = filter_col->convertToFullIfNeeded();
    if (const auto * nullable_filter = checkAndGetColumn<ColumnNullable>(filter_col.get()))
    {
        /// NULL in filter means comparison was indeterminate (e.g., tuple with NULL component).
        /// Treat as "not equal" → keep the element (filter = 1).
        auto result_col = ColumnUInt8::create(nullable_filter->size());
        auto & result_data = result_col->getData();
        const auto & nested_data = assert_cast<const ColumnUInt8 &>(nullable_filter->getNestedColumn()).getData();
        const auto & null_map = nullable_filter->getNullMapData();
        for (size_t i = 0; i < null_map.size(); ++i)
            result_data[i] = null_map[i] ? 1 : nested_data[i];
        filter_col = std::move(result_col);
    }

    const auto * filter_col_uint8 = checkAndGetColumn<ColumnUInt8>(filter_col.get());
    if (!filter_col_uint8)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Filter column for arrayRemove was not evaluated as ColumnUInt8 but as {}", filter_col->getDataType());

    const IColumn::Filter & filter = filter_col_uint8->getData();
    ColumnPtr filtered = arr_col->getData().filter(filter, -1);

    auto column_offsets = ColumnArray::ColumnOffsets::create(arr_offsets.size());
    IColumn::Offsets & out_offsets = column_offsets->getData();

    size_t in_pos = 0;
    size_t out_pos = 0;
    for (size_t i = 0; i < arr_offsets.size(); ++i)
    {
        size_t src_end = arr_offsets[i];
        for (; in_pos < src_end; ++in_pos)
        {
            if (filter[in_pos])
            {
                ++out_pos;
            }
        }
        out_offsets[i] = out_pos;
    }

    return ColumnArray::create(filtered, std::move(column_offsets));
}

REGISTER_FUNCTION(ArrayRemove)
{
    FunctionDocumentation::Description description = R"(
Removes all elements equal to a given value from an array.
NULLs are treated as equal.
)";
    FunctionDocumentation::Syntax syntax = "arrayRemove(arr, elem)";
    FunctionDocumentation::Examples examples = {
        {"Example 1", "SELECT arrayRemove([1, 2, 2, 3], 2)", "[1, 3]"},
        {"Example 2", "SELECT arrayRemove(['a', NULL, 'b', NULL], NULL)", "['a', 'b']"}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a subset of the source array", {"Array(T)"}};

    FunctionDocumentation::IntroducedIn introduced_in = {25, 11};
    FunctionDocumentation documentation = {
        description, syntax,
        {{"arr", "Array(T)"}, {"elem", "T"}},
        {},
        returned_value,
        examples,
        introduced_in,
        FunctionDocumentation::Category::Array
    };

    factory.registerFunction<FunctionArrayRemove>(documentation);
    factory.registerAlias("array_remove", "arrayRemove", FunctionFactory::Case::Insensitive);
}

}
