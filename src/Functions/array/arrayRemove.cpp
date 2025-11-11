#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>

#include <Common/HashTable/HashTable.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/array/arrayRemove.h>

#include <Interpreters/castColumn.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DataTypePtr FunctionArrayRemove::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function {} doesn't match: passed {}, should be 2",
            getName(), arguments.size());

    const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
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
    const auto * return_type_array = checkAndGetDataType<DataTypeArray>(return_type.get());
    if (!return_type_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Return type for function {} must be Array, got {}", getName(), return_type->getName());

    if (typeid_cast<const DataTypeNothing *>(return_type_array->getNestedType().get()))
        return return_type->createColumnConstWithDefaultValue(input_rows_count);

    const auto * arr_col = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!arr_col)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
            "First argument for function {} must be Array, got {}", getName(), arguments[0].column->getName());

    ColumnPtr arr_data_col = arr_col->getDataPtr();
    const auto & arr_offsets = arr_col->getOffsets();
    size_t arr_elements_count = arr_data_col->size();

    auto arr_data_type = typeid_cast<const DataTypeArray &>(*arguments[0].type).getNestedType();
    auto elem_type = arguments[1].type;

    auto build_and_execute_function = [&](
        const std::string & func_name,
        const ColumnsWithTypeAndName & func_args) -> ColumnPtr
    {
        auto func = FunctionFactory::instance().get(func_name, context)->build(func_args);
        return func->execute(func_args, func->getResultType(), arr_elements_count, /* dry_run = */ false);
    };

    bool elem_is_const_null = false;
    if (const auto * elem_nullable = checkAndGetColumn<ColumnNullable>(arguments[1].column.get()))
    {
        if (elem_nullable->size() == 1 && elem_nullable->isNullAt(0))
            elem_is_const_null = true;
    }

    ColumnPtr replicated_elem_col;
    if (!elem_is_const_null)
        replicated_elem_col = arguments[1].column->replicate(arr_offsets);

    ColumnPtr filter_col;
    if (elem_is_const_null)
    {
        // Filter = NOT isNull(arr)
        auto is_arr_null_col = build_and_execute_function("isNull", { {arr_data_col, arr_data_type, "arr"} });
        filter_col = build_and_execute_function("not",
            { { is_arr_null_col, std::make_shared<DataTypeUInt8>(), "is_null_res" } });
    }
    else if (!arr_data_type->isNullable() && !elem_type->isNullable())
    {
        // Filter = notEquals(arr, replicated_elem_col)
        filter_col = build_and_execute_function("notEquals",
            {{arr_data_col, arr_data_type, "arr"},
             {replicated_elem_col, elem_type, "elem"}});
    }
    else
    {
        // Filter = NOT if(
        //      or(isNull(arr), isNull(elem)),       // cond
        //      and(isNull(arr), isNull(elem)),      // then (NULL == NULL)
        //      equals(arr, elem)                    // else (Value == Value)
        // )
        auto is_arr_null_col = build_and_execute_function("isNull", { {arr_data_col, arr_data_type, "arr"} });
        auto is_elem_null_col = build_and_execute_function("isNull", { {replicated_elem_col, elem_type, "elem"} });

        auto is_arr_null_arg = ColumnWithTypeAndName{is_arr_null_col, std::make_shared<DataTypeUInt8>(), "is_arr_null"};
        auto is_elem_null_arg = ColumnWithTypeAndName{is_elem_null_col, std::make_shared<DataTypeUInt8>(), "is_elem_null"};

        // cond: or(isNull(arr), isNull(elem))
        auto cond_col = build_and_execute_function("or", {is_arr_null_arg, is_elem_null_arg});

        // then: and(isNull(arr), isNull(elem))
        auto then_col = build_and_execute_function("and", {is_arr_null_arg, is_elem_null_arg});

        auto unnest_nullable_arg
            = [&](const ColumnPtr & nullable_col, const DataTypePtr & nullable_type) -> std::pair<ColumnPtr, DataTypePtr>
        {
            const auto * nullable_type_ptr = typeid_cast<const DataTypeNullable *>(nullable_type.get());
            if (nullable_type_ptr)
            {
                DataTypePtr nested_type = nullable_type_ptr->getNestedType();

                const auto * nullable_col_ptr = checkAndGetColumn<ColumnNullable>(nullable_col.get());
                if (!nullable_col_ptr)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ColumnNullable but received {}", nullable_col->getName());

                return {nullable_col_ptr->getNestedColumnPtr(), nested_type};
            }
            return { nullable_col, nullable_type };
        };

        // else: equals(arr, elem)
        // When the `if` function will execute this `else` clause, both arr and elem are non-nullable.
        // So first extract the nested column out if they exist. This will ensure the output type of
        // the `else` clause is UInt8 and not Nullable(UInt8).
        auto [unnested_arr_col, unnested_arr_type] = unnest_nullable_arg(arr_data_col, arr_data_type);
        auto [unnested_elem_col, unnested_elem_type] = unnest_nullable_arg(replicated_elem_col, elem_type);
        auto else_col = build_and_execute_function(
            "equals",
            {{unnested_arr_col, unnested_arr_type, "arr"}, {unnested_elem_col, unnested_elem_type, "elem"}});

        auto cond_arg = ColumnWithTypeAndName{cond_col, std::make_shared<DataTypeUInt8>(), "cond"};
        auto then_arg = ColumnWithTypeAndName{then_col, std::make_shared<DataTypeUInt8>(), "then"};
        auto else_arg = ColumnWithTypeAndName{else_col, std::make_shared<DataTypeUInt8>(), "else"};

        auto equality_check_col = build_and_execute_function("if", {cond_arg, then_arg, else_arg});
        auto equality_check_arg =
            ColumnWithTypeAndName{equality_check_col, std::make_shared<DataTypeUInt8>(), "eq_check"};

        filter_col = build_and_execute_function("not", {equality_check_arg});
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
        returned_value,
        examples,
        introduced_in,
        FunctionDocumentation::Category::Array
    };

    factory.registerFunction<FunctionArrayRemove>(documentation);
    factory.registerAlias("array_remove", "arrayRemove", FunctionFactory::Case::Insensitive);
}

}
