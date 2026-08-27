#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

class FunctionKqlArraySort : public IFunction
{
public:
    explicit FunctionKqlArraySort(ContextPtr context, const String & name_, bool is_desc_)
        : function_name(name_)
        , length_func(FunctionFactory::instance().get("length", context))
        , array_resize_func(FunctionFactory::instance().get("arrayResize", context))
        , array_zip_func(FunctionFactory::instance().get("arrayZip", context))
        , sort_func(FunctionFactory::instance().get(is_desc_ ? "arrayReverseSort" : "arraySort", context))
        , tuple_element_func(FunctionFactory::instance().get("tupleElement", context))
        , index_of_func(FunctionFactory::instance().get("indexOf", context))
        , array_slice_func(FunctionFactory::instance().get("arraySlice", context))
        , array_concat_func(FunctionFactory::instance().get("arrayConcat", context))
    {
    }

    String getName() const override { return function_name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Function {} needs at least one argument; passed {}.",
                getName(),
                arguments.size());

        auto array_count = arguments.size();

        const auto & last_arg = arguments[array_count - 1];
        if (!isArray(last_arg.type))
        {
            --array_count;

            if (!isUInt8(last_arg.type))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument null_last of function {} must have type UInt8 or Bool.", getName());
            if (!last_arg.column || !last_arg.column->isConst())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument null_last of function {} must be constant.", getName());
        }

        DataTypes nested_types;
        for (size_t index = 0; index < array_count; ++index)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[index].type.get());
            if (!array_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} of function {} must be array. Found {} instead.",
                    index + 1,
                    getName(),
                    arguments[0].type->getName());

            nested_types.emplace_back(array_type->getNestedType());
        }

        DataTypes data_types(array_count);

        for (size_t i = 0; i < array_count; ++i)
            data_types[i] = std::make_shared<DataTypeArray>(makeNullable(nested_types[i]));

        return std::make_shared<DataTypeTuple>(data_types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        size_t array_count = arguments.size();
        const auto & last_arg = arguments[array_count - 1];

        bool null_last = true;
        if (!isArray(last_arg.type))
        {
            --array_count;
            null_last = last_arg.column->getBool(0);
        }

        ColumnsWithTypeAndName new_args;
        ColumnPtr first_array_column;
        DataTypes nested_types;

        for (size_t i = 0; i < array_count; ++i)
        {
            ColumnPtr holder = arguments[i].column->convertToFullColumnIfConst();

            const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(holder.get());
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[i].type.get());

            if (!column_array)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Argument {} of function {} must be array. Found column {} instead.",
                    i + 1,
                    getName(),
                    holder->getName());

            nested_types.emplace_back(makeNullable(array_type->getNestedType()));
            if (i == 0)
            {
                first_array_column = holder;
                new_args.push_back(arguments[i]);
            }
            else if (!column_array->hasEqualOffsets(static_cast<const ColumnArray &>(*first_array_column)))
            {
                /// Per-row resize to match the first array's lengths, padding with defaults.
                /// This ensures each row is processed independently: mismatched array lengths
                /// in one row don't affect other rows.
                ColumnsWithTypeAndName length_args = {{first_array_column, arguments[0].type, ""}};
                auto lengths = length_func
                    ->build(length_args)->execute(length_args, std::make_shared<DataTypeUInt64>(), input_rows_count, /* dry_run = */ false);

                ColumnsWithTypeAndName resize_args = {
                    arguments[i],
                    {lengths, std::make_shared<DataTypeUInt64>(), ""}
                };
                auto resized = array_resize_func
                    ->build(resize_args)->execute(resize_args, arguments[i].type, input_rows_count, /* dry_run = */ false);
                new_args.push_back({resized, arguments[i].type, arguments[i].name});
            }
            else
                new_args.push_back(arguments[i]);
        }

        auto zipped
            = array_zip_func->build(new_args)->execute(new_args, result_type, input_rows_count, /* dry_run = */ false);

        ColumnsWithTypeAndName sort_arg({{zipped, std::make_shared<DataTypeArray>(result_type), "zipped"}});
        auto sorted_tuple
            = sort_func->build(sort_arg)->execute(sort_arg, result_type, input_rows_count, /* dry_run = */ false);

        Columns tuple_columns(array_count);
        for (size_t i = 0; i < array_count; ++i)
        {
            ColumnsWithTypeAndName untuple_args(
                {{ColumnWithTypeAndName(sorted_tuple, std::make_shared<DataTypeArray>(result_type), "sorted")},
                 {DataTypeUInt8().createColumnConst(1, toField(UInt8(i + 1))), std::make_shared<DataTypeUInt8>(), ""}});
            auto tuple_column = tuple_element_func
                                    ->build(untuple_args)
                                    ->execute(untuple_args, result_type, input_rows_count, /* dry_run = */ false);
            tuple_column = tuple_column->convertToFullColumnIfConst();

            auto out_tmp = ColumnArray::create(nested_types[i]->createColumn());

            size_t array_size = tuple_column->size();
            const auto & arr = checkAndGetColumn<ColumnArray>(*tuple_column);

            for (size_t j = 0; j < array_size; ++j)
            {
                Field arr_field;
                arr.get(j, arr_field);
                out_tmp->insert(arr_field);
            }

            tuple_columns[i] = std::move(out_tmp);
        }

        if (!null_last)
        {
            Columns adjusted_columns(array_count);

            ColumnWithTypeAndName arg_of_index{nullptr, std::make_shared<DataTypeArray>(nested_types[0]), "array"};
            arg_of_index.column = tuple_columns[0];

            auto inside_null_type = nested_types[0];
            ColumnsWithTypeAndName indexof_args({
                arg_of_index,
                {inside_null_type->createColumnConstWithDefaultValue(input_rows_count), inside_null_type, "NULL"},
            });

            auto null_index_datetype = std::make_shared<DataTypeUInt64>();

            ColumnWithTypeAndName slice_index{nullptr, null_index_datetype, ""};
            slice_index.column = index_of_func
                                     ->build(indexof_args)
                                     ->execute(indexof_args, result_type, input_rows_count, /* dry_run = */ false);

            auto null_index_in_array = slice_index.column->get64(0);
            if (null_index_in_array > 0)
            {
                ColumnWithTypeAndName slice_index_len{nullptr, null_index_datetype, ""};
                slice_index_len.column = DataTypeUInt64().createColumnConst(1, toField(UInt64(null_index_in_array - 1)));

                auto fun_slice = array_slice_func;

                for (size_t i = 0; i < array_count; ++i)
                {
                    DataTypePtr arg_type = std::make_shared<DataTypeArray>(nested_types[i]);

                    ColumnsWithTypeAndName slice_args_left(
                        {{ColumnWithTypeAndName(tuple_columns[i], arg_type, "array")},
                         {DataTypeUInt8().createColumnConst(1, toField(UInt8(1))), std::make_shared<DataTypeUInt8>(), ""},
                         slice_index_len});

                    ColumnsWithTypeAndName slice_args_right(
                        {{ColumnWithTypeAndName(tuple_columns[i], arg_type, "array")}, slice_index});
                    ColumnWithTypeAndName arr_left{
                        fun_slice->build(slice_args_left)->execute(slice_args_left, arg_type, input_rows_count, /* dry_run = */ false), arg_type, ""};
                    ColumnWithTypeAndName arr_right{
                        fun_slice->build(slice_args_right)->execute(slice_args_right, arg_type, input_rows_count, /* dry_run = */ false), arg_type, ""};

                    ColumnsWithTypeAndName arr_cancat({arr_right, arr_left});
                    auto out_tmp = array_concat_func
                                       ->build(arr_cancat)
                                       ->execute(arr_cancat, arg_type, input_rows_count, /* dry_run = */ false);
                    adjusted_columns[i] = std::move(out_tmp);
                }
                return ColumnTuple::create(adjusted_columns);
            }
        }
        return ColumnTuple::create(tuple_columns);
    }

private:
    String function_name;
    FunctionOverloadResolverPtr length_func;
    FunctionOverloadResolverPtr array_resize_func;
    FunctionOverloadResolverPtr array_zip_func;
    FunctionOverloadResolverPtr sort_func;
    FunctionOverloadResolverPtr tuple_element_func;
    FunctionOverloadResolverPtr index_of_func;
    FunctionOverloadResolverPtr array_slice_func;
    FunctionOverloadResolverPtr array_concat_func;
};

REGISTER_FUNCTION(KqlArraySort)
{
    FunctionDocumentation::Description description_asc = R"(
Sorts one or more arrays in ascending order. The first array is sorted, and subsequent arrays are reordered to match the first array's sorted order. Null values are placed at the end. This is a KQL (Kusto Query Language) compatibility function.
    )";
    FunctionDocumentation::Syntax syntax_asc = "kql_array_sort_asc(array1[, array2, ..., nulls_last])";
    FunctionDocumentation::Arguments arguments_asc = {
        {"array1", "The array to sort.", {"Array(T)"}},
        {"array2", "Optional. Additional arrays to reorder according to array1's sort order.", {"Array(T)"}},
        {"nulls_last", "Optional. A boolean indicating whether nulls should appear last. Default is true.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_asc = {"Returns a tuple of arrays sorted in ascending order.", {"Tuple(Array, ...)"}};
    FunctionDocumentation::Examples examples_asc = {{"Basic usage", "SELECT kql_array_sort_asc([3, 1, 2])", "([1, 2, 3])"}};
    FunctionDocumentation::IntroducedIn introduced_in_asc = {23, 10};
    FunctionDocumentation::Category category_asc = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_asc = {description_asc, syntax_asc, arguments_asc, {}, returned_value_asc, examples_asc, introduced_in_asc, category_asc};

    factory.registerFunction("kql_array_sort_asc", [](ContextPtr context) -> FunctionPtr
    {
        return std::make_shared<FunctionKqlArraySort>(context, "kql_array_sort_asc", /* is_desc = */ false);
    }, documentation_asc);

    FunctionDocumentation::Description description_desc = R"(
Sorts one or more arrays in descending order. The first array is sorted, and subsequent arrays are reordered to match the first array's sorted order. Null values are placed at the end. This is a KQL (Kusto Query Language) compatibility function.
    )";
    FunctionDocumentation::Syntax syntax_desc = "kql_array_sort_desc(array1[, array2, ..., nulls_last])";
    FunctionDocumentation::Arguments arguments_desc = {
        {"array1", "The array to sort.", {"Array(T)"}},
        {"array2", "Optional additional arrays to reorder according to array1's sort order.", {"Array(T)"}},
        {"nulls_last", "Optional boolean indicating whether nulls should appear last. Default is true.", {"UInt8"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_desc = {"Returns a tuple of arrays sorted in descending order.", {"Tuple(Array, ...)"}};
    FunctionDocumentation::Examples examples_desc = {{"Basic usage", "SELECT kql_array_sort_desc([3, 1, 2])", "([3, 2, 1])"}};
    FunctionDocumentation::IntroducedIn introduced_in_desc = {23, 10};
    FunctionDocumentation::Category category_desc = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_desc = {description_desc, syntax_desc, arguments_desc, {}, returned_value_desc, examples_desc, introduced_in_desc, category_desc};

    factory.registerFunction("kql_array_sort_desc", [](ContextPtr context) -> FunctionPtr
    {
        return std::make_shared<FunctionKqlArraySort>(context, "kql_array_sort_desc", /* is_desc = */ true);
    }, documentation_desc);
}

}
