#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/Kusto/KqlFunctionBase.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

template <typename Name, bool is_desc>
class FunctionKqlArraySort : public KqlFunctionBase
{
public:
    static constexpr auto name = Name::name;
    explicit FunctionKqlArraySort(ContextPtr context_) : context(context_) { }
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionKqlArraySort>(context); }

    String getName() const override { return name; }

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

        if (!isArray(arguments.at(array_count - 1).type))
            --array_count;

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
            null_last = check_condition(last_arg, context, input_rows_count);
        }

        ColumnsWithTypeAndName new_args;
        ColumnPtr first_array_column;
        std::unordered_set<size_t> null_indices;
        DataTypes nested_types;

        String sort_function = is_desc ? "arrayReverseSort" : "arraySort";

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
                null_indices.insert(i);
            }
            else
                new_args.push_back(arguments[i]);
        }

        auto zipped
            = FunctionFactory::instance().get("arrayZip", context)->build(new_args)->execute(new_args, result_type, input_rows_count);

        ColumnsWithTypeAndName sort_arg({{zipped, std::make_shared<DataTypeArray>(result_type), "zipped"}});
        auto sorted_tuple
            = FunctionFactory::instance().get(sort_function, context)->build(sort_arg)->execute(sort_arg, result_type, input_rows_count);

        auto null_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt8>());

        Columns tuple_columns(array_count);
        size_t sorted_index = 0;
        for (size_t i = 0; i < array_count; ++i)
        {
            if (null_indices.contains(i))
            {
                auto fun_array = FunctionFactory::instance().get("array", context);

                DataTypePtr arg_type
                    = std::make_shared<DataTypeArray>(makeNullable(nested_types[i]));

                ColumnsWithTypeAndName null_array_arg({
                    {null_type->createColumnConstWithDefaultValue(input_rows_count), null_type, "NULL"},
                });

                tuple_columns[i] = fun_array->build(null_array_arg)->execute(null_array_arg, arg_type, input_rows_count);
                tuple_columns[i] = tuple_columns[i]->convertToFullColumnIfConst();
            }
            else
            {
                ColumnsWithTypeAndName untuple_args(
                    {{ColumnWithTypeAndName(sorted_tuple, std::make_shared<DataTypeArray>(result_type), "sorted")},
                     {DataTypeUInt8().createColumnConst(1, toField(UInt8(sorted_index + 1))), std::make_shared<DataTypeUInt8>(), ""}});
                auto tuple_coulmn = FunctionFactory::instance()
                                        .get("tupleElement", context)
                                        ->build(untuple_args)
                                        ->execute(untuple_args, result_type, input_rows_count);

                auto out_tmp = ColumnArray::create(nested_types[i]->createColumn());

                size_t array_size = tuple_coulmn->size();
                const auto & arr = checkAndGetColumn<ColumnArray>(*tuple_coulmn);

                for (size_t j = 0; j < array_size; ++j)
                {
                    Field arr_field;
                    arr.get(j, arr_field);
                    out_tmp->insert(arr_field);
                }

                tuple_columns[i] = std::move(out_tmp);

                ++sorted_index;
            }
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
            slice_index.column = FunctionFactory::instance()
                                     .get("indexOf", context)
                                     ->build(indexof_args)
                                     ->execute(indexof_args, result_type, input_rows_count);

            auto null_index_in_array = slice_index.column->get64(0);
            if (null_index_in_array > 0)
            {
                ColumnWithTypeAndName slice_index_len{nullptr, null_index_datetype, ""};
                slice_index_len.column = DataTypeUInt64().createColumnConst(1, toField(UInt64(null_index_in_array - 1)));

                auto fun_slice = FunctionFactory::instance().get("arraySlice", context);

                for (size_t i = 0; i < array_count; ++i)
                {
                    if (null_indices.contains(i))
                    {
                        adjusted_columns[i] = std::move(tuple_columns[i]);
                    }
                    else
                    {
                        DataTypePtr arg_type = std::make_shared<DataTypeArray>(nested_types[i]);

                        ColumnsWithTypeAndName slice_args_left(
                            {{ColumnWithTypeAndName(tuple_columns[i], arg_type, "array")},
                             {DataTypeUInt8().createColumnConst(1, toField(UInt8(1))), std::make_shared<DataTypeUInt8>(), ""},
                             slice_index_len});

                        ColumnsWithTypeAndName slice_args_right(
                            {{ColumnWithTypeAndName(tuple_columns[i], arg_type, "array")}, slice_index});
                        ColumnWithTypeAndName arr_left{
                            fun_slice->build(slice_args_left)->execute(slice_args_left, arg_type, input_rows_count), arg_type, ""};
                        ColumnWithTypeAndName arr_right{
                            fun_slice->build(slice_args_right)->execute(slice_args_right, arg_type, input_rows_count), arg_type, ""};

                        ColumnsWithTypeAndName arr_cancat({arr_right, arr_left});
                        auto out_tmp = FunctionFactory::instance()
                                           .get("arrayConcat", context)
                                           ->build(arr_cancat)
                                           ->execute(arr_cancat, arg_type, input_rows_count);
                        adjusted_columns[i] = std::move(out_tmp);
                    }
                }
                return ColumnTuple::create(adjusted_columns);
            }
        }
        return ColumnTuple::create(tuple_columns);
    }

private:
    ContextPtr context;
};

struct NameKqlArraySortAsc
{
    static constexpr auto name = "kql_array_sort_asc";
};

struct NameKqlArraySortDesc
{
    static constexpr auto name = "kql_array_sort_desc";
};

using FunctionKqlArraySortAsc = FunctionKqlArraySort<NameKqlArraySortAsc, false>;
using FunctionKqlArraySortDesc = FunctionKqlArraySort<NameKqlArraySortDesc, true>;

REGISTER_FUNCTION(KqlArraySort)
{
    factory.registerFunction<FunctionKqlArraySortAsc>();
    factory.registerFunction<FunctionKqlArraySortDesc>();
}

}
