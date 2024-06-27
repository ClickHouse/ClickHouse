#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

#include <algorithm>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

inline constexpr auto function_name = "autoregress";

[[noreturn]] void throwIllegalTypeArgOffsets(DataTypePtr data_type)
{
    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "The argument 2 'offsets' of the function '{}' should be single UInt8 or Tuple(UInt8, UInt8,...), but got {}",
        function_name,
        data_type->getName());
}

[[noreturn]] void throwIllegalTypeArgInitialValues(DataTypePtr data_type)
{
    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "The argument 3 'initial values' of the function '{}' should be single Float64 or Tuple(Float64,Float64,...),but got {}",
        function_name,
        data_type->getName());
}

void checkArguments(const DataTypes & arguments)
{
    if (arguments[0]->getTypeId() != TypeIndex::Function)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "The argument 1 of the function '{}' should be lambda expression",
            function_name);
    }

    size_t offsets_size = 1;
    WhichDataType arg_offsets_type{arguments[1]};
    if (!arg_offsets_type.isUInt8())
    {
        const auto * offsets_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].get());
        if (!offsets_tuple)
        {
            throwIllegalTypeArgOffsets(arguments[1]);
        }

        for (const auto & sub_type : offsets_tuple->getElements())
        {
            if (sub_type->getTypeId() != TypeIndex::UInt8)
            {
                throwIllegalTypeArgOffsets(arguments[1]);
            }
        }
        offsets_size = offsets_tuple->getElements().size();
    }

    size_t init_values_size = 1;
    WhichDataType arg_init_values_type{arguments[2]};
    if (!arg_init_values_type.isFloat64())
    {
        const auto * init_values_tuple = checkAndGetDataType<DataTypeTuple>(arguments[2].get());
        if (!init_values_tuple)
        {
            throwIllegalTypeArgInitialValues(arguments[2]);
        }
        for (const auto & sub_type : init_values_tuple->getElements())
        {
            if (sub_type->getTypeId() != TypeIndex::Float64)
            {
                throwIllegalTypeArgInitialValues(arguments[2]);
            }
        }
        init_values_size = init_values_tuple->getElements().size();
    }

    const DataTypeFunction * lambda = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
    const DataTypes & lambda_args = lambda->getArgumentTypes();
    if (lambda_args.size() != offsets_size || lambda_args.size() != init_values_size)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The input arguments of the lambda expression should be equal to the number of elements in the tuple of argument 2 and the number of elements in the tuple of argument 3 for function '{}', but got: number of lambda input arguments = {}, number of elements in the tuple of argument 2 = {}, number of elements in the tuple of argument 3 = {}",
            function_name,
            lambda_args.size(),
            offsets_size,
            init_values_size);
    }
}

class ExecutableFunctionAutoregress final : public IExecutableFunction
{
public:
    String getName() const override { return function_name; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto column_res = result_type->createColumn();
        if (input_rows_count == 0)
        {
            return column_res;
        }

        Field offsets_field;
        arguments[1].column->get(0, offsets_field);
        ColumnNumbers offsets;
        Tuple offsets_tuple;
        size_t single_offset;
        if (offsets_field.tryGet<Tuple>(offsets_tuple))
        {
            for (const auto & offset_field : offsets_tuple)
            {
                offsets.push_back(offset_field.get<size_t>());
            }
        }
        else if (offsets_field.tryGet<size_t>(single_offset))
        {
            offsets.push_back(single_offset);
        }
        else
        {
            throwIllegalTypeArgOffsets(arguments[1].type);
        }

        if (std::ranges::any_of(offsets, [](auto x) { return x < 1; }))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The offsets specified by argument 2 of function '{}' should be at least 1",
                function_name);
        }

        auto common_col_init_values = arguments[2].column->convertToFullIfNeeded();
        ColumnPtr col_tuple_init;
        const auto * column_init_values = checkAndGetColumn<ColumnTuple>(common_col_init_values.get());
        if (!column_init_values)
        {
            if (checkAndGetColumn<ColumnFloat64>(common_col_init_values.get()) ||
                checkAndGetColumnConstData<ColumnFloat64>(common_col_init_values.get()))
            {
                col_tuple_init = ColumnTuple::create(Columns{common_col_init_values});
                column_init_values = assert_cast<const ColumnTuple *>(col_tuple_init.get());
            }
            else
            {
                throwIllegalTypeArgInitialValues(arguments[2].type);
            }
        }

        Columns col_lambda_arguments;
        col_lambda_arguments.resize(offsets.size());
        for (size_t i = 0; i < col_lambda_arguments.size(); ++i)
        {
            col_lambda_arguments[i] = column_init_values->getColumn(i).cut(0, 1);
        }
        const auto * column_function = assert_cast<const ColumnFunction *>(arguments[0].column.get());
        const auto lambda_arg_type = std::make_shared<DataTypeFloat64>();
        for (size_t i = 0; i < input_rows_count; i+=1)
        {
            auto column_block = column_function->cut(i, 1);
            ColumnFunction * function_block_rows = const_cast<ColumnFunction *>(assert_cast<const ColumnFunction *>(column_block.get()));
            ColumnsWithTypeAndName lambda_args;
            for (auto & col_lambda_arg : col_lambda_arguments)
            {
                lambda_args.emplace_back(col_lambda_arg, lambda_arg_type, "");
            }
            function_block_rows->appendArguments(lambda_args);
            auto block_res = function_block_rows->reduce();
            if (!block_res.type->equals(*result_type)) [[unlikely]]
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The argument 1 of lambda expresson of the function '{}' must return the same data type with argument 3, argument 1 is {}, but argument 3 is {}",
                    function_name,
                    block_res.type->getName(),
                    result_type->getName());
            }

            column_res->insertRangeFrom(*block_res.column, 0, 1);
            auto next_i = i+1;
            if (next_i < input_rows_count)
            {
                for (size_t j = 0; j < col_lambda_arguments.size(); ++j)
                {
                    if (next_i >= offsets[j])
                    {
                        col_lambda_arguments[j] = column_res->cut(next_i-offsets[j], 1);
                    }
                    else
                    {

                        col_lambda_arguments[j] = column_init_values->getColumn(j).cut(next_i, 1);
                    }
                }
            }
        }
        return column_res;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

private:
    Field state;
};

class FunctionAutoregress final : public IFunctionBase
{

public:
    explicit FunctionAutoregress(DataTypes argument_types_, DataTypePtr return_type_)
        : executable_function{std::make_shared<ExecutableFunctionAutoregress>()}
        , argument_types{argument_types_}
        , return_type{return_type_}
    { }

    String getName() const override { return function_name; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        return executable_function;
    }

    bool isStateful() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

private:
    ExecutableFunctionPtr executable_function{};
    DataTypes argument_types;
    DataTypePtr return_type;
};

class FunctionAutoregressResolver final : public IFunctionOverloadResolver
{
public:
    static FunctionOverloadResolverPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<FunctionAutoregressResolver>();
    }

    static constexpr auto name = function_name;

    String getName() const override
    {
        return function_name;
    }

    size_t getNumberOfArguments() const override { return 3; }

    bool isStateful() const override { return true; }

    void getLambdaArgumentTypesImpl(DataTypes & arguments) const override
    {
        checkArguments(arguments);

        const auto * init_values_tuple = checkAndGetDataType<DataTypeTuple>(arguments[2].get());
        DataTypes lambda_arg_types;
        auto lambda_arg_type = std::make_shared<DataTypeFloat64>();
        auto lambda_return_type = lambda_arg_type;
        if (init_values_tuple)
        {
            for (size_t i = 0; i < init_values_tuple->getElements().size(); ++i)
            {
                lambda_arg_types.push_back(lambda_arg_type);
            }
        }
        else
        {
            lambda_arg_types.push_back(lambda_arg_type);
        }
        arguments[0] = std::make_shared<DataTypeFunction>(lambda_arg_types, lambda_return_type);
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

protected:

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName &  arguments, const DataTypePtr & result_type) const override
    {
        DataTypes argument_types = {arguments[0].type, arguments[1].type, arguments[2].type};
        checkArguments(argument_types);
        return std::make_shared<FunctionAutoregress>(argument_types, result_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkArguments(arguments);
        const auto * lambda = assert_cast<const DataTypeFunction *>(arguments[0].get());
        return lambda->getReturnType();
    }
};

REGISTER_FUNCTION(Autoregress)
{
    factory.registerFunction<FunctionAutoregressResolver>(
        FunctionDocumentation{
            .description = "Calculates autoregressive function. The Autoregressive (AR) model is a fundamental component in the realm of time series analysis and forecasting.",
            .syntax = "autoregress(x->{ar_expr}, backward_offset, initial_value)",
            .arguments = {
                {"ar_expr","Autoregressive function. Lambda."},
                {"backward_offset", "autoregressive need `T-n` result which is calculated previously, the argument specifies the `n`. UInt64."},
                {"initial_value", "Initial values used for autoregressive. Float64."}
            },
            .returned_value = "Autoregressive function result.",
            .examples = {
                {"autoregress", "select groupArray(autoregress(x-> toFloat64(x+1.25), 1, toFloat64(100))) from numbers(5);", "[101.25,102.5,103.75,105,106.25]"}
            },
            .categories = {"autoregressive"}
        },
        FunctionFactory::CaseInsensitive);
}

}
