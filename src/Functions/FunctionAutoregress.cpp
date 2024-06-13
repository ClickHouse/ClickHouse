#include <Columns/ColumnFunction.h>
#include <Common/assert_cast.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/TypeId.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

inline constexpr auto function_name = "autoregress";

void checkArguments(const DataTypes & arguments)
{
    if (arguments[0]->getTypeId() != TypeIndex::Function)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "The argument 1 of the function '{}' should be lambda expression",
            function_name);
    }
    if (arguments[1]->getTypeId() != TypeIndex::UInt8)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "The argument 2 of the function '{}' should be UInt8",
            function_name);
    }
    if (arguments[2]->getTypeId() != TypeIndex::Int64 && arguments[2]->getTypeId() != TypeIndex::Float64)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "The argument 3 of the function '{}' should be Int64 or Float64",
            function_name);
    }
}

class ExecutableFunctionAutoregress final : public IExecutableFunction
{
public:
    String getName() const override { return function_name; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto res = result_type->createColumn();
        if (input_rows_count == 0)
        {
            return res;
        }

        size_t backward_step = arguments[1].column->getUInt(0);

        const ColumnFunction * column_function = assert_cast<const ColumnFunction *>(arguments[0].column.get());
        assert(column_function);

        auto lambda_inputs = arguments[2].column->cut(0, std::min(backward_step, input_rows_count));
        String lambda_arg_name {"x"};
        for (size_t i = 0; i < input_rows_count; i+=backward_step)
        {
            auto block_size = lambda_inputs->size();
            auto block_rows = column_function->cut(i, block_size);
            ColumnFunction * function_block_rows = const_cast<ColumnFunction *>(assert_cast<const ColumnFunction *>(block_rows.get()));
            ColumnsWithTypeAndName lambda_args;
            lambda_args.emplace_back(lambda_inputs, arguments[2].type, lambda_arg_name);
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
            auto block_res_col = block_res.column->convertToFullIfNeeded();
            res->insertRangeFrom(*block_res_col, 0, block_size);
            if (input_rows_count > i + backward_step) [[likely]]
            {
                lambda_inputs = block_res_col->cut(0, std::min(backward_step, input_rows_count-i-backward_step));
            }
        }
        return res;
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
        arguments[0] = std::make_shared<DataTypeFunction>(DataTypes{arguments[2]}, arguments[2]);
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
        return arguments[2];
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
