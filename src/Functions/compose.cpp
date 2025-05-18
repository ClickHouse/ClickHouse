#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>

#include <Common/Exception.h>
#include "Core/ColumnWithTypeAndName.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "DataTypes/IDataType.h"

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
class ComposeCallbackExecutableFunction : public IExecutableFunction
{
public:
    ComposeCallbackExecutableFunction(ColumnPtr f_column_, ColumnPtr g_column_)
        :  f_column(f_column_), g_column(g_column_)
    {
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    String getName() const override { return "compose"; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        ColumnWithTypeAndName lambda_res;
        {
            auto replicated_column_function_ptr = f_column->cloneResized(arguments[0].column->size());
            auto& column_function = typeid_cast<ColumnFunction &>(*replicated_column_function_ptr);

            column_function.appendArguments(arguments);

            lambda_res = column_function.reduce();
        }

        {
            auto replicated_column_function_ptr = g_column->cloneResized(lambda_res.column->size());
            auto& column_function = typeid_cast<ColumnFunction &>(*replicated_column_function_ptr);

            column_function.appendArguments({lambda_res});

            auto lambda_result = column_function.reduce();

            return lambda_result.column;
        }
    }

private:
    ColumnPtr f_column;
    ColumnPtr g_column;
};


class ComposeCallbackFunction : public IFunctionBase
{
public:
    ComposeCallbackFunction(
        ColumnPtr f_column_,
        ColumnPtr g_column_,
        DataTypes arg_types_,
        DataTypePtr return_type_
    )
        : f_column(f_column_), g_column(g_column_), argument_types(arg_types_), return_type(return_type_)
    {
    }

    String getName() const override { return "compose"; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return false;
    }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_shared<ComposeCallbackExecutableFunction>(f_column, g_column);
    }

private:
    ColumnPtr f_column;
    ColumnPtr g_column;
    DataTypes argument_types;
    DataTypePtr return_type;
};


class ComposeFunction : public IFunction
{
public:
    static constexpr auto name = "compose";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<ComposeFunction>();
    }

    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "compose function accepts 2 arguments, got {}",
                arguments.size());
        }

        const auto * f_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        const auto * g_type = checkAndGetDataType<DataTypeFunction>(arguments[1].get());

        if (!f_type || !g_type)
        {
            throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Both arguments in compose function should be lambdas or compose functions"
            );
        }

        if (g_type->getArgumentTypes().size() != 1)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Expected 1 argument in the right function inside compose, got {}",
                g_type->getArgumentTypes().size());
        }

        if (!f_type->getReturnType()->equals(*g_type->getArgumentTypes()[0]))
        {
            throw Exception(ErrorCodes::TYPE_MISMATCH, "Output type of first function must be compatible with input type of second function");
        }


        return std::make_shared<DataTypeFunction>(
            f_type->getArgumentTypes(),
            g_type->getReturnType()
        );
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeFunction>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments.size() != 2)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "compose function accepts 2 arguments, got {}",
                arguments.size());
        }

        const auto * result_func_type = checkAndGetDataType<DataTypeFunction>(result_type.get());
        if (!result_func_type)
        {
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "Result type of compose must be a function");
        }
        const auto & arg_types = result_func_type->getArgumentTypes();
        const auto & inner_result_type = result_func_type->getReturnType();

        auto composition = std::make_shared<ComposeCallbackFunction>(
            arguments[0].column->convertToFullColumnIfConst(),
            arguments[1].column->convertToFullColumnIfConst(),
            arg_types,
            inner_result_type
        );

        return ColumnConst::create(ColumnFunction::create(1, composition, ColumnsWithTypeAndName{}), input_rows_count);
    }
};


}

REGISTER_FUNCTION(Compose)
{
    factory.registerFunction<ComposeFunction>({}, FunctionFactory::Case::Sensitive);
}

}
