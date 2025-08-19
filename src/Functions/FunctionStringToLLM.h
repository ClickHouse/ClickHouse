#pragma once

#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}


template <typename Impl, typename Name>
class FunctionStringToLLM : public IFunction
{
    ContextPtr context {};
public:
    static constexpr auto name = Name::name;
    explicit FunctionStringToLLM(ContextPtr context_) : IFunction(), context(context_) {}
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionStringToLLM>(context_);
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 3;
    }

    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 3.",
                getName(), arguments.size());

        if (!isStringOrFixedString(arguments[2]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[2]->getName(), getName());

        //return arguments[0];
        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        /// In case of default implementation for Dynamic always return String even for FixedString types
        /// to avoid Dynamic result of this function.
        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (arguments.size() < 3)
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Number of arguments for function {} doesn't match: passed {}, should be at least 3.",
                getName(), arguments.size());
        const ColumnConst * model_json_detail = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!model_json_detail)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be constant string: "
                "detail of model.", getName());

        const ColumnConst * prompt_json_detail = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());
        if (!prompt_json_detail)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument for function {} must be constant string: "
                "detail of prompt.", getName());

        auto model_string = model_json_detail->getValue<String>();
        auto prompt_string = prompt_json_detail->getValue<String>();
        const ColumnPtr column = arguments[2].column;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(column.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vector(context, model_string, prompt_string, col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), input_rows_count);
            return col_res;
        }
#if 0
        if (const ColumnFixedString * col_fixed = checkAndGetColumn<ColumnFixedString>(column.get()))
        {
            auto col_res = ColumnString::create();
            Impl::vectorFixed(context, model_string, prompt_string, col_fixed->getChars(), col_fixed->getN(), col_res->getChars(), col_res->getOffsets(), input_rows_count);
            return col_res;
        }
#endif
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[2].column->getName(), getName());
    }
};

struct RemoteLLMUtils
{
    static String process(const ContextPtr context, const String & endpoint, const String & prompt, const String & result_name);
};
}
