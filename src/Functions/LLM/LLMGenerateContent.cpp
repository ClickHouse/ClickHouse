#include <Functions/LLM/LLMFunctionBase.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FunctionLLMGenerateContent final : public LLMFunctionBase
{
public:
    static constexpr auto name = "LLMGenerateContent";
    static FunctionPtr create(ContextPtr ctx) { return std::make_shared<FunctionLLMGenerateContent>(std::move(ctx)); }
    explicit FunctionLLMGenerateContent(ContextPtr ctx) : LLMFunctionBase(std::move(ctx)) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.empty() || arguments.size() > 4)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 1-4 arguments: [collection,] prompt[, system_prompt][, temperature]", name);

        if (hasNamedCollectionArg(arguments) && arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} with a named collection as first argument requires at least a prompt argument", name);

        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

protected:
    String functionName() const override { return name; }
    float defaultTemperature() const override { return 0.7f; }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t idx = getFirstDataArgIndex(arguments);

        if (arguments.size() > idx + 1 && isString(arguments[idx + 1].type))
        {
            String system_prompt(arguments[idx + 1].column->getDataAt(0));
            if (!system_prompt.empty())
                return system_prompt;
        }
        return "You are a helpful assistant. Provide a clear and concise response.";
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        size_t idx = getFirstDataArgIndex(arguments);
        return String(arguments[idx].column->getDataAt(row));
    }
};

}

REGISTER_FUNCTION(LLMGenerateContent)
{
    factory.registerFunction<FunctionLLMGenerateContent>(FunctionDocumentation{
        .description = "Generates text content from a prompt using an LLM.",
        .syntax = "LLMGenerateContent([collection,] prompt[, system_prompt][, temperature])",
        .arguments = {{"prompt", "The user prompt or question"}, {"system_prompt", "Optional system prompt to guide generation"}},
        .returned_value = {"Generated text as String.", {"String"}},
        .examples = {{"basic", "SELECT LLMGenerateContent('Explain what ClickHouse is in one sentence')", ""}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::Other});
}

}
