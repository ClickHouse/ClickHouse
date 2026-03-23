#include <Functions/FunctionBaseLLM.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FunctionLLMTranslate final : public FunctionBaseLLM
{
public:
    static constexpr auto name = "llmTranslate";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionLLMTranslate>(context); }
    explicit FunctionLLMTranslate(ContextPtr context) : FunctionBaseLLM(context) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2 || arguments.size() > 5)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2-5 arguments: [collection,] text, target_language[, instructions][, temperature]", name);
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

protected:
    String functionName() const override { return name; }
    float defaultTemperature() const override { return 0.3f; }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t idx = getFirstDataArgIndex(arguments);
        String target_lang(arguments[idx + 1].column->getDataAt(0));
        String prompt = "Translate the following text into " + target_lang + ". Return only the translation, nothing else.";

        if (arguments.size() > idx + 2)
        {
            String instructions(arguments[idx + 2].column->getDataAt(0));
            if (!instructions.empty())
                prompt += " Additional instructions: " + instructions;
        }
        return prompt;
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        size_t idx = getFirstDataArgIndex(arguments);
        return String(arguments[idx].column->getDataAt(row));
    }
};

}

REGISTER_FUNCTION(llmTranslate)
{
    factory.registerFunction<FunctionLLMTranslate>(FunctionDocumentation{
        .description = "Translates input text into the specified target language using an LLM.",
        .syntax = "llmTranslate([collection,] text, target_language[, instructions][, temperature])",
        .arguments = {{"text", "Input text"}, {"target_language", "Target language name or BCP-47 code"}},
        .returned_value = {"Translated text as String.", {"String"}},
        .examples = {{"basic", "SELECT llmTranslate(body, 'French') FROM articles", ""}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});
}

}
