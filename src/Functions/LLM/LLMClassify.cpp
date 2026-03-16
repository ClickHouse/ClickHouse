#include <Functions/LLM/LLMFunctionBase.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionLLMClassify final : public LLMFunctionBase
{
public:
    static constexpr auto name = "LLMClassify";
    static FunctionPtr create(ContextPtr ctx) { return std::make_shared<FunctionLLMClassify>(std::move(ctx)); }
    explicit FunctionLLMClassify(ContextPtr ctx) : LLMFunctionBase(std::move(ctx)) {}

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2 || arguments.size() > 4)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2-4 arguments: [collection,] text, categories[, temperature]", name);
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    }

protected:
    String functionName() const override { return name; }
    float defaultTemperature() const override { return 0.0f; }

    String buildSystemPrompt(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t idx = getFirstDataArgIndex(arguments);
        const auto * cat_col = checkAndGetColumn<ColumnConst>(arguments[idx + 1].column.get());
        if (!cat_col)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Categories argument must be a constant array");

        auto field = (*cat_col->getDataColumnPtr())[0];
        const auto & arr = field.safeGet<Array>();
        String categories;
        for (size_t i = 0; i < arr.size(); ++i)
        {
            if (i > 0) categories += ", ";
            categories += arr[i].safeGet<String>();
        }
        return "You are a text classifier. Classify the given text into exactly one of these categories: "
            + categories
            + ". Respond with ONLY the category label, nothing else.";
    }

    String buildUserMessage(const ColumnsWithTypeAndName & arguments, size_t row) const override
    {
        size_t idx = getFirstDataArgIndex(arguments);
        return String(arguments[idx].column->getDataAt(row));
    }

    String buildResponseFormatJSON(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t idx = getFirstDataArgIndex(arguments);
        const auto * cat_col = checkAndGetColumn<ColumnConst>(arguments[idx + 1].column.get());
        if (!cat_col)
            return "";

        auto field = (*cat_col->getDataColumnPtr())[0];
        const auto & arr = field.safeGet<Array>();

        String enum_values;
        for (size_t i = 0; i < arr.size(); ++i)
        {
            if (i > 0) enum_values += ",";
            enum_values += "\"" + arr[i].safeGet<String>() + "\"";
        }

        return R"({"type":"json_schema","json_schema":{"name":"classification","strict":true,"schema":{"type":"object","properties":{"category":{"type":"string","enum":[)"
            + enum_values
            + R"(]}},"required":["category"],"additionalProperties":false}}})";
    }

    String postProcessResponse(const String & raw_response) const override
    {
        if (raw_response.empty())
            return raw_response;
        if (raw_response.front() == '{')
        {
            auto pos = raw_response.find("\"category\"");
            if (pos != String::npos)
            {
                auto val_start = raw_response.find('"', pos + 10);
                if (val_start != String::npos)
                {
                    val_start++;
                    auto val_end = raw_response.find('"', val_start);
                    if (val_end != String::npos)
                        return raw_response.substr(val_start, val_end - val_start);
                }
            }
        }
        return raw_response;
    }
};

}

REGISTER_FUNCTION(LLMClassify)
{
    factory.registerFunction<FunctionLLMClassify>(FunctionDocumentation{
        .description = "Classifies input text into one of the provided categories using an LLM.",
        .syntax = "LLMClassify([collection,] text, categories[, temperature])",
        .arguments = {{"text", "Input text to classify"}, {"categories", "Array of category labels"}},
        .returned_value = {"The category label from the provided array.", {"String"}},
        .examples = {{"basic", "SELECT LLMClassify(body, ['positive', 'negative']) FROM reviews", ""}},
        .category = FunctionDocumentation::Category::Other});
}

}
