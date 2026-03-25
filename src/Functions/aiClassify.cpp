#include <Functions/FunctionBaseAI.h>
#include <Functions/FunctionFactory.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/Exception.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class FunctionAiClassify final : public FunctionBaseAI
{
public:
    static constexpr auto name = "aiClassify";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionAiClassify>(context); }
    explicit FunctionAiClassify(ContextPtr context) : FunctionBaseAI(context) {}

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
        const auto * categories_col = checkAndGetColumn<ColumnConst>(arguments[idx + 1].column.get());
        if (!categories_col)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Categories argument must be a constant array");

        auto field = (*categories_col->getDataColumnPtr())[0];
        const auto & arr = field.safeGet<Array>();
        String categories;
        bool first = true;
        for (const auto & elem : arr)
        {
            if (!first) categories += ", ";
            first = false;
            categories += elem.safeGet<String>();
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
        const auto * categories_col = checkAndGetColumn<ColumnConst>(arguments[idx + 1].column.get());
        if (!categories_col)
            return "";

        auto field = (*categories_col->getDataColumnPtr())[0];
        const auto & arr = field.safeGet<Array>();

        Poco::JSON::Array enum_array;
        for (const auto & elem : arr)
            enum_array.add(elem.safeGet<String>());

        std::ostringstream enum_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        enum_array.stringify(enum_stream);

        return R"({"type":"json_schema","json_schema":{"name":"classification","strict":true,"schema":{"type":"object","properties":{"category":{"type":"string","enum":)"
            + enum_stream.str()
            + R"(}},"required":["category"],"additionalProperties":false}}})";
    }

    String postProcessResponse(const String & raw_response) const override
    {
        if (raw_response.empty())
            return raw_response;
        if (raw_response.front() == '{')
        {
            try
            {
                Poco::JSON::Parser parser;
                auto result = parser.parse(raw_response);
                auto obj = result.extract<Poco::JSON::Object::Ptr>();
                if (obj && obj->has("category"))
                    return obj->getValue<String>("category");
            }
            catch (...) {} // NOLINT(bugprone-empty-catch) Ok: best-effort JSON extraction
        }
        return raw_response;
    }
};

}

REGISTER_FUNCTION(AiClassify)
{
    factory.registerFunction<FunctionAiClassify>(FunctionDocumentation{
        .description = "Classifies input text into one of the provided categories using an LLM.",
        .syntax = "aiClassify([collection,] text, categories[, temperature])",
        .arguments = {{"text", "Input text to classify"}, {"categories", "Array of category labels"}},
        .returned_value = {"The category label from the provided array.", {"String"}},
        .examples = {{"basic", "SELECT aiClassify(body, ['positive', 'negative']) FROM reviews", ""}},
        .introduced_in = {26, 4},
        .category = FunctionDocumentation::Category::AI});
}

}
