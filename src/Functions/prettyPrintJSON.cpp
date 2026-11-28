#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromVector.h>
#include <Interpreters/Context.h>
#include "config.h"

#if USE_RAPIDJSON

/// Prevent stack overflow:
#define RAPIDJSON_PARSE_DEFAULT_FLAGS (kParseIterativeFlag)

#include <rapidjson/reader.h>
#include <rapidjson/memorystream.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/error/en.h>

#include <Common/JSONParsers/RapidJSONMemoryTrackerAllocator.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// rapidjson types whose internal stacks and output buffer are accounted against the memory
/// tracker, so a pathological input cannot allocate without bound (see RapidJSONMemoryTrackerAllocator).
using TrackedStringBuffer = rapidjson::GenericStringBuffer<rapidjson::UTF8<char>, RapidJSONMemoryTrackerAllocator>;
using TrackedPrettyWriter
    = rapidjson::PrettyWriter<TrackedStringBuffer, rapidjson::UTF8<char>, rapidjson::UTF8<char>, RapidJSONMemoryTrackerAllocator>;
using TrackedReader = rapidjson::GenericReader<rapidjson::UTF8<char>, rapidjson::UTF8<char>, RapidJSONMemoryTrackerAllocator>;

/// The JSON text of each row, as `toJSONString` writes it.
ColumnPtr serializeToJSONText(const ColumnWithTypeAndName & argument, const FormatSettings & format_settings, size_t input_rows_count)
{
    auto res = ColumnString::create();
    ColumnString::Chars & data_to = res->getChars();
    ColumnString::Offsets & offsets_to = res->getOffsets();
    offsets_to.resize(input_rows_count);

    auto serialization = argument.type->getDefaultSerialization();
    WriteBufferFromVector<ColumnString::Chars> json(data_to);
    for (size_t i = 0; i < input_rows_count; ++i)
    {
        serialization->serializeTextJSON(*argument.column, i, json, format_settings);
        offsets_to[i] = json.count();
    }
    json.finalize();
    return res;
}

class FunctionPrettyPrintJSON : public IFunction
{
public:
    static constexpr auto name = "prettyPrintJSON";
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionPrettyPrintJSON>(context); }

    explicit FunctionPrettyPrintJSON(ContextPtr context)
        : format_settings(getFormatSettings(context))
        , format_settings_hash(getFormatSettingsHash(context->getSettingsRef()))
    {
    }

    [[nodiscard]] String getName() const override { return name; }
    void updateHash(SipHash & hash) const override { hash.update(format_settings_hash); }
    [[nodiscard]] bool isVariadic() const override { return true; }
    [[nodiscard]] size_t getNumberOfArguments() const override { return 0; }
    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return true; }
    /// An empty string is not a JSON document.
    [[nodiscard]] bool canBeExecutedOnDefaultArguments() const override { return false; }
    [[nodiscard]] ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"json", nullptr, nullptr, "Any"}
        };
        FunctionArgumentDescriptors optional_args{
            {"indent", &isNativeUInt, isColumnConst, "const UInt*"}
        };
        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeString>();
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        ColumnPtr json_text = arguments[0].column;
        if (!isString(arguments[0].type))
            json_text = serializeToJSONText(arguments[0], format_settings, input_rows_count);

        const auto * col = checkAndGetColumn<ColumnString>(json_text.get());
        if (!col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be a String column", getName());

        unsigned indent_count = 4;
        if (arguments.size() > 1)
        {
            UInt64 indent_value = assert_cast<const ColumnConst &>(*arguments[1].column).getValue<UInt64>();
            if (indent_value > 32)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Indent parameter of function {} must be between 0 and 32, got {}",
                    getName(),
                    indent_value);
            indent_count = static_cast<unsigned>(indent_value);
        }

        auto result = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto str_view = col->getDataAt(i);

            /// Since RapidJSON uses '\0' as end-of-stream char in its stream abstraction,
            /// we have to default to this check to prevent silent truncation of the input
            /// unescaped '\0' is not valid in JSON strings anyway
            if (str_view.contains('\0'))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid JSON string in function {}: embedded NULL byte",
                    getName());

            TrackedStringBuffer buffer;
            TrackedPrettyWriter writer(buffer);
            writer.SetIndent(' ', indent_count);

            /// Stream JSON directly from Reader to PrettyWriter without building
            /// a DOM. Both Reader (with kParseIterativeFlag) and PrettyWriter use
            /// heap-allocated stacks, so arbitrarily deep nesting is safe. The stacks and the
            /// output buffer are accounted against the memory tracker, so an input that would
            /// produce an enormous output is rejected with MEMORY_LIMIT_EXCEEDED.
            rapidjson::MemoryStream ms(str_view.data(), str_view.size());
            TrackedReader reader;
            auto parse_result = reader.Parse(ms, writer);

            if (parse_result.IsError())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid JSON string in function {}: {}",
                    getName(),
                    rapidjson::GetParseError_En(parse_result.Code()));

            result->insertData(buffer.GetString(), buffer.GetSize());
        }

        return result;
    }

private:
    const FormatSettings format_settings;
    const UInt64 format_settings_hash;
};

}

REGISTER_FUNCTION(PrettyPrintJSON)
{
    FunctionDocumentation::Description description = R"(
Returns a pretty-printed version of a JSON string with newlines and indentation with spaces.
The argument can also be of another type, such as `JSON`, `Dynamic` or `Array`.
Such a value is converted to JSON first, as `toJSONString` does, so `prettyPrintJSON(j)` returns the same as `prettyPrintJSON(j::String)` for a `JSON` value `j`.
    )";
    FunctionDocumentation::Syntax syntax = "prettyPrintJSON(json [, indent])";
    FunctionDocumentation::Arguments arguments = {
        {"json", "JSON text as a `String`, or a value of any other type.", {"String", "JSON", "Dynamic", "Any"}},
        {"indent", "Number of spaces per indentation level. Default: 4. Max: 32", {"UInt*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"A pretty-printed JSON string.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Simple object",
            R"(SELECT prettyPrintJSON('{"a":1,"b":"hello"}');)",
            R"(
{\n    "a": 1,\n    "b": "hello"\n}
            )"
        },
        {
            "Custom indent",
            R"(SELECT prettyPrintJSON('{"a":1}', 8);)",
            R"(
{\n        "a": 1\n}
            )"
        },
        {
            "JSON value",
            R"(SELECT prettyPrintJSON('{"a":{"b":"x"},"d":[1,2]}'::JSON);)",
            R"(
{\n    "a": {\n        "b": "x"\n    },\n    "d": [\n        1,\n        2\n    ]\n}
            )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionPrettyPrintJSON>(documentation);
}

}

#endif
