#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromVector.h>
#include <Interpreters/Context.h>
#include "config.h"

#if USE_SIMDJSON
#    include <Common/JSONParsers/SimdJSONParser.h>
#elif USE_RAPIDJSON
#    include <Common/JSONParsers/RapidJSONParser.h>
#else
#    include <Common/JSONParsers/DummyJSONParser.h>
#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{
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

    /// JSONArrayLength(json)
    class FunctionJSONArrayLength final : public IFunction
    {
    public:
        static constexpr auto name = "JSONArrayLength";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionJSONArrayLength>(context); }

        explicit FunctionJSONArrayLength(ContextPtr context)
            : format_settings(getFormatSettings(context))
            , format_settings_hash(getFormatSettingsHash(context->getSettingsRef()))
        {
        }

        [[nodiscard]] String getName() const override { return name; }
        void updateHash(SipHash & hash) const override { hash.update(format_settings_hash); }

        [[nodiscard]] bool isVariadic() const override { return false; }
        [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

        [[nodiscard]] size_t getNumberOfArguments() const override { return 1; }
        [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }

        [[nodiscard]] String getSignatureString() const override
        {
            return "(Any) -> Nullable(UInt64)";
        }

        [[nodiscard]] DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
        {
            return makeNullable(std::make_shared<DataTypeUInt64>());
        }

        [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
        {
            const auto & type = arguments[0].type;

            /// The JSON text of a `JSON` value is always an object.
            if (isObject(type))
                return ColumnNullable::create(ColumnUInt64::create(input_rows_count, UInt64(0)), ColumnUInt8::create(input_rows_count, UInt8(1)));

            if (isArray(type))
            {
                const auto & offsets = assert_cast<const ColumnArray &>(*arguments[0].column).getOffsets();
                auto lengths = ColumnUInt64::create(input_rows_count);
                auto & lengths_data = lengths->getData();
                for (size_t i = 0; i < input_rows_count; ++i)
                    lengths_data[i] = offsets[i] - offsets[i - 1];
                return ColumnNullable::create(std::move(lengths), ColumnUInt8::create(input_rows_count, UInt8(0)));
            }

            ColumnPtr column = arguments[0].column;
            if (!isString(type))
                column = serializeToJSONText(arguments[0], format_settings, input_rows_count);

            const ColumnString * col = typeid_cast<const ColumnString *>(column.get());
            if (!col)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function {} must be string", getName());

            auto null_map = ColumnUInt8::create();
            auto data = ColumnUInt64::create();
            null_map->reserve(input_rows_count);
            data->reserve(input_rows_count);

#if USE_SIMDJSON
            SimdJSONParser parser;
            SimdJSONParser::Element element;
#elif USE_RAPIDJSON
            RapidJSONParser parser;
            RapidJSONParser::Element element;
#else
            DummyJSONParser parser;
            DummyJSONParser::Element element;
#endif

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                auto str_view = col->getDataAt(i);
                bool ok = parser.parse(std::move(str_view), element);
                if (!ok || !element.isArray())
                {
                    null_map->insertValue(1);
                    data->insertDefault();
                }
                else
                {
                    auto array = element.getArray();
                    null_map->insertValue(0);
                    data->insertValue(array.size());
                }
            }
            return ColumnNullable::create(std::move(data), std::move(null_map));
        }

    private:
        const FormatSettings format_settings;
        const UInt64 format_settings_hash;
    };

}

REGISTER_FUNCTION(JSONArrayLength)
{
    /// JSONArrayLength documentation
    FunctionDocumentation::Description description = R"(
Returns the number of elements in the outermost JSON array.
The function returns `NULL` if input JSON string is invalid.
For an argument of another type, the value is converted to JSON first, as `toJSONString` does.
So an `Array` returns its length, and a `JSON` value returns `NULL`.
    )";
    FunctionDocumentation::Syntax syntax = "JSONArrayLength(json)";
    FunctionDocumentation::Arguments arguments = {
        {"json", "JSON text as a `String`, or a value of any other type.", {"String", "Array", "JSON", "Dynamic", "Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the number of array elements if `json` is a valid JSON array, otherwise returns `NULL`.", {"Nullable(UInt64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT
    JSONArrayLength(''),
    JSONArrayLength('[1,2,3]');
        )",
        R"(
┌─JSONArrayLength('')─┬─JSONArrayLength('[1,2,3]')─┐
│                ᴺᵁᴸᴸ │                          3 │
└─────────────────────┴────────────────────────────┘
        )"
    },
    {
        "JSON value and its array subcolumn",
        R"(
WITH '{"a":{"b":"x"},"d":[1,2]}'::JSON AS j
SELECT
    JSONArrayLength(j),
    JSONArrayLength(j.d);
        )",
        R"(
┌─JSONArrayLength(j)─┬─JSONArrayLength(j.d)─┐
│               ᴺᵁᴸᴸ │                    2 │
└────────────────────┴──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {23, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionJSONArrayLength>(documentation);

    /// For Spark compatibility.
    factory.registerAlias("JSON_ARRAY_LENGTH", "JSONArrayLength", FunctionFactory::Case::Insensitive);
}

}
