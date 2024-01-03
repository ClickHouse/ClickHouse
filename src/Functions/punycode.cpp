#include "config.h"

#if USE_IDNA

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wnewline-eof"
#endif
#    include <ada/idna/punycode.h>
#    include <ada/idna/to_ascii.h>
#    include <ada/idna/to_unicode.h>
#    include <ada/idna/unicode_transcoding.h>
#ifdef __clang__
#    pragma clang diagnostic pop
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

enum class ErrorHandling
{
    Throw,
    Null
};

/// Implementation of
/// - punycode(En|De)code[OrNull](), see [1]
/// - idna(En|De)code[OrNull](), see [2, 3]
///
/// [1] https://en.wikipedia.org/wiki/Punycode
/// [2] https://en.wikipedia.org/wiki/Internationalized_domain_name#ToASCII_and_ToUnicode
/// [3] https://www.unicode.org/reports/tr46/#ToUnicode


/// Kind of similar to FunctionStringToString but accepts String arguments only, and supports `OrNull` overloads
template <typename Impl>
class FunctionIdnaPunycodeBase : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionIdnaPunycodeBase<Impl>>(); }
    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"str", &isString<IDataType>, nullptr, "String"},
        };
        validateFunctionArgumentTypes(*this, arguments, args);

        auto return_type = std::make_shared<DataTypeString>();

        if constexpr (Impl::error_handling == ErrorHandling::Null)
            return makeNullable(return_type);
        else
            return return_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        auto col_res = ColumnString::create();
        ColumnUInt8::MutablePtr col_res_null;
        if (const ColumnString * col = checkAndGetColumn<ColumnString>(arguments[0].column.get()))
            Impl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), col_res_null);
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of argument of function {}",
                arguments[0].column->getName(), getName());

        if constexpr (Impl::error_handling == ErrorHandling::Null)
            return ColumnNullable::create(std::move(col_res), std::move(col_res_null));
        else
            return col_res;
    }
};


template <ErrorHandling error_handling_>
struct PunycodeEncodeImpl
{
    static constexpr auto error_handling = error_handling_;
    static constexpr auto name = (error_handling == ErrorHandling::Null) ? "punycodeEncodeOrNull" : "punycodeEncode";

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        ColumnUInt8::MutablePtr & col_res_null)
    {
        const size_t rows = offsets.size();
        res_data.reserve(data.size()); /// just a guess, assuming the input is all-ASCII
        res_offsets.reserve(rows);
        if constexpr (error_handling == ErrorHandling::Null)
            col_res_null = ColumnUInt8::create(rows, 0);

        size_t prev_offset = 0;
        std::u32string value_utf32;
        std::string value_puny;
        for (size_t row = 0; row < rows; ++row)
        {
            const char * value = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t value_length = offsets[row] - prev_offset - 1;

            const size_t value_utf32_length = ada::idna::utf32_length_from_utf8(value, value_length);
            value_utf32.resize(value_utf32_length);
            ada::idna::utf8_to_utf32(value, value_length, value_utf32.data());

            const bool ok = ada::idna::utf32_to_punycode(value_utf32, value_puny);
            if (!ok)
            {
                if constexpr (error_handling == ErrorHandling::Throw)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' cannot be converted to Punycode", std::string_view(value, value_length));
                }
                else
                {
                    value_puny.clear();
                    col_res_null->getData()[row] = 1;
                }
            }

            res_data.insert(value_puny.c_str(), value_puny.c_str() + value_puny.size() + 1);
            res_offsets.push_back(res_data.size());

            prev_offset = offsets[row];

            value_utf32.clear();
            value_puny.clear(); /// utf32_to_punycode() appends to its output string
        }
    }
};


template <ErrorHandling error_handling_>
struct PunycodeDecodeImpl
{
    static constexpr auto error_handling = error_handling_;
    static constexpr auto name = (error_handling == ErrorHandling::Null) ? "punycodeDecodeOrNull" : "punycodeDecode";

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        ColumnUInt8::MutablePtr & col_res_null)
    {
        const size_t rows = offsets.size();
        res_data.reserve(data.size()); /// just a guess, assuming the input is all-ASCII
        res_offsets.reserve(rows);
        if constexpr (error_handling == ErrorHandling::Null)
            col_res_null = ColumnUInt8::create(rows, 0);

        size_t prev_offset = 0;
        std::u32string value_utf32;
        std::string value_utf8;
        for (size_t row = 0; row < rows; ++row)
        {
            const char * value = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t value_length = offsets[row] - prev_offset - 1;

            const std::string_view value_punycode(value, value_length);
            const bool ok = ada::idna::punycode_to_utf32(value_punycode, value_utf32);
            if (!ok)
            {
                if constexpr (error_handling == ErrorHandling::Throw)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' is not a valid Punycode-encoded string", value_punycode);
                }
                else
                {
                    value_utf32.clear();
                    col_res_null->getData()[row] = 1;
                }
            }

            const size_t utf8_length = ada::idna::utf8_length_from_utf32(value_utf32.data(), value_utf32.size());
            value_utf8.resize(utf8_length);
            ada::idna::utf32_to_utf8(value_utf32.data(), value_utf32.size(), value_utf8.data());

            res_data.insert(value_utf8.c_str(), value_utf8.c_str() + value_utf8.size() + 1);
            res_offsets.push_back(res_data.size());

            prev_offset = offsets[row];

            value_utf32.clear(); /// punycode_to_utf32() appends to its output string
            value_utf8.clear();
        }
    }
};


/// Translates a UTF-8 string (typically an Internationalized Domain Name for Applications, IDNA) to an ASCII-encoded equivalent. The
/// encoding is performed per domain component and based on Punycode with ASCII Compatible Encoding (ACE) prefix "xn--".
/// Example: "straße.münchen.de" --> "xn--strae-oqa.xn--mnchen-3ya.de"
/// Note: doesn't do percent decoding. Doesn't trim tabs, spaces or control characters. Expects non-empty inputs.
template <ErrorHandling error_handling_>
struct IdnaEncodeImpl
{
    static constexpr auto error_handling = error_handling_;
    static constexpr auto name = (error_handling == ErrorHandling::Null) ? "idnaEncodeOrNull" : "idnaEncode";

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        ColumnUInt8::MutablePtr & col_res_null)
    {
        const size_t rows = offsets.size();
        res_data.reserve(data.size()); /// just a guess, assuming the input is all-ASCII
        res_offsets.reserve(rows);
        if constexpr (error_handling == ErrorHandling::Null)
            col_res_null = ColumnUInt8::create(rows, 0);

        size_t prev_offset = 0;
        std::string ascii;
        for (size_t row = 0; row < rows; ++row)
        {
            const char * value = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t value_length = offsets[row] - prev_offset - 1;
            std::string_view value_view(value, value_length);

            if (!value_view.empty()) /// to_ascii() expects non-empty input
            {
                ascii = ada::idna::to_ascii(value_view);
                const bool ok = !ascii.empty();
                if (!ok)
                {
                    if constexpr (error_handling == ErrorHandling::Throw)
                    {
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' cannot be converted to Punycode", std::string_view(value, value_length));
                    }
                    else
                    {
                        ascii.clear();
                        col_res_null->getData()[row] = 1;
                    }
                }
            }

            res_data.insert(ascii.c_str(), ascii.c_str() + ascii.size() + 1);
            res_offsets.push_back(res_data.size());

            prev_offset = offsets[row];

            ascii.clear();
        }
    }
};


/// As per the specification, invalid inputs are returned as is, i.e. there is no special error handling.
/// Therefore, this struct implements no error handling.
struct IdnaDecodeImpl
{
    static constexpr auto error_handling = ErrorHandling::Throw; /// dummy
    static constexpr auto name = "idnaDecode";

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        ColumnUInt8::MutablePtr & /*col_res_null*/)
    {
        const size_t rows = offsets.size();
        res_data.reserve(data.size()); /// just a guess, assuming the input is all-ASCII
        res_offsets.reserve(rows);

        size_t prev_offset = 0;
        std::string unicode;
        for (size_t row = 0; row < rows; ++row)
        {
            const char * value = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t value_length = offsets[row] - prev_offset - 1;
            std::string_view value_view(value, value_length);

            unicode = ada::idna::to_unicode(value_view);

            res_data.insert(unicode.c_str(), unicode.c_str() + unicode.size() + 1);
            res_offsets.push_back(res_data.size());

            prev_offset = offsets[row];

            unicode.clear();
        }
    }
};


}

REGISTER_FUNCTION(Punycode)
{
    factory.registerFunction<FunctionIdnaPunycodeBase<PunycodeEncodeImpl<ErrorHandling::Throw>>>(FunctionDocumentation{
        .description=R"(
Computes a Punycode representation of a string. Throws an exception in case of error.)",
        .syntax="punycodeEncode(str)",
        .arguments={{"str", "Input string"}},
        .returned_value="The punycode representation [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT punycodeEncode('München') AS puny;",
            R"(
┌─puny───────┐
│ Mnchen-3ya │
└────────────┘
            )"
            }}
    });

    factory.registerFunction<FunctionIdnaPunycodeBase<PunycodeEncodeImpl<ErrorHandling::Null>>>(FunctionDocumentation{
        .description=R"(
Computes a Punycode representation of a string. Returns NULL in case of error)",
        .syntax="punycodeEncode(str)",
        .arguments={{"str", "Input string"}},
        .returned_value="The punycode representation [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT punycodeEncodeOrNull('München') AS puny;",
            R"(
┌─puny───────┐
│ Mnchen-3ya │
└────────────┘
            )"
            }}
    });

    factory.registerFunction<FunctionIdnaPunycodeBase<PunycodeDecodeImpl<ErrorHandling::Throw>>>(FunctionDocumentation{
        .description=R"(
Computes a Punycode representation of a string. Throws an exception in case of error.)",
        .syntax="punycodeDecode(str)",
        .arguments={{"str", "A Punycode-encoded string"}},
        .returned_value="The plaintext representation [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT punycodeDecode('Mnchen-3ya') AS plain;",
            R"(
┌─plain───┐
│ München │
└─────────┘
            )"
            }}
    });

    factory.registerFunction<FunctionIdnaPunycodeBase<PunycodeDecodeImpl<ErrorHandling::Null>>>(FunctionDocumentation{
        .description=R"(
Computes a Punycode representation of a string. Returns NULL in case of error)",
        .syntax="punycodeDecode(str)",
        .arguments={{"str", "A Punycode-encoded string"}},
        .returned_value="The plaintext representation [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT punycodeDecodeOrNull('Mnchen-3ya') AS plain;",
            R"(
┌─plain───┐
│ München │
└─────────┘
            )"
            }}
    });

    factory.registerFunction<FunctionIdnaPunycodeBase<IdnaEncodeImpl<ErrorHandling::Throw>>>(FunctionDocumentation{
        .description=R"(
Computes an ASCII representation of an Internationalized Domain Name. Throws an exception in case of error.)",
        .syntax="idnaEncode(str)",
        .arguments={{"str", "Input string"}},
        .returned_value="An Unicode-encoded domain name [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT idnaEncode('straße.münchen.de') AS ascii;",
            R"(
┌─ascii───────────────────────────┐
│ xn--strae-oqa.xn--mnchen-3ya.de │
└─────────────────────────────────┘
            )"
            }}
    });

    factory.registerFunction<FunctionIdnaPunycodeBase<IdnaEncodeImpl<ErrorHandling::Null>>>(FunctionDocumentation{
        .description=R"(
Computes a ASCII representation of an Internationalized Domain Name. Returns NULL in case of error)",
        .syntax="punycodeEncode(str)",
        .arguments={{"str", "Input string"}},
        .returned_value="An Unicode-encoded domain name [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT idnaEncodeOrNull('München') AS ascii;",
            R"(
┌─ascii───────────────────────────┐
│ xn--strae-oqa.xn--mnchen-3ya.de │
└─────────────────────────────────┘
            )"
            }}
    });

    factory.registerFunction<FunctionIdnaPunycodeBase<IdnaDecodeImpl>>(FunctionDocumentation{
        .description=R"(
Computes a Unicode representation of an Internationalized Domain Name.)",
        .syntax="idnaDecode(str)",
        .arguments={{"str", "Input string"}},
        .returned_value="An Unicode-encoded domain name [String](/docs/en/sql-reference/data-types/string.md).",
        .examples={
            {"simple",
            "SELECT idnaDecode('xn--strae-oqa.xn--mnchen-3ya.de') AS unicode;",
            R"(
┌─unicode───────────┐
│ straße.münchen.de │
└───────────────────┘
            )"
            }}
    });
}

}

#endif
