#include "config.h"

#if USE_IDNA

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>

#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wnewline-eof"
#endif
#    include <ada/idna/punycode.h>
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

struct PunycodeEncodeImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const size_t rows = offsets.size();
        res_data.reserve(data.size()); /// just a guess, assuming the input is all-ASCII
        res_offsets.reserve(rows);

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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Internal error during Punycode encoding");

            res_data.insert(value_puny.c_str(), value_puny.c_str() + value_puny.size() + 1);
            res_offsets.push_back(res_data.size());

            prev_offset = offsets[row];

            value_utf32.clear();
            value_puny.clear(); /// utf32_to_punycode() appends to its output string
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column of type FixedString is not supported by punycodeEncode function");
    }
};

struct PunycodeDecodeImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets)
    {
        const size_t rows = offsets.size();
        res_data.reserve(data.size()); /// just a guess, assuming the input is all-ASCII
        res_offsets.reserve(rows);

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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Internal error during Punycode decoding");

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

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column of type FixedString is not supported by punycodeDecode function");
    }
};

struct NamePunycodeEncode
{
    static constexpr auto name = "punycodeEncode";
};

struct NamePunycodeDecode
{
    static constexpr auto name = "punycodeDecode";
};

REGISTER_FUNCTION(Punycode)
{
    factory.registerFunction<FunctionStringToString<PunycodeEncodeImpl, NamePunycodeEncode>>(FunctionDocumentation{
        .description=R"(
Computes a Punycode representation of a string.)",
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

    factory.registerFunction<FunctionStringToString<PunycodeDecodeImpl, NamePunycodeDecode>>(FunctionDocumentation{
        .description=R"(
Computes a Punycode representation of a string.)",
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
}

}

#endif
