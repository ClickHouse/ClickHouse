#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <base/find_symbols.h>
#include <Common/Hex.h>
#include <Common/StringUtils.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace
{

constexpr size_t min_base64_length = 11;
constexpr size_t min_hex_length = 16;

/// Base64 symbol of a byte, with both alphabets merged: `+` and `-` are 62, `/` and `_` are 63. -1 for other bytes.
constexpr std::array<Int8, 256> base64_symbols = []
{
    std::array<Int8, 256> table{};
    table.fill(-1);
    for (int i = 0; i < 26; ++i)
    {
        table['A' + i] = static_cast<Int8>(i);
        table['a' + i] = static_cast<Int8>(26 + i);
    }
    for (int i = 0; i < 10; ++i)
        table['0' + i] = static_cast<Int8>(52 + i);
    table['+'] = table['-'] = 62;
    table['/'] = table['_'] = 63;
    return table;
}();

/// Ends of the symbol classes: digits and letters for hex; uppercase, lowercase, digits and the two other symbols for base64.
constexpr std::array<size_t, 2> hex_classes = {10, 16};
constexpr std::array<size_t, 4> base64_classes = {26, 52, 62, 64};

/// G-test on the class shares, chi-squared on the symbols. Both tolerate a small per-symbol bias, as in base64 of JSON (JWT).
template <size_t alphabet_size, size_t num_classes>
bool isCloseToUniform(
    const std::array<UInt64, alphabet_size> & counts, const std::array<size_t, num_classes> & class_ends, double class_threshold)
{
    constexpr double k = static_cast<double>(alphabet_size);
    double n = 0;
    double sum_of_squares = 0;
    for (UInt64 count : counts)
    {
        n += static_cast<double>(count);
        sum_of_squares += static_cast<double>(count) * static_cast<double>(count);
    }

    double g = 0;
    size_t class_begin = 0;
    for (size_t class_end : class_ends)
    {
        double observed = 0;
        for (size_t i = class_begin; i < class_end; ++i)
            observed += static_cast<double>(counts[i]);
        if (observed > 0)
        {
            double expected = n * static_cast<double>(class_end - class_begin) / k;
            g += 2 * observed * std::log(observed / expected);
        }
        class_begin = class_end;
    }
    if (g > std::max(class_threshold, 0.15 * n))
        return false;

    double chi_squared = k * sum_of_squares / n - n;
    return chi_squared <= std::max(k - 1 + 4 * std::sqrt(2 * (k - 1)), 1.5 * n);
}

bool isGarbage(const char * begin, const char * end)
{
    const size_t size = end - begin;
    if (size < min_base64_length)
        return false;

    /// 3.84 and 7.81 are the 95% quantiles of the chi-squared distribution with 1 and 3 degrees of freedom.
    if (std::all_of(begin, end, isHexDigit))
    {
        if (size < min_hex_length)
            return false;
        std::array<UInt64, 16> counts{};
        for (const char * pos = begin; pos < end; ++pos)
            ++counts[unhex(*pos)];
        return isCloseToUniform(counts, hex_classes, 3.84);
    }

    for (size_t padding = 0; padding < 2 && end[-1] == '='; ++padding)
        --end;

    /// `.` splits the value into parts, as in JWT.
    std::array<UInt64, 64> counts{};
    size_t num_symbols = 0;
    size_t part_size = 0;
    for (const char * pos = begin; pos < end; ++pos)
    {
        if (*pos == '.')
        {
            if (part_size < min_base64_length)
                return false;
            part_size = 0;
            continue;
        }
        Int8 symbol = base64_symbols[static_cast<UInt8>(*pos)];
        if (symbol < 0)
            return false;
        ++counts[symbol];
        ++num_symbols;
        ++part_size;
    }
    if ((part_size > 0 && part_size < min_base64_length) || num_symbols < min_base64_length)
        return false;
    return isCloseToUniform(counts, base64_classes, 7.81);
}

/// Parameters are separated by `&` and `#`. A `?` before the `=` (as in `#/route?a=b`) starts a new section.
char * removeGarbageParameters(const char * pos, const char * end, char * out)
{
    auto copy = [&out](const char * from, const char * to)
    {
        memcpy(out, from, to - from);
        out += to - from;
    };

    const char * query_begin = find_first_symbols<'?', '#'>(pos, end);
    if (query_begin == end)
    {
        copy(pos, end);
        return out;
    }

    copy(pos, query_begin + 1);
    pos = query_begin + 1;

    bool first_in_section = true;
    while (true)
    {
        const char * name_end = find_first_symbols<'=', '&', '#', '?'>(pos, end);
        if (name_end != end && *name_end == '?')
        {
            if (!first_in_section)
                *out++ = '&';
            copy(pos, name_end + 1);
            first_in_section = true;
            pos = name_end + 1;
            continue;
        }

        const char * value_end = find_first_symbols<'&', '#'>(name_end, end);
        if (name_end == end || *name_end != '=' || !isGarbage(name_end + 1, value_end))
        {
            if (!first_in_section)
                *out++ = '&';
            copy(pos, value_end);
            first_in_section = false;
        }

        if (value_end == end)
            break;
        if (*value_end == '#')
        {
            *out++ = '#';
            first_in_section = true;
        }
        pos = value_end + 1;
    }
    return out;
}

struct RemoveGarbageParametersFromURLImpl
{
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        /// The result is never longer than the input.
        res_data.resize(data.size());
        res_offsets.resize(input_rows_count);

        size_t prev_offset = 0;
        size_t res_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * src = reinterpret_cast<const char *>(data.data() + prev_offset);
            const char * src_end = reinterpret_cast<const char *>(data.data() + offsets[i]);
            char * dst = reinterpret_cast<char *>(res_data.data() + res_offset);

            res_offset += removeGarbageParameters(src, src_end, dst) - dst;
            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }

        res_data.resize(res_offset);
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column of type FixedString is not supported by URL functions");
    }
};

struct NameRemoveGarbageParametersFromURL
{
    static constexpr auto name = "removeGarbageParametersFromURL";
};

using FunctionRemoveGarbageParametersFromURL = FunctionStringToString<RemoveGarbageParametersFromURLImpl, NameRemoveGarbageParametersFromURL>;

}

REGISTER_FUNCTION(RemoveGarbageParametersFromURL)
{
    FunctionDocumentation::Description description = R"(
Removes URL parameters whose values look like random tokens, such as session IDs, signatures, hashes and JWTs.

A value is removed when both hold:
- It is hex of at least 16 digits, or base64 of at least 11 characters without padding. A value of hex digits only is checked as hex. Base64 can use the standard or the URL-safe alphabet, with up to two `=` of padding. It can be split by `.` into parts of at least 11 characters each, as in JWT, and the last part can be empty.
- Its symbol histogram is close to uniform. Letters, digits and other symbols occur in the proportions of a random string, and no symbol repeats much more often than in one.

A removed parameter goes together with one `&` next to it. The `?` and `#` delimiters stay, as with `cutURLParameter`.

Parameters without `=` are kept. Values are not decoded, so `%2B`, `%2F` and `%3D` are not base64 symbols.

Short values that mix letters and digits, such as `BlackFriday2024`, can look random and be removed.
    )";
    FunctionDocumentation::Syntax syntax = "removeGarbageParametersFromURL(url)";
    FunctionDocumentation::Arguments arguments = {
        {"url", "URL.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns the URL without the parameters whose values look random.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT removeGarbageParametersFromURL('https://example.com/?sid=d41d8cd98f00b204e9800998ecf8427e&lang=en&ts=1458904227333') AS url;
        )",
        R"(
┌─url───────────────────────────────────────────┐
│ https://example.com/?lang=en&ts=1458904227333 │
└───────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionRemoveGarbageParametersFromURL>(documentation);
}

}
