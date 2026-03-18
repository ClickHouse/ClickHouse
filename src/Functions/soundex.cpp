#include <cctype>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Common/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}
/** Soundex algorithm, https://en.wikipedia.org/wiki/Soundex
  * Implemented similarly as in most SQL dialects:
  * 1. Save the first letter. Map all occurrences of a, e, i, o, u, y, h, w. to zero(0)
  * 2. Replace all consonants (include the first letter) with digits as follows:
  *  - b, f, p, v → 1
  *  - c, g, j, k, q, s, x, z → 2
  *  - d, t → 3
  *  - l → 4
  *  - m, n → 5
  *  - r → 6
  * 3. Replace all adjacent same digits with one digit, and then remove all the zero (0) digits
  * 4. If the saved letter's digit is the same as the resulting first digit, remove the digit (keep the letter).
  * 5. Append 3 zeros if result contains less than 3 digits. Remove all except first letter and 3 digits after it.
  */

struct SoundexImpl
{
    static constexpr auto length = 4z;
    static constexpr auto soundex_map = "01230120022455012623010202";

    static void calculate(const char * value, size_t value_length, char * out)
    {
        const char * cur = value;
        const char * const end = value + value_length;
        char * const out_end = out + length;

        while (cur < end && !isAlphaASCII(*cur))
            ++cur;

        char prev_code = '0';
        if (cur < end)
        {
            *out = toUpperIfAlphaASCII(*cur);
            ++out;
            prev_code = soundex_map[toUpperIfAlphaASCII(*cur) - 'A'];
            ++cur;
        }

        while (cur < end && !isAlphaASCII(*cur))
            ++cur;

        while (cur < end && out < out_end)
        {
            char current_code = soundex_map[toUpperIfAlphaASCII(*cur) - 'A'];
            if ((current_code != '0') && (current_code != prev_code))
            {
                *out = current_code;
                ++out;
            }
            prev_code = current_code;
            ++cur;

            while (cur < end && !isAlphaASCII(*cur))
                ++cur;
        }

        while (out < out_end)
        {
            *out = '0';
            ++out;
        }
    }

    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        size_t input_rows_count)
    {
        res_data.resize(input_rows_count * (length + 1));
        res_offsets.resize(input_rows_count);

        size_t prev_offset = 0;
        for (size_t i = 0; i < input_rows_count; ++i)
        {
            const char * value = reinterpret_cast<const char *>(&data[prev_offset]);
            const size_t value_length = offsets[i] - prev_offset - 1;
            const size_t out_index = i * (length + 1);
            calculate(value, value_length, reinterpret_cast<char *>(&res_data[out_index]));
            res_data[out_index + length] = '\0';
            res_offsets[i] = (out_index + length + 1);
            prev_offset = offsets[i];
        }
    }

    [[noreturn]] static void vectorFixed(const ColumnString::Chars &, size_t, ColumnString::Chars &, size_t)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Column of type FixedString is not supported by soundex function");
    }
};

struct NameSoundex
{
    static constexpr auto name = "soundex";
};

REGISTER_FUNCTION(Soundex)
{
    factory.registerFunction<FunctionStringToString<SoundexImpl, NameSoundex>>(
        FunctionDocumentation{.description="Returns Soundex code of a string."}, FunctionFactory::Case::Insensitive);
}


}
