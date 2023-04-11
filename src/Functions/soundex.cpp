#include <cctype>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

/** The implementation of this algorithm is the same as most SQL language implementations.

  * Soundex wiki: https://en.wikipedia.org/wiki/Soundex

  * The details are as follows:

  * 1. Save the first letter. Map all occurrences of a, e, i, o, u, y, h, w. to zero(0)
  * 2. Replace all consonants (include the first letter) with digits as follows:
  *
  *  - b, f, p, v → 1
  *  - c, g, j, k, q, s, x, z → 2
  *  - d, t → 3
  *  - l → 4
  *  - m, n → 5
  *  - r → 6
  *
  * 3. Replace all adjacent same digits with one digit, and then remove all the zero (0) digits
  * 4. If the saved letter's digit is the same as the resulting first digit, remove the digit (keep the letter).
  * 5. Append 3 zeros if result contains less than 3 digits. Remove all except first letter and 3 digits after it.
  *
  */

struct SoundexImpl
{
    static constexpr auto name = "soundex";
    enum
    {
        length = 4
    };
    static constexpr auto soundex_map = "01230120022455012623010202";

    static void skipNonAlphaASCII(const char *& start, const char * end)
    {
        while (start < end && !isAlphaASCII(*start))
            ++start;
    }
    static char getScode(char c)
    {
        return soundex_map[c - 'A'];
    }

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        const char * in_cur = begin;
        const char * in_end = begin + size;
        unsigned char * out_end = out_char_data + length;

        char prev_code = '0';
        skipNonAlphaASCII(in_cur, in_end);
        if (in_cur < in_end)
        {
          *out_char_data++ = toUpperIfAlphaASCII(*in_cur);
          prev_code = getScode(toUpperIfAlphaASCII(*in_cur));
          skipNonAlphaASCII(++in_cur, in_end);
        }

        char current_code = '0';
        while (in_cur < in_end && out_char_data < out_end)
        {
            current_code = getScode(toUpperIfAlphaASCII(*in_cur));
            if ((current_code != '0') && (current_code != prev_code))
            {
                *out_char_data++ = current_code;
            }
            prev_code = current_code;
            skipNonAlphaASCII(++in_cur, in_end);
        }
        while (out_char_data < out_end)
        {
            *out_char_data++ = '0';
        }
    }
};

REGISTER_FUNCTION(Soundex)
{
    factory.registerFunction<FunctionStringHashFixedString<SoundexImpl>>(
        Documentation{"Returns soundex code of a string."}, FunctionFactory::CaseInsensitive);
}


}
