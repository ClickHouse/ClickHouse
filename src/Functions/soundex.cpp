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
    /* ABCDEFGHIJKLMNOPQRSTUVWXYZ */
    /* :::::::::::::::::::::::::: */
    static constexpr auto soundex_map = "01230120022455012623010202";

    static char getScode(const char *& ptr, const char * in_end)
    {
        while (ptr < in_end && !isAlphaASCII(*ptr))
        {
            ptr++;
        }
        if (ptr == in_end)
            return 0;
        return soundex_map[toUpperIfAlphaASCII(*ptr) - 'A'];
    }

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        const char * in_cur = begin;
        const char * in_end = begin + size;
        unsigned char * out_end = out_char_data + length;

        while (in_cur < in_end && !isAlphaASCII(*in_cur))
        {
            in_cur++;
        }
        if (in_cur < in_end)
        {
            *out_char_data++ = toUpperIfAlphaASCII(*in_cur);
        }
        char last_ch = getScode(in_cur, in_end);

        char ch;
        in_cur++;
        while (in_cur < in_end && out_char_data < out_end && (ch = getScode(in_cur, in_end)) != 0)
        {
            if (in_cur == in_end)
            {
                break;
            }
            in_cur++;
            if ((ch != '0') && (ch != last_ch))
            {
                *out_char_data++ = ch;
            }
            last_ch = ch;
        }
        while (out_char_data < out_end)
        {
            *out_char_data++ = '0';
        }
        return;
    }
};

REGISTER_FUNCTION(Soundex)
{
    factory.registerFunction<FunctionStringHashFixedString<SoundexImpl>>(
        Documentation{"Returns soundex code of a string."}, FunctionFactory::CaseInsensitive);
}


}
