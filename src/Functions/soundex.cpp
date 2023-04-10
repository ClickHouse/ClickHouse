#include <cctype>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsHashing.h>


namespace DB
{

struct SoundexImpl
{
    static constexpr auto name = "soundex";
    static constexpr size_t length = 4;
    // enum { length = 4 };
    /* ABCDEFGHIJKLMNOPQRSTUVWXYZ */
    /* :::::::::::::::::::::::::: */
    static constexpr auto soundex_map = "01230120022455012623010202";

    static char getScode(const char * &ptr, const char * in_end)
    {
        while (ptr < in_end && !std::isalpha(*ptr))
        {
            (ptr)++;
        }
        if (ptr == in_end)
            return 0;
        return soundex_map[std::toupper(*ptr) - 'A'];
    }

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        const char * in_cur = begin;
        const char * in_end = begin + size;
        unsigned char * out_end = out_char_data + length;

        while (in_cur < in_end && !std::isalpha(*in_cur))
        {
            in_cur++;
        }
        if (in_cur < in_end)
        {
            *out_char_data++ = std::toupper(*in_cur);
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
    factory.registerFunction<FunctionStringHashFixedString<SoundexImpl>>({}, FunctionFactory::CaseInsensitive);
}


}
