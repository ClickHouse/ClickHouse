#pragma once

#include <string_view>
#include <vector>

namespace Jieba
{

using Rune = uint16_t;

struct RuneInfo
{
    Rune rune = 0;
    size_t offset = 0;
    size_t len = 0;
};

struct RuneRange
{
    size_t begin = 0;
    size_t end = 0;
};

using Word = std::string_view;

inline RuneInfo decodeUTF8Rune(const char * str, size_t len)
{
    RuneInfo info;
    if (!str || len == 0)
        return info;

    uint8_t first = static_cast<uint8_t>(str[0]);

    if (!(first & 0x80))
    {
        info.rune = first & 0x7f;
        info.len = 1;
    }
    else if (first <= 0xdf && len >= 2)
    {
        info.rune = (first & 0x1f) << 6 | (static_cast<uint8_t>(str[1]) & 0x3f);
        info.len = 2;
    }
    else if (first <= 0xef && len >= 3)
    {
        info.rune = (first & 0x0f) << 12 | (static_cast<uint8_t>(str[1]) & 0x3f) << 6 | (static_cast<uint8_t>(str[2]) & 0x3f);
        info.len = 3;
    }
    else if (first <= 0xf7 && len >= 4)
    {
        info.rune = (first & 0x07) << 18 | (static_cast<uint8_t>(str[1]) & 0x3f) << 12 | (static_cast<uint8_t>(str[2]) & 0x3f) << 6
            | (static_cast<uint8_t>(str[3]) & 0x3f);
        info.len = 4;
    }

    return info;
}

inline bool decodeUTF8String(std::string_view str, std::vector<RuneInfo> & runes)
{
    runes.clear();
    runes.reserve(str.size() / 2);

    size_t pos = 0;
    while (pos < str.size())
    {
        RuneInfo info = decodeUTF8Rune(str.data() + pos, str.size() - pos);
        if (info.len == 0)
            return false;

        info.offset = pos;
        runes.push_back(info);

        pos += info.len;
    }

    return true;
}

struct DictUnit
{
    std::vector<Rune> word;
    double weight = 0.0;
};

}
