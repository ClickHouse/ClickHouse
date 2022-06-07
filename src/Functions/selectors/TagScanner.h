#pragma once

#include "Types.h"

#include <base/find_symbols.h>
#include <Common/StringUtils/StringUtils.h>

#include <string_view>

namespace DB
{

struct TagScanner
{
    const char * last_tag_start = nullptr;

    /// @returns position of the symbol after a tag name or end if no tag is found
    const char * scan(const char * begin, const char * end, TagPreview & result)
    {
        while (begin != end)
        {
            begin = find_first_symbols<'<'>(begin, end);
            if (begin + 1 >= end)
                return end;

            ++begin;

            // if (*begin == '!')
            //{
            //    TODO: properly skip comment, DOCTYPE, etc.
            // }

            if (*begin == '/')
            {
                const char * gt = find_first_symbols<'>'>(begin, end);
                if (gt == end)
                    return end;

                result.name = std::string_view(begin + 1, gt - begin - 1);
                result.is_closing = true;
                return gt;
            }

            if (!isAlphaASCII(*begin))
                continue;

            last_tag_start = begin - 1;

            const char * after_tag_name = find_first_symbols<'>', ' ', '\t', '\n', '\r', '\f', '\v'>(begin, end);
            if (after_tag_name == end)
                return end;

            result.name = std::string_view(begin, after_tag_name - begin);
            result.is_closing = false;
            return after_tag_name;
            // TODO: skip <style>, <script> ???
        }

        return end;
    }
};

}
