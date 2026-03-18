#include "TestTags.h"

#include <cstring>

namespace DB
{

size_t getTestTagsLength(const String & multiline_query)
{
    const String & text = multiline_query;
    size_t pos = 0;
    bool first_line = true;

    while (true)
    {
        size_t line_start = pos;

        /// Skip spaces.
        while ((pos != text.length()) && (text[pos] == ' ' || text[pos] == '\t'))
            ++pos;

        /// Skip comment "--".
        static constexpr const char comment[] = "--";
        if (text.compare(pos, strlen(comment), comment) != 0)
            return line_start;
        pos += strlen(comment);

        /// Skip the prefix "Tags:" if it's the first line.
        if (first_line)
        {
            while ((pos != text.length()) && (text[pos] == ' ' || text[pos] == '\t'))
                ++pos;

            static constexpr const char tags_prefix[] = "Tags:";
            if (text.compare(pos, strlen(tags_prefix), tags_prefix) != 0)
                return 0;
            pos += strlen(tags_prefix);
            first_line = false;
        }

        /// Skip end-of-line.
        size_t eol_pos = text.find_first_of("\r\n", pos);
        if (eol_pos == String::npos)
            return text.length();
        bool two_chars_eol = (eol_pos + 1 < text.length()) && ((text[eol_pos + 1] == '\r') || (text[eol_pos + 1] == '\n')) && (text[eol_pos + 1] != text[eol_pos]);
        size_t eol_length = two_chars_eol ? 2 : 1;
        pos = eol_pos + eol_length;
    }
}

}
