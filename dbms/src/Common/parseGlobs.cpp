#include <Common/parseGlobs.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <algorithm>

namespace DB
{
/*  Because of difference between grep-wildcard-syntax and perl-regexp one we need some transformation of string to use RE2 library for matching.
 *  It couldn't be one pass because of various configurations of braces in filenames (Linux allow almost any symbols in paths).
 *  So there are some iterations of escaping and replacements to make correct perl-regexp.
 */
std::string makeRegexpPatternFromGlobs(const std::string & initial_str)
{
    std::string first_prepare;
    first_prepare.reserve(initial_str.size());
    for (const auto & letter : initial_str)
    {
        if ((letter == '[') || (letter == ']') || (letter == '|') || (letter == '+'))
            first_prepare.push_back('\\');
        first_prepare.push_back(letter);
    }
    re2::RE2 char_range(R"(({[^*?/\\]\.\.[^*?/\\]}))");
    re2::StringPiece input_for_range(first_prepare);
    re2::StringPiece matched_range(first_prepare);

    std::string second_prepare;
    second_prepare.reserve(first_prepare.size());
    size_t current_index = 0;
    size_t pos;
    while (RE2::FindAndConsume(&input_for_range, char_range, &matched_range))
    {
        pos = matched_range.data() - first_prepare.data();
        second_prepare += first_prepare.substr(current_index, pos - current_index);
        second_prepare.append({'[', matched_range.ToString()[1], '-', matched_range.ToString()[4], ']'});
        current_index = input_for_range.data() - first_prepare.data();
    }
    second_prepare += first_prepare.substr(current_index);
    re2::RE2 enumeration(R"(({[^{}*,]+,[^{}*]*[^{}*,]}))");
    re2::StringPiece input_enum(second_prepare);
    re2::StringPiece matched_enum(second_prepare);
    current_index = 0;
    std::string third_prepare;
    while (RE2::FindAndConsume(&input_enum, enumeration, &matched_enum))
    {
        pos = matched_enum.data() - second_prepare.data();
        third_prepare.append(second_prepare.substr(current_index, pos - current_index));
        std::string buffer = matched_enum.ToString();
        buffer[0] = '(';
        buffer.back() = ')';
        std::replace(buffer.begin(), buffer.end(), ',', '|');
        third_prepare.append(buffer);
        current_index = input_enum.data() - second_prepare.data();
    }
    third_prepare += second_prepare.substr(current_index);
    std::string result;
    result.reserve(third_prepare.size());
    for (const auto & letter : third_prepare)
    {
        if ((letter == '?') || (letter == '*'))
        {
            result += "[^/]";
            if (letter == '?')
                continue;
        }
        if ((letter == '.') || (letter == '{') || (letter == '}'))
            result.push_back('\\');
        result.push_back(letter);
    }
    return result;
}
}
