#include <Storages/TimeSeries/tryGetLiteralAlternatives.h>


namespace DB
{

std::optional<Strings> tryGetLiteralAlternatives(std::string_view regex)
{
    Strings terms(1);
    size_t depth = 0;
    bool term_ended = false;
    for (size_t pos = 0; pos < regex.size(); ++pos)
    {
        char c = regex[pos];
        if (c == '|')
        {
            terms.emplace_back();
            term_ended = false;
        }
        else if (c == '(')
        {
            if (term_ended || !terms.back().empty())
                return {};
            if (regex.substr(pos + 1).starts_with("?:"))
                pos += 2;
            ++depth;
        }
        else if (c == ')')
        {
            if (depth == 0)
                return {};
            --depth;
            term_ended = true;
        }
        else if (term_ended || std::string_view("\\.^$?*+[]{}").contains(c))
            return {};
        else
            terms.back() += c;
    }
    if (depth != 0)
        return {};
    return terms;
}

String anchorRegex(const String & regex)
{
    size_t depth = 0;
    bool in_class = false;
    bool in_quote = false;
    bool top_level_alternation = false;
    bool top_level_multiline = false;
    bool ends_with_anchor = false;
    for (size_t pos = 0; pos < regex.size(); ++pos)
    {
        char c = regex[pos];
        ends_with_anchor = false;
        if (in_quote)
        {
            /// Everything after `\Q` is literal up to `\E` or the end of the regex.
            if (c == '\\' && pos + 1 < regex.size() && regex[pos + 1] == 'E')
            {
                in_quote = false;
                ++pos;
            }
        }
        else if (c == '\\')
        {
            if (pos + 1 < regex.size() && regex[pos + 1] == 'Q')
                in_quote = true;
            ++pos;
        }
        else if (in_class)
        {
            if (c == ']')
                in_class = false;
            else if (c == '[' && pos + 1 < regex.size() && regex[pos + 1] == ':')
            {
                /// A POSIX named class such as `[:alpha:]` is a part of the enclosing class, its `]` does not close it.
                /// RE2 treats `[:` without a following `:]` as literal characters.
                if (size_t end_of_named_class = regex.find(":]", pos + 2); end_of_named_class != String::npos)
                    pos = end_of_named_class + 1;
            }
        }
        else if (c == '[')
        {
            in_class = true;
            /// A `]` right after `[` or `[^` is literal.
            if (pos + 1 < regex.size() && regex[pos + 1] == '^')
                ++pos;
            if (pos + 1 < regex.size() && regex[pos + 1] == ']')
                ++pos;
        }
        else if (c == '(')
        {
            /// An inline flag group like `(?m)` at the top level would also apply to the `$` appended after the regex,
            /// and in the multiline mode `$` matches before every newline, so the regex must be wrapped into a group.
            if (depth == 0 && pos + 1 < regex.size() && regex[pos + 1] == '?')
            {
                bool negated = false;
                bool multiline = false;
                size_t flag_pos = pos + 2;
                for (; flag_pos < regex.size(); ++flag_pos)
                {
                    char flag = regex[flag_pos];
                    if (flag == '-')
                        negated = true;
                    else if (flag == 'm')
                        multiline = !negated;
                    else if (flag != 'i' && flag != 's' && flag != 'U')
                        break;
                }
                if (multiline && flag_pos < regex.size() && regex[flag_pos] == ')')
                    top_level_multiline = true;
            }
            ++depth;
        }
        else if (c == ')')
        {
            if (depth > 0)
                --depth;
        }
        else if (c == '|' && depth == 0)
            top_level_alternation = true;
        else if (c == '$')
            ends_with_anchor = true;
    }

    String res = regex;
    /// A quote can extend to the end of the regex, and it must not swallow the anchor or the group added after it.
    if (in_quote)
        res += "\\E";

    if (top_level_alternation || top_level_multiline)
        return "^(?:" + res + ")$";

    if (!res.starts_with('^'))
        res = '^' + res;
    if (!ends_with_anchor)
        res += '$';
    return res;
}

}
