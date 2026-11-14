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

String toFullMatchRegex(std::string_view regex)
{
    String res(regex);
    bool has_pipe = false;
    bool has_flags = false;
    bool ends_with_anchor = false;
    for (size_t pos = 0; pos < regex.size(); ++pos)
    {
        ends_with_anchor = false;
        if (regex.substr(pos).starts_with("\\Q"))
        {
            /// Everything up to `\E` or the end of the regex is literal, so close the quote before adding the anchors.
            size_t quote_end = regex.find("\\E", pos + 2);
            if (quote_end == std::string_view::npos)
            {
                res += "\\E";
                break;
            }
            pos = quote_end + 1;
        }
        else if (regex[pos] == '\\')
            ++pos;
        else if (regex[pos] == '|')
            has_pipe = true;
        else if (regex.substr(pos).starts_with("(?"))
            has_flags = true;
        else if (regex[pos] == '$')
            ends_with_anchor = true;
    }

    /// `a|b` becomes `^(a|b)$` and not `^a|b$`, and flags like `(?m)` stay inside the group so they do not change the anchors.
    /// Other regexes keep the simple form, which the primary key can use.
    if (has_pipe || has_flags)
        return "^(" + res + ")$";
    if (!res.starts_with('^'))
        res = '^' + res;
    if (!ends_with_anchor)
        res += '$';
    return res;
}

}
