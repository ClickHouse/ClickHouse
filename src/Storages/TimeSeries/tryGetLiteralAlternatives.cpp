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

}
