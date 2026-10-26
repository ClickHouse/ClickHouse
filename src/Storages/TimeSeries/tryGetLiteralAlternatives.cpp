#include <Storages/TimeSeries/tryGetLiteralAlternatives.h>


namespace DB
{

namespace
{

/// The most strings a group may be expanded to together with what precedes it, so that `(a|b)(c|d)...` cannot blow up.
constexpr size_t MAX_EXPANDED_TERMS = 100;

std::optional<Strings> parseAlternation(std::string_view regex, size_t & pos);

/// Parses literals and groups up to the next `|`, the `)` closing the current group, or the end of the regex.
std::optional<Strings> parseConcatenation(std::string_view regex, size_t & pos)
{
    Strings terms(1);
    while (pos < regex.size() && regex[pos] != '|' && regex[pos] != ')')
    {
        char c = regex[pos];
        if (c == '(')
        {
            ++pos;
            if (regex.substr(pos).starts_with("?:"))
                pos += 2;
            auto group = parseAlternation(regex, pos);
            if (!group || pos == regex.size())
                return {};
            ++pos;

            if (terms.size() > 1 && group->size() > 1 && terms.size() * group->size() > MAX_EXPANDED_TERMS)
                return {};
            Strings expanded;
            expanded.reserve(terms.size() * group->size());
            for (const auto & prefix : terms)
                for (const auto & suffix : *group)
                    expanded.push_back(prefix + suffix);
            terms = std::move(expanded);
        }
        else if (std::string_view("\\.^$?*+[]{}").contains(c))
            return {};
        else
        {
            for (auto & term : terms)
                term += c;
            ++pos;
        }
    }
    return terms;
}

/// Parses terms separated by `|` up to the `)` closing the current group or the end of the regex.
std::optional<Strings> parseAlternation(std::string_view regex, size_t & pos)
{
    auto terms = parseConcatenation(regex, pos);
    while (terms && pos < regex.size() && regex[pos] == '|')
    {
        ++pos;
        auto next_terms = parseConcatenation(regex, pos);
        if (!next_terms)
            return {};
        terms->insert(terms->end(), std::make_move_iterator(next_terms->begin()), std::make_move_iterator(next_terms->end()));
    }
    return terms;
}

}

std::optional<Strings> tryGetLiteralAlternatives(std::string_view regex)
{
    size_t pos = 0;
    auto terms = parseAlternation(regex, pos);
    /// The parsing stops before the end at a `)` without a matching `(`.
    if (!terms || pos != regex.size())
        return {};
    return terms;
}

}
