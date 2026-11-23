#include <Storages/TimeSeries/tryGetLiteralAlternatives.h>


namespace DB
{

namespace
{

/// The most strings a group may be expanded to together with the literals around it,
/// so that neither `(a|b)(c|d)...` nor a long literal next to a big group can blow up.
constexpr size_t MAX_EXPANDED_TERMS = 100;

/// The deepest nesting of groups that is parsed, so that a regex with many nested groups cannot overflow the stack.
constexpr size_t MAX_GROUP_DEPTH = 32;

std::optional<Strings> parseAlternation(std::string_view regex, size_t & pos, size_t depth);

/// Parses literals and groups up to the next `|`, the `)` closing the current group, or the end of the regex.
std::optional<Strings> parseConcatenation(std::string_view regex, size_t & pos, size_t depth)
{
    Strings terms(1);
    while (pos < regex.size() && regex[pos] != '|' && regex[pos] != ')')
    {
        char c = regex[pos];
        if (c == '(')
        {
            if (depth == MAX_GROUP_DEPTH)
                return {};
            ++pos;
            if (regex.substr(pos).starts_with("?:"))
                pos += 2;
            auto group = parseAlternation(regex, pos, depth + 1);
            if (!group || pos == regex.size())
                return {};
            ++pos;

            bool has_prefix = terms.size() > 1 || !terms.front().empty();
            if (has_prefix && terms.size() * group->size() > MAX_EXPANDED_TERMS)
                return {};
            if (group->size() == 1)
            {
                /// A group of one string is appended in place, so many such groups do not copy the terms again and again.
                for (auto & term : terms)
                    term += group->front();
                continue;
            }
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
            if (terms.size() > MAX_EXPANDED_TERMS)
                return {};
            for (auto & term : terms)
                term += c;
            ++pos;
        }
    }
    return terms;
}

/// Parses terms separated by `|` up to the `)` closing the current group or the end of the regex.
std::optional<Strings> parseAlternation(std::string_view regex, size_t & pos, size_t depth)
{
    auto terms = parseConcatenation(regex, pos, depth);
    while (terms && pos < regex.size() && regex[pos] == '|')
    {
        ++pos;
        auto next_terms = parseConcatenation(regex, pos, depth);
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
    auto terms = parseAlternation(regex, pos, 0);
    /// The parsing stops before the end at a `)` without a matching `(`.
    if (!terms || pos != regex.size())
        return {};
    return terms;
}

}
