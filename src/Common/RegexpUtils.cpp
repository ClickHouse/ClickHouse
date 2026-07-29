#include <Common/RegexpUtils.h>

#include <base/defines.h>

#include <algorithm>
#include <utility>
#include <vector>


namespace DB
{

namespace
{

/// Returns true if '\' followed by this character means "match this character
/// literally". For example, '\.' matches a literal dot, '\(' matches a
/// literal '(', '\-' matches a literal '-'.
/// Returns false for escape sequences where the matched character is different
/// from what follows '\': '\n' matches a newline (not 'n'), '\d' matches any
/// digit (not 'd'), '\x41' matches 'A' (not 'x').
bool isLiteralEscape(char c)
{
    switch (c)
    {
        case '|':
        case '(':
        case ')':
        case '^':
        case '$':
        case '.':
        case '[':
        case ']':
        case '?':
        case '*':
        case '+':
        case '\\':
        case '{':
        case '}':
        case '-':
            return true;
        default:
            return false;
    }

    UNREACHABLE();
}

}


String extractFixedPrefixFromRegularExpression(const String & regexp)
{
    /// We can only analyze regexes that start with '^' — those are the only ones that guarantee a fixed prefix.
    if (regexp.size() <= 1 || regexp[0] != '^')
        return {};

    String fixed_prefix;
    const char * begin = regexp.data() + 1;
    const char * pos = begin;
    const char * end = regexp.data() + regexp.size();

    while (pos < end)
    {
        switch (*pos)
        {
            case '\0':
                pos = end;
            break;

            case '\\':
            {
                ++pos;
                if (pos == end)
                    break;

                if (isLiteralEscape(*pos))
                {
                    fixed_prefix += *pos;
                    ++pos;
                }
                else
                    pos = end;

                break;
            }

            /// non-trivial cases
            case '|':
                fixed_prefix.clear();
            [[fallthrough]];
            case '(':
            case '[':
            case '^':
            case '$':
            case '.':
            case '+':
                pos = end;
            break;

            /// Quantifiers that allow a zero number of occurrences.
            case '{':
            case '?':
            case '*':
                if (!fixed_prefix.empty())
                    fixed_prefix.pop_back();

            pos = end;
            break;
            default:
                fixed_prefix += *pos;
            pos++;
            break;
        }
    }

    return fixed_prefix;
}

bool expressionHasUnescapedAlternation(const String & expression)
{
    for (size_t i = 0; i < expression.size(); ++i)
    {
        /// \\| is not an alternation, but a literal '|', so skip the next character after a backslash.
        if (expression[i] == '\\' && i + 1 < expression.size())
        {
            ++i;
            continue;
        }
        if (expression[i] == '|')
            return true;
    }
    return false;
}

String extractCommonPrefixFromAlternationBranches(const String & expression)
{
    /// We only handle "^(literal1|literal2|...)$?".
    /// Reject anything that doesn't start with "^(".
    if (expression.size() < 4 || expression[0] != '^' || expression[1] != '(')
        return {};

    const char * pos = expression.data() + 2; /// Start right after "^("
    const char * end = expression.data() + expression.size();

    /// Split branches by '|'. Each branch must be a plain literal —
    /// no metacharacters, no nested groups, no character classes.
    /// If we see anything other than a literal char, escaped char, or '|',
    /// we give up.
    std::vector<String> branches;
    String current_branch;

    while (pos < end)
    {
        if (*pos == '\\' && pos + 1 < end)
        {
            char next = *(pos + 1);
            if (isLiteralEscape(next))
            {
                current_branch += next;
                pos += 2;
            }
            else
                return {};
        }
        else if (*pos == '|')
        {
            /// Branch separator — save the current branch and start a new one.
            branches.push_back(std::move(current_branch));
            current_branch.clear();
            ++pos;
        }
        else if (*pos == ')')
        {
            /// End of the group. Save the last branch.
            branches.push_back(std::move(current_branch));
            ++pos;

            /// Allow only '$' or end of expression after ')'.
            if (pos < end && *pos == '$')
                ++pos;
            if (pos != end)
                return {};

            break;
        }
        else if (
            *pos == '(' || *pos == '[' || *pos == '.' || *pos == '*' || *pos == '+' || *pos == '?' || *pos == '{' || *pos == '^'
            || *pos == '$')
        {
            /// Any metacharacter inside a branch — too complex, give up.
            return {};
        }
        else
        {
            /// Plain literal character.
            current_branch += *pos;
            ++pos;
        }
    }

    if (branches.size() < 2)
        return {};

    /// Compute the longest prefix common to all branches.
    String common_prefix = branches[0];
    for (size_t i = 1; i < branches.size(); ++i)
    {
        size_t common_len = 0;
        size_t max_len = std::min(common_prefix.size(), branches[i].size());
        while (common_len < max_len && common_prefix[common_len] == branches[i][common_len])
            ++common_len;
        common_prefix.resize(common_len);
        if (common_prefix.empty())
            return {};
    }

    return common_prefix;
}

}
