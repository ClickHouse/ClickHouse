#include <Common/RegexpUtils.h>

#include <algorithm>
#include <utility>
#include <vector>


namespace DB
{

namespace
{

/// Returns true if '\' followed by this character matches that character literally, e.g. '\.' matches a dot.
/// Returns false when the escape matches something else: '\n' matches a newline (not 'n'), '\d' matches a digit.
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
}

/// Returns true if the expression contains any unescaped '|'.
bool expressionHasUnescapedAlternation(std::string_view expression)
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

/// Extracts the longest prefix common to all branches of "^(branch1|branch2|...)$?", where every branch
/// is a plain literal, for example "abc-" for "^(abc-xx|abc-yy)". Returns empty for anything more complex:
/// a branch containing a metacharacter or a nested group, a single branch, or a group followed by something
/// other than '$'.
String extractCommonPrefixFromAlternationBranches(std::string_view expression)
{
    if (expression.size() < 4 || expression[0] != '^' || expression[1] != '(')
        return {};

    const char * pos = expression.data() + 2; /// Start right after "^("
    const char * end = expression.data() + expression.size();

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
            branches.push_back(std::move(current_branch));
            current_branch.clear();
            ++pos;
        }
        else if (*pos == ')')
        {
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
            /// A metacharacter inside a branch is too complex to analyze.
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

/// Extracts the prefix and its flags without looking at `requires_perfect_prefix`.
RegexpFixedPrefix extractFixedPrefix(std::string_view regexp)
{
    /// We can only analyze regexes that start with '^' — those are the only ones that guarantee a fixed prefix.
    if (regexp.size() <= 1 || regexp[0] != '^')
        return {};

    /// The scan below stops at '|' and '(', so alternations are analyzed separately.
    /// Their prefix is never perfect: strings starting with the common prefix of all branches
    /// do not necessarily match one of the branches.
    if (expressionHasUnescapedAlternation(regexp))
        return {.prefix = extractCommonPrefixFromAlternationBranches(regexp)};

    String fixed_prefix;
    fixed_prefix.reserve(regexp.size());

    const char * pos = regexp.data() + 1;
    const char * end = regexp.data() + regexp.size();

    while (pos < end)
    {
        switch (*pos)
        {
            case '\\':
            {
                ++pos;
                /// A trailing escape is an invalid pattern which the matcher rejects,
                /// so never report it as exact.
                if (pos == end || !isLiteralEscape(*pos))
                    return {.prefix = fixed_prefix};

                fixed_prefix += *pos;
                ++pos;
                break;
            }

            case '$':
                /// A trailing '$' means the pattern matches `fixed_prefix` and nothing else.
                /// A '$' in the middle constrains the rest of the pattern, which we don't analyze.
                if (pos + 1 == end)
                    return {.prefix = fixed_prefix, .is_exact = true};
                return {.prefix = fixed_prefix};

            case '.':
                /// A trailing ".*" allows any continuation.
                /// A trailing ".*$" also allows any continuation because of
                /// flag OptimizedRegularExpression::RE_DOT_NL (see `Regexps::createRegexp`).
                if ((end - pos == 2 || (end - pos == 3 && *(pos + 2) == '$')) && *(pos + 1) == '*')
                    return {.prefix = fixed_prefix, .is_perfect = true};
                return {.prefix = fixed_prefix};

            /// An unescaped '|' is handled by the alternation path above, so this is unreachable.
            case '|':
                return {};

            /// None of these gives another fixed character.
            case '\0':
            case '(':
            case ')':
            case '[':
            case '^':
            case '+':
                return {.prefix = fixed_prefix};

            /// Quantifiers that allow a zero number of occurrences make the previous character optional.
            case '{':
            case '?':
            case '*':
                if (!fixed_prefix.empty())
                    fixed_prefix.pop_back();
                return {.prefix = fixed_prefix};

            default:
                fixed_prefix += *pos;
                ++pos;
                break;
        }
    }

    /// The whole pattern is a literal string, so it matches every string starting with `fixed_prefix`.
    return {.prefix = fixed_prefix, .is_perfect = true};
}

}


RegexpFixedPrefix extractFixedPrefixFromRegularExpression(std::string_view regexp, bool requires_perfect_prefix)
{
    RegexpFixedPrefix result = extractFixedPrefix(regexp);

    /// An exact prefix is returned even though it is not perfect, see the comment for `is_exact`.
    if (requires_perfect_prefix && !result.is_perfect && !result.is_exact)
        return {};

    return result;
}

}
