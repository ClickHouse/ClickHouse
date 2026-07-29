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

/// Handles the simple alternation pattern "^(branch1|branch2|...)$?" where
/// each branch is a plain literal string (no metacharacters, no nesting).
///
/// This is used when the expression contains an unescaped '|', meaning the
/// character-by-character scan cannot be used (it would stop at '|' or '(').
/// Returns empty for any pattern more complex than simple literal branches
/// inside a single group.
///
/// "^(abc-xx|abc-yy)"
///   The '|' gives two alternatives: the string starts with "abc-xx" or "abc-yy".
///   Both start with "abc-", so every matching string begins with "abc-".
///   Prefix: "abc-".
///
/// "^(abc-xx-1|abc-xx-2|abc-yy-1)"
///   Three alternatives. All three start with "abc-", but they diverge
///   after that ('x' vs 'y'). So "abc-" is the longest common start.
///   Prefix: "abc-".
///
/// "^(abc|def)"
///   Two alternatives: "abc" and "def". They share nothing at the start —
///   'a' vs 'd' already differ. We cannot guarantee any prefix.
///   Prefix: "".
///
/// "^(abc|def)$"
///   '$' means "must end at the end of the string". It constrains what
///   comes after the match, but does not change what the string starts
///   with. So the prefix analysis is the same as without '$'.
///   Prefix: "".
///
/// Not supported (returns empty — could be improved in the future):
///
/// "^(abc.*|abd.*)"
///   Branches contain '.*' (wildcard). We only handle plain literal branches.
///   The common prefix "ab" could theoretically be extracted, but is not.
///   Prefix: "".
///
/// "^(abc|abd)+"
///   The '+' after the group means it must appear at least once.
///   We only handle patterns where the group is followed by '$' or end
///   of expression. Prefix: "".
///
/// "^(abc(1|2)|abc(3|4))"
///   Branches contain nested groups. We only handle flat literal branches.
///   The common prefix "abc" could theoretically be extracted, but is not.
///   Prefix: "".
String extractCommonPrefixFromAlternationBranches(std::string_view expression)
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

/// Extracts {fixed_prefix, is_perfect_prefix, is_exact} without looking at `requires_perfect_prefix`.
std::tuple<String, bool, bool> extractFixedPrefix(std::string_view regexp)
{
    /// We can only analyze regexes that start with '^' — those are the only ones that guarantee a fixed prefix.
    if (regexp.size() <= 1 || regexp[0] != '^')
        return {"", /* perfect = */ false, /* exact = */ false};

    /// The scan below stops at '|' and '(', so alternations are analyzed separately.
    /// Their prefix is never perfect: strings starting with the common prefix of all branches
    /// do not necessarily match one of the branches.
    if (expressionHasUnescapedAlternation(regexp))
        return {extractCommonPrefixFromAlternationBranches(regexp), /* perfect = */ false, /* exact = */ false};

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
                    return {fixed_prefix, /* perfect = */ false, /* exact = */ false};

                fixed_prefix += *pos;
                ++pos;
                break;
            }

            case '$':
                /// '$' means "must end at the end of the string", so "^abc$" matches only "abc".
                /// A '$' in the middle constrains the rest of the pattern, which we don't analyze.
                if (pos + 1 == end)
                    return {fixed_prefix, /* perfect = */ false, /* exact = */ true};
                return {fixed_prefix, /* perfect = */ false, /* exact = */ false};

            case '.':
                /// A trailing ".*" allows any continuation.
                /// A trailing ".*$" also allows any continuation because of
                /// flag OptimizedRegularExpression::RE_DOT_NL (see `Regexps::createRegexp`).
                if ((end - pos == 2 || (end - pos == 3 && *(pos + 2) == '$')) && *(pos + 1) == '*')
                    return {fixed_prefix, /* perfect = */ true, /* exact = */ false};
                return {fixed_prefix, /* perfect = */ false, /* exact = */ false};

            /// An unescaped '|' is handled by the alternation path above, so this is unreachable.
            case '|':
                return {"", /* perfect = */ false, /* exact = */ false};

            /// None of these gives another fixed character.
            case '\0':
            case '(':
            case '[':
            case '^':
            case '+':
                return {fixed_prefix, /* perfect = */ false, /* exact = */ false};

            /// Quantifiers that allow a zero number of occurrences make the previous character optional.
            case '{':
            case '?':
            case '*':
                if (!fixed_prefix.empty())
                    fixed_prefix.pop_back();
                return {fixed_prefix, /* perfect = */ false, /* exact = */ false};

            default:
                fixed_prefix += *pos;
                ++pos;
                break;
        }
    }

    /// The whole pattern is a literal string and nothing constrains the end of the matched string,
    /// so "^abc" matches every string starting with "abc".
    return {fixed_prefix, /* perfect = */ true, /* exact = */ false};
}

}


std::tuple<String, bool, bool> extractFixedPrefixFromRegularExpression(std::string_view regexp, bool requires_perfect_prefix)
{
    auto [fixed_prefix, is_perfect_prefix, is_exact] = extractFixedPrefix(regexp);

    /// An exact prefix is not a perfect one, but it is returned anyway: it describes the set of
    /// matching strings even more precisely, so a caller which needs a perfect prefix can use it too.
    if (requires_perfect_prefix && !is_perfect_prefix && !is_exact)
        return {"", /* perfect = */ false, /* exact = */ false};

    return {fixed_prefix, is_perfect_prefix, is_exact};
}

}
