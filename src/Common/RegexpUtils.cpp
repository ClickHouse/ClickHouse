#include <Common/RegexpUtils.h>
#include <Common/UTF8Helpers.h>

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
/// is a plain literal, for example "abc-" for "^(abc-xx|abc-yy)". The group may be non-capturing,
/// "^(?:branch1|branch2|...)$?", which is how `fullMatch` and PromQL matchers anchor a top-level alternation.
/// Returns empty for anything more complex: a branch containing a metacharacter or a nested group,
/// a single branch, or a group followed by something other than '$'.
String extractCommonPrefixFromAlternationBranches(std::string_view expression)
{
    if (expression.size() < 4 || expression[0] != '^' || expression[1] != '(')
        return {};

    const char * pos = expression.data() + 2; /// Start right after "^("
    const char * end = expression.data() + expression.size();

    /// A non-capturing group "^(?:" matches the same strings; any other "(?" form is rejected below.
    if (pos + 1 < end && pos[0] == '?' && pos[1] == ':')
        pos += 2;

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

}

/// A quantifier that allows zero occurrences makes the literal before it optional. RE2 compiles a
/// valid UTF-8 pattern in UTF-8 mode, where such a literal is a whole code point, so the whole code
/// point has to go: `^é*c` requires nothing, while dropping one byte leaves the malformed prefix
/// `0xC3` which no matching string starts with. A pattern that is not valid UTF-8 falls back to
/// Latin-1, where the quantifier applies to the last byte only; dropping the continuation bytes in
/// front of it as well merely weakens the prefix, which stays correct.
void dropLastLiteral(String & fixed_prefix)
{
    if (fixed_prefix.empty())
        return;

    size_t new_size = fixed_prefix.size() - 1;
    while (new_size > 0 && UTF8::isContinuationOctet(static_cast<UInt8>(fixed_prefix[new_size])))
        --new_size;

    fixed_prefix.resize(new_size);
}

/// Whether `pos` points at a quantifier that allows zero occurrences of whatever precedes it.
bool isZeroAllowingQuantifier(const char * pos, const char * end)
{
    if (pos >= end)
        return false;

    if (*pos == '*' || *pos == '?')
        return true;

    /// `{0}` and `{0,n}` allow zero occurrences; every other `{...}` requires at least one.
    return *pos == '{' && pos + 1 < end && pos[1] == '0';
}

/// The position after a pure inline flag group - `(?i)`, `(?-i)`, `(?ims)`, or `(?)` with no flags - which RE2 treats as a
/// zero-width operator. Returns nullptr when `pos` does not point at such a group; `(?i:x)` and `()`
/// are groups with a body instead, and a quantifier after those applies to the group itself.
static const char * findEndOfPureFlagGroup(const char * pos, const char * end)
{
    if (pos + 2 >= end || pos[1] != '?')
        return nullptr;

    auto is_flag_char = [](char c) { return c == 'i' || c == 'm' || c == 's' || c == 'U' || c == 'u' || c == '-'; };

    const char * it = pos + 2;
    while (it < end && is_flag_char(*it))
        ++it;

    if (it == end || *it != ')')
        return nullptr;

    return it + 1;
}

/// The position after a chain of zero-width constructs - pure inline flag groups and empty `\Q\E`
/// quotes - that starts at `pos`. RE2 keeps the atom before such a chain on the parser stack, so a
/// quantifier written after the whole chain applies to that atom: `^ab(?i)(?m)*c` requires only `a`.
/// Returns `pos` itself when there is no zero-width construct at `pos`.
const char * skipZeroWidthConstructs(const char * pos, const char * end)
{
    while (pos < end)
    {
        if (*pos == '(')
        {
            const char * after_flag_group = findEndOfPureFlagGroup(pos, end);
            if (!after_flag_group)
                break;
            pos = after_flag_group;
        }
        else if (*pos == '\\' && pos + 3 < end && pos[1] == 'Q' && pos[2] == '\\' && pos[3] == 'E')
            pos += 4;
        else
            break;
    }

    return pos;
}

namespace
{

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
                {
                    /// An empty `\Q\E` quote is zero-width, like a pure flag group, so a quantifier
                    /// written after it - or after a whole chain of such constructs - applies to the
                    /// character before it: both `^ab\Q\E*c` and `^ab\Q\E(?i)*c` require only `a`.
                    if (isZeroAllowingQuantifier(skipZeroWidthConstructs(pos - 1, end), end))
                        dropLastLiteral(fixed_prefix);

                    return {.prefix = fixed_prefix};
                }

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

            case '(':
                /// A pure inline flag group such as `(?i)` is a zero-width operator for RE2: it
                /// leaves nothing for a quantifier to apply to, so a quantifier written after it
                /// applies to the character before the group, and the same holds for a whole chain
                /// of such constructs. `^ab(?i)*c` and `^ab(?i)(?m)*c` therefore require only `a`,
                /// and reporting `ab` as required pruned granules holding matching rows.
                if (const char * after_zero_width = skipZeroWidthConstructs(pos, end); after_zero_width != pos)
                {
                    if (isZeroAllowingQuantifier(after_zero_width, end))
                        dropLastLiteral(fixed_prefix);
                }
                return {.prefix = fixed_prefix};

            /// None of these gives another fixed character.
            case ')':
            case '[':
            case '^':
            case '+':
                return {.prefix = fixed_prefix};

            /// Quantifiers that allow a zero number of occurrences make the previous literal optional.
            case '{':
            case '?':
            case '*':
                dropLastLiteral(fixed_prefix);
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

String anchorRegexpToMatchWholeString(std::string_view regexp)
{
    size_t depth = 0;
    bool in_class = false;
    bool after_range_start = false;
    bool in_quote = false;
    bool top_level_alternation = false;
    bool top_level_multiline = false;
    bool ends_with_anchor = false;
    bool trailing_escape = false;
    for (size_t pos = 0; pos < regexp.size(); ++pos)
    {
        char c = regexp[pos];
        ends_with_anchor = false;
        if (in_quote)
        {
            /// Everything after `\Q` is literal up to `\E` or the end of the regex.
            if (c == '\\' && pos + 1 < regexp.size() && regexp[pos + 1] == 'E')
            {
                in_quote = false;
                ++pos;
            }
        }
        else if (c == '\\')
        {
            if (pos + 1 < regexp.size() && regexp[pos + 1] == 'Q')
                in_quote = true;
            trailing_escape = pos + 1 >= regexp.size();
            ++pos;
            if (in_class && pos < regexp.size())
            {
                /// A class such as `\d` or `\p{Greek}` can not start a range, unlike an escaped character.
                after_range_start = std::string_view("dDsSwWpP").find(regexp[pos]) == std::string_view::npos;
                if ((regexp[pos] == 'p' || regexp[pos] == 'P') && pos + 1 < regexp.size())
                    pos = regexp[pos + 1] == '{' ? std::min(regexp.find('}', pos), regexp.size()) : pos + 1;
            }
        }
        else if (in_class)
        {
            if (c == ']')
                in_class = false;
            else if (c == '[' && pos + 1 < regexp.size() && regexp[pos + 1] == ':')
            {
                /// A POSIX named class such as `[:alpha:]` is a part of the enclosing class, its `]` does not close it.
                /// RE2 treats `[:` without a following `:]` as literal characters.
                if (size_t end_of_named_class = regexp.find(":]", pos + 2); end_of_named_class != String::npos)
                    pos = end_of_named_class + 1;
                after_range_start = false;
            }
            else if (c == '-' && after_range_start && pos + 1 < regexp.size() && regexp[pos + 1] != ']')
            {
                /// The end of a range is a single character, so RE2 reads `[` in `[!-[:]` as a literal, not as a named class.
                pos += regexp[pos + 1] == '\\' ? 2 : 1;
                after_range_start = false;
            }
            else
                after_range_start = true;
        }
        else if (c == '[')
        {
            in_class = true;
            after_range_start = false;
            /// A `]` right after `[` or `[^` is literal.
            if (pos + 1 < regexp.size() && regexp[pos + 1] == '^')
                ++pos;
            if (pos + 1 < regexp.size() && regexp[pos + 1] == ']')
                ++pos;
        }
        else if (c == '(')
        {
            /// An inline flag group like `(?m)` at the top level would also apply to the `$` appended after the regex,
            /// and in the multiline mode `$` matches before every newline, so the regex must be wrapped into a group.
            if (depth == 0 && pos + 1 < regexp.size() && regexp[pos + 1] == '?')
            {
                bool negated = false;
                bool multiline = false;
                size_t flag_pos = pos + 2;
                for (; flag_pos < regexp.size(); ++flag_pos)
                {
                    char flag = regexp[flag_pos];
                    if (flag == '-')
                        negated = true;
                    else if (flag == 'm')
                        multiline = !negated;
                    else if (flag != 'i' && flag != 's' && flag != 'U')
                        break;
                }
                if (multiline && flag_pos < regexp.size() && regexp[flag_pos] == ')')
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

    String res{regexp};
    /// A quote can extend to the end of the regex, and it must not swallow the anchor or the group added after it.
    if (in_quote)
        res += "\\E";

    /// The added anchors must not become a part of the regex's own syntax: a quantifier at the position
    /// of the leading '^' (e.g. "^?a", "+a", "^(?i)*a") would apply to the anchor, and a trailing
    /// backslash would escape the appended '$'. Wrap such regexes into a group too — they have no fixed
    /// prefix anyway, so the prefix extraction loses nothing.
    const char * end = res.data() + res.size();
    const char * after_start_anchor = skipZeroWidthConstructs(res.data() + (res.starts_with('^') ? 1 : 0), end);
    const bool quantifier_after_start_anchor = after_start_anchor < end
        && (*after_start_anchor == '*' || *after_start_anchor == '?' || *after_start_anchor == '+' || *after_start_anchor == '{');

    /// The non-capturing group isolates the regex from the added anchors.
    if (top_level_alternation || top_level_multiline || trailing_escape || quantifier_after_start_anchor)
        return "^(?:" + res + ")$";

    if (!res.starts_with('^'))
        res = '^' + res;
    if (!ends_with_anchor)
        res += '$';
    return res;
}

}
