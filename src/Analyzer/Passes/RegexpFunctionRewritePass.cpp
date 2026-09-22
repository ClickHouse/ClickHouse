#include <Analyzer/Passes/RegexpFunctionRewritePass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/StringUtils.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_rewrite_regexp_functions;
}

namespace
{

class RegexpFunctionRewriteVisitor : public InDepthQueryTreeVisitorWithContext<RegexpFunctionRewriteVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RegexpFunctionRewriteVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_regexp_functions])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isOrdinaryFunction() || !isString(removeNullable(function_node->getResultType())))
            return;

        /// If a regular expression without alternatives starts with ^ or ends with an unescaped $, rewrite
        /// replaceRegexpAll with replaceRegexpOne.
        if (function_node->getFunctionName() == "replaceRegexpAll" || Poco::toLower(function_node->getFunctionName()) == "regexp_replace")
            handleReplaceRegexpAll(*function_node);

        /// If an extract function has a regexp with some subpatterns and the regexp ends with an
        /// unescaped .*$, remove this suffix.
        if (function_node->getFunctionName() == "extract")
            handleExtract(*function_node);
    }

private:
    /// Returns true if the character at pos is unescaped
    bool isUnescaped(const std::string & str, size_t pos)
    {
        if (pos == 0 || pos >= str.size())
            return true;

        size_t backslash_count = 0;
        for (ssize_t i = static_cast<ssize_t>(pos) - 1; i >= 0 && str[i] == '\\'; --i)
            ++backslash_count;

        return backslash_count % 2 == 0;
    }

    /// Whether the pattern turns the `m` (multiline) flag on with an inline group, `(?m)` or
    /// `(?im:...)`. The scope of such a group is not tracked - any occurrence is enough to decline.
    bool enablesMultiline(const std::string & regexp)
    {
        for (size_t i = 0; i + 2 < regexp.size(); ++i)
        {
            if (regexp[i] != '(' || regexp[i + 1] != '?' || !isUnescaped(regexp, i))
                continue;

            /// re2 flags are `i`, `m`, `s` and `U`, and everything after a `-` is turned off.
            bool negated = false;
            for (size_t j = i + 2; j < regexp.size(); ++j)
            {
                const char flag = regexp[j];
                if (flag == '-')
                    negated = true;
                else if (flag == 'm' && !negated)
                    return true;
                else if (flag != 'i' && flag != 's' && flag != 'U' && flag != 'm')
                    break; /// Not a flag group, or its flag list has ended.
            }
        }
        return false;
    }

    /// One element of a regular expression, in as much detail as is needed to tell whether the
    /// pattern's tail can match the empty string.
    struct RegexpAtom
    {
        bool zero_width = false; /// An assertion such as `\b`: always matches the empty string and never consumes.
        bool nullable = false; /// Can match the empty string, e.g. `o*`, or is not understood well enough to tell.
        bool is_dollar = false; /// The element is a bare `$` anchor, not quantified and not quoted by `\Q...\E`.
    };

    /// The result of tokenizing a pattern that is not understood: a single element that can match
    /// the empty string, which only ever makes the caller decline the rewrite.
    static std::vector<RegexpAtom> giveUp()
    {
        return {RegexpAtom{.zero_width = false, .nullable = true}};
    }

    /// Whether the group that spans `[begin, end)` is an inline flag group, `(?i)`, rather than a
    /// capturing or a non-capturing one.
    static bool isInlineFlagGroup(const std::string & regexp, size_t begin, size_t end)
    {
        if (end - begin < 4 || regexp[begin + 1] != '?')
            return false;

        for (size_t i = begin + 2; i + 1 < end; ++i)
        {
            const char flag = regexp[i];
            if (flag != 'i' && flag != 'm' && flag != 's' && flag != 'U' && flag != '-')
                return false;
        }
        return true;
    }

    /// Split a pattern into its top-level elements. Deliberately conservative: an element that is
    /// not recognized, as well as anything that makes the shape of the pattern unclear, yields a
    /// single nullable atom, which only makes the caller decline the rewrite.
    static std::vector<RegexpAtom> tokenizeRegexp(const std::string & regexp)
    {
        std::vector<RegexpAtom> atoms;
        size_t i = 0;
        while (i < regexp.size())
        {
            RegexpAtom atom;
            const char c = regexp[i];

            if (c == '\\' && i + 1 < regexp.size() && regexp[i + 1] == 'Q')
            {
                /// `\Q...\E` quotes its body: every byte in it is a literal, including `$`, and an
                /// unterminated `\Q` quotes up to the end of the pattern. The body is a run of
                /// literals, so it consumes - unless it is empty, in which case the whole construct
                /// matches the empty string without consuming, exactly like an assertion.
                size_t j = i + 2;
                size_t body_size = 0;
                while (j < regexp.size())
                {
                    if (j + 1 < regexp.size() && regexp[j] == '\\' && regexp[j + 1] == 'E')
                    {
                        j += 2;
                        break;
                    }
                    ++body_size;
                    ++j;
                }
                atom.zero_width = body_size == 0;
                i = j;
            }
            else if (c == '\\' && i + 1 < regexp.size() && regexp[i + 1] == 'E')
            {
                /// A `\E` without a matching `\Q`. re2 rejects such a pattern outright, so the query
                /// throws either way; zero-width is the conservative reading of it here.
                atom.zero_width = true;
                i += 2;
            }
            else if (c == '\\')
            {
                if (i + 1 >= regexp.size())
                    return giveUp(); /// A trailing backslash - the pattern is not even valid.

                /// re2's zero-width escapes. Everything else, such as `\d` or `\.`, consumes a character,
                /// but not every escape is two bytes long: `\x{41}`, `\x41`, `\p{Greek}`, `\pL` and the
                /// octal `\101` are single literals or classes that span more bytes. Reading only two
                /// would leave `{41}` to be mistaken for a repetition and `?` for its non-greedy modifier.
                const char escaped = regexp[i + 1];
                atom.zero_width = escaped == 'b' || escaped == 'B' || escaped == 'A' || escaped == 'z' || escaped == 'Z';
                i += 2;

                if (escaped == 'x')
                {
                    if (i < regexp.size() && regexp[i] == '{')
                    {
                        /// `\x{HHHH}`.
                        const size_t closing = regexp.find('}', i + 1);
                        if (closing == std::string::npos)
                            return giveUp(); /// Unterminated.
                        i = closing + 1;
                    }
                    else
                    {
                        /// `\xHH`.
                        if (i + 2 > regexp.size() || !isHexDigit(regexp[i]) || !isHexDigit(regexp[i + 1]))
                            return giveUp(); /// Not a valid escape, re2 rejects it anyway.
                        i += 2;
                    }
                }
                else if (escaped == 'p' || escaped == 'P')
                {
                    /// A Unicode class: `\pL` or `\p{Greek}`, with `\P` for the negation.
                    if (i >= regexp.size())
                        return giveUp();
                    if (regexp[i] == '{')
                    {
                        const size_t closing = regexp.find('}', i + 1);
                        if (closing == std::string::npos)
                            return giveUp(); /// Unterminated.
                        i = closing + 1;
                    }
                    else
                    {
                        ++i;
                    }
                }
                else if (escaped >= '0' && escaped <= '7')
                {
                    /// An octal character code of up to three digits.
                    size_t digits = 1;
                    while (digits < 3 && i < regexp.size() && regexp[i] >= '0' && regexp[i] <= '7')
                    {
                        ++digits;
                        ++i;
                    }
                }
            }
            else if (c == '^' || c == '$')
            {
                atom.zero_width = true;
                atom.is_dollar = c == '$';
                ++i;
            }
            else if (c == '[')
            {
                /// A character class. `]` right after `[` or `[^` is a literal, not the terminator.
                size_t j = i + 1;
                if (j < regexp.size() && regexp[j] == '^')
                    ++j;
                if (j < regexp.size() && regexp[j] == ']')
                    ++j;
                while (j < regexp.size() && regexp[j] != ']')
                    j += regexp[j] == '\\' ? 2 : 1;
                if (j >= regexp.size())
                    return giveUp(); /// Unterminated.
                i = j + 1;
            }
            else if (c == '(')
            {
                /// A group. Its contents are not analyzed: an inline flag group is zero-width, and
                /// anything else is treated as nullable, because its body could be nullable in turn.
                size_t j = i + 1;
                size_t depth = 1;
                while (j < regexp.size() && depth > 0)
                {
                    if (regexp[j] == '\\')
                        ++j;
                    else if (regexp[j] == '(')
                        ++depth;
                    else if (regexp[j] == ')')
                        --depth;
                    ++j;
                }
                if (depth > 0)
                    return giveUp(); /// Unbalanced.
                atom.zero_width = isInlineFlagGroup(regexp, i, j);
                atom.nullable = !atom.zero_width;
                i = j;
            }
            else if (c == '|' || c == ')' || c == '*' || c == '+' || c == '?' || c == '{')
            {
                /// An alternation, or a quantifier where an element was expected.
                return giveUp();
            }
            else
            {
                ++i; /// An ordinary character.
            }

            /// A quantifier binds to the element that has just been read. A quantified `$` is no
            /// longer an anchor that the pattern must end at, so it does not count as one.
            const size_t element_end = i;
            if (i < regexp.size() && (regexp[i] == '*' || regexp[i] == '?'))
            {
                atom.nullable = true;
                ++i;
            }
            else if (i < regexp.size() && regexp[i] == '+')
            {
                ++i; /// At least one repetition, so the atom keeps having to consume.
            }
            else if (i < regexp.size() && regexp[i] == '{')
            {
                /// `{n}`, `{n,}` or `{n,m}`: only a lower bound of zero makes the element nullable.
                size_t j = i + 1;
                size_t lower_bound = 0;
                bool has_digits = false;
                while (j < regexp.size() && isNumericASCII(regexp[j]))
                {
                    lower_bound = lower_bound * 10 + static_cast<size_t>(regexp[j] - '0');
                    has_digits = true;
                    ++j;
                }
                if (j < regexp.size() && regexp[j] == ',')
                {
                    ++j;
                    while (j < regexp.size() && isNumericASCII(regexp[j]))
                        ++j;
                }
                if (!has_digits || j >= regexp.size() || regexp[j] != '}')
                    return giveUp(); /// Not a repetition after all - re2 takes `{` literally here.
                if (lower_bound == 0)
                    atom.nullable = true;
                i = j + 1;
            }

            /// A quantifier can be made non-greedy, which does not change what it can match.
            if (i < regexp.size() && regexp[i] == '?')
                ++i;

            if (i != element_end)
                atom.is_dollar = false;

            atoms.push_back(atom);
        }
        return atoms;
    }

    /// Whether the pattern ends with a `$` anchor and has to consume a character before it, so that
    /// it can match at most once, at the end of the subject.
    /// A global replace resumes right after a match, so a pattern that can also match the empty
    /// string at the end of the subject matches there once more, which a single replace never does:
    /// `replaceRegexpAll('foo', 'o*$', 'Z')` is `fZZ`, while `replaceRegexpOne` gives `fZ`. Trailing
    /// zero-width assertions consume nothing and are looked through, so `a?\b$` is nullable just
    /// like `a?$`. A `$` that is quoted by `\Q...\E` or quantified is a literal, not an anchor.
    static bool endsWithRequiredDollarAnchor(const std::string & regexp)
    {
        auto atoms = tokenizeRegexp(regexp);

        if (atoms.empty() || !atoms.back().is_dollar)
            return false;

        /// Drop the trailing `$` together with every assertion in front of it.
        while (!atoms.empty() && atoms.back().zero_width)
            atoms.pop_back();

        if (atoms.empty())
            return false; /// Nothing but assertions, so the pattern matches the empty string.

        return !atoms.back().nullable;
    }
    /// Whether a `\Q` quoted section reaches the end of the pattern. re2 treats everything after
    /// `\Q` as literal text, up to a closing `\E` or the end of the pattern, so trailing `.*$` or
    /// `$` bytes inside such a section are ordinary characters and not regexp syntax: they neither
    /// anchor the match nor can be removed. The `extract` rewrite below decides from raw bytes
    /// whether the pattern ends with syntax, so it has to decline for such a pattern. (The
    /// `replaceRegexpAll` rewrite tokenizes the pattern instead and sees the quoted `$` for itself.)
    static bool endsInsideQuotedLiteral(const std::string & regexp)
    {
        size_t i = 0;
        while (i + 1 < regexp.size())
        {
            if (regexp[i] != '\\')
            {
                ++i;
                continue;
            }

            if (regexp[i + 1] != 'Q')
            {
                /// An ordinary escape sequence - both of its bytes are consumed, so that the
                /// second backslash of `\\Q` does not open a quoted section.
                i += 2;
                continue;
            }

            /// Inside a quoted section a backslash is a literal byte of its own, and only the
            /// exact two-byte sequence `\E` closes the section.
            i += 2;
            while (i + 1 < regexp.size() && !(regexp[i] == '\\' && regexp[i + 1] == 'E'))
                ++i;

            if (i + 1 >= regexp.size())
                return true;

            i += 2;
        }
        return false;
    }

    /// Whether the pattern turns the `s` (dot matches a newline) flag off with an inline group,
    /// `(?-s)` or `(?i-s:...)`. ClickHouse compiles regexp functions with `dot_nl` on, so `.`
    /// normally matches a newline and a trailing `.*$` never constrains the match; with the flag
    /// off it does. The scope of such a group is not tracked - any occurrence is enough to decline.
    static bool disablesDotAll(const std::string & regexp)
    {
        for (size_t i = 0; i + 2 < regexp.size(); ++i)
        {
            if (regexp[i] != '(' || regexp[i + 1] != '?')
                continue;

            /// re2 flags are `i`, `m`, `s` and `U`, and everything after a `-` is turned off.
            bool negated = false;
            for (size_t j = i + 2; j < regexp.size(); ++j)
            {
                const char flag = regexp[j];
                if (flag == '-')
                    negated = true;
                else if (flag == 's' && negated)
                    return true;
                else if (flag != 'i' && flag != 's' && flag != 'U' && flag != 'm')
                    break; /// Not a flag group, or its flag list has ended.
            }
        }
        return false;
    }

    bool handleReplaceRegexpAll(FunctionNode & function_node)
    {
        auto & function_node_arguments_nodes = function_node.getArguments().getNodes();
        if (function_node_arguments_nodes.size() != 3)
            return false;

        const auto * constant_node = function_node_arguments_nodes[1]->as<ConstantNode>();
        if (!constant_node)
            return false;

        if (auto constant_type = constant_node->getResultType(); !isString(constant_type))
            return false;

        String regexp = constant_node->getValue().safeGet<String>();
        if (regexp.empty())
            return false;

        /// A `^` at the very start cannot be quoted - a `\Q` would have to precede it - so it is
        /// always an anchor, and the pattern can only match at offset 0. A pattern anchored only by
        /// a trailing `$` matches once at the end - if the `$` is an anchor rather than quoted or
        /// quantified text, and unless the pattern can also match the empty string there, in which
        /// case a global replace replaces twice: `replaceRegexpAll('foo', 'o*$', 'Z')` is `fZZ`,
        /// while `replaceRegexpOne` gives `fZ`.
        const bool starts_with_caret = regexp.front() == '^';
        if (!starts_with_caret && !endsWithRequiredDollarAnchor(regexp))
            return false;

        /// An inline `m` flag makes `^` and `$` match at every line boundary rather than only at the
        /// ends of the subject, so the pattern can match once per line and neither anchor proves a
        /// single match: `replaceRegexpAll` over `(?m)a$` replaces every line's `a`, while
        /// `replaceRegexpOne` replaces only the first line's.
        if (enablesMultiline(regexp))
            return false;

        /// Analyze the regular expression to detect presence of alternatives (e.g., 'a|b'). If any alternatives are
        /// found, return false to indicate the regexp is not suitable for optimization.
        RegexpAnalysisResult result = OptimizedRegularExpression::analyze(regexp);
        if (!result.alternatives.empty())
            return false;

        resolveOrdinaryFunctionNodeByName(function_node, "replaceRegexpOne", getContext());
        return true;
    }

    void handleExtract(FunctionNode & function_node)
    {
        auto & function_node_arguments_nodes = function_node.getArguments().getNodes();
        if (function_node_arguments_nodes.size() != 2)
            return;

        const auto * constant_node = function_node_arguments_nodes[1]->as<ConstantNode>();
        if (!constant_node)
            return;

        if (auto constant_type = constant_node->getResultType(); !isString(constant_type))
            return;

        String regexp = constant_node->getValue().safeGet<String>();

        /// A NUL (`\0`) byte is an ordinary literal byte in the pattern (re2 is binary-safe), and the
        /// analyzer no longer stops at it, so captures placed after a NUL are now visible here. Be
        /// conservative and skip the rewrite for patterns containing a NUL, so that fix does not
        /// change `extract` results for such patterns.
        if (regexp.contains('\0'))
            return;

        RegexpAnalysisResult result = OptimizedRegularExpression::analyze(regexp);
        if (!result.has_capture)
            return;

        /// Only a trailing `.*$` is removed. Leftmost-first matching fixes where the match starts
        /// before the tail is considered, so dropping the tail cannot change what is captured.
        ///
        /// A leading greedy `^.*` must be left alone: it consumes as much as it can and then
        /// backtracks, so the capture binds at the *last* offset where the rest of the pattern
        /// matches, while the stripped pattern binds at the first one.
        /// `extract('a1b2c3', '^.*(\d)')` is `3` - the idiomatic "last digit" pattern - while
        /// `extract('a1b2c3', '(\d)')` is `1`.
        ///
        /// The tail is only free of consequences while `.` matches a newline. ClickHouse compiles
        /// regexp functions with `dot_nl` on, but an inline `(?-s)` turns it back off, and then
        /// `.*$` cannot cross a newline: it pins the match to the last line, and dropping it moves
        /// the capture to an earlier one.
        ///
        /// The tail also has to be regexp syntax rather than literal text: an unterminated `\Q`
        /// quotes it, and then `.*$` are three ordinary characters to match.
        ///
        /// For simplicity, this optimization ignores alternations.
        if (regexp.size() >= 3 && regexp.ends_with(".*$") && isUnescaped(regexp, regexp.size() - 3) && !disablesDotAll(regexp)
            && !endsInsideQuotedLiteral(regexp))
        {
            regexp = regexp.substr(0, regexp.size() - 3);
            function_node_arguments_nodes[1] = std::make_shared<ConstantNode>(std::move(regexp));
        }
    }
};

}

void RegexpFunctionRewritePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RegexpFunctionRewriteVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
