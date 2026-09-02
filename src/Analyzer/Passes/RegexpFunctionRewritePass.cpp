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

        /// If an extract function has a regexp with some subpatterns and the regexp starts with ^.* or ending with an
        /// unescaped .*$, remove this prefix and/or suffix.
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
    };

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
        const std::vector<RegexpAtom> give_up = {RegexpAtom{.zero_width = false, .nullable = true}};

        std::vector<RegexpAtom> atoms;
        size_t i = 0;
        while (i < regexp.size())
        {
            RegexpAtom atom;
            const char c = regexp[i];

            if (c == '\\')
            {
                if (i + 1 >= regexp.size())
                    return give_up; /// A trailing backslash - the pattern is not even valid.

                /// re2's zero-width escapes. Everything else, such as `\d` or `\.`, consumes a character.
                const char escaped = regexp[i + 1];
                atom.zero_width = escaped == 'b' || escaped == 'B' || escaped == 'A' || escaped == 'z' || escaped == 'Z';
                i += 2;
            }
            else if (c == '^' || c == '$')
            {
                atom.zero_width = true;
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
                    return give_up; /// Unterminated.
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
                    return give_up; /// Unbalanced.
                atom.zero_width = isInlineFlagGroup(regexp, i, j);
                atom.nullable = !atom.zero_width;
                i = j;
            }
            else if (c == '|' || c == ')' || c == '*' || c == '+' || c == '?' || c == '{')
            {
                /// An alternation, or a quantifier where an element was expected.
                return give_up;
            }
            else
            {
                ++i; /// An ordinary character.
            }

            /// A quantifier binds to the element that has just been read.
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
                    return give_up; /// Not a repetition after all - re2 takes `{` literally here.
                if (lower_bound == 0)
                    atom.nullable = true;
                i = j + 1;
            }

            /// A quantifier can be made non-greedy, which does not change what it can match.
            if (i < regexp.size() && regexp[i] == '?')
                ++i;

            atoms.push_back(atom);
        }
        return atoms;
    }

    /// Whether a pattern that ends with an unescaped `$` can also match the empty string there.
    /// A global replace resumes right after a match, so it can match the empty string at the end of
    /// the subject once more, which a single replace never does: `replaceRegexpAll('foo', 'o*$', 'Z')`
    /// is `fZZ`, while `replaceRegexpOne` gives `fZ`. Trailing zero-width assertions consume nothing
    /// and are looked through, so `a?\b$` is nullable just like `a?$`.
    static bool canMatchEmptyBeforeDollar(const std::string & regexp)
    {
        auto atoms = tokenizeRegexp(regexp);

        /// Drop the trailing `$` together with every assertion in front of it.
        while (!atoms.empty() && atoms.back().zero_width)
            atoms.pop_back();

        if (atoms.empty())
            return true; /// Nothing but assertions, so the pattern matches the empty string.

        return atoms.back().nullable;
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

        bool starts_with_caret = regexp.front() == '^';
        bool ends_with_unescaped_dollar = false;

        if (!regexp.empty() && regexp.back() == '$')
            ends_with_unescaped_dollar = isUnescaped(regexp, regexp.size() - 1);

        if (!starts_with_caret && !ends_with_unescaped_dollar)
            return false;

        /// An inline `m` flag makes `^` and `$` match at every line boundary rather than only at the
        /// ends of the subject, so the pattern can match once per line: `replaceRegexpAll` over
        /// `(?m)a$` replaces every line's `a`, `replaceRegexpOne` only the first line's.
        if (enablesMultiline(regexp))
            return false;

        /// A `^`-anchored pattern can only match at offset 0, so replacing all and replacing one are
        /// the same. A pattern anchored only by a trailing `$` matches once at the end - unless it
        /// can also match the empty string there, in which case a global replace replaces twice:
        /// `replaceRegexpAll('foo', 'o*$', 'Z')` is `fZZ`, while `replaceRegexpOne` gives `fZ`.
        if (!starts_with_caret && canMatchEmptyBeforeDollar(regexp))
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
        /// analyzer no longer stops at it, so captures placed after a NUL are now visible here. The
        /// `^.*` prefix removal below changes which occurrence is captured when the part after the
        /// prefix can match at more than one offset (greedy `^.*` selects the last occurrence, while
        /// the stripped pattern selects the first). Be conservative and skip the rewrite for patterns
        /// containing a NUL, so this fix does not change `extract` results for such patterns.
        if (regexp.contains('\0'))
            return;

        RegexpAnalysisResult result = OptimizedRegularExpression::analyze(regexp);
        if (!result.has_capture)
            return;

        /// For simplicity, this optimization ignores alternations and only considers anchoring at the start or end of the pattern.
        bool starts_with_caret_dot_star = regexp.starts_with("^.*") && !regexp.starts_with("^.*?");
        bool ends_with_unescaped_dot_star_dollar = false;

        if (regexp.size() >= 3 && regexp.ends_with(".*$"))
        {
            size_t dot_pos = regexp.size() - 3;
            ends_with_unescaped_dot_star_dollar = isUnescaped(regexp, dot_pos);
        }

        if (starts_with_caret_dot_star || ends_with_unescaped_dot_star_dollar)
        {
            if (starts_with_caret_dot_star)
                regexp = regexp.substr(3);
            if (ends_with_unescaped_dot_star_dollar && regexp.ends_with(".*$"))
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
