#include <Analyzer/Passes/RegexpFunctionRewritePass.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Common/OptimizedRegularExpression.h>
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

        bool starts_with_caret = regexp.front() == '^';
        bool ends_with_unescaped_dollar = false;

        if (!regexp.empty() && regexp.back() == '$')
            ends_with_unescaped_dollar = isUnescaped(regexp, regexp.size() - 1);

        if (!starts_with_caret && !ends_with_unescaped_dollar)
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
        /// For simplicity, this optimization ignores alternations.
        if (regexp.size() >= 3 && regexp.ends_with(".*$") && isUnescaped(regexp, regexp.size() - 3) && !disablesDotAll(regexp))
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
