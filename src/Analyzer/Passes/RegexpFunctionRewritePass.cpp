#include <Analyzer/Passes/RegexpFunctionRewritePass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Common/OptimizedRegularExpression.h>
#include <Core/Settings.h>
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
        if (!function_node || !function_node->isOrdinaryFunction() || !isString(function_node->getResultType()))
            return;

        /// If a regular expression without alternatives starts with ^ or ends with an unescaped $, rewrite
        /// replaceRegexpAll with replaceRegexpOne.
        if (function_node->getFunctionName() == "replaceRegexpAll" || Poco::toLower(function_node->getFunctionName()) == "regexp_replace")
        {
            if (!handleReplaceRegexpAll(*function_node))
                return;

            /// After optimization, function_node might now be "replaceRegexpOne", so continue processing
        }

        /// If a replaceRegexpOne function has a regexp that matches entire haystack, and a replacement of nothing other
        /// than \1 and some subpatterns in the regexp, or \0 and no subpatterns in the regexp, rewrite it with extract.
        if (function_node->getFunctionName() == "replaceRegexpOne")
        {
            if (!handleReplaceRegexpOne(*function_node))
                return;

            /// After optimization, function_node might now be "extract", so continue processing
        }

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

    bool handleReplaceRegexpOne(FunctionNode & function_node)
    {
        auto & function_node_arguments_nodes = function_node.getArguments().getNodes();
        if (function_node_arguments_nodes.size() != 3)
            return false;

        const auto * constant_node = function_node_arguments_nodes[2]->as<ConstantNode>();
        if (!constant_node)
            return false;

        if (auto constant_type = constant_node->getResultType(); !isString(constant_type))
            return false;

        String replacement = constant_node->getValue().safeGet<String>();
        bool replacement_zero = replacement == "\\0";
        bool replacement_one = replacement == "\\1";
        if (!replacement_zero && !replacement_one)
            return false;

        const auto * regexp_node = function_node_arguments_nodes[1]->as<ConstantNode>();
        if (!regexp_node)
            return false;

        if (auto regexp_type = regexp_node->getResultType(); !isString(regexp_type))
            return false;

        String regexp = regexp_node->getValue().safeGet<String>();

        /// Currently only look for ^...$ patterns without alternatives.
        bool starts_with_caret = regexp.front() == '^';
        if (!starts_with_caret)
            return false;

        bool ends_with_unescaped_dollar = false;
        if (!regexp.empty() && regexp.back() == '$')
            ends_with_unescaped_dollar = isUnescaped(regexp, regexp.size() - 1);

        if (!ends_with_unescaped_dollar)
            return false;

        /// Analyze the regular expression to detect presence of alternatives (e.g., 'a|b'). If any alternatives are
        /// found, return false to indicate the regexp is not suitable for optimization.
        RegexpAnalysisResult result = OptimizedRegularExpression::analyze(regexp);
        if (!result.alternatives.empty())
            return false;

        if ((replacement_one && result.has_capture) || (replacement_zero && !result.has_capture))
        {
            function_node_arguments_nodes.resize(2);
            resolveOrdinaryFunctionNodeByName(function_node, "extract", getContext());
            return true;
        }

        return false;
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
