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
        if (!function_node || !function_node->isOrdinaryFunction() || !function_node->getResultType()->equals(DataTypeString()))
            return;

        auto function_name = function_node->getFunctionName();

        /// If a regular expression without alternatives starts with ^ or ends with an unescaped $, rewrite
        /// replaceRegexpAll with replaceRegexpOne.
        if (function_name == "replaceRegexpAll" || Poco::toLower(function_name) == "regexp_replace")
        {
            auto & function_node_arguments_nodes = function_node->getArguments().getNodes();
            if (function_node_arguments_nodes.size() != 3)
                return;

            const auto * constant_node = function_node_arguments_nodes[1]->as<ConstantNode>();
            if (!constant_node)
                return;

            if (auto constant_type = constant_node->getResultType(); !isString(constant_type))
                return;

            String regexp = constant_node->getValue().safeGet<String>();
            if (regexp.empty())
                return;

            String dummy_required_substring;
            bool dummy_is_trivial;
            bool dummy_has_capture;
            bool dummy_required_substring_is_prefix;
            std::vector<String> alternatives;
            OptimizedRegularExpression::analyze(
                regexp, dummy_required_substring, dummy_is_trivial, dummy_has_capture, dummy_required_substring_is_prefix, alternatives);

            if (!alternatives.empty())
                return;

            bool starts_with_caret = regexp.front() == '^';
            bool ends_with_unescaped_dollar = false;
            if (regexp.back() == '$')
            {
                // Count number of consecutive backslashes before the $
                size_t backslash_count = 0;
                for (ssize_t i = static_cast<ssize_t>(regexp.size()) - 2; i >= 0 && regexp[i] == '\\'; --i)
                    ++backslash_count;

                // If even number of backslashes, then $ is unescaped
                ends_with_unescaped_dollar = (backslash_count % 2 == 0);
            }

            if (starts_with_caret || ends_with_unescaped_dollar)
            {
                function_name = "replaceRegexpOne";
                resolveOrdinaryFunctionNodeByName(*function_node, "replaceRegexpOne", getContext());
            }
        }

        /// If a replaceRegexpOne function has a regexp that matches entire haystack, and a replacement of nothing other
        /// than \1 and some subpatterns in the regexp, or \0 and no subpatterns in the regexp, rewrite it with extract.
        if (function_name == "replaceRegexpOne")
        {
            auto & function_node_arguments_nodes = function_node->getArguments().getNodes();
            if (function_node_arguments_nodes.size() != 3)
                return;

            const auto * constant_node = function_node_arguments_nodes[2]->as<ConstantNode>();
            if (!constant_node)
                return;

            if (auto constant_type = constant_node->getResultType(); !isString(constant_type))
                return;

            String replacement = constant_node->getValue().safeGet<String>();

            bool replacement_zero = replacement == "\\0";
            bool replacement_one = replacement == "\\1";
            if (!replacement_zero && !replacement_one)
                return;

            const auto * regexp_node = function_node_arguments_nodes[1]->as<ConstantNode>();
            if (!regexp_node)
                return;

            if (auto regexp_type = regexp_node->getResultType(); !isString(regexp_type))
                return;

            String regexp = regexp_node->getValue().safeGet<String>();

            /// Currently only look for ^...$ patterns without alternatives.
            bool starts_with_caret = regexp.front() == '^';
            if (!starts_with_caret)
                return;

            bool ends_with_unescaped_dollar = false;
            if (regexp.back() == '$')
            {
                // Count number of consecutive backslashes before the $
                size_t backslash_count = 0;
                for (ssize_t i = static_cast<ssize_t>(regexp.size()) - 2; i >= 0 && regexp[i] == '\\'; --i)
                    ++backslash_count;

                // If even number of backslashes, then $ is unescaped
                ends_with_unescaped_dollar = (backslash_count % 2 == 0);
            }

            if (!ends_with_unescaped_dollar)
                return;

            String dummy_required_substring;
            bool dummy_is_trivial;
            bool has_capture;
            bool dummy_required_substring_is_prefix;
            std::vector<String> alternatives;
            OptimizedRegularExpression::analyze(
                regexp, dummy_required_substring, dummy_is_trivial, has_capture, dummy_required_substring_is_prefix, alternatives);

            if (!alternatives.empty())
                return;

            if ((replacement_one && has_capture) || (replacement_zero && !has_capture))
            {
                function_name = "extract";
                function_node_arguments_nodes.resize(2);
                resolveOrdinaryFunctionNodeByName(*function_node, "extract", getContext());
            }
            else
            {
                return;
            }
        }

        /// If an extract function has a regexp with some subpatterns and the regexp starts with ^.* or ending with an
        /// unescaped .*$, remove this prefix and/or suffix.
        if (function_name == "extract")
        {
            auto & function_node_arguments_nodes = function_node->getArguments().getNodes();
            if (function_node_arguments_nodes.size() != 2)
                return;

            const auto * constant_node = function_node_arguments_nodes[1]->as<ConstantNode>();
            if (!constant_node)
                return;

            if (auto constant_type = constant_node->getResultType(); !isString(constant_type))
                return;

            String regexp = constant_node->getValue().safeGet<String>();
            String dummy_required_substring;
            bool dummy_is_trivial;
            bool has_capture;
            bool dummy_required_substring_is_prefix;
            std::vector<String> alternatives;
            OptimizedRegularExpression::analyze(
                regexp, dummy_required_substring, dummy_is_trivial, has_capture, dummy_required_substring_is_prefix, alternatives);

            if (!has_capture)
                return;

            /// For simplicity, this optimization ignores alternations and only considers anchoring at the start or end of the pattern.

            bool starts_with_caret_dot_star = regexp.starts_with("^.*");
            bool ends_with_unescaped_dot_star_dollar = false;

            /// Check if ends with unescaped .*$
            if (regexp.ends_with(".*$"))
            {
                size_t dot_pos = regexp.size() - 3;

                /// Count the number of consecutive backslashes before the dot
                size_t backslash_count = 0;
                for (ssize_t i = static_cast<ssize_t>(dot_pos) - 1; i >= 0 && regexp[i] == '\\'; --i)
                    ++backslash_count;

                /// If the number of backslashes is even, the dot is unescaped
                if (backslash_count % 2 == 0)
                    ends_with_unescaped_dot_star_dollar = true;
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
    }
};
}

void RegexpFunctionRewritePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RegexpFunctionRewriteVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
