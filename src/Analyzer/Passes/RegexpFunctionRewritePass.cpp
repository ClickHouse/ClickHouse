#include <Analyzer/Passes/RegexpFunctionRewritePass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeString.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool optimize_rewrite_regexp_functions;
}

namespace
{

bool mayHaveCaptureGroup(const String & regexp)
{
    for (size_t i = 0; i < regexp.size(); ++i)
    {
        if (regexp[i] == '(')
        {
            /// Count backslashes before the (
            size_t backslash_count = 0;
            for (ssize_t j = static_cast<ssize_t>(i) - 1; j >= 0 && regexp[j] == '\\'; --j)
                ++backslash_count;

            if (backslash_count % 2 == 0)
            {
                /// It's an unescaped (
                /// Check if it's a non-capturing group like (?: and skip if so.
                if (i + 2 < regexp.size() && regexp[i + 1] == '?' && regexp[i + 2] == ':')
                    continue;

                return true;
            }
        }
    }
    return false;
}

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

        /// If a regular expression starts with ^ or ends with an unescaped $, rewrite replaceRegexpAll with
        /// replaceRegexpOne.
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
            bool starts_with_caret = !regexp.empty() && regexp.front() == '^';
            bool ends_with_unescaped_dollar = false;
            if (regexp.size() >= 1 && regexp.back() == '$')
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

        /// If a replaceRegexpOne function has a replacement of nothing other than \1 and some subpatterns in the
        /// regexp, or \0 and no subpatterns in the regexp, rewrite it with extract.
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

            bool replacement_zero = replacement == String("\0", 1);
            bool replacement_one = replacement == "\\1";
            if (!replacement_zero && !replacement_one)
                return;

            const auto * regexp_node = function_node_arguments_nodes[1]->as<ConstantNode>();
            if (!regexp_node)
                return;

            if (auto regexp_type = regexp_node->getResultType(); !isString(regexp_type))
                return;

            String regexp = regexp_node->getValue().safeGet<String>();
            bool may_have_capture_group = mayHaveCaptureGroup(regexp);
            if ((replacement_one && may_have_capture_group) || (replacement_zero && !may_have_capture_group))
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
            if (!mayHaveCaptureGroup(regexp))
                return;

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
