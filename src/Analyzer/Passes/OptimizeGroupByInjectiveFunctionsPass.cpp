#include <Analyzer/Passes/OptimizeGroupByInjectiveFunctionsPass.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include "Analyzer/ConstantNode.h"
#include "Analyzer/FunctionNode.h"
#include "Analyzer/IQueryTreeNode.h"
#include "DataTypes/IDataType.h"
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include "Functions/FunctionFactory.h"
#include "Interpreters/Context_fwd.h"
#include "Interpreters/ExternalDictionariesLoader.h"

namespace DB
{

namespace
{

const std::unordered_set<String> possibly_injective_function_names
{
        "dictGet",
        "dictGetString",
        "dictGetUInt8",
        "dictGetUInt16",
        "dictGetUInt32",
        "dictGetUInt64",
        "dictGetInt8",
        "dictGetInt16",
        "dictGetInt32",
        "dictGetInt64",
        "dictGetFloat32",
        "dictGetFloat64",
        "dictGetDate",
        "dictGetDateTime"
};

class OptimizeGroupByInjectiveFunctionsVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeGroupByInjectiveFunctionsVisitor>
{
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeGroupByInjectiveFunctionsVisitor>;
public:
    explicit OptimizeGroupByInjectiveFunctionsVisitor(ContextPtr context)
        : Base(std::move(context))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasGroupBy())
            return;

        if (query->isGroupByWithCube() || query->isGroupByWithRollup())
            return;

        auto & group_by = query->getGroupBy().getNodes();
        if (query->isGroupByWithGroupingSets())
        {
            for (auto & set : group_by)
            {
                auto & grouping_set = set->as<ListNode>()->getNodes();
                optimizeGroupingSet(grouping_set);
            }
        }
        else
            optimizeGroupingSet(group_by);
    }

private:
    void optimizeGroupingSet(QueryTreeNodes & grouping_set)
    {
        auto context = getContext();
        const FunctionFactory & function_factory = FunctionFactory::instance();

        QueryTreeNodes new_group_by_keys;
        new_group_by_keys.reserve(grouping_set.size());
        for (auto & group_by_elem : grouping_set)
        {
            if (auto const * function_node = group_by_elem->as<FunctionNode>())
            {
                bool can_be_eliminated = false;
                if (possibly_injective_function_names.contains(function_node->getFunctionName()))
                {
                    can_be_eliminated = canBeEliminated(function_node, context);
                }
                else
                {
                    FunctionOverloadResolverPtr function_builder = UserDefinedExecutableFunctionFactory::instance().tryGet(function_node->getFunctionName(), context);

                    // TODO: fix me
                    if (!function_builder)
                        function_builder = function_factory.get(function_node->getFunctionName(), context);

                    can_be_eliminated = function_builder->isInjective({});
                }

                if (can_be_eliminated)
                {
                    for (auto const & argument : function_node->getArguments())
                    {
                        if (argument->getNodeType() != QueryTreeNodeType::CONSTANT)
                            new_group_by_keys.push_back(argument);
                    }
                }
                else
                    new_group_by_keys.push_back(group_by_elem);
            }
            else
                new_group_by_keys.push_back(group_by_elem);
        }

        grouping_set = std::move(new_group_by_keys);
    }

    bool canBeEliminated(const FunctionNode * function_node, const ContextPtr & context)
    {
        auto const * dict_name_arg = function_node->getArguments().getNodes()[0]->as<ConstantNode>();
        if (!dict_name_arg || isString(dict_name_arg->getResultType()))
            return false;
        auto dict_name = dict_name_arg->getValue().safeGet<String>();

        const auto & dict_ptr = context->getExternalDictionariesLoader().getDictionary(dict_name, context);

        auto const * attr_arg = function_node->getArguments().getNodes()[0]->as<ConstantNode>();
        if (!attr_arg || isString(attr_arg->getResultType()))
            return false;
        auto attr_name = attr_arg->getValue().safeGet<String>();

        return dict_ptr->isInjective(attr_name);
    }

};

}

void OptimizeGroupByInjectiveFunctionsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    OptimizeGroupByInjectiveFunctionsVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
