#include <Planner/PlannerCollectSets.h>

#include <Interpreters/Context.h>

#include <Analyzer/Utils.h>
#include <Analyzer/SetUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>

namespace DB
{

namespace
{

class CollectSetsMatcher
{
public:
    using Visitor = ConstInDepthQueryTreeVisitor<CollectSetsMatcher, true, false>;

    struct Data
    {
        const PlannerContext & planner_context;
    };

    static void visit(const QueryTreeNodePtr & node, Data & data)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !isNameOfInFunction(function_node->getFunctionName()))
            return;

        auto in_first_argument = function_node->getArguments().getNodes().at(0);
        auto in_second_argument = function_node->getArguments().getNodes().at(1);
        auto in_second_argument_node_type = in_second_argument->getNodeType();

        const auto & planner_context = data.planner_context;
        const auto & global_planner_context = planner_context.getGlobalPlannerContext();
        const auto & settings = planner_context.getQueryContext()->getSettingsRef();

        String set_key = global_planner_context->getSetKey(in_second_argument.get());
        auto prepared_set = global_planner_context->getSetOrNull(set_key);

        if (prepared_set)
            return;

        if (in_second_argument_node_type == QueryTreeNodeType::QUERY ||
            in_second_argument_node_type == QueryTreeNodeType::UNION)
        {
            SizeLimits size_limits_for_set = {settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode};
            bool tranform_null_in = settings.transform_null_in;

            auto set = std::make_shared<Set>(size_limits_for_set, false /*fill_set_elements*/, tranform_null_in);

            global_planner_context->registerSet(set_key, set);
            global_planner_context->registerSubqueryNodeForSet(set_key, SubqueryNodeForSet{in_second_argument, set});
        }
        else if (in_second_argument_node_type == QueryTreeNodeType::CONSTANT)
        {
            auto & in_second_argument_constant_node = in_second_argument->as<ConstantNode &>();

            auto set = makeSetForConstantValue(
                in_first_argument->getResultType(),
                in_second_argument_constant_node.getResultType(),
                in_second_argument_constant_node.getConstantValue(),
                settings);

            global_planner_context->registerSet(set_key, std::move(set));
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Function IN is supported only if second argument is constant or table expression");
        }
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return child_node->getNodeType() != QueryTreeNodeType::QUERY;
    }
};

using CollectSetsVisitor = CollectSetsMatcher::Visitor;

}

void collectSets(const QueryTreeNodePtr & node, const PlannerContext & planner_context)
{
    CollectSetsVisitor::Data data {planner_context};
    CollectSetsVisitor visitor(data);
    visitor.visit(node);
}

}
