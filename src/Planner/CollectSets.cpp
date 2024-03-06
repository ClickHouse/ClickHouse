#include <Planner/CollectSets.h>

#include <Interpreters/Context.h>

#include <Storages/StorageSet.h>

#include <Analyzer/Utils.h>
#include <Analyzer/SetUtils.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

class CollectSetsVisitor : public ConstInDepthQueryTreeVisitor<CollectSetsVisitor>
{
public:
    explicit CollectSetsVisitor(PlannerContext & planner_context_)
        : planner_context(planner_context_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !isNameOfInFunction(function_node->getFunctionName()))
            return;

        auto in_first_argument = function_node->getArguments().getNodes().at(0);
        auto in_second_argument = function_node->getArguments().getNodes().at(1);
        auto in_second_argument_node_type = in_second_argument->getNodeType();

        const auto & settings = planner_context.getQueryContext()->getSettingsRef();

        String set_key = planner_context.createSetKey(in_second_argument);

        if (planner_context.hasSet(set_key))
            return;

        /// Tables and table functions are replaced with subquery at Analysis stage, except special Set table.
        auto * second_argument_table = in_second_argument->as<TableNode>();
        StorageSet * storage_set = second_argument_table != nullptr ? dynamic_cast<StorageSet *>(second_argument_table->getStorage().get()) : nullptr;

        if (storage_set)
        {
            planner_context.registerSet(set_key, PlannerSet(storage_set->getSet()));
        }
        else if (const auto * constant_node = in_second_argument->as<ConstantNode>())
        {
            auto set = makeSetForConstantValue(
                in_first_argument->getResultType(),
                constant_node->getValue(),
                constant_node->getResultType(),
                settings);

            planner_context.registerSet(set_key, PlannerSet(std::move(set)));
        }
        else if (in_second_argument_node_type == QueryTreeNodeType::QUERY ||
            in_second_argument_node_type == QueryTreeNodeType::UNION)
        {
            SizeLimits size_limits_for_set = {settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode};
            bool tranform_null_in = settings.transform_null_in;
            auto set = std::make_shared<Set>(size_limits_for_set, false /*fill_set_elements*/, tranform_null_in);

            planner_context.registerSet(set_key, PlannerSet(std::move(set), in_second_argument));
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Function '{}' is supported only if second argument is constant or table expression",
                function_node->getFunctionName());
        }
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

private:
    PlannerContext & planner_context;
};

}

void collectSets(const QueryTreeNodePtr & node, PlannerContext & planner_context)
{
    CollectSetsVisitor visitor(planner_context);
    visitor.visit(node);
}

}
