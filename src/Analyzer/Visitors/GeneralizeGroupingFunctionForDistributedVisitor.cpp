#include <Analyzer/Visitors/GeneralizeGroupingFunctionForDistributedVisitor.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class GeneralizeGroupingFunctionForDistributedVisitor : public InDepthQueryTreeVisitor<GeneralizeGroupingFunctionForDistributedVisitor>
{
public:
    static void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function = node->as<FunctionNode>();
        if (!function)
            return;

        const auto & function_name = function->getFunctionName();
        bool ordinary_grouping = function_name == "groupingOrdinary";

        if (!ordinary_grouping
            && function_name != "groupingForRollup"
            && function_name != "groupingForCube"
            && function_name != "groupingForGroupingSets")
            return;

        function->resolveAsFunctionWithName("GROUPING");
        if (ordinary_grouping)
            return;

        auto & arguments = function->getArguments().getNodes();

        if (arguments.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Grouping function specialization must have arguments");
        auto * grouping_set_arg = arguments[0]->as<ColumnNode>();
        if (!grouping_set_arg || grouping_set_arg->getColumnName() != "__grouping_set")
            throw Exception(ErrorCodes::LOGICAL_ERROR,
            "The first argument of Grouping function specialization must be '__grouping_set' column but {} found",
            arguments[0]->dumpTree());
        arguments.erase(arguments.begin());
    }
};

void removeGroupingFunctionSpecializations(QueryTreeNodePtr & node)
{
    GeneralizeGroupingFunctionForDistributedVisitor visitor;
    visitor.visit(node);
}

}
