#include <Storages/removeGroupingFunctionSpecializations.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Common/Exception.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/grouping.h>

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


        auto & arguments = function->getArguments().getNodes();

        /// The analyzer appends constant arguments carrying the specialization parameters (two for
        /// `groupingOrdinary`, three for the rest); they must not reach the query text either.
        const size_t num_state_arguments = ordinary_grouping ? 2 : 3;
        if (arguments.size() < num_state_arguments)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Grouping function specialization must have arguments");
        arguments.resize(arguments.size() - num_state_arguments);

        if (!ordinary_grouping)
        {
            if (arguments.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Grouping function specialization must have arguments");
            auto * grouping_set_arg = arguments[0]->as<ColumnNode>();
            if (!grouping_set_arg || grouping_set_arg->getColumnName() != "__grouping_set")
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                "The first argument of Grouping function specialization must be '__grouping_set' column but {} found",
                arguments[0]->dumpTree());
            arguments.erase(arguments.begin());
        }

        // This node will be only converted to AST, so we don't need
        // to pass the correct force_compatibility flag to FunctionGrouping.
        auto function_adaptor = std::make_shared<FunctionToOverloadResolverAdaptor>(
            std::make_shared<FunctionGrouping>(false)
        );
        function->resolveAsFunction(function_adaptor);
    }
};

void removeGroupingFunctionSpecializations(QueryTreeNodePtr & node)
{
    GeneralizeGroupingFunctionForDistributedVisitor visitor;
    visitor.visit(node);
}

}
