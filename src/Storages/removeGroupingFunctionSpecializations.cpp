#include <Storages/removeGroupingFunctionSpecializations.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/grouping.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

class GeneralizeGroupingFunctionForDistributedVisitor : public InDepthQueryTreeVisitor<GeneralizeGroupingFunctionForDistributedVisitor>
{
public:
    static void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function = node->as<FunctionNode>();
        if (!function)
            return;

        const auto & function_name = function->getFunctionName();
        bool ordinary_grouping = function_name == "__groupingOrdinary";

        if (!ordinary_grouping
            && function_name != "__groupingForRollup"
            && function_name != "__groupingForCube"
            && function_name != "__groupingForGroupingSets")
            return;


        /// The specializations are registered functions, so a query can also spell them directly,
        /// and such a call must reach the remote server unchanged. The analyzer produces its nodes
        /// by resolving a `grouping` call in place, so the original AST tells the two apart.
        if (const auto & original_ast = function->getOriginalAST())
        {
            const auto * original_function = original_ast->as<ASTFunction>();
            if (!original_function || original_function->name != "grouping")
                return;
        }

        auto & arguments = function->getArguments().getNodes();

        /// The analyzer appends constant arguments carrying the specialization parameters (two for
        /// `__groupingOrdinary`, three for the rest), and for the other specializations it prepends
        /// the `__grouping_set` column; they must not reach the query text. As above, leave a node
        /// that does not match the analyzer-built shape untouched.
        const size_t num_state_arguments = ordinary_grouping ? 2 : 3;
        if (arguments.size() <= num_state_arguments)
            return;
        for (size_t i = arguments.size() - num_state_arguments; i < arguments.size(); ++i)
            if (!arguments[i]->as<ConstantNode>())
                return;
        if (!ordinary_grouping)
        {
            const auto * grouping_set_arg = arguments[0]->as<ColumnNode>();
            if (!grouping_set_arg || grouping_set_arg->getColumnName() != "__grouping_set")
                return;
        }

        arguments.resize(arguments.size() - num_state_arguments);
        if (!ordinary_grouping)
            arguments.erase(arguments.begin());

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
