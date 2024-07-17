#include <Analyzer/QueryTreePassManager.h>

#include <memory>

#include <Common/Exception.h>
#include "Analyzer/Passes/OptimizeGroupByInjectiveFunctionsPass.h"

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Interpreters/Context.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/Passes/RemoveUnusedProjectionColumnsPass.h>
#include <Analyzer/Passes/RewriteSumFunctionWithSumAndCountPass.h>
#include <Analyzer/Passes/CountDistinctPass.h>
#include <Analyzer/Passes/UniqToCountPass.h>
#include <Analyzer/Passes/FunctionToSubcolumnsPass.h>
#include <Analyzer/Passes/RewriteAggregateFunctionWithIfPass.h>
#include <Analyzer/Passes/SumIfToCountIfPass.h>
#include <Analyzer/Passes/MultiIfToIfPass.h>
#include <Analyzer/Passes/IfConstantConditionPass.h>
#include <Analyzer/Passes/IfChainToMultiIfPass.h>
#include <Analyzer/Passes/OrderByTupleEliminationPass.h>
#include <Analyzer/Passes/NormalizeCountVariantsPass.h>
#include <Analyzer/Passes/AggregateFunctionsArithmericOperationsPass.h>
#include <Analyzer/Passes/UniqInjectiveFunctionsEliminationPass.h>
#include <Analyzer/Passes/OrderByLimitByDuplicateEliminationPass.h>
#include <Analyzer/Passes/FuseFunctionsPass.h>
#include <Analyzer/Passes/OptimizeGroupByFunctionKeysPass.h>
#include <Analyzer/Passes/IfTransformStringsToEnumPass.h>
#include <Analyzer/Passes/ConvertOrLikeChainPass.h>
#include <Analyzer/Passes/OptimizeRedundantFunctionsInOrderByPass.h>
#include <Analyzer/Passes/GroupingFunctionsResolvePass.h>
#include <Analyzer/Passes/AutoFinalOnQueryPass.h>
#include <Analyzer/Passes/ArrayExistsToHasPass.h>
#include <Analyzer/Passes/ComparisonTupleEliminationPass.h>
#include <Analyzer/Passes/LogicalExpressionOptimizerPass.h>
#include <Analyzer/Passes/CrossToInnerJoinPass.h>
#include <Analyzer/Passes/ShardNumColumnToFunctionPass.h>
#include <Analyzer/Passes/ConvertQueryToCNFPass.h>
#include <Analyzer/Passes/AggregateFunctionOfGroupByKeysPass.h>
#include <Analyzer/Passes/OptimizeDateOrDateTimeConverterWithPreimagePass.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

#if defined(DEBUG_OR_SANITIZER_BUILD)

/** This visitor checks if Query Tree structure is valid after each pass
  * in debug build.
  */
class ValidationChecker : public InDepthQueryTreeVisitor<ValidationChecker>
{
public:
    explicit ValidationChecker(String pass_name_)
        : pass_name(std::move(pass_name_))
    {}

    static bool needChildVisit(VisitQueryTreeNodeType & parent, VisitQueryTreeNodeType &)
    {
        if (parent->getNodeType() == QueryTreeNodeType::TABLE_FUNCTION)
            return false;

        return true;
    }

    void visitImpl(QueryTreeNodePtr & node) const
    {
        if (auto * column = node->as<ColumnNode>())
            visitColumn(column);
        else if (auto * function = node->as<FunctionNode>())
            visitFunction(function);
    }
private:
    void visitColumn(ColumnNode * column) const
    {
        if (column->getColumnSourceOrNull() == nullptr && column->getColumnName() != "__grouping_set")
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column {} {} query tree node does not have valid source node after running {} pass",
                column->getColumnName(), column->getColumnType(), pass_name);
    }

    void visitFunction(FunctionNode * function) const
    {
        if (!function->isResolved())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Function {} is not resolved after running {} pass",
                function->toAST()->formatForErrorMessage(), pass_name);

        if (isNameOfInFunction(function->getFunctionName()))
            return;

        const auto & expected_argument_types = function->getArgumentTypes();
        size_t expected_argument_types_size = expected_argument_types.size();
        auto actual_argument_columns = function->getArgumentColumns();

        if (expected_argument_types_size != actual_argument_columns.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Function {} expects {} arguments but has {} after running {} pass",
                function->toAST()->formatForErrorMessage(),
                expected_argument_types_size,
                actual_argument_columns.size(),
                pass_name);

        for (size_t i = 0; i < expected_argument_types_size; ++i)
        {
            const auto & expected_argument_type = expected_argument_types[i];
            const auto & actual_argument_type = actual_argument_columns[i].type;

            if (!expected_argument_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function {} expected argument {} type is not set after running {} pass",
                    function->toAST()->formatForErrorMessage(),
                    i + 1,
                    pass_name);

            if (!actual_argument_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function {} actual argument {} type is not set after running {} pass",
                    function->toAST()->formatForErrorMessage(),
                    i + 1,
                    pass_name);

            if (!expected_argument_type->equals(*actual_argument_type))
            {
                /// Aggregate functions remove low cardinality for their argument types
                if ((function->isAggregateFunction() || function->isWindowFunction()) &&
                    expected_argument_type->equals(*recursiveRemoveLowCardinality(actual_argument_type)))
                    continue;

                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function {} expects argument {} to have {} type but receives {} after running {} pass",
                    function->toAST()->formatForErrorMessage(),
                    i + 1,
                    expected_argument_type->getName(),
                    actual_argument_type->getName(),
                    pass_name);
            }
        }
    }

    String pass_name;
};
#endif

}

/** ClickHouse query tree pass manager.
  *
  * TODO: Add optimizations based on function semantics. Example: SELECT * FROM test_table WHERE id != id. (id is not nullable column).
  */

QueryTreePassManager::QueryTreePassManager(ContextPtr context_) : WithContext(context_) {}

void QueryTreePassManager::addPass(QueryTreePassPtr pass)
{
    passes.push_back(std::move(pass));
}

void QueryTreePassManager::run(QueryTreeNodePtr query_tree_node)
{
    auto current_context = getContext();
    size_t passes_size = passes.size();

    for (size_t i = 0; i < passes_size; ++i)
    {
        passes[i]->run(query_tree_node, current_context);
#if defined(DEBUG_OR_SANITIZER_BUILD)
        ValidationChecker(passes[i]->getName()).visit(query_tree_node);
#endif
    }
}

void QueryTreePassManager::runOnlyResolve(QueryTreeNodePtr query_tree_node)
{
    // Run only QueryAnalysisPass and GroupingFunctionsResolvePass passes.
    run(query_tree_node, 3);
}

void QueryTreePassManager::run(QueryTreeNodePtr query_tree_node, size_t up_to_pass_index)
{
    size_t passes_size = passes.size();
    if (up_to_pass_index > passes_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Requested to run passes up to {} pass. There are only {} passes",
            up_to_pass_index,
            passes_size);

    auto current_context = getContext();
    for (size_t i = 0; i < up_to_pass_index; ++i)
    {
        passes[i]->run(query_tree_node, current_context);
#if defined(DEBUG_OR_SANITIZER_BUILD)
        ValidationChecker(passes[i]->getName()).visit(query_tree_node);
#endif
    }
}

void QueryTreePassManager::dump(WriteBuffer & buffer)
{
    size_t passes_size = passes.size();

    for (size_t i = 0; i < passes_size; ++i)
    {
        auto & pass = passes[i];
        buffer << "Pass " << (i + 1) << ' ' << pass->getName() << " - " << pass->getDescription();
        if (i + 1 != passes_size)
            buffer << '\n';
    }
}

void QueryTreePassManager::dump(WriteBuffer & buffer, size_t up_to_pass_index)
{
    size_t passes_size = passes.size();
    if (up_to_pass_index > passes_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Requested to dump passes up to {} pass. There are only {} passes",
            up_to_pass_index,
            passes_size);

    for (size_t i = 0; i < up_to_pass_index; ++i)
    {
        auto & pass = passes[i];
        buffer << "Pass " << (i + 1) << " " << pass->getName() << " - " << pass->getDescription();
        if (i + 1 != up_to_pass_index)
            buffer << '\n';
    }
}

void addQueryTreePasses(QueryTreePassManager & manager, bool only_analyze)
{
    manager.addPass(std::make_unique<QueryAnalysisPass>(only_analyze));
    manager.addPass(std::make_unique<GroupingFunctionsResolvePass>());
    manager.addPass(std::make_unique<AutoFinalOnQueryPass>());

    manager.addPass(std::make_unique<RemoveUnusedProjectionColumnsPass>());
    manager.addPass(std::make_unique<FunctionToSubcolumnsPass>());

    manager.addPass(std::make_unique<ConvertLogicalExpressionToCNFPass>());

    manager.addPass(std::make_unique<RewriteSumFunctionWithSumAndCountPass>());
    manager.addPass(std::make_unique<CountDistinctPass>());
    manager.addPass(std::make_unique<UniqToCountPass>());
    manager.addPass(std::make_unique<RewriteArrayExistsToHasPass>());
    manager.addPass(std::make_unique<NormalizeCountVariantsPass>());

    /// should before AggregateFunctionsArithmericOperationsPass
    manager.addPass(std::make_unique<AggregateFunctionOfGroupByKeysPass>());

    manager.addPass(std::make_unique<AggregateFunctionsArithmericOperationsPass>());
    manager.addPass(std::make_unique<UniqInjectiveFunctionsEliminationPass>());

    // Should run before optimization of GROUP BY keys to allow the removal of
    // toString function.
    manager.addPass(std::make_unique<IfTransformStringsToEnumPass>());

    manager.addPass(std::make_unique<OptimizeGroupByFunctionKeysPass>());
    manager.addPass(std::make_unique<OptimizeGroupByInjectiveFunctionsPass>());

    /// The order here is important as we want to keep collapsing in order
    manager.addPass(std::make_unique<MultiIfToIfPass>());
    manager.addPass(std::make_unique<IfConstantConditionPass>());
    manager.addPass(std::make_unique<IfChainToMultiIfPass>());
    manager.addPass(std::make_unique<RewriteAggregateFunctionWithIfPass>());
    manager.addPass(std::make_unique<SumIfToCountIfPass>());

    manager.addPass(std::make_unique<ComparisonTupleEliminationPass>());

    manager.addPass(std::make_unique<OptimizeRedundantFunctionsInOrderByPass>());

    manager.addPass(std::make_unique<OrderByTupleEliminationPass>());
    manager.addPass(std::make_unique<OrderByLimitByDuplicateEliminationPass>());

    manager.addPass(std::make_unique<FuseFunctionsPass>());

    manager.addPass(std::make_unique<ConvertOrLikeChainPass>());

    manager.addPass(std::make_unique<LogicalExpressionOptimizerPass>());

    manager.addPass(std::make_unique<CrossToInnerJoinPass>());
    manager.addPass(std::make_unique<ShardNumColumnToFunctionPass>());

    manager.addPass(std::make_unique<OptimizeDateOrDateTimeConverterWithPreimagePass>());
}

}
