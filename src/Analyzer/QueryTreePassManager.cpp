#include <Analyzer/QueryTreePassManager.h>

#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/Passes/CountDistinctPass.h>
#include <Analyzer/Passes/FunctionToSubcolumnsPass.h>
#include <Analyzer/Passes/SumIfToCountIfPass.h>
#include <Analyzer/Passes/MultiIfToIfPass.h>
#include <Analyzer/Passes/IfConstantConditionPass.h>
#include <Analyzer/Passes/IfChainToMultiIfPass.h>
#include <Analyzer/Passes/OrderByTupleEliminationPass.h>
#include <Analyzer/Passes/NormalizeCountVariantsPass.h>
#include <Analyzer/Passes/CustomizeFunctionsPass.h>
#include <Analyzer/Passes/AggregateFunctionsArithmericOperationsPass.h>
#include <Analyzer/Passes/UniqInjectiveFunctionsEliminationPass.h>
#include <Analyzer/Passes/OrderByLimitByDuplicateEliminationPass.h>
#include <Analyzer/Passes/FuseFunctionsPass.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Interpreters/Context.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

#ifndef NDEBUG

/** This visitor checks if Query Tree structure is valid after each pass
  * in debug build.
  */
class ValidationChecker : public InDepthQueryTreeVisitor<ValidationChecker>
{
    String pass_name;
public:
    explicit ValidationChecker(String pass_name_)
        : pass_name(std::move(pass_name_))
    {}

    void visitImpl(QueryTreeNodePtr & node) const
    {
        auto * column = node->as<ColumnNode>();
        if (!column)
            return;
        if (column->getColumnSourceOrNull() == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Column {} {} query tree node does not have valid source node after running {} pass",
                column->getColumnName(), column->getColumnType(), pass_name);
    }
};
#endif

}

/** ClickHouse query tree pass manager.
  *
  * TODO: Support _shard_num into shardNum() rewriting.
  * TODO: Support logical expressions optimizer.
  * TODO: Support setting convert_query_to_cnf.
  * TODO: Support setting optimize_using_constraints.
  * TODO: Support setting optimize_substitute_columns.
  * TODO: Support GROUP BY injective function elimination.
  * TODO: Support GROUP BY functions of other keys elimination.
  * TODO: Support setting optimize_move_functions_out_of_any.
  * TODO: Support setting optimize_aggregators_of_group_by_keys.
  * TODO: Support setting optimize_duplicate_order_by_and_distinct.
  * TODO: Support setting optimize_redundant_functions_in_order_by.
  * TODO: Support setting optimize_monotonous_functions_in_order_by.
  * TODO: Support setting optimize_if_transform_strings_to_enum.
  * TODO: Support settings.optimize_or_like_chain.
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
#ifndef NDEBUG
        ValidationChecker(passes[i]->getName()).visit(query_tree_node);
#endif
    }
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
#ifndef NDEBUG
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

void addQueryTreePasses(QueryTreePassManager & manager)
{
    auto context = manager.getContext();
    const auto & settings = context->getSettingsRef();

    manager.addPass(std::make_unique<QueryAnalysisPass>());

    if (settings.optimize_functions_to_subcolumns)
        manager.addPass(std::make_unique<FunctionToSubcolumnsPass>());

    if (settings.count_distinct_optimization)
        manager.addPass(std::make_unique<CountDistinctPass>());

    if (settings.optimize_rewrite_sum_if_to_count_if)
        manager.addPass(std::make_unique<SumIfToCountIfPass>());

    if (settings.optimize_normalize_count_variants)
        manager.addPass(std::make_unique<NormalizeCountVariantsPass>());

    manager.addPass(std::make_unique<CustomizeFunctionsPass>());

    if (settings.optimize_arithmetic_operations_in_aggregate_functions)
        manager.addPass(std::make_unique<AggregateFunctionsArithmericOperationsPass>());

    if (settings.optimize_injective_functions_inside_uniq)
        manager.addPass(std::make_unique<UniqInjectiveFunctionsEliminationPass>());

    if (settings.optimize_multiif_to_if)
        manager.addPass(std::make_unique<MultiIfToIfPass>());

    manager.addPass(std::make_unique<IfConstantConditionPass>());

    if (settings.optimize_if_chain_to_multiif)
        manager.addPass(std::make_unique<IfChainToMultiIfPass>());

    manager.addPass(std::make_unique<OrderByTupleEliminationPass>());
    manager.addPass(std::make_unique<OrderByLimitByDuplicateEliminationPass>());

    if (settings.optimize_syntax_fuse_functions)
        manager.addPass(std::make_unique<FuseFunctionsPass>());
}

}
