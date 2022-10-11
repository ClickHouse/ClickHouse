#include <Analyzer/QueryTreePassManager.h>

#include <Analyzer/QueryAnalysisPass.h>
#include <Analyzer/MultiIfToIfPass.h>
#include <Analyzer/IfConstantConditionPass.h>
#include <Analyzer/IfChainToMultiIfPass.h>
#include <Analyzer/OrderByTupleEliminationPass.h>
#include <Analyzer/NormalizeCountVariantsPass.h>
#include <Analyzer/CountDistinctPass.h>
#include <Analyzer/CustomizeFunctionsPass.h>

#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/** ClickHouse query tree pass manager
  *
  * TODO: Support _shard_num into shardNum() rewriting.
  * TODO: Support logical expressions optimizer.
  * TODO: Support fuse sum count optimize_fuse_sum_count_avg, optimize_syntax_fuse_functions.
  * TODO: Support setting optimize_functions_to_subcolumns.
  * TODO: Support setting optimize_arithmetic_operations_in_aggregate_functions.
  * TODO: Support setting convert_query_to_cnf.
  * TODO: Support setting optimize_using_constraints.
  * TODO: Support setting optimize_substitute_columns.
  * TODO: Support GROUP BY injective function elimination.
  * TODO: Support GROUP BY functions of other keys elimination.
  * TODO: Support setting optimize_move_functions_out_of_any.
  * TODO: Support setting optimize_rewrite_sum_if_to_count_if.
  * TODO: Support setting optimize_aggregators_of_group_by_keys.
  * TODO: Support setting optimize_duplicate_order_by_and_distinct.
  * TODO: Support setting optimize_redundant_functions_in_order_by.
  * TODO: Support setting optimize_monotonous_functions_in_order_by.
  * TODO: Support setting optimize_if_transform_strings_to_enum.
  * TODO: Remove duplicate elements from ORDER BY clause.
  * TODO: Remove duplicated elements from LIMIT BY clause.
  * TODO: Remove duplicated elements from USING clause.
  * TODO: Support settings.optimize_syntax_fuse_functions.
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
    size_t optimizations_size = passes.size();

    for (size_t i = 0; i < optimizations_size; ++i)
        passes[i]->run(query_tree_node, current_context);
}

void QueryTreePassManager::run(QueryTreeNodePtr query_tree_node, size_t up_to_pass_index)
{
    size_t optimizations_size = passes.size();
    if (up_to_pass_index > optimizations_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Requested to run optimizations up to {} pass. There are only {} pass",
            up_to_pass_index,
            optimizations_size);

    auto current_context = getContext();
    for (size_t i = 0; i < up_to_pass_index; ++i)
        passes[i]->run(query_tree_node, current_context);
}

void QueryTreePassManager::dump(WriteBuffer & buffer)
{
    size_t passes_size = passes.size();

    for (size_t i = 0; i < passes_size; ++i)
    {
        auto & pass = passes[i];
        buffer << "Pass " << (i + 1) << ' ' << pass->getName() << " - " << pass->getDescription();
        if (i < passes_size)
            buffer << '\n';
    }
}

void QueryTreePassManager::dump(WriteBuffer & buffer, size_t up_to_pass_index)
{
    size_t optimizations_size = passes.size();
    if (up_to_pass_index > optimizations_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Requested to dump optimizations up to {} pass. There are only {} pass",
            up_to_pass_index,
            optimizations_size);

    for (size_t i = 0; i < up_to_pass_index; ++i)
    {
        auto & pass = passes[i];
        buffer << "Pass " << (i + 1) << " " << pass->getName() << " - " << pass->getDescription();
        if (i < up_to_pass_index)
            buffer << '\n';
    }
}

void addQueryTreePasses(QueryTreePassManager & manager)
{
    auto context = manager.getContext();
    const auto & settings = context->getSettingsRef();

    manager.addPass(std::make_shared<QueryAnalysisPass>());

    if (settings.count_distinct_optimization)
        manager.addPass(std::make_shared<CountDistinctPass>());

    if (settings.optimize_normalize_count_variants)
        manager.addPass(std::make_shared<NormalizeCountVariantsPass>());

    manager.addPass(std::make_shared<CustomizeFunctionsPass>());

    if (settings.optimize_multiif_to_if)
        manager.addPass(std::make_shared<MultiIfToIfPass>());

    manager.addPass(std::make_shared<IfConstantConditionPass>());

    if (settings.optimize_if_chain_to_multiif)
        manager.addPass(std::make_shared<IfChainToMultiIfPass>());

    manager.addPass(std::make_shared<OrderByTupleEliminationPass>());
}

}
