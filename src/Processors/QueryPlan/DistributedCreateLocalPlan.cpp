#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>

#include <Common/checkStackSize.h>
#include <Core/Settings.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ConvertingActions.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Storages/StorageDummy.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

/// #111893, Form 2: a Distributed table declared with an empty database (relying on a
/// per-shard <default_database> to fill it in, e.g. `ENGINE = Distributed('cluster', '', 't')`)
/// carries that empty database in the query tree's StorageDummy from the point it's built,
/// well before any per-shard information is available. The original code path filled it in via
/// `new_context->setCurrentDatabase(default_database)` plus a fresh, catalog-dependent
/// re-resolution of `query_ast` -- but reusing the already-resolved query_tree (see below,
/// #111893 Form 1) skips that resolution, so the empty database never gets filled in. Walk the
/// tree and fix it up directly: same pattern as `replaceStorageInQueryTree`, but correcting the
/// database on an existing empty-database table node instead of substituting a whole new storage.
static void fillInDefaultDatabaseForEmptyDatabaseTables(QueryTreeNodePtr & query_tree, const ContextPtr & context, const std::string & default_database)
{
    auto nodes = extractAllTableReferences(query_tree);
    IQueryTreeNode::ReplacementMap replacement_map;

    for (auto & node : nodes)
    {
        auto & table_node = node->as<TableNode &>();
        if (table_node.getStorageID().hasDatabase())
            continue;

        auto column_names_and_types = table_node.getStorageSnapshot()->getColumns(GetColumnsOptions(GetColumnsOptions::All));
        StorageID fixed_storage_id{default_database, table_node.getStorageID().table_name};
        auto storage = std::make_shared<StorageDummy>(fixed_storage_id, ColumnsDescription{column_names_and_types});

        auto replacement_table_expression = std::make_shared<TableNode>(std::move(storage), context);
        replacement_table_expression->setAlias(node->getAlias());

        if (auto table_expression_modifiers = table_node.getTableExpressionModifiers())
            replacement_table_expression->setTableExpressionModifiers(*table_expression_modifiers);

        replacement_map.emplace(&table_node, std::move(replacement_table_expression));
    }

    if (!replacement_map.empty())
        query_tree = query_tree->cloneAndReplace(replacement_map);
}

std::unique_ptr<QueryPlan> createLocalPlan(
    const ASTPtr & query_ast,
    const Block & header,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t shard_num,
    size_t shard_count,
    bool build_logical_plan,
    const std::string & default_database,
    const QueryTreeNodePtr & query_tree)
{
    checkStackSize();

    auto query_plan = std::make_unique<QueryPlan>();
    auto new_context = Context::createCopy(context);

    /// setCurrentDatabase() asserts the database actually exists in this (the initiator's) own
    /// DatabaseCatalog -- true when it's about to support AST-based re-resolution (the fallback
    /// path below, when no already-resolved query_tree is available), but not otherwise: with a
    /// reused query_tree (#111893), the per-shard database is instead filled in directly on the
    /// tree (see fillInDefaultDatabaseForEmptyDatabaseTables above), and "current database" is
    /// never consulted, so asserting it exists here would reject valid per-shard databases that
    /// legitimately don't exist on the initiator (e.g. shard_1 when this is shard_0).
    if (build_logical_plan && !default_database.empty() && !query_tree)
        new_context->setCurrentDatabase(default_database);

    /// Do not apply AST optimizations, because query
    /// is already optimized and some optimizations
    /// can be applied only for non-distributed tables
    /// and we can produce query, inconsistent with remote plans.
    auto select_query_options = SelectQueryOptions(processed_stage)
        .setShardInfo(static_cast<UInt32>(shard_num), static_cast<UInt32>(shard_count))
        .ignoreASTOptimizations();

    select_query_options.build_logical_plan = build_logical_plan;
    select_query_options.is_local_shard_plan
        = !build_logical_plan && processed_stage == QueryProcessingStage::WithMergeableStateAfterAggregationAndLimit;
    /// A logical plan is serialized and shipped to a shard, which sends the blocks back over the
    /// network, so it must keep marshalling. Only the plan executed in this process must skip it.
    select_query_options.is_local_plan_for_distributed_query = !build_logical_plan;

    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        /// Positional arguments in the outer query were already resolved by the initiator.
        /// Use a context flag instead of disabling enable_positional_arguments so that
        /// view-inner queries on this node (which were never resolved by the initiator) are
        /// still processed correctly. See https://github.com/ClickHouse/ClickHouse/issues/62289.
        new_context->setPositionalArgumentsAlreadyResolved(true);

        /// #111893: re-resolving `query_ast` from scratch needs the shard's table to exist in
        /// this (the initiator's) DatabaseCatalog -- true for a real Distributed table, but not
        /// for e.g. remote()'s target database or a per-shard default_database. When the caller
        /// already has a resolved tree from the outer analysis, reuse it instead: it was already
        /// validly resolved against the shard's actual schema, without needing local catalog
        /// access here at all.
        if (query_tree)
        {
            QueryTreeNodePtr resolved_query_tree = query_tree;
            if (!default_database.empty())
                fillInDefaultDatabaseForEmptyDatabaseTables(resolved_query_tree, new_context, default_database);

            auto interpreter = InterpreterSelectQueryAnalyzer(resolved_query_tree, new_context, select_query_options);
            query_plan = std::make_unique<QueryPlan>(std::move(interpreter).extractQueryPlan());
        }
        else
        {
            auto interpreter = InterpreterSelectQueryAnalyzer(query_ast, new_context, select_query_options);
            query_plan = std::make_unique<QueryPlan>(std::move(interpreter).extractQueryPlan());
        }
    }
    else
    {
        auto interpreter = InterpreterSelectQuery(query_ast, new_context, select_query_options);
        interpreter.buildQueryPlan(*query_plan);
    }

    addConvertingActions(*query_plan, header, new_context);
    return query_plan;
}

}
