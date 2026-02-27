#include <Processors/Sources/RecursiveCTESource.h>

#include <Storages/IStorage.h>
#include <Storages/StorageMemory.h>

#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Transforms/SquashingTransform.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <QueryPipeline/Chain.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>

#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ListNode.h>

#include <Core/Settings.h>

#include <set>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace Setting
{
    extern const SettingsMap additional_table_filters;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsUInt64 max_recursive_cte_evaluation_depth;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_DEEP_RECURSION;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

namespace
{

std::vector<TableNode *> collectTableNodesWithTemporaryTableName(const std::string & temporary_table_name, IQueryTreeNode * root)
{
    std::vector<TableNode *> result;

    std::vector<IQueryTreeNode *> nodes_to_process;
    nodes_to_process.push_back(root);

    while (!nodes_to_process.empty())
    {
        auto * subtree_node = nodes_to_process.back();
        nodes_to_process.pop_back();

        auto * table_node = subtree_node->as<TableNode>();
        if (table_node && table_node->getTemporaryTableName() == temporary_table_name)
            result.push_back(table_node);

        for (auto & child : subtree_node->getChildren())
        {
            if (child)
                nodes_to_process.push_back(child.get());
        }
    }

    return result;
}

/// Information about a join key between a CTE table and a real table.
struct JoinKeyInfo
{
    String real_table_column_name;
    String cte_column_name;
    StorageID real_table_storage_id;
};

/// Check if a query tree node pointer matches one of the CTE table nodes.
bool isCTETableNode(const IQueryTreeNode * node, const std::vector<TableNode *> & recursive_table_nodes)
{
    for (const auto * table_node : recursive_table_nodes)
        if (node == table_node)
            return true;
    return false;
}

/// Extract equi-join key pairs from an ON join expression.
/// Handles single `equals` and `AND`-combined conditions.
void extractEquiJoinKeys(
    const QueryTreeNodePtr & expression,
    const std::vector<TableNode *> & recursive_table_nodes,
    std::vector<JoinKeyInfo> & result)
{
    const auto * function_node = expression->as<FunctionNode>();
    if (!function_node)
        return;

    if (function_node->getFunctionName() == "and")
    {
        for (const auto & arg : function_node->getArguments().getNodes())
            extractEquiJoinKeys(arg, recursive_table_nodes, result);
        return;
    }

    if (function_node->getFunctionName() != "equals")
        return;

    const auto & args = function_node->getArguments().getNodes();
    if (args.size() != 2)
        return;

    const auto * left_column = args[0]->as<ColumnNode>();
    const auto * right_column = args[1]->as<ColumnNode>();
    if (!left_column || !right_column)
        return;

    auto left_source = left_column->getColumnSourceOrNull();
    auto right_source = right_column->getColumnSourceOrNull();
    if (!left_source || !right_source)
        return;

    bool left_is_cte = isCTETableNode(left_source.get(), recursive_table_nodes);
    bool right_is_cte = isCTETableNode(right_source.get(), recursive_table_nodes);

    /// Both columns from the same side (both CTE or both real) — skip.
    if (left_is_cte == right_is_cte)
        return;

    const auto * cte_column = left_is_cte ? left_column : right_column;
    const auto * real_column = left_is_cte ? right_column : left_column;
    const auto & real_source = left_is_cte ? right_source : left_source;

    const auto * real_table_node = real_source->as<TableNode>();
    if (!real_table_node)
        return;

    result.push_back({
        real_column->getColumnName(),
        cte_column->getColumnName(),
        real_table_node->getStorageID()
    });
}

/// Walk the join tree of the recursive query to find equi-join keys
/// between the CTE table and real tables.
/// Only handles INNER JOINs with ON expressions.
std::vector<JoinKeyInfo> findJoinKeysWithCTETable(
    const QueryTreeNodePtr & recursive_query,
    const std::vector<TableNode *> & recursive_table_nodes)
{
    std::vector<JoinKeyInfo> result;

    const auto * query_node = recursive_query->as<QueryNode>();
    if (!query_node)
        return result;

    /// Walk the join tree to find all JoinNodes.
    std::vector<IQueryTreeNode *> nodes_to_visit;
    nodes_to_visit.push_back(query_node->getJoinTree().get());

    while (!nodes_to_visit.empty())
    {
        auto * node = nodes_to_visit.back();
        nodes_to_visit.pop_back();

        auto * join_node = node->as<JoinNode>();
        if (!join_node)
            continue;

        if (join_node->getKind() == JoinKind::Inner
            && join_node->hasJoinExpression()
            && join_node->isOnJoinExpression())
        {
            extractEquiJoinKeys(join_node->getJoinExpression(), recursive_table_nodes, result);
        }

        nodes_to_visit.push_back(join_node->getLeftTableExpression().get());
        nodes_to_visit.push_back(join_node->getRightTableExpression().get());
    }

    return result;
}

/// Read all values of a column from a StorageMemory-backed temporary table.
std::vector<Field> readColumnValuesFromMemoryStorage(
    const StoragePtr & storage,
    const String & column_name,
    const ContextPtr & context)
{
    std::vector<Field> values;

    auto * memory_storage = typeid_cast<StorageMemory *>(storage.get());
    if (!memory_storage)
        return values;

    auto metadata = memory_storage->getInMemoryMetadataPtr();
    auto snapshot = memory_storage->getStorageSnapshot(metadata, context);
    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*snapshot->data);

    if (!snapshot_data.blocks)
        return values;

    for (const auto & block : *snapshot_data.blocks)
    {
        if (!block.has(column_name))
            continue;

        const auto & column = block.getByName(column_name).column;
        for (size_t i = 0; i < column->size(); ++i)
        {
            Field value;
            column->get(i, value);
            values.push_back(std::move(value));
        }
    }

    return values;
}

/// Build a SQL filter expression like: `column_name` IN (val1, val2, ...)
String buildInFilterExpression(const String & column_name, const std::vector<Field> & values)
{
    if (values.empty())
        return {};

    String result = backQuoteIfNeed(column_name) + " IN (";
    for (size_t i = 0; i < values.size(); ++i)
    {
        if (i > 0)
            result += ", ";
        result += applyVisitor(FieldVisitorToString(), values[i]);
    }
    result += ")";
    return result;
}

/// Build the `additional_table_filters` Map for a recursive CTE step.
/// Reads join key values from the working (CTE) table and creates IN filters
/// for the corresponding real tables.
/// Merges with user-specified `additional_table_filters` to avoid overwriting them.
Map buildAdditionalTableFiltersForRecursiveStep(
    const QueryTreeNodePtr & recursive_query,
    std::optional<std::vector<JoinKeyInfo>> & cached_join_keys,
    const std::vector<TableNode *> & recursive_table_nodes,
    const StoragePtr & working_table_storage,
    const ContextPtr & context,
    const Map & original_additional_table_filters)
{
    if (!cached_join_keys.has_value())
        cached_join_keys = findJoinKeysWithCTETable(recursive_query, recursive_table_nodes);

    if (cached_join_keys->empty())
        return original_additional_table_filters;

    /// Group filter expressions by real table.
    std::map<String, std::vector<String>> table_filter_parts;

    for (const auto & key_info : *cached_join_keys)
    {
        auto values = readColumnValuesFromMemoryStorage(working_table_storage, key_info.cte_column_name, context);
        if (values.empty())
            continue;

        String filter_expr = buildInFilterExpression(key_info.real_table_column_name, values);
        if (!filter_expr.empty())
            table_filter_parts[key_info.real_table_storage_id.getFullNameNotQuoted()].push_back(std::move(filter_expr));
    }

    /// Index user-specified filters by table name so we can combine them.
    std::map<String, String> user_filter_by_table;
    for (const auto & entry : original_additional_table_filters)
    {
        const auto & tuple = entry.safeGet<Tuple>();
        user_filter_by_table[tuple.at(0).safeGet<String>()] = tuple.at(1).safeGet<String>();
    }

    Map filters_map;
    std::set<String> processed_tables;

    for (auto & [table_name, parts] : table_filter_parts)
    {
        String combined_filter;
        for (size_t i = 0; i < parts.size(); ++i)
        {
            if (i > 0)
                combined_filter += " AND ";
            combined_filter += parts[i];
        }

        /// Combine with user-specified filter for the same table, if any.
        auto it = user_filter_by_table.find(table_name);
        if (it != user_filter_by_table.end())
        {
            combined_filter = "(" + combined_filter + ") AND (" + it->second + ")";
            processed_tables.insert(table_name);
        }

        Tuple tuple;
        tuple.push_back(Field(table_name));
        tuple.push_back(Field(std::move(combined_filter)));
        filters_map.push_back(Field(std::move(tuple)));
    }

    /// Preserve user-specified filters for tables not involved in the CTE join.
    for (const auto & entry : original_additional_table_filters)
    {
        const auto & tuple = entry.safeGet<Tuple>();
        if (!processed_tables.contains(tuple.at(0).safeGet<String>()))
            filters_map.push_back(entry);
    }

    return filters_map;
}

}

class RecursiveCTEChunkGenerator
{
public:
    RecursiveCTEChunkGenerator(SharedHeader header_, QueryTreeNodePtr recursive_cte_union_node_)
        : header(std::move(header_))
        , recursive_cte_union_node(std::move(recursive_cte_union_node_))
    {
        auto & recursive_cte_union_node_typed = recursive_cte_union_node->as<UnionNode &>();
        chassert(recursive_cte_union_node_typed.hasRecursiveCTETable());

        auto & recursive_cte_table = recursive_cte_union_node_typed.getRecursiveCTETable();

        const auto & cte_name = recursive_cte_union_node_typed.getCTEName();
        recursive_table_nodes = collectTableNodesWithTemporaryTableName(cte_name, recursive_cte_union_node.get());
        if (recursive_table_nodes.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "UNION query {} is not recursive", recursive_cte_union_node->formatASTForErrorMessage());

        size_t recursive_cte_union_node_queries_size = recursive_cte_union_node_typed.getQueries().getNodes().size();
        chassert(recursive_cte_union_node_queries_size > 1);

        non_recursive_query = recursive_cte_union_node_typed.getQueries().getNodes()[0];
        recursive_query = recursive_cte_union_node_typed.getQueries().getNodes()[1];

        if (recursive_cte_union_node_queries_size > 2)
        {
            auto working_union_query = std::make_shared<UnionNode>(recursive_cte_union_node_typed.getMutableContext(),
                recursive_cte_union_node_typed.getUnionMode());
            auto & working_union_query_subqueries = working_union_query->getQueries().getNodes();

            for (size_t i = 1; i < recursive_cte_union_node_queries_size; ++i)
                working_union_query_subqueries.push_back(recursive_cte_union_node_typed.getQueries().getNodes()[i]);

            recursive_query = std::move(working_union_query);
        }

        recursive_query_context = recursive_query->as<QueryNode>() ? recursive_query->as<QueryNode &>().getMutableContext() :
            recursive_query->as<UnionNode &>().getMutableContext();

        /// Disable parallel replicas for recursive CTE step queries. When parallel replicas
        /// is enabled, JOINs are rewritten to GLOBAL JOINs and the right-side subquery is
        /// materialized into a cached external table keyed by tree hash. Since the recursive
        /// CTE temporary table has the same tree structure across steps (only the data changes),
        /// the hash stays identical and stale cached data is reused, producing wrong results.
        recursive_query_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(UInt64(0)));

        /// Save the original additional_table_filters so we can merge CTE-derived filters
        /// with user-specified ones on each recursive step, instead of overwriting them.
        original_additional_table_filters = recursive_query_context->getSettingsRef()[Setting::additional_table_filters].value;

        const auto & recursive_query_projection_columns = recursive_query->as<QueryNode>() ? recursive_query->as<QueryNode &>().getProjectionColumns() :
            recursive_query->as<UnionNode &>().computeProjectionColumns();

        if (recursive_cte_table->columns.size() != recursive_query_projection_columns.size())
            throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH,
            "Recursive CTE subquery {}. Expected projection columns to have same size in recursive and non recursive subquery.",
            recursive_cte_union_node->formatASTForErrorMessage());

        working_temporary_table_holder = recursive_cte_table->holder;
        working_temporary_table_storage = recursive_cte_table->storage;

        intermediate_temporary_table_holder = std::make_shared<TemporaryTableHolder>(
            recursive_query_context,
            ColumnsDescription{NamesAndTypesList{recursive_cte_table->columns.begin(), recursive_cte_table->columns.end()}},
            ConstraintsDescription{},
            nullptr /*query*/,
            true /*create_for_global_subquery*/);
        intermediate_temporary_table_storage = intermediate_temporary_table_holder->getTable();
    }

    Chunk generate()
    {
        Chunk current_chunk;

        while (!finished)
        {
            if (!executor.has_value())
                buildStepExecutor();

            while (current_chunk.getNumRows() == 0 && executor->pull(current_chunk))
            {
            }

            read_rows_during_recursive_step += current_chunk.getNumRows();

            if (current_chunk.getNumRows() > 0)
                break;

            executor.reset();

            if (read_rows_during_recursive_step == 0)
            {
                finished = true;
                truncateTemporaryTable(intermediate_temporary_table_storage);
                continue;
            }

            read_rows_during_recursive_step = 0;

            for (auto & recursive_table_node : recursive_table_nodes)
                recursive_table_node->updateStorage(intermediate_temporary_table_storage, recursive_query_context);

            truncateTemporaryTable(working_temporary_table_storage);

            std::swap(intermediate_temporary_table_holder, working_temporary_table_holder);
            std::swap(intermediate_temporary_table_storage, working_temporary_table_storage);
        }

        return current_chunk;
    }

private:
    void buildStepExecutor()
    {
        const auto & recursive_subquery_settings = recursive_query_context->getSettingsRef();

        if (recursive_step > recursive_subquery_settings[Setting::max_recursive_cte_evaluation_depth])
            throw Exception(
                ErrorCodes::TOO_DEEP_RECURSION,
                "Maximum recursive CTE evaluation depth ({}) exceeded, during evaluation of {}. Consider raising "
                "max_recursive_cte_evaluation_depth setting.",
                recursive_subquery_settings[Setting::max_recursive_cte_evaluation_depth].value,
                recursive_cte_union_node->formatASTForErrorMessage());

        auto & query_to_execute = recursive_step > 0 ? recursive_query : non_recursive_query;
        ++recursive_step;

        SelectQueryOptions select_query_options;
        select_query_options.merge_tree_enable_remove_parts_from_snapshot_optimization = false;

        const auto & recursive_table_name = recursive_cte_union_node->as<UnionNode &>().getCTEName();
        recursive_query_context->addOrUpdateExternalTable(recursive_table_name, working_temporary_table_holder);

        /// For recursive steps, inject additional_table_filters to push join key values
        /// into MergeTree's key condition, enabling index usage.
        /// recursive_step was already incremented above, so >1 means we're executing the recursive query.
        if (recursive_step > 1)
        {
            auto filters = buildAdditionalTableFiltersForRecursiveStep(
                recursive_query, cached_join_keys, recursive_table_nodes,
                working_temporary_table_storage, recursive_query_context,
                original_additional_table_filters);

            recursive_query_context->setSetting("additional_table_filters", Field(std::move(filters)));
        }

        auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(query_to_execute, recursive_query_context, select_query_options);
        auto pipeline_builder = interpreter->buildQueryPipeline();

        pipeline_builder.addSimpleTransform([&](const SharedHeader & in_header)
        {
            return std::make_shared<MaterializingTransform>(in_header);
        });

        auto convert_to_temporary_tables_header_actions_dag = ActionsDAG::makeConvertingActions(
            pipeline_builder.getHeader().getColumnsWithTypeAndName(),
            header->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            interpreter->getContext());
        auto convert_to_temporary_tables_header_actions = std::make_shared<ExpressionActions>(std::move(convert_to_temporary_tables_header_actions_dag));
        pipeline_builder.addSimpleTransform([&](const SharedHeader & input_header)
        {
            return std::make_shared<ExpressionTransform>(input_header, convert_to_temporary_tables_header_actions);
        });

        /// TODO: Support squashing transform

        auto intermediate_temporary_table_storage_sink = intermediate_temporary_table_storage->write(
            {},
            intermediate_temporary_table_storage->getInMemoryMetadataPtr(),
            recursive_query_context,
            false /*async_insert*/);

        pipeline_builder.addChain(Chain(std::move(intermediate_temporary_table_storage_sink)));

        pipeline = QueryPipelineBuilder::getPipeline(std::move(pipeline_builder));
        pipeline.setProgressCallback(recursive_query_context->getProgressCallback());
        pipeline.setProcessListElement(recursive_query_context->getProcessListElement());

        executor.emplace(pipeline);
    }

    void truncateTemporaryTable(StoragePtr & temporary_table)
    {
        /// TODO: Support proper locking
        TableExclusiveLockHolder table_exclusive_lock;
        temporary_table->truncate({},
            temporary_table->getInMemoryMetadataPtr(),
            recursive_query_context,
            table_exclusive_lock);
    }

    SharedHeader header;
    QueryTreeNodePtr recursive_cte_union_node;
    std::vector<TableNode *> recursive_table_nodes;

    QueryTreeNodePtr non_recursive_query;
    QueryTreeNodePtr recursive_query;
    ContextMutablePtr recursive_query_context;

    TemporaryTableHolderPtr working_temporary_table_holder;
    StoragePtr working_temporary_table_storage;

    TemporaryTableHolderPtr intermediate_temporary_table_holder;
    StoragePtr intermediate_temporary_table_storage;

    QueryPipeline pipeline;
    std::optional<PullingAsyncPipelineExecutor> executor;

    std::optional<std::vector<JoinKeyInfo>> cached_join_keys;
    Map original_additional_table_filters;

    size_t recursive_step = 0;
    size_t read_rows_during_recursive_step = 0;
    bool finished = false;
};

RecursiveCTESource::RecursiveCTESource(SharedHeader header, QueryTreeNodePtr recursive_cte_union_node_)
    : ISource(header)
    , generator(std::make_unique<RecursiveCTEChunkGenerator>(std::move(header), std::move(recursive_cte_union_node_)))
{}

RecursiveCTESource::~RecursiveCTESource() = default;

Chunk RecursiveCTESource::generate()
{
    return generator->generate();
}

}
