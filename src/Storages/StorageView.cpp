#include <Access/ViewDefinerDependencies.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/TreeRewriter.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTExpressionList.h>

#include <Storages/AlterCommands.h>
#include <Storages/StorageView.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryDescription.h>

#include <Common/CurrentThread.h>
#include <Common/typeid_cast.h>

#include <Core/Names.h>
#include <Core/Settings.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>

#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/QueryParameterVisitor.h>
#include <Storages/StorageWithCommonVirtualColumns.h>

#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/QueryTreePassManager.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Planner/findQueryForParallelReplicas.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsSetOperationMode except_default_mode;
    extern const SettingsBool extremes;
    extern const SettingsSetOperationMode intersect_default_mode;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_allow_view_over_mergetree;
    extern const SettingsBool enable_positional_arguments;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int VIOLATED_CONSTRAINT;
}


namespace
{

bool isNullableOrLcNullable(DataTypePtr type)
{
    if (type->isNullable())
        return true;

    if (const auto * lc_type = typeid_cast<const DataTypeLowCardinality *>(type.get()))
        return lc_type->getDictionaryType()->isNullable();

    return false;
}

/// Returns `true` if there are nullable column in src but corresponding column in dst is not
bool changedNullabilityOneWay(const Block & src_block, const Block & dst_block)
{
    std::unordered_map<String, bool> src_nullable;
    for (const auto & col : src_block)
        src_nullable[col.name] = isNullableOrLcNullable(col.type);

    for (const auto & col : dst_block)
    {
        if (!isNullableOrLcNullable(col.type) && src_nullable[col.name])
            return true;
    }
    return false;
}

bool hasJoin(const ASTSelectQuery & select)
{
    const auto & tables = select.tables();
    if (!tables || tables->children.size() < 2)
        return false;

    const auto & joined_table = tables->children[1]->as<ASTTablesInSelectQueryElement &>();
    return joined_table.table_join != nullptr;
}

bool hasJoin(const ASTSelectWithUnionQuery & ast)
{
    for (const auto & child : ast.list_of_selects->children)
    {
        if (const auto * select = child->as<ASTSelectQuery>(); select && hasJoin(*select))
            return true;
    }
    return false;
}

/// Extracts the single ASTSelectQuery from the view definition.
/// Throws if the view uses UNION.
const ASTSelectQuery & getSingleSelectQuery(const StorageMetadataPtr & metadata, const StorageID & view_id)
{
    const auto & inner_query = metadata->getSelectQuery().inner_query;
    const auto * select_union = inner_query->as<ASTSelectWithUnionQuery>();

    if (!select_union || select_union->list_of_selects->children.size() != 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {} because its query contains UNION",
            view_id.getFullTableName());

    const auto * select = select_union->list_of_selects->children[0]->as<ASTSelectQuery>();
    if (!select)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {} because its query is not a simple SELECT",
            view_id.getFullTableName());

    return *select;
}

/// Validates the view's SELECT query is simple enough for INSERTs.
void validateViewSelectForInsert(const ASTSelectQuery & select, const StorageID & view_id)
{
    if (select.prewhere())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {} because its query contains PREWHERE",
            view_id.getFullTableName());

    if (select.distinct)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {} because its query contains DISTINCT",
            view_id.getFullTableName());

    if (select.groupBy())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {} because its query contains GROUP BY",
            view_id.getFullTableName());

    if (select.having())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {} because its query contains HAVING",
            view_id.getFullTableName());

    if (select.limitLength() || select.limitBy())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {} because its query contains LIMIT",
            view_id.getFullTableName());

    if (hasJoin(select))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {} because its query contains a JOIN",
            view_id.getFullTableName());

    /// A `WITH` clause can introduce aliases that look like simple column references in the SELECT list
    /// (e.g. `WITH a + 1 AS x SELECT x FROM t`) but do not correspond to columns of the underlying table.
    /// Rejecting `WITH` keeps the "simple projection" contract honest.
    if (const auto & with_expr = select.with(); with_expr && !with_expr->children.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {} because its query contains a WITH clause",
            view_id.getFullTableName());

    /// If the view has a WHERE clause with identifiers, we need to ensure they can be evaluated.
    /// For now, we accept WHERE clauses as-is and rely on runtime validation during INSERT execution.
    /// Lambda parameters (e.g., in WHERE arrayExists(x -> x > 0, [1])) are not table columns
    /// and should not prevent insertion.
}

/// Extracts column mapping from view column names to target table column names.
/// Returns empty map for asterisk (all columns map 1:1 by name).
///
/// Each identifier in the SELECT list must name a real column of the target table.
/// Otherwise the projection is not a "simple column reference" — for example, the identifier
/// could resolve to a `WITH` alias or another non-table symbol — and the view is rejected
/// as not insertable.
std::unordered_map<String, String> extractColumnMapping(
    const ASTSelectQuery & select,
    const StorageID & view_id,
    const NameSet & target_columns)
{
    std::unordered_map<String, String> mapping;

    if (!select.select())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "View {} has no SELECT expression list", view_id.getFullTableName());

    bool has_asterisk = false;
    bool has_explicit_column = false;

    for (const auto & expr : select.select()->children)
    {
        if (const auto * asterisk = expr->as<ASTAsterisk>())
        {
            if (asterisk->transformers)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot INSERT into view {} because its SELECT list contains * with column transformers",
                    view_id.getFullTableName());

            has_asterisk = true;
            continue;
        }

        if (const auto * qualified_asterisk = expr->as<ASTQualifiedAsterisk>())
        {
            if (qualified_asterisk->transformers)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot INSERT into view {} because its SELECT list contains * with column transformers",
                    view_id.getFullTableName());

            has_asterisk = true;
            continue;
        }

        /// Must be a simple column reference (possibly with alias).
        const auto * identifier = expr->as<ASTIdentifier>();
        if (!identifier)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Cannot INSERT into view {} because its SELECT list contains "
                "expressions that are not simple column references",
                view_id.getFullTableName());

        has_explicit_column = true;
        if (has_asterisk)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Cannot INSERT into view {} because its SELECT list mixes * with explicit columns",
                view_id.getFullTableName());

        String target_col = identifier->shortName();
        if (!target_columns.contains(target_col))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Cannot INSERT into view {} because identifier '{}' in its SELECT list "
                "does not refer to a column of the underlying table",
                view_id.getFullTableName(), target_col);

        String view_col = identifier->tryGetAlias();
        if (view_col.empty())
            view_col = target_col;

        if (view_col != target_col)
            mapping[view_col] = target_col;
    }

    if (has_asterisk && has_explicit_column)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {} because its SELECT list mixes * with explicit columns",
            view_id.getFullTableName());

    if (has_asterisk)
        return {};

    return mapping;
}

/// Gets the target table from the view's FROM clause.
StoragePtr getViewTargetTable(
    const ASTSelectQuery & select,
    const StorageID & view_id,
    ContextPtr context)
{
    const auto & tables = select.tables();
    if (!tables || tables->children.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {}: no tables in FROM clause",
            view_id.getFullTableName());

    if (tables->children.size() > 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {} because its query references multiple tables",
            view_id.getFullTableName());

    const auto * element = tables->children[0]->as<ASTTablesInSelectQueryElement>();
    if (!element || !element->table_expression)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {}: unsupported FROM clause",
            view_id.getFullTableName());

    const auto * table_expr = element->table_expression->as<ASTTableExpression>();
    if (!table_expr || !table_expr->database_and_table_name)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {}: FROM clause must reference a table directly",
            view_id.getFullTableName());

    const auto * table_id = table_expr->database_and_table_name->as<ASTTableIdentifier>();
    if (!table_id)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into view {}: FROM clause must reference a table directly",
            view_id.getFullTableName());

    auto resolved_id = context->resolveStorageID(table_id->getTableId());
    return DatabaseCatalog::instance().getTable(resolved_id, context);
}


/// Sink that inserts data into the target table of a view.
/// Handles column name mapping (for aliased views) and WHERE constraint checking.
class SinkToStorageView : public SinkToStorage
{
public:
    SinkToStorageView(
        SharedHeader view_header,
        StoragePtr target_table,
        ContextPtr context,
        std::unordered_map<String, String> column_mapping,
        Names user_specified_columns,
        ASTPtr where_condition,
        const StorageID & view_id,
        bool async_insert)
        : SinkToStorage(view_header)
        , target_table_(std::move(target_table))
        , column_mapping_(std::move(column_mapping))
        , user_specified_columns_(std::move(user_specified_columns))
        , view_id_(view_id)
    {
        auto insert_context = Context::createCopy(context);

        /// Build an INSERT query targeting the underlying table.
        auto insert_query = make_intrusive<ASTInsertQuery>();
        insert_query->table_id = target_table_->getStorageID();

        /// Determine which view columns to forward to the underlying table.
        /// If the user wrote `INSERT INTO view (col1, col2) ...`, only those columns
        /// are forwarded so that any other column gets the *target table's* default
        /// rather than the view-schema default that `InsertDependenciesBuilder::createPreSink`
        /// would otherwise fill in.
        Names forwarded_view_columns;
        if (!user_specified_columns_.empty())
        {
            forwarded_view_columns = user_specified_columns_;
        }
        else
        {
            forwarded_view_columns.reserve(getHeader().columns());
            for (const auto & col : getHeader().getColumnsWithTypeAndName())
                forwarded_view_columns.push_back(col.name);
        }

        /// Set column list: view columns mapped to target column names.
        auto columns_ast = make_intrusive<ASTExpressionList>();
        for (const auto & view_name : forwarded_view_columns)
        {
            auto it = column_mapping_.find(view_name);
            String target_name = (it != column_mapping_.end()) ? it->second : view_name;
            columns_ast->children.push_back(make_intrusive<ASTIdentifier>(target_name));
        }
        insert_query->columns = columns_ast;
        insert_query->children.push_back(insert_query->columns);

        /// Create the inner INSERT pipeline for the target table.
        ASTPtr insert_query_ptr = insert_query;
        InterpreterInsertQuery interpreter(insert_query_ptr, insert_context, /*allow_materialized=*/false, /*no_squash=*/false, /*no_destination=*/false, async_insert);
        auto block_io = interpreter.execute();
        pipeline_ = std::move(block_io.pipeline);
        executor_ = std::make_unique<PushingPipelineExecutor>(pipeline_);
        executor_->start();

        /// Build WHERE constraint expression if the view has a WHERE clause.
        if (where_condition)
        {
            auto where_clone = where_condition->clone();

            /// Provide both view names and target names for aliases used in the `WHERE` condition.
            NamesAndTypesList source_columns;
            NameSet source_column_names;
            for (const auto & col : getHeader().getColumnsWithTypeAndName())
            {
                source_columns.emplace_back(col.name, col.type);
                source_column_names.insert(col.name);

                auto it = column_mapping_.find(col.name);
                String target_name = (it != column_mapping_.end()) ? it->second : col.name;
                if (!source_column_names.contains(target_name))
                {
                    source_columns.emplace_back(target_name, col.type);
                    source_column_names.insert(target_name);
                }
            }

            TreeRewriterResultPtr syntax;
            try
            {
                syntax = TreeRewriter(context).analyze(where_clone, source_columns);
            }
            catch (const Exception & e)
            {
                if (e.code() != ErrorCodes::UNKNOWN_IDENTIFIER)
                    throw;

                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Cannot INSERT into view {} because its WHERE condition references columns "
                    "that are not projected by the view",
                    view_id_.getFullTableName());
            }

            where_actions_ = ExpressionAnalyzer(where_clone, syntax, context).getActions(false, true);
            where_column_name_ = where_clone->getColumnName();
        }

        addInterpreterContext(std::move(insert_context));
    }

    String getName() const override { return "SinkToStorageView"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.detachColumns());

        /// Check the `WHERE` constraint: every inserted row must satisfy the view's condition.
        if (where_actions_)
        {
            Block check_block(block);
            for (const auto & [view_name, target_name] : column_mapping_)
            {
                if (check_block.has(view_name) && !check_block.has(target_name))
                {
                    auto target_column = check_block.getByName(view_name);
                    target_column.name = target_name;
                    check_block.insert(std::move(target_column));
                }
            }

            where_actions_->execute(check_block);

            const auto & result_column = check_block.getByName(where_column_name_).column;
            for (size_t i = 0; i < result_column->size(); ++i)
            {
                if (result_column->isNullAt(i) || !result_column->getBool(i))
                    throw Exception(
                        ErrorCodes::VIOLATED_CONSTRAINT,
                        "Cannot INSERT into view {}: the inserted row does not satisfy "
                        "the view's WHERE condition",
                        view_id_.getFullTableName());
            }
        }

        /// If the user listed an explicit subset of view columns in the INSERT,
        /// drop the other columns from the block before forwarding so that the
        /// target table applies its own defaults to them.
        if (!user_specified_columns_.empty())
        {
            Block forwarded_block;
            for (const auto & view_name : user_specified_columns_)
                forwarded_block.insert(block.getByName(view_name));
            block = std::move(forwarded_block);
        }

        /// Rename view columns to target table columns.
        for (const auto & [view_name, target_name] : column_mapping_)
            if (block.has(view_name))
                block.getByName(view_name).name = target_name;

        /// Push to the inner `INSERT` pipeline which handles defaults, constraints, and writing.
        executor_->push(std::move(block));
    }

    void onFinish() override
    {
        executor_->finish();
    }

private:
    StoragePtr target_table_;
    std::unordered_map<String, String> column_mapping_;
    Names user_specified_columns_;
    StorageID view_id_;

    QueryPipeline pipeline_;
    std::unique_ptr<PushingPipelineExecutor> executor_;

    ExpressionActionsPtr where_actions_;
    String where_column_name_;
};


/** There are no limits on the maximum size of the result for the view.
  *  Since the result of the view is not the result of the entire query.
  *
  * The context is also marked as a view inner context so that the query analyzer
  * resolves positional arguments inside the view even on remote/secondary nodes
  * (views are expanded on remote nodes, unlike the outer query).
  */
ContextPtr getViewContext(ContextPtr context, const StorageSnapshotPtr & storage_snapshot, const StorageView * view)
{
    auto view_context = storage_snapshot->metadata->getSQLSecurityOverriddenContext(context);
    Settings view_settings = view_context->getSettingsCopy();

    if (context->canUseParallelReplicasOnInitiator() && view_settings[Setting::parallel_replicas_allow_view_over_mergetree])
    {
        if (auto storage = view->getUnderlyingMergeTreeStorageForParallelReplicas(context))
            view_settings[Setting::allow_experimental_parallel_reading_from_replicas] = Field{0};
    }

    view_settings[Setting::max_result_rows] = 0;
    view_settings[Setting::max_result_bytes] = 0;
    view_settings[Setting::extremes] = false;
    view_context->setSettings(view_settings);
    view_context->setIsViewInnerQuery(true);
    return view_context;
}

}

VirtualColumnsDescription StorageView::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

StorageView::StorageView(
    const StorageID & table_id_,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns_,
    const String & comment,
    bool is_parameterized_view_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    if (!is_parameterized_view_)
    {
        /// If CREATE query is to create parameterized view, then we dont want to set columns
        if (!query.isParameterizedView())
            storage_metadata.setColumns(columns_);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setComment(comment);
    if (query.sql_security)
        storage_metadata.setSQLSecurity(query.sql_security->as<ASTSQLSecurity &>());

    if (storage_metadata.sql_security_type == SQLSecurityType::DEFINER)
        ViewDefinerDependencies::instance().addViewDependency(*storage_metadata.definer, table_id_);

    if (!query.select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "SELECT query is not specified for {}", getName());
    SelectQueryDescription description;

    description.inner_query = query.select->ptr();

    NormalizeSelectWithUnionQueryVisitor::Data data{SetOperationMode::Unspecified};
    NormalizeSelectWithUnionQueryVisitor{data}.visit(description.inner_query);

    is_parameterized_view = is_parameterized_view_ || query.isParameterizedView();
    storage_metadata.setSelectQuery(description);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

/// Build and resolve the view's inner query tree
/// Then find the leftmost underlying MT storage eligible for parallel replicas.
/// Returns nullptr if the view is too complex or resolution fails.
StoragePtr StorageView::getUnderlyingMergeTreeStorageForParallelReplicas(const ContextPtr & context) const
{
    if (isParameterizedView())
        return nullptr;

    /// When called from INSERT ... SELECT context, the context carries insertion table info.
    /// If we resolve the view's inner query with this context, table functions like file()
    /// may incorrectly infer schema from the insertion table (via use_structure_from_insertion_table_in_table_functions),
    /// poisoning the schema cache with wrong column names.
    if (context->hasInsertionTable())
        return nullptr;

    auto inner_query_ast = getInMemoryMetadataPtr(context, false)->getSelectQuery().inner_query;

    QueryTreeNodePtr inner_query_tree;
    try
    {
        inner_query_tree = buildQueryTree(inner_query_ast->clone(), context);
        QueryTreePassManager pass_manager(context);
        addQueryTreePasses(pass_manager);
        pass_manager.runOnlyResolve(inner_query_tree);
    }
    catch (const Exception &)
    {
        /// The view may reference table functions, use SQL SECURITY DEFINER,
        /// or have other constructs that prevent resolution with the current user's context.
        /// Example: 03667_view_with_s3_cluster_and_sql_security_definer.
        /// Just return nullptr to indicate the view is not suitable for this optimization.
        tryLogCurrentException(
            __func__, fmt::format("Failed to resolve inner query of view {}", getStorageID().getFullTableName()), LogsLevel::trace);
        return nullptr;
    }

    /// Recursively walk the resolved query tree to find the underlying MergeTree storage.
    /// For UNION nodes, all branches must be eligible.
    /// Returns nullptr if the view is not suitable for parallel replicas.
    std::function<StoragePtr(const IQueryTreeNode *)> find_storage = [&](const IQueryTreeNode * node) -> StoragePtr
    {
        while (node)
        {
            switch (node->getNodeType())
            {
                case QueryTreeNodeType::QUERY:
                {
                    const auto & query_node = node->as<QueryNode &>();
                    /// Only simple pass-through views are eligible. Any clause that changes
                    /// result semantics when evaluated per-replica must disqualify the view.
                    if (query_node.hasGroupBy() || query_node.hasHaving()
                        || query_node.hasWindow() || query_node.hasQualify()
                        || query_node.hasOrderBy() || query_node.isDistinct()
                        || query_node.hasLimitByLimit() || query_node.hasLimitByOffset()
                        || query_node.hasLimitBy()
                        || query_node.hasLimit() || query_node.hasOffset()
                        || hasWindowFunctionNodes(query_node.getProjectionNode()))
                        return nullptr;

                    node = query_node.getJoinTree().get();
                    break;
                }
                case QueryTreeNodeType::UNION:
                {
                    const auto & union_node = node->as<UnionNode &>();

                    /// Only UNION ALL is safe to parallelize.
                    if (union_node.getUnionMode() != SelectUnionMode::UNION_ALL)
                        return nullptr;

                    const auto & queries = union_node.getQueries().getNodes();
                    if (queries.empty())
                        return nullptr;

                    /// Check ALL branches of the UNION — not just the first one.
                    /// Every branch must resolve to an eligible MergeTree storage.
                    /// The branches may reference different tables, but if the same
                    /// table appears in multiple branches, reject it —
                    /// we avoid supporting it, since it requires to complicate parallel replicas protocol
                    /// and considered as not very practical case
                    StoragePtr result;
                    std::unordered_set<StorageID, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual> seen_ids;
                    for (const auto & query : queries)
                    {
                        auto branch_storage = find_storage(query.get());
                        if (!branch_storage)
                            return nullptr;

                        if (!seen_ids.insert(branch_storage->getStorageID()).second)
                            return nullptr;

                        if (!result)
                            result = branch_storage;
                    }
                    return result;
                }
                case QueryTreeNodeType::TABLE:
                {
                    const auto & table_node = node->as<const TableNode &>();
                    const auto & storage = table_node.getStorage();

                    /// If the table is itself a view, recursively check its inner query.
                    const auto * nested_view = typeid_cast<const StorageView *>(storage.get());
                    if (nested_view)
                        return nested_view->getUnderlyingMergeTreeStorageForParallelReplicas(context);

                    if (!isTableNodeEligibleForParallelReplicas(table_node, storage, context))
                        return nullptr;

                    return table_node.getStorage();
                }
                default:
                    return nullptr;
            }
        }
        return nullptr;
    };

    return find_storage(inner_query_tree.get());
}

void StorageView::readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        const size_t /*max_block_size*/,
        const size_t /*num_streams*/)
{
    ASTPtr current_inner_query = storage_snapshot->metadata->getSelectQuery().inner_query;

    if (query_info.view_query)
    {
        if (!query_info.view_query->as<ASTSelectWithUnionQuery>())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected optimized VIEW query");
        current_inner_query = query_info.view_query->clone();
    }

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);

    if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
    {
        auto view_context = getViewContext(context, storage_snapshot, this);
        InterpreterSelectQueryAnalyzer interpreter(
            current_inner_query, view_context, options, column_names, query_info.filter_actions_dag.get());
        interpreter.addStorageLimits(*query_info.storage_limits);
        query_plan = std::move(interpreter).extractQueryPlan();
    }
    else
    {
        auto view_context = getViewContext(context, storage_snapshot, this);
        InterpreterSelectWithUnionQuery interpreter(current_inner_query, view_context, options, column_names);
        interpreter.addStorageLimits(*query_info.storage_limits);
        interpreter.buildQueryPlan(query_plan);
    }

    /// It's expected that the columns read from storage are not constant.
    /// Because method 'getSampleBlockForColumns' is used to obtain a structure of result in InterpreterSelectQuery.
    ActionsDAG materializing_actions(query_plan.getCurrentHeader()->getColumnsWithTypeAndName());
    materializing_actions.addMaterializingOutputActions(/*materialize_sparse=*/ true);

    auto materializing = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(materializing_actions));
    materializing->setStepDescription("Materialize constants after VIEW subquery");
    query_plan.addStep(std::move(materializing));

    /// And also convert to expected structure.
    const auto & expected_header = storage_snapshot->getSampleBlockForColumns(column_names);
    const auto & header = query_plan.getCurrentHeader();

    const auto * select_with_union = current_inner_query->as<ASTSelectWithUnionQuery>();
    if (select_with_union && hasJoin(*select_with_union) && changedNullabilityOneWay(*header, expected_header))
    {
        throw DB::Exception(ErrorCodes::INCORRECT_QUERY,
                            "Query from view {} returned Nullable column having not Nullable type in structure. "
                            "If query from view has JOIN, it may be cause by different values of 'join_use_nulls' setting. "
                            "You may explicitly specify 'join_use_nulls' in 'CREATE VIEW' query to avoid this error",
                            getStorageID().getFullTableName());
    }

    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            header->getColumnsWithTypeAndName(),
            expected_header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            context);

    auto converting = std::make_unique<ExpressionStep>(query_plan.getCurrentHeader(), std::move(convert_actions_dag));
    converting->setStepDescription("Convert VIEW subquery result to VIEW table structure");
    query_plan.addStep(std::move(converting));
}

SinkToStoragePtr StorageView::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool async_insert)
{
    if (is_parameterized_view)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Cannot INSERT into parameterized view {}", getStorageID().getFullTableName());

    const auto & select = getSingleSelectQuery(metadata_snapshot, getStorageID());
    validateViewSelectForInsert(select, getStorageID());

    /// Use the view's SQL security context for accessing the target table.
    auto context = metadata_snapshot->getSQLSecurityOverriddenContext(local_context);
    auto target_table = getViewTargetTable(select, getStorageID(), context);

    NameSet target_columns;
    for (const auto & col : target_table->getInMemoryMetadataPtr(context, false)->getColumns().getOrdinary())
        target_columns.insert(col.name);

    auto column_mapping = extractColumnMapping(select, getStorageID(), target_columns);

    /// Honor an explicit column list in `INSERT INTO view (col1, col2) ...`.
    /// Without this, omitted view columns would receive the *view-schema* default
    /// (filled in by `InsertDependenciesBuilder::createPreSink`) instead of the
    /// target table's own default.
    Names user_specified_columns;
    if (const auto * insert_query = query ? query->as<ASTInsertQuery>() : nullptr; insert_query && insert_query->columns)
    {
        const auto & view_sample = metadata_snapshot->getSampleBlockNonMaterialized();
        for (const auto & child : insert_query->columns->children)
        {
            if (const auto * id = child->as<ASTIdentifier>())
            {
                const String name = id->shortName();
                if (view_sample.has(name))
                    user_specified_columns.push_back(name);
            }
        }
    }

    /// The view's WHERE clause becomes a constraint for inserts.
    ASTPtr where_condition = select.where() ? select.where()->clone() : nullptr;

    auto view_header = std::make_shared<const Block>(metadata_snapshot->getSampleBlockNonMaterialized());

    return std::make_shared<SinkToStorageView>(
        std::move(view_header),
        std::move(target_table),
        context,
        std::move(column_mapping),
        std::move(user_specified_columns),
        std::move(where_condition),
        getStorageID(),
        async_insert);
}

void StorageView::drop()
{
    auto table_id = getStorageID();

    if (getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false)->sql_security_type == SQLSecurityType::DEFINER)
        ViewDefinerDependencies::instance().removeViewDependencies(table_id);
}

void StorageView::alter(
    const AlterCommands & params,
    ContextPtr context,
    AlterLockHolder &)
{
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = *getInMemoryMetadataPtr(context, false);
    StorageInMemoryMetadata old_metadata = *getInMemoryMetadataPtr(context, false);
    params.apply(new_metadata, context);

    DatabaseCatalog::instance()
        .getDatabase(table_id.database_name)
        ->alterTable(context, table_id, new_metadata, /*validate_new_create_query=*/true);

    auto & instance = ViewDefinerDependencies::instance();
    if (old_metadata.sql_security_type == SQLSecurityType::DEFINER)
        instance.removeViewDependencies(table_id);

    if (new_metadata.sql_security_type == SQLSecurityType::DEFINER)
        instance.addViewDependency(*new_metadata.definer, table_id);

    setInMemoryMetadata(new_metadata);
}

static ASTTableExpression * getFirstTableExpression(ASTSelectQuery & select_query)
{
    if (!select_query.tables() || select_query.tables()->children.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No table expression in view select AST");

    auto * select_element = select_query.tables()->children[0]->as<ASTTablesInSelectQueryElement>();

    if (!select_element->table_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect table expression");

    return select_element->table_expression->as<ASTTableExpression>();
}

void StorageView::replaceQueryParametersIfParameterizedView(ASTPtr & outer_query, const NameToNameMap & parameter_values)
{
    ReplaceQueryParameterVisitor visitor(parameter_values);
    visitor.visit(outer_query);
}

void StorageView::replaceWithSubquery(ASTSelectQuery & outer_query, ASTPtr view_query, ASTPtr & view_name, bool parameterized_view)
{
    ASTTableExpression * table_expression = getFirstTableExpression(outer_query);

    if (!table_expression->database_and_table_name)
    {
        /// If it's a view or merge table function, add a fake db.table name.
        /// For parameterized view, the function name is the db.view name, so add the function name
        if (table_expression->table_function)
        {
            auto table_function_name = table_expression->table_function->as<ASTFunction>()->name;
            if (table_function_name == "view" || table_function_name == "viewIfPermitted")
                table_expression->database_and_table_name = make_intrusive<ASTTableIdentifier>("__view");
            else if (table_function_name == "merge")
                table_expression->database_and_table_name = make_intrusive<ASTTableIdentifier>("__merge");
            else if (parameterized_view)
                table_expression->database_and_table_name = make_intrusive<ASTTableIdentifier>(table_function_name);

        }
        if (!table_expression->database_and_table_name)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect table expression");
    }

    DatabaseAndTableWithAlias db_table(table_expression->database_and_table_name);
    String alias = db_table.alias.empty() ? db_table.table : db_table.alias;

    view_name = table_expression->database_and_table_name;
    table_expression->database_and_table_name = {};
    table_expression->subquery = make_intrusive<ASTSubquery>(view_query);
    table_expression->subquery->setAlias(alias);

    for (auto & child : table_expression->children)
        if (child.get() == view_name.get())
            child = view_query;
        else if (child.get()
                 && child->as<ASTFunction>()
                 && table_expression->table_function
                 && table_expression->table_function->as<ASTFunction>()
                 && child->as<ASTFunction>()->name == table_expression->table_function->as<ASTFunction>()->name)
            child = view_query;
}

ASTPtr StorageView::restoreViewName(ASTSelectQuery & select_query, const ASTPtr & view_name)
{
    ASTTableExpression * table_expression = getFirstTableExpression(select_query);

    if (!table_expression->subquery)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect table expression");

    ASTPtr subquery = table_expression->subquery;
    table_expression->subquery = {};
    table_expression->database_and_table_name = view_name;

    for (auto & child : table_expression->children)
        if (child.get() == subquery.get())
            child = view_name;
    return subquery->children[0];
}

void StorageView::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* local_context */) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter() && command.type != AlterCommand::MODIFY_SQL_SECURITY)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
    }
}

void registerStorageView(StorageFactory & factory)
{
    factory.registerStorage("View", [](const StorageFactory::Arguments & args)
    {
        if (args.query.storage)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Specifying ENGINE is not allowed for a View");

        /// Resolve INTERSECT/EXCEPT precedence before constructing StorageView.
        /// StorageView's constructor runs NormalizeSelectWithUnionQueryVisitor which
        /// does not understand INTERSECT/EXCEPT modes and would incorrectly drop
        /// SELECT branches connected by these operators.
        /// This is needed when the AST is freshly parsed from stored metadata
        /// (e.g. during ATTACH) and has not been through executeQuery's visitors.
        /// For already-processed ASTs (e.g. from CREATE VIEW via executeQuery),
        /// this is a safe no-op since INTERSECT/EXCEPT modes have already been
        /// converted to ASTSelectIntersectExceptQuery nodes.
        if (args.query.select)
        {
            auto context = args.getContext();
            SelectIntersectExceptQueryVisitor::Data data{
                context->getSettingsRef()[Setting::intersect_default_mode],
                context->getSettingsRef()[Setting::except_default_mode]};
            auto select = args.query.select->ptr();
            SelectIntersectExceptQueryVisitor{data}.visit(select);
        }

        return std::make_shared<StorageView>(args.table_id, args.query, args.columns, args.comment);
    });
}

ContextPtr StorageView::getViewSubqueryContext(ContextPtr context, const StorageSnapshotPtr &storage_snapshot)
{
    auto view_context = storage_snapshot->metadata->getSQLSecurityOverriddenContext(context);
    Settings view_settings = view_context->getSettingsCopy();
    view_settings[Setting::max_result_rows] = 0;
    view_settings[Setting::max_result_bytes] = 0;
    view_settings[Setting::extremes] = false;
    view_context->setSettings(view_settings);
    return view_context;
}

}
