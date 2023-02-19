#include <Planner/PlannerJoinTree.h>

#include <DataTypes/DataTypeString.h>

#include <Functions/FunctionFactory.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>

#include <Storages/IStorage.h>
#include <Storages/StorageDictionary.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/Utils.h>

#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>

#include <Interpreters/Context.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/HashJoin.h>
#include <Interpreters/ArrayJoinAction.h>

#include <Planner/CollectColumnIdentifiers.h>
#include <Planner/Planner.h>
#include <Planner/PlannerJoins.h>
#include <Planner/PlannerActionsVisitor.h>
#include <Planner/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SYNTAX_ERROR;
    extern const int ACCESS_DENIED;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int TOO_MANY_COLUMNS;
    extern const int UNSUPPORTED_METHOD;
}

namespace
{

/// Check if current user has privileges to SELECT columns from table
void checkAccessRights(const TableNode & table_node, const Names & column_names, const ContextPtr & query_context)
{
    const auto & storage_id = table_node.getStorageID();
    const auto & storage_snapshot = table_node.getStorageSnapshot();

    if (column_names.empty())
    {
        /** For a trivial queries like "SELECT count() FROM table", "SELECT 1 FROM table" access is granted if at least
          * one table column is accessible.
          */
        auto access = query_context->getAccess();

        for (const auto & column : storage_snapshot->metadata->getColumns())
        {
            if (access->isGranted(AccessType::SELECT, storage_id.database_name, storage_id.table_name, column.name))
                return;
        }

        throw Exception(ErrorCodes::ACCESS_DENIED,
            "{}: Not enough privileges. To execute this query it's necessary to have grant SELECT for at least one column on {}",
            query_context->getUserName(),
            storage_id.getFullTableName());
    }

    query_context->checkAccess(AccessType::SELECT, storage_id, column_names);
}

NameAndTypePair chooseSmallestColumnToReadFromStorage(const StoragePtr & storage, const StorageSnapshotPtr & storage_snapshot)
{
    /** We need to read at least one column to find the number of rows.
      * We will find a column with minimum <compressed_size, type_size, uncompressed_size>.
      * Because it is the column that is cheapest to read.
      */
    class ColumnWithSize
    {
    public:
        ColumnWithSize(NameAndTypePair column_, ColumnSize column_size_)
            : column(std::move(column_))
            , compressed_size(column_size_.data_compressed)
            , uncompressed_size(column_size_.data_uncompressed)
            , type_size(column.type->haveMaximumSizeOfValue() ? column.type->getMaximumSizeOfValueInMemory() : 100)
        {
        }

        bool operator<(const ColumnWithSize & rhs) const
        {
            return std::tie(compressed_size, type_size, uncompressed_size)
                < std::tie(rhs.compressed_size, rhs.type_size, rhs.uncompressed_size);
        }

        NameAndTypePair column;
        size_t compressed_size = 0;
        size_t uncompressed_size = 0;
        size_t type_size = 0;
    };

    std::vector<ColumnWithSize> columns_with_sizes;

    auto column_sizes = storage->getColumnSizes();
    auto column_names_and_types = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns());

    if (!column_sizes.empty())
    {
        for (auto & column_name_and_type : column_names_and_types)
        {
            auto it = column_sizes.find(column_name_and_type.name);
            if (it == column_sizes.end())
                continue;

            columns_with_sizes.emplace_back(column_name_and_type, it->second);
        }
    }

    NameAndTypePair result;

    if (!columns_with_sizes.empty())
        result = std::min_element(columns_with_sizes.begin(), columns_with_sizes.end())->column;
    else
        /// If we have no information about columns sizes, choose a column of minimum size of its data type
        result = ExpressionActions::getSmallestColumn(column_names_and_types);

    return result;
}

JoinTreeQueryPlan buildQueryPlanForTableExpression(const QueryTreeNodePtr & table_expression,
    const SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    PlannerContextPtr & planner_context,
    bool is_single_table_expression)
{
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    QueryProcessingStage::Enum from_stage = QueryProcessingStage::Enum::FetchColumns;

    auto * table_node = table_expression->as<TableNode>();
    auto * table_function_node = table_expression->as<TableFunctionNode>();
    auto * query_node = table_expression->as<QueryNode>();
    auto * union_node = table_expression->as<UnionNode>();

    QueryPlan query_plan;

    auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);

    if (table_node || table_function_node)
    {
        const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        auto table_expression_query_info = select_query_info;
        table_expression_query_info.table_expression = table_expression;

        size_t max_streams = settings.max_threads;
        size_t max_threads_execute_query = settings.max_threads;

        /** With distributed query processing, almost no computations are done in the threads,
          * but wait and receive data from remote servers.
          * If we have 20 remote servers, and max_threads = 8, then it would not be efficient to
          * connect and ask only 8 servers at a time.
          * To simultaneously query more remote servers,
          * instead of max_threads, max_distributed_connections is used.
          */
        bool is_remote = table_expression_data.isRemote();
        if (is_remote)
        {
            max_streams = settings.max_distributed_connections;
            max_threads_execute_query = settings.max_distributed_connections;
        }

        UInt64 max_block_size = settings.max_block_size;

        auto & main_query_node = select_query_info.query_tree->as<QueryNode &>();

        if (is_single_table_expression)
        {
            size_t limit_length = 0;
            if (main_query_node.hasLimit())
            {
                /// Constness of limit is validated during query analysis stage
                limit_length = main_query_node.getLimit()->as<ConstantNode &>().getValue().safeGet<UInt64>();
            }

            size_t limit_offset = 0;
            if (main_query_node.hasOffset())
            {
                /// Constness of offset is validated during query analysis stage
                limit_offset = main_query_node.getOffset()->as<ConstantNode &>().getValue().safeGet<UInt64>();
            }

            /** If not specified DISTINCT, WHERE, GROUP BY, HAVING, ORDER BY, JOIN, LIMIT BY, LIMIT WITH TIES
              * but LIMIT is specified, and limit + offset < max_block_size,
              * then as the block size we will use limit + offset (not to read more from the table than requested),
              * and also set the number of threads to 1.
              */
            if (main_query_node.hasLimit() &&
                !main_query_node.isDistinct() &&
                !main_query_node.isLimitWithTies() &&
                !main_query_node.hasPrewhere() &&
                !main_query_node.hasWhere() &&
                select_query_info.filter_asts.empty() &&
                !main_query_node.hasGroupBy() &&
                !main_query_node.hasHaving() &&
                !main_query_node.hasOrderBy() &&
                !main_query_node.hasLimitBy() &&
                !select_query_info.need_aggregate &&
                !select_query_info.has_window &&
                limit_length <= std::numeric_limits<UInt64>::max() - limit_offset)
            {
                if (limit_length + limit_offset < max_block_size)
                {
                    max_block_size = std::max<UInt64>(1, limit_length + limit_offset);
                    max_streams = 1;
                    max_threads_execute_query = 1;
                }

                if (limit_length + limit_offset < select_query_info.local_storage_limits.local_limits.size_limits.max_rows)
                {
                    table_expression_query_info.limit = limit_length + limit_offset;
                }
            }

            if (!max_block_size)
                throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND,
                    "Setting 'max_block_size' cannot be zero");
        }

        if (max_streams == 0)
            max_streams = 1;

        /// If necessary, we request more sources than the number of threads - to distribute the work evenly over the threads
        if (max_streams > 1 && !is_remote)
            max_streams = static_cast<size_t>(max_streams * settings.max_streams_to_max_threads_ratio);

        if (table_node)
            table_expression_query_info.table_expression_modifiers = table_node->getTableExpressionModifiers();
        else
            table_expression_query_info.table_expression_modifiers = table_function_node->getTableExpressionModifiers();

        from_stage = storage->getQueryProcessingStage(query_context, select_query_options.to_stage, storage_snapshot, table_expression_query_info);

        Names columns_names = table_expression_data.getColumnNames();

        /** The current user must have the SELECT privilege.
          * We do not check access rights for table functions because they have been already checked in ITableFunction::execute().
          */
        if (table_node)
        {
            auto column_names_with_aliases = columns_names;
            const auto & alias_columns_names = table_expression_data.getAliasColumnsNames();
            column_names_with_aliases.insert(column_names_with_aliases.end(), alias_columns_names.begin(), alias_columns_names.end());
            checkAccessRights(*table_node, column_names_with_aliases, planner_context->getQueryContext());
        }

        /// Limitation on the number of columns to read
        if (settings.max_columns_to_read && columns_names.size() > settings.max_columns_to_read)
            throw Exception(ErrorCodes::TOO_MANY_COLUMNS,
                "Limit for number of columns to read exceeded. Requested: {}, maximum: {}",
                columns_names.size(),
                settings.max_columns_to_read);

        if (columns_names.empty())
        {
            auto additional_column_to_read = chooseSmallestColumnToReadFromStorage(storage, storage_snapshot);
            const auto & column_identifier = planner_context->getGlobalPlannerContext()->createColumnIdentifier(additional_column_to_read, table_expression);
            columns_names.push_back(additional_column_to_read.name);
            table_expression_data.addColumn(additional_column_to_read, column_identifier);
        }

        bool need_rewrite_query_with_final = storage->needRewriteQueryWithFinal(columns_names);
        if (need_rewrite_query_with_final)
        {
            if (table_expression_query_info.table_expression_modifiers)
            {
                const auto & table_expression_modifiers = table_expression_query_info.table_expression_modifiers;
                auto sample_size_ratio = table_expression_modifiers->getSampleSizeRatio();
                auto sample_offset_ratio = table_expression_modifiers->getSampleOffsetRatio();

                table_expression_query_info.table_expression_modifiers = TableExpressionModifiers(true /*has_final*/,
                    sample_size_ratio,
                    sample_offset_ratio);
            }
            else
            {
                table_expression_query_info.table_expression_modifiers = TableExpressionModifiers(true /*has_final*/,
                    {} /*sample_size_ratio*/,
                    {} /*sample_offset_ratio*/);
            }
        }

        storage->read(query_plan, columns_names, storage_snapshot, table_expression_query_info, query_context, from_stage, max_block_size, max_streams);

        if (query_plan.isInitialized())
        {
            /** Specify the number of threads only if it wasn't specified in storage.
              *
              * But in case of remote query and prefer_localhost_replica=1 (default)
              * The inner local query (that is done in the same process, without
              * network interaction), it will setMaxThreads earlier and distributed
              * query will not update it.
              */
            if (!query_plan.getMaxThreads() || is_remote)
                query_plan.setMaxThreads(max_threads_execute_query);
        }
        else
        {
            /// Create step which reads from empty source if storage has no data.
            auto source_header = storage_snapshot->getSampleBlockForColumns(columns_names);
            Pipe pipe(std::make_shared<NullSource>(source_header));
            auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
            read_from_pipe->setStepDescription("Read from NullSource");
            query_plan.addStep(std::move(read_from_pipe));
        }
    }
    else if (query_node || union_node)
    {
        auto subquery_options = select_query_options.subquery();
        Planner subquery_planner(table_expression, subquery_options, planner_context->getGlobalPlannerContext());
        /// Propagate storage limits to subquery
        subquery_planner.addStorageLimits(*select_query_info.storage_limits);
        subquery_planner.buildQueryPlanIfNeeded();
        query_plan = std::move(subquery_planner).extractQueryPlan();
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected table, table function, query or union. Actual {}",
                        table_expression->formatASTForErrorMessage());
    }

    if (from_stage == QueryProcessingStage::FetchColumns)
    {
        auto rename_actions_dag = std::make_shared<ActionsDAG>(query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());
        ActionsDAG::NodeRawConstPtrs updated_actions_dag_outputs;

        for (auto & output_node : rename_actions_dag->getOutputs())
        {
            const auto * column_identifier = table_expression_data.getColumnIdentifierOrNull(output_node->result_name);
            if (!column_identifier)
                continue;

            updated_actions_dag_outputs.push_back(&rename_actions_dag->addAlias(*output_node, *column_identifier));
        }

        rename_actions_dag->getOutputs() = std::move(updated_actions_dag_outputs);

        auto rename_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), rename_actions_dag);
        rename_step->setStepDescription("Change column names to column identifiers");
        query_plan.addStep(std::move(rename_step));
    }
    else
    {
        Planner planner(select_query_info.query_tree,
            SelectQueryOptions(from_stage).analyze(),
            select_query_info.planner_context);
        planner.buildQueryPlanIfNeeded();

        auto expected_header = planner.getQueryPlan().getCurrentDataStream().header;
        materializeBlockInplace(expected_header);

        auto rename_actions_dag = ActionsDAG::makeConvertingActions(
            query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
            expected_header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            true /*ignore_constant_values*/);
        auto rename_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), std::move(rename_actions_dag));
        std::string step_description = table_expression_data.isRemote() ? "Change remote column names to local column names" : "Change column names";
        rename_step->setStepDescription(std::move(step_description));
        query_plan.addStep(std::move(rename_step));
    }

    return {std::move(query_plan), from_stage};
}

JoinTreeQueryPlan buildQueryPlanForJoinNode(const QueryTreeNodePtr & join_table_expression,
    JoinTreeQueryPlan left_join_tree_query_plan,
    JoinTreeQueryPlan right_join_tree_query_plan,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context)
{
    auto & join_node = join_table_expression->as<JoinNode &>();
    if (left_join_tree_query_plan.from_stage != QueryProcessingStage::FetchColumns)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "JOIN {} left table expression expected to process query to fetch columns stage. Actual {}",
            join_node.formatASTForErrorMessage(),
            QueryProcessingStage::toString(left_join_tree_query_plan.from_stage));

    auto left_plan = std::move(left_join_tree_query_plan.query_plan);
    auto left_plan_output_columns = left_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();
    if (right_join_tree_query_plan.from_stage != QueryProcessingStage::FetchColumns)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "JOIN {} right table expression expected to process query to fetch columns stage. Actual {}",
            join_node.formatASTForErrorMessage(),
            QueryProcessingStage::toString(right_join_tree_query_plan.from_stage));

    auto right_plan = std::move(right_join_tree_query_plan.query_plan);
    auto right_plan_output_columns = right_plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

    JoinClausesAndActions join_clauses_and_actions;
    JoinKind join_kind = join_node.getKind();

    std::optional<bool> join_constant;

    if (join_node.getStrictness() == JoinStrictness::All)
        join_constant = tryExtractConstantFromJoinNode(join_table_expression);

    if (join_constant)
    {
        /** If there is JOIN with always true constant, we transform it to cross.
          * If there is JOIN with always false constant, we do not process JOIN keys.
          * It is expected by join algorithm to handle such case.
          *
          * Example: SELECT * FROM test_table AS t1 INNER JOIN test_table AS t2 ON 1;
          */
        if (*join_constant)
            join_kind = JoinKind::Cross;
    }
    else if (join_node.isOnJoinExpression())
    {
        join_clauses_and_actions = buildJoinClausesAndActions(left_plan_output_columns,
            right_plan_output_columns,
            join_table_expression,
            planner_context);

        join_clauses_and_actions.left_join_expressions_actions->projectInput();
        auto left_join_expressions_actions_step = std::make_unique<ExpressionStep>(left_plan.getCurrentDataStream(), join_clauses_and_actions.left_join_expressions_actions);
        left_join_expressions_actions_step->setStepDescription("JOIN actions");
        left_plan.addStep(std::move(left_join_expressions_actions_step));

        join_clauses_and_actions.right_join_expressions_actions->projectInput();
        auto right_join_expressions_actions_step = std::make_unique<ExpressionStep>(right_plan.getCurrentDataStream(), join_clauses_and_actions.right_join_expressions_actions);
        right_join_expressions_actions_step->setStepDescription("JOIN actions");
        right_plan.addStep(std::move(right_join_expressions_actions_step));
    }

    std::unordered_map<ColumnIdentifier, DataTypePtr> left_plan_column_name_to_cast_type;
    std::unordered_map<ColumnIdentifier, DataTypePtr> right_plan_column_name_to_cast_type;

    if (join_node.isUsingJoinExpression())
    {
        auto & join_node_using_columns_list = join_node.getJoinExpression()->as<ListNode &>();
        for (auto & join_node_using_node : join_node_using_columns_list.getNodes())
        {
            auto & join_node_using_column_node = join_node_using_node->as<ColumnNode &>();
            auto & inner_columns_list = join_node_using_column_node.getExpressionOrThrow()->as<ListNode &>();

            auto & left_inner_column_node = inner_columns_list.getNodes().at(0);
            auto & left_inner_column = left_inner_column_node->as<ColumnNode &>();

            auto & right_inner_column_node = inner_columns_list.getNodes().at(1);
            auto & right_inner_column = right_inner_column_node->as<ColumnNode &>();

            const auto & join_node_using_column_node_type = join_node_using_column_node.getColumnType();
            if (!left_inner_column.getColumnType()->equals(*join_node_using_column_node_type))
            {
                const auto & left_inner_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(left_inner_column_node);
                left_plan_column_name_to_cast_type.emplace(left_inner_column_identifier, join_node_using_column_node_type);
            }

            if (!right_inner_column.getColumnType()->equals(*join_node_using_column_node_type))
            {
                const auto & right_inner_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(right_inner_column_node);
                right_plan_column_name_to_cast_type.emplace(right_inner_column_identifier, join_node_using_column_node_type);
            }
        }
    }

    auto join_cast_plan_output_nodes = [&](QueryPlan & plan_to_add_cast, std::unordered_map<std::string, DataTypePtr> & plan_column_name_to_cast_type)
    {
        auto cast_actions_dag = std::make_shared<ActionsDAG>(plan_to_add_cast.getCurrentDataStream().header.getColumnsWithTypeAndName());

        for (auto & output_node : cast_actions_dag->getOutputs())
        {
            auto it = plan_column_name_to_cast_type.find(output_node->result_name);
            if (it == plan_column_name_to_cast_type.end())
                continue;

            const auto & cast_type = it->second;
            output_node = &cast_actions_dag->addCast(*output_node, cast_type, output_node->result_name);
        }

        cast_actions_dag->projectInput();
        auto cast_join_columns_step
            = std::make_unique<ExpressionStep>(plan_to_add_cast.getCurrentDataStream(), std::move(cast_actions_dag));
        cast_join_columns_step->setStepDescription("Cast JOIN USING columns");
        plan_to_add_cast.addStep(std::move(cast_join_columns_step));
    };

    if (!left_plan_column_name_to_cast_type.empty())
        join_cast_plan_output_nodes(left_plan, left_plan_column_name_to_cast_type);

    if (!right_plan_column_name_to_cast_type.empty())
        join_cast_plan_output_nodes(right_plan, right_plan_column_name_to_cast_type);

    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    bool join_use_nulls = settings.join_use_nulls;
    auto to_nullable_function = FunctionFactory::instance().get("toNullable", query_context);

    auto join_cast_plan_columns_to_nullable = [&](QueryPlan & plan_to_add_cast)
    {
        auto cast_actions_dag = std::make_shared<ActionsDAG>(plan_to_add_cast.getCurrentDataStream().header.getColumnsWithTypeAndName());

        for (auto & output_node : cast_actions_dag->getOutputs())
        {
            if (planner_context->getGlobalPlannerContext()->hasColumnIdentifier(output_node->result_name))
                output_node = &cast_actions_dag->addFunction(to_nullable_function, {output_node}, output_node->result_name);
        }

        cast_actions_dag->projectInput();
        auto cast_join_columns_step = std::make_unique<ExpressionStep>(plan_to_add_cast.getCurrentDataStream(), std::move(cast_actions_dag));
        cast_join_columns_step->setStepDescription("Cast JOIN columns to Nullable");
        plan_to_add_cast.addStep(std::move(cast_join_columns_step));
    };

    if (join_use_nulls)
    {
        if (isFull(join_kind))
        {
            join_cast_plan_columns_to_nullable(left_plan);
            join_cast_plan_columns_to_nullable(right_plan);
        }
        else if (isLeft(join_kind))
        {
            join_cast_plan_columns_to_nullable(right_plan);
        }
        else if (isRight(join_kind))
        {
            join_cast_plan_columns_to_nullable(left_plan);
        }
    }

    auto table_join = std::make_shared<TableJoin>(settings, query_context->getTemporaryVolume());
    table_join->getTableJoin() = join_node.toASTTableJoin()->as<ASTTableJoin &>();
    table_join->getTableJoin().kind = join_kind;

    if (join_kind == JoinKind::Comma)
    {
        join_kind = JoinKind::Cross;
        table_join->getTableJoin().kind = JoinKind::Cross;
    }

    table_join->setIsJoinWithConstant(join_constant != std::nullopt);

    if (join_node.isOnJoinExpression())
    {
        const auto & join_clauses = join_clauses_and_actions.join_clauses;
        bool is_asof = table_join->strictness() == JoinStrictness::Asof;

        if (join_clauses.size() > 1)
        {
            if (is_asof)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "ASOF join {} doesn't support multiple ORs for keys in JOIN ON section",
                    join_node.formatASTForErrorMessage());
        }

        auto & table_join_clauses = table_join->getClauses();

        for (const auto & join_clause : join_clauses)
        {
            table_join_clauses.emplace_back();
            auto & table_join_clause = table_join_clauses.back();

            const auto & join_clause_left_key_nodes = join_clause.getLeftKeyNodes();
            const auto & join_clause_right_key_nodes = join_clause.getRightKeyNodes();

            size_t join_clause_key_nodes_size = join_clause_left_key_nodes.size();
            assert(join_clause_key_nodes_size == join_clause_right_key_nodes.size());

            for (size_t i = 0; i < join_clause_key_nodes_size; ++i)
            {
                table_join_clause.key_names_left.push_back(join_clause_left_key_nodes[i]->result_name);
                table_join_clause.key_names_right.push_back(join_clause_right_key_nodes[i]->result_name);
            }

            const auto & join_clause_get_left_filter_condition_nodes = join_clause.getLeftFilterConditionNodes();
            if (!join_clause_get_left_filter_condition_nodes.empty())
            {
                if (join_clause_get_left_filter_condition_nodes.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "JOIN {} left filter conditions size must be 1. Actual {}",
                        join_node.formatASTForErrorMessage(),
                        join_clause_get_left_filter_condition_nodes.size());

                const auto & join_clause_left_filter_condition_name = join_clause_get_left_filter_condition_nodes[0]->result_name;
                table_join_clause.analyzer_left_filter_condition_column_name = join_clause_left_filter_condition_name;
            }

            const auto & join_clause_get_right_filter_condition_nodes = join_clause.getRightFilterConditionNodes();
            if (!join_clause_get_right_filter_condition_nodes.empty())
            {
                if (join_clause_get_right_filter_condition_nodes.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "JOIN {} right filter conditions size must be 1. Actual {}",
                        join_node.formatASTForErrorMessage(),
                        join_clause_get_right_filter_condition_nodes.size());

                const auto & join_clause_right_filter_condition_name = join_clause_get_right_filter_condition_nodes[0]->result_name;
                table_join_clause.analyzer_right_filter_condition_column_name = join_clause_right_filter_condition_name;
            }

            if (is_asof)
            {
                if (!join_clause.hasASOF())
                    throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                        "JOIN {} no inequality in ASOF JOIN ON section.",
                        join_node.formatASTForErrorMessage());

                if (table_join_clause.key_names_left.size() <= 1)
                    throw Exception(ErrorCodes::SYNTAX_ERROR,
                        "JOIN {} ASOF join needs at least one equi-join column",
                        join_node.formatASTForErrorMessage());
            }

            if (join_clause.hasASOF())
            {
                const auto & asof_conditions = join_clause.getASOFConditions();
                assert(asof_conditions.size() == 1);

                const auto & asof_condition = asof_conditions[0];
                table_join->setAsofInequality(asof_condition.asof_inequality);

                /// Execution layer of JOIN algorithms expects that ASOF keys are last JOIN keys
                std::swap(table_join_clause.key_names_left.at(asof_condition.key_index), table_join_clause.key_names_left.back());
                std::swap(table_join_clause.key_names_right.at(asof_condition.key_index), table_join_clause.key_names_right.back());
            }
        }
    }
    else if (join_node.isUsingJoinExpression())
    {
        auto & table_join_clauses = table_join->getClauses();
        table_join_clauses.emplace_back();
        auto & table_join_clause = table_join_clauses.back();

        auto & using_list = join_node.getJoinExpression()->as<ListNode &>();

        for (auto & join_using_node : using_list.getNodes())
        {
            auto & join_using_column_node = join_using_node->as<ColumnNode &>();
            auto & using_join_columns_list = join_using_column_node.getExpressionOrThrow()->as<ListNode &>();
            auto & using_join_left_join_column_node = using_join_columns_list.getNodes().at(0);
            auto & using_join_right_join_column_node = using_join_columns_list.getNodes().at(1);

            const auto & left_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(using_join_left_join_column_node);
            const auto & right_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(using_join_right_join_column_node);

            table_join_clause.key_names_left.push_back(left_column_identifier);
            table_join_clause.key_names_right.push_back(right_column_identifier);
        }
    }

    const Block & left_header = left_plan.getCurrentDataStream().header;
    auto left_table_names = left_header.getNames();
    NameSet left_table_names_set(left_table_names.begin(), left_table_names.end());

    auto columns_from_joined_table = right_plan.getCurrentDataStream().header.getNamesAndTypesList();
    table_join->setColumnsFromJoinedTable(columns_from_joined_table, left_table_names_set, "");

    for (auto & column_from_joined_table : columns_from_joined_table)
    {
        /// Add columns from joined table only if they are presented in outer scope, otherwise they can be dropped
        if (planner_context->getGlobalPlannerContext()->hasColumnIdentifier(column_from_joined_table.name) &&
            outer_scope_columns.contains(column_from_joined_table.name))
            table_join->addJoinedColumn(column_from_joined_table);
    }

    const Block & right_header = right_plan.getCurrentDataStream().header;
    auto join_algorithm = chooseJoinAlgorithm(table_join, join_node.getRightTableExpression(), left_header, right_header, planner_context);

    auto result_plan = QueryPlan();

    if (join_algorithm->isFilled())
    {
        auto filled_join_step = std::make_unique<FilledJoinStep>(
            left_plan.getCurrentDataStream(),
            join_algorithm,
            settings.max_block_size);

        filled_join_step->setStepDescription("Filled JOIN");
        left_plan.addStep(std::move(filled_join_step));

        result_plan = std::move(left_plan);
    }
    else
    {
        auto add_sorting = [&] (QueryPlan & plan, const Names & key_names, JoinTableSide join_table_side)
        {
            SortDescription sort_description;
            sort_description.reserve(key_names.size());
            for (const auto & key_name : key_names)
                sort_description.emplace_back(key_name);

            SortingStep::Settings sort_settings(*query_context);

            auto sorting_step = std::make_unique<SortingStep>(
                plan.getCurrentDataStream(),
                std::move(sort_description),
                0 /*limit*/,
                sort_settings,
                settings.optimize_sorting_by_input_stream_properties);
            sorting_step->setStepDescription(fmt::format("Sort {} before JOIN", join_table_side));
            plan.addStep(std::move(sorting_step));
        };

        auto crosswise_connection = CreateSetAndFilterOnTheFlyStep::createCrossConnection();
        auto add_create_set = [&settings, crosswise_connection](QueryPlan & plan, const Names & key_names, JoinTableSide join_table_side)
        {
            auto creating_set_step = std::make_unique<CreateSetAndFilterOnTheFlyStep>(
                plan.getCurrentDataStream(),
                key_names,
                settings.max_rows_in_set_to_optimize_join,
                crosswise_connection,
                join_table_side);
            creating_set_step->setStepDescription(fmt::format("Create set and filter {} joined stream", join_table_side));

            auto * step_raw_ptr = creating_set_step.get();
            plan.addStep(std::move(creating_set_step));
            return step_raw_ptr;
        };

        if (join_algorithm->pipelineType() == JoinPipelineType::YShaped)
        {
            const auto & join_clause = table_join->getOnlyClause();

            bool kind_allows_filtering = isInner(join_kind) || isLeft(join_kind) || isRight(join_kind);
            if (settings.max_rows_in_set_to_optimize_join > 0 && kind_allows_filtering)
            {
                auto * left_set = add_create_set(left_plan, join_clause.key_names_left, JoinTableSide::Left);
                auto * right_set = add_create_set(right_plan, join_clause.key_names_right, JoinTableSide::Right);

                if (isInnerOrLeft(join_kind))
                    right_set->setFiltering(left_set->getSet());

                if (isInnerOrRight(join_kind))
                    left_set->setFiltering(right_set->getSet());
            }

            add_sorting(left_plan, join_clause.key_names_left, JoinTableSide::Left);
            add_sorting(right_plan, join_clause.key_names_right, JoinTableSide::Right);
        }

        auto join_pipeline_type = join_algorithm->pipelineType();
        auto join_step = std::make_unique<JoinStep>(
            left_plan.getCurrentDataStream(),
            right_plan.getCurrentDataStream(),
            std::move(join_algorithm),
            settings.max_block_size,
            settings.max_threads,
            false /*optimize_read_in_order*/);

        join_step->setStepDescription(fmt::format("JOIN {}", join_pipeline_type));

        std::vector<QueryPlanPtr> plans;
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(left_plan)));
        plans.emplace_back(std::make_unique<QueryPlan>(std::move(right_plan)));

        result_plan.unitePlans(std::move(join_step), {std::move(plans)});
    }

    auto drop_unused_columns_after_join_actions_dag = std::make_shared<ActionsDAG>(result_plan.getCurrentDataStream().header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs drop_unused_columns_after_join_actions_dag_updated_outputs;
    std::unordered_set<std::string_view> drop_unused_columns_after_join_actions_dag_updated_outputs_names;
    std::optional<size_t> first_skipped_column_node_index;

    auto & drop_unused_columns_after_join_actions_dag_outputs = drop_unused_columns_after_join_actions_dag->getOutputs();
    size_t drop_unused_columns_after_join_actions_dag_outputs_size = drop_unused_columns_after_join_actions_dag_outputs.size();

    for (size_t i = 0; i < drop_unused_columns_after_join_actions_dag_outputs_size; ++i)
    {
        const auto & output = drop_unused_columns_after_join_actions_dag_outputs[i];

        const auto & global_planner_context = planner_context->getGlobalPlannerContext();
        if (drop_unused_columns_after_join_actions_dag_updated_outputs_names.contains(output->result_name)
            || !global_planner_context->hasColumnIdentifier(output->result_name))
            continue;

        if (!outer_scope_columns.contains(output->result_name))
        {
            if (!first_skipped_column_node_index)
                first_skipped_column_node_index = i;
            continue;
        }

        drop_unused_columns_after_join_actions_dag_updated_outputs.push_back(output);
        drop_unused_columns_after_join_actions_dag_updated_outputs_names.insert(output->result_name);
    }

    /** It is expected that JOIN TREE query plan will contain at least 1 column, even if there are no columns in outer scope.
      *
      * Example: SELECT count() FROM test_table_1 AS t1, test_table_2 AS t2;
      */
    if (drop_unused_columns_after_join_actions_dag_updated_outputs.empty() && first_skipped_column_node_index)
        drop_unused_columns_after_join_actions_dag_updated_outputs.push_back(drop_unused_columns_after_join_actions_dag_outputs[*first_skipped_column_node_index]);

    drop_unused_columns_after_join_actions_dag_outputs = std::move(drop_unused_columns_after_join_actions_dag_updated_outputs);

    auto drop_unused_columns_after_join_transform_step = std::make_unique<ExpressionStep>(result_plan.getCurrentDataStream(), std::move(drop_unused_columns_after_join_actions_dag));
    drop_unused_columns_after_join_transform_step->setStepDescription("DROP unused columns after JOIN");
    result_plan.addStep(std::move(drop_unused_columns_after_join_transform_step));

    return {std::move(result_plan), QueryProcessingStage::FetchColumns};
}

JoinTreeQueryPlan buildQueryPlanForArrayJoinNode(const QueryTreeNodePtr & array_join_table_expression,
    JoinTreeQueryPlan join_tree_query_plan,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context)
{
    auto & array_join_node = array_join_table_expression->as<ArrayJoinNode &>();
    if (join_tree_query_plan.from_stage != QueryProcessingStage::FetchColumns)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "ARRAY JOIN {} table expression expected to process query to fetch columns stage. Actual {}",
            array_join_node.formatASTForErrorMessage(),
            QueryProcessingStage::toString(join_tree_query_plan.from_stage));

    auto plan = std::move(join_tree_query_plan.query_plan);
    auto plan_output_columns = plan.getCurrentDataStream().header.getColumnsWithTypeAndName();

    ActionsDAGPtr array_join_action_dag = std::make_shared<ActionsDAG>(plan_output_columns);
    PlannerActionsVisitor actions_visitor(planner_context);
    std::unordered_set<std::string> array_join_expressions_output_nodes;

    NameSet array_join_column_names;
    for (auto & array_join_expression : array_join_node.getJoinExpressions().getNodes())
    {
        const auto & array_join_column_identifier = planner_context->getColumnNodeIdentifierOrThrow(array_join_expression);
        array_join_column_names.insert(array_join_column_identifier);

        auto & array_join_expression_column = array_join_expression->as<ColumnNode &>();
        auto expression_dag_index_nodes = actions_visitor.visit(array_join_action_dag, array_join_expression_column.getExpressionOrThrow());

        for (auto & expression_dag_index_node : expression_dag_index_nodes)
        {
            const auto * array_join_column_node = &array_join_action_dag->addAlias(*expression_dag_index_node, array_join_column_identifier);
            array_join_action_dag->getOutputs().push_back(array_join_column_node);
            array_join_expressions_output_nodes.insert(array_join_column_node->result_name);
        }
    }

    array_join_action_dag->projectInput();
    auto array_join_actions = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(), array_join_action_dag);
    array_join_actions->setStepDescription("ARRAY JOIN actions");
    plan.addStep(std::move(array_join_actions));

    auto drop_unused_columns_before_array_join_actions_dag = std::make_shared<ActionsDAG>(plan.getCurrentDataStream().header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs drop_unused_columns_before_array_join_actions_dag_updated_outputs;
    std::unordered_set<std::string_view> drop_unused_columns_before_array_join_actions_dag_updated_outputs_names;

    auto & drop_unused_columns_before_array_join_actions_dag_outputs = drop_unused_columns_before_array_join_actions_dag->getOutputs();
    size_t drop_unused_columns_before_array_join_actions_dag_outputs_size = drop_unused_columns_before_array_join_actions_dag_outputs.size();

    for (size_t i = 0; i < drop_unused_columns_before_array_join_actions_dag_outputs_size; ++i)
    {
        const auto & output = drop_unused_columns_before_array_join_actions_dag_outputs[i];

        if (drop_unused_columns_before_array_join_actions_dag_updated_outputs_names.contains(output->result_name))
            continue;

        if (!array_join_expressions_output_nodes.contains(output->result_name) &&
            !outer_scope_columns.contains(output->result_name))
            continue;

        drop_unused_columns_before_array_join_actions_dag_updated_outputs.push_back(output);
        drop_unused_columns_before_array_join_actions_dag_updated_outputs_names.insert(output->result_name);
    }

    drop_unused_columns_before_array_join_actions_dag_outputs = std::move(drop_unused_columns_before_array_join_actions_dag_updated_outputs);

    auto drop_unused_columns_before_array_join_transform_step = std::make_unique<ExpressionStep>(plan.getCurrentDataStream(),
        std::move(drop_unused_columns_before_array_join_actions_dag));
    drop_unused_columns_before_array_join_transform_step->setStepDescription("DROP unused columns before ARRAY JOIN");
    plan.addStep(std::move(drop_unused_columns_before_array_join_transform_step));

    auto array_join_action = std::make_shared<ArrayJoinAction>(array_join_column_names, array_join_node.isLeft(), planner_context->getQueryContext());
    auto array_join_step = std::make_unique<ArrayJoinStep>(plan.getCurrentDataStream(), std::move(array_join_action));
    array_join_step->setStepDescription("ARRAY JOIN");
    plan.addStep(std::move(array_join_step));

    return {std::move(plan), QueryProcessingStage::FetchColumns};
}

}

JoinTreeQueryPlan buildJoinTreeQueryPlan(const QueryTreeNodePtr & query_node,
    const SelectQueryInfo & select_query_info,
    const SelectQueryOptions & select_query_options,
    const ColumnIdentifierSet & outer_scope_columns,
    PlannerContextPtr & planner_context)
{
    const auto & query_node_typed = query_node->as<QueryNode &>();
    auto table_expressions_stack = buildTableExpressionsStack(query_node_typed.getJoinTree());
    size_t table_expressions_stack_size = table_expressions_stack.size();
    bool is_single_table_expression = table_expressions_stack_size == 1;

    std::vector<ColumnIdentifierSet> table_expressions_outer_scope_columns(table_expressions_stack_size);
    ColumnIdentifierSet current_outer_scope_columns = outer_scope_columns;

    for (Int64 i = static_cast<Int64>(table_expressions_stack_size) - 1; i >= 0; --i)
    {
        table_expressions_outer_scope_columns[i] = current_outer_scope_columns;
        auto & table_expression = table_expressions_stack[i];
        auto table_expression_type = table_expression->getNodeType();

        if (table_expression_type == QueryTreeNodeType::JOIN)
            collectTopLevelColumnIdentifiers(table_expression, planner_context, current_outer_scope_columns);
        else if (table_expression_type == QueryTreeNodeType::ARRAY_JOIN)
            collectTopLevelColumnIdentifiers(table_expression, planner_context, current_outer_scope_columns);
    }

    std::vector<JoinTreeQueryPlan> query_plans_stack;

    for (size_t i = 0; i < table_expressions_stack_size; ++i)
    {
        const auto & table_expression = table_expressions_stack[i];
        auto table_expression_node_type = table_expression->getNodeType();

        if (table_expression_node_type == QueryTreeNodeType::ARRAY_JOIN)
        {
            if (query_plans_stack.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected at least 1 query plan on stack before ARRAY JOIN processing. Actual {}",
                    query_plans_stack.size());

            auto query_plan = std::move(query_plans_stack.back());
            query_plans_stack.back() = buildQueryPlanForArrayJoinNode(table_expression,
                std::move(query_plan),
                table_expressions_outer_scope_columns[i],
                planner_context);
        }
        else if (table_expression_node_type == QueryTreeNodeType::JOIN)
        {
            if (query_plans_stack.size() < 2)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected at least 2 query plans on stack before JOIN processing. Actual {}",
                    query_plans_stack.size());

            auto right_query_plan = std::move(query_plans_stack.back());
            query_plans_stack.pop_back();

            auto left_query_plan = std::move(query_plans_stack.back());
            query_plans_stack.pop_back();

            query_plans_stack.push_back(buildQueryPlanForJoinNode(table_expression,
                std::move(left_query_plan),
                std::move(right_query_plan),
                table_expressions_outer_scope_columns[i],
                planner_context));
        }
        else
        {
            const auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);
            if (table_expression_data.isRemote() && i != 0)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "JOIN with multiple remote storages is unsupported");

            query_plans_stack.push_back(buildQueryPlanForTableExpression(table_expression,
                select_query_info,
                select_query_options,
                planner_context,
                is_single_table_expression));

            if (query_plans_stack.back().from_stage != QueryProcessingStage::FetchColumns)
                break;
        }
    }

    if (query_plans_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Expected 1 query plan for JOIN TREE. Actual {}",
            query_plans_stack.size());

    return std::move(query_plans_stack.back());
}

}
