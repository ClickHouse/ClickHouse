#include <Common/FieldVisitorToString.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeObjectDeprecated.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/getLeastSupertype.h>

#include <Functions/FunctionFactory.h>
#include <Functions/UserDefined/UserDefinedExecutableFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/grouping.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <Formats/FormatFactory.h>

#include <Storages/IStorage.h>
#include <Storages/StorageJoin.h>

#include <Interpreters/misc.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>

#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <Analyzer/Utils.h>
#include <Analyzer/SetUtils.h>
#include <Analyzer/AggregationUtils.h>
#include <Analyzer/WindowFunctionsUtils.h>
#include <Analyzer/ValidationUtils.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/IdentifierNode.h>
#include <Analyzer/MatcherNode.h>
#include <Analyzer/ColumnTransformers.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/LambdaNode.h>
#include <Analyzer/SortNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/WindowNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Identifier.h>
#include <Analyzer/FunctionSecretArgumentsFinderTreeNode.h>
#include <Analyzer/RecursiveCTE.h>

#include <Analyzer/Resolve/QueryAnalyzer.h>
#include <Analyzer/Resolve/QueryExpressionsAliasVisitor.h>
#include <Analyzer/Resolve/IdentifierResolveScope.h>
#include <Analyzer/Resolve/TableExpressionsAliasVisitor.h>
#include <Analyzer/Resolve/ReplaceColumnsVisitor.h>

#include <Planner/PlannerActionsVisitor.h>

#include <Core/Settings.h>

namespace ProfileEvents
{
    extern const Event ScalarSubqueriesGlobalCacheHit;
    extern const Event ScalarSubqueriesLocalCacheHit;
    extern const Event ScalarSubqueriesCacheMiss;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool aggregate_functions_null_for_empty;
    extern const SettingsBool analyzer_compatibility_join_using_top_level_identifier;
    extern const SettingsBool asterisk_include_alias_columns;
    extern const SettingsBool asterisk_include_materialized_columns;
    extern const SettingsString count_distinct_implementation;
    extern const SettingsBool enable_global_with_statement;
    extern const SettingsBool enable_order_by_all;
    extern const SettingsBool enable_positional_arguments;
    extern const SettingsBool enable_scalar_subquery_optimization;
    extern const SettingsBool extremes;
    extern const SettingsBool force_grouping_standard_compatibility;
    extern const SettingsBool format_display_secrets_in_show_and_select;
    extern const SettingsBool joined_subquery_requires_alias;
    extern const SettingsUInt64 max_bytes_in_set;
    extern const SettingsUInt64 max_expanded_ast_elements;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsUInt64 max_rows_in_set;
    extern const SettingsUInt64 max_subquery_depth;
    extern const SettingsBool prefer_column_name_to_alias;
    extern const SettingsBool rewrite_count_distinct_if_with_count_distinct_implementation;
    extern const SettingsOverflowMode set_overflow_mode;
    extern const SettingsBool single_join_prefer_left_table;
    extern const SettingsBool transform_null_in;
    extern const SettingsUInt64 use_structure_from_insertion_table_in_table_functions;
}


namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_FUNCTION;
    extern const int LOGICAL_ERROR;
    extern const int CYCLIC_ALIASES;
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
    extern const int TYPE_MISMATCH;
    extern const int AMBIGUOUS_IDENTIFIER;
    extern const int INVALID_WITH_FILL_EXPRESSION;
    extern const int INVALID_LIMIT_EXPRESSION;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int TOO_DEEP_SUBQUERIES;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_FINAL;
    extern const int SAMPLING_NOT_SUPPORTED;
    extern const int NO_COMMON_TYPE;
    extern const int NOT_IMPLEMENTED;
    extern const int ALIAS_REQUIRED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_TABLE;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int SYNTAX_ERROR;
    extern const int UNEXPECTED_EXPRESSION;
    extern const int INVALID_IDENTIFIER;
}

QueryAnalyzer::QueryAnalyzer(bool only_analyze_)
    : identifier_resolver(ctes_in_resolve_process, node_to_projection_name)
    , only_analyze(only_analyze_)
{}

QueryAnalyzer::~QueryAnalyzer() = default;

void QueryAnalyzer::resolve(QueryTreeNodePtr & node, const QueryTreeNodePtr & table_expression, ContextPtr context)
{
    IdentifierResolveScope scope(node, nullptr /*parent_scope*/);

    if (!scope.context)
        scope.context = context;

    auto node_type = node->getNodeType();

    switch (node_type)
    {
        case QueryTreeNodeType::QUERY:
        {
            if (table_expression)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "For query analysis table expression must be empty");

            resolveQuery(node, scope);
            break;
        }
        case QueryTreeNodeType::UNION:
        {
            if (table_expression)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "For union analysis table expression must be empty");

            resolveUnion(node, scope);
            break;
        }
        case QueryTreeNodeType::IDENTIFIER:
            [[fallthrough]];
        case QueryTreeNodeType::CONSTANT:
            [[fallthrough]];
        case QueryTreeNodeType::COLUMN:
            [[fallthrough]];
        case QueryTreeNodeType::FUNCTION:
            [[fallthrough]];
        case QueryTreeNodeType::LIST:
        {
            if (table_expression)
            {
                scope.expression_join_tree_node = table_expression;
                validateTableExpressionModifiers(scope.expression_join_tree_node, scope);
                initializeTableExpressionData(scope.expression_join_tree_node, scope);
            }

            if (node_type == QueryTreeNodeType::LIST)
                resolveExpressionNodeList(node, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
            else
                resolveExpressionNode(node, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

            break;
        }
        case QueryTreeNodeType::TABLE_FUNCTION:
        {
            QueryExpressionsAliasVisitor expressions_alias_visitor(scope.aliases);
            resolveTableFunction(node, scope, expressions_alias_visitor, false /*nested_table_function*/);
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Node {} with type {} is not supported by query analyzer. "
                            "Supported nodes are query, union, identifier, constant, column, function, list.",
                            node->formatASTForErrorMessage(),
                            node->getNodeTypeName());
        }
    }
}

std::optional<JoinTableSide> QueryAnalyzer::getColumnSideFromJoinTree(const QueryTreeNodePtr & resolved_identifier, const JoinNode & join_node)
{
    if (resolved_identifier->getNodeType() == QueryTreeNodeType::CONSTANT)
        return {};

    if (resolved_identifier->getNodeType() == QueryTreeNodeType::FUNCTION)
    {
        const auto & resolved_function = resolved_identifier->as<FunctionNode &>();

        const auto & argument_nodes = resolved_function.getArguments().getNodes();

        std::optional<JoinTableSide> result;
        for (const auto & argument_node : argument_nodes)
        {
            auto table_side = getColumnSideFromJoinTree(argument_node, join_node);
            if (table_side && result && *table_side != *result)
            {
                throw Exception(ErrorCodes::AMBIGUOUS_IDENTIFIER,
                    "Ambiguous identifier {}. In scope {}",
                    resolved_identifier->formatASTForErrorMessage(),
                    join_node.formatASTForErrorMessage());
            }
            result = table_side;
        }
        return result;
    }

    const auto * column_src = resolved_identifier->as<ColumnNode &>().getColumnSource().get();

    if (join_node.getLeftTableExpression().get() == column_src)
        return JoinTableSide::Left;
    if (join_node.getRightTableExpression().get() == column_src)
        return JoinTableSide::Right;
    return {};
}

ProjectionName QueryAnalyzer::calculateFunctionProjectionName(const QueryTreeNodePtr & function_node, const ProjectionNames & parameters_projection_names,
    const ProjectionNames & arguments_projection_names)
{
    const auto & function_node_typed = function_node->as<FunctionNode &>();
    const auto & function_node_name = function_node_typed.getFunctionName();

    bool is_array_function = function_node_name == "array";
    bool is_tuple_function = function_node_name == "tuple";

    WriteBufferFromOwnString buffer;

    if (!is_array_function && !is_tuple_function)
        buffer << function_node_name;

    if (!parameters_projection_names.empty())
    {
        buffer << '(';

        size_t function_parameters_projection_names_size = parameters_projection_names.size();
        for (size_t i = 0; i < function_parameters_projection_names_size; ++i)
        {
            buffer << parameters_projection_names[i];

            if (i + 1 != function_parameters_projection_names_size)
                buffer << ", ";
        }

        buffer << ')';
    }

    char open_bracket = '(';
    char close_bracket = ')';

    if (is_array_function)
    {
        open_bracket = '[';
        close_bracket = ']';
    }

    buffer << open_bracket;

    size_t function_arguments_projection_names_size = arguments_projection_names.size();
    for (size_t i = 0; i < function_arguments_projection_names_size; ++i)
    {
        buffer << arguments_projection_names[i];

        if (i + 1 != function_arguments_projection_names_size)
            buffer << ", ";
    }

    buffer << close_bracket;

    return buffer.str();
}

ProjectionName QueryAnalyzer::calculateWindowProjectionName(const QueryTreeNodePtr & window_node,
    const QueryTreeNodePtr & parent_window_node,
    const String & parent_window_name,
    const ProjectionNames & partition_by_projection_names,
    const ProjectionNames & order_by_projection_names,
    const ProjectionName & frame_begin_offset_projection_name,
    const ProjectionName & frame_end_offset_projection_name)
{
    const auto & window_node_typed = window_node->as<WindowNode &>();
    const auto & window_frame = window_node_typed.getWindowFrame();

    bool parent_window_node_has_partition_by = false;
    bool parent_window_node_has_order_by = false;

    if (parent_window_node)
    {
        const auto & parent_window_node_typed = parent_window_node->as<WindowNode &>();
        parent_window_node_has_partition_by = parent_window_node_typed.hasPartitionBy();
        parent_window_node_has_order_by = parent_window_node_typed.hasOrderBy();
    }

    WriteBufferFromOwnString buffer;

    if (!parent_window_name.empty())
        buffer << parent_window_name;

    if (!partition_by_projection_names.empty() && !parent_window_node_has_partition_by)
    {
        if (!parent_window_name.empty())
            buffer << ' ';

        buffer << "PARTITION BY ";

        size_t partition_by_projection_names_size = partition_by_projection_names.size();
        for (size_t i = 0; i < partition_by_projection_names_size; ++i)
        {
            buffer << partition_by_projection_names[i];
            if (i + 1 != partition_by_projection_names_size)
                buffer << ", ";
        }
    }

    if (!order_by_projection_names.empty() && !parent_window_node_has_order_by)
    {
        if (!partition_by_projection_names.empty() || !parent_window_name.empty())
            buffer << ' ';

        buffer << "ORDER BY ";

        size_t order_by_projection_names_size = order_by_projection_names.size();
        for (size_t i = 0; i < order_by_projection_names_size; ++i)
        {
            buffer << order_by_projection_names[i];
            if (i + 1 != order_by_projection_names_size)
                buffer << ", ";
        }
    }

    if (!window_frame.is_default)
    {
        if (!partition_by_projection_names.empty() || !order_by_projection_names.empty() || !parent_window_name.empty())
            buffer << ' ';

        buffer << window_frame.type << " BETWEEN ";
        if (window_frame.begin_type == WindowFrame::BoundaryType::Current)
        {
            buffer << "CURRENT ROW";
        }
        else if (window_frame.begin_type == WindowFrame::BoundaryType::Unbounded)
        {
            buffer << "UNBOUNDED";
            buffer << " " << (window_frame.begin_preceding ? "PRECEDING" : "FOLLOWING");
        }
        else
        {
            buffer << frame_begin_offset_projection_name;
            buffer << " " << (window_frame.begin_preceding ? "PRECEDING" : "FOLLOWING");
        }

        buffer << " AND ";

        if (window_frame.end_type == WindowFrame::BoundaryType::Current)
        {
            buffer << "CURRENT ROW";
        }
        else if (window_frame.end_type == WindowFrame::BoundaryType::Unbounded)
        {
            buffer << "UNBOUNDED";
            buffer << " " << (window_frame.end_preceding ? "PRECEDING" : "FOLLOWING");
        }
        else
        {
            buffer << frame_end_offset_projection_name;
            buffer << " " << (window_frame.end_preceding ? "PRECEDING" : "FOLLOWING");
        }
    }

    return buffer.str();
}

ProjectionName QueryAnalyzer::calculateSortColumnProjectionName(const QueryTreeNodePtr & sort_column_node, const ProjectionName & sort_expression_projection_name,
    const ProjectionName & fill_from_expression_projection_name, const ProjectionName & fill_to_expression_projection_name, const ProjectionName & fill_step_expression_projection_name)
{
    auto & sort_node_typed = sort_column_node->as<SortNode &>();

    WriteBufferFromOwnString sort_column_projection_name_buffer;
    sort_column_projection_name_buffer << sort_expression_projection_name;

    auto sort_direction = sort_node_typed.getSortDirection();
    sort_column_projection_name_buffer << (sort_direction == SortDirection::ASCENDING ? " ASC" : " DESC");

    auto nulls_sort_direction = sort_node_typed.getNullsSortDirection();

    if (nulls_sort_direction)
        sort_column_projection_name_buffer << " NULLS " << (nulls_sort_direction == sort_direction ? "LAST" : "FIRST");

    if (auto collator = sort_node_typed.getCollator())
        sort_column_projection_name_buffer << " COLLATE " << collator->getLocale();

    if (sort_node_typed.withFill())
    {
        sort_column_projection_name_buffer << " WITH FILL";

        if (sort_node_typed.hasFillFrom())
            sort_column_projection_name_buffer << " FROM " << fill_from_expression_projection_name;

        if (sort_node_typed.hasFillTo())
            sort_column_projection_name_buffer << " TO " << fill_to_expression_projection_name;

        if (sort_node_typed.hasFillStep())
            sort_column_projection_name_buffer << " STEP " << fill_step_expression_projection_name;
    }

    return sort_column_projection_name_buffer.str();
}

/** Try to get lambda node from sql user defined functions if sql user defined function with function name exists.
  * Returns lambda node if function exists, nullptr otherwise.
  */
QueryTreeNodePtr QueryAnalyzer::tryGetLambdaFromSQLUserDefinedFunctions(const std::string & function_name, ContextPtr context)
{
    auto user_defined_function = UserDefinedSQLFunctionFactory::instance().tryGet(function_name);
    if (!user_defined_function)
        return {};

    auto it = function_name_to_user_defined_lambda.find(function_name);
    if (it != function_name_to_user_defined_lambda.end())
        return it->second;

    const auto & create_function_query = user_defined_function->as<ASTCreateFunctionQuery>();
    auto result_node = buildQueryTree(create_function_query->function_core, context);
    if (result_node->getNodeType() != QueryTreeNodeType::LAMBDA)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SQL user defined function {} must represent lambda expression. Actual {}",
            function_name,
            create_function_query->function_core->formatForErrorMessage());

    function_name_to_user_defined_lambda.emplace(function_name, result_node);

    return result_node;
}

bool subtreeHasViewSource(const IQueryTreeNode * node, const Context & context)
{
    if (!node)
        return false;

    if (const auto * table_node = node->as<TableNode>())
    {
        if (table_node->getStorageID().getFullNameNotQuoted() == context.getViewSource()->getStorageID().getFullNameNotQuoted())
            return true;
    }

    for (const auto & child : node->getChildren())
        if (subtreeHasViewSource(child.get(), context))
            return true;

    return false;
}

/// Evaluate scalar subquery and perform constant folding if scalar subquery does not have constant value
void QueryAnalyzer::evaluateScalarSubqueryIfNeeded(QueryTreeNodePtr & node, IdentifierResolveScope & scope)
{
    auto * query_node = node->as<QueryNode>();
    auto * union_node = node->as<UnionNode>();
    if (!query_node && !union_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Node must have query or union type. Actual {} {}",
            node->getNodeTypeName(),
            node->formatASTForErrorMessage());

    auto & context = scope.context;

    Block scalar_block;

    auto node_without_alias = node->clone();
    node_without_alias->removeAlias();

    QueryTreeNodePtrWithHash node_with_hash(node_without_alias);
    auto str_hash = DB::toString(node_with_hash.hash);

    bool can_use_global_scalars = !only_analyze && !(context->getViewSource() && subtreeHasViewSource(node_without_alias.get(), *context));

    auto & scalars_cache = can_use_global_scalars ? scalar_subquery_to_scalar_value_global : scalar_subquery_to_scalar_value_local;

    if (scalars_cache.contains(node_with_hash))
    {
        if (can_use_global_scalars)
            ProfileEvents::increment(ProfileEvents::ScalarSubqueriesGlobalCacheHit);
        else
            ProfileEvents::increment(ProfileEvents::ScalarSubqueriesLocalCacheHit);

        scalar_block = scalars_cache.at(node_with_hash);
    }
    else if (context->hasQueryContext() && can_use_global_scalars && context->getQueryContext()->hasScalar(str_hash))
    {
        scalar_block = context->getQueryContext()->getScalar(str_hash);
        scalar_subquery_to_scalar_value_global.emplace(node_with_hash, scalar_block);
        ProfileEvents::increment(ProfileEvents::ScalarSubqueriesGlobalCacheHit);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::ScalarSubqueriesCacheMiss);
        auto subquery_context = Context::createCopy(context);

        Settings subquery_settings = context->getSettingsCopy();
        subquery_settings[Setting::max_result_rows] = 1;
        subquery_settings[Setting::extremes] = false;
        subquery_context->setSettings(subquery_settings);
        /// When execute `INSERT INTO t WITH ... SELECT ...`, it may lead to `Unknown columns`
        /// exception with this settings enabled(https://github.com/ClickHouse/ClickHouse/issues/52494).
        subquery_context->setSetting("use_structure_from_insertion_table_in_table_functions", false);

        auto options = SelectQueryOptions(QueryProcessingStage::Complete, scope.subquery_depth, true /*is_subquery*/);
        options.only_analyze = only_analyze;
        auto interpreter = std::make_unique<InterpreterSelectQueryAnalyzer>(node->toAST(), subquery_context, subquery_context->getViewSource(), options);

        if (only_analyze)
        {
            /// If query is only analyzed, then constants are not correct.
            scalar_block = interpreter->getSampleBlock();
            for (auto & column : scalar_block)
            {
                if (column.column->empty())
                {
                    auto mut_col = column.column->cloneEmpty();
                    mut_col->insertDefault();
                    column.column = std::move(mut_col);
                }
            }
        }
        else
        {
            auto io = interpreter->execute();
            PullingAsyncPipelineExecutor executor(io.pipeline);
            io.pipeline.setProgressCallback(context->getProgressCallback());
            io.pipeline.setProcessListElement(context->getProcessListElement());

            Block block;

            while (block.rows() == 0 && executor.pull(block))
            {
            }

            if (block.rows() == 0)
            {
                auto types = interpreter->getSampleBlock().getDataTypes();
                if (types.size() != 1)
                    types = {std::make_shared<DataTypeTuple>(types)};

                auto & type = types[0];
                if (!type->isNullable())
                {
                    if (!type->canBeInsideNullable())
                        throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                            "Scalar subquery returned empty result of type {} which cannot be Nullable",
                            type->getName());

                    type = makeNullable(type);
                }

                auto scalar_column = type->createColumn();
                scalar_column->insert(Null());
                scalar_block.insert({std::move(scalar_column), type, "null"});
            }
            else
            {
                if (block.rows() != 1)
                    throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

                Block tmp_block;
                while (tmp_block.rows() == 0 && executor.pull(tmp_block))
                {
                }

                if (tmp_block.rows() != 0)
                    throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one row");

                block = materializeBlock(block);
                size_t columns = block.columns();

                if (columns == 1)
                {
                    auto & column = block.getByPosition(0);
                    /// Here we wrap type to nullable if we can.
                    /// It is needed cause if subquery return no rows, it's result will be Null.
                    /// In case of many columns, do not check it cause tuple can't be nullable.
                    if (!column.type->isNullable() && column.type->canBeInsideNullable())
                    {
                        column.type = makeNullable(column.type);
                        column.column = makeNullable(column.column);
                    }

                    scalar_block = block;
                }
                else
                {
                    /** Make unique column names for tuple.
                      *
                      * Example: SELECT (SELECT 2 AS x, x)
                      */
                    makeUniqueColumnNamesInBlock(block);

                    scalar_block.insert({
                        ColumnTuple::create(block.getColumns()),
                        std::make_shared<DataTypeTuple>(block.getDataTypes(), block.getNames()),
                        "tuple"});
                }
            }
        }

        scalars_cache.emplace(node_with_hash, scalar_block);
        if (can_use_global_scalars && context->hasQueryContext())
            context->getQueryContext()->addScalar(str_hash, scalar_block);
    }

    const auto & scalar_column_with_type = scalar_block.safeGetByPosition(0);
    const auto & scalar_type = scalar_column_with_type.type;

    Field scalar_value;
    scalar_column_with_type.column->get(0, scalar_value);

    const auto * scalar_type_name = scalar_block.safeGetByPosition(0).type->getFamilyName();
    static const std::set<std::string_view> useless_literal_types = {"Array", "Tuple", "AggregateFunction", "Function", "Set", "LowCardinality"};
    auto * nearest_query_scope = scope.getNearestQueryScope();

    /// Always convert to literals when there is no query context
    if (!context->getSettingsRef()[Setting::enable_scalar_subquery_optimization] || !useless_literal_types.contains(scalar_type_name)
        || !context->hasQueryContext() || !nearest_query_scope)
    {
        auto constant_value = std::make_shared<ConstantValue>(std::move(scalar_value), scalar_type);
        auto constant_node = std::make_shared<ConstantNode>(constant_value, node);

        if (constant_node->getValue().isNull())
        {
            node = buildCastFunction(constant_node, constant_node->getResultType(), context);
            node = std::make_shared<ConstantNode>(std::move(constant_value), node);
        }
        else
            node = std::move(constant_node);

        return;
    }

    auto & nearest_query_scope_query_node = nearest_query_scope->scope_node->as<QueryNode &>();
    auto & mutable_context = nearest_query_scope_query_node.getMutableContext();

    auto scalar_query_hash_string = DB::toString(node_with_hash.hash) + (only_analyze ? "_analyze" : "");

    if (mutable_context->hasQueryContext())
        mutable_context->getQueryContext()->addScalar(scalar_query_hash_string, scalar_block);

    mutable_context->addScalar(scalar_query_hash_string, scalar_block);

    std::string get_scalar_function_name = "__getScalar";

    auto scalar_query_hash_constant_value = std::make_shared<ConstantValue>(std::move(scalar_query_hash_string), std::make_shared<DataTypeString>());
    auto scalar_query_hash_constant_node = std::make_shared<ConstantNode>(std::move(scalar_query_hash_constant_value));

    auto get_scalar_function_node = std::make_shared<FunctionNode>(get_scalar_function_name);
    get_scalar_function_node->getArguments().getNodes().push_back(std::move(scalar_query_hash_constant_node));

    auto get_scalar_function = FunctionFactory::instance().get(get_scalar_function_name, mutable_context);
    get_scalar_function_node->resolveAsFunction(get_scalar_function->build(get_scalar_function_node->getArgumentColumns()));

    node = std::move(get_scalar_function_node);
}

void QueryAnalyzer::mergeWindowWithParentWindow(const QueryTreeNodePtr & window_node, const QueryTreeNodePtr & parent_window_node, IdentifierResolveScope & scope)
{
    auto & window_node_typed = window_node->as<WindowNode &>();
    auto parent_window_name = window_node_typed.getParentWindowName();

    auto & parent_window_node_typed = parent_window_node->as<WindowNode &>();

    /** If an existing_window_name is specified it must refer to an earlier
      * entry in the WINDOW list; the new window copies its partitioning clause
      * from that entry, as well as its ordering clause if any. In this case
      * the new window cannot specify its own PARTITION BY clause, and it can
      * specify ORDER BY only if the copied window does not have one. The new
      * window always uses its own frame clause; the copied window must not
      * specify a frame clause.
      * https://www.postgresql.org/docs/current/sql-select.html
      */
    if (window_node_typed.hasPartitionBy())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Derived window definition '{}' is not allowed to override PARTITION BY. In scope {}",
            window_node_typed.formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    if (window_node_typed.hasOrderBy() && parent_window_node_typed.hasOrderBy())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Derived window definition '{}' is not allowed to override a non-empty ORDER BY. In scope {}",
            window_node_typed.formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    if (!parent_window_node_typed.getWindowFrame().is_default)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Parent window '{}' is not allowed to define a frame: while processing derived window definition '{}'. In scope {}",
            parent_window_name,
            window_node_typed.formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    window_node_typed.getPartitionByNode() = parent_window_node_typed.getPartitionBy().clone();

    if (parent_window_node_typed.hasOrderBy())
        window_node_typed.getOrderByNode() = parent_window_node_typed.getOrderBy().clone();
}

/** Replace nodes in node list with positional arguments.
  *
  * Example: SELECT id, value FROM test_table GROUP BY 1, 2;
  * Example: SELECT id, value FROM test_table ORDER BY 1, 2;
  * Example: SELECT id, value FROM test_table LIMIT 5 BY 1, 2;
  */
void QueryAnalyzer::replaceNodesWithPositionalArguments(QueryTreeNodePtr & node_list, const QueryTreeNodes & projection_nodes, IdentifierResolveScope & scope)
{
    const auto & settings = scope.context->getSettingsRef();
    if (!settings[Setting::enable_positional_arguments] || scope.context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY)
        return;

    auto & node_list_typed = node_list->as<ListNode &>();

    for (auto & node : node_list_typed.getNodes())
    {
        auto * node_to_replace = &node;

        if (auto * sort_node = node->as<SortNode>())
            node_to_replace = &sort_node->getExpression();

        auto * constant_node = (*node_to_replace)->as<ConstantNode>();

        if (!constant_node
            || (constant_node->getValue().getType() != Field::Types::UInt64
                && constant_node->getValue().getType() != Field::Types::Int64))
            continue;

        UInt64 pos;
        if (constant_node->getValue().getType() == Field::Types::UInt64)
        {
            pos = constant_node->getValue().safeGet<UInt64>();
        }
        else // Int64
        {
            auto value = constant_node->getValue().safeGet<Int64>();
            if (value > 0)
                pos = value;
            else
            {
                if (value < -static_cast<Int64>(projection_nodes.size()))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Negative positional argument number {} is out of bounds. Expected in range [-{}, -1]. In scope {}",
                        value,
                        projection_nodes.size(),
                        scope.scope_node->formatASTForErrorMessage());
                pos = projection_nodes.size() + value + 1;
            }
        }

        if (!pos || pos > projection_nodes.size())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Positional argument number {} is out of bounds. Expected in range [1, {}]. In scope {}",
                pos,
                projection_nodes.size(),
                scope.scope_node->formatASTForErrorMessage());

        --pos;
        *node_to_replace = projection_nodes[pos]->clone();
        if (auto it = resolved_expressions.find(projection_nodes[pos]); it != resolved_expressions.end())
        {
            resolved_expressions[*node_to_replace] = it->second;
        }
    }
}

void QueryAnalyzer::convertLimitOffsetExpression(QueryTreeNodePtr & expression_node, const String & expression_description, IdentifierResolveScope & scope)
{
    const auto * limit_offset_constant_node = expression_node->as<ConstantNode>();
    if (!limit_offset_constant_node || !isNativeNumber(removeNullable(limit_offset_constant_node->getResultType())))
        throw Exception(ErrorCodes::INVALID_LIMIT_EXPRESSION,
            "{} expression must be constant with numeric type. Actual {}. In scope {}",
            expression_description,
            expression_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());

    Field converted_value = convertFieldToType(limit_offset_constant_node->getValue(), DataTypeUInt64());
    if (converted_value.isNull())
        throw Exception(ErrorCodes::INVALID_LIMIT_EXPRESSION,
            "{} numeric constant expression is not representable as UInt64",
            expression_description);

    auto constant_value = std::make_shared<ConstantValue>(std::move(converted_value), std::make_shared<DataTypeUInt64>());
    auto result_constant_node = std::make_shared<ConstantNode>(std::move(constant_value));
    result_constant_node->getSourceExpression() = limit_offset_constant_node->getSourceExpression();

    expression_node = std::move(result_constant_node);
}

void QueryAnalyzer::validateTableExpressionModifiers(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    auto * table_node = table_expression_node->as<TableNode>();
    auto * table_function_node = table_expression_node->as<TableFunctionNode>();
    auto * query_node = table_expression_node->as<QueryNode>();
    auto * union_node = table_expression_node->as<UnionNode>();

    if (!table_node && !table_function_node && !query_node && !union_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
        "Unexpected table expression. Expected table, table function, query or union node. Table node: {}, scope node: {}",
        table_expression_node->formatASTForErrorMessage(),
        scope.scope_node->formatASTForErrorMessage());

    if (table_node || table_function_node)
    {
        auto table_expression_modifiers = table_node ? table_node->getTableExpressionModifiers() : table_function_node->getTableExpressionModifiers();

        if (table_expression_modifiers.has_value())
        {
            const auto & storage = table_node ? table_node->getStorage() : table_function_node->getStorage();
            if (table_expression_modifiers->hasFinal() && !storage->supportsFinal())
                throw Exception(ErrorCodes::ILLEGAL_FINAL,
                    "Storage {} doesn't support FINAL",
                    storage->getName());

            if (table_expression_modifiers->hasSampleSizeRatio() && !storage->supportsSampling())
                throw Exception(ErrorCodes::SAMPLING_NOT_SUPPORTED,
                    "Storage {} doesn't support sampling",
                    storage->getStorageID().getFullNameNotQuoted());
        }
    }
}

void QueryAnalyzer::validateJoinTableExpressionWithoutAlias(const QueryTreeNodePtr & join_node, const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    if (!scope.context->getSettingsRef()[Setting::joined_subquery_requires_alias])
        return;

    bool table_expression_has_alias = table_expression_node->hasAlias();
    if (table_expression_has_alias)
        return;

    if (join_node->as<JoinNode &>().getKind() == JoinKind::Paste)
        return;

    auto * query_node = table_expression_node->as<QueryNode>();
    auto * union_node = table_expression_node->as<UnionNode>();
    if ((query_node && !query_node->getCTEName().empty()) || (union_node && !union_node->getCTEName().empty()))
        return;

    auto table_expression_node_type = table_expression_node->getNodeType();

    if (table_expression_node_type == QueryTreeNodeType::TABLE_FUNCTION ||
        table_expression_node_type == QueryTreeNodeType::QUERY ||
        table_expression_node_type == QueryTreeNodeType::UNION)
        throw Exception(ErrorCodes::ALIAS_REQUIRED,
                        "JOIN {} no alias for subquery or table function {}. "
                        "In scope {} (set joined_subquery_requires_alias = 0 to disable restriction)",
                        join_node->formatASTForErrorMessage(),
                        table_expression_node->formatASTForErrorMessage(),
                        scope.scope_node->formatASTForErrorMessage());
}

std::pair<bool, UInt64> QueryAnalyzer::recursivelyCollectMaxOrdinaryExpressions(QueryTreeNodePtr & node, QueryTreeNodes & into)
{
    checkStackSize();

    if (node->as<ColumnNode>())
    {
        into.push_back(node);
        return {false, 1};
    }

    auto * function = node->as<FunctionNode>();

    if (!function)
        return {false, 0};

    if (function->isAggregateFunction())
        return {true, 0};

    UInt64 pushed_children = 0;
    bool has_aggregate = false;

    for (auto & child : function->getArguments().getNodes())
    {
        auto [child_has_aggregate, child_pushed_children] = recursivelyCollectMaxOrdinaryExpressions(child, into);
        has_aggregate |= child_has_aggregate;
        pushed_children += child_pushed_children;
    }

    /// The current function is not aggregate function and there is no aggregate function in its arguments,
    /// so use the current function to replace its arguments
    if (!has_aggregate)
    {
        for (UInt64 i = 0; i < pushed_children; i++)
            into.pop_back();

        into.push_back(node);
        pushed_children = 1;
    }

    return {has_aggregate, pushed_children};
}

/** Expand GROUP BY ALL by extracting all the SELECT-ed expressions that are not aggregate functions.
  *
  * For a special case that if there is a function having both aggregate functions and other fields as its arguments,
  * the `GROUP BY` keys will contain the maximum non-aggregate fields we can extract from it.
  *
  * Example:
  * SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t GROUP BY ALL
  * will expand as
  * SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t GROUP BY substring(a, 4, 2), substring(a, 1, 2)
  */
void QueryAnalyzer::expandGroupByAll(QueryNode & query_tree_node_typed)
{
    if (!query_tree_node_typed.isGroupByAll())
        return;

    auto & group_by_nodes = query_tree_node_typed.getGroupBy().getNodes();
    auto & projection_list = query_tree_node_typed.getProjection();

    for (auto & node : projection_list.getNodes())
        recursivelyCollectMaxOrdinaryExpressions(node, group_by_nodes);
    query_tree_node_typed.setIsGroupByAll(false);
}

void QueryAnalyzer::expandOrderByAll(QueryNode & query_tree_node_typed, const Settings & settings)
{
    if (!settings[Setting::enable_order_by_all] || !query_tree_node_typed.isOrderByAll())
        return;

    auto * all_node = query_tree_node_typed.getOrderBy().getNodes()[0]->as<SortNode>();
    if (!all_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Select analyze for not sort node.");

    auto & projection_nodes = query_tree_node_typed.getProjection().getNodes();
    auto list_node = std::make_shared<ListNode>();
    list_node->getNodes().reserve(projection_nodes.size());

    for (auto & node : projection_nodes)
    {
        /// Detect and reject ambiguous statements:
        /// E.g. for a table with columns "all", "a", "b":
        /// - SELECT all, a, b ORDER BY all;        -- should we sort by all columns in SELECT or by column "all"?
        /// - SELECT a, b AS all ORDER BY all;      -- like before but "all" as alias
        /// - SELECT func(...) AS all ORDER BY all; -- like before but "all" as function
        /// - SELECT a, b ORDER BY all;             -- tricky in other way: does the user want to sort by columns in SELECT clause or by not SELECTed column "all"?

        auto resolved_expression_it = resolved_expressions.find(node);
        if (resolved_expression_it != resolved_expressions.end())
        {
            auto projection_names = resolved_expression_it->second;
            if (projection_names.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Expression nodes list expected 1 projection names. Actual {}",
                                projection_names.size());
            if (boost::iequals(projection_names[0], "all"))
                throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION,
                                "Cannot use ORDER BY ALL to sort a column with name 'all', please disable setting `enable_order_by_all` and try again");
        }

        auto sort_node = std::make_shared<SortNode>(node, all_node->getSortDirection(), all_node->getNullsSortDirection());
        list_node->getNodes().push_back(sort_node);
    }

    query_tree_node_typed.getOrderByNode() = list_node;
    query_tree_node_typed.setIsOrderByAll(false);
}

std::string QueryAnalyzer::rewriteAggregateFunctionNameIfNeeded(
    const std::string & aggregate_function_name, NullsAction action, const ContextPtr & context)
{
    std::string result_aggregate_function_name = aggregate_function_name;
    auto aggregate_function_name_lowercase = Poco::toLower(aggregate_function_name);

    const auto & settings = context->getSettingsRef();

    if (aggregate_function_name_lowercase == "countdistinct")
    {
        result_aggregate_function_name = settings[Setting::count_distinct_implementation];
    }
    else if (
        aggregate_function_name_lowercase == "countifdistinct"
        || (settings[Setting::rewrite_count_distinct_if_with_count_distinct_implementation]
            && aggregate_function_name_lowercase == "countdistinctif"))
    {
        result_aggregate_function_name = settings[Setting::count_distinct_implementation];
        result_aggregate_function_name += "If";
    }
    else if (aggregate_function_name_lowercase.ends_with("ifdistinct"))
    {
        /// Replace aggregateFunctionIfDistinct into aggregateFunctionDistinctIf to make execution more optimal
        size_t prefix_length = result_aggregate_function_name.size() - strlen("ifdistinct");
        result_aggregate_function_name = result_aggregate_function_name.substr(0, prefix_length) + "DistinctIf";
    }

    bool need_add_or_null = settings[Setting::aggregate_functions_null_for_empty] && !result_aggregate_function_name.ends_with("OrNull");
    if (need_add_or_null)
    {
        auto properties = AggregateFunctionFactory::instance().tryGetProperties(result_aggregate_function_name, action);
        if (!properties->returns_default_when_only_null)
            result_aggregate_function_name += "OrNull";
    }

    /** Move -OrNull suffix ahead, this should execute after add -OrNull suffix.
      * Used to rewrite aggregate functions with -OrNull suffix in some cases.
      * Example: sumIfOrNull.
      * Result: sumOrNullIf.
      */
    if (result_aggregate_function_name.ends_with("OrNull"))
    {
        auto function_properies = AggregateFunctionFactory::instance().tryGetProperties(result_aggregate_function_name, action);
        if (function_properies && !function_properies->returns_default_when_only_null)
        {
            size_t function_name_size = result_aggregate_function_name.size();

            static constexpr std::array<std::string_view, 4> suffixes_to_replace = {"MergeState", "Merge", "State", "If"};
            for (const auto & suffix : suffixes_to_replace)
            {
                auto suffix_string_value = String(suffix);
                auto suffix_to_check = suffix_string_value + "OrNull";

                if (!result_aggregate_function_name.ends_with(suffix_to_check))
                    continue;

                result_aggregate_function_name = result_aggregate_function_name.substr(0, function_name_size - suffix_to_check.size());
                result_aggregate_function_name += "OrNull";
                result_aggregate_function_name += suffix_string_value;

                break;
            }
        }
    }

    return result_aggregate_function_name;
}

/// Resolve identifier functions implementation

/** Resolve identifier from scope aliases.
  *
  * Resolve strategy:
  * 1. If alias is registered in current expressions that are in resolve process and if top expression is not part of bottom expression with the same alias subtree
  * throw cyclic aliases exception.
  * Otherwise prevent cache usage for identifier lookup and return nullptr.
  *
  * This is special scenario where identifier has name the same as alias name in one of its parent expressions including itself.
  * In such case we cannot resolve identifier from aliases because of recursion. It is client responsibility to register and deregister alias
  * names during expressions resolve.
  *
  * We must prevent cache usage for lookup because lookup outside of expression is supposed to return other value.
  * Example: SELECT (id + 1) AS id, id + 2. Lookup for id inside (id + 1) as id should return id from table, but lookup (id + 2) should return
  * (id + 1) AS id.
  *
  * Below cases should work:
  * Example:
  * SELECT id AS id FROM test_table;
  * SELECT value.value1 AS value FROM test_table;
  * SELECT (id + 1) AS id FROM test_table;
  * SELECT (1 + (1 + id)) AS id FROM test_table;
  *
  * Below cases should throw cyclic aliases exception:
  * SELECT (id + b) AS id, id as b FROM test_table;
  * SELECT (1 + b + 1 + id) AS id, b as c, id as b FROM test_table;
  *
  * 2. Depending on IdentifierLookupContext get alias name to node map from IdentifierResolveScope.
  * 3. Try to bind identifier to alias name in map. If there are no such binding return nullptr.
  * 4. If node in map is not resolved, resolve it. It is important in case of compound expressions.
  * Example: SELECT value.a, cast('(1)', 'Tuple(a UInt64)') AS value;
  *
  * Special case if node is identifier node.
  * Example: SELECT value, id AS value FROM test_table;
  *
  * Add node in current scope expressions in resolve process stack.
  * Try to resolve identifier.
  * If identifier is resolved, depending on lookup context, erase entry from expression or lambda map. Check QueryExpressionsAliasVisitor documentation.
  * Pop node from current scope expressions in resolve process stack.
  *
  * 5. If identifier is compound and identifier lookup is in expression context, use `tryResolveIdentifierFromCompoundExpression`.
  */
QueryTreeNodePtr QueryAnalyzer::tryResolveIdentifierFromAliases(const IdentifierLookup & identifier_lookup,
    IdentifierResolveScope & scope,
    IdentifierResolveSettings identifier_resolve_settings)
{
    const auto & identifier_bind_part = identifier_lookup.identifier.front();

    auto * it = scope.aliases.find(identifier_lookup, ScopeAliases::FindOption::FIRST_NAME);
    if (it == nullptr)
        return {};

    QueryTreeNodePtr & alias_node = *it;

    if (!alias_node)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Node with alias {} is not valid. In scope {}",
            identifier_bind_part,
            scope.scope_node->formatASTForErrorMessage());

    if (auto root_expression_with_alias = scope.expressions_in_resolve_process_stack.getExpressionWithAlias(identifier_bind_part))
    {
        const auto top_expression = scope.expressions_in_resolve_process_stack.getTop();

        if (!isNodePartOfTree(top_expression.get(), root_expression_with_alias.get()))
            throw Exception(ErrorCodes::CYCLIC_ALIASES,
                "Cyclic aliases for identifier '{}'. In scope {}",
                identifier_lookup.identifier.getFullName(),
                scope.scope_node->formatASTForErrorMessage());

        scope.non_cached_identifier_lookups_during_expression_resolve.insert(identifier_lookup);
        return {};
    }

    auto node_type = alias_node->getNodeType();

    /// Resolve expression if necessary
    if (node_type == QueryTreeNodeType::IDENTIFIER)
    {
        scope.pushExpressionNode(alias_node);

        auto & alias_identifier_node = alias_node->as<IdentifierNode &>();
        auto identifier = alias_identifier_node.getIdentifier();
        auto lookup_result = tryResolveIdentifier(IdentifierLookup{identifier, identifier_lookup.lookup_context}, scope, identifier_resolve_settings);
        if (!lookup_result.resolved_identifier)
        {
            std::unordered_set<Identifier> valid_identifiers;
            IdentifierResolver::collectScopeWithParentScopesValidIdentifiersForTypoCorrection(identifier, scope, true, false, false, valid_identifiers);
            auto hints = IdentifierResolver::collectIdentifierTypoHints(identifier, valid_identifiers);

            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown {} identifier '{}'. In scope {}{}",
                toStringLowercase(identifier_lookup.lookup_context),
                identifier.getFullName(),
                scope.scope_node->formatASTForErrorMessage(),
                getHintsErrorMessageSuffix(hints));
        }

        alias_node = lookup_result.resolved_identifier;
        scope.popExpressionNode();
    }
    else if (node_type == QueryTreeNodeType::FUNCTION)
    {
        resolveExpressionNode(alias_node, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
    }
    else if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
    {
        if (identifier_resolve_settings.allow_to_resolve_subquery_during_identifier_resolution)
            resolveExpressionNode(alias_node, scope, false /*allow_lambda_expression*/, identifier_lookup.isTableExpressionLookup() /*allow_table_expression*/);
    }

    if (identifier_lookup.identifier.isCompound() && alias_node)
    {
        if (identifier_lookup.isExpressionLookup())
        {
            return identifier_resolver.tryResolveIdentifierFromCompoundExpression(
                identifier_lookup.identifier,
                1 /*identifier_bind_size*/,
                alias_node,
                {} /* compound_expression_source */,
                scope,
                identifier_resolve_settings.allow_to_check_join_tree /* can_be_not_found */);
        }
        else if (identifier_lookup.isFunctionLookup() || identifier_lookup.isTableExpressionLookup())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Compound identifier '{}' cannot be resolved as {}. In scope {}",
                identifier_lookup.identifier.getFullName(),
                identifier_lookup.isFunctionLookup() ? "function" : "table expression",
                scope.scope_node->formatASTForErrorMessage());
        }
    }

    return alias_node;
}

/** Try resolve identifier in current scope parent scopes.
  *
  * TODO: If column is matched, throw exception that nested subqueries are not supported.
  *
  * If initial scope is expression. Then try to resolve identifier in parent scopes until query scope is hit.
  * For query scope resolve strategy is same as if initial scope if query.
  */
IdentifierResolveResult QueryAnalyzer::tryResolveIdentifierInParentScopes(const IdentifierLookup & identifier_lookup, IdentifierResolveScope & scope)
{
    bool initial_scope_is_query = scope.scope_node->getNodeType() == QueryTreeNodeType::QUERY;
    bool initial_scope_is_expression = !initial_scope_is_query;

    IdentifierResolveSettings identifier_resolve_settings;
    identifier_resolve_settings.allow_to_check_parent_scopes = false;
    identifier_resolve_settings.allow_to_check_database_catalog = false;

    IdentifierResolveScope * scope_to_check = scope.parent_scope;

    if (initial_scope_is_expression)
    {
        while (scope_to_check != nullptr)
        {
            auto resolve_result = tryResolveIdentifier(identifier_lookup, *scope_to_check, identifier_resolve_settings);
            if (resolve_result.resolved_identifier)
                return resolve_result;

            bool scope_was_query = scope_to_check->scope_node->getNodeType() == QueryTreeNodeType::QUERY;
            scope_to_check = scope_to_check->parent_scope;

            if (scope_was_query)
                break;
        }
    }

    if (!scope.context->getSettingsRef()[Setting::enable_global_with_statement])
        return {};

    /** Nested subqueries cannot access outer subqueries table expressions from JOIN tree because
      * that can prevent resolution of table expression from CTE.
      *
      * Example: WITH a AS (SELECT number FROM numbers(1)), b AS (SELECT number FROM a) SELECT * FROM a as l, b as r;
      */
    if (identifier_lookup.isTableExpressionLookup())
        identifier_resolve_settings.allow_to_check_join_tree = false;

    while (scope_to_check != nullptr)
    {
        auto lookup_result = tryResolveIdentifier(identifier_lookup, *scope_to_check, identifier_resolve_settings);
        const auto & resolved_identifier = lookup_result.resolved_identifier;

        scope_to_check = scope_to_check->parent_scope;

        if (resolved_identifier)
        {
            auto * subquery_node = resolved_identifier->as<QueryNode>();
            auto * union_node = resolved_identifier->as<UnionNode>();

            bool is_cte = (subquery_node && subquery_node->isCTE()) || (union_node && union_node->isCTE());
            bool is_table_from_expression_arguments = lookup_result.resolve_place == IdentifierResolvePlace::EXPRESSION_ARGUMENTS &&
                resolved_identifier->getNodeType() == QueryTreeNodeType::TABLE;
            bool is_valid_table_expression = is_cte || is_table_from_expression_arguments;

            /** From parent scopes we can resolve table identifiers only as CTE.
              * Example: SELECT (SELECT 1 FROM a) FROM test_table AS a;
              *
              * During child scope table identifier resolve a, table node test_table with alias a from parent scope
              * is invalid.
              */
            if (identifier_lookup.isTableExpressionLookup() && !is_valid_table_expression)
                continue;

            if (is_valid_table_expression || resolved_identifier->as<ConstantNode>())
            {
                return lookup_result;
            }
            else if (auto * resolved_function = resolved_identifier->as<FunctionNode>())
            {
                /// Special case: scalar subquery was executed and replaced by __getScalar function.
                /// Handle it as a constant.
                if (resolved_function->getFunctionName() == "__getScalar")
                    return lookup_result;
            }

            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Resolve identifier '{}' from parent scope only supported for constants and CTE. Actual {} node type {}. In scope {}",
                identifier_lookup.identifier.getFullName(),
                resolved_identifier->formatASTForErrorMessage(),
                resolved_identifier->getNodeTypeName(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }

    return {};
}

/** Resolve identifier in scope.
  *
  * If identifier was resolved resolve identified lookup status will be updated.
  *
  * Steps:
  * 1. Register identifier lookup in scope identifier lookup to resolve status table.
  * If entry is already registered and is not resolved, that means that we have cyclic aliases for identifier.
  * Example: SELECT a AS b, b AS a;
  * Try resolve identifier in current scope:
  * 3. Try resolve identifier from expression arguments.
  *
  * If prefer_column_name_to_alias = true.
  * 4. Try to resolve identifier from join tree.
  * 5. Try to resolve identifier from aliases.
  * Otherwise.
  * 4. Try to resolve identifier from aliases.
  * 5. Try to resolve identifier from join tree.
  *
  * 6. If it is table identifier lookup try to lookup identifier in current scope CTEs.
  *
  * 7. If identifier is not resolved in current scope, try to resolve it in parent scopes.
  * 8. If identifier is not resolved from parent scopes and it is table identifier lookup try to lookup identifier
  * in database catalog.
  *
  * Same is not done for functions because function resolution is more complex, and in case of aggregate functions requires not only name
  * but also argument types, it is responsibility of resolve function method to handle resolution of function name.
  *
  * 9. If identifier was not resolved, or identifier caching was disabled remove it from identifier lookup to resolve status table.
  *
  * It is okay for identifier to be not resolved, in case we want first try to lookup identifier in one context,
  * then if there is no identifier in this context, try to lookup in another context.
  * Example: Try to lookup identifier as expression, if it is not found, lookup as function.
  * Example: Try to lookup identifier as expression, if it is not found, lookup as table.
  */
IdentifierResolveResult QueryAnalyzer::tryResolveIdentifier(const IdentifierLookup & identifier_lookup,
    IdentifierResolveScope & scope,
    IdentifierResolveSettings identifier_resolve_settings)
{
    auto it = scope.identifier_lookup_to_resolve_state.find(identifier_lookup);
    if (it != scope.identifier_lookup_to_resolve_state.end())
    {
        if (it->second.cyclic_identifier_resolve)
            throw Exception(ErrorCodes::CYCLIC_ALIASES,
                "Cyclic aliases for identifier '{}'. In scope {}",
                identifier_lookup.identifier.getFullName(),
                scope.scope_node->formatASTForErrorMessage());

        if (!it->second.resolve_result.isResolved())
            it->second.cyclic_identifier_resolve = true;

        if (it->second.resolve_result.isResolved() &&
            scope.use_identifier_lookup_to_result_cache &&
            !scope.non_cached_identifier_lookups_during_expression_resolve.contains(identifier_lookup) &&
            (!it->second.resolve_result.isResolvedFromCTEs() || !ctes_in_resolve_process.contains(identifier_lookup.identifier.getFullName())))
            return it->second.resolve_result;
    }
    else
    {
        auto [insert_it, _] = scope.identifier_lookup_to_resolve_state.insert({identifier_lookup, IdentifierResolveState()});
        it = insert_it;
    }

    /// Resolve identifier from current scope

    IdentifierResolveResult resolve_result;
    resolve_result.resolved_identifier = identifier_resolver.tryResolveIdentifierFromExpressionArguments(identifier_lookup, scope);
    if (resolve_result.resolved_identifier)
        resolve_result.resolve_place = IdentifierResolvePlace::EXPRESSION_ARGUMENTS;

    if (!resolve_result.resolved_identifier)
    {
        bool prefer_column_name_to_alias = scope.context->getSettingsRef()[Setting::prefer_column_name_to_alias];

        if (identifier_lookup.isExpressionLookup())
        {
            /* For aliases from ARRAY JOIN we prefer column from join tree:
             * SELECT id FROM ( SELECT ... ) AS subquery ARRAY JOIN [0] AS id INNER JOIN second_table USING (id)
             * In the example, identifier `id` should be resolved into one from USING (id) column.
             */

            auto * alias_it = scope.aliases.find(identifier_lookup, ScopeAliases::FindOption::FULL_NAME);
            if (alias_it && (*alias_it)->getNodeType() == QueryTreeNodeType::COLUMN)
            {
                const auto & column_node = (*alias_it)->as<ColumnNode &>();
                if (column_node.getColumnSource()->getNodeType() == QueryTreeNodeType::ARRAY_JOIN)
                    prefer_column_name_to_alias = true;
            }
        }

        if (unlikely(prefer_column_name_to_alias))
        {
            if (identifier_resolve_settings.allow_to_check_join_tree)
            {
                resolve_result.resolved_identifier = identifier_resolver.tryResolveIdentifierFromJoinTree(identifier_lookup, scope);

                if (resolve_result.resolved_identifier)
                    resolve_result.resolve_place = IdentifierResolvePlace::JOIN_TREE;
            }

            if (!resolve_result.resolved_identifier)
            {
                resolve_result.resolved_identifier = tryResolveIdentifierFromAliases(identifier_lookup, scope, identifier_resolve_settings);

                if (resolve_result.resolved_identifier)
                    resolve_result.resolve_place = IdentifierResolvePlace::ALIASES;
            }
        }
        else
        {
            resolve_result.resolved_identifier = tryResolveIdentifierFromAliases(identifier_lookup, scope, identifier_resolve_settings);

            if (resolve_result.resolved_identifier)
            {
                resolve_result.resolve_place = IdentifierResolvePlace::ALIASES;
            }
            else if (identifier_resolve_settings.allow_to_check_join_tree)
            {
                resolve_result.resolved_identifier = identifier_resolver.tryResolveIdentifierFromJoinTree(identifier_lookup, scope);

                if (resolve_result.resolved_identifier)
                    resolve_result.resolve_place = IdentifierResolvePlace::JOIN_TREE;
            }
        }

        if (resolve_result.resolve_place == IdentifierResolvePlace::JOIN_TREE)
        {
            std::stack<IQueryTreeNode *> stack;
            stack.push(resolve_result.resolved_identifier.get());
            while (!stack.empty())
            {
                auto * node = stack.top();
                stack.pop();

                if (auto * column_node = typeid_cast<ColumnNode *>(node))
                    if (column_node->hasExpression())
                        resolveExpressionNode(column_node->getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
            }
        }
    }

    if (!resolve_result.resolved_identifier && identifier_lookup.isTableExpressionLookup())
    {
        auto full_name = identifier_lookup.identifier.getFullName();
        auto cte_query_node_it = scope.cte_name_to_query_node.find(full_name);

        /// CTE may reference table expressions with the same name, e.g.:
        ///
        /// WITH test1 AS (SELECT * FROM test1) SELECT * FROM test1;
        ///
        /// Since we don't support recursive CTEs, `test1` identifier inside of CTE
        /// references to table <default database name>.test1.
        /// This means that the example above is equivalent to the following query:
        ///
        /// SELECT * FROM test1;
        ///
        /// To accomplish this behaviour it's not allowed to resolve identifiers to
        /// CTE that is being resolved.
        if (cte_query_node_it != scope.cte_name_to_query_node.end()
            && !ctes_in_resolve_process.contains(full_name))
        {
            resolve_result.resolved_identifier = cte_query_node_it->second;
            resolve_result.resolve_place = IdentifierResolvePlace::CTE;
        }
    }

    /// Try to resolve identifier from parent scopes

    if (!resolve_result.resolved_identifier && identifier_resolve_settings.allow_to_check_parent_scopes)
    {
        resolve_result = tryResolveIdentifierInParentScopes(identifier_lookup, scope);

        if (resolve_result.resolved_identifier)
            resolve_result.resolved_from_parent_scopes = true;
    }

    /// Try to resolve table identifier from database catalog

    if (!resolve_result.resolved_identifier && identifier_resolve_settings.allow_to_check_database_catalog && identifier_lookup.isTableExpressionLookup())
    {
        resolve_result.resolved_identifier = IdentifierResolver::tryResolveTableIdentifierFromDatabaseCatalog(identifier_lookup.identifier, scope.context);

        if (resolve_result.resolved_identifier)
            resolve_result.resolve_place = IdentifierResolvePlace::DATABASE_CATALOG;
    }

    bool was_cyclic_identifier_resolve = it->second.cyclic_identifier_resolve;
    if (!was_cyclic_identifier_resolve)
        it->second.resolve_result = resolve_result;
    it->second.cyclic_identifier_resolve = false;

    /** If identifier was not resolved, or during expression resolution identifier was explicitly added into non cached set,
      * or identifier caching was disabled in resolve scope we remove identifier lookup result from identifier lookup to result table.
      */
    if (!was_cyclic_identifier_resolve && (!resolve_result.resolved_identifier ||
        scope.non_cached_identifier_lookups_during_expression_resolve.contains(identifier_lookup) ||
        !scope.use_identifier_lookup_to_result_cache))
        scope.identifier_lookup_to_resolve_state.erase(it);

    return resolve_result;
}

/// Resolve query tree nodes functions implementation

/** Qualify column nodes with projection names.
  *
  * Example: SELECT * FROM test_table AS t1, test_table AS t2;
  */
void QueryAnalyzer::qualifyColumnNodesWithProjectionNames(const QueryTreeNodes & column_nodes,
    const QueryTreeNodePtr & table_expression_node,
    const IdentifierResolveScope & scope)
{
    /// Build additional column qualification parts array
    std::vector<std::string> additional_column_qualification_parts;

    if (table_expression_node->hasAlias())
        additional_column_qualification_parts = {table_expression_node->getAlias()};
    else if (auto * table_node = table_expression_node->as<TableNode>())
        additional_column_qualification_parts = {table_node->getStorageID().getDatabaseName(), table_node->getStorageID().getTableName()};
    else if (auto * query_node = table_expression_node->as<QueryNode>(); query_node && query_node->isCTE())
        additional_column_qualification_parts = {query_node->getCTEName()};
    else if (auto * union_node = table_expression_node->as<UnionNode>(); union_node && union_node->isCTE())
        additional_column_qualification_parts = {union_node->getCTEName()};

    size_t additional_column_qualification_parts_size = additional_column_qualification_parts.size();
    const auto & table_expression_data = scope.getTableExpressionDataOrThrow(table_expression_node);

    /** For each matched column node iterate over additional column qualifications and apply them if column needs to be qualified.
      * To check if column needs to be qualified we check if column name can bind to any other table expression in scope or to scope aliases.
      */
    std::vector<std::string> column_qualified_identifier_parts;

    for (const auto & column_node : column_nodes)
    {
        const auto & column_name = column_node->as<ColumnNode &>().getColumnName();
        column_qualified_identifier_parts = Identifier(column_name).getParts();

        /// Iterate over additional column qualifications and apply them if needed
        for (size_t i = 0; i < additional_column_qualification_parts_size; ++i)
        {
            auto identifier_to_check = Identifier(column_qualified_identifier_parts);
            IdentifierLookup identifier_lookup{identifier_to_check, IdentifierLookupContext::EXPRESSION};
            bool need_to_qualify = table_expression_data.should_qualify_columns;
            if (need_to_qualify)
                need_to_qualify = IdentifierResolver::tryBindIdentifierToTableExpressions(identifier_lookup, table_expression_node, scope);

            if (IdentifierResolver::tryBindIdentifierToAliases(identifier_lookup, scope))
                need_to_qualify = true;

            if (need_to_qualify)
            {
                /** Add last qualification part that was not used into column qualified identifier.
                  * If additional column qualification parts consists from [database_name, table_name].
                  * On first iteration if column is needed to be qualified to qualify it with table_name.
                  * On second iteration if column is needed to be qualified to qualify it with database_name.
                  */
                size_t part_index_to_use_for_qualification = additional_column_qualification_parts_size - i - 1;
                const auto & part_to_use = additional_column_qualification_parts[part_index_to_use_for_qualification];
                column_qualified_identifier_parts.insert(column_qualified_identifier_parts.begin(), part_to_use);
            }
            else
            {
                break;
            }
        }

        auto qualified_node_name = Identifier(column_qualified_identifier_parts).getFullName();
        node_to_projection_name.emplace(column_node, qualified_node_name);
    }
}

/// Build get columns options for matcher
GetColumnsOptions QueryAnalyzer::buildGetColumnsOptions(QueryTreeNodePtr & matcher_node, const ContextPtr & context)
{
    auto & matcher_node_typed = matcher_node->as<MatcherNode &>();
    UInt8 get_columns_options_kind = GetColumnsOptions::AllPhysicalAndAliases;

    if (matcher_node_typed.isAsteriskMatcher())
    {
        get_columns_options_kind = GetColumnsOptions::Ordinary;

        const auto & settings = context->getSettingsRef();

        if (settings[Setting::asterisk_include_alias_columns])
            get_columns_options_kind |= GetColumnsOptions::Kind::Aliases;

        if (settings[Setting::asterisk_include_materialized_columns])
            get_columns_options_kind |= GetColumnsOptions::Kind::Materialized;
    }

    return GetColumnsOptions(static_cast<GetColumnsOptions::Kind>(get_columns_options_kind));
}

QueryAnalyzer::QueryTreeNodesWithNames QueryAnalyzer::getMatchedColumnNodesWithNames(const QueryTreeNodePtr & matcher_node,
    const QueryTreeNodePtr & table_expression_node,
    const NamesAndTypes & matched_columns,
    const IdentifierResolveScope & scope)
{
    auto & matcher_node_typed = matcher_node->as<MatcherNode &>();

    /** Use resolved columns from table expression data in nearest query scope if available.
      * It is important for ALIAS columns to use column nodes with resolved ALIAS expression.
      */
    const AnalysisTableExpressionData * table_expression_data = nullptr;
    const auto * nearest_query_scope = scope.getNearestQueryScope();
    if (nearest_query_scope)
        table_expression_data = &nearest_query_scope->getTableExpressionDataOrThrow(table_expression_node);

    QueryTreeNodes matched_column_nodes;

    for (const auto & column : matched_columns)
    {
        const auto & column_name = column.name;
        if (!matcher_node_typed.isMatchingColumn(column_name))
            continue;

        if (table_expression_data)
        {
            auto column_node_it = table_expression_data->column_name_to_column_node.find(column_name);
            if (column_node_it != table_expression_data->column_name_to_column_node.end())
            {
                matched_column_nodes.emplace_back(column_node_it->second);
                continue;
            }
        }

        matched_column_nodes.emplace_back(std::make_shared<ColumnNode>(column, table_expression_node));
    }

    const auto & qualify_matched_column_nodes_scope = nearest_query_scope ? *nearest_query_scope : scope;
    qualifyColumnNodesWithProjectionNames(matched_column_nodes, table_expression_node, qualify_matched_column_nodes_scope);

    QueryAnalyzer::QueryTreeNodesWithNames matched_column_nodes_with_names;
    matched_column_nodes_with_names.reserve(matched_column_nodes.size());

    for (auto && matched_column_node : matched_column_nodes)
    {
        auto column_name = matched_column_node->as<ColumnNode &>().getColumnName();
        matched_column_nodes_with_names.emplace_back(std::move(matched_column_node), std::move(column_name));
    }

    return matched_column_nodes_with_names;
}

bool hasTableExpressionInJoinTree(const QueryTreeNodePtr & join_tree_node, const QueryTreeNodePtr & table_expression)
{
    QueryTreeNodes nodes_to_process;
    nodes_to_process.push_back(join_tree_node);

    while (!nodes_to_process.empty())
    {
        auto node_to_process = std::move(nodes_to_process.back());
        nodes_to_process.pop_back();
        if (node_to_process == table_expression)
            return true;

        if (node_to_process->getNodeType() == QueryTreeNodeType::JOIN)
        {
            const auto & join_node = node_to_process->as<JoinNode &>();
            nodes_to_process.push_back(join_node.getLeftTableExpression());
            nodes_to_process.push_back(join_node.getRightTableExpression());
        }
    }
    return false;
}

/// Columns that resolved from matcher can also match columns from JOIN USING.
/// In that case we update type to type of column in USING section.
/// TODO: It's not completely correct for qualified matchers, so t1.* should be resolved to left table column type.
/// But in planner we do not distinguish such cases.
void QueryAnalyzer::updateMatchedColumnsFromJoinUsing(
    QueryTreeNodesWithNames & result_matched_column_nodes_with_names,
    const QueryTreeNodePtr & source_table_expression,
    IdentifierResolveScope & scope)
{
    auto * nearest_query_scope = scope.getNearestQueryScope();
    auto * nearest_query_scope_query_node = nearest_query_scope ? nearest_query_scope->scope_node->as<QueryNode>() : nullptr;

    /// If there are no parent query scope or query scope does not have join tree
    if (!nearest_query_scope_query_node || !nearest_query_scope_query_node->getJoinTree())
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "There are no table sources. In scope {}",
            scope.scope_node->formatASTForErrorMessage());
    }

    const auto & join_tree = nearest_query_scope_query_node->getJoinTree();

    const auto * join_node = join_tree->as<JoinNode>();
    if (join_node && join_node->isUsingJoinExpression())
    {
        const auto & join_using_list = join_node->getJoinExpression()->as<ListNode &>();
        const auto & join_using_nodes = join_using_list.getNodes();

        for (auto & [matched_column_node, _] : result_matched_column_nodes_with_names)
        {
            auto & matched_column_node_typed = matched_column_node->as<ColumnNode &>();
            const auto & matched_column_name = matched_column_node_typed.getColumnName();

            for (const auto & join_using_node : join_using_nodes)
            {
                auto & join_using_column_node = join_using_node->as<ColumnNode &>();
                const auto & join_using_column_name = join_using_column_node.getColumnName();

                if (matched_column_name != join_using_column_name)
                    continue;

                const auto & join_using_column_nodes_list = join_using_column_node.getExpressionOrThrow()->as<ListNode &>();
                const auto & join_using_column_nodes = join_using_column_nodes_list.getNodes();

                auto it = node_to_projection_name.find(matched_column_node);

                if (hasTableExpressionInJoinTree(join_node->getLeftTableExpression(), source_table_expression))
                    matched_column_node = join_using_column_nodes.at(0);
                else if (hasTableExpressionInJoinTree(join_node->getRightTableExpression(), source_table_expression))
                    matched_column_node = join_using_column_nodes.at(1);
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot find column {} in JOIN USING section {}",
                        matched_column_node->dumpTree(), join_node->dumpTree());

                matched_column_node = matched_column_node->clone();
                if (it != node_to_projection_name.end())
                    node_to_projection_name.emplace(matched_column_node, it->second);

                matched_column_node->as<ColumnNode &>().setColumnType(join_using_column_node.getResultType());
                if (!matched_column_node->isEqual(*join_using_column_nodes.at(0)))
                    scope.join_columns_with_changed_types[matched_column_node] = join_using_column_nodes.at(0);
            }
        }
    }
}

/** Resolve qualified tree matcher.
  *
  * First try to match qualified identifier to expression. If qualified identifier matched expression node then
  * if expression is compound match it column names using matcher `isMatchingColumn` method, if expression is not compound, throw exception.
  * If qualified identifier did not match expression in query tree, try to lookup qualified identifier in table context.
  */
QueryAnalyzer::QueryTreeNodesWithNames QueryAnalyzer::resolveQualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope)
{
    auto & matcher_node_typed = matcher_node->as<MatcherNode &>();
    assert(matcher_node_typed.isQualified());

    auto expression_identifier_lookup = IdentifierLookup{matcher_node_typed.getQualifiedIdentifier(), IdentifierLookupContext::EXPRESSION};
    auto expression_identifier_resolve_result = tryResolveIdentifier(expression_identifier_lookup, scope);
    auto expression_query_tree_node = expression_identifier_resolve_result.resolved_identifier;

    /// Try to resolve unqualified matcher for query expression

    if (expression_query_tree_node)
    {
        auto result_type = expression_query_tree_node->getResultType();

        while (true)
        {
            if (const auto * array_type = typeid_cast<const DataTypeArray *>(result_type.get()))
                result_type = array_type->getNestedType();
            else if (const auto * map_type = typeid_cast<const DataTypeMap *>(result_type.get()))
                result_type = map_type->getNestedType();
            else
                break;
        }

        const auto * tuple_data_type = typeid_cast<const DataTypeTuple *>(result_type.get());
        if (!tuple_data_type)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Qualified matcher {} found a non-compound expression {} with type {}. Expected a tuple or an array of tuples. In scope {}",
                matcher_node->formatASTForErrorMessage(),
                expression_query_tree_node->formatASTForErrorMessage(),
                expression_query_tree_node->getResultType()->getName(),
                scope.scope_node->formatASTForErrorMessage());

        const auto & element_names = tuple_data_type->getElementNames();
        QueryTreeNodesWithNames matched_expression_nodes_with_column_names;

        auto qualified_matcher_element_identifier = matcher_node_typed.getQualifiedIdentifier();
        for (const auto & element_name : element_names)
        {
            if (!matcher_node_typed.isMatchingColumn(element_name))
                continue;

            auto get_subcolumn_function = std::make_shared<FunctionNode>("getSubcolumn");
            get_subcolumn_function->getArguments().getNodes().push_back(expression_query_tree_node);
            get_subcolumn_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(element_name));

            QueryTreeNodePtr function_query_node = get_subcolumn_function;
            resolveFunction(function_query_node, scope);

            qualified_matcher_element_identifier.push_back(element_name);
            node_to_projection_name.emplace(function_query_node, qualified_matcher_element_identifier.getFullName());
            qualified_matcher_element_identifier.pop_back();

            matched_expression_nodes_with_column_names.emplace_back(std::move(function_query_node), element_name);
        }

        return matched_expression_nodes_with_column_names;
    }

    /// Try to resolve qualified matcher for table expression

    IdentifierResolveSettings identifier_resolve_settings;
    identifier_resolve_settings.allow_to_check_cte = false;
    identifier_resolve_settings.allow_to_check_database_catalog = false;

    auto table_identifier_lookup = IdentifierLookup{matcher_node_typed.getQualifiedIdentifier(), IdentifierLookupContext::TABLE_EXPRESSION};
    auto table_identifier_resolve_result = tryResolveIdentifier(table_identifier_lookup, scope, identifier_resolve_settings);
    auto table_expression_node = table_identifier_resolve_result.resolved_identifier;

    if (!table_expression_node)
    {
        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
            "Qualified matcher {} does not find table. In scope {}",
            matcher_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    NamesAndTypes matched_columns;

    auto * table_expression_query_node = table_expression_node->as<QueryNode>();
    auto * table_expression_union_node = table_expression_node->as<UnionNode>();
    auto * table_expression_table_node = table_expression_node->as<TableNode>();
    auto * table_expression_table_function_node = table_expression_node->as<TableFunctionNode>();

    if (table_expression_query_node || table_expression_union_node)
    {
        matched_columns = table_expression_query_node ? table_expression_query_node->getProjectionColumns()
                                                              : table_expression_union_node->computeProjectionColumns();
    }
    else if (table_expression_table_node || table_expression_table_function_node)
    {
        const auto & storage_snapshot = table_expression_table_node ? table_expression_table_node->getStorageSnapshot()
                                                                    : table_expression_table_function_node->getStorageSnapshot();
        auto get_columns_options = buildGetColumnsOptions(matcher_node, scope.context);
        auto storage_columns_list = storage_snapshot->getColumns(get_columns_options);
        matched_columns = NamesAndTypes(storage_columns_list.begin(), storage_columns_list.end());
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid table expression node {}. In scope {}",
            table_expression_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    auto result_matched_column_nodes_with_names = getMatchedColumnNodesWithNames(matcher_node,
        table_expression_node,
        matched_columns,
        scope);

    updateMatchedColumnsFromJoinUsing(result_matched_column_nodes_with_names, table_expression_node, scope);

    return result_matched_column_nodes_with_names;
}

/// Resolve non qualified matcher, using scope join tree node.
QueryAnalyzer::QueryTreeNodesWithNames QueryAnalyzer::resolveUnqualifiedMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope)
{
    auto & matcher_node_typed = matcher_node->as<MatcherNode &>();
    assert(matcher_node_typed.isUnqualified());

    /** There can be edge case if matcher is inside lambda expression.
      * Try to find parent query expression using parent scopes.
      */
    auto * nearest_query_scope = scope.getNearestQueryScope();
    auto * nearest_query_scope_query_node = nearest_query_scope ? nearest_query_scope->scope_node->as<QueryNode>() : nullptr;

    /// If there are no parent query scope or query scope does not have join tree
    if (!nearest_query_scope_query_node || !nearest_query_scope_query_node->getJoinTree())
    {
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Unqualified matcher {} cannot be resolved. There are no table sources. In scope {}",
            matcher_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    }

    /** For unqualifited matcher resolve we build table expressions stack from JOIN tree and then process it.
      * For table, table function, query, union table expressions add matched columns into table expressions columns stack.
      * For array join continue processing.
      * For join node combine last left and right table expressions columns on stack together. It is important that if JOIN has USING
      * we must add USING columns before combining left and right table expressions columns. Columns from left and right table
      * expressions that have same names as columns in USING clause must be skipped.
      */

    auto table_expressions_stack = buildTableExpressionsStack(nearest_query_scope_query_node->getJoinTree());
    std::vector<QueryTreeNodesWithNames> table_expressions_column_nodes_with_names_stack;

    std::unordered_set<std::string> table_expression_column_names_to_skip;

    QueryTreeNodesWithNames result;

    if (matcher_node_typed.getMatcherType() == MatcherNodeType::COLUMNS_LIST)
    {
        auto identifiers = matcher_node_typed.getColumnsIdentifiers();
        result.reserve(identifiers.size());

        for (const auto & identifier : identifiers)
        {
            auto resolve_result = tryResolveIdentifier(IdentifierLookup{identifier, IdentifierLookupContext::EXPRESSION}, scope);
            if (!resolve_result.isResolved())
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                        "Unknown identifier '{}' inside COLUMNS matcher. In scope {}",
                        identifier.getFullName(), scope.dump());

            // TODO: Introduce IdentifierLookupContext::COLUMN and get rid of this check
            auto * resolved_column = resolve_result.resolved_identifier->as<ColumnNode>();
            if (!resolved_column)
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                        "Identifier '{}' inside COLUMNS matcher must resolve into a column, but got {}. In scope {}",
                        identifier.getFullName(),
                        resolve_result.resolved_identifier->getNodeTypeName(),
                        scope.scope_node->formatASTForErrorMessage());
            result.emplace_back(resolve_result.resolved_identifier, resolved_column->getColumnName());
        }
        return result;
    }

    result.resize(matcher_node_typed.getColumnsIdentifiers().size());

    for (auto & table_expression : table_expressions_stack)
    {
        bool table_expression_in_resolve_process = nearest_query_scope->table_expressions_in_resolve_process.contains(table_expression.get());

        if (auto * array_join_node = table_expression->as<ArrayJoinNode>())
        {
            if (table_expressions_column_nodes_with_names_stack.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected at least 1 table expressions on stack before ARRAY JOIN processing");

            if (table_expression_in_resolve_process)
                continue;

            auto & table_expression_column_nodes_with_names = table_expressions_column_nodes_with_names_stack.back();

            for (auto & [table_expression_column_node, _] : table_expression_column_nodes_with_names)
            {
                auto array_join_resolved_expression = identifier_resolver.tryResolveExpressionFromArrayJoinExpressions(table_expression_column_node,
                        table_expression,
                        scope);
                if (array_join_resolved_expression)
                    table_expression_column_node = std::move(array_join_resolved_expression);
            }

            continue;
        }

        auto * join_node = table_expression->as<JoinNode>();

        if (join_node)
        {
            size_t table_expressions_column_nodes_with_names_stack_size = table_expressions_column_nodes_with_names_stack.size();
            if (table_expressions_column_nodes_with_names_stack_size < 2)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected at least 2 table expressions on stack before JOIN processing. Actual {}",
                    table_expressions_column_nodes_with_names_stack_size);

            auto right_table_expression_columns = std::move(table_expressions_column_nodes_with_names_stack.back());
            table_expressions_column_nodes_with_names_stack.pop_back();

            auto left_table_expression_columns = std::move(table_expressions_column_nodes_with_names_stack.back());
            table_expressions_column_nodes_with_names_stack.pop_back();

            table_expression_column_names_to_skip.clear();

            QueryTreeNodesWithNames matched_expression_nodes_with_column_names;

            /** If there is JOIN with USING we need to match only single USING column and do not use left table expression
              * and right table expression column with same name.
              *
              * Example: SELECT id FROM test_table_1 AS t1 INNER JOIN test_table_2 AS t2 USING (id);
              */
            if (!table_expression_in_resolve_process && join_node->isUsingJoinExpression())
            {
                auto & join_using_list = join_node->getJoinExpression()->as<ListNode &>();

                for (auto & join_using_node : join_using_list.getNodes())
                {
                    auto & join_using_column_node = join_using_node->as<ColumnNode &>();
                    const auto & join_using_column_name = join_using_column_node.getColumnName();

                    if (!matcher_node_typed.isMatchingColumn(join_using_column_name))
                        continue;

                    const auto & join_using_column_nodes_list = join_using_column_node.getExpressionOrThrow()->as<ListNode &>();
                    const auto & join_using_column_nodes = join_using_column_nodes_list.getNodes();

                    /** If column doesn't exists in the table, then do not match column from USING clause.
                      * Example: SELECT a + 1 AS id, * FROM (SELECT 1 AS a) AS t1 JOIN (SELECT 2 AS id) AS t2 USING (id);
                      * In this case `id` is not present in the left table expression,
                      * so asterisk should return `id` from the right table expression.
                      */
                    auto is_column_from_parent_scope = [&scope](const QueryTreeNodePtr & using_node_from_table)
                    {
                        const auto & using_column_from_table = using_node_from_table->as<ColumnNode &>();
                        auto table_expression_data_it = scope.table_expression_node_to_data.find(using_column_from_table.getColumnSource());
                        if (table_expression_data_it != scope.table_expression_node_to_data.end())
                        {
                            const auto & table_expression_data = table_expression_data_it->second;
                            const auto & column_name = using_column_from_table.getColumnName();
                            return !table_expression_data.column_name_to_column_node.contains(column_name);
                        }
                        return false;
                    };

                    if (is_column_from_parent_scope(join_using_column_nodes.at(0)) ||
                        is_column_from_parent_scope(join_using_column_nodes.at(1)))
                        continue;

                    QueryTreeNodePtr matched_column_node;

                    if (isRight(join_node->getKind()))
                        matched_column_node = join_using_column_nodes.at(1);
                    else
                        matched_column_node = join_using_column_nodes.at(0);

                    matched_column_node = matched_column_node->clone();
                    matched_column_node->as<ColumnNode &>().setColumnType(join_using_column_node.getResultType());
                    if (!matched_column_node->isEqual(*join_using_column_nodes.at(0)))
                        scope.join_columns_with_changed_types[matched_column_node] = join_using_column_nodes.at(0);

                    table_expression_column_names_to_skip.insert(join_using_column_name);
                    matched_expression_nodes_with_column_names.emplace_back(std::move(matched_column_node), join_using_column_name);
                }
            }

            for (auto && left_table_column_with_name : left_table_expression_columns)
            {
                if (table_expression_column_names_to_skip.contains(left_table_column_with_name.second))
                    continue;

                matched_expression_nodes_with_column_names.push_back(std::move(left_table_column_with_name));
            }

            for (auto && right_table_column_with_name : right_table_expression_columns)
            {
                if (table_expression_column_names_to_skip.contains(right_table_column_with_name.second))
                    continue;

                matched_expression_nodes_with_column_names.push_back(std::move(right_table_column_with_name));
            }

            table_expressions_column_nodes_with_names_stack.push_back(std::move(matched_expression_nodes_with_column_names));
            continue;
        }

        if (table_expression_in_resolve_process)
        {
            table_expressions_column_nodes_with_names_stack.emplace_back();
            continue;
        }

        auto * table_node = table_expression->as<TableNode>();
        auto * table_function_node = table_expression->as<TableFunctionNode>();
        auto * query_node = table_expression->as<QueryNode>();
        auto * union_node = table_expression->as<UnionNode>();

        NamesAndTypes table_expression_columns;

        if (query_node || union_node)
        {
            table_expression_columns = query_node ? query_node->getProjectionColumns() : union_node->computeProjectionColumns();
        }
        else if (table_node || table_function_node)
        {
            const auto & storage_snapshot
                = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();
            auto get_columns_options = buildGetColumnsOptions(matcher_node, scope.context);
            auto storage_columns_list = storage_snapshot->getColumns(get_columns_options);
            table_expression_columns = NamesAndTypes(storage_columns_list.begin(), storage_columns_list.end());
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unqualified matcher {} resolve unexpected table expression. In scope {}",
                matcher_node_typed.formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }

        auto matched_column_nodes_with_names = getMatchedColumnNodesWithNames(matcher_node,
            table_expression,
            table_expression_columns,
            scope);

        table_expressions_column_nodes_with_names_stack.push_back(std::move(matched_column_nodes_with_names));
    }

    for (auto & table_expression_column_nodes_with_names : table_expressions_column_nodes_with_names_stack)
    {
        for (auto && table_expression_column_node_with_name : table_expression_column_nodes_with_names)
            result.push_back(std::move(table_expression_column_node_with_name));
    }

    return result;
}


/** Resolve query tree matcher. Check MatcherNode.h for detailed matcher description. Check ColumnTransformers.h for detailed transformers description.
  *
  * 1. Populate matched expression nodes resolving qualified or unqualified matcher.
  * 2. Apply column transformers to matched expression nodes. For strict column transformers save used column names.
  * 3. Validate strict column transformers.
  */
ProjectionNames QueryAnalyzer::resolveMatcher(QueryTreeNodePtr & matcher_node, IdentifierResolveScope & scope)
{
    auto & matcher_node_typed = matcher_node->as<MatcherNode &>();

    QueryTreeNodesWithNames matched_expression_nodes_with_names;

    if (matcher_node_typed.isQualified())
        matched_expression_nodes_with_names = resolveQualifiedMatcher(matcher_node, scope);
    else
        matched_expression_nodes_with_names = resolveUnqualifiedMatcher(matcher_node, scope);

    if (scope.join_use_nulls)
    {
        /** If we are resolving matcher came from the result of JOIN and `join_use_nulls` is set,
          * we need to convert joined column type to Nullable.
          * We are taking the nearest JoinNode to check to which table column belongs,
          * because for LEFT/RIGHT join, we convert only the corresponding side.
          */
        const auto * nearest_query_scope = scope.getNearestQueryScope();
        const QueryNode * nearest_scope_query_node = nearest_query_scope ? nearest_query_scope->scope_node->as<QueryNode>() : nullptr;
        const QueryTreeNodePtr & nearest_scope_join_tree = nearest_scope_query_node ? nearest_scope_query_node->getJoinTree() : nullptr;
        const JoinNode * nearest_scope_join_node = nearest_scope_join_tree ? nearest_scope_join_tree->as<JoinNode>() : nullptr;
        if (nearest_scope_join_node)
        {
            for (auto & [node, node_name] : matched_expression_nodes_with_names)
            {
                auto join_identifier_side = getColumnSideFromJoinTree(node, *nearest_scope_join_node);
                auto projection_name_it = node_to_projection_name.find(node);
                auto nullable_node = IdentifierResolver::convertJoinedColumnTypeToNullIfNeeded(node, nearest_scope_join_node->getKind(), join_identifier_side, scope);
                if (nullable_node)
                {
                    node = nullable_node;
                    /// Set the same projection name for new nullable node
                    if (projection_name_it != node_to_projection_name.end())
                    {
                        node_to_projection_name.emplace(node, projection_name_it->second);
                    }
                }
            }
        }
    }

    if (!scope.expressions_in_resolve_process_stack.hasAggregateFunction())
    {
        for (auto & [node, _] : matched_expression_nodes_with_names)
        {
            auto it = scope.nullable_group_by_keys.find(node);
            if (it != scope.nullable_group_by_keys.end())
            {
                node = it->node->clone();
                node->convertToNullable();
            }
        }
    }

    std::unordered_map<const IColumnTransformerNode *, std::unordered_set<std::string>> strict_transformer_to_used_column_names;
    for (const auto & transformer : matcher_node_typed.getColumnTransformers().getNodes())
    {
        auto * except_transformer = transformer->as<ExceptColumnTransformerNode>();
        auto * replace_transformer = transformer->as<ReplaceColumnTransformerNode>();

        if (except_transformer && except_transformer->isStrict())
            strict_transformer_to_used_column_names.emplace(except_transformer, std::unordered_set<std::string>());
        else if (replace_transformer && replace_transformer->isStrict())
            strict_transformer_to_used_column_names.emplace(replace_transformer, std::unordered_set<std::string>());
    }

    ListNodePtr list = std::make_shared<ListNode>();
    ProjectionNames result_projection_names;
    ProjectionNames node_projection_names;

    for (auto & [node, column_name] : matched_expression_nodes_with_names)
    {
        bool apply_transformer_was_used = false;
        bool replace_transformer_was_used = false;
        bool execute_apply_transformer = false;
        bool execute_replace_transformer = false;

        auto projection_name_it = node_to_projection_name.find(node);
        if (projection_name_it != node_to_projection_name.end())
            result_projection_names.push_back(projection_name_it->second);
        else
            result_projection_names.push_back(column_name);

        for (const auto & transformer : matcher_node_typed.getColumnTransformers().getNodes())
        {
            if (auto * apply_transformer = transformer->as<ApplyColumnTransformerNode>())
            {
                const auto & expression_node = apply_transformer->getExpressionNode();
                apply_transformer_was_used = true;

                if (apply_transformer->getApplyTransformerType() == ApplyColumnTransformerType::LAMBDA)
                {
                    auto lambda_expression_to_resolve = expression_node->clone();
                    IdentifierResolveScope lambda_scope(expression_node, &scope /*parent_scope*/);
                    node_projection_names = resolveLambda(expression_node, lambda_expression_to_resolve, {node}, lambda_scope);
                    auto & lambda_expression_to_resolve_typed = lambda_expression_to_resolve->as<LambdaNode &>();
                    node = lambda_expression_to_resolve_typed.getExpression();
                }
                else if (apply_transformer->getApplyTransformerType() == ApplyColumnTransformerType::FUNCTION)
                {
                    auto function_to_resolve_untyped = expression_node->clone();
                    auto & function_to_resolve_typed = function_to_resolve_untyped->as<FunctionNode &>();
                    function_to_resolve_typed.getArguments().getNodes().push_back(node);
                    node_projection_names = resolveFunction(function_to_resolve_untyped, scope);
                    node = function_to_resolve_untyped;
                }
                else
                {
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Unsupported apply matcher expression type. Expected lambda or function apply transformer. Actual {}. In scope {}",
                        transformer->formatASTForErrorMessage(),
                        scope.scope_node->formatASTForErrorMessage());
                }

                execute_apply_transformer = true;
            }
            else if (auto * except_transformer = transformer->as<ExceptColumnTransformerNode>())
            {
                if (apply_transformer_was_used || replace_transformer_was_used)
                    continue;

                if (except_transformer->isColumnMatching(column_name))
                {
                    if (except_transformer->isStrict())
                        strict_transformer_to_used_column_names[except_transformer].insert(column_name);

                    node = {};
                    break;
                }
            }
            else if (auto * replace_transformer = transformer->as<ReplaceColumnTransformerNode>())
            {
                if (apply_transformer_was_used || replace_transformer_was_used)
                    continue;

                auto replace_expression = replace_transformer->findReplacementExpression(column_name);
                if (!replace_expression)
                    continue;

                replace_transformer_was_used = true;

                if (replace_transformer->isStrict())
                    strict_transformer_to_used_column_names[replace_transformer].insert(column_name);

                node = replace_expression->clone();
                node_projection_names = resolveExpressionNode(node, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

                /** If replace expression resolved as single node, we want to use replace column name as result projection name, instead
                  * of using replace expression projection name.
                  *
                  * Example: SELECT * REPLACE id + 5 AS id FROM test_table;
                  */
                if (node_projection_names.size() == 1)
                    node_projection_names[0] = column_name;

                execute_replace_transformer = true;
            }

            if (execute_apply_transformer || execute_replace_transformer)
            {
                if (auto * node_list = node->as<ListNode>())
                {
                    auto & node_list_nodes = node_list->getNodes();
                    size_t node_list_nodes_size = node_list_nodes.size();

                    if (node_list_nodes_size != 1)
                        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "{} transformer {} resolved as list node with size {}. Expected 1. In scope {}",
                            execute_apply_transformer ? "APPLY" : "REPLACE",
                            transformer->formatASTForErrorMessage(),
                            node_list_nodes_size,
                            scope.scope_node->formatASTForErrorMessage());

                    node = node_list_nodes[0];
                }

                if (node_projection_names.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Matcher node expected 1 projection name. Actual {}", node_projection_names.size());

                result_projection_names.back() = std::move(node_projection_names[0]);
                node_to_projection_name.emplace(node, result_projection_names.back());
                node_projection_names.clear();
            }
        }

        if (node)
            list->getNodes().push_back(node);
        else
            result_projection_names.pop_back();
    }

    for (auto & [strict_transformer, used_column_names] : strict_transformer_to_used_column_names)
    {
        auto strict_transformer_type = strict_transformer->getTransformerType();
        const Names * strict_transformer_column_names = nullptr;

        switch (strict_transformer_type)
        {
            case ColumnTransfomerType::EXCEPT:
            {
                const auto * except_transformer = static_cast<const ExceptColumnTransformerNode *>(strict_transformer);
                const auto & except_names = except_transformer->getExceptColumnNames();

                if (except_names.size() != used_column_names.size())
                    strict_transformer_column_names = &except_transformer->getExceptColumnNames();

                break;
            }
            case ColumnTransfomerType::REPLACE:
            {
                const auto * replace_transformer = static_cast<const ReplaceColumnTransformerNode *>(strict_transformer);
                const auto & replacement_names = replace_transformer->getReplacementsNames();

                if (replacement_names.size() != used_column_names.size())
                    strict_transformer_column_names = &replace_transformer->getReplacementsNames();

                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Expected strict EXCEPT or REPLACE column transformer. Actual type {}. In scope {}",
                    toString(strict_transformer_type),
                    scope.scope_node->formatASTForErrorMessage());
            }
        }

        if (!strict_transformer_column_names)
            continue;

        Names non_matched_column_names;
        size_t strict_transformer_column_names_size = strict_transformer_column_names->size();
        for (size_t i = 0; i < strict_transformer_column_names_size; ++i)
        {
            const auto & column_name = (*strict_transformer_column_names)[i];
            if (used_column_names.find(column_name) == used_column_names.end())
                non_matched_column_names.push_back(column_name);
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Strict {} column transformer {} expects following column(s) : {}. In scope {}",
            toString(strict_transformer_type),
            strict_transformer->formatASTForErrorMessage(),
            fmt::join(non_matched_column_names, ", "),
            scope.scope_node->formatASTForErrorMessage());
    }

    auto original_ast = matcher_node->getOriginalAST();
    matcher_node = std::move(list);
    if (original_ast)
        matcher_node->setOriginalAST(original_ast);

    return result_projection_names;
}

/** Resolve window function window node.
  *
  * Node can be identifier or window node.
  * Example: SELECT count(*) OVER w FROM test_table WINDOW w AS (PARTITION BY id);
  * Example: SELECT count(*) OVER (PARTITION BY id);
  *
  * If node has parent window name specified, then parent window definition is searched in nearest query scope WINDOW section.
  * If node is identifier, than node is replaced with window definition.
  * If node is window, that window node is merged with parent window node.
  *
  * Window node PARTITION BY and ORDER BY parts are resolved.
  * If window node has frame begin OFFSET or frame end OFFSET specified, they are resolved, and window node frame constants are updated.
  * Window node frame is validated.
  */
ProjectionName QueryAnalyzer::resolveWindow(QueryTreeNodePtr & node, IdentifierResolveScope & scope)
{
    std::string parent_window_name;
    auto * identifier_node = node->as<IdentifierNode>();

    ProjectionName result_projection_name;
    QueryTreeNodePtr parent_window_node;

    if (identifier_node)
        parent_window_name = identifier_node->getIdentifier().getFullName();
    else if (auto * window_node = node->as<WindowNode>())
        parent_window_name = window_node->getParentWindowName();

    if (!parent_window_name.empty())
    {
        auto * nearest_query_scope = scope.getNearestQueryScope();

        if (!nearest_query_scope)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Window '{}' does not exist.", parent_window_name);

        auto & scope_window_name_to_window_node = nearest_query_scope->window_name_to_window_node;

        auto window_node_it = scope_window_name_to_window_node.find(parent_window_name);
        if (window_node_it == scope_window_name_to_window_node.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Window '{}' does not exist. In scope {}",
                parent_window_name,
                nearest_query_scope->scope_node->formatASTForErrorMessage());

        parent_window_node = window_node_it->second;

        if (identifier_node)
        {
            node = parent_window_node->clone();
            result_projection_name = parent_window_name;
        }
        else
        {
            mergeWindowWithParentWindow(node, parent_window_node, scope);
        }
    }

    auto & window_node = node->as<WindowNode &>();
    window_node.setParentWindowName({});

    ProjectionNames partition_by_projection_names = resolveExpressionNodeList(window_node.getPartitionByNode(),
        scope,
        false /*allow_lambda_expression*/,
        false /*allow_table_expression*/);

    ProjectionNames order_by_projection_names = resolveSortNodeList(window_node.getOrderByNode(), scope);

    ProjectionNames frame_begin_offset_projection_names;
    ProjectionNames frame_end_offset_projection_names;

    if (window_node.hasFrameBeginOffset())
    {
        frame_begin_offset_projection_names = resolveExpressionNode(window_node.getFrameBeginOffsetNode(),
            scope,
            false /*allow_lambda_expression*/,
            false /*allow_table_expression*/);

        const auto * window_frame_begin_constant_node = window_node.getFrameBeginOffsetNode()->as<ConstantNode>();
        if (!window_frame_begin_constant_node || !isNativeNumber(removeNullable(window_frame_begin_constant_node->getResultType())))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Window frame begin OFFSET expression must be constant with numeric type. Actual {}. In scope {}",
                window_node.getFrameBeginOffsetNode()->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());

        window_node.getWindowFrame().begin_offset = window_frame_begin_constant_node->getValue();
        if (frame_begin_offset_projection_names.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Window FRAME begin offset expected 1 projection name. Actual {}",
                frame_begin_offset_projection_names.size());
    }

    if (window_node.hasFrameEndOffset())
    {
        frame_end_offset_projection_names = resolveExpressionNode(window_node.getFrameEndOffsetNode(),
            scope,
            false /*allow_lambda_expression*/,
            false /*allow_table_expression*/);

        const auto * window_frame_end_constant_node = window_node.getFrameEndOffsetNode()->as<ConstantNode>();
        if (!window_frame_end_constant_node || !isNativeNumber(removeNullable(window_frame_end_constant_node->getResultType())))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Window frame begin OFFSET expression must be constant with numeric type. Actual {}. In scope {}",
                window_node.getFrameEndOffsetNode()->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());

        window_node.getWindowFrame().end_offset = window_frame_end_constant_node->getValue();
        if (frame_end_offset_projection_names.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Window FRAME begin offset expected 1 projection name. Actual {}",
                frame_end_offset_projection_names.size());
    }

    window_node.getWindowFrame().checkValid();

    if (result_projection_name.empty())
    {
        result_projection_name = calculateWindowProjectionName(node,
            parent_window_node,
            parent_window_name,
            partition_by_projection_names,
            order_by_projection_names,
            frame_begin_offset_projection_names.empty() ? "" : frame_begin_offset_projection_names.front(),
            frame_end_offset_projection_names.empty() ? "" : frame_end_offset_projection_names.front());
    }

    return result_projection_name;
}

/** Resolve lambda function.
  * This function modified lambda_node during resolve. It is caller responsibility to clone lambda before resolve
  * if it is needed for later use.
  *
  * Lambda body expression result projection names is used as lambda projection names.
  *
  * Lambda expression can be resolved into list node. It is caller responsibility to handle it properly.
  *
  * lambda_node - node that must have LambdaNode type.
  * lambda_node_to_resolve - lambda node to resolve that must have LambdaNode type.
  * arguments - lambda arguments.
  * scope - lambda scope. It is client responsibility to create it.
  *
  * Resolve steps:
  * 1. Validate arguments.
  * 2. Register lambda node in lambdas in resolve process. This is necessary to prevent recursive lambda resolving.
  * 3. Initialize scope with lambda aliases.
  * 4. Validate lambda argument names, and scope expressions.
  * 5. Resolve lambda body expression.
  * 6. Deregister lambda node from lambdas in resolve process.
  */
ProjectionNames QueryAnalyzer::resolveLambda(const QueryTreeNodePtr & lambda_node,
    const QueryTreeNodePtr & lambda_node_to_resolve,
    const QueryTreeNodes & lambda_arguments,
    IdentifierResolveScope & scope)
{
    auto & lambda_to_resolve = lambda_node_to_resolve->as<LambdaNode &>();
    auto & lambda_arguments_nodes = lambda_to_resolve.getArguments().getNodes();
    size_t lambda_arguments_nodes_size = lambda_arguments_nodes.size();

    /** Register lambda as being resolved, to prevent recursive lambdas resolution.
      * Example: WITH (x -> x + lambda_2(x)) AS lambda_1, (x -> x + lambda_1(x)) AS lambda_2 SELECT 1;
      */
    auto it = lambdas_in_resolve_process.find(lambda_node.get());
    if (it != lambdas_in_resolve_process.end())
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Recursive lambda {}. In scope {}",
            lambda_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());
    lambdas_in_resolve_process.emplace(lambda_node.get());

    size_t arguments_size = lambda_arguments.size();
    if (lambda_arguments_nodes_size != arguments_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Lambda {} expect {} arguments. Actual {}. In scope {}",
            lambda_to_resolve.formatASTForErrorMessage(),
            lambda_arguments_nodes_size,
            arguments_size,
            scope.scope_node->formatASTForErrorMessage());

    /// Initialize aliases in lambda scope
    QueryExpressionsAliasVisitor visitor(scope.aliases);
    visitor.visit(lambda_to_resolve.getExpression());

    /** Replace lambda arguments with new arguments.
      * Additionally validate that there are no aliases with same name as lambda arguments.
      * Arguments are registered in current scope expression_argument_name_to_node map.
      */
    QueryTreeNodes lambda_new_arguments_nodes;
    lambda_new_arguments_nodes.reserve(lambda_arguments_nodes_size);

    for (size_t i = 0; i < lambda_arguments_nodes_size; ++i)
    {
        auto & lambda_argument_node = lambda_arguments_nodes[i];
        const auto * lambda_argument_identifier = lambda_argument_node->as<IdentifierNode>();
        const auto * lambda_argument_column = lambda_argument_node->as<ColumnNode>();
        if (!lambda_argument_identifier && !lambda_argument_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected IDENTIFIER or COLUMN as lambda argument, got {}", lambda_node->dumpTree());
        const auto & lambda_argument_name = lambda_argument_identifier ? lambda_argument_identifier->getIdentifier().getFullName()
                                                                       : lambda_argument_column->getColumnName();

        bool has_expression_node = scope.aliases.alias_name_to_expression_node->contains(lambda_argument_name);
        bool has_alias_node = scope.aliases.alias_name_to_lambda_node.contains(lambda_argument_name);

        if (has_expression_node || has_alias_node)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Alias name '{}' inside lambda {} cannot have same name as lambda argument. In scope {}",
                lambda_argument_name,
                lambda_argument_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }

        scope.expression_argument_name_to_node.emplace(lambda_argument_name, lambda_arguments[i]);
        lambda_new_arguments_nodes.push_back(lambda_arguments[i]);
    }

    lambda_to_resolve.getArguments().getNodes() = std::move(lambda_new_arguments_nodes);

    /// Lambda body expression is resolved as standard query expression node.
    auto result_projection_names = resolveExpressionNode(lambda_to_resolve.getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    lambdas_in_resolve_process.erase(lambda_node.get());

    return result_projection_names;
}

namespace
{
void checkFunctionNodeHasEmptyNullsAction(FunctionNode const & node)
{
    if (node.getNullsAction() != NullsAction::EMPTY)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Function with name {} cannot use {} NULLS",
            backQuote(node.getFunctionName()),
            node.getNullsAction() == NullsAction::IGNORE_NULLS ? "IGNORE" : "RESPECT");
}
}

/** Resolve function node in scope.
  * During function node resolve, function node can be replaced with another expression (if it match lambda or sql user defined function),
  * with constant (if it allow constant folding), or with expression list. It is caller responsibility to handle such cases appropriately.
  *
  * Steps:
  * 1. Resolve function parameters. Validate that each function parameter must be constant node.
  * 2. Try to lookup function as lambda in current scope. If it is lambda we can skip `in` and `count` special handling.
  * 3. If function is count function, that take unqualified ASTERISK matcher, remove it from its arguments. Example: SELECT count(*) FROM test_table;
  * 4. If function is `IN` function, then right part of `IN` function is replaced as subquery.
  * 5. Resolve function arguments list, lambda expressions are allowed as function arguments.
  * For `IN` function table expressions are allowed as function arguments.
  * 6. Initialize argument_columns, argument_types, function_lambda_arguments_indexes arrays from function arguments.
  * 7. If function name identifier was not resolved as function in current scope, try to lookup lambda from sql user defined functions factory.
  * 8. If function was resolve as lambda from step 2 or 7, then resolve lambda using function arguments and replace function node with lambda result.
  * After than function node is resolved.
  * 9. If function was not resolved during step 6 as lambda, then try to resolve function as window function or executable user defined function
  * or ordinary function or aggregate function.
  *
  * If function is resolved as window function or executable user defined function or aggregate function, function node is resolved
  * no additional special handling is required.
  *
  * 8. If function was resolved as non aggregate function. Then if some of function arguments are lambda expressions, their result types need to be initialized and
  * they must be resolved.
  * 9. If function is suitable for constant folding, try to perform constant folding for function node.
  */
ProjectionNames QueryAnalyzer::resolveFunction(QueryTreeNodePtr & node, IdentifierResolveScope & scope)
{
    FunctionNodePtr function_node_ptr = std::static_pointer_cast<FunctionNode>(node);
    auto function_name = function_node_ptr->getFunctionName();

    /// Resolve function parameters

    auto parameters_projection_names = resolveExpressionNodeList(function_node_ptr->getParametersNode(),
        scope,
        false /*allow_lambda_expression*/,
        false /*allow_table_expression*/);

    /// Convert function parameters into constant parameters array

    Array parameters;

    auto & parameters_nodes = function_node_ptr->getParameters().getNodes();
    parameters.reserve(parameters_nodes.size());

    for (auto & parameter_node : parameters_nodes)
    {
        const auto * constant_node = parameter_node->as<ConstantNode>();
        if (!constant_node)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Parameter for function '{}' expected to have constant value. Actual {}. In scope {}",
            function_name,
            parameter_node->formatASTForErrorMessage(),
            scope.scope_node->formatASTForErrorMessage());

        parameters.push_back(constant_node->getValue());
    }

    //// If function node is not window function try to lookup function node name as lambda identifier.
    QueryTreeNodePtr lambda_expression_untyped;
    if (!function_node_ptr->isWindowFunction())
    {
        auto function_lookup_result = tryResolveIdentifier({Identifier{function_name}, IdentifierLookupContext::FUNCTION}, scope);
        lambda_expression_untyped = function_lookup_result.resolved_identifier;
    }

    bool is_special_function_in = false;
    bool is_special_function_dict_get = false;
    bool is_special_function_join_get = false;
    bool is_special_function_exists = false;
    bool is_special_function_if = false;

    if (!lambda_expression_untyped)
    {
        is_special_function_in = isNameOfInFunction(function_name);
        is_special_function_dict_get = functionIsDictGet(function_name);
        is_special_function_join_get = functionIsJoinGet(function_name);
        is_special_function_exists = function_name == "exists";
        is_special_function_if = function_name == "if";

        auto function_name_lowercase = Poco::toLower(function_name);

        /** Special handling for count and countState functions.
          *
          * Example: SELECT count(*) FROM test_table
          * Example: SELECT countState(*) FROM test_table;
          */
        if (function_node_ptr->getArguments().getNodes().size() == 1 &&
            (function_name_lowercase == "count" || function_name_lowercase == "countstate"))
        {
            auto * matcher_node = function_node_ptr->getArguments().getNodes().front()->as<MatcherNode>();
            if (matcher_node && matcher_node->isUnqualified())
                function_node_ptr->getArguments().getNodes().clear();
        }
    }

    /** Special functions dictGet and its variations and joinGet can be executed when first argument is identifier.
      * Example: SELECT dictGet(identifier, 'value', toUInt64(0));
      *
      * Try to resolve identifier as expression identifier and if it is resolved use it.
      * Example: WITH 'dict_name' AS identifier SELECT dictGet(identifier, 'value', toUInt64(0));
      *
      * Otherwise replace identifier with identifier full name constant.
      * Validation that dictionary exists or table exists will be performed during function `getReturnType` method call.
      */
    if ((is_special_function_dict_get || is_special_function_join_get) &&
        !function_node_ptr->getArguments().getNodes().empty() &&
        function_node_ptr->getArguments().getNodes()[0]->getNodeType() == QueryTreeNodeType::IDENTIFIER)
    {
        auto & first_argument = function_node_ptr->getArguments().getNodes()[0];
        auto & first_argument_identifier = first_argument->as<IdentifierNode &>();
        auto identifier = first_argument_identifier.getIdentifier();

        IdentifierLookup identifier_lookup{identifier, IdentifierLookupContext::EXPRESSION};
        auto resolve_result = tryResolveIdentifier(identifier_lookup, scope);

        if (resolve_result.isResolved())
        {
            first_argument = std::move(resolve_result.resolved_identifier);
        }
        else
        {
            size_t parts_size = identifier.getPartsSize();
            if (parts_size < 1 || parts_size > 2)
                throw Exception(ErrorCodes::INVALID_IDENTIFIER,
                    "Expected {} function first argument identifier to contain 1 or 2 parts. Actual '{}'. In scope {}",
                    function_name,
                    identifier.getFullName(),
                    scope.scope_node->formatASTForErrorMessage());

            if (is_special_function_dict_get)
            {
                scope.context->getExternalDictionariesLoader().assertDictionaryStructureExists(identifier.getFullName(), scope.context);
            }
            else
            {
                auto table_node = IdentifierResolver::tryResolveTableIdentifierFromDatabaseCatalog(identifier, scope.context);
                if (!table_node)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} first argument expected table identifier '{}'. In scope {}",
                        function_name,
                        identifier.getFullName(),
                        scope.scope_node->formatASTForErrorMessage());

                auto & table_node_typed = table_node->as<TableNode &>();
                if (!std::dynamic_pointer_cast<StorageJoin>(table_node_typed.getStorage()))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} table '{}' should have engine StorageJoin. In scope {}",
                        function_name,
                        identifier.getFullName(),
                        scope.scope_node->formatASTForErrorMessage());
            }

            first_argument = std::make_shared<ConstantNode>(identifier.getFullName());
        }
    }

    if (is_special_function_exists)
    {
        checkFunctionNodeHasEmptyNullsAction(*function_node_ptr);
        /// Rewrite EXISTS (subquery) into 1 IN (SELECT 1 FROM (subquery) LIMIT 1).
        auto & exists_subquery_argument = function_node_ptr->getArguments().getNodes().at(0);

        auto constant_data_type = std::make_shared<DataTypeUInt64>();

        auto in_subquery = std::make_shared<QueryNode>(Context::createCopy(scope.context));
        in_subquery->setIsSubquery(true);
        in_subquery->getProjection().getNodes().push_back(std::make_shared<ConstantNode>(1UL, constant_data_type));
        in_subquery->getJoinTree() = exists_subquery_argument;
        in_subquery->getLimit() = std::make_shared<ConstantNode>(1UL, constant_data_type);

        function_node_ptr = std::make_shared<FunctionNode>("in");
        function_node_ptr->getArguments().getNodes() = {std::make_shared<ConstantNode>(1UL, constant_data_type), in_subquery};
        node = function_node_ptr;
        function_name = "in";
        is_special_function_in = true;
    }

    if (is_special_function_if && !function_node_ptr->getArguments().getNodes().empty())
    {
        checkFunctionNodeHasEmptyNullsAction(*function_node_ptr);
        /** Handle special case with constant If function, even if some of the arguments are invalid.
          *
          * SELECT if(hasColumnInTable('system', 'numbers', 'not_existing_column'), not_existing_column, 5) FROM system.numbers;
          */
        auto & if_function_arguments = function_node_ptr->getArguments().getNodes();
        auto if_function_condition = if_function_arguments[0];
        resolveExpressionNode(if_function_condition, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

        auto constant_condition = tryExtractConstantFromConditionNode(if_function_condition);

        if (constant_condition.has_value() && if_function_arguments.size() == 3)
        {
            QueryTreeNodePtr constant_if_result_node;
            QueryTreeNodePtr possibly_invalid_argument_node;

            if (*constant_condition)
            {
                possibly_invalid_argument_node = if_function_arguments[2];
                constant_if_result_node = if_function_arguments[1];
            }
            else
            {
                possibly_invalid_argument_node = if_function_arguments[1];
                constant_if_result_node = if_function_arguments[2];
            }

            bool apply_constant_if_optimization = false;

            try
            {
                resolveExpressionNode(possibly_invalid_argument_node,
                    scope,
                    false /*allow_lambda_expression*/,
                    false /*allow_table_expression*/);
            }
            catch (...)
            {
                apply_constant_if_optimization = true;
            }

            if (apply_constant_if_optimization)
            {
                auto result_projection_names = resolveExpressionNode(constant_if_result_node,
                    scope,
                    false /*allow_lambda_expression*/,
                    false /*allow_table_expression*/);
                node = std::move(constant_if_result_node);
                return result_projection_names;
            }
        }
    }

    /// Resolve function arguments
    bool allow_table_expressions = is_special_function_in;
    auto arguments_projection_names = resolveExpressionNodeList(function_node_ptr->getArgumentsNode(),
        scope,
        true /*allow_lambda_expression*/,
        allow_table_expressions /*allow_table_expression*/);

    /// Mask arguments if needed
    if (!scope.context->getSettingsRef()[Setting::format_display_secrets_in_show_and_select])
    {
        if (FunctionSecretArgumentsFinder::Result secret_arguments = FunctionSecretArgumentsFinderTreeNode(*function_node_ptr).getResult(); secret_arguments.count)
        {
            auto & argument_nodes = function_node_ptr->getArgumentsNode()->as<ListNode &>().getNodes();

            for (size_t n = secret_arguments.start; n < secret_arguments.start + secret_arguments.count; ++n)
            {
                if (auto * constant = argument_nodes[n]->as<ConstantNode>())
                {
                    auto mask = scope.projection_mask_map->insert({constant->getTreeHash(), scope.projection_mask_map->size() + 1}).first->second;
                    constant->setMaskId(mask);
                    arguments_projection_names[n] = "[HIDDEN id: " + std::to_string(mask) + "]";
                }
            }
        }
    }

    auto & function_node = *function_node_ptr;

    /// Replace right IN function argument if it is table or table function with subquery that read ordinary columns
    if (is_special_function_in)
    {
        checkFunctionNodeHasEmptyNullsAction(function_node);
        if (scope.context->getSettingsRef()[Setting::transform_null_in])
        {
            static constexpr std::array<std::pair<std::string_view, std::string_view>, 4> in_function_to_replace_null_in_function_map =
            {{
                {"in", "nullIn"},
                {"notIn", "notNullIn"},
                {"globalIn", "globalNullIn"},
                {"globalNotIn", "globalNotNullIn"},
            }};

            for (const auto & [in_function_name, in_function_name_to_replace] : in_function_to_replace_null_in_function_map)
            {
                if (function_name == in_function_name)
                {
                    function_name = in_function_name_to_replace;
                    break;
                }
            }
        }

        auto & function_in_arguments_nodes = function_node.getArguments().getNodes();
        if (function_in_arguments_nodes.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function '{}' expects 2 arguments", function_name);

        auto & in_second_argument = function_in_arguments_nodes[1];
        auto * table_node = in_second_argument->as<TableNode>();
        auto * table_function_node = in_second_argument->as<TableFunctionNode>();

        if (table_node)
        {
            /// If table is already prepared set, we do not replace it with subquery.
            /// If table is not a StorageSet, we'll create plan to build set in the Planner.
        }
        else if (table_function_node)
        {
            const auto & storage_snapshot = table_function_node->getStorageSnapshot();
            auto columns_to_select = storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::Ordinary));

            size_t columns_to_select_size = columns_to_select.size();

            auto column_nodes_to_select = std::make_shared<ListNode>();
            column_nodes_to_select->getNodes().reserve(columns_to_select_size);

            NamesAndTypes projection_columns;
            projection_columns.reserve(columns_to_select_size);

            for (auto & column : columns_to_select)
            {
                column_nodes_to_select->getNodes().emplace_back(std::make_shared<ColumnNode>(column, in_second_argument));
                projection_columns.emplace_back(column.name, column.type);
            }

            auto in_second_argument_query_node = std::make_shared<QueryNode>(Context::createCopy(scope.context));
            in_second_argument_query_node->setIsSubquery(true);
            in_second_argument_query_node->getProjectionNode() = std::move(column_nodes_to_select);
            in_second_argument_query_node->getJoinTree() = std::move(in_second_argument);
            in_second_argument_query_node->resolveProjectionColumns(std::move(projection_columns));

            in_second_argument = std::move(in_second_argument_query_node);
        }
        else
        {
            /// Replace storage with values storage of insertion block
            if (StoragePtr storage = scope.context->getViewSource())
            {
                QueryTreeNodePtr table_expression;
                /// Process possibly nested sub-selects
                for (auto * query_node = in_second_argument->as<QueryNode>(); query_node; query_node = table_expression->as<QueryNode>())
                    table_expression = extractLeftTableExpression(query_node->getJoinTree());

                if (table_expression)
                {
                    if (auto * query_table_node = table_expression->as<TableNode>())
                    {
                        if (query_table_node->getStorageID().getFullNameNotQuoted() == storage->getStorageID().getFullNameNotQuoted())
                        {
                            auto replacement_table_expression = std::make_shared<TableNode>(storage, scope.context);
                            if (std::optional<TableExpressionModifiers> table_expression_modifiers = query_table_node->getTableExpressionModifiers())
                                replacement_table_expression->setTableExpressionModifiers(*table_expression_modifiers);
                            in_second_argument = in_second_argument->cloneAndReplace(table_expression, std::move(replacement_table_expression));
                        }
                    }
                }
            }

            resolveExpressionNode(in_second_argument, scope, false /*allow_lambda_expression*/, true /*allow_table_expression*/);
        }

        /// Edge case when the first argument of IN is scalar subquery.
        auto & in_first_argument = function_in_arguments_nodes[0];
        auto first_argument_type = in_first_argument->getNodeType();
        if (first_argument_type == QueryTreeNodeType::QUERY || first_argument_type == QueryTreeNodeType::UNION)
        {
            IdentifierResolveScope subquery_scope(in_first_argument, &scope /*parent_scope*/);
            subquery_scope.subquery_depth = scope.subquery_depth + 1;

            evaluateScalarSubqueryIfNeeded(in_first_argument, subquery_scope);
        }
    }

    /// Initialize function argument columns

    ColumnsWithTypeAndName argument_columns;
    DataTypes argument_types;
    bool all_arguments_constants = true;
    std::vector<size_t> function_lambda_arguments_indexes;

    auto & function_arguments = function_node.getArguments().getNodes();
    size_t function_arguments_size = function_arguments.size();

    for (size_t function_argument_index = 0; function_argument_index < function_arguments_size; ++function_argument_index)
    {
        auto & function_argument = function_arguments[function_argument_index];

        ColumnWithTypeAndName argument_column;
        argument_column.name = arguments_projection_names[function_argument_index];

        /** If function argument is lambda, save lambda argument index and initialize argument type as DataTypeFunction
          * where function argument types are initialized with empty array of lambda arguments size.
          */
        if (const auto * lambda_node = function_argument->as<const LambdaNode>())
        {
            size_t lambda_arguments_size = lambda_node->getArguments().getNodes().size();
            argument_column.type = std::make_shared<DataTypeFunction>(DataTypes(lambda_arguments_size, nullptr), nullptr);
            function_lambda_arguments_indexes.push_back(function_argument_index);
        }
        else if (is_special_function_in && function_argument_index == 1)
        {
            argument_column.type = std::make_shared<DataTypeSet>();
        }
        else
        {
            argument_column.type = function_argument->getResultType();
        }

        if (!argument_column.type)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Function '{}' argument is not resolved. In scope {}",
                function_name,
                scope.scope_node->formatASTForErrorMessage());

        bool argument_is_constant = false;
        const auto * constant_node = function_argument->as<ConstantNode>();
        if (constant_node)
        {
            argument_column.column = constant_node->getResultType()->createColumnConst(1, constant_node->getValue());
            argument_column.type = constant_node->getResultType();
            argument_is_constant = true;
        }
        else if (const auto * get_scalar_function_node = function_argument->as<FunctionNode>();
                get_scalar_function_node && get_scalar_function_node->getFunctionName() == "__getScalar")
        {
            /// Allow constant folding through getScalar
            const auto * get_scalar_const_arg = get_scalar_function_node->getArguments().getNodes().at(0)->as<ConstantNode>();
            if (get_scalar_const_arg && scope.context->hasQueryContext())
            {
                auto query_context = scope.context->getQueryContext();
                auto scalar_string = toString(get_scalar_const_arg->getValue());
                if (query_context->hasScalar(scalar_string))
                {
                    auto scalar = query_context->getScalar(scalar_string);
                    argument_column.column = ColumnConst::create(scalar.getByPosition(0).column, 1);
                    argument_column.type = get_scalar_function_node->getResultType();
                    argument_is_constant = true;
                }
            }
        }

        all_arguments_constants &= argument_is_constant;

        argument_types.push_back(argument_column.type);
        argument_columns.emplace_back(std::move(argument_column));
    }

    /// Calculate function projection name
    ProjectionNames result_projection_names = { calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names) };

    /** Try to resolve function as
      * 1. Lambda function in current scope. Example: WITH (x -> x + 1) AS lambda SELECT lambda(1);
      * 2. Lambda function from sql user defined functions.
      * 3. Special `untuple` function.
      * 4. Special `grouping` function.
      * 5. Window function.
      * 6. Executable user defined function.
      * 7. Ordinary function.
      * 8. Aggregate function.
      *
      * TODO: Provide better error hints.
      */
    if (!function_node.isWindowFunction())
    {
        if (!lambda_expression_untyped)
            lambda_expression_untyped = tryGetLambdaFromSQLUserDefinedFunctions(function_node.getFunctionName(), scope.context);

        /** If function is resolved as lambda.
          * Clone lambda before resolve.
          * Initialize lambda arguments as function arguments.
          * Resolve lambda and then replace function node with resolved lambda expression body.
          * Example: WITH (x -> x + 1) AS lambda SELECT lambda(value) FROM test_table;
          * Result: SELECT value + 1 FROM test_table;
          */
        if (lambda_expression_untyped)
        {
            auto * lambda_expression = lambda_expression_untyped->as<LambdaNode>();
            if (!lambda_expression)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function identifier '{}' must be resolved as lambda. Actual {}. In scope {}",
                    function_node.getFunctionName(),
                    lambda_expression_untyped->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            checkFunctionNodeHasEmptyNullsAction(function_node);

            if (!parameters.empty())
            {
                throw Exception(
                    ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS, "Function {} is not parametric", function_node.formatASTForErrorMessage());
            }

            auto lambda_expression_clone = lambda_expression_untyped->clone();

            IdentifierResolveScope lambda_scope(lambda_expression_clone, &scope /*parent_scope*/);
            ProjectionNames lambda_projection_names = resolveLambda(lambda_expression_untyped, lambda_expression_clone, function_arguments, lambda_scope);

            auto & resolved_lambda = lambda_expression_clone->as<LambdaNode &>();
            node = resolved_lambda.getExpression();

            if (node->getNodeType() == QueryTreeNodeType::LIST)
                result_projection_names = std::move(lambda_projection_names);

            return result_projection_names;
        }

        if (function_name == "untuple")
        {
            /// Special handling of `untuple` function

            if (function_arguments.size() != 1)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Function 'untuple' must have 1 argument. In scope {}",
                    scope.scope_node->formatASTForErrorMessage());

            checkFunctionNodeHasEmptyNullsAction(function_node);

            const auto & untuple_argument = function_arguments[0];
            /// Handle this special case first as `getResultType()` might return nullptr
            if (untuple_argument->as<LambdaNode>())
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function untuple can't have lambda-expressions as arguments");

            auto result_type = untuple_argument->getResultType();
            const auto * tuple_data_type = typeid_cast<const DataTypeTuple *>(result_type.get());
            if (!tuple_data_type)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Function 'untuple' argument must have compound type. Actual type {}. In scope {}",
                    result_type->getName(),
                    scope.scope_node->formatASTForErrorMessage());

            const auto & element_names = tuple_data_type->getElementNames();

            auto result_list = std::make_shared<ListNode>();
            result_list->getNodes().reserve(element_names.size());

            for (const auto & element_name : element_names)
            {
                auto tuple_element_function = std::make_shared<FunctionNode>("tupleElement");
                tuple_element_function->getArguments().getNodes().push_back(untuple_argument);
                tuple_element_function->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(element_name));

                QueryTreeNodePtr function_query_node = tuple_element_function;
                resolveFunction(function_query_node, scope);

                result_list->getNodes().push_back(std::move(function_query_node));
            }

            auto untuple_argument_projection_name = arguments_projection_names.at(0);
            result_projection_names.clear();

            for (const auto & element_name : element_names)
            {
                if (node->hasAlias())
                    result_projection_names.push_back(node->getAlias() + '.' + element_name);
                else
                    result_projection_names.push_back(fmt::format("tupleElement({}, '{}')", untuple_argument_projection_name, element_name));
            }

            node = std::move(result_list);
            return result_projection_names;
        }
        else if (function_name == "grouping")
        {
            /// It is responsibility of planner to perform additional handling of grouping function
            if (function_arguments_size == 0)
                throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                    "Function GROUPING expects at least one argument");
            else if (function_arguments_size > 64)
                throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                    "Function GROUPING can have up to 64 arguments, but {} provided",
                    function_arguments_size);
            checkFunctionNodeHasEmptyNullsAction(function_node);

            bool force_grouping_standard_compatibility = scope.context->getSettingsRef()[Setting::force_grouping_standard_compatibility];
            auto grouping_function = std::make_shared<FunctionGrouping>(force_grouping_standard_compatibility);
            auto grouping_function_adaptor = std::make_shared<FunctionToOverloadResolverAdaptor>(std::move(grouping_function));
            function_node.resolveAsFunction(grouping_function_adaptor->build(argument_columns));

            return result_projection_names;
        }
    }

    if (function_node.isWindowFunction())
    {
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function_name))
        {
            throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION, "Aggregate function with name '{}' does not exist. In scope {}{}",
                            function_name, scope.scope_node->formatASTForErrorMessage(),
                            getHintsErrorMessageSuffix(AggregateFunctionFactory::instance().getHints(function_name)));
        }

        if (!function_lambda_arguments_indexes.empty())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Window function '{}' does not support lambda arguments",
                function_name);

        auto action = function_node_ptr->getNullsAction();
        std::string aggregate_function_name = rewriteAggregateFunctionNameIfNeeded(function_name, action, scope.context);

        AggregateFunctionProperties properties;
        auto aggregate_function
            = AggregateFunctionFactory::instance().get(aggregate_function_name, action, argument_types, parameters, properties);

        function_node.resolveAsWindowFunction(std::move(aggregate_function));

        bool window_node_is_identifier = function_node.getWindowNode()->getNodeType() == QueryTreeNodeType::IDENTIFIER;
        ProjectionName window_projection_name = resolveWindow(function_node.getWindowNode(), scope);

        if (window_node_is_identifier)
            result_projection_names[0] += " OVER " + window_projection_name;
        else
            result_projection_names[0] += " OVER (" + window_projection_name + ')';

        return result_projection_names;
    }

    FunctionOverloadResolverPtr function = UserDefinedExecutableFunctionFactory::instance().tryGet(function_name, scope.context, parameters); /// NOLINT(readability-static-accessed-through-instance)
    bool is_executable_udf = true;

    IdentifierResolveScope::ResolvedFunctionsCache * function_cache = nullptr;

    if (!function)
    {
        /// This is a hack to allow a query like `select randConstant(), randConstant(), randConstant()`.
        /// Function randConstant() would return the same value for the same arguments (in scope).

        auto hash = function_node_ptr->getTreeHash();
        function_cache = &scope.functions_cache[hash];
        if (!function_cache->resolver)
            function_cache->resolver = FunctionFactory::instance().tryGet(function_name, scope.context);

        function = function_cache->resolver;

        is_executable_udf = false;
    }

    if (function)
    {
        checkFunctionNodeHasEmptyNullsAction(function_node);
    }
    else
    {
        if (!AggregateFunctionFactory::instance().isAggregateFunctionName(function_name))
        {
            std::vector<std::string> possible_function_names;

            auto function_names = UserDefinedExecutableFunctionFactory::instance().getRegisteredNames(scope.context); /// NOLINT(readability-static-accessed-through-instance)
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            function_names = UserDefinedSQLFunctionFactory::instance().getAllRegisteredNames();
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            function_names = FunctionFactory::instance().getAllRegisteredNames();
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            function_names = AggregateFunctionFactory::instance().getAllRegisteredNames();
            possible_function_names.insert(possible_function_names.end(), function_names.begin(), function_names.end());

            for (auto & [name, lambda_node] : scope.aliases.alias_name_to_lambda_node)
            {
                if (lambda_node->getNodeType() == QueryTreeNodeType::LAMBDA)
                    possible_function_names.push_back(name);
            }

            auto hints = NamePrompter<2>::getHints(function_name, possible_function_names);

            throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
                "Function with name {} does not exist. In scope {}{}",
                backQuote(function_name),
                scope.scope_node->formatASTForErrorMessage(),
                getHintsErrorMessageSuffix(hints));
        }

        if (!function_lambda_arguments_indexes.empty())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Aggregate function {} does not support lambda arguments",
                backQuote(function_name));

        auto action = function_node_ptr->getNullsAction();
        std::string aggregate_function_name = rewriteAggregateFunctionNameIfNeeded(function_name, action, scope.context);

        AggregateFunctionProperties properties;
        auto aggregate_function
            = AggregateFunctionFactory::instance().get(aggregate_function_name, action, argument_types, parameters, properties);

        function_node.resolveAsAggregateFunction(std::move(aggregate_function));

        return result_projection_names;
    }

    /// Executable UDFs may have parameters. They are checked in UserDefinedExecutableFunctionFactory.
    if (!parameters.empty() && !is_executable_udf)
    {
        throw Exception(ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS, "Function {} is not parametric", function_name);
    }

    /** For lambda arguments we need to initialize lambda argument types DataTypeFunction using `getLambdaArgumentTypes` function.
      * Then each lambda arguments are initialized with columns, where column source is lambda.
      * This information is important for later steps of query processing.
      * Example: SELECT arrayMap(x -> x + 1, [1, 2, 3]).
      * lambda node x -> x + 1 identifier x is resolved as column where source is lambda node.
      */
    bool has_lambda_arguments = !function_lambda_arguments_indexes.empty();
    if (has_lambda_arguments)
    {
        function->getLambdaArgumentTypes(argument_types);

        ProjectionNames lambda_projection_names;
        for (auto & function_lambda_argument_index : function_lambda_arguments_indexes)
        {
            auto & lambda_argument = function_arguments[function_lambda_argument_index];
            auto lambda_to_resolve = lambda_argument->clone();
            auto & lambda_to_resolve_typed = lambda_to_resolve->as<LambdaNode &>();

            const auto & lambda_argument_names = lambda_to_resolve_typed.getArgumentNames();
            size_t lambda_arguments_size = lambda_to_resolve_typed.getArguments().getNodes().size();

            const auto * function_data_type = typeid_cast<const DataTypeFunction *>(argument_types[function_lambda_argument_index].get());
            if (!function_data_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function '{}' expected function data type for lambda argument with index {}. Actual {}. In scope {}",
                    function_name,
                    function_lambda_argument_index,
                    argument_types[function_lambda_argument_index]->getName(),
                    scope.scope_node->formatASTForErrorMessage());

            const auto & function_data_type_argument_types = function_data_type->getArgumentTypes();
            size_t function_data_type_arguments_size = function_data_type_argument_types.size();
            if (function_data_type_arguments_size != lambda_arguments_size)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "Function '{}"
                                "' function data type for lambda argument with index {} arguments size mismatch. "
                                "Actual {}. Expected {}. In scope {}",
                                function_name,
                                function_data_type_arguments_size,
                                lambda_arguments_size,
                                argument_types[function_lambda_argument_index]->getName(),
                                scope.scope_node->formatASTForErrorMessage());

            QueryTreeNodes lambda_arguments;
            lambda_arguments.reserve(lambda_arguments_size);

            for (size_t i = 0; i < lambda_arguments_size; ++i)
            {
                const auto & argument_type = function_data_type_argument_types[i];
                auto column_name_and_type = NameAndTypePair{lambda_argument_names[i], argument_type};
                lambda_arguments.push_back(std::make_shared<ColumnNode>(std::move(column_name_and_type), lambda_to_resolve));
            }

            IdentifierResolveScope lambda_scope(lambda_to_resolve, &scope /*parent_scope*/);
            lambda_projection_names = resolveLambda(lambda_argument, lambda_to_resolve, lambda_arguments, lambda_scope);

            if (auto * lambda_list_node_result = lambda_to_resolve_typed.getExpression()->as<ListNode>())
            {
                size_t lambda_list_node_result_nodes_size = lambda_list_node_result->getNodes().size();

                if (lambda_list_node_result_nodes_size != 1)
                    throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                        "Lambda as function argument resolved as list node with size {}. Expected 1. In scope {}",
                        lambda_list_node_result_nodes_size,
                        lambda_to_resolve->formatASTForErrorMessage());

                lambda_to_resolve_typed.getExpression() = lambda_list_node_result->getNodes().front();
            }

            if (arguments_projection_names.at(function_lambda_argument_index) == PROJECTION_NAME_PLACEHOLDER)
            {
                size_t lambda_projection_names_size =lambda_projection_names.size();
                if (lambda_projection_names_size != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Lambda argument inside function expected to have 1 projection name. Actual {}",
                        lambda_projection_names_size);

                WriteBufferFromOwnString lambda_argument_projection_name_buffer;
                lambda_argument_projection_name_buffer << "lambda(";
                lambda_argument_projection_name_buffer << "tuple(";

                size_t lambda_argument_names_size = lambda_argument_names.size();

                for (size_t i = 0; i < lambda_argument_names_size; ++i)
                {
                    const auto & lambda_argument_name = lambda_argument_names[i];
                    lambda_argument_projection_name_buffer << lambda_argument_name;

                    if (i + 1 != lambda_argument_names_size)
                        lambda_argument_projection_name_buffer << ", ";
                }

                lambda_argument_projection_name_buffer << "), ";
                lambda_argument_projection_name_buffer << lambda_projection_names[0];
                lambda_argument_projection_name_buffer << ")";

                lambda_projection_names.clear();

                arguments_projection_names[function_lambda_argument_index] = lambda_argument_projection_name_buffer.str();
            }

            auto lambda_resolved_type = std::make_shared<DataTypeFunction>(function_data_type_argument_types, lambda_to_resolve_typed.getExpression()->getResultType());
            lambda_to_resolve_typed.resolve(lambda_resolved_type);

            argument_types[function_lambda_argument_index] = lambda_resolved_type;
            argument_columns[function_lambda_argument_index].type = lambda_resolved_type;
            function_arguments[function_lambda_argument_index] = std::move(lambda_to_resolve);
        }

        /// Recalculate function projection name after lambda resolution
        result_projection_names = { calculateFunctionProjectionName(node, parameters_projection_names, arguments_projection_names) };
    }

    /** Create SET column for special function IN to allow constant folding
      * if left and right arguments are constants.
      *
      * Example: SELECT * FROM test_table LIMIT 1 IN 1;
      */
    if (is_special_function_in)
    {
        const auto * first_argument_constant_node = function_arguments[0]->as<ConstantNode>();
        const auto * second_argument_constant_node = function_arguments[1]->as<ConstantNode>();

        if (first_argument_constant_node && second_argument_constant_node)
        {
            const auto & first_argument_constant_type = first_argument_constant_node->getResultType();
            const auto & second_argument_constant_literal = second_argument_constant_node->getValue();
            const auto & second_argument_constant_type = second_argument_constant_node->getResultType();

            const auto & settings = scope.context->getSettingsRef();

            auto result_block = getSetElementsForConstantValue(
                first_argument_constant_type, second_argument_constant_literal, second_argument_constant_type, settings[Setting::transform_null_in]);

            SizeLimits size_limits_for_set = {settings[Setting::max_rows_in_set], settings[Setting::max_bytes_in_set], settings[Setting::set_overflow_mode]};

            auto set = std::make_shared<Set>(size_limits_for_set, 0, settings[Setting::transform_null_in]);

            set->setHeader(result_block.cloneEmpty().getColumnsWithTypeAndName());
            set->insertFromBlock(result_block.getColumnsWithTypeAndName());
            set->finishInsert();

            auto future_set = std::make_shared<FutureSetFromStorage>(std::move(set));

            /// Create constant set column for constant folding

            auto column_set = ColumnSet::create(1, std::move(future_set));
            argument_columns[1].column = ColumnConst::create(std::move(column_set), 1);
        }

        argument_columns[1].type = std::make_shared<DataTypeSet>();
    }

    std::shared_ptr<ConstantValue> constant_value;

    try
    {
        FunctionBasePtr function_base;
        if (function_cache)
        {
            auto & cached_function = function_cache->function_base;
            if (!cached_function)
                cached_function = function->build(argument_columns);

            function_base = cached_function;
        }
        else
            function_base = function->build(argument_columns);

        /// Do not constant fold get scalar functions
        // bool disable_constant_folding = function_name == "__getScalar" || function_name == "shardNum" ||
        //     function_name == "shardCount" || function_name == "hostName" || function_name == "tcpPort";

        /** If function is suitable for constant folding try to convert it to constant.
          * Example: SELECT plus(1, 1);
          * Result: SELECT 2;
          */
        if (function_base->isSuitableForConstantFolding()) // && !disable_constant_folding)
        {
            auto result_type = function_base->getResultType();
            auto executable_function = function_base->prepare(argument_columns);

            ColumnPtr column;

            if (all_arguments_constants)
            {
                size_t num_rows = function_arguments.empty() ? 0 : argument_columns.front().column->size();
                column = executable_function->execute(argument_columns, result_type, num_rows, true);
            }
            else
            {
                column = function_base->getConstantResultForNonConstArguments(argument_columns, result_type);
            }

            if (column && column->getDataType() != result_type->getColumnType())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Unexpected return type from {}. Expected {}. Got {}",
                    function->getName(),
                    result_type->getColumnType(),
                    column->getDataType());

            /** Do not perform constant folding if there are aggregate or arrayJoin functions inside function.
              * Example: SELECT toTypeName(sum(number)) FROM numbers(10);
              */
            if (column && isColumnConst(*column) && !typeid_cast<const ColumnConst *>(column.get())->getDataColumn().isDummy() &&
                !hasAggregateFunctionNodes(node) && !hasFunctionNode(node, "arrayJoin") &&
                /// Sanity check: do not convert large columns to constants
                column->byteSize() < 1_MiB)
            {
                /// Replace function node with result constant node
                Field column_constant_value;
                column->get(0, column_constant_value);
                constant_value = std::make_shared<ConstantValue>(std::move(column_constant_value), result_type);
            }
        }

        function_node.resolveAsFunction(std::move(function_base));
    }
    catch (Exception & e)
    {
        e.addMessage("In scope {}", scope.scope_node->formatASTForErrorMessage());
        throw;
    }

    if (constant_value)
        node = std::make_shared<ConstantNode>(std::move(constant_value), node);

    return result_projection_names;
}

/** Resolve expression node.
  * Argument node can be replaced with different node, or even with list node in case of matcher resolution.
  * Example: SELECT * FROM test_table;
  * * - is matcher node, and it can be resolved into ListNode.
  *
  * Steps:
  * 1. If node has alias, replace node with its value in scope alias map. Register alias in expression_aliases_in_resolve_process, to prevent resolving identifier
  * which can bind to expression alias name. Check tryResolveIdentifierFromAliases documentation for additional explanation.
  * Example:
  * SELECT id AS id FROM test_table;
  * SELECT value.value1 AS value FROM test_table;
  *
  * 2. Call specific resolve method depending on node type.
  *
  * If allow_table_expression = true and node is query node, then it is not evaluated as scalar subquery.
  * Although if node is identifier that is resolved into query node that query is evaluated as scalar subquery.
  * SELECT id, (SELECT 1) AS c FROM test_table WHERE a IN c;
  * SELECT id, FROM test_table WHERE a IN (SELECT 1);
  *
  * 3. Special case identifier node.
  * Try resolve it as expression identifier.
  * Then if allow_lambda_expression = true try to resolve it as function.
  * Then if allow_table_expression = true try to resolve it as table expression.
  *
  * 4. If node has alias, update its value in scope alias map. Deregister alias from expression_aliases_in_resolve_process.
  */
ProjectionNames QueryAnalyzer::resolveExpressionNode(
    QueryTreeNodePtr & node, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression, bool ignore_alias)
{
    checkStackSize();

    auto resolved_expression_it = resolved_expressions.find(node);
    if (resolved_expression_it != resolved_expressions.end())
    {
        /** There can be edge case, when subquery for IN function is resolved multiple times in different context.
          * SELECT id IN (subquery AS value), value FROM test_table;
          * When we start to resolve `value` identifier, subquery is already resolved but constant folding is not performed.
          */
        auto node_type = node->getNodeType();
        if (!allow_table_expression && (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION))
        {
            IdentifierResolveScope subquery_scope(node, &scope /*parent_scope*/);
            subquery_scope.subquery_depth = scope.subquery_depth + 1;

            evaluateScalarSubqueryIfNeeded(node, subquery_scope);
        }

        return resolved_expression_it->second;
    }

    String node_alias = node->getAlias();
    ProjectionNames result_projection_names;

    if (node_alias.empty())
    {
        auto projection_name_it = node_to_projection_name.find(node);
        if (projection_name_it != node_to_projection_name.end())
            result_projection_names.push_back(projection_name_it->second);
    }
    else
    {
        result_projection_names.push_back(node_alias);
    }

    bool is_duplicated_alias = scope.aliases.nodes_with_duplicated_aliases.contains(node);
    if (is_duplicated_alias)
        scope.non_cached_identifier_lookups_during_expression_resolve.insert({Identifier{node_alias}, IdentifierLookupContext::EXPRESSION});

    /** Do not use alias table if node has alias same as some other node.
      * Example: WITH x -> x + 1 AS lambda SELECT 1 AS lambda;
      * During 1 AS lambda resolve if we use alias table we replace node with x -> x + 1 AS lambda.
      *
      * Do not use alias table if allow_table_expression = true and we resolve query node directly.
      * Example: SELECT a FROM test_table WHERE id IN (SELECT 1) AS a;
      * To support both (SELECT 1) AS expression in projection and (SELECT 1) as subquery in IN, do not use
      * alias table because in alias table subquery could be evaluated as scalar.
      */
    bool use_alias_table = !ignore_alias;
    if (is_duplicated_alias || (allow_table_expression && IdentifierResolver::isSubqueryNodeType(node->getNodeType())))
        use_alias_table = false;

    if (!node_alias.empty() && use_alias_table)
    {
        /** Node could be potentially resolved by resolving other nodes.
          * SELECT b, a as b FROM test_table;
          *
          * To resolve b we need to resolve a.
          */
        auto it = scope.aliases.alias_name_to_expression_node->find(node_alias);
        if (it != scope.aliases.alias_name_to_expression_node->end())
            node = it->second;

        if (allow_lambda_expression)
        {
            it = scope.aliases.alias_name_to_lambda_node.find(node_alias);
            if (it != scope.aliases.alias_name_to_lambda_node.end())
                node = it->second;
        }
    }

    scope.pushExpressionNode(node);

    auto node_type = node->getNodeType();

    switch (node_type)
    {
        case QueryTreeNodeType::IDENTIFIER:
        {
            auto & identifier_node = node->as<IdentifierNode &>();
            auto unresolved_identifier = identifier_node.getIdentifier();
            auto resolve_identifier_expression_result = tryResolveIdentifier({unresolved_identifier, IdentifierLookupContext::EXPRESSION}, scope);
            auto resolved_identifier_node = resolve_identifier_expression_result.resolved_identifier;

            if (resolved_identifier_node && result_projection_names.empty() &&
                (resolve_identifier_expression_result.isResolvedFromJoinTree() || resolve_identifier_expression_result.isResolvedFromExpressionArguments()))
            {
                auto projection_name_it = node_to_projection_name.find(resolved_identifier_node);
                if (projection_name_it != node_to_projection_name.end())
                    result_projection_names.push_back(projection_name_it->second);
            }

            if (!resolved_identifier_node && allow_lambda_expression)
                resolved_identifier_node = tryResolveIdentifier({unresolved_identifier, IdentifierLookupContext::FUNCTION}, scope).resolved_identifier;

            if (!resolved_identifier_node && allow_table_expression)
            {
                resolved_identifier_node = tryResolveIdentifier({unresolved_identifier, IdentifierLookupContext::TABLE_EXPRESSION}, scope).resolved_identifier;

                if (resolved_identifier_node)
                {
                    /// If table identifier is resolved as CTE clone it and resolve
                    auto * subquery_node = resolved_identifier_node->as<QueryNode>();
                    auto * union_node = resolved_identifier_node->as<UnionNode>();
                    bool resolved_as_cte = (subquery_node && subquery_node->isCTE()) || (union_node && union_node->isCTE());

                    if (resolved_as_cte)
                    {
                        resolved_identifier_node = resolved_identifier_node->clone();
                        subquery_node = resolved_identifier_node->as<QueryNode>();
                        union_node = resolved_identifier_node->as<UnionNode>();

                        std::string_view cte_name = subquery_node ? subquery_node->getCTEName() : union_node->getCTEName();

                        if (subquery_node)
                            subquery_node->setIsCTE(false);
                        else
                            union_node->setIsCTE(false);

                        IdentifierResolveScope subquery_scope(resolved_identifier_node, &scope /*parent_scope*/);
                        subquery_scope.subquery_depth = scope.subquery_depth + 1;

                        /// CTE is being resolved, it's required to forbid to resolve to it again
                        /// because recursive CTEs are not supported, e.g.:
                        ///
                        /// WITH test1 AS (SELECT i + 1, j + 1 FROM test1) SELECT toInt64(4) i, toInt64(5) j FROM numbers(3) WHERE (i, j) IN test1;
                        ///
                        /// In this example argument of function `in` is being resolve here. If CTE `test1` is not forbidden,
                        /// `test1` is resolved to CTE (not to the table) in `initializeQueryJoinTreeNode` function.
                        ctes_in_resolve_process.insert(cte_name);

                        if (subquery_node)
                            resolveQuery(resolved_identifier_node, subquery_scope);
                        else
                            resolveUnion(resolved_identifier_node, subquery_scope);

                        ctes_in_resolve_process.erase(cte_name);
                    }
                }
            }

            if (!resolved_identifier_node)
            {
                std::string message_clarification;
                if (allow_lambda_expression)
                    message_clarification = std::string(" or ") + toStringLowercase(IdentifierLookupContext::FUNCTION);

                if (allow_table_expression)
                    message_clarification = std::string(" or ") + toStringLowercase(IdentifierLookupContext::TABLE_EXPRESSION);

                std::unordered_set<Identifier> valid_identifiers;
                IdentifierResolver::collectScopeWithParentScopesValidIdentifiersForTypoCorrection(unresolved_identifier,
                    scope,
                    true,
                    allow_lambda_expression,
                    allow_table_expression,
                    valid_identifiers);

                auto hints = IdentifierResolver::collectIdentifierTypoHints(unresolved_identifier, valid_identifiers);

                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown {}{} identifier {} in scope {}{}",
                    toStringLowercase(IdentifierLookupContext::EXPRESSION),
                    message_clarification,
                    backQuote(unresolved_identifier.getFullName()),
                    scope.scope_node->formatASTForErrorMessage(),
                    getHintsErrorMessageSuffix(hints));
            }

            node = std::move(resolved_identifier_node);

            if (node->getNodeType() == QueryTreeNodeType::LIST)
            {
                result_projection_names.clear();
                resolved_expression_it = resolved_expressions.find(node);
                if (resolved_expression_it != resolved_expressions.end())
                    return resolved_expression_it->second;
                else
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Identifier '{}' resolve into list node and list node projection names are not initialized. In scope {}",
                        unresolved_identifier.getFullName(),
                        scope.scope_node->formatASTForErrorMessage());
            }

            if (result_projection_names.empty())
                result_projection_names.push_back(unresolved_identifier.getFullName());

            break;
        }
        case QueryTreeNodeType::MATCHER:
        {
            result_projection_names = resolveMatcher(node, scope);
            break;
        }
        case QueryTreeNodeType::LIST:
        {
            /** Edge case if list expression has alias.
              * Matchers cannot have aliases, but `untuple` function can.
              * Example: SELECT a, untuple(CAST(('hello', 1) AS Tuple(name String, count UInt32))) AS a;
              * During resolveFunction `untuple` function is replaced by list of 2 constants 'hello', 1.
              */
            result_projection_names = resolveExpressionNodeList(node, scope, allow_lambda_expression, allow_lambda_expression);
            break;
        }
        case QueryTreeNodeType::CONSTANT:
        {
            if (result_projection_names.empty())
            {
                const auto & constant_node = node->as<ConstantNode &>();
                result_projection_names.push_back(constant_node.getValueStringRepresentation());
            }

            /// Already resolved
            break;
        }
        case QueryTreeNodeType::COLUMN:
        {
            auto & column_node = node->as<ColumnNode &>();
            if (column_node.hasExpression())
                resolveExpressionNode(column_node.getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

            if (result_projection_names.empty())
                result_projection_names.push_back(column_node.getColumnName());

            break;
        }
        case QueryTreeNodeType::FUNCTION:
        {
            auto function_projection_names = resolveFunction(node, scope);

            if (result_projection_names.empty() || node->getNodeType() == QueryTreeNodeType::LIST)
                result_projection_names = std::move(function_projection_names);

            break;
        }
        case QueryTreeNodeType::LAMBDA:
        {
            if (!allow_lambda_expression)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Lambda {} is not allowed in expression context. In scope {}",
                    node->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            if (result_projection_names.empty())
                result_projection_names.push_back(PROJECTION_NAME_PLACEHOLDER);

            /// Lambda must be resolved by caller
            break;
        }
        case QueryTreeNodeType::QUERY:
            [[fallthrough]];
        case QueryTreeNodeType::UNION:
        {
            IdentifierResolveScope subquery_scope(node, &scope /*parent_scope*/);
            subquery_scope.subquery_depth = scope.subquery_depth + 1;

            std::string projection_name = "_subquery_" + std::to_string(subquery_counter);
            ++subquery_counter;

            if (node_type == QueryTreeNodeType::QUERY)
                resolveQuery(node, subquery_scope);
            else
                resolveUnion(node, subquery_scope);

            if (!allow_table_expression)
                evaluateScalarSubqueryIfNeeded(node, subquery_scope);

            if (result_projection_names.empty())
                result_projection_names.push_back(std::move(projection_name));

            break;
        }
        case QueryTreeNodeType::TABLE:
        {
            if (!allow_table_expression)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Table {} is not allowed in expression context. In scope {}",
                    node->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            auto & table_node = node->as<TableNode &>();
            if (result_projection_names.empty())
                result_projection_names.push_back(table_node.getStorageID().getFullNameNotQuoted());

            break;
        }
        case QueryTreeNodeType::TRANSFORMER:
            [[fallthrough]];
        case QueryTreeNodeType::SORT:
            [[fallthrough]];
        case QueryTreeNodeType::INTERPOLATE:
            [[fallthrough]];
        case QueryTreeNodeType::WINDOW:
            [[fallthrough]];
        case QueryTreeNodeType::TABLE_FUNCTION:
            [[fallthrough]];
        case QueryTreeNodeType::ARRAY_JOIN:
            [[fallthrough]];
        case QueryTreeNodeType::JOIN:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "{} {} is not allowed in expression context. In scope {}",
                node->getNodeType(),
                node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }

    validateTreeSize(node, scope.context->getSettingsRef()[Setting::max_expanded_ast_elements], node_to_tree_size);

    /// Lambda can be inside the aggregate function, so we should check parent scopes.
    /// Most likely only the root scope can have an aggregate function, but let's check all just in case.
    bool in_aggregate_function_scope = false;
    for (const auto * scope_ptr = &scope; scope_ptr; scope_ptr = scope_ptr->parent_scope)
        in_aggregate_function_scope = in_aggregate_function_scope || scope_ptr->expressions_in_resolve_process_stack.hasAggregateFunction();

    if (!in_aggregate_function_scope)
    {
        for (const auto * scope_ptr = &scope; scope_ptr; scope_ptr = scope_ptr->parent_scope)
        {
            auto it = scope_ptr->nullable_group_by_keys.find(node);
            if (it != scope_ptr->nullable_group_by_keys.end())
            {
                node = it->node->clone();
                node->convertToNullable();
                break;
            }

            /// Check parent scopes until find current query scope.
            if (scope_ptr->scope_node->getNodeType() == QueryTreeNodeType::QUERY)
                break;
        }
    }

    /** Update aliases after expression node was resolved.
      * Do not update node in alias table if we resolve it for duplicate alias.
      */
    if (!node_alias.empty() && use_alias_table && !scope.group_by_use_nulls)
    {
        auto it = scope.aliases.alias_name_to_expression_node->find(node_alias);
        if (it != scope.aliases.alias_name_to_expression_node->end())
            it->second = node;

        if (allow_lambda_expression)
        {
            it = scope.aliases.alias_name_to_lambda_node.find(node_alias);
            if (it != scope.aliases.alias_name_to_lambda_node.end())
                it->second = node;
        }
    }

    if (is_duplicated_alias)
        scope.non_cached_identifier_lookups_during_expression_resolve.erase({Identifier{node_alias}, IdentifierLookupContext::EXPRESSION});

    if (!ignore_alias)
        resolved_expressions.emplace(node, result_projection_names);

    scope.popExpressionNode();
    bool expression_was_root = scope.expressions_in_resolve_process_stack.empty();
    if (expression_was_root)
        scope.non_cached_identifier_lookups_during_expression_resolve.clear();

    return result_projection_names;
}

/** Resolve expression node list.
  * If expression is CTE subquery node it is skipped.
  * If expression is resolved in list, it is flattened into initial node list.
  *
  * Such examples must work:
  * Example: CREATE TABLE test_table (id UInt64, value UInt64) ENGINE=TinyLog; SELECT plus(*) FROM test_table;
  * Example: SELECT *** FROM system.one;
  */
ProjectionNames QueryAnalyzer::resolveExpressionNodeList(QueryTreeNodePtr & node_list, IdentifierResolveScope & scope, bool allow_lambda_expression, bool allow_table_expression)
{
    auto & node_list_typed = node_list->as<ListNode &>();
    size_t node_list_size = node_list_typed.getNodes().size();

    QueryTreeNodes result_nodes;
    result_nodes.reserve(node_list_size);

    ProjectionNames result_projection_names;

    for (auto & node : node_list_typed.getNodes())
    {
        auto node_to_resolve = node;
        auto expression_node_projection_names = resolveExpressionNode(node_to_resolve, scope, allow_lambda_expression, allow_table_expression);
        size_t expected_projection_names_size = 1;
        if (auto * expression_list = node_to_resolve->as<ListNode>())
        {
            expected_projection_names_size = expression_list->getNodes().size();
            for (auto & expression_list_node : expression_list->getNodes())
                result_nodes.push_back(expression_list_node);
        }
        else
        {
            result_nodes.push_back(std::move(node_to_resolve));
        }

        if (expression_node_projection_names.size() != expected_projection_names_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expression nodes list expected {} projection names. Actual {}",
                expected_projection_names_size,
                expression_node_projection_names.size());

        result_projection_names.insert(result_projection_names.end(), expression_node_projection_names.begin(), expression_node_projection_names.end());
        expression_node_projection_names.clear();
    }

    node_list_typed.getNodes() = std::move(result_nodes);

    return result_projection_names;
}

/** Resolve sort columns nodes list.
  */
ProjectionNames QueryAnalyzer::resolveSortNodeList(QueryTreeNodePtr & sort_node_list, IdentifierResolveScope & scope)
{
    ProjectionNames result_projection_names;
    ProjectionNames sort_expression_projection_names;
    ProjectionNames fill_from_expression_projection_names;
    ProjectionNames fill_to_expression_projection_names;
    ProjectionNames fill_step_expression_projection_names;

    auto & sort_node_list_typed = sort_node_list->as<ListNode &>();
    for (auto & node : sort_node_list_typed.getNodes())
    {
        auto & sort_node = node->as<SortNode &>();
        sort_expression_projection_names = resolveExpressionNode(sort_node.getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

        if (auto * sort_column_list_node = sort_node.getExpression()->as<ListNode>())
        {
            size_t sort_column_list_node_size = sort_column_list_node->getNodes().size();
            if (sort_column_list_node_size != 1)
            {
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "Sort column node expression resolved into list with size {}. Expected 1. In scope {}",
                    sort_column_list_node_size,
                    scope.scope_node->formatASTForErrorMessage());
            }

            sort_node.getExpression() = sort_column_list_node->getNodes().front();
        }

        size_t sort_expression_projection_names_size = sort_expression_projection_names.size();
        if (sort_expression_projection_names_size != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Sort expression expected 1 projection name. Actual {}",
                sort_expression_projection_names_size);

        if (sort_node.hasFillFrom())
        {
            fill_from_expression_projection_names = resolveExpressionNode(sort_node.getFillFrom(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

            const auto * constant_node = sort_node.getFillFrom()->as<ConstantNode>();
            if (!constant_node || !isColumnedAsNumber(constant_node->getResultType()))
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "Sort FILL FROM expression must be constant with numeric type. Actual {}. In scope {}",
                    sort_node.getFillFrom()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            size_t fill_from_expression_projection_names_size = fill_from_expression_projection_names.size();
            if (fill_from_expression_projection_names_size != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Sort node FILL FROM expression expected 1 projection name. Actual {}",
                    fill_from_expression_projection_names_size);
        }

        if (sort_node.hasFillTo())
        {
            fill_to_expression_projection_names = resolveExpressionNode(sort_node.getFillTo(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

            const auto * constant_node = sort_node.getFillTo()->as<ConstantNode>();
            if (!constant_node || !isColumnedAsNumber(constant_node->getResultType()))
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "Sort FILL TO expression must be constant with numeric type. Actual {}. In scope {}",
                    sort_node.getFillFrom()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            size_t fill_to_expression_projection_names_size = fill_to_expression_projection_names.size();
            if (fill_to_expression_projection_names_size != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Sort node FILL TO expression expected 1 projection name. Actual {}",
                    fill_to_expression_projection_names_size);
        }

        if (sort_node.hasFillStep())
        {
            fill_step_expression_projection_names = resolveExpressionNode(sort_node.getFillStep(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

            const auto * constant_node = sort_node.getFillStep()->as<ConstantNode>();
            if (!constant_node)
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "Sort FILL STEP expression must be constant with numeric or interval type. Actual {}. In scope {}",
                    sort_node.getFillStep()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            bool is_number = isColumnedAsNumber(constant_node->getResultType());
            bool is_interval = WhichDataType(constant_node->getResultType()).isInterval();
            if (!is_number && !is_interval)
                throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                    "Sort FILL STEP expression must be constant with numeric or interval type. Actual {}. In scope {}",
                    sort_node.getFillStep()->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());

            size_t fill_step_expression_projection_names_size = fill_step_expression_projection_names.size();
            if (fill_step_expression_projection_names_size != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Sort FILL STEP expression expected 1 projection name. Actual {}",
                    fill_step_expression_projection_names_size);
        }

        auto sort_column_projection_name = calculateSortColumnProjectionName(node,
            sort_expression_projection_names[0],
            fill_from_expression_projection_names.empty() ? "" : fill_from_expression_projection_names.front(),
            fill_to_expression_projection_names.empty() ? "" : fill_to_expression_projection_names.front(),
            fill_step_expression_projection_names.empty() ? "" : fill_step_expression_projection_names.front());

        result_projection_names.push_back(std::move(sort_column_projection_name));

        sort_expression_projection_names.clear();
        fill_from_expression_projection_names.clear();
        fill_to_expression_projection_names.clear();
        fill_step_expression_projection_names.clear();
    }

    return result_projection_names;
}

namespace
{

void expandTuplesInList(QueryTreeNodes & key_list)
{
    QueryTreeNodes expanded_keys;
    expanded_keys.reserve(key_list.size());
    for (auto const & key : key_list)
    {
        if (auto * function = key->as<FunctionNode>(); function != nullptr && function->getFunctionName() == "tuple")
        {
            std::copy(function->getArguments().begin(), function->getArguments().end(), std::back_inserter(expanded_keys));
        }
        else
            expanded_keys.push_back(key);
    }
    key_list = std::move(expanded_keys);
}

}

/** Resolve GROUP BY clause.
  */
void QueryAnalyzer::resolveGroupByNode(QueryNode & query_node_typed, IdentifierResolveScope & scope)
{
    if (query_node_typed.isGroupByWithGroupingSets())
    {
        for (auto & grouping_sets_keys_list_node : query_node_typed.getGroupBy().getNodes())
        {
            replaceNodesWithPositionalArguments(grouping_sets_keys_list_node, query_node_typed.getProjection().getNodes(), scope);

            resolveExpressionNodeList(grouping_sets_keys_list_node, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

            // Remove redundant calls to `tuple` function. It simplifies checking if expression is an aggregation key.
            // It's required to support queries like: SELECT number FROM numbers(3) GROUP BY (number, number % 2)
            auto & group_by_list = grouping_sets_keys_list_node->as<ListNode &>().getNodes();
            expandTuplesInList(group_by_list);
        }

        if (scope.group_by_use_nulls)
        {
            for (const auto & grouping_set : query_node_typed.getGroupBy().getNodes())
            {
                for (const auto & group_by_elem : grouping_set->as<ListNode>()->getNodes())
                    scope.nullable_group_by_keys.insert(group_by_elem);
            }
        }
    }
    else
    {
        replaceNodesWithPositionalArguments(query_node_typed.getGroupByNode(), query_node_typed.getProjection().getNodes(), scope);

        resolveExpressionNodeList(query_node_typed.getGroupByNode(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

        // Remove redundant calls to `tuple` function. It simplifies checking if expression is an aggregation key.
        // It's required to support queries like: SELECT number FROM numbers(3) GROUP BY (number, number % 2)
        auto & group_by_list = query_node_typed.getGroupBy().getNodes();
        expandTuplesInList(group_by_list);

        if (scope.group_by_use_nulls)
        {
            for (const auto & group_by_elem : query_node_typed.getGroupBy().getNodes())
                scope.nullable_group_by_keys.insert(group_by_elem);
        }
    }
}

/** Resolve interpolate columns nodes list.
  */
void QueryAnalyzer::resolveInterpolateColumnsNodeList(QueryTreeNodePtr & interpolate_node_list, IdentifierResolveScope & scope)
{
    auto & interpolate_node_list_typed = interpolate_node_list->as<ListNode &>();

    for (auto & interpolate_node : interpolate_node_list_typed.getNodes())
    {
        auto & interpolate_node_typed = interpolate_node->as<InterpolateNode &>();

        auto column_to_interpolate_name = interpolate_node_typed.getExpressionName();

        resolveExpressionNode(interpolate_node_typed.getExpression(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

        bool is_column_constant = interpolate_node_typed.getExpression()->getNodeType() == QueryTreeNodeType::CONSTANT;

        auto & interpolation_to_resolve = interpolate_node_typed.getInterpolateExpression();
        IdentifierResolveScope interpolate_scope(interpolation_to_resolve, &scope /*parent_scope*/);

        auto fake_column_node = std::make_shared<ColumnNode>(NameAndTypePair(column_to_interpolate_name, interpolate_node_typed.getExpression()->getResultType()), interpolate_node);
        if (is_column_constant)
            interpolate_scope.expression_argument_name_to_node.emplace(column_to_interpolate_name, fake_column_node);

        resolveExpressionNode(interpolation_to_resolve, interpolate_scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
    }
}

/** Resolve window nodes list.
  */
void QueryAnalyzer::resolveWindowNodeList(QueryTreeNodePtr & window_node_list, IdentifierResolveScope & scope)
{
    auto & window_node_list_typed = window_node_list->as<ListNode &>();
    for (auto & node : window_node_list_typed.getNodes())
        resolveWindow(node, scope);
}

NamesAndTypes QueryAnalyzer::resolveProjectionExpressionNodeList(QueryTreeNodePtr & projection_node_list, IdentifierResolveScope & scope)
{
    ProjectionNames projection_names = resolveExpressionNodeList(projection_node_list, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    auto projection_nodes = projection_node_list->as<ListNode &>().getNodes();
    size_t projection_nodes_size = projection_nodes.size();

    NamesAndTypes projection_columns;
    projection_columns.reserve(projection_nodes_size);

    for (size_t i = 0; i < projection_nodes_size; ++i)
    {
        auto projection_node = projection_nodes[i];

        if (!IdentifierResolver::isExpressionNodeType(projection_node->getNodeType()))
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Projection node must be constant, function, column, query or union");

        projection_columns.emplace_back(projection_names[i], projection_node->getResultType());
    }

    return projection_columns;
}

/** Initialize query join tree node.
  *
  * 1. Resolve identifiers.
  * 2. Register table, table function, query, union, join, array join nodes in scope table expressions in resolve process.
  */
void QueryAnalyzer::initializeQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope)
{
    std::deque<QueryTreeNodePtr *> join_tree_node_ptrs_to_process_queue;
    join_tree_node_ptrs_to_process_queue.push_back(&join_tree_node);

    while (!join_tree_node_ptrs_to_process_queue.empty())
    {
        auto * current_join_tree_node_ptr = join_tree_node_ptrs_to_process_queue.front();
        join_tree_node_ptrs_to_process_queue.pop_front();

        auto & current_join_tree_node = *current_join_tree_node_ptr;
        auto current_join_tree_node_type = current_join_tree_node->getNodeType();

        switch (current_join_tree_node_type)
        {
            case QueryTreeNodeType::IDENTIFIER:
            {
                auto & from_table_identifier = current_join_tree_node->as<IdentifierNode &>();
                auto table_identifier_lookup = IdentifierLookup{from_table_identifier.getIdentifier(), IdentifierLookupContext::TABLE_EXPRESSION};

                auto from_table_identifier_alias = from_table_identifier.getAlias();

                IdentifierResolveSettings resolve_settings;
                /// In join tree initialization ignore join tree as identifier lookup source
                resolve_settings.allow_to_check_join_tree = false;
                /** Disable resolve of subquery during identifier resolution.
                  * Example: SELECT * FROM (SELECT 1) AS t1, t1;
                  * During `t1` identifier resolution we resolve it into subquery SELECT 1, but we want to disable
                  * subquery resolution at this stage, because JOIN TREE of parent query is not resolved.
                  */
                resolve_settings.allow_to_resolve_subquery_during_identifier_resolution = false;

                scope.pushExpressionNode(current_join_tree_node);

                auto table_identifier_resolve_result = tryResolveIdentifier(table_identifier_lookup, scope, resolve_settings);

                scope.popExpressionNode();
                bool expression_was_root = scope.expressions_in_resolve_process_stack.empty();
                if (expression_was_root)
                    scope.non_cached_identifier_lookups_during_expression_resolve.clear();

                auto resolved_identifier = table_identifier_resolve_result.resolved_identifier;

                if (!resolved_identifier)
                    throw Exception(ErrorCodes::UNKNOWN_TABLE,
                        "Unknown table expression identifier '{}' in scope {}",
                        from_table_identifier.getIdentifier().getFullName(),
                        scope.scope_node->formatASTForErrorMessage());

                resolved_identifier = resolved_identifier->clone();

                /// Update alias name to table expression map
                auto table_expression_it = scope.aliases.alias_name_to_table_expression_node.find(from_table_identifier_alias);
                if (table_expression_it != scope.aliases.alias_name_to_table_expression_node.end())
                    table_expression_it->second = resolved_identifier;

                auto table_expression_modifiers = from_table_identifier.getTableExpressionModifiers();

                auto * resolved_identifier_query_node = resolved_identifier->as<QueryNode>();
                auto * resolved_identifier_union_node = resolved_identifier->as<UnionNode>();

                if (resolved_identifier_query_node || resolved_identifier_union_node)
                {
                    if (table_expression_modifiers.has_value())
                    {
                        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                            "Table expression modifiers {} are not supported for subquery {}",
                            table_expression_modifiers->formatForErrorMessage(),
                            resolved_identifier->formatASTForErrorMessage());
                    }
                }
                else if (auto * resolved_identifier_table_node = resolved_identifier->as<TableNode>())
                {
                    if (table_expression_modifiers.has_value())
                        resolved_identifier_table_node->setTableExpressionModifiers(*table_expression_modifiers);
                }
                else if (auto * resolved_identifier_table_function_node = resolved_identifier->as<TableFunctionNode>())
                {
                    if (table_expression_modifiers.has_value())
                        resolved_identifier_table_function_node->setTableExpressionModifiers(*table_expression_modifiers);
                }
                else
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Identifier in JOIN TREE '{}' resolved into unexpected table expression. In scope {}",
                        from_table_identifier.getIdentifier().getFullName(),
                        scope.scope_node->formatASTForErrorMessage());
                }

                auto current_join_tree_node_alias = current_join_tree_node->getAlias();
                resolved_identifier->setAlias(current_join_tree_node_alias);
                current_join_tree_node = resolved_identifier;

                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::QUERY:
            {
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::UNION:
            {
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::TABLE_FUNCTION:
            {
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::TABLE:
            {
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::ARRAY_JOIN:
            {
                auto & array_join = current_join_tree_node->as<ArrayJoinNode &>();
                join_tree_node_ptrs_to_process_queue.push_back(&array_join.getTableExpression());
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                break;
            }
            case QueryTreeNodeType::JOIN:
            {
                auto & join = current_join_tree_node->as<JoinNode &>();
                join_tree_node_ptrs_to_process_queue.push_back(&join.getLeftTableExpression());
                join_tree_node_ptrs_to_process_queue.push_back(&join.getRightTableExpression());
                scope.table_expressions_in_resolve_process.insert(current_join_tree_node.get());
                ++scope.joins_count;
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Query FROM section expected table, table function, query, UNION, ARRAY JOIN or JOIN. Actual {} {}. In scope {}",
                    current_join_tree_node->getNodeTypeName(),
                    current_join_tree_node->formatASTForErrorMessage(),
                    scope.scope_node->formatASTForErrorMessage());
            }
        }
    }
}

/// Initialize table expression data for table expression node
void QueryAnalyzer::initializeTableExpressionData(const QueryTreeNodePtr & table_expression_node, IdentifierResolveScope & scope)
{
    auto * table_node = table_expression_node->as<TableNode>();
    auto * query_node = table_expression_node->as<QueryNode>();
    auto * union_node = table_expression_node->as<UnionNode>();
    auto * table_function_node = table_expression_node->as<TableFunctionNode>();

    if (!table_node && !table_function_node && !query_node && !union_node)
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
        "Unexpected table expression. Expected table, table function, query or union node. Actual {}. In scope {}",
        table_expression_node->formatASTForErrorMessage(),
        scope.scope_node->formatASTForErrorMessage());

    auto table_expression_data_it = scope.table_expression_node_to_data.find(table_expression_node);
    if (table_expression_data_it != scope.table_expression_node_to_data.end())
        return;

    AnalysisTableExpressionData table_expression_data;

    if (table_node)
    {
        if (!table_node->getTemporaryTableName().empty())
        {
            table_expression_data.table_name = table_node->getTemporaryTableName();
            table_expression_data.table_expression_name = table_node->getTemporaryTableName();
        }
        else
        {
            const auto & table_storage_id = table_node->getStorageID();
            table_expression_data.database_name = table_storage_id.database_name;
            table_expression_data.table_name = table_storage_id.table_name;
            table_expression_data.table_expression_name = table_storage_id.getFullNameNotQuoted();
        }

        table_expression_data.table_expression_description = "table";
    }
    else if (query_node || union_node)
    {
        table_expression_data.table_name = query_node ? query_node->getCTEName() : union_node->getCTEName();
        table_expression_data.table_expression_description = "subquery";
    }
    else if (table_function_node)
    {
        table_expression_data.table_expression_description = "table_function";
    }

    if (table_expression_node->hasAlias())
        table_expression_data.table_expression_name = table_expression_node->getAlias();

    if (table_node || table_function_node)
    {
        const auto & storage_snapshot = table_node ? table_node->getStorageSnapshot() : table_function_node->getStorageSnapshot();

        auto get_column_options = GetColumnsOptions(GetColumnsOptions::All).withExtendedObjects().withVirtuals();
        if (storage_snapshot->storage.supportsSubcolumns())
        {
            get_column_options.withSubcolumns();
            table_expression_data.supports_subcolumns = true;
        }

        auto column_names_and_types = storage_snapshot->getColumns(get_column_options);
        table_expression_data.column_names_and_types = NamesAndTypes(column_names_and_types.begin(), column_names_and_types.end());

        const auto & columns_description = storage_snapshot->metadata->getColumns();

        std::vector<std::pair<std::string, ColumnNodePtr>> alias_columns_to_resolve;
        ColumnNameToColumnNodeMap column_name_to_column_node;
        column_name_to_column_node.reserve(column_names_and_types.size());

        /** For ALIAS columns in table we must additionally analyze ALIAS expressions.
          * Example: CREATE TABLE test_table (id UInt64, alias_value_1 ALIAS id + 5);
          *
          * To do that we collect alias columns and build table column name to column node map.
          * For each alias column we build identifier resolve scope, initialize it with table column name to node map
          * and resolve alias column.
          */
        for (const auto & column_name_and_type : table_expression_data.column_names_and_types)
        {
            for (const auto & subcolumn : columns_description.getSubcolumns(column_name_and_type.name))
                table_expression_data.subcolumn_names.insert(subcolumn.name);
            const auto & column_default = columns_description.getDefault(column_name_and_type.name);

            if (column_default && column_default->kind == ColumnDefaultKind::Alias)
            {
                auto alias_expression = buildQueryTree(column_default->expression, scope.context);
                auto column_node = std::make_shared<ColumnNode>(column_name_and_type, std::move(alias_expression), table_expression_node);
                column_name_to_column_node.emplace(column_name_and_type.name, column_node);
                alias_columns_to_resolve.emplace_back(column_name_and_type.name, column_node);
            }
            else
            {
                auto column_node = std::make_shared<ColumnNode>(column_name_and_type, table_expression_node);
                column_name_to_column_node.emplace(column_name_and_type.name, column_node);
            }
        }

        for (auto & [alias_column_to_resolve_name, alias_column_to_resolve] : alias_columns_to_resolve)
        {
            /** Alias column could be potentially resolved during resolve of other ALIAS column.
              * Example: CREATE TABLE test_table (id UInt64, alias_value_1 ALIAS id + alias_value_2, alias_value_2 ALIAS id + 5) ENGINE=TinyLog;
              *
              * During resolve of alias_value_1, alias_value_2 column will be resolved.
              */
            alias_column_to_resolve = column_name_to_column_node[alias_column_to_resolve_name];

            IdentifierResolveScope alias_column_resolve_scope(alias_column_to_resolve, nullptr /*parent_scope*/);
            alias_column_resolve_scope.column_name_to_column_node = std::move(column_name_to_column_node);
            alias_column_resolve_scope.context = scope.context;

            /// Initialize aliases in alias column scope
            QueryExpressionsAliasVisitor visitor(alias_column_resolve_scope.aliases);
            visitor.visit(alias_column_to_resolve->getExpression());

            resolveExpressionNode(alias_column_resolve_scope.scope_node,
                alias_column_resolve_scope,
                false /*allow_lambda_expression*/,
                false /*allow_table_expression*/);
            auto & resolved_expression = alias_column_to_resolve->getExpression();
            if (!resolved_expression->getResultType()->equals(*alias_column_to_resolve->getResultType()))
                resolved_expression = buildCastFunction(resolved_expression, alias_column_to_resolve->getResultType(), scope.context, true);
            column_name_to_column_node = std::move(alias_column_resolve_scope.column_name_to_column_node);
            column_name_to_column_node[alias_column_to_resolve_name] = alias_column_to_resolve;
        }

        table_expression_data.column_name_to_column_node = std::move(column_name_to_column_node);
    }
    else if (query_node || union_node)
    {
        table_expression_data.column_names_and_types = query_node ? query_node->getProjectionColumns() : union_node->computeProjectionColumns();
        table_expression_data.column_name_to_column_node.reserve(table_expression_data.column_names_and_types.size());

        for (const auto & column_name_and_type : table_expression_data.column_names_and_types)
        {
            auto column_node = std::make_shared<ColumnNode>(column_name_and_type, table_expression_node);
            table_expression_data.column_name_to_column_node.emplace(column_name_and_type.name, column_node);
        }
    }

    table_expression_data.column_identifier_first_parts.reserve(table_expression_data.column_name_to_column_node.size());

    for (auto & [column_name, _] : table_expression_data.column_name_to_column_node)
    {
        Identifier column_name_identifier(column_name);
        table_expression_data.column_identifier_first_parts.insert(column_name_identifier.at(0));
    }

    if (auto * scope_query_node = scope.scope_node->as<QueryNode>())
    {
        auto left_table_expression = extractLeftTableExpression(scope_query_node->getJoinTree());
        if (table_expression_node.get() == left_table_expression.get() && scope.joins_count == 1
            && scope.context->getSettingsRef()[Setting::single_join_prefer_left_table])
            table_expression_data.should_qualify_columns = false;
    }

    scope.table_expression_node_to_data.emplace(table_expression_node, std::move(table_expression_data));
}

bool findIdentifier(const FunctionNode & function)
{
    for (const auto & argument : function.getArguments())
    {
        if (argument->as<IdentifierNode>())
            return true;
        if (const auto * f = argument->as<FunctionNode>(); f && findIdentifier(*f))
            return true;
    }
    return false;
}

/// Resolve table function node in scope
void QueryAnalyzer::resolveTableFunction(QueryTreeNodePtr & table_function_node,
    IdentifierResolveScope & scope,
    QueryExpressionsAliasVisitor & expressions_visitor,
    bool nested_table_function)
{
    auto & table_function_node_typed = table_function_node->as<TableFunctionNode &>();

    if (!nested_table_function)
        expressions_visitor.visit(table_function_node_typed.getArgumentsNode());

    const auto & table_function_name = table_function_node_typed.getTableFunctionName();

    auto & scope_context = scope.context;

    TableFunctionPtr table_function_ptr = TableFunctionFactory::instance().tryGet(table_function_name, scope_context);
    if (!table_function_ptr)
    {
        String database_name = scope_context->getCurrentDatabase();
        String table_name;

        auto function_ast = table_function_node->toAST();
        Identifier table_identifier{table_function_name};
        if (table_identifier.getPartsSize() == 1)
        {
            table_name = table_identifier[0];
        }
        else if (table_identifier.getPartsSize() == 2)
        {
            database_name = table_identifier[0];
            table_name = table_identifier[1];
        }

        /// Collect parametrized view arguments
        NameToNameMap view_params;
        for (const auto & argument : table_function_node_typed.getArguments())
        {
            if (auto * arg_func = argument->as<FunctionNode>())
            {
                if (arg_func->getFunctionName() != "equals")
                    continue;

                auto nodes = arg_func->getArguments().getNodes();
                if (nodes.size() != 2)
                    continue;

                if (auto * identifier_node = nodes[0]->as<IdentifierNode>())
                {
                    resolveExpressionNode(nodes[1], scope, /* allow_lambda_expression */false, /* allow_table_function */false);
                    if (auto * constant = nodes[1]->as<ConstantNode>())
                    {
                        /// Serialize the constant value using datatype specific
                        /// interfaces to match the deserialization in ReplaceQueryParametersVistor.
                        WriteBufferFromOwnString buf;
                        const auto & value = constant->getValue();
                        auto real_type = constant->getResultType();
                        auto temporary_column  = real_type->createColumn();
                        temporary_column->insert(value);
                        real_type->getDefaultSerialization()->serializeTextEscaped(*temporary_column, 0, buf, {});
                        view_params[identifier_node->getIdentifier().getFullName()] = buf.str();
                    }
                }
            }
        }

        auto context = scope_context->getQueryContext();
        auto parametrized_view_storage = context->buildParametrizedViewStorage(
            database_name,
            table_name,
            view_params);

        if (parametrized_view_storage)
        {
            auto fake_table_node = std::make_shared<TableNode>(parametrized_view_storage, scope_context);
            fake_table_node->setAlias(table_function_node->getAlias());
            table_function_node = fake_table_node;
            return;
        }

        auto hints = TableFunctionFactory::instance().getHints(table_function_name);
        if (!hints.empty())
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
                "Unknown table function {}. Maybe you meant: {}",
                table_function_name,
                DB::toString(hints));
        else
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
                "Unknown table function {}",
                table_function_name);
    }

    QueryTreeNodes result_table_function_arguments;

    auto skip_analysis_arguments_indexes = table_function_ptr->skipAnalysisForArguments(table_function_node, scope_context);

    auto & table_function_arguments = table_function_node_typed.getArguments().getNodes();
    size_t table_function_arguments_size = table_function_arguments.size();

    for (size_t table_function_argument_index = 0; table_function_argument_index < table_function_arguments_size; ++table_function_argument_index)
    {
        auto & table_function_argument = table_function_arguments[table_function_argument_index];

        auto skip_argument_index_it = std::find(skip_analysis_arguments_indexes.begin(),
            skip_analysis_arguments_indexes.end(),
            table_function_argument_index);
        if (skip_argument_index_it != skip_analysis_arguments_indexes.end())
        {
            result_table_function_arguments.push_back(table_function_argument);
            continue;
        }

        if (auto * identifier_node = table_function_argument->as<IdentifierNode>())
        {
            const auto & unresolved_identifier = identifier_node->getIdentifier();
            auto identifier_resolve_result = tryResolveIdentifier({unresolved_identifier, IdentifierLookupContext::EXPRESSION}, scope);
            auto resolved_identifier = std::move(identifier_resolve_result.resolved_identifier);

            if (resolved_identifier && resolved_identifier->getNodeType() == QueryTreeNodeType::CONSTANT)
                result_table_function_arguments.push_back(std::move(resolved_identifier));
            else
                result_table_function_arguments.push_back(table_function_argument);

            continue;
        }
        else if (auto * table_function_argument_function = table_function_argument->as<FunctionNode>())
        {
            const auto & table_function_argument_function_name = table_function_argument_function->getFunctionName();
            if (TableFunctionFactory::instance().isTableFunctionName(table_function_argument_function_name))
            {
                auto table_function_node_to_resolve_typed = std::make_shared<TableFunctionNode>(table_function_argument_function_name);
                table_function_node_to_resolve_typed->getArgumentsNode() = table_function_argument_function->getArgumentsNode();

                QueryTreeNodePtr table_function_node_to_resolve = std::move(table_function_node_to_resolve_typed);
                resolveTableFunction(table_function_node_to_resolve, scope, expressions_visitor, true /*nested_table_function*/);

                result_table_function_arguments.push_back(std::move(table_function_node_to_resolve));
                continue;
            }
        }

        /** Table functions arguments can contain expressions with invalid identifiers.
          * We cannot skip analysis for such arguments, because some table functions cannot provide
          * information if analysis for argument should be skipped until other arguments will be resolved.
          *
          * Example: SELECT key from remote('127.0.0.{1,2}', view(select number AS key from numbers(2)), cityHash64(key));
          * Example: SELECT id from remote('127.0.0.{1,2}', 'default', 'test_table', cityHash64(id));
          */
        try
        {
            resolveExpressionNode(table_function_argument, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        }
        catch (const Exception & exception)
        {
            if (exception.code() == ErrorCodes::UNKNOWN_IDENTIFIER)
            {
                result_table_function_arguments.push_back(table_function_argument);
                continue;
            }

            throw;
        }

        if (auto * expression_list = table_function_argument->as<ListNode>())
        {
            for (auto & expression_list_node : expression_list->getNodes())
                result_table_function_arguments.push_back(expression_list_node);
        }
        else
        {
            result_table_function_arguments.push_back(table_function_argument);
        }
    }

    table_function_node_typed.getArguments().getNodes() = std::move(result_table_function_arguments);

    auto table_function_ast = table_function_node_typed.toAST();
    table_function_ptr->parseArguments(table_function_ast, scope_context);


    uint64_t use_structure_from_insertion_table_in_table_functions
        = scope_context->getSettingsRef()[Setting::use_structure_from_insertion_table_in_table_functions];
    if (!nested_table_function && use_structure_from_insertion_table_in_table_functions && scope_context->hasInsertionTable()
        && table_function_ptr->needStructureHint())
    {
        const auto & insertion_table = scope_context->getInsertionTable();
        if (!insertion_table.empty())
        {
            const auto & insert_columns = DatabaseCatalog::instance()
                                              .getTable(insertion_table, scope_context)
                                              ->getInMemoryMetadataPtr()
                                              ->getColumns();
            const auto & insert_column_names = scope_context->hasInsertionTableColumnNames() ? *scope_context->getInsertionTableColumnNames() : insert_columns.getOrdinary().getNames();
            DB::ColumnsDescription structure_hint;

            bool use_columns_from_insert_query = true;

            /// Insert table matches columns against SELECT expression by position, so we want to map
            /// insert table columns to table function columns through names from SELECT expression.

            auto insert_column_name_it = insert_column_names.begin();
            auto insert_column_names_end = insert_column_names.end();  /// end iterator of the range covered by possible asterisk
            auto virtual_column_names = table_function_ptr->getVirtualsToCheckBeforeUsingStructureHint();
            bool asterisk = false;
            const auto & expression_list = scope.scope_node->as<QueryNode &>().getProjection();
            auto expression = expression_list.begin();

            /// We want to go through SELECT expression list and correspond each expression to column in insert table
            /// which type will be used as a hint for the file structure inference.
            for (; expression != expression_list.end() && insert_column_name_it != insert_column_names_end; ++expression)
            {
                if (auto * identifier_node = (*expression)->as<IdentifierNode>())
                {

                    if (!virtual_column_names.contains(identifier_node->getIdentifier().getFullName()))
                    {
                        if (asterisk)
                        {
                            if (use_structure_from_insertion_table_in_table_functions == 1)
                                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Asterisk cannot be mixed with column list in INSERT SELECT query.");

                            use_columns_from_insert_query = false;
                            break;
                        }

                        ColumnDescription column = insert_columns.get(*insert_column_name_it);
                        column.name = identifier_node->getIdentifier().getFullName();
                        /// Change ephemeral columns to default columns.
                        column.default_desc.kind = ColumnDefaultKind::Default;
                        structure_hint.add(std::move(column));
                    }

                    /// Once we hit asterisk we want to find end of the range covered by asterisk
                    /// contributing every further SELECT expression to the tail of insert structure
                    if (asterisk)
                        --insert_column_names_end;
                    else
                        ++insert_column_name_it;
                }
                else if (auto * matcher_node = (*expression)->as<MatcherNode>(); matcher_node && matcher_node->getMatcherType() == MatcherNodeType::ASTERISK)
                {
                    if (asterisk)
                    {
                        if (use_structure_from_insertion_table_in_table_functions == 1)
                            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Only one asterisk can be used in INSERT SELECT query.");

                        use_columns_from_insert_query = false;
                        break;
                    }
                    if (!structure_hint.empty())
                    {
                        if (use_structure_from_insertion_table_in_table_functions == 1)
                            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Asterisk cannot be mixed with column list in INSERT SELECT query.");

                        use_columns_from_insert_query = false;
                        break;
                    }

                    asterisk = true;
                }
                else if (auto * function = (*expression)->as<FunctionNode>())
                {
                    if (use_structure_from_insertion_table_in_table_functions == 2 && findIdentifier(*function))
                    {
                        use_columns_from_insert_query = false;
                        break;
                    }

                    /// Once we hit asterisk we want to find end of the range covered by asterisk
                    /// contributing every further SELECT expression to the tail of insert structure
                    if (asterisk)
                        --insert_column_names_end;
                    else
                        ++insert_column_name_it;
                }
                else
                {
                    /// Once we hit asterisk we want to find end of the range covered by asterisk
                    /// contributing every further SELECT expression to the tail of insert structure
                    if (asterisk)
                        --insert_column_names_end;
                    else
                        ++insert_column_name_it;
                }
            }

            if (use_structure_from_insertion_table_in_table_functions == 2 && !asterisk)
            {
                /// For input function we should check if input format supports reading subset of columns.
                if (table_function_ptr->getName() == "input")
                    use_columns_from_insert_query = FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(scope.context->getInsertFormat(), scope.context);
                else
                    use_columns_from_insert_query = table_function_ptr->supportsReadingSubsetOfColumns(scope.context);
            }

            if (use_columns_from_insert_query)
            {
                if (expression == expression_list.end())
                {
                    /// Append tail of insert structure to the hint
                    if (asterisk)
                    {
                        for (; insert_column_name_it != insert_column_names_end; ++insert_column_name_it)
                        {
                            ColumnDescription column = insert_columns.get(*insert_column_name_it);
                            /// Change ephemeral columns to default columns.
                            column.default_desc.kind = ColumnDefaultKind::Default;
                            structure_hint.add(std::move(column));
                        }
                    }

                    if (!structure_hint.empty())
                        table_function_ptr->setStructureHint(structure_hint);
                }
                else if (use_structure_from_insertion_table_in_table_functions == 1)
                    throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH, "Number of columns in insert table less than required by SELECT expression.");
            }
        }
    }

    auto table_function_storage = scope_context->getQueryContext()->executeTableFunction(table_function_ast, table_function_ptr);
    table_function_node_typed.resolve(std::move(table_function_ptr), std::move(table_function_storage), scope_context, std::move(skip_analysis_arguments_indexes));
}

/// Resolve array join node in scope
void QueryAnalyzer::resolveArrayJoin(QueryTreeNodePtr & array_join_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor)
{
    auto & array_join_node_typed = array_join_node->as<ArrayJoinNode &>();
    resolveQueryJoinTreeNode(array_join_node_typed.getTableExpression(), scope, expressions_visitor);

    std::unordered_set<String> array_join_column_names;

    /// Wrap array join expressions into column nodes, where array join expression is inner expression

    auto & array_join_nodes = array_join_node_typed.getJoinExpressions().getNodes();
    size_t array_join_nodes_size = array_join_nodes.size();

    if (array_join_nodes_size == 0)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "ARRAY JOIN requires at least single expression");

    std::vector<QueryTreeNodePtr> array_join_column_expressions;
    array_join_column_expressions.reserve(array_join_nodes_size);

    for (auto & array_join_expression : array_join_nodes)
    {
        auto array_join_expression_alias = array_join_expression->getAlias();

        for (const auto & elem : array_join_nodes)
        {
            if (elem->hasAlias())
                scope.aliases.array_join_aliases.insert(elem->getAlias());

            for (auto & child : elem->getChildren())
            {
                if (child)
                    expressions_visitor.visit(child);
            }
        }

        std::string identifier_full_name;

        if (auto * identifier_node = array_join_expression->as<IdentifierNode>())
            identifier_full_name = identifier_node->getIdentifier().getFullName();

        resolveExpressionNode(array_join_expression, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/, true /*ignore_alias*/);

        auto process_array_join_expression = [&](QueryTreeNodePtr & expression)
        {
            auto result_type = expression->getResultType();
            bool is_array_type = isArray(result_type);
            bool is_map_type = isMap(result_type);

            if (!is_array_type && !is_map_type)
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "ARRAY JOIN {} requires expression {} with Array or Map type. Actual {}. In scope {}",
                    array_join_node_typed.formatASTForErrorMessage(),
                    expression->formatASTForErrorMessage(),
                    result_type->getName(),
                    scope.scope_node->formatASTForErrorMessage());

            if (is_map_type)
                result_type = assert_cast<const DataTypeMap &>(*result_type).getNestedType();

            result_type = assert_cast<const DataTypeArray &>(*result_type).getNestedType();

            String array_join_column_name;

            if (!array_join_expression_alias.empty())
            {
                array_join_column_name = array_join_expression_alias;
            }
            else if (auto * array_join_expression_inner_column = array_join_expression->as<ColumnNode>())
            {
                array_join_column_name = array_join_expression_inner_column->getColumnName();
            }
            else if (!identifier_full_name.empty())
            {
                array_join_column_name = identifier_full_name;
            }
            else
            {
                array_join_column_name = "__array_join_expression_" + std::to_string(array_join_expressions_counter);
                ++array_join_expressions_counter;
            }

            if (array_join_column_names.contains(array_join_column_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "ARRAY JOIN {} multiple columns with name {}. In scope {}",
                    array_join_node_typed.formatASTForErrorMessage(),
                    array_join_column_name,
                    scope.scope_node->formatASTForErrorMessage());
            array_join_column_names.emplace(array_join_column_name);

            NameAndTypePair array_join_column(array_join_column_name, result_type);
            auto array_join_column_node = std::make_shared<ColumnNode>(std::move(array_join_column), expression, array_join_node);
            array_join_column_node->setAlias(array_join_expression_alias);
            array_join_column_expressions.push_back(std::move(array_join_column_node));
        };

        // Support ARRAY JOIN COLUMNS(...). COLUMNS transformer is resolved to list of columns.
        if (auto * columns_list = array_join_expression->as<ListNode>())
        {
            for (auto & array_join_subexpression : columns_list->getNodes())
                process_array_join_expression(array_join_subexpression);
        }
        else
        {
            process_array_join_expression(array_join_expression);
        }
    }

    array_join_nodes = std::move(array_join_column_expressions);
}

void QueryAnalyzer::checkDuplicateTableNamesOrAlias(const QueryTreeNodePtr & join_node, QueryTreeNodePtr & left_table_expr, QueryTreeNodePtr & right_table_expr, IdentifierResolveScope & scope)
{
    Names column_names;
    if (!scope.context->getSettingsRef()[Setting::joined_subquery_requires_alias])
        return;

    if (join_node->as<JoinNode &>().getKind() != JoinKind::Paste)
        return;

    auto * left_node = left_table_expr->as<QueryNode>();
    auto * right_node = right_table_expr->as<QueryNode>();

    if (!left_node && !right_node)
        return;

    if (left_node)
        for (const auto & name_and_type : left_node->getProjectionColumns())
            column_names.push_back(name_and_type.name);
    if (right_node)
        for (const auto & name_and_type : right_node->getProjectionColumns())
            column_names.push_back(name_and_type.name);

    if (column_names.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Names of projection columns cannot be empty");

    std::sort(column_names.begin(), column_names.end());
    for (size_t i = 0; i < column_names.size() - 1; i++) // Check if there is no any duplicates because it will lead to broken result
        if (column_names[i] == column_names[i+1])
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Name of columns and aliases should be unique for this query (you can add/change aliases to avoid duplication)"
                            "While processing '{}'", join_node->formatASTForErrorMessage());
}

/// Resolve join node in scope
void QueryAnalyzer::resolveJoin(QueryTreeNodePtr & join_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor)
{
    auto & join_node_typed = join_node->as<JoinNode &>();

    resolveQueryJoinTreeNode(join_node_typed.getLeftTableExpression(), scope, expressions_visitor);
    validateJoinTableExpressionWithoutAlias(join_node, join_node_typed.getLeftTableExpression(), scope);

    resolveQueryJoinTreeNode(join_node_typed.getRightTableExpression(), scope, expressions_visitor);
    validateJoinTableExpressionWithoutAlias(join_node, join_node_typed.getRightTableExpression(), scope);

    if (!join_node_typed.getLeftTableExpression()->hasAlias() && !join_node_typed.getRightTableExpression()->hasAlias())
        checkDuplicateTableNamesOrAlias(join_node, join_node_typed.getLeftTableExpression(), join_node_typed.getRightTableExpression(), scope);

    if (join_node_typed.isOnJoinExpression())
    {
        expressions_visitor.visit(join_node_typed.getJoinExpression());
        auto join_expression = join_node_typed.getJoinExpression();
        resolveExpressionNode(join_expression, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        join_node_typed.getJoinExpression() = std::move(join_expression);
    }
    else if (join_node_typed.isUsingJoinExpression())
    {
        auto & join_using_list = join_node_typed.getJoinExpression()->as<ListNode &>();
        std::unordered_set<std::string> join_using_identifiers;

        for (auto & join_using_node : join_using_list.getNodes())
        {
            auto * identifier_node = join_using_node->as<IdentifierNode>();
            if (!identifier_node)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "JOIN {} USING clause expected identifier. Actual {}",
                    join_node_typed.formatASTForErrorMessage(),
                    join_using_node->formatASTForErrorMessage());

            const auto & identifier_full_name = identifier_node->getIdentifier().getFullName();

            if (join_using_identifiers.contains(identifier_full_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "JOIN {} identifier '{}' appears more than once in USING clause",
                    join_node_typed.formatASTForErrorMessage(),
                    identifier_full_name);

            join_using_identifiers.insert(identifier_full_name);

            const auto & settings = scope.context->getSettingsRef();

            /** While resolving JOIN USING identifier, try to resolve identifier from parent subquery projection.
              * Example: SELECT a + 1 AS b FROM (SELECT 1 AS a) t1 JOIN (SELECT 2 AS b) USING b
              * In this case `b` is not in the left table expression, but it is in the parent subquery projection.
              */
            auto try_resolve_identifier_from_query_projection = [this](const String & identifier_full_name_,
                                                                       const QueryTreeNodePtr & left_table_expression,
                                                                       const IdentifierResolveScope & scope_) -> QueryTreeNodePtr
            {
                const QueryNode * query_node = scope_.scope_node ? scope_.scope_node->as<QueryNode>() : nullptr;
                if (!query_node)
                    return nullptr;

                const auto & projection_list = query_node->getProjection();
                for (const auto & projection_node : projection_list.getNodes())
                {
                    if (projection_node->hasAlias() && identifier_full_name_ == projection_node->getAlias())
                    {
                        auto left_subquery = std::make_shared<QueryNode>(query_node->getMutableContext());
                        left_subquery->getProjection().getNodes().push_back(projection_node->clone());
                        left_subquery->getJoinTree() = left_table_expression;

                        IdentifierResolveScope left_subquery_scope(left_subquery, nullptr /*parent_scope*/);
                        resolveQuery(left_subquery, left_subquery_scope);

                        const auto & resolved_nodes = left_subquery->getProjection().getNodes();
                        if (resolved_nodes.size() == 1)
                        {
                            /// Create ColumnNode with expression from parent projection
                            return std::make_shared<ColumnNode>(
                                NameAndTypePair{identifier_full_name_, resolved_nodes.front()->getResultType()},
                                resolved_nodes.front(), left_table_expression);
                        }
                    }
                }
                return nullptr;
            };

            QueryTreeNodePtr result_left_table_expression = nullptr;
            /** With `analyzer_compatibility_join_using_top_level_identifier` alias in projection has higher priority than column from left table.
              * But if aliased expression cannot be resolved from left table, we get UNKNOW_IDENTIFIER error,
              * despite the fact that column from USING could be resolved from left table.
              * It's compatibility with a default behavior for old analyzer.
              */
            if (settings[Setting::analyzer_compatibility_join_using_top_level_identifier])
                result_left_table_expression = try_resolve_identifier_from_query_projection(identifier_full_name, join_node_typed.getLeftTableExpression(), scope);

            IdentifierLookup identifier_lookup{identifier_node->getIdentifier(), IdentifierLookupContext::EXPRESSION};
            if (!result_left_table_expression)
                result_left_table_expression = identifier_resolver.tryResolveIdentifierFromJoinTreeNode(identifier_lookup, join_node_typed.getLeftTableExpression(), scope);

            /** Here we may try to resolve identifier from projection in case it's not resolved from left table expression
              * and analyzer_compatibility_join_using_top_level_identifier is disabled.
              * For now we do not do this, because not all corner cases are clear.
              * But let's at least mention it in error message
              */
            /// if (!settings.analyzer_compatibility_join_using_top_level_identifier && !result_left_table_expression)
            ///     result_left_table_expression = try_resolve_identifier_from_query_projection(identifier_full_name, join_node_typed.getLeftTableExpression(), scope);

            if (!result_left_table_expression)
            {
                String extra_message;
                const QueryNode * query_node = scope.scope_node ? scope.scope_node->as<QueryNode>() : nullptr;
                if (settings[Setting::analyzer_compatibility_join_using_top_level_identifier] && query_node)
                {
                    for (const auto & projection_node : query_node->getProjection().getNodes())
                    {
                        if (projection_node->hasAlias() && identifier_full_name == projection_node->getAlias())
                        {
                            extra_message = fmt::format(
                                ", but alias '{}' is present in SELECT list."
                                " You may try to SET analyzer_compatibility_join_using_top_level_identifier = 1, to allow to use it in USING clause",
                                projection_node->formatASTForErrorMessage());
                            break;
                        }
                    }
                }

                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                    "JOIN {} using identifier '{}' cannot be resolved from left table expression{}. In scope {}",
                    join_node_typed.formatASTForErrorMessage(),
                    identifier_full_name,
                    extra_message,
                    scope.scope_node->formatASTForErrorMessage());
            }

            if (result_left_table_expression->getNodeType() != QueryTreeNodeType::COLUMN)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "JOIN {} using identifier '{}' must be resolved into column node from left table expression. In scope {}",
                    join_node_typed.formatASTForErrorMessage(),
                    identifier_full_name,
                    scope.scope_node->formatASTForErrorMessage());

            auto result_right_table_expression = identifier_resolver.tryResolveIdentifierFromJoinTreeNode(identifier_lookup, join_node_typed.getRightTableExpression(), scope);
            if (!result_right_table_expression)
                throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER,
                    "JOIN {} using identifier '{}' cannot be resolved from right table expression. In scope {}",
                    join_node_typed.formatASTForErrorMessage(),
                    identifier_full_name,
                    scope.scope_node->formatASTForErrorMessage());

            if (result_right_table_expression->getNodeType() != QueryTreeNodeType::COLUMN)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                    "JOIN {} using identifier '{}' must be resolved into column node from right table expression. In scope {}",
                    join_node_typed.formatASTForErrorMessage(),
                    identifier_full_name,
                    scope.scope_node->formatASTForErrorMessage());

            auto expression_types = DataTypes{result_left_table_expression->getResultType(), result_right_table_expression->getResultType()};
            DataTypePtr common_type = tryGetLeastSupertype(expression_types);

            if (!common_type)
                throw Exception(ErrorCodes::NO_COMMON_TYPE,
                    "JOIN {} cannot infer common type for {} and {} in USING for identifier '{}'. In scope {}",
                    join_node_typed.formatASTForErrorMessage(),
                    result_left_table_expression->getResultType()->getName(),
                    result_right_table_expression->getResultType()->getName(),
                    identifier_full_name,
                    scope.scope_node->formatASTForErrorMessage());

            NameAndTypePair join_using_column(identifier_full_name, common_type);
            ListNodePtr join_using_expression = std::make_shared<ListNode>(QueryTreeNodes{result_left_table_expression, result_right_table_expression});
            auto join_using_column_node = std::make_shared<ColumnNode>(std::move(join_using_column), std::move(join_using_expression), join_node);
            join_using_node = std::move(join_using_column_node);
        }
    }
}

/** Resolve query join tree.
  *
  * Query join tree must be initialized before calling this function.
  */
void QueryAnalyzer::resolveQueryJoinTreeNode(QueryTreeNodePtr & join_tree_node, IdentifierResolveScope & scope, QueryExpressionsAliasVisitor & expressions_visitor)
{
    auto from_node_type = join_tree_node->getNodeType();

    switch (from_node_type)
    {
        case QueryTreeNodeType::QUERY:
            [[fallthrough]];
        case QueryTreeNodeType::UNION:
        {
            resolveExpressionNode(join_tree_node, scope, false /*allow_lambda_expression*/, true /*allow_table_expression*/);
            break;
        }
        case QueryTreeNodeType::TABLE_FUNCTION:
        {
            resolveTableFunction(join_tree_node, scope, expressions_visitor, false /*nested_table_function*/);
            break;
        }
        case QueryTreeNodeType::TABLE:
        {
            break;
        }
        case QueryTreeNodeType::ARRAY_JOIN:
        {
            resolveArrayJoin(join_tree_node, scope, expressions_visitor);
            break;
        }
        case QueryTreeNodeType::JOIN:
        {
            resolveJoin(join_tree_node, scope, expressions_visitor);
            break;
        }
        case QueryTreeNodeType::IDENTIFIER:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Identifiers in FROM section must be already resolved. Node {}, scope {}",
                join_tree_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
        default:
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Query FROM section expected table, table function, query, ARRAY JOIN or JOIN. Actual {}. In scope {}",
                join_tree_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }

    auto join_tree_node_type = join_tree_node->getNodeType();
    if (IdentifierResolver::isTableExpressionNodeType(join_tree_node_type))
    {
        validateTableExpressionModifiers(join_tree_node, scope);
        initializeTableExpressionData(join_tree_node, scope);

        auto & query_node = scope.scope_node->as<QueryNode &>();
        auto & mutable_context = query_node.getMutableContext();

        if (!mutable_context->isDistributed())
        {
            bool is_distributed = false;

            if (auto * table_node = join_tree_node->as<TableNode>())
                is_distributed = table_node->getStorage()->isRemote();
            else if (auto * table_function_node = join_tree_node->as<TableFunctionNode>())
                is_distributed = table_function_node->getStorage()->isRemote();

            mutable_context->setDistributed(is_distributed);
        }
    }

    auto add_table_expression_alias_into_scope = [&](const QueryTreeNodePtr & table_expression_node)
    {
        const auto & alias_name = table_expression_node->getAlias();
        if (alias_name.empty())
            return;

        auto [it, inserted] = scope.aliases.alias_name_to_table_expression_node.emplace(alias_name, table_expression_node);
        if (!inserted)
            throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                "Duplicate aliases {} for table expressions in FROM section are not allowed. Try to register {}. Already registered {}.",
                alias_name,
                table_expression_node->formatASTForErrorMessage(),
                it->second->formatASTForErrorMessage());
    };

    add_table_expression_alias_into_scope(join_tree_node);
    scope.table_expressions_in_resolve_process.erase(join_tree_node.get());
}

/** Resolve query.
  * This function modifies query node during resolve. It is caller responsibility to clone query node before resolve
  * if it is needed for later use.
  *
  * query_node - query_tree_node that must have QueryNode type.
  * scope - query scope. It is caller responsibility to create it.
  *
  * Resolve steps:
  * 1. Validate subqueries depth, perform GROUP BY validation that does not depend on information about aggregate functions.
  * 2. Initialize query scope with aliases.
  * 3. Register CTE subqueries from WITH section in scope and remove them from WITH section.
  * 4. Resolve JOIN TREE.
  * 5. Resolve projection columns.
  * 6. Resolve expressions in other query parts.
  * 7. Validate nodes with duplicate aliases.
  * 8. Validate aggregate functions, GROUPING function, window functions.
  * 9. Remove WITH and WINDOW sections from query.
  * 10. Remove aliases from expression and lambda nodes.
  * 11. Resolve query tree node with projection columns.
  */
void QueryAnalyzer::resolveQuery(const QueryTreeNodePtr & query_node, IdentifierResolveScope & scope)
{
    size_t max_subquery_depth = scope.context->getSettingsRef()[Setting::max_subquery_depth];
    if (max_subquery_depth && scope.subquery_depth > max_subquery_depth)
        throw Exception(ErrorCodes::TOO_DEEP_SUBQUERIES, "Too deep subqueries. Maximum: {}", max_subquery_depth);

    auto & query_node_typed = query_node->as<QueryNode &>();

    if (query_node_typed.isCTE())
        ctes_in_resolve_process.insert(query_node_typed.getCTEName());

    bool is_rollup_or_cube = query_node_typed.isGroupByWithRollup() || query_node_typed.isGroupByWithCube();

    if (query_node_typed.isGroupByWithGroupingSets()
        && query_node_typed.isGroupByWithTotals()
        && query_node_typed.getGroupBy().getNodes().size() != 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH TOTALS and GROUPING SETS are not supported together");

    if (query_node_typed.isGroupByWithGroupingSets() && is_rollup_or_cube)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "GROUPING SETS are not supported together with ROLLUP and CUBE");

    if (query_node_typed.isGroupByWithRollup() && (query_node_typed.isGroupByWithGroupingSets() || query_node_typed.isGroupByWithCube()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ROLLUP is not supported together with GROUPING SETS and CUBE");

    if (query_node_typed.isGroupByWithCube() && (query_node_typed.isGroupByWithGroupingSets() || query_node_typed.isGroupByWithRollup()))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CUBE is not supported together with GROUPING SETS and ROLLUP");

    if (query_node_typed.hasHaving() && query_node_typed.isGroupByWithTotals() && is_rollup_or_cube)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH TOTALS and WITH ROLLUP or CUBE are not supported together in presence of HAVING");

    if (query_node_typed.hasQualify() && query_node_typed.isGroupByWithTotals() && is_rollup_or_cube)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "WITH TOTALS and WITH ROLLUP or CUBE are not supported together in presence of QUALIFY");

    /// Initialize aliases in query node scope
    QueryExpressionsAliasVisitor visitor(scope.aliases);

    if (query_node_typed.hasWith())
        visitor.visit(query_node_typed.getWithNode());

    if (!query_node_typed.getProjection().getNodes().empty())
        visitor.visit(query_node_typed.getProjectionNode());

    if (query_node_typed.getPrewhere())
        visitor.visit(query_node_typed.getPrewhere());

    if (query_node_typed.getWhere())
        visitor.visit(query_node_typed.getWhere());

    if (query_node_typed.hasGroupBy())
        visitor.visit(query_node_typed.getGroupByNode());

    if (query_node_typed.hasHaving())
        visitor.visit(query_node_typed.getHaving());

    if (query_node_typed.hasWindow())
        visitor.visit(query_node_typed.getWindowNode());

    if (query_node_typed.hasQualify())
        visitor.visit(query_node_typed.getQualify());

    if (query_node_typed.hasOrderBy())
        visitor.visit(query_node_typed.getOrderByNode());

    if (query_node_typed.hasInterpolate())
        visitor.visit(query_node_typed.getInterpolate());

    if (query_node_typed.hasLimitByLimit())
        visitor.visit(query_node_typed.getLimitByLimit());

    if (query_node_typed.hasLimitByOffset())
        visitor.visit(query_node_typed.getLimitByOffset());

    if (query_node_typed.hasLimitBy())
        visitor.visit(query_node_typed.getLimitByNode());

    if (query_node_typed.hasLimit())
        visitor.visit(query_node_typed.getLimit());

    if (query_node_typed.hasOffset())
        visitor.visit(query_node_typed.getOffset());

    /// Register CTE subqueries and remove them from WITH section

    auto & with_nodes = query_node_typed.getWith().getNodes();

    for (auto & node : with_nodes)
    {
        auto * subquery_node = node->as<QueryNode>();
        auto * union_node = node->as<UnionNode>();

        bool subquery_is_cte = (subquery_node && subquery_node->isCTE()) || (union_node && union_node->isCTE());
        if (!subquery_is_cte)
            continue;

        const auto & cte_name = subquery_node ? subquery_node->getCTEName() : union_node->getCTEName();

        auto [_, inserted] = scope.cte_name_to_query_node.emplace(cte_name, node);
        if (!inserted)
            throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                "CTE with name {} already exists. In scope {}",
                cte_name,
                scope.scope_node->formatASTForErrorMessage());
    }

    /** WITH section can be safely removed, because WITH section only can provide aliases to query expressions
      * and CTE for other sections to use.
      *
      * Example: WITH 1 AS constant, (x -> x + 1) AS lambda, a AS (SELECT * FROM test_table);
      */
    query_node_typed.getWith().getNodes().clear();

    for (auto & window_node : query_node_typed.getWindow().getNodes())
    {
        auto & window_node_typed = window_node->as<WindowNode &>();
        auto parent_window_name = window_node_typed.getParentWindowName();
        if (!parent_window_name.empty())
        {
            auto window_node_it = scope.window_name_to_window_node.find(parent_window_name);
            if (window_node_it == scope.window_name_to_window_node.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Window '{}' does not exist. In scope {}",
                    parent_window_name,
                    scope.scope_node->formatASTForErrorMessage());

            mergeWindowWithParentWindow(window_node, window_node_it->second, scope);
            window_node_typed.setParentWindowName({});
        }

        auto [_, inserted] = scope.window_name_to_window_node.emplace(window_node_typed.getAlias(), window_node);
        if (!inserted)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Window '{}' is already defined. In scope {}",
                window_node_typed.getAlias(),
                scope.scope_node->formatASTForErrorMessage());
    }

    /** Disable identifier cache during JOIN TREE resolve.
      * Depending on JOIN expression section, identifier with same name
      * can be resolved in different columns.
      *
      * Example: SELECT id FROM test_table AS t1 INNER JOIN test_table AS t2 ON t1.id = t2.id INNER JOIN test_table AS t3 ON t1.id = t3.id
      * In first join expression ON t1.id = t2.id t1.id is resolved into test_table.id column.
      * In second join expression ON t1.id = t3.id t1.id must be resolved into test_table.id column after first JOIN.
      */
    scope.use_identifier_lookup_to_result_cache = false;

    if (query_node_typed.getJoinTree())
    {
        TableExpressionsAliasVisitor table_expressions_visitor(scope);
        table_expressions_visitor.visit(query_node_typed.getJoinTree());

        initializeQueryJoinTreeNode(query_node_typed.getJoinTree(), scope);
        scope.aliases.alias_name_to_table_expression_node.clear();

        resolveQueryJoinTreeNode(query_node_typed.getJoinTree(), scope, visitor);
    }

    if (!scope.group_by_use_nulls)
        scope.use_identifier_lookup_to_result_cache = true;

    /// Resolve query node sections.

    NamesAndTypes projection_columns;

    if (!scope.group_by_use_nulls)
    {
        projection_columns = resolveProjectionExpressionNodeList(query_node_typed.getProjectionNode(), scope);
        if (query_node_typed.getProjection().getNodes().empty())
            throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED,
                "Empty list of columns in projection. In scope {}",
                scope.scope_node->formatASTForErrorMessage());
    }

    if (auto & prewhere_node = query_node_typed.getPrewhere())
    {
        resolveExpressionNode(prewhere_node, scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

        /** Expressions in PREWHERE with JOIN should not change their type.
          * Example: SELECT * FROM t1 JOIN t2 USING (a) PREWHERE a = 1
          * Column `a` in PREWHERE should be resolved from the left table
          * and should not change its type to Nullable or to the supertype of `a` from t1 and t2.
          * Here's a more complicated example where the column is somewhere inside an expression:
          * SELECT a + 1 as b FROM t1 JOIN t2 USING (id) PREWHERE b = 1
          * The expression `a + 1 as b` in the projection and in PREWHERE should have different `a`.
          */
        prewhere_node = prewhere_node->clone();
        ReplaceColumnsVisitor replace_visitor(scope.join_columns_with_changed_types, scope.context);
        replace_visitor.visit(prewhere_node);
    }

    if (query_node_typed.getWhere())
        resolveExpressionNode(query_node_typed.getWhere(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    if (query_node_typed.hasGroupBy())
        resolveGroupByNode(query_node_typed, scope);

    if (scope.group_by_use_nulls)
    {
        resolved_expressions.clear();
        /// Clone is needed cause aliases share subtrees.
        /// If not clone, the same (shared) subtree could be resolved again with different (Nullable) type
        /// See 03023_group_by_use_nulls_analyzer_crashes
        for (auto & [key, node] : scope.aliases.alias_name_to_expression_node_before_group_by)
            scope.aliases.alias_name_to_expression_node_after_group_by[key] = node->clone();

        scope.aliases.alias_name_to_expression_node = &scope.aliases.alias_name_to_expression_node_after_group_by;
    }

    if (query_node_typed.hasHaving())
        resolveExpressionNode(query_node_typed.getHaving(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    if (query_node_typed.hasWindow())
        resolveWindowNodeList(query_node_typed.getWindowNode(), scope);

    if (query_node_typed.hasQualify())
        resolveExpressionNode(query_node_typed.getQualify(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);

    if (query_node_typed.hasOrderBy())
    {
        replaceNodesWithPositionalArguments(query_node_typed.getOrderByNode(), query_node_typed.getProjection().getNodes(), scope);

        const auto & settings = scope.context->getSettingsRef();

        expandOrderByAll(query_node_typed, settings);
        resolveSortNodeList(query_node_typed.getOrderByNode(), scope);
    }

    if (query_node_typed.hasInterpolate())
        resolveInterpolateColumnsNodeList(query_node_typed.getInterpolate(), scope);

    if (query_node_typed.hasLimitByLimit())
    {
        resolveExpressionNode(query_node_typed.getLimitByLimit(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        convertLimitOffsetExpression(query_node_typed.getLimitByLimit(), "LIMIT BY LIMIT", scope);
    }

    if (query_node_typed.hasLimitByOffset())
    {
        resolveExpressionNode(query_node_typed.getLimitByOffset(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        convertLimitOffsetExpression(query_node_typed.getLimitByOffset(), "LIMIT BY OFFSET", scope);
    }

    if (query_node_typed.hasLimitBy())
    {
        replaceNodesWithPositionalArguments(query_node_typed.getLimitByNode(), query_node_typed.getProjection().getNodes(), scope);

        resolveExpressionNodeList(query_node_typed.getLimitByNode(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
    }

    if (query_node_typed.hasLimit())
    {
        resolveExpressionNode(query_node_typed.getLimit(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        convertLimitOffsetExpression(query_node_typed.getLimit(), "LIMIT", scope);
    }

    if (query_node_typed.hasOffset())
    {
        resolveExpressionNode(query_node_typed.getOffset(), scope, false /*allow_lambda_expression*/, false /*allow_table_expression*/);
        convertLimitOffsetExpression(query_node_typed.getOffset(), "OFFSET", scope);
    }

    if (scope.group_by_use_nulls)
    {
        projection_columns = resolveProjectionExpressionNodeList(query_node_typed.getProjectionNode(), scope);
        if (query_node_typed.getProjection().getNodes().empty())
            throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED,
                "Empty list of columns in projection. In scope {}",
                scope.scope_node->formatASTForErrorMessage());
    }

    /** Resolve nodes with duplicate aliases.
      * Table expressions cannot have duplicate aliases.
      *
      * Such nodes during scope aliases collection are placed into duplicated array.
      * After scope nodes are resolved, we can compare node with duplicate alias with
      * node from scope alias table.
      */
    for (const auto & node_with_duplicated_alias : scope.aliases.cloned_nodes_with_duplicated_aliases)
    {
        auto node = node_with_duplicated_alias;
        auto node_alias = node->getAlias();

        /// Add current alias to non cached set, because in case of cyclic alias identifier should not be substituted from cache.
        /// See 02896_cyclic_aliases_crash.
        resolveExpressionNode(node, scope, true /*allow_lambda_expression*/, true /*allow_table_expression*/);

        bool has_node_in_alias_table = false;

        auto it = scope.aliases.alias_name_to_expression_node->find(node_alias);
        if (it != scope.aliases.alias_name_to_expression_node->end())
        {
            has_node_in_alias_table = true;

            bool matched = it->second->isEqual(*node);
            if (!matched)
                /// Table expression could be resolved as scalar subquery,
                /// but for duplicating alias we allow table expression to be returned.
                /// So, check constant node source expression as well.
                if (const auto * constant_node = it->second->as<ConstantNode>())
                    if (const auto & source_expression = constant_node->getSourceExpression())
                        matched = source_expression->isEqual(*node);

            if (!matched)
                throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                    "Multiple expressions {} and {} for alias {}. In scope {}",
                    node->formatASTForErrorMessage(),
                    it->second->formatASTForErrorMessage(),
                    node_alias,
                    scope.scope_node->formatASTForErrorMessage());
        }

        it = scope.aliases.alias_name_to_lambda_node.find(node_alias);
        if (it != scope.aliases.alias_name_to_lambda_node.end())
        {
            has_node_in_alias_table = true;

            if (!it->second->isEqual(*node))
                throw Exception(ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS,
                    "Multiple expressions {} and {} for alias {}. In scope {}",
                    node->formatASTForErrorMessage(),
                    it->second->formatASTForErrorMessage(),
                    node_alias,
                    scope.scope_node->formatASTForErrorMessage());
        }

        if (!has_node_in_alias_table)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Node {} with duplicate alias {} does not exist in alias table. In scope {}",
                node->formatASTForErrorMessage(),
                node_alias,
                scope.scope_node->formatASTForErrorMessage());

        node->removeAlias();
    }

    expandGroupByAll(query_node_typed);

    validateFilters(query_node);
    validateAggregates(query_node, {.group_by_use_nulls = scope.group_by_use_nulls});

    for (const auto & column : projection_columns)
    {
        if (isNotCreatable(column.type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Invalid projection column with type {}. In scope {}",
                column.type->getName(),
                scope.scope_node->formatASTForErrorMessage());
    }

    /** WINDOW section can be safely removed, because WINDOW section can only provide window definition to window functions.
      *
      * Example: SELECT count(*) OVER w FROM test_table WINDOW w AS (PARTITION BY id);
      */
    query_node_typed.getWindow().getNodes().clear();

    /// Remove aliases from expression and lambda nodes

    for (auto & [_, node] : *scope.aliases.alias_name_to_expression_node)
        node->removeAlias();

    for (auto & [_, node] : scope.aliases.alias_name_to_lambda_node)
        node->removeAlias();

    query_node_typed.resolveProjectionColumns(std::move(projection_columns));

    if (query_node_typed.isCTE())
        ctes_in_resolve_process.erase(query_node_typed.getCTEName());
}

void QueryAnalyzer::resolveUnion(const QueryTreeNodePtr & union_node, IdentifierResolveScope & scope)
{
    auto & union_node_typed = union_node->as<UnionNode &>();

    if (union_node_typed.isCTE())
        ctes_in_resolve_process.insert(union_node_typed.getCTEName());

    auto & queries_nodes = union_node_typed.getQueries().getNodes();

    std::optional<RecursiveCTETable> recursive_cte_table;
    TableNodePtr recursive_cte_table_node;

    if (union_node_typed.isCTE() && union_node_typed.isRecursiveCTE())
    {
        auto & non_recursive_query = queries_nodes[0];
        bool non_recursive_query_is_query_node = non_recursive_query->getNodeType() == QueryTreeNodeType::QUERY;
        auto & non_recursive_query_mutable_context = non_recursive_query_is_query_node ? non_recursive_query->as<QueryNode &>().getMutableContext()
            :  non_recursive_query->as<UnionNode &>().getMutableContext();

        IdentifierResolveScope non_recursive_subquery_scope(non_recursive_query, &scope /*parent_scope*/);
        non_recursive_subquery_scope.subquery_depth = scope.subquery_depth + 1;

        if (non_recursive_query_is_query_node)
            resolveQuery(non_recursive_query, non_recursive_subquery_scope);
        else
            resolveUnion(non_recursive_query, non_recursive_subquery_scope);

        auto temporary_table_columns = non_recursive_query_is_query_node
            ? non_recursive_query->as<QueryNode &>().getProjectionColumns()
            : non_recursive_query->as<UnionNode &>().computeProjectionColumns();

        auto temporary_table_holder = std::make_shared<TemporaryTableHolder>(
            non_recursive_query_mutable_context,
            ColumnsDescription{NamesAndTypesList{temporary_table_columns.begin(), temporary_table_columns.end()}},
            ConstraintsDescription{},
            nullptr /*query*/,
            true /*create_for_global_subquery*/);
        auto temporary_table_storage = temporary_table_holder->getTable();

        recursive_cte_table_node = std::make_shared<TableNode>(temporary_table_storage, non_recursive_query_mutable_context);
        recursive_cte_table_node->setTemporaryTableName(union_node_typed.getCTEName());

        recursive_cte_table.emplace(std::move(temporary_table_holder), std::move(temporary_table_storage), std::move(temporary_table_columns));
    }

    size_t queries_nodes_size = queries_nodes.size();
    for (size_t i = recursive_cte_table.has_value(); i < queries_nodes_size; ++i)
    {
        auto & query_node = queries_nodes[i];

        IdentifierResolveScope subquery_scope(query_node, &scope /*parent_scope*/);

        if (recursive_cte_table_node)
            subquery_scope.expression_argument_name_to_node[union_node_typed.getCTEName()] = recursive_cte_table_node;

        auto query_node_type = query_node->getNodeType();

        if (query_node_type == QueryTreeNodeType::QUERY)
        {
            resolveQuery(query_node, subquery_scope);
        }
        else if (query_node_type == QueryTreeNodeType::UNION)
        {
            resolveUnion(query_node, subquery_scope);
        }
        else
        {
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "UNION unsupported node {}. In scope {}",
                query_node->formatASTForErrorMessage(),
                scope.scope_node->formatASTForErrorMessage());
        }
    }

    if (recursive_cte_table && isStorageUsedInTree(recursive_cte_table->storage, union_node.get()))
    {
        if (union_node_typed.getUnionMode() != SelectUnionMode::UNION_ALL)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Recursive CTE subquery {} with {} union mode is unsupported, only UNION ALL union mode is supported",
            union_node_typed.formatASTForErrorMessage(),
            toString(union_node_typed.getUnionMode()));

        union_node_typed.setRecursiveCTETable(std::move(*recursive_cte_table));
    }

    if (union_node_typed.isCTE())
        ctes_in_resolve_process.erase(union_node_typed.getCTEName());
}

}
