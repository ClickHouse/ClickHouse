#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/formatAST.h>
#include <Interpreters/misc.h>
#include <Common/typeid_cast.h>
#include <DataTypes/NestedUtils.h>
#include <Interpreters/ActionsDAG.h>
#include <base/map.h>

namespace DB
{

/// Conditions like "x = N" are considered good if abs(N) > threshold.
/// This is used to assume that condition is likely to have good selectivity.
static constexpr auto threshold = 2;

static NameToIndexMap fillNamesPositions(const Names & names)
{
    NameToIndexMap names_positions;

    for (size_t position = 0; position < names.size(); ++position)
    {
        const auto & name = names[position];
        names_positions[name] = position;
    }

    return names_positions;
}

/// Find minimal position of any of the column in primary key.
static Int64 findMinPosition(const NameSet & condition_table_columns, const NameToIndexMap & primary_key_positions)
{
    Int64 min_position = std::numeric_limits<Int64>::max() - 1;

    for (const auto & column : condition_table_columns)
    {
        auto it = primary_key_positions.find(column);
        if (it != primary_key_positions.end())
            min_position = std::min(min_position, static_cast<Int64>(it->second));
    }

    return min_position;
}

MergeTreeWhereOptimizer::MergeTreeWhereOptimizer(
    std::unordered_map<std::string, UInt64> column_sizes_,
    const StorageMetadataPtr & metadata_snapshot,
    const Names & queried_columns_,
    const std::optional<NameSet> & supported_columns_,
    Poco::Logger * log_)
    : table_columns{collections::map<std::unordered_set>(
        metadata_snapshot->getColumns().getAllPhysical(), [](const NameAndTypePair & col) { return col.name; })}
    , queried_columns{queried_columns_}
    , supported_columns{supported_columns_}
    , sorting_key_names{NameSet(
          metadata_snapshot->getSortingKey().column_names.begin(), metadata_snapshot->getSortingKey().column_names.end())}
    , primary_key_names_positions(fillNamesPositions(metadata_snapshot->getPrimaryKey().column_names))
    , log{log_}
    , column_sizes{std::move(column_sizes_)}
{
    for (const auto & name : queried_columns)
    {
        auto it = column_sizes.find(name);
        if (it != column_sizes.end())
            total_size_of_queried_columns += it->second;
    }
}

void MergeTreeWhereOptimizer::optimize(
    SelectQueryInfo & select_query_info,
    const ContextPtr & context,
    const ProjectionsDescription & projections,
    const String & primary_key,
    bool & proj_optimized) const
{
    auto & select = select_query_info.query->as<ASTSelectQuery &>();
    if (!select.where() || select.prewhere())
        return;

    auto block_with_constants = KeyCondition::getBlockWithConstants(select_query_info.query->clone(),
        select_query_info.syntax_analyzer_result,
        context);

    WhereOptimizerContext where_optimizer_context;
    where_optimizer_context.context = context;
    where_optimizer_context.array_joined_names = determineArrayJoinedNames(select);
    where_optimizer_context.move_all_conditions_to_prewhere = context->getSettingsRef().move_all_conditions_to_prewhere;
    where_optimizer_context.move_primary_key_columns_to_end_of_prewhere = context->getSettingsRef().move_primary_key_columns_to_end_of_prewhere;
    where_optimizer_context.is_final = select.final();

    Settings query_settings = context->getSettings();

    //default to false, need to set to true to use this optimization
    if (query_settings.optimize_project_query && !projections.empty() && !proj_optimized)
    {
        const auto main_table_name = getTableName(select.tables());
        auto pkoptimized_where_ast = pkOptimization(projections, select.where(), main_table_name, primary_key);
        select.setExpression(ASTSelectQuery::Expression::WHERE, std::move(pkoptimized_where_ast));
    }

    RPNBuilderTreeContext tree_context(context, std::move(block_with_constants), {} /*prepared_sets*/);
    RPNBuilderTreeNode node(select.where().get(), tree_context);
    auto optimize_result = optimizeImpl(node, where_optimizer_context);
    if (!optimize_result)
        return;

    /// Rewrite the SELECT query.

    auto where_filter_ast = reconstructAST(optimize_result->where_conditions);
    auto prewhere_filter_ast = reconstructAST(optimize_result->prewhere_conditions);

    select.setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_filter_ast));
    select.setExpression(ASTSelectQuery::Expression::PREWHERE, std::move(prewhere_filter_ast));

    UInt64 log_queries_cut_to_length = context->getSettingsRef().log_queries_cut_to_length;
    LOG_DEBUG(log, "MergeTreeWhereOptimizer: condition \"{}\" moved to PREWHERE", select.prewhere()->formatForLogging(log_queries_cut_to_length));
}

std::optional<MergeTreeWhereOptimizer::FilterActionsOptimizeResult> MergeTreeWhereOptimizer::optimize(const ActionsDAGPtr & filter_dag,
    const std::string & filter_column_name,
    const ContextPtr & context,
    bool is_final)
{
    WhereOptimizerContext where_optimizer_context;
    where_optimizer_context.context = context;
    where_optimizer_context.array_joined_names = {};
    where_optimizer_context.move_all_conditions_to_prewhere = context->getSettingsRef().move_all_conditions_to_prewhere;
    where_optimizer_context.move_primary_key_columns_to_end_of_prewhere = context->getSettingsRef().move_primary_key_columns_to_end_of_prewhere;
    where_optimizer_context.is_final = is_final;

    RPNBuilderTreeContext tree_context(context);
    RPNBuilderTreeNode node(&filter_dag->findInOutputs(filter_column_name), tree_context);

    auto optimize_result = optimizeImpl(node, where_optimizer_context);
    if (!optimize_result)
        return {};

    auto filter_actions = reconstructDAG(optimize_result->where_conditions, context);
    auto prewhere_filter_actions = reconstructDAG(optimize_result->prewhere_conditions, context);

    FilterActionsOptimizeResult result = { std::move(filter_actions), std::move(prewhere_filter_actions) };
    return result;
}

static void collectColumns(const RPNBuilderTreeNode & node, const NameSet & columns_names, NameSet & result_set, bool & has_invalid_column)
{
    if (node.isConstant())
        return;

    if (node.isSubqueryOrSet())
        return;

    if (!node.isFunction())
    {
        auto column_name = node.getColumnName();
        if (!columns_names.contains(column_name))
        {
            has_invalid_column = true;
            return;
        }

        result_set.insert(column_name);
        return;
    }

    auto function_node = node.toFunctionNode();
    size_t arguments_size = function_node.getArgumentsSize();
    for (size_t i = 0; i < arguments_size; ++i)
    {
        auto function_argument = function_node.getArgumentAt(i);
        collectColumns(function_argument, columns_names, result_set, has_invalid_column);
    }
}

static bool isConditionGood(const RPNBuilderTreeNode & condition, const NameSet & columns_names)
{
    if (!condition.isFunction())
        return false;

    auto function_node = condition.toFunctionNode();

    /** We are only considering conditions of form `equals(one, another)` or `one = another`,
      * especially if either `one` or `another` is ASTIdentifier
      */
    if (function_node.getFunctionName() != "equals" || function_node.getArgumentsSize() != 2)
        return false;

    auto lhs_argument = function_node.getArgumentAt(0);
    auto rhs_argument = function_node.getArgumentAt(1);

    auto lhs_argument_column_name = lhs_argument.getColumnName();
    auto rhs_argument_column_name = rhs_argument.getColumnName();

    bool lhs_argument_is_column = columns_names.contains(lhs_argument_column_name);
    bool rhs_argument_is_column = columns_names.contains(rhs_argument_column_name);

    bool lhs_argument_is_constant = lhs_argument.isConstant();
    bool rhs_argument_is_constant = rhs_argument.isConstant();

    RPNBuilderTreeNode * constant_node = nullptr;

    if (lhs_argument_is_column && rhs_argument_is_constant)
        constant_node = &rhs_argument;
    else if (lhs_argument_is_constant && rhs_argument_is_column)
        constant_node = &lhs_argument;
    else
        return false;

    Field output_value;
    DataTypePtr output_type;
    if (!constant_node->tryGetConstant(output_value, output_type))
        return false;

    const auto type = output_value.getType();

    /// check the value with respect to threshold
    if (type == Field::Types::UInt64)
    {
        const auto value = output_value.get<UInt64>();
        return value > threshold;
    }
    else if (type == Field::Types::Int64)
    {
        const auto value = output_value.get<Int64>();
        return value < -threshold || threshold < value;
    }
    else if (type == Field::Types::Float64)
    {
        const auto value = output_value.get<Float64>();
        return value < threshold || threshold < value;
    }

    return false;
}

void MergeTreeWhereOptimizer::analyzeImpl(Conditions & res, const RPNBuilderTreeNode & node, const WhereOptimizerContext & where_optimizer_context, std::set<Int64> & pk_positions) const
{
    auto function_node_optional = node.toFunctionNodeOrNull();

    if (function_node_optional.has_value() && function_node_optional->getFunctionName() == "and")
    {
        size_t arguments_size = function_node_optional->getArgumentsSize();

        for (size_t i = 0; i < arguments_size; ++i)
        {
            auto argument = function_node_optional->getArgumentAt(i);
            analyzeImpl(res, argument, where_optimizer_context, pk_positions);
        }
    }
    else
    {
        Condition cond(node);
        bool has_invalid_column = false;
        collectColumns(node, table_columns, cond.table_columns, has_invalid_column);

        cond.columns_size = getColumnsSize(cond.table_columns);

        cond.viable =
            !has_invalid_column &&
            /// Condition depend on some column. Constant expressions are not moved.
            !cond.table_columns.empty()
            && !cannotBeMoved(node, where_optimizer_context)
            /// When use final, do not take into consideration the conditions with non-sorting keys. Because final select
            /// need to use all sorting keys, it will cause correctness issues if we filter other columns before final merge.
            && (!where_optimizer_context.is_final || isExpressionOverSortingKey(node))
            /// Some identifiers can unable to support PREWHERE (usually because of different types in Merge engine)
            && columnsSupportPrewhere(cond.table_columns)
            /// Do not move conditions involving all queried columns.
            && cond.table_columns.size() < queried_columns.size();

        if (cond.viable)
            cond.good = isConditionGood(node, table_columns);

        if (where_optimizer_context.move_primary_key_columns_to_end_of_prewhere)
        {
            /// Consider all conditions good with this setting enabled.
            cond.good = cond.viable;
            /// Find min position in PK of any column that is used in this condition.
            cond.min_position_in_primary_key = findMinPosition(cond.table_columns, primary_key_names_positions);
            pk_positions.emplace(cond.min_position_in_primary_key);
        }

        res.emplace_back(std::move(cond));
    }
}

ASTPtr MergeTreeWhereOptimizer::pkOptimization(
    const ProjectionsDescription & projections,
    const ASTPtr & where_ast,
    const String & main_table,
    const String & main_primary_key) const
{
    auto where_column_name = where_ast->getColumnName();
    NameSet proj_pks = {};
    for (auto & projection: projections)
    {
        if (projection.type == ProjectionDescription::Type::Normal){
            //sorting key of projection
            const auto & projection_primary_key = projection.metadata->getSortingKey().column_names.at(0);
            proj_pks.insert(projection_primary_key);
            auto projection_columns = projection.getRequiredColumns();

            // projection columns needs to include projection primary key and main table primary key
            // in order to use this optimization
            bool proj_col_include_ppk = std::find(projection_columns.begin(), projection_columns.end(), projection_primary_key) != projection_columns.end();
            bool proj_col_include_mpk = std::find(projection_columns.begin(), projection_columns.end(), main_primary_key) != projection_columns.end();

            if(!proj_col_include_ppk || !proj_col_include_mpk)
            {
                return where_ast;
            }
        }

    }

    const auto and_function =  makeASTFunction("and");
    //for keys in where_ast
    bool contains_primay_key = false;
    analyze_where_ast(where_ast, and_function, proj_pks, main_table, main_primary_key, contains_primay_key);
    and_function->arguments->children.push_back(where_ast->clone());

    return and_function;
}

void MergeTreeWhereOptimizer::analyze_where_ast(const ASTPtr & ast, const ASTPtr & func, NameSet & proj_pks, const String & main_table, const String & main_primary_key, bool & contains_pk) const
{
    if (contains_pk)
        return;
    const auto * ast_function_node = ast->as<ASTFunction>();
    if(ast_function_node->name == "equals" && ast_function_node->arguments->children.size() == 2)
    {
        auto lhs = ast_function_node->arguments->children.at(0)->as<ASTIdentifier>()->name();
        if (lhs == main_primary_key)
        {
            contains_pk = true;
            return;
        }
        if(proj_pks.contains(lhs))
        {
            ASTPtr new_ast = create_proj_optimized_ast(ast, main_table, main_primary_key);
            auto * function_node = func->as<ASTFunction>();
            function_node->arguments->children.push_back(new_ast);
        }
    }
    else
    {
        auto arg_size = ast_function_node->arguments ? ast_function_node->arguments->children.size() : 0;
        for (size_t i = 0; i < arg_size; i++)
        {
            auto argument = ast_function_node->arguments->children[i];
            analyze_where_ast(argument, func, proj_pks, main_table, main_primary_key, contains_pk);
        }
    }
}

/**
 * @brief Manually rewrite the WHERE query, Insert a new where condition in order to
 * leverage projection features
 *
 * For example, a qualified table with projection
 * CREATE TABLE test_a(`src` String,`dst` String, `other_cols` String,
 * PROJECTION p1(SELECT src, dst ORDER BY dst)) ENGINE = MergeTree ORDER BY src;
 *
 * A qualified SELECT query would looks like this
 * select * from test_a where dst='-42';
 * The where key is the projection table primary key.
 * The following code will convert this select query to the following
 * select * from test_a where src in (select src from test_a where dst='-42') and dst='-42';
 */
ASTPtr MergeTreeWhereOptimizer::create_proj_optimized_ast(const ASTPtr & ast, const String & main_table, const String & main_primary_key) const
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    const auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->list_of_selects = std::make_shared<ASTExpressionList>();

    auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
    auto table_expr = std::make_shared<ASTTableExpression>();

    table_expr->database_and_table_name = std::make_shared<ASTTableIdentifier>(main_table);
    table_expr->children.push_back(table_expr->database_and_table_name);

    tables_elem->table_expression = std::move(table_expr);
    tables_elem->children.push_back(tables_elem->table_expression);

    auto tables_in_select = std::make_shared<ASTTablesInSelectQuery>();
    tables_in_select->children.push_back(std::move(tables_elem));


    select_query->select()->children.push_back(std::make_shared<ASTIdentifier>(main_primary_key));

    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables_in_select);
    select_query->setExpression(ASTSelectQuery::Expression::WHERE, ast->clone());

    select_with_union_query->list_of_selects->children.push_back(select_query);

    auto subquery = std::make_shared<ASTSubquery>();
    subquery->children.push_back(select_with_union_query);

    const auto in_function = makeASTFunction("in", std::make_shared<ASTIdentifier>(main_primary_key), subquery);

    return in_function;
}
/// Transform conjunctions chain in WHERE expression to Conditions list.
MergeTreeWhereOptimizer::Conditions MergeTreeWhereOptimizer::analyze(const RPNBuilderTreeNode & node,
    const WhereOptimizerContext & where_optimizer_context) const
{
    Conditions res;
    std::set<Int64> pk_positions;
    analyzeImpl(res, node, where_optimizer_context, pk_positions);

    /// E.g., if the primary key is (a, b, c) but the condition is a = 1 and c = 1,
    /// we should only put (a = 1) to the tail of PREWHERE,
    /// and treat (c = 1) as a normal column.
    if (where_optimizer_context.move_primary_key_columns_to_end_of_prewhere)
    {
        Int64 min_valid_pk_pos = -1;
        for (auto pk_pos : pk_positions)
        {
            if (pk_pos != min_valid_pk_pos + 1)
                break;
            min_valid_pk_pos = pk_pos;
        }
        for (auto & cond : res)
        {
            if (cond.min_position_in_primary_key > min_valid_pk_pos)
                cond.min_position_in_primary_key = std::numeric_limits<Int64>::max() - 1;
        }
        LOG_TRACE(log, "The min valid primary key position for moving to the tail of PREWHERE is {}", min_valid_pk_pos);
    }

    return res;
}

/// Transform Conditions list to WHERE or PREWHERE expression.
ASTPtr MergeTreeWhereOptimizer::reconstructAST(const Conditions & conditions)
{
    if (conditions.empty())
        return {};

    if (conditions.size() == 1)
        return conditions.front().node.getASTNode()->clone();

    const auto function = std::make_shared<ASTFunction>();

    function->name = "and";
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);

    for (const auto & elem : conditions)
        function->arguments->children.push_back(elem.node.getASTNode()->clone());

    return function;
}

ActionsDAGPtr MergeTreeWhereOptimizer::reconstructDAG(const Conditions & conditions, const ContextPtr & context)
{
    if (conditions.empty())
        return {};

    ActionsDAG::NodeRawConstPtrs filter_nodes;
    filter_nodes.reserve(conditions.size());

    for (const auto & condition : conditions)
        filter_nodes.push_back(condition.node.getDAGNode());

    return ActionsDAG::buildFilterActionsDAG(filter_nodes, {} /*node_name_to_input_node_column*/, context);
}

std::optional<MergeTreeWhereOptimizer::OptimizeResult> MergeTreeWhereOptimizer::optimizeImpl(const RPNBuilderTreeNode & node,
    const WhereOptimizerContext & where_optimizer_context) const
{
    Conditions where_conditions = analyze(node, where_optimizer_context);
    Conditions prewhere_conditions;

    UInt64 total_size_of_moved_conditions = 0;
    UInt64 total_number_of_moved_columns = 0;

    /// Move condition and all other conditions depend on the same set of columns.
    auto move_condition = [&](Conditions::iterator cond_it)
    {
        prewhere_conditions.splice(prewhere_conditions.end(), where_conditions, cond_it);
        total_size_of_moved_conditions += cond_it->columns_size;
        total_number_of_moved_columns += cond_it->table_columns.size();

        /// Move all other viable conditions that depend on the same set of columns.
        for (auto jt = where_conditions.begin(); jt != where_conditions.end();)
        {
            if (jt->viable && jt->columns_size == cond_it->columns_size && jt->table_columns == cond_it->table_columns)
                prewhere_conditions.splice(prewhere_conditions.end(), where_conditions, jt++);
            else
                ++jt;
        }
    };

    /// Move conditions unless the ratio of total_size_of_moved_conditions to the total_size_of_queried_columns is less than some threshold.
    while (!where_conditions.empty())
    {
        /// Move the best condition to PREWHERE if it is viable.

        auto it = std::min_element(where_conditions.begin(), where_conditions.end());

        if (!it->viable)
            break;

        if (!where_optimizer_context.move_all_conditions_to_prewhere)
        {
            bool moved_enough = false;
            if (total_size_of_queried_columns > 0)
            {
                /// If we know size of queried columns use it as threshold. 10% ratio is just a guess.
                moved_enough = total_size_of_moved_conditions > 0
                    && (total_size_of_moved_conditions + it->columns_size) * 10 > total_size_of_queried_columns;
            }
            else
            {
                /// Otherwise, use number of moved columns as a fallback.
                /// It can happen, if table has only compact parts. 25% ratio is just a guess.
                moved_enough = total_number_of_moved_columns > 0
                    && (total_number_of_moved_columns + it->table_columns.size()) * 4 > queried_columns.size();
            }

            if (moved_enough)
                break;
        }

        move_condition(it);
    }

    /// Nothing was moved.
    if (prewhere_conditions.empty())
        return {};

    OptimizeResult result = {std::move(where_conditions), std::move(prewhere_conditions)};
    return result;
}


UInt64 MergeTreeWhereOptimizer::getColumnsSize(const NameSet & columns) const
{
    UInt64 size = 0;

    for (const auto & column : columns)
        if (column_sizes.contains(column))
            size += column_sizes.at(column);

    return size;
}

bool MergeTreeWhereOptimizer::columnsSupportPrewhere(const NameSet & columns) const
{
    if (!supported_columns.has_value())
        return true;

    for (const auto & column : columns)
        if (!supported_columns->contains(column))
            return false;

    return true;
}

bool MergeTreeWhereOptimizer::isExpressionOverSortingKey(const RPNBuilderTreeNode & node) const
{
    if (node.isFunction())
    {
        auto function_node = node.toFunctionNode();
        size_t arguments_size = function_node.getArgumentsSize();

        for (size_t i = 0; i < arguments_size; ++i)
        {
            auto argument = function_node.getArgumentAt(i);
            auto argument_column_name = argument.getColumnName();

            if (argument.isConstant() || sorting_key_names.contains(argument_column_name))
                continue;

            if (!isExpressionOverSortingKey(argument))
                return false;
        }

        return true;
    }

    return node.isConstant() || sorting_key_names.contains(node.getColumnName());
}

bool MergeTreeWhereOptimizer::isSortingKey(const String & column_name) const
{
    return sorting_key_names.contains(column_name);
}

bool MergeTreeWhereOptimizer::isSubsetOfTableColumns(const NameSet & columns) const
{
    for (const auto & column : columns)
        if (!table_columns.contains(column))
            return false;

    return true;
}

bool MergeTreeWhereOptimizer::cannotBeMoved(const RPNBuilderTreeNode & node, const WhereOptimizerContext & where_optimizer_context) const
{
    if (node.isFunction())
    {
        auto function_node = node.toFunctionNode();
        auto function_name = function_node.getFunctionName();

        /// disallow arrayJoin expressions to be moved to PREWHERE for now
        if (function_name == "arrayJoin")
            return true;

        /// disallow GLOBAL IN, GLOBAL NOT IN
        /// TODO why?
        if (function_name == "globalIn" || function_name == "globalNotIn")
            return true;

        /// indexHint is a special function that it does not make sense to transfer to PREWHERE
        if (function_name == "indexHint")
            return true;

        size_t arguments_size = function_node.getArgumentsSize();
        for (size_t i = 0; i < arguments_size; ++i)
        {
            auto argument = function_node.getArgumentAt(i);
            if (cannotBeMoved(argument, where_optimizer_context))
                return true;
        }
    }
    else
    {
        auto column_name = node.getColumnName();

        /// disallow moving result of ARRAY JOIN to PREWHERE
        if (where_optimizer_context.array_joined_names.contains(column_name) ||
            where_optimizer_context.array_joined_names.contains(Nested::extractTableName(column_name)) ||
            (table_columns.contains(column_name) && where_optimizer_context.is_final && !isSortingKey(column_name)))
            return true;
    }

    return false;
}

String MergeTreeWhereOptimizer::getTableName(const ASTPtr & tables_in_select_query_ast) const
{
    String table_id;
    const auto & tables_in_select_query = tables_in_select_query_ast->as<ASTTablesInSelectQuery &>();
    const auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();
    const auto & table_expression = tables_element.table_expression->as<ASTTableExpression &>();
    table_id = table_expression.database_and_table_name->as<ASTTableIdentifier>()->getTableId().table_name;

    return table_id;
}

NameSet MergeTreeWhereOptimizer::determineArrayJoinedNames(const ASTSelectQuery & select)
{
    auto [array_join_expression_list, _] = select.arrayJoinExpressionList();

    /// much simplified code from ExpressionAnalyzer::getArrayJoinedColumns()
    if (!array_join_expression_list)
        return {};

    NameSet array_joined_names;
    for (const auto & ast : array_join_expression_list->children)
        array_joined_names.emplace(ast->getAliasOrColumnName());

    return array_joined_names;
}

}
