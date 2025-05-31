#include <memory>
#include <Analyzer/QueryNode.h>

#include <fmt/core.h>

#include <Common/assert_cast.h>
#include <Common/SipHash.h>
#include <Common/FieldVisitorToString.h>

#include <Core/NamesAndTypes.h>

#include <DataTypes/IDataType.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/InterpolateNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
}

QueryNode::QueryNode(ContextMutablePtr context_, SettingsChanges settings_changes_)
    : IQueryTreeNode(children_size)
    , context(std::move(context_))
    , settings_changes(std::move(settings_changes_))
{
    children[with_child_index] = std::make_shared<ListNode>();
    children[projection_child_index] = std::make_shared<ListNode>();
    children[group_by_child_index] = std::make_shared<ListNode>();
    children[window_child_index] = std::make_shared<ListNode>();
    children[order_by_child_index] = std::make_shared<ListNode>();
    children[limit_by_child_index] = std::make_shared<ListNode>();
    children[correlated_columns_list_index] = std::make_shared<ListNode>();
}

QueryNode::QueryNode(ContextMutablePtr context_)
    : QueryNode(std::move(context_), {} /*settings_changes*/)
{}

void QueryNode::resolveProjectionColumns(NamesAndTypes projection_columns_value)
{

    // Ensure the number of aliases matches the number of projection columns
    if (!this->projection_aliases_to_override.empty())
    {
        if (this->projection_aliases_to_override.size() != projection_columns_value.size())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Number of aliases does not match number of projection columns. "
                "Expected {}, got {}",
                projection_columns_value.size(),
                this->projection_aliases_to_override.size());

        for (size_t i = 0; i < projection_columns_value.size(); ++i)
            projection_columns_value[i].name = this->projection_aliases_to_override[i];
    }
    projection_columns = std::move(projection_columns_value);
}

void QueryNode::removeUnusedProjectionColumns(const std::unordered_set<size_t> & used_projection_columns_indexes)
{
    auto & projection_nodes = getProjection().getNodes();
    size_t projection_columns_size = projection_columns.size();
    size_t write_index = 0;

    for (size_t i = 0; i < projection_columns_size; ++i)
    {
        if (!used_projection_columns_indexes.contains(i))
            continue;

        projection_nodes[write_index] = projection_nodes[i];
        projection_columns[write_index] = projection_columns[i];
        ++write_index;
    }

    projection_nodes.erase(projection_nodes.begin() + write_index, projection_nodes.end());
    projection_columns.erase(projection_columns.begin() + write_index, projection_columns.end());

    if (hasInterpolate())
    {
        std::unordered_set<String> used_projection_columns;
        for (const auto & projection : projection_columns)
            used_projection_columns.insert(projection.name);

        auto & interpolate_node = getInterpolate();
        auto & interpolate_list_nodes = interpolate_node->as<ListNode &>().getNodes();
        std::erase_if(
            interpolate_list_nodes,
            [&used_projection_columns](const QueryTreeNodePtr & interpolate)
            { return !used_projection_columns.contains(interpolate->as<InterpolateNode &>().getExpressionName()); });

        if (interpolate_list_nodes.empty())
            interpolate_node = nullptr;
    }
}

ColumnNodePtrWithHashSet QueryNode::getCorrelatedColumnsSet() const
{
    ColumnNodePtrWithHashSet result;

    const auto & correlated_columns = getCorrelatedColumns().getNodes();
    result.reserve(correlated_columns.size());

    for (const auto & column : correlated_columns)
    {
        result.insert(std::static_pointer_cast<ColumnNode>(column));
    }
    return result;
}

void QueryNode::addCorrelatedColumn(const QueryTreeNodePtr & correlated_column)
{
    auto & correlated_columns = getCorrelatedColumns().getNodes();
    for (const auto & column : correlated_columns)
    {
        if (column->isEqual(*correlated_column))
            return;
    }
    correlated_columns.push_back(correlated_column);
}

DataTypePtr QueryNode::getResultType() const
{
    if (isCorrelated())
    {
        if (projection_columns.size() == 1)
        {
            return projection_columns[0].type;
        }
        else
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Method getResultType is supported only for correlated query node with 1 column, but got {}",
                projection_columns.size());
    }
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Method getResultType is supported only for correlated query node");
}

void QueryNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "QUERY id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    if (is_subquery)
        buffer << ", is_subquery: " << is_subquery;

    if (is_cte)
        buffer << ", is_cte: " << is_cte;

    if (is_recursive_with)
        buffer << ", is_recursive_with: " << is_recursive_with;

    if (is_distinct)
        buffer << ", is_distinct: " << is_distinct;

    if (is_limit_with_ties)
        buffer << ", is_limit_with_ties: " << is_limit_with_ties;

    if (is_group_by_with_totals)
        buffer << ", is_group_by_with_totals: " << is_group_by_with_totals;

    if (is_group_by_all)
        buffer << ", is_group_by_all: " << is_group_by_all;

    if (is_order_by_all)
        buffer << ", is_order_by_all: " << is_order_by_all;

    std::string group_by_type;
    if (is_group_by_with_rollup)
        group_by_type = "rollup";
    else if (is_group_by_with_cube)
        group_by_type = "cube";
    else if (is_group_by_with_grouping_sets)
        group_by_type = "grouping_sets";

    if (!group_by_type.empty())
        buffer << ", group_by_type: " << group_by_type;

    if (!cte_name.empty())
        buffer << ", cte_name: " << cte_name;

    if (isCorrelated())
    {
        buffer << ", is_correlated: 1\n" << std::string(indent + 2, ' ') << "CORRELATED COLUMNS\n";
        getCorrelatedColumns().dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasWith())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "WITH\n";
        getWith().dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (!projection_columns.empty())
    {
        buffer << '\n';
        buffer << std::string(indent + 2, ' ') << "PROJECTION COLUMNS\n";

        size_t projection_columns_size = projection_columns.size();
        for (size_t i = 0; i < projection_columns_size; ++i)
        {
            const auto & projection_column = projection_columns[i];
            buffer << std::string(indent + 4, ' ') << projection_column.name << " " << projection_column.type->getName();
            if (i + 1 != projection_columns_size)
                buffer << '\n';
        }
    }

    buffer << '\n';
    buffer << std::string(indent + 2, ' ') << "PROJECTION\n";
    getProjection().dumpTreeImpl(buffer, format_state, indent + 4);

    if (getJoinTree())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "JOIN TREE\n";
        getJoinTree()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (getPrewhere())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "PREWHERE\n";
        getPrewhere()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (getWhere())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "WHERE\n";
        getWhere()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (!is_group_by_all && hasGroupBy())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "GROUP BY\n";
        getGroupBy().dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasHaving())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "HAVING\n";
        getHaving()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasWindow())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "WINDOW\n";
        getWindow().dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasQualify())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "QUALIFY\n";
        getQualify()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasOrderBy())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "ORDER BY\n";
        getOrderBy().dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasInterpolate())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "INTERPOLATE\n";
        getInterpolate()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasLimitByLimit())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "LIMIT BY LIMIT\n";
        getLimitByLimit()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasLimitByOffset())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "LIMIT BY OFFSET\n";
        getLimitByOffset()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasLimitBy())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "LIMIT BY\n";
        getLimitBy().dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasLimit())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "LIMIT\n";
        getLimit()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasOffset())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "OFFSET\n";
        getOffset()->dumpTreeImpl(buffer, format_state, indent + 4);
    }

    if (hasSettingsChanges())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "SETTINGS";
        for (const auto & change : settings_changes)
            buffer << fmt::format(" {}={}", change.name, toString(change.value));
    }
}

bool QueryNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions options) const
{
    const auto & rhs_typed = assert_cast<const QueryNode &>(rhs);

    return is_subquery == rhs_typed.is_subquery &&
        (options.ignore_cte || (is_cte == rhs_typed.is_cte && cte_name == rhs_typed.cte_name)) &&
        is_recursive_with == rhs_typed.is_recursive_with &&
        is_distinct == rhs_typed.is_distinct &&
        is_limit_with_ties == rhs_typed.is_limit_with_ties &&
        is_group_by_with_totals == rhs_typed.is_group_by_with_totals &&
        is_group_by_with_rollup == rhs_typed.is_group_by_with_rollup &&
        is_group_by_with_cube == rhs_typed.is_group_by_with_cube &&
        is_group_by_with_grouping_sets == rhs_typed.is_group_by_with_grouping_sets &&
        is_group_by_all == rhs_typed.is_group_by_all &&
        is_order_by_all == rhs_typed.is_order_by_all &&
        projection_columns == rhs_typed.projection_columns &&
        settings_changes == rhs_typed.settings_changes;
}

void QueryNode::updateTreeHashImpl(HashState & state, CompareOptions options) const
{
    state.update(is_subquery);

    if (options.ignore_cte)
    {
        state.update(false);
        state.update(size_t(0));
        state.update(std::string());
    }
    else
    {
        state.update(is_cte);
        state.update(cte_name.size());
        state.update(cte_name);
    }

    state.update(projection_columns.size());
    for (const auto & projection_column : projection_columns)
    {
        state.update(projection_column.name.size());
        state.update(projection_column.name);

        auto projection_column_type_name = projection_column.type->getName();
        state.update(projection_column_type_name.size());
        state.update(projection_column_type_name);
    }

    for (const auto & projection_alias : projection_aliases_to_override)
    {
        state.update(projection_alias.size());
        state.update(projection_alias);
    }

    state.update(is_recursive_with);
    state.update(is_distinct);
    state.update(is_limit_with_ties);
    state.update(is_group_by_with_totals);
    state.update(is_group_by_with_rollup);
    state.update(is_group_by_with_cube);
    state.update(is_group_by_with_grouping_sets);
    state.update(is_group_by_all);
    state.update(is_order_by_all);

    state.update(settings_changes.size());

    for (const auto & setting_change : settings_changes)
    {
        state.update(setting_change.name.size());
        state.update(setting_change.name);

        auto setting_change_value_dump = setting_change.value.dump();
        state.update(setting_change_value_dump.size());
        state.update(setting_change_value_dump);
    }
}

QueryTreeNodePtr QueryNode::cloneImpl() const
{
    auto result_query_node = std::make_shared<QueryNode>(context);

    result_query_node->is_subquery = is_subquery;
    result_query_node->is_cte = is_cte;
    result_query_node->is_recursive_with = is_recursive_with;
    result_query_node->is_distinct = is_distinct;
    result_query_node->is_limit_with_ties = is_limit_with_ties;
    result_query_node->is_group_by_with_totals = is_group_by_with_totals;
    result_query_node->is_group_by_with_rollup = is_group_by_with_rollup;
    result_query_node->is_group_by_with_cube = is_group_by_with_cube;
    result_query_node->is_group_by_with_grouping_sets = is_group_by_with_grouping_sets;
    result_query_node->is_group_by_all = is_group_by_all;
    result_query_node->is_order_by_all = is_order_by_all;
    result_query_node->cte_name = cte_name;
    result_query_node->projection_columns = projection_columns;
    result_query_node->settings_changes = settings_changes;
    result_query_node->projection_aliases_to_override = projection_aliases_to_override;

    return result_query_node;
}

ASTPtr QueryNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    select_query->recursive_with = is_recursive_with;
    select_query->distinct = is_distinct;
    select_query->limit_with_ties = is_limit_with_ties;
    select_query->group_by_with_totals = is_group_by_with_totals;
    select_query->group_by_with_rollup = is_group_by_with_rollup;
    select_query->group_by_with_cube = is_group_by_with_cube;
    select_query->group_by_with_grouping_sets = is_group_by_with_grouping_sets;
    select_query->group_by_all = is_group_by_all;
    select_query->order_by_all = is_order_by_all;

    if (hasWith())
    {
        const auto & with = getWith();
        auto expression_list_ast = std::make_shared<ASTExpressionList>();
        expression_list_ast->children.reserve(with.getNodes().size());

        for (const auto & with_node : with)
        {
            auto with_node_ast = with_node->toAST(options);
            expression_list_ast->children.push_back(with_node_ast);

            const auto * with_query_node = with_node->as<QueryNode>();
            const auto * with_union_node = with_node->as<UnionNode>();
            if (!with_query_node && !with_union_node)
                continue;

            bool is_with_node_cte = with_query_node ? with_query_node->isCTE() : with_union_node->isCTE();
            if (!is_with_node_cte)
                continue;

            const auto & with_node_cte_name = with_query_node ? with_query_node->cte_name : with_union_node->getCTEName();

            auto * with_node_ast_subquery = with_node_ast->as<ASTSubquery>();
            if (with_node_ast_subquery)
                with_node_ast_subquery->cte_name = "";

            auto with_element_ast = std::make_shared<ASTWithElement>();
            with_element_ast->name = with_node_cte_name;
            with_element_ast->subquery = std::move(with_node_ast);
            with_element_ast->children.push_back(with_element_ast->subquery);

            expression_list_ast->children.back() = std::move(with_element_ast);
        }

        select_query->setExpression(ASTSelectQuery::Expression::WITH, std::move(expression_list_ast));
    }

    auto projection_ast = getProjection().toAST(options);
    auto & projection_expression_list_ast = projection_ast->as<ASTExpressionList &>();
    size_t projection_expression_list_ast_children_size = projection_expression_list_ast.children.size();
    if (projection_expression_list_ast_children_size != getProjection().getNodes().size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query node invalid projection conversion to AST");

    if (!projection_columns.empty())
    {
        for (size_t i = 0; i < projection_expression_list_ast_children_size; ++i)
        {
            auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(projection_expression_list_ast.children[i].get());

            if (ast_with_alias)
                ast_with_alias->setAlias(projection_columns[i].name);
        }
    }

    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(projection_ast));

    ASTPtr tables_in_select_query_ast = std::make_shared<ASTTablesInSelectQuery>();
    addTableExpressionOrJoinIntoTablesInSelectQuery(tables_in_select_query_ast, getJoinTree(), options);
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select_query_ast));

    if (getPrewhere())
        select_query->setExpression(ASTSelectQuery::Expression::PREWHERE, getPrewhere()->toAST(options));

    if (getWhere())
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, getWhere()->toAST(options));

    if (!is_group_by_all && hasGroupBy())
        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, getGroupBy().toAST(options));

    if (hasHaving())
        select_query->setExpression(ASTSelectQuery::Expression::HAVING, getHaving()->toAST(options));

    if (hasWindow())
        select_query->setExpression(ASTSelectQuery::Expression::WINDOW, getWindow().toAST(options));

    if (hasQualify())
        select_query->setExpression(ASTSelectQuery::Expression::QUALIFY, getQualify()->toAST(options));

    if (hasOrderBy())
        select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, getOrderBy().toAST(options));

    if (hasInterpolate())
        select_query->setExpression(ASTSelectQuery::Expression::INTERPOLATE, getInterpolate()->toAST(options));

    if (hasLimitByLimit())
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, getLimitByLimit()->toAST(options));

    if (hasLimitByOffset())
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY_OFFSET, getLimitByOffset()->toAST(options));

    if (hasLimitBy())
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_BY, getLimitBy().toAST(options));

    if (hasLimit())
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, getLimit()->toAST(options));

    if (hasOffset())
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, getOffset()->toAST(options));

    if (hasSettingsChanges())
    {
        auto settings_query = std::make_shared<ASTSetQuery>();
        settings_query->changes = settings_changes;
        settings_query->is_standalone = false;
        select_query->setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(settings_query));
    }

    auto result_select_query = std::make_shared<ASTSelectWithUnionQuery>();
    result_select_query->union_mode = SelectUnionMode::UNION_DEFAULT;

    auto list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));

    result_select_query->children.push_back(std::move(list_of_selects));
    result_select_query->list_of_selects = result_select_query->children.back();

    if (is_subquery)
    {
        auto subquery = std::make_shared<ASTSubquery>(std::move(result_select_query));
        if (options.set_subquery_cte_name)
            subquery->cte_name = cte_name;
        return subquery;
    }

    return result_select_query;
}

}
