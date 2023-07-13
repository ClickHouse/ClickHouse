#include <Analyzer/QueryNode.h>

#include <fmt/core.h>

#include <Common/assert_cast.h>
#include <Common/SipHash.h>
#include <Common/FieldVisitorToString.h>

#include <Core/NamesAndTypes.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>

#include <Analyzer/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
}

QueryNode::QueryNode(ContextMutablePtr context_)
    : QueryNode(std::move(context_), {} /*settings_changes*/)
{}

void QueryNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "QUERY id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    if (is_subquery)
        buffer << ", is_subquery: " << is_subquery;

    if (is_cte)
        buffer << ", is_cte: " << is_cte;

    if (is_distinct)
        buffer << ", is_distinct: " << is_distinct;

    if (is_limit_with_ties)
        buffer << ", is_limit_with_ties: " << is_limit_with_ties;

    if (is_group_by_with_totals)
        buffer << ", is_group_by_with_totals: " << is_group_by_with_totals;

    if (is_group_by_all)
        buffer << ", is_group_by_all: " << is_group_by_all;

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

bool QueryNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const QueryNode &>(rhs);

    return is_subquery == rhs_typed.is_subquery &&
        is_cte == rhs_typed.is_cte &&
        is_distinct == rhs_typed.is_distinct &&
        is_limit_with_ties == rhs_typed.is_limit_with_ties &&
        is_group_by_with_totals == rhs_typed.is_group_by_with_totals &&
        is_group_by_with_rollup == rhs_typed.is_group_by_with_rollup &&
        is_group_by_with_cube == rhs_typed.is_group_by_with_cube &&
        is_group_by_with_grouping_sets == rhs_typed.is_group_by_with_grouping_sets &&
        is_group_by_all == rhs_typed.is_group_by_all &&
        cte_name == rhs_typed.cte_name &&
        projection_columns == rhs_typed.projection_columns &&
        settings_changes == rhs_typed.settings_changes;
}

void QueryNode::updateTreeHashImpl(HashState & state) const
{
    state.update(is_subquery);
    state.update(is_cte);

    state.update(cte_name.size());
    state.update(cte_name);

    state.update(projection_columns.size());
    for (const auto & projection_column : projection_columns)
    {
        state.update(projection_column.name.size());
        state.update(projection_column.name);

        auto projection_column_type_name = projection_column.type->getName();
        state.update(projection_column_type_name.size());
        state.update(projection_column_type_name);
    }

    state.update(is_distinct);
    state.update(is_limit_with_ties);
    state.update(is_group_by_with_totals);
    state.update(is_group_by_with_rollup);
    state.update(is_group_by_with_cube);
    state.update(is_group_by_with_grouping_sets);
    state.update(is_group_by_all);

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
    result_query_node->is_distinct = is_distinct;
    result_query_node->is_limit_with_ties = is_limit_with_ties;
    result_query_node->is_group_by_with_totals = is_group_by_with_totals;
    result_query_node->is_group_by_with_rollup = is_group_by_with_rollup;
    result_query_node->is_group_by_with_cube = is_group_by_with_cube;
    result_query_node->is_group_by_with_grouping_sets = is_group_by_with_grouping_sets;
    result_query_node->is_group_by_all = is_group_by_all;
    result_query_node->cte_name = cte_name;
    result_query_node->projection_columns = projection_columns;
    result_query_node->settings_changes = settings_changes;

    return result_query_node;
}

ASTPtr QueryNode::toASTImpl(const ConvertToASTOptions & options) const
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    select_query->distinct = is_distinct;
    select_query->limit_with_ties = is_limit_with_ties;
    select_query->group_by_with_totals = is_group_by_with_totals;
    select_query->group_by_with_rollup = is_group_by_with_rollup;
    select_query->group_by_with_cube = is_group_by_with_cube;
    select_query->group_by_with_grouping_sets = is_group_by_with_grouping_sets;
    select_query->group_by_all = is_group_by_all;

    if (hasWith())
        select_query->setExpression(ASTSelectQuery::Expression::WITH, getWith().toAST(options));

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
        auto subquery = std::make_shared<ASTSubquery>();

        subquery->cte_name = cte_name;
        subquery->children.push_back(std::move(result_select_query));

        return subquery;
    }

    return result_select_query;
}

}
