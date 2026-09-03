#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>


namespace DB::PrometheusQueryToSQL
{

ASTPtr SelectQueryBuilder::getSelectQuery()
{
    auto select_query = make_intrusive<ASTSelectQuery>();

    chassert(!select_list.empty());
    auto select_list_exp = make_intrusive<ASTExpressionList>();
    select_list_exp->children = select_list;
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));

    if (!from_table.empty() || from_table_function)
    {
        auto table_exp = make_intrusive<ASTTableExpression>();
        if (!from_table.empty())
        {
            table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(std::move(from_table));
            table_exp->children.emplace_back(table_exp->database_and_table_name);
        }
        else if (from_table_function)
        {
            table_exp->table_function = std::move(from_table_function);
            table_exp->children.emplace_back(table_exp->table_function);
        }
        auto table = make_intrusive<ASTTablesInSelectQueryElement>();
        table->table_expression = table_exp;
        table->children.push_back(std::move(table_exp));

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(std::move(table));

        if (!join_table.empty())
        {
            auto table_join = make_intrusive<ASTTableJoin>();
            table_join->kind = join_kind;
            table_join->strictness = join_strictness;
            table_join->on_expression = std::move(join_on);
            table_join->children.emplace_back(table_join->on_expression);

            table_exp = make_intrusive<ASTTableExpression>();
            table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(std::move(join_table));
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table = make_intrusive<ASTTablesInSelectQueryElement>();
            table->table_join = table_join;
            table->table_expression = table_exp;
            table->children.push_back(std::move(table_join));
            table->children.push_back(std::move(table_exp));

            tables->children.push_back(std::move(table));
        }

        select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
    }

    if (!group_by.empty())
    {
        auto group_by_list = make_intrusive<ASTExpressionList>();
        group_by_list->children = std::move(group_by);
        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_by_list));
    }

    if (having)
        select_query->setExpression(ASTSelectQuery::Expression::HAVING, std::move(having));

    if (!order_by.empty())
    {
        auto order_by_list = make_intrusive<ASTExpressionList>();
        for (auto & order_by_expression : order_by)
        {
            auto order_by_element = make_intrusive<ASTOrderByElement>();
            order_by_element->children.push_back(std::move(order_by_expression));
            chassert(abs(order_direction) == 1); /// `direction` must be set either to 1 or -1
            order_by_element->direction = order_direction;
            order_by_list->children.push_back(std::move(order_by_element));
        }
        select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_by_list));
    }

    if (where)
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where));

    if (limit)
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, make_intrusive<ASTLiteral>(*limit));

    if (!with.empty())
    {
        auto with_expression_list_ast = make_intrusive<ASTExpressionList>();
        for (auto & subquery : with)
        {
            auto subquery_ast = make_intrusive<ASTSubquery>(std::move(subquery.ast));
            if (subquery.subquery_type == SQLSubqueryType::TABLE)
            {
                auto with_element_ast = make_intrusive<ASTWithElement>();
                with_element_ast->name = std::move(subquery.name);
                with_element_ast->subquery = subquery_ast;
                with_element_ast->children.push_back(std::move(subquery_ast));
                with_expression_list_ast->children.push_back(std::move(with_element_ast));
            }
            else
            {
                subquery_ast->setAlias(subquery.name);
                with_expression_list_ast->children.push_back(std::move(subquery_ast));
            }
        }
        select_query->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list_ast));
    }

    auto list_of_selects = make_intrusive<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));
    std::optional<SelectUnionMode> union_mode;

    if (!union_table.empty())
    {
        select_query = make_intrusive<ASTSelectQuery>();

        select_list_exp = make_intrusive<ASTExpressionList>();
        select_list_exp->children.reserve(select_list.size());
        for (const auto & select_exp : select_list)
            select_list_exp->children.push_back(select_exp->clone());
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        auto table = make_intrusive<ASTTablesInSelectQueryElement>();

        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(std::move(union_table));
        table_exp->children.emplace_back(table_exp->database_and_table_name);
        table->table_expression = table_exp;
        table->children.push_back(std::move(table_exp));

        tables->children.push_back(std::move(table));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

        list_of_selects->children.push_back(std::move(select_query));
        union_mode = SelectUnionMode::UNION_ALL;
    }

    auto select_with_union_query = make_intrusive<ASTSelectWithUnionQuery>();
    select_with_union_query->list_of_selects = list_of_selects;
    select_with_union_query->children.push_back(std::move(list_of_selects));

    if (union_mode)
    {
        select_with_union_query->union_mode = *union_mode;
        select_with_union_query->is_normalized = true;
    }

    /// We moved some fields so the builder can't be used again.
    select_list.clear();

    return select_with_union_query;
}

}
