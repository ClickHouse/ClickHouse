#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>

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

ASTPtr buildSelectQuery(SelectQueryParams && params)
{
    auto select_query = std::make_shared<ASTSelectQuery>();

    auto select_list_exp = std::make_shared<ASTExpressionList>();
    select_list_exp->children = params.select_list;
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));

    if (!params.from_table.empty() || params.from_table_function)
    {
        auto table_exp = std::make_shared<ASTTableExpression>();
        if (!params.from_table.empty())
        {
            table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(std::move(params.from_table));
            table_exp->children.emplace_back(table_exp->database_and_table_name);
        }
        else if (params.from_table_function)
        {
            table_exp->table_function = std::move(params.from_table_function);
            table_exp->children.emplace_back(table_exp->table_function);
        }
        auto table = std::make_shared<ASTTablesInSelectQueryElement>();
        table->table_expression = table_exp;
        table->children.push_back(std::move(table_exp));

        auto tables = std::make_shared<ASTTablesInSelectQuery>();
        tables->children.push_back(std::move(table));

        if (!params.join_table.empty())
        {
            auto table_join = std::make_shared<ASTTableJoin>();
            table_join->kind = params.join_kind;
            table_join->strictness = params.join_strictness;
            table_join->on_expression = std::move(params.join_on);
            table_join->children.emplace_back(table_join->on_expression);

            table_exp = std::make_shared<ASTTableExpression>();
            table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(std::move(params.join_table));
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table = std::make_shared<ASTTablesInSelectQueryElement>();
            table->table_join = table_join;
            table->table_expression = table_exp;
            table->children.push_back(std::move(table_join));
            table->children.push_back(std::move(table_exp));

            tables->children.push_back(std::move(table));
        }

        select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));
    }

    if (!params.group_by.empty())
    {
        auto group_by_list = std::make_shared<ASTExpressionList>();
        group_by_list->children = std::move(params.group_by);
        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_by_list));
    }

    if (!params.order_by.empty())
    {
        auto order_by_list = std::make_shared<ASTExpressionList>();
        for (auto & order_by_expression : params.order_by)
        {
            auto order_by_element = std::make_shared<ASTOrderByElement>();
            order_by_element->children.push_back(std::move(order_by_expression));
            chassert(abs(params.order_direction) == 1); /// `direction` must be set either to 1 or -1
            order_by_element->direction = params.order_direction;
            order_by_list->children.push_back(std::move(order_by_element));
        }
        select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_by_list));
    }

    if (params.where)
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(params.where));

    if (params.limit)
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::make_shared<ASTLiteral>(*params.limit));

    if (!params.with.empty())
    {
        auto with_expression_list_ast = std::make_shared<ASTExpressionList>();
        for (auto & subquery : params.with)
        {
            auto subquery_ast = std::make_shared<ASTSubquery>(std::move(subquery.ast));
            if (subquery.subquery_type == SQLSubqueryType::TABLE)
            {
                auto with_element_ast = std::make_shared<ASTWithElement>();
                with_element_ast->name = std::move(subquery.name);
                with_element_ast->subquery = subquery_ast;
                with_element_ast->children.push_back(std::move(subquery_ast));
                with_expression_list_ast->children.push_back(std::move(with_element_ast));
            }
            else
            {
                subquery_ast->setAlias(std::move(subquery.name));
                with_expression_list_ast->children.push_back(std::move(subquery_ast));
            }
        }
        select_query->setExpression(ASTSelectQuery::Expression::WITH, std::move(with_expression_list_ast));
    }

    auto list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));
    std::optional<SelectUnionMode> union_mode;

    if (!params.union_table.empty())
    {
        select_query = std::make_shared<ASTSelectQuery>();

        select_list_exp = std::make_shared<ASTExpressionList>();
        select_list_exp->children.reserve(params.select_list.size());
        for (auto select_exp : params.select_list)
            select_list_exp->children.push_back(select_exp->clone());
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_list_exp));

        auto tables = std::make_shared<ASTTablesInSelectQuery>();
        auto table = std::make_shared<ASTTablesInSelectQueryElement>();

        auto table_exp = std::make_shared<ASTTableExpression>();
        table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(std::move(params.union_table));
        table_exp->children.emplace_back(table_exp->database_and_table_name);
        table->table_expression = table_exp;
        table->children.push_back(std::move(table_exp));

        tables->children.push_back(std::move(table));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

        list_of_selects->children.push_back(std::move(select_query));
        union_mode = SelectUnionMode::UNION_ALL;
    }

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    select_with_union_query->list_of_selects = list_of_selects;
    select_with_union_query->children.push_back(std::move(list_of_selects));

    if (union_mode)
    {
        select_with_union_query->union_mode = *union_mode;
        select_with_union_query->is_normalized = true;
    }

    return select_with_union_query;
}

}
