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
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list_exp);
    select_list_exp->children = std::move(params.select_list);

    if (!params.from_subquery.empty() || params.from_table_function)
    {
        auto tables = std::make_shared<ASTTablesInSelectQuery>();
        auto table = std::make_shared<ASTTablesInSelectQueryElement>();
        auto table_exp = std::make_shared<ASTTableExpression>();
        if (!params.from_subquery.empty())
        {
            table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(params.from_subquery);
            table_exp->children.emplace_back(table_exp->database_and_table_name);
        }
        else if (params.from_table_function)
        {
            table_exp->table_function = params.from_table_function;
            table_exp->children.emplace_back(table_exp->table_function);
        }
        table->table_expression = table_exp;
        tables->children.push_back(table);
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
    }

    if (!params.group_by.empty())
    {
        auto group_by_list = std::make_shared<ASTExpressionList>();
        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by_list);
        group_by_list->children = std::move(params.group_by);
    }

    if (!params.order_by.empty())
    {
        auto order_by_list = std::make_shared<ASTExpressionList>();
        select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, order_by_list);
        for (auto & order_by_expression : params.order_by)
        {
            auto order_by_element = std::make_shared<ASTOrderByElement>();
            order_by_list->children.push_back(order_by_element);
            order_by_element->children.push_back(std::move(order_by_expression));
            chassert(abs(params.order_direction) == 1); /// `direction` must be set either to 1 or -1
            order_by_element->direction = params.order_direction;
        }
    }

    if (params.where)
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(params.where));

    if (params.limit)
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::make_shared<ASTLiteral>(*params.limit));

    if (!params.with.empty())
    {
        auto with_expression_list_ast = std::make_shared<ASTExpressionList>();
        for (const auto & subquery : params.with)
        {
            auto subquery_ast = std::make_shared<ASTSubquery>(subquery.ast);
            if (subquery.subquery_type == SQLSubqueryType::TABLE)
            {
                auto with_element_ast = std::make_shared<ASTWithElement>();
                with_element_ast->name = subquery.name;
                with_element_ast->subquery = subquery_ast;
                with_element_ast->children.push_back(subquery_ast);
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

    auto select_with_union_query = std::make_shared<ASTSelectWithUnionQuery>();
    auto list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));
    select_with_union_query->list_of_selects = list_of_selects;
    select_with_union_query->children.push_back(list_of_selects);

    return select_with_union_query;
}

}
