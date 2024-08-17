#include "ParserMongoSelectQuery.h"

#include <rapidjson/document.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/IParserBase.h>
#include <Parsers/IAST_fwd.h>

#include <Parsers/Mongo/ParserMongoFilter.h>
#include <Parsers/Mongo/ParserMongoOrderBy.h>
#include <Parsers/Mongo/ParserMongoProjection.h>
#include <Parsers/Mongo/Utils.h>

namespace DB
{

namespace Mongo
{

bool ParserMongoSelectQuery::parseImpl(ASTPtr & node)
{
    auto select_query = std::make_shared<ASTSelectQuery>();

    auto projection = findField(data, "$projection");
    /// Equals to SELECT * ...
    if (!projection)
    {
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
        select_query->select()->children.push_back(std::make_shared<ASTAsterisk>());
    }
    else
    {
        /// Otherwise traverse projection tree to parse projection
        data.EraseMember("$projection");
        ASTPtr projection_node;

        auto parser = ParserMongoProjection(std::move(*projection), metadata, "$projection");
        if (!parser.parseImpl(projection_node))
        {
            return false;
        }
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(projection_node));
    }

    /// Attach collection to AST.
    node = select_query;
    ASTPtr tables = std::make_shared<ASTTablesInSelectQuery>();

    auto table_expression_ast = std::make_shared<ASTTableExpression>();
    table_expression_ast->children.push_back(std::make_shared<ASTTableIdentifier>(metadata->getCollectionName()));
    table_expression_ast->database_and_table_name = table_expression_ast->children.back();

    auto tables_in_select_query_element_ast = std::make_shared<ASTTablesInSelectQueryElement>();
    tables_in_select_query_element_ast->children.push_back(std::move(table_expression_ast));
    tables_in_select_query_element_ast->table_expression = tables_in_select_query_element_ast->children.back();

    tables->children.push_back(std::move(tables_in_select_query_element_ast));

    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

    if (metadata->getLimit())
    {
        size_t limit_value = *metadata->getLimit();
        auto literal = std::make_shared<ASTLiteral>(Field(limit_value));
        select_query->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, std::move(literal));
    }

    if (metadata->getOrderBy())
    {
        ASTPtr order_by_node;
        auto order_by = *metadata->getOrderBy();
        auto order_by_tree = parseData(order_by.data(), order_by.data() + order_by.size());
        if (!ParserMongoOrderBy(std::move(order_by_tree), metadata, "").parseImpl(order_by_node))
        {
            return false;
        }
        select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, std::move(order_by_node));
    }

    /// Traverse data tree for WHERE operator
    ASTPtr where_condition;
    if (ParserMongoFilter(std::move(data), metadata, "").parseImpl(where_condition))
    {
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_condition));
        return true;
    }
    else
    {
        return false;
    }
}

}

}
