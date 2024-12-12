#include "Parsers/GoogleSQL/ParserGoogleSQLQuery.h"
#include <memory>
#include "Core/Types.h"
#include "IO/WriteHelpers.h"
#include "Parsers/ASTAsterisk.h"
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTSelectQuery.h"
#include "Parsers/ASTSelectWithUnionQuery.h"
#include "Parsers/ASTTablesInSelectQuery.h"
#include "Parsers/CommonParsers.h"
#include "Parsers/ExpressionElementParsers.h"
#include "Parsers/IAST_fwd.h"
#include "Parsers/GoogleSQL/ASTPipelineQuery.h"
#include "Parsers/IParser.h"
#include "Parsers/ParserCreateQuery.h"
#include "Parsers/ParserTablesInSelectQuery.h"
#include <IO/WriteBufferFromOStream.h>
#include "Common/Exception.h"

namespace DB::GoogleSQL
{
        bool ParserGoogleSQLQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
        {
            ASTPtr table_identifier;
            ParserIdentifier table_parser;

            if(!ParserKeyword(Keyword::FROM).ignore(pos, expected))
                return false;

            if(!table_parser.parse(pos, table_identifier, expected))
                return false;

            auto pipeline_query = std::make_shared<ASTPipelineQuery>();

            pipeline_query->stages.push_back({
                ASTPipelineQuery::StageType::FROM,
                table_identifier
            });

            node = translateToClickHouseAST(pipeline_query);
            return true;
        }

        ASTPtr ParserGoogleSQLQuery::translateToClickHouseAST(std::shared_ptr<ASTPipelineQuery> & query_ast)
        {
            auto result = std::make_shared<ASTSelectWithUnionQuery>();

            // FROM table_identifier; query.
            if (query_ast->stages.size() == 1) {
                auto select_ast = std::make_shared<ASTSelectQuery>();
                select_ast->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
                select_ast->select()->children.push_back(std::make_shared<ASTAsterisk>());

                auto list_of_selects = std::make_shared<ASTExpressionList>();
                list_of_selects->children.push_back(select_ast);

                result->children.push_back(std::move(list_of_selects));
                result->list_of_selects = result->children.back();

                auto tables = std::make_shared<ASTTablesInSelectQuery>();
                select_ast->setExpression(ASTSelectQuery::Expression::TABLES, tables);
                auto tables_element = std::make_shared<ASTTablesInSelectQueryElement>();
                auto tables_expression = std::make_shared<ASTTableExpression>();
                tables->children.push_back(tables_element);
                tables_element->table_expression = tables_expression;
                tables_element->children.push_back(tables_expression);
                auto table_name = query_ast->stages[0].expression->as<ASTIdentifier&>().full_name;
                auto  table_identifier = std::make_shared<ASTTableIdentifier>(table_name);

                tables_expression->database_and_table_name = table_identifier;
                tables_expression->children.push_back(table_identifier);
            } else {
                throw Exception();
            }

            return result;
        }

        /**
        auto result_select_query = std::make_shared<ASTSelectWithUnionQuery>();

    {
        auto select_ast = std::make_shared<ASTSelectQuery>();
        select_ast->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
        select_ast->select()->children.push_back(std::make_shared<ASTAsterisk>());

        auto list_of_selects = std::make_shared<ASTExpressionList>();
        list_of_selects->children.push_back(select_ast);

        result_select_query->children.push_back(std::move(list_of_selects));
        result_select_query->list_of_selects = result_select_query->children.back();

        {
            auto tables = std::make_shared<ASTTablesInSelectQuery>();
            select_ast->setExpression(ASTSelectQuery::Expression::TABLES, tables);
            auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_expr = std::make_shared<ASTTableExpression>();
            tables->children.push_back(tables_elem);
            tables_elem->table_expression = table_expr;
            tables_elem->children.push_back(table_expr);

            table_expr->table_function = ast_function;
            table_expr->children.push_back(table_expr->table_function);
        }
    }

    return result_select_query;

         */


}
