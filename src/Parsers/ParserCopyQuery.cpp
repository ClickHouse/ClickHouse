#include <Parsers/ParserCopyQuery.h>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTInterpolateElement.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserSampleRatio.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/ParserWithElement.h>

#include <memory>
#include <Parsers/ASTCopyQuery.h>

namespace DB
{

namespace
{

ASTPtr getSelectAllQuery(ASTPtr table_identifier, bool use_table_function = false)
{
    auto select_query = std::make_shared<ASTSelectQuery>();
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
    select_query->select()->children.push_back(std::make_shared<ASTAsterisk>());

    ASTPtr tables = std::make_shared<ASTTablesInSelectQuery>();

    auto table_expression_ast = std::make_shared<ASTTableExpression>();
    table_expression_ast->children.push_back(table_identifier);
    if (!use_table_function)
        table_expression_ast->database_and_table_name = table_expression_ast->children.back();
    else
        table_expression_ast->table_function = table_expression_ast->children.back();

    auto tables_in_select_query_element_ast = std::make_shared<ASTTablesInSelectQueryElement>();
    tables_in_select_query_element_ast->children.push_back(std::move(table_expression_ast));
    tables_in_select_query_element_ast->table_expression = tables_in_select_query_element_ast->children.back();

    tables->children.push_back(std::move(tables_in_select_query_element_ast));

    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

    auto result_select_query = std::make_shared<ASTSelectWithUnionQuery>();
    result_select_query->union_mode = SelectUnionMode::UNION_DEFAULT;

    auto list_of_selects = std::make_shared<ASTExpressionList>();
    list_of_selects->children.push_back(std::move(select_query));

    result_select_query->children.push_back(std::move(list_of_selects));
    result_select_query->list_of_selects = result_select_query->children.back();
    return result_select_query;
}

ASTPtr getSelectAllQuery(const String & table_name)
{
    return getSelectAllQuery(std::make_shared<ASTTableIdentifier>(table_name));
}


ASTPtr getInsertQuery(const String & table_name, ASTPtr function_storage)
{
    auto select_query = getSelectAllQuery(function_storage, true);
    auto target_table_identifier = std::make_shared<ASTIdentifier>(table_name);
    auto insert_query = std::make_shared<ASTInsertQuery>();
    insert_query->select = select_query;
    insert_query->table = target_table_identifier;
    insert_query->children.push_back(target_table_identifier);
    insert_query->children.push_back(select_query);
    insert_query->table = std::make_shared<ASTLiteral>(table_name);
    insert_query->table_id = StorageID("", table_name);
    return insert_query;
}

}

bool ParserCopyQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier s_ident;
    ParserKeyword s_copy(Keyword::COPY);
    ParserKeyword s_to(Keyword::TO);
    ParserKeyword s_from(Keyword::FROM);
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);

    ParserSubquery s_subquery;
    ParserIdentifierWithOptionalParameters s_engine;

    auto copy_element = std::make_shared<ASTCopyQuery>();

    if (!s_copy.ignore(pos, expected))
        return false;

    auto saved_pos = pos;
    if (!open_bracket.ignore(pos, expected))
    {
        ParserIdentifier s_table_identifier;
        ASTPtr table_name;
        if (!s_table_identifier.parse(pos, table_name, expected))
            return false;

        auto table_name_str = table_name->as<ASTIdentifier>()->name();

        saved_pos = pos;
        if (s_to.ignore(pos, expected))
        {
            copy_element->type = ASTCopyQuery::QueryType::COPY_TO;
            copy_element->data = getSelectAllQuery(table_name_str);
        }
        else if (pos = saved_pos; s_from.ignore(pos, expected))
        {
            copy_element->type = ASTCopyQuery::QueryType::COPY_FROM;
        }
        else
        {
            return false;
        }

        if (s_engine.parse(pos, copy_element->file, expected))
        {
            node = copy_element;
            if (copy_element->type == ASTCopyQuery::QueryType::COPY_FROM)
                copy_element->data = getInsertQuery(table_name_str, copy_element->file);
            return true;
        }
        else
        {
            return false;
        }
    }

    pos = saved_pos;
    ASTPtr name_or_expr;
    if (!(s_ident.parse(pos, name_or_expr, expected) || ParserExpressionWithOptionalAlias(false).parse(pos, name_or_expr, expected)))
    {
        return false;
    }

    saved_pos = pos;
    if (s_to.ignore(pos, expected))
    {
        copy_element->type = ASTCopyQuery::QueryType::COPY_TO;
    }
    else
    {
        return false;
    }

    if (s_engine.parse(pos, copy_element->file, expected))
    {
        copy_element->data = name_or_expr->children[0];
        node = copy_element;
    }
    else
    {
        return false;
    }
    return true;
}

}
