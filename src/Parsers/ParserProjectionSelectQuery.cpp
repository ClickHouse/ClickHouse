#include <memory>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTProjectionSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserProjectionSelectQuery.h>
#include <Parsers/ParserSetQuery.h>


namespace DB
{

bool ParserProjectionColumnDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserExpression expr_name_parser;
    ParserIdentifier name_parser;
    ParserKeyword s_as{"AS"};
    ParserKeyword s_comment{"COMMENT"};
    ParserKeyword s_codec{"CODEC"};
    ParserStringLiteral string_literal_parser;
    ParserCodec codec_parser;

    /// mandatory expr name
    ASTPtr expr_name;
    if (!expr_name_parser.parse(pos, expr_name, expected))
        return false;

    if (!s_as.ignore(pos, expected))
        return false;

    /// mandatory column name
    ASTPtr name;
    if (!name_parser.parse(pos, name, expected))
        return false;

    const auto column_declaration = std::make_shared<ASTColumnDeclaration>();
    tryGetIdentifierNameInto(name, column_declaration->name);

    ASTPtr comment_expression;
    ASTPtr codec_expression;

    if (s_comment.ignore(pos, expected))
    {
        /// should be followed by a string literal
        if (!string_literal_parser.parse(pos, comment_expression, expected))
            return false;
    }

    if (s_codec.ignore(pos, expected))
    {
        if (!codec_parser.parse(pos, codec_expression, expected))
            return false;
    }

    node = column_declaration;

    if (expr_name)
    {
        column_declaration->expr_name = expr_name;
        column_declaration->children.push_back(std::move(expr_name));
    }

    if (comment_expression)
    {
        column_declaration->comment = comment_expression;
        column_declaration->children.push_back(std::move(comment_expression));
    }

    if (codec_expression)
    {
        column_declaration->codec = codec_expression;
        column_declaration->children.push_back(std::move(codec_expression));
    }

    return true;
}

bool ParserProjectionDescriptionDeclaration::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_index("INDEX");

    ParserIndexDeclaration index_p;
    ParserProjectionColumnDeclaration column_p;

    ASTPtr new_node = nullptr;

    if (s_index.ignore(pos, expected))
    {
        if (!index_p.parse(pos, new_node, expected))
            return false;
    }
    else
    {
        if (!column_p.parse(pos, new_node, expected))
            return false;
    }

    node = new_node;
    return true;
}

bool ParserProjectionDescriptionDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ASTPtr list;
    if (!ParserList(std::make_unique<ParserProjectionDescriptionDeclaration>(), std::make_unique<ParserToken>(TokenType::Comma), false)
             .parse(pos, list, expected))
        return false;

    ASTPtr columns = std::make_shared<ASTExpressionList>();
    ASTPtr indices = std::make_shared<ASTExpressionList>();

    for (const auto & elem : list->children)
    {
        if (elem->as<ASTColumnDeclaration>())
            columns->children.push_back(elem);
        else if (elem->as<ASTIndexDeclaration>())
            indices->children.push_back(elem);
        else
            return false;
    }

    ParserToken comma_p(TokenType::Comma);
    ParserKeyword s_settings("SETTINGS");
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);
    ASTPtr settings;
    ParserKeyword s_comment("COMMENT");
    ParserStringLiteral string_literal_parser;
    ASTPtr comment;

    if (comma_p.ignore(pos, expected))
    {
        if (s_comment.ignore(pos, expected))
        {
            if (!string_literal_parser.parse(pos, comment, expected))
                return false;

            if (comma_p.ignore(pos, expected))
            {
                if (!s_settings.ignore(pos, expected))
                    return false;
                if (!settings_p.parse(pos, settings, expected))
                    return false;
            }
        }
        else if (s_settings.ignore(pos, expected))
        {
            if (!settings_p.parse(pos, settings, expected))
                return false;
        }
        else
            return false;
    }

    auto res = std::make_shared<ASTProjectionColumnsWithSettingsAndComment>();
    if (!columns->children.empty())
        res->set(res->columns, columns);
    if (!indices->children.empty())
        res->set(res->indices, indices);
    if (comment)
        res->set(res->comment, comment);
    if (settings)
        res->set(res->settings, settings);
    node = res;
    return true;
}

bool ParserProjectionDescription::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserProjectionDescriptionDeclarationList projection_properties_p;

    if (!s_lparen.ignore(pos, expected))
        return false;

    if (!projection_properties_p.parse(pos, node, expected))
        return false;

    if (!s_rparen.ignore(pos, expected))
        return false;

    return true;
}

bool ParserProjectionSelectQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto select_query = std::make_shared<ASTProjectionSelectQuery>();
    node = select_query;

    ParserKeyword s_with("WITH");
    ParserKeyword s_select("SELECT");
    ParserKeyword s_group_by("GROUP BY");
    ParserKeyword s_order_by("ORDER BY");

    ParserNotEmptyExpressionList exp_list_for_with_clause(false);
    ParserNotEmptyExpressionList exp_list_for_select_clause(true); /// Allows aliases without AS keyword.
    ParserExpression order_expression_p;

    ASTPtr with_expression_list;
    ASTPtr select_expression_list;
    ASTPtr group_expression_list;
    ASTPtr order_expression_list;

    /// WITH expr list
    {
        if (s_with.ignore(pos, expected))
        {
            if (!exp_list_for_with_clause.parse(pos, with_expression_list, expected))
                return false;
        }
    }

    /// SELECT expr list
    {
        if (!s_select.ignore(pos, expected))
            return false;

        if (!exp_list_for_select_clause.parse(pos, select_expression_list, expected))
            return false;
    }

    // If group by is specified, AggregatingMergeTree engine is used, and the group by keys are implied to be order by keys
    if (s_group_by.ignore(pos, expected))
    {
        if (!ParserList(std::make_unique<ParserExpression>(), std::make_unique<ParserToken>(TokenType::Comma))
                 .parse(pos, group_expression_list, expected))
            return false;
    }

    // if order by is speficied, MergeTree engine is used, and order by keys are prepended to the
    // select list when converting to ASTSelectQuery.
    if (s_order_by.ignore(pos, expected))
    {
        if (!ParserList(std::make_unique<ParserExpression>(), std::make_unique<ParserToken>(TokenType::Comma))
                 .parse(pos, order_expression_list, expected))
            return false;
    }

    select_query->setExpression(ASTProjectionSelectQuery::Expression::WITH, std::move(with_expression_list));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::SELECT, std::move(select_expression_list));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::GROUP_BY, std::move(group_expression_list));
    select_query->setExpression(ASTProjectionSelectQuery::Expression::ORDER_BY, std::move(order_expression_list));
    return true;
}

}
