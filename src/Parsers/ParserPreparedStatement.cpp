#include <Parsers/ParserPreparedStatement.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

ASTPtr ASTPreparedStatement::clone() const
{
    auto res = std::make_shared<ASTPreparedStatement>(*this);
    res->children.clear();
    return res;
}

ASTPtr ASTExecute::clone() const
{
    auto res = std::make_shared<ASTExecute>(*this);
    res->children.clear();
    return res;
}

ASTPtr ASTDeallocate::clone() const
{
    auto res = std::make_shared<ASTDeallocate>(*this);
    res->children.clear();
    return res;
}

bool ParserPrepare::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_prepare(Keyword::PREPARE);
    ParserKeyword s_as(Keyword::AS);
    ParserIdentifier s_ident;

    auto result = std::make_shared<ASTPreparedStatement>();
    node = result;

    if (!s_prepare.ignore(pos, expected))
        return false;

    ASTPtr ast_ident;
    if (!s_ident.parse(pos, ast_ident, expected))
        return false;

    result->function_name = ast_ident->as<ASTIdentifier>()->full_name;

    if (!s_as.ignore(pos, expected))
        return false;

    result->function_body = std::string(pos->begin);
    while (!pos->isEnd())
        ++pos;

    return true;
}

bool ParserExecute::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserNotEmptyExpressionList exp_args(/*allow_alias_without_as_keyword*/ true, /*allow_trailing_commas*/ true);
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);
    ParserKeyword s_execute(Keyword::EXECUTE);
    ParserIdentifier s_ident;

    auto result = std::make_shared<ASTExecute>();
    node = result;

    if (!s_execute.ignore(pos, expected))
        return false;

    ASTPtr ast_ident;
    if (!s_ident.parse(pos, ast_ident, expected))
        return false;

    result->function_name = ast_ident->as<ASTIdentifier>()->full_name;

    if (!open_bracket.ignore(pos, expected))
        return false;

    ASTPtr ast_args;
    if (!exp_args.parse(pos, ast_args, expected))
        return false;

    for (size_t i = 0; i < ast_args->children.size(); ++i)
    {
        result->arguments.push_back(toString(ast_args->children[i]->as<ASTLiteral>()->value));
    }
    if (!close_bracket.ignore(pos, expected))
        return false;

    return true;
}

bool ParserDeallocate::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_execute(Keyword::DEALLOCATE);
    ParserIdentifier s_ident;

    auto result = std::make_shared<ASTDeallocate>();
    node = result;

    if (!s_execute.ignore(pos, expected))
        return false;

    ASTPtr ast_ident;
    if (!s_ident.parse(pos, ast_ident, expected))
        return false;

    result->function_name = ast_ident->as<ASTIdentifier>()->full_name;
    return true;
}


}
