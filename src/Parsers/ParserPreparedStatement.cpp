#include <Parsers/ParserPreparedStatement.h>

#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ASTPtr ASTPreparedStatement::clone() const
{
    auto res = make_intrusive<ASTPreparedStatement>(*this);
    res->children.clear();
    return res;
}

ASTPtr ASTExecute::clone() const
{
    auto res = make_intrusive<ASTExecute>(*this);
    res->children.clear();
    return res;
}

ASTPtr ASTDeallocate::clone() const
{
    auto res = make_intrusive<ASTDeallocate>(*this);
    res->children.clear();
    return res;
}

bool ParserPrepare::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_prepare(Keyword::PREPARE);
    ParserKeyword s_as(Keyword::AS);
    ParserIdentifier s_ident;

    auto result = make_intrusive<ASTPreparedStatement>();
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

    auto result = make_intrusive<ASTExecute>();
    node = result;

    if (!s_execute.ignore(pos, expected))
        return false;

    ASTPtr ast_ident;
    if (!s_ident.parse(pos, ast_ident, expected))
        return false;

    result->function_name = ast_ident->as<ASTIdentifier>()->full_name;

    /// Accept both forms of a zero-parameter statement: `EXECUTE s` and `EXECUTE s()`.
    if (open_bracket.ignore(pos, expected) && !close_bracket.ignore(pos, expected))
    {
        ASTPtr ast_args;
        if (!exp_args.parse(pos, ast_args, expected))
            return false;

        for (const auto & child : ast_args->children)
        {
            /// Expressions cannot be substituted as one parameter value without changing semantics.
            const auto * literal = child->as<ASTLiteral>();
            if (!literal)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "EXECUTE argument must be a literal value, not an expression: {}",
                    child->formatWithSecretsOneLine());
            /// `FieldVisitorToString` quotes and escapes string literals.
            result->arguments.push_back(applyVisitor(FieldVisitorToString(), literal->value));
        }

        if (!close_bracket.ignore(pos, expected))
            return false;
    }

    return true;
}

bool ParserDeallocate::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_execute(Keyword::DEALLOCATE);
    ParserIdentifier s_ident;

    auto result = make_intrusive<ASTDeallocate>();
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
