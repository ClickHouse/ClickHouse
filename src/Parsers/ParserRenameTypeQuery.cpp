#include <Parsers/ParserRenameTypeQuery.h>

#include <Parsers/ASTAlterTypeCommentQuery.h>
#include <Parsers/ASTRenameTypeQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace
{

bool parseTypeName(IParser::Pos & pos, Expected & expected, ASTPtr & database, ASTPtr & type_name)
{
    ParserIdentifier identifier_parser;
    ParserToken dot(TokenType::Dot);
    if (!identifier_parser.parse(pos, type_name, expected))
        return false;
    if (dot.ignore(pos, expected))
    {
        database = type_name;
        if (!identifier_parser.parse(pos, type_name, expected))
            return false;
        if (dot.check(pos, expected))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "A type name can contain at most a database and a type component");
    }
    return true;
}

}

bool ParserRenameTypeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword::createDeprecated("ALTER TYPE").ignore(pos, expected))
        return false;

    const bool if_exists = ParserKeyword(Keyword::IF_EXISTS).ignore(pos, expected);

    ASTPtr database;
    ASTPtr type_name;
    if (!parseTypeName(pos, expected, database, type_name))
        return false;

    String cluster;
    if (ParserKeyword(Keyword::ON).ignore(pos, expected) && !ASTQueryWithOnCluster::parse(pos, cluster, expected))
        return false;

    ASTPtr new_type_name;
    ASTPtr comment;
    if (ParserKeyword::createDeprecated("RENAME TO").ignore(pos, expected))
    {
        if (!ParserIdentifier().parse(pos, new_type_name, expected))
            return false;
        if (ParserToken(TokenType::Dot).check(pos, expected))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "ALTER TYPE RENAME TO cannot move a type to another database");
    }
    else if (ParserKeyword(Keyword::COMMENT).ignore(pos, expected))
    {
        if (!ParserStringLiteral().parse(pos, comment, expected))
            return false;
        if (comment->as<ASTLiteral &>().value.safeGet<String>().find('\0') != String::npos)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "ALTER TYPE COMMENT cannot contain a NUL byte");
    }
    else
    {
        if (ParserKeyword(Keyword::AS).checkWithoutMoving(pos, expected))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "ALTER TYPE supports RENAME TO or COMMENT only");
        return false;
    }

    if (comment)
    {
        auto query = make_intrusive<ASTAlterTypeCommentQuery>();
        query->if_exists = if_exists;
        query->database = std::move(database);
        query->type_name = std::move(type_name);
        query->comment = std::move(comment);
        query->cluster = std::move(cluster);
        query->normalizeChildrenOrder();
        node = std::move(query);
    }
    else
    {
        auto query = make_intrusive<ASTRenameTypeQuery>();
        query->if_exists = if_exists;
        query->database = std::move(database);
        query->type_name = std::move(type_name);
        query->new_type_name = std::move(new_type_name);
        query->cluster = std::move(cluster);
        query->normalizeChildrenOrder();
        node = std::move(query);
    }
    return true;
}

}
