#include <Parsers/ParserDropTypeQuery.h>

#include <Parsers/ASTDropTypeQuery.h>
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

bool ParserDropTypeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword::createDeprecated("DROP TYPE").ignore(pos, expected))
        return false;

    const bool if_exists = ParserKeyword(Keyword::IF_EXISTS).ignore(pos, expected);

    ASTPtr database;
    ASTPtr type_name;
    if (!parseTypeName(pos, expected, database, type_name))
        return false;

    String cluster;
    if (ParserKeyword(Keyword::ON).ignore(pos, expected) && !ASTQueryWithOnCluster::parse(pos, cluster, expected))
        return false;

    auto restrict_keyword = ParserKeyword::createDeprecated("RESTRICT");
    restrict_keyword.ignore(pos, expected);

    if (ParserKeyword::createDeprecated("CASCADE").checkWithoutMoving(pos, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "DROP TYPE supports RESTRICT semantics only");
    if (restrict_keyword.checkWithoutMoving(pos, expected))
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate RESTRICT clause in DROP TYPE");

    auto query = make_intrusive<ASTDropTypeQuery>();
    query->if_exists = if_exists;
    query->database = std::move(database);
    query->type_name = std::move(type_name);
    query->cluster = std::move(cluster);
    query->normalizeChildrenOrder();
    node = std::move(query);
    return true;
}

}
