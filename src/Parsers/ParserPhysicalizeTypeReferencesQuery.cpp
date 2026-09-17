#include <Parsers/ParserPhysicalizeTypeReferencesQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPhysicalizeTypeReferencesQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Common/Exception.h>

#include <array>
#include <cstddef>
#include <string_view>


namespace DB
{

namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace
{

constexpr size_t max_apply_token_size = 4096;

bool parseObjectKind(IParser::Pos & pos, Expected & expected, ASTPtr & object_kind)
{
    if (ParserKeyword::createDeprecated("MATERIALIZED VIEW").ignore(pos, expected))
    {
        object_kind = make_intrusive<ASTIdentifier>("MATERIALIZED VIEW");
        return true;
    }
    return ParserIdentifier().parse(pos, object_kind, expected);
}

bool parseObjectName(IParser::Pos & pos, Expected & expected, ASTPtr & database, ASTPtr & object_name)
{
    ParserIdentifier identifier_parser;
    ParserToken dot(TokenType::Dot);

    if (!identifier_parser.parse(pos, object_name, expected))
        return false;

    if (dot.ignore(pos, expected))
    {
        database = object_name;
        if (!identifier_parser.parse(pos, object_name, expected))
            return false;
        if (dot.check(pos, expected))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "An object name can contain at most a database and an object component");
    }

    return true;
}

void rejectAdditionalDryRunClause(IParser::Pos & pos, Expected & expected)
{
    static constexpr std::array<std::string_view, 7> clauses{
        "OBJECT", "CLOSURE OF", "DATABASE", "ON CLUSTER", "DROP UNUSED TYPES", "DRY RUN", "APPLY"};

    for (const auto clause : clauses)
    {
        auto parser = ParserKeyword::createDeprecated(clause);
        if (parser.checkWithoutMoving(pos, expected))
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Duplicate or out-of-order {} clause in PHYSICALIZE TYPE REFERENCES", clause);
    }
}

}

bool ParserPhysicalizeTypeReferencesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword::createDeprecated("PHYSICALIZE TYPE REFERENCES").ignore(pos, expected))
        return false;

    auto query = make_intrusive<ASTPhysicalizeTypeReferencesQuery>();

    if (ParserKeyword::createDeprecated("OBJECT").ignore(pos, expected))
    {
        query->scope = ASTPhysicalizeTypeReferencesQuery::Scope::Object;
        if (!parseObjectKind(pos, expected, query->object_kind) || !parseObjectName(pos, expected, query->database, query->object_name))
            return false;
    }
    else if (ParserKeyword::createDeprecated("CLOSURE OF").ignore(pos, expected))
    {
        query->scope = ASTPhysicalizeTypeReferencesQuery::Scope::DependentClosure;
        if (!parseObjectKind(pos, expected, query->object_kind) || !parseObjectName(pos, expected, query->database, query->object_name))
            return false;
    }
    else if (ParserKeyword::createDeprecated("DATABASE").ignore(pos, expected))
    {
        query->scope = ASTPhysicalizeTypeReferencesQuery::Scope::Database;
        if (!ParserIdentifier().parse(pos, query->database, expected))
            return false;
    }
    else
    {
        return false;
    }

    if (ParserKeyword(Keyword::ON).ignore(pos, expected) && !ASTQueryWithOnCluster::parse(pos, query->cluster, expected))
        return false;

    if (ParserKeyword::createDeprecated("DROP UNUSED TYPES").ignore(pos, expected))
        query->drop_unused_types = true;

    if (!ParserKeyword::createDeprecated("DRY RUN").ignore(pos, expected))
    {
        rejectAdditionalDryRunClause(pos, expected);
        throw Exception(ErrorCodes::SYNTAX_ERROR, "PHYSICALIZE TYPE REFERENCES planning queries require DRY RUN");
    }

    rejectAdditionalDryRunClause(pos, expected);

    query->normalizeChildrenOrder();
    node = std::move(query);
    return true;
}

bool ParserApplyPhysicalizeTypeReferencesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword::createDeprecated("PHYSICALIZE TYPE REFERENCES APPLY TOKEN").ignore(pos, expected))
        return false;

    ASTPtr token_ast;
    if (!ParserStringLiteral().parse(pos, token_ast, expected))
        return false;

    auto token = token_ast->as<ASTLiteral &>().value.safeGet<String>();
    if (token.size() > max_apply_token_size)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR, "PHYSICALIZE TYPE REFERENCES apply token exceeds the {}-byte limit", max_apply_token_size);
    if (token.find('\0') != String::npos)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "PHYSICALIZE TYPE REFERENCES apply token cannot contain a NUL byte");

    auto query = make_intrusive<ASTApplyPhysicalizeTypeReferencesQuery>();
    query->setToken(std::move(token));
    node = std::move(query);
    return true;
}

}
