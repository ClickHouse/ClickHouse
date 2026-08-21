#include <Parsers/ParserHypotheticalObjectQuery.h>

#include <Parsers/ASTHypotheticalObjectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTProjectionDeclaration.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateIndexQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserProjectionSelectQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>

namespace DB
{

namespace
{

/// `(SELECT ...) [WITH SETTINGS (...)]` — the body of a projection declaration, without the leading
/// name. The name and the target table are parsed by the caller, so that
/// `CREATE HYPOTHETICAL PROJECTION name ON table (...)` reads the same way as the index statement
bool parseProjectionBody(IParser::Pos & pos, Expected & expected, const String & name, ASTPtr & node)
{
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserKeyword s_with_settings(Keyword::WITH_SETTINGS);
    ParserProjectionSelectQuery query_p;
    ParserSetQuery settings_p(/* parse_only_internals_ = */ true);

    if (!s_lparen.ignore(pos, expected))
        return false;

    ASTPtr query;
    if (!query_p.parse(pos, query, expected))
        return false;

    if (!s_rparen.ignore(pos, expected))
        return false;

    ASTPtr with_settings;
    if (s_with_settings.ignore(pos, expected))
    {
        if (!s_lparen.ignore(pos, expected))
            return false;
        if (!settings_p.parse(pos, with_settings, expected))
            return false;
        if (!s_rparen.ignore(pos, expected))
            return false;
    }

    auto projection = make_intrusive<ASTProjectionDeclaration>();
    projection->name = name;
    projection->set(projection->query, query);
    if (with_settings)
        projection->set(projection->with_settings, with_settings);
    node = projection;
    return true;
}

}

bool ParserHypotheticalObjectQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_all(Keyword::ALL);
    ParserKeyword s_hypothetical(Keyword::HYPOTHETICAL);
    ParserKeyword s_index(Keyword::INDEX);
    ParserKeyword s_indexes(Keyword::INDEXES);
    ParserKeyword s_projection(Keyword::PROJECTION);
    ParserKeyword s_projections(Keyword::PROJECTIONS);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);

    ParserIdentifier object_name_p;
    ParserCreateIndexDeclaration parser_create_idx_decl;

    auto query = make_intrusive<ASTHypotheticalObjectQuery>();

    if (s_create.ignore(pos, expected))
    {
        if (!s_hypothetical.ignore(pos, expected))
            return false;

        if (s_index.ignore(pos, expected))
            query->object_kind = ASTHypotheticalObjectQuery::Index;
        else if (s_projection.ignore(pos, expected))
            query->object_kind = ASTHypotheticalObjectQuery::Projection;
        else
            return false;

        query->kind = ASTHypotheticalObjectQuery::Create;

        if (s_if_not_exists.ignore(pos, expected))
            query->if_not_exists = true;

        ASTPtr object_name;
        if (!object_name_p.parse(pos, object_name, expected))
            return false;

        if (!s_on.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
            return false;

        query->object_name = object_name;
        query->children.push_back(object_name);

        if (query->object_kind == ASTHypotheticalObjectQuery::Projection)
        {
            ASTPtr projection_decl;
            if (!parseProjectionBody(pos, expected, object_name->as<ASTIdentifier &>().name(), projection_decl))
                return false;

            query->projection_decl = projection_decl;
            query->children.push_back(projection_decl);
        }
        else
        {
            ASTPtr index_decl;
            if (!parser_create_idx_decl.parse(pos, index_decl, expected))
                return false;

            index_decl->as<ASTIndexDeclaration &>().name = object_name->as<ASTIdentifier &>().name();

            query->index_decl = index_decl;
            query->children.push_back(index_decl);
        }
    }
    else if (s_drop.ignore(pos, expected))
    {
        /// DROP ALL HYPOTHETICAL INDEXES | DROP ALL HYPOTHETICAL PROJECTIONS
        if (s_all.ignore(pos, expected))
        {
            if (!s_hypothetical.ignore(pos, expected))
                return false;

            if (s_indexes.ignore(pos, expected))
                query->object_kind = ASTHypotheticalObjectQuery::Index;
            else if (s_projections.ignore(pos, expected))
                query->object_kind = ASTHypotheticalObjectQuery::Projection;
            else
                return false;

            query->kind = ASTHypotheticalObjectQuery::DropAll;
            node = query;
            return true;
        }

        if (!s_hypothetical.ignore(pos, expected))
            return false;

        if (s_index.ignore(pos, expected))
            query->object_kind = ASTHypotheticalObjectQuery::Index;
        else if (s_projection.ignore(pos, expected))
            query->object_kind = ASTHypotheticalObjectQuery::Projection;
        else
            return false;

        query->kind = ASTHypotheticalObjectQuery::Drop;

        if (s_if_exists.ignore(pos, expected))
            query->if_exists = true;

        ASTPtr object_name;
        if (!object_name_p.parse(pos, object_name, expected))
            return false;

        if (!s_on.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
            return false;

        query->object_name = object_name;
        query->children.push_back(object_name);
    }
    else
    {
        return false;
    }

    if (query->database)
        query->children.push_back(query->database);
    if (query->table)
        query->children.push_back(query->table);

    node = query;
    return true;
}

}
