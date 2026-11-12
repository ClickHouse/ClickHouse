#include <Parsers/ParserHypotheticalIndexQuery.h>

#include <Parsers/ASTHypotheticalIndexQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateIndexQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>

namespace DB
{

bool ParserHypotheticalIndexQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_all(Keyword::ALL);
    ParserKeyword s_hypothetical(Keyword::HYPOTHETICAL);
    ParserKeyword s_index(Keyword::INDEX);
    ParserKeyword s_indexes(Keyword::INDEXES);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserKeyword s_on(Keyword::ON);

    ParserIdentifier index_name_p;
    ParserCreateIndexDeclaration parser_create_idx_decl;

    auto query = make_intrusive<ASTHypotheticalIndexQuery>();

    if (s_create.ignore(pos, expected))
    {
        /// CREATE HYPOTHETICAL INDEX ...
        if (!s_hypothetical.ignore(pos, expected))
            return false;
        if (!s_index.ignore(pos, expected))
            return false;

        query->kind = ASTHypotheticalIndexQuery::Create;

        if (s_if_not_exists.ignore(pos, expected))
            query->if_not_exists = true;

        ASTPtr index_name;
        if (!index_name_p.parse(pos, index_name, expected))
            return false;

        if (!s_on.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
            return false;

        ASTPtr index_decl;
        if (!parser_create_idx_decl.parse(pos, index_decl, expected))
            return false;

        auto & ast_index_decl = index_decl->as<ASTIndexDeclaration &>();
        ast_index_decl.name = index_name->as<ASTIdentifier &>().name();

        query->index_name = index_name;
        query->children.push_back(index_name);

        query->index_decl = index_decl;
        query->children.push_back(index_decl);
    }
    else if (s_drop.ignore(pos, expected))
    {
        /// DROP ALL HYPOTHETICAL INDEXES
        if (s_all.ignore(pos, expected))
        {
            if (!s_hypothetical.ignore(pos, expected))
                return false;
            if (!s_indexes.ignore(pos, expected))
                return false;

            query->kind = ASTHypotheticalIndexQuery::DropAll;
            node = query;
            return true;
        }

        /// DROP HYPOTHETICAL INDEX ...
        if (!s_hypothetical.ignore(pos, expected))
            return false;
        if (!s_index.ignore(pos, expected))
            return false;

        query->kind = ASTHypotheticalIndexQuery::Drop;

        if (s_if_exists.ignore(pos, expected))
            query->if_exists = true;

        ASTPtr index_name;
        if (!index_name_p.parse(pos, index_name, expected))
            return false;

        if (!s_on.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
            return false;

        query->index_name = index_name;
        query->children.push_back(index_name);
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
