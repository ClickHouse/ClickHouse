#include <Parsers/Access/ParserCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ASTCreateMaskingPolicyQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/CommonParsers.h>
#include <Access/IAccessStorage.h>


namespace DB
{
namespace
{
    bool parseRenameTo(IParserBase::Pos & pos, Expected & expected, String & new_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::RENAME_TO}.ignore(pos, expected))
                return false;

            return parseIdentifierOrStringLiteral(pos, expected, new_name);
        });
    }

    bool parseUpdateAssignments(IParserBase::Pos & pos, Expected & expected, ASTPtr & assignments)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::UPDATE}.ignore(pos, expected))
                return false;

            ParserList parser_assignment_list(
                std::make_unique<ParserAssignment>(),
                std::make_unique<ParserToken>(TokenType::Comma),
                /* allow_empty = */ false);

            if (!parser_assignment_list.parse(pos, assignments, expected))
                return false;

            return true;
        });
    }

    bool parseWhereCondition(IParserBase::Pos & pos, Expected & expected, ASTPtr & condition)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::WHERE}.ignore(pos, expected))
                return false;

            ParserExpression parser;
            if (!parser.parse(pos, condition, expected))
                return false;

            return true;
        });
    }

    bool parsePriority(IParserBase::Pos & pos, Expected & expected, Int64 & priority)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::PRIORITY}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            ParserNumber parser;
            if (!parser.parse(pos, ast, expected))
                return false;

            const auto * literal = ast->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::UInt64)
                return false;

            priority = literal->value.safeGet<UInt64>();
            return true;
        });
    }

    bool parseToRoles(IParserBase::Pos & pos, Expected & expected, bool id_mode, std::shared_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (!ParserKeyword{Keyword::TO}.ignore(pos, expected))
                return false;

            ParserRolesOrUsersSet roles_p;
            roles_p.allowAll().allowRoles().allowUsers().allowCurrentUser().useIDMode(id_mode);
            if (!roles_p.parse(pos, ast, expected))
                return false;

            roles = std::static_pointer_cast<ASTRolesOrUsersSet>(ast);
            return true;
        });
    }

    bool parseOnCluster(IParserBase::Pos & pos, Expected & expected, String & cluster)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::ON}.ignore(pos, expected) && ASTQueryWithOnCluster::parse(pos, cluster, expected);
        });
    }
}


bool ParserCreateMaskingPolicy::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;

    if (ParserKeyword{Keyword::ATTACH_MASKING_POLICY}.ignore(pos, expected))
        attach_mode = true;
    else if (ParserKeyword{Keyword::ALTER_MASKING_POLICY}.ignore(pos, expected))
        alter = true;
    else if (!ParserKeyword{Keyword::CREATE_MASKING_POLICY}.ignore(pos, expected))
        return false;

    bool if_exists = false;
    bool if_not_exists = false;
    bool or_replace = false;
    if (alter)
    {
        if (ParserKeyword{Keyword::IF_EXISTS}.ignore(pos, expected))
            if_exists = true;
    }
    else
    {
        if (ParserKeyword{Keyword::IF_NOT_EXISTS}.ignore(pos, expected))
            if_not_exists = true;
        else if (ParserKeyword{Keyword::OR_REPLACE}.ignore(pos, expected))
            or_replace = true;
    }

    String name;
    if (!parseIdentifierOrStringLiteral(pos, expected, name))
        return false;

    if (!ParserKeyword{Keyword::ON}.ignore(pos, expected))
        return false;

    String database;
    String table_name;
    if (!parseDatabaseAndTableName(pos, expected, database, table_name))
        return false;

    String cluster;

    String new_name;
    ASTPtr update_assignments;
    ASTPtr where_condition;
    std::shared_ptr<ASTRolesOrUsersSet> roles;
    Int64 priority = 0;
    bool has_priority = false;
    String storage_name;

    while (true)
    {
        if (alter && new_name.empty() && parseRenameTo(pos, expected, new_name))
            continue;

        if (!update_assignments && parseUpdateAssignments(pos, expected, update_assignments))
            continue;

        if (!where_condition && parseWhereCondition(pos, expected, where_condition))
            continue;

        if (!roles && parseToRoles(pos, expected, attach_mode, roles))
            continue;

        if (!has_priority)
        {
            Int64 new_priority = 0;
            if (parsePriority(pos, expected, new_priority))
            {
                priority = new_priority;
                has_priority = true;
                continue;
            }
        }

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        if (storage_name.empty() && ParserKeyword{Keyword::IN}.ignore(pos, expected) && parseAccessStorageName(pos, expected, storage_name))
            continue;

        break;
    }

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    if (!roles && !alter)
    {
        roles = std::make_shared<ASTRolesOrUsersSet>();
        roles->all = true;
    }

    if (!update_assignments && !alter)
        return false;

    auto query = std::make_shared<ASTCreateMaskingPolicyQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach_mode;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->name = std::move(name);
    query->database = std::move(database);
    query->table_name = std::move(table_name);
    query->new_name = std::move(new_name);
    query->update_assignments = std::move(update_assignments);
    query->where_condition = std::move(where_condition);
    query->roles = std::move(roles);
    query->priority = priority;
    query->storage_name = std::move(storage_name);

    return true;
}
}
