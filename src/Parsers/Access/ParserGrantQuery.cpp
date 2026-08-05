#include <Parsers/Access/ParserGrantQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/Access/parseAccessRightsElements.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{
    bool parseCurrentGrants(IParser::Pos & pos, Expected & expected, AccessRightsElements & elements)
    {
        if (ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
        {
            if (!parseAccessRightsElementsWithoutOptions(pos, expected, elements))
                return false;

            if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
                return false;
        }
        else
        {
            AccessRightsElement default_element(AccessType::ALL);

            if (!ParserKeyword{Keyword::ON}.ignore(pos, expected))
                return false;

            String database_name;
            String table_name;
            bool wildcard = false;
            bool default_database = false;
            if (!parseDatabaseAndTableNameOrAsterisks(pos, expected, database_name, table_name, wildcard, default_database))
                return false;

            default_element.database = database_name;
            default_element.table = table_name;
            default_element.wildcard = wildcard;
            default_element.default_database = default_database;
            elements.push_back(std::move(default_element));
        }

        return true;
    }


    bool parseRoles(IParser::Pos & pos, Expected & expected, bool is_revoke, bool id_mode, std::shared_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ParserRolesOrUsersSet roles_p;
            roles_p.allowRoles().useIDMode(id_mode);
            if (is_revoke)
                roles_p.allowAll();

            ASTPtr ast;
            if (!roles_p.parse(pos, ast, expected))
                return false;

            roles = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(ast);
            return true;
        });
    }


    bool parseToGrantees(IParser::Pos & pos, Expected & expected, bool is_revoke, std::shared_ptr<ASTRolesOrUsersSet> & grantees)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{is_revoke ? Keyword::FROM : Keyword::TO}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            ParserRolesOrUsersSet roles_p;
            roles_p.allowRoles().allowUsers().allowCurrentUser().allowAll(is_revoke);
            if (!roles_p.parse(pos, ast, expected))
                return false;

            grantees = typeid_cast<std::shared_ptr<ASTRolesOrUsersSet>>(ast);
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


bool ParserGrantQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (attach_mode && !ParserKeyword{Keyword::ATTACH}.ignore(pos, expected))
        return false;

    bool is_replace = false;
    bool is_revoke = false;
    if (ParserKeyword{Keyword::REVOKE}.ignore(pos, expected))
        is_revoke = true;
    else if (!ParserKeyword{Keyword::GRANT}.ignore(pos, expected))
        return false;

    String cluster;
    parseOnCluster(pos, expected, cluster);

    bool grant_option = false;
    bool admin_option = false;
    if (is_revoke)
    {
        if (ParserKeyword{Keyword::GRANT_OPTION_FOR}.ignore(pos, expected))
            grant_option = true;
        else if (ParserKeyword{Keyword::ADMIN_OPTION_FOR}.ignore(pos, expected))
            admin_option = true;
    }

    AccessRightsElements elements;
    std::shared_ptr<ASTRolesOrUsersSet> roles;

    bool current_grants = false;
    if (!is_revoke && ParserKeyword{Keyword::CURRENT_GRANTS}.ignore(pos, expected))
    {
        current_grants = true;
        if (!parseCurrentGrants(pos, expected, elements))
            return false;
    }
    else
    {
        if (!parseAccessRightsElementsWithoutOptions(pos, expected, elements) && !parseRoles(pos, expected, is_revoke, attach_mode, roles))
            return false;
    }

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    std::shared_ptr<ASTRolesOrUsersSet> grantees;
    if (!parseToGrantees(pos, expected, is_revoke, grantees) && !allow_no_grantees)
        return false;

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    if (!is_revoke)
    {
        if (ParserKeyword{Keyword::WITH_GRANT_OPTION}.ignore(pos, expected))
            grant_option = true;
        else if (ParserKeyword{Keyword::WITH_ADMIN_OPTION}.ignore(pos, expected))
            admin_option = true;

        if (ParserKeyword{Keyword::WITH_REPLACE_OPTION}.ignore(pos, expected))
            is_replace = true;
    }

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    if (grant_option && roles)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "GRANT OPTION should be specified for access types");
    if (admin_option && !elements.empty())
        throw Exception(ErrorCodes::SYNTAX_ERROR, "ADMIN OPTION should be specified for roles");

    if (grant_option)
    {
        for (auto & element : elements)
            element.grant_option = true;
    }


    bool replace_access = false;
    bool replace_role = false;
    if (is_replace)
    {
        if (roles)
            replace_role = true;
        else
            replace_access = true;
    }

    if (!is_revoke && !attach_mode)
        elements.throwIfNotGrantable();

    auto query = std::make_shared<ASTGrantQuery>();
    node = query;

    query->is_revoke = is_revoke;
    query->attach_mode = attach_mode;
    query->cluster = std::move(cluster);
    query->access_rights_elements = std::move(elements);
    query->roles = std::move(roles);
    query->grantees = std::move(grantees);
    query->admin_option = admin_option;
    query->replace_access = replace_access;
    query->replace_granted_roles = replace_role;
    query->current_grants = current_grants;

    return true;
}
}
