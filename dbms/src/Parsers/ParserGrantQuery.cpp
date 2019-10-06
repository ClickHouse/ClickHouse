#include <Parsers/ParserGrantQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseUserName.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_GRANT;
}


bool ParserGrantQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// Parse "GRANT" or "REVOKE".
    ParserKeyword grant_p("GRANT");
    ParserKeyword revoke_p("REVOKE");

    using Kind = ASTGrantQuery::Kind;
    Kind kind;
    if (grant_p.ignore(pos, expected))
        kind = Kind::GRANT;
    else if (revoke_p.ignore(pos, expected))
        kind = Kind::REVOKE;
    else
        return false;

    /// Parse "GRANT OPTION FOR" or "ADMIN OPTION FOR".
    bool grant_option = false;
    std::optional<bool> expect_access_types;
    if (kind == Kind::REVOKE)
    {
        if (ParserKeyword{"GRANT OPTION FOR"}.ignore(pos, expected))
        {
            grant_option = true;
            expect_access_types = true;
        }
        else if (ParserKeyword{"ADMIN OPTION FOR"}.ignore(pos, expected))
        {
            grant_option = true;
            expect_access_types = false;
        }
    }

    /// Parse access types, maybe with column list.
    bool access_types_used = false;
    using AccessType = AccessPrivileges::Type;
    AccessType access = 0;
    std::unordered_map<String, AccessType> columns_access;
    ParserToken comma{TokenType::Comma};
    if (!expect_access_types || *expect_access_types)
    {
        do
        {
            for (const auto & [access_type, access_name] : AccessPrivileges::getAllTypeNames())
            {
                ParserKeyword access_p{access_name.c_str()};
                if (access_p.ignore(pos, expected))
                {
                    access_types_used = true;

                    if (access_type == AccessPrivileges::ALL)
                        ParserKeyword{"PRIVILEGES"}.ignore(pos, expected);

                    ParserToken open(TokenType::OpeningRoundBracket);
                    ParserToken close(TokenType::ClosingRoundBracket);
                    if (open.ignore(pos, expected))
                    {
                        do
                        {
                            ParserIdentifier column_name_p;
                            ASTPtr column_name;
                            if (!column_name_p.parse(pos, column_name, expected))
                                return false;
                            columns_access[getIdentifierName(column_name)] |= access_type;
                        }
                        while (comma.ignore(pos, expected));

                        if (!close.ignore(pos, expected))
                            return false;
                    }
                    else
                        access |= access_type;
                }
            }
        }
        while (access_types_used && comma.ignore(pos, expected));
        if (expect_access_types && *expect_access_types && !access_types_used)
            return false;
    }

    ASTPtr database;
    bool use_current_database = false;
    ASTPtr table;
    std::vector<String> roles;

    /// Parse "ON database.table".
    if (access_types_used)
    {
        /// Grant access to roles.
        if (!ParserKeyword{"ON"}.ignore(pos, expected))
            return false;

        ParserIdentifier database_p;
        ParserIdentifier table_p;
        ParserToken dot{TokenType::Dot};
        ParserToken asterisk{TokenType::Asterisk};
        if (!asterisk.ignore(pos, expected) && !database_p.parse(pos, database, expected))
            return false;
        if (dot.ignore(pos, expected))
        {
            if (!asterisk.ignore(pos, expected) && (!database || !table_p.parse(pos, table, expected)))
                return false;
        }
        else
        {
            table = database;
            database = nullptr;
            use_current_database = true;
        }
    }

    /// Check access types.
    if (access_types_used)
    {
        if (!columns_access.empty())
        {
            if (!table)
                throw Exception("Cannot specify privileges on columns without specifying a table", ErrorCodes::INVALID_GRANT);
            for (auto & columns_access_item : columns_access)
            {
                auto & column_access = columns_access_item.second;
                if (column_access == AccessPrivileges::ALL)
                    column_access = AccessPrivileges::ALL_COLUMN_LEVEL;
                else if (column_access & ~AccessPrivileges::ALL_COLUMN_LEVEL)
                    throw Exception(
                        "Privilege " + AccessPrivileges::typeToString(column_access & ~AccessPrivileges::ALL_COLUMN_LEVEL)
                            + " cannot be granted on a column",
                        ErrorCodes::INVALID_GRANT);
            }
        }
        if (table)
        {
            if (access == AccessPrivileges::ALL)
                access = AccessPrivileges::ALL_TABLE_LEVEL;
            else if (access & ~AccessPrivileges::ALL_TABLE_LEVEL)
                throw Exception(
                    "Privileges " + AccessPrivileges::accessToString(access & ~AccessPrivileges::ALL_TABLE_LEVEL)
                        + " cannot be granted on a table",
                    ErrorCodes::INVALID_GRANT);
        }
        else if (database || use_current_database)
        {
            if (access == AccessPrivileges::ALL)
                access = AccessPrivileges::ALL_DATABASE_LEVEL;
            else if (access & ~AccessPrivileges::ALL_DATABASE_LEVEL)
                throw Exception(
                    "Privileges " + AccessPrivileges::accessToString(access & ~AccessPrivileges::ALL_TABLE_LEVEL)
                        + " cannot be granted on a database",
                    ErrorCodes::INVALID_GRANT);
        }
    }

    /// Parse list of roles which granted to other roles.
    if (!access_types_used)
    {
        do
        {
            String role_name;
            if (!parseRoleName(pos, expected, role_name))
                return false;
            roles.emplace_back(std::move(role_name));
        }
        while (comma.ignore(pos, expected));
    }

    /// Parse "TO" or "FROM" and list of roles which get/lost new privileges.
    if (kind == Kind::GRANT)
    {
        ParserKeyword to_p{"TO"};
        if (!to_p.ignore(pos, expected))
            return false;
    }
    else
    {
        ParserKeyword from_p{"FROM"};
        if (!from_p.ignore(pos, expected))
            return false;
    }

    std::vector<String> to_roles;
    do
    {
        String role_name;
        if (!parseRoleName(pos, expected, role_name))
            return false;
        to_roles.emplace_back(std::move(role_name));
    }
    while (comma.ignore(pos, expected));

    /// Parse "WITH GRANT OPTION" or "WITH ADMIN OPTION".
    if (kind == Kind::GRANT)
    {
        if (access_types_used)
        {
            ParserKeyword with_grant_option_p{"WITH GRANT OPTION"};
            if (with_grant_option_p.ignore(pos, expected))
                grant_option = true;
        }
        else
        {
            ParserKeyword with_admin_option_p{"WITH ADMIN OPTION"};
            if (with_admin_option_p.ignore(pos, expected))
                grant_option = true;
        }
    }

    auto query = std::make_shared<ASTGrantQuery>();
    node = query;
    query->kind = kind;
    query->roles = std::move(roles);
    query->database = database ? getIdentifierName(database) : "";
    query->use_current_database = use_current_database;
    query->table = table ? getIdentifierName(table) : "";
    query->access = access;
    query->columns_access = std::move(columns_access);
    query->to_roles = std::move(to_roles);
    query->grant_option = grant_option;
    return true;
}

}
