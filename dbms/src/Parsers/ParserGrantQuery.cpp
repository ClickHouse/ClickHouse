#include <Parsers/ParserGrantQuery.h>
#include <Parsers/ASTGrantQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

bool ParserGrantQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
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

    bool grant_option;
    std::optional<bool> should_be_access_specifiers;
    if (kind == Kind::REVOKE)
    {
        if (ParserKeyword{"GRANT OPTION FOR"}.ignore(pos, expected))
        {
            grant_option = true;
            should_be_access_specifiers = true;
        }
        else if (ParserKeyword{"ADMIN OPTION FOR"}.ignore(pos, expected))
        {
            grant_option = true;
            should_be_access_specifiers = false;
        }
    }

    ParserToken comma{TokenType::Comma};

    using AccessType = ASTGrantQuery::AccessType;
    AccessType access = 0;
    std::unordered_map<String, AccessType> columns_access;
    bool access_specifiers_found = false;
    if (!should_be_access_specifiers || *should_be_access_specifiers)
    {
        do
        {
            for (const auto & [access_type, access_name] : ASTGrantQuery::getAccessTypeNames())
            {
                ParserKeyword access_p{access_name.c_str()};
                if (access_p.ignore(pos, expected))
                {
                    access_specifiers_found = true;

                    if (access_type == ASTGrantQuery::ALL)
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
        while (access_specifiers_found && comma.ignore(pos, expected));
        if (should_be_access_specifiers && *should_be_access_specifiers && !access_specifiers_found)
            return false;
    }

    ASTPtr database;
    bool use_current_database = false;
    ASTPtr table;
    std::vector<String> roles;

    if (access_specifiers_found)
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
    else
    {
        /// Grant roles to roles.
        do
        {
            ParserIdentifier role_name_p;
            ASTPtr role_name;
            if (!role_name_p.parse(pos, role_name, expected))
                return false;
            roles.emplace_back(getIdentifierName(role_name));
        }
        while (comma.ignore(pos, expected));
    }

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
        ParserIdentifier role_name_p;
        ASTPtr role_name;
        if (!role_name_p.parse(pos, role_name, expected))
            return false;
        to_roles.emplace_back(getIdentifierName(role_name));
    }
    while (comma.ignore(pos, expected));

    if (kind == Kind::GRANT)
    {
        if (access_specifiers_found)
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
