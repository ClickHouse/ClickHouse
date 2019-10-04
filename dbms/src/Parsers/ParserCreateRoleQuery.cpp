#include <Parsers/ParserCreateRoleQuery.h>
#include <Parsers/ASTCreateRoleQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseUserName.h>


namespace DB
{
bool ParserCreateRoleQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword create_role_p("CREATE ROLE");
    if (!create_role_p.ignore(pos, expected))
        return false;

    bool if_not_exists = false;
    ParserKeyword if_not_exists_p("IF NOT EXISTS");
    if (if_not_exists_p.ignore(pos, expected))
        if_not_exists = true;

    Strings role_names;
    ParserToken comma{TokenType::Comma};
    do
    {
        String role_name;
        if (!parseRoleName(pos, expected, role_name))
            return false;
        role_names.emplace_back(std::move(role_name));
    }
    while (comma.ignore(pos, expected));

    auto query = std::make_shared<ASTCreateRoleQuery>();
    node = query;

    query->role_names = std::move(role_names);
    query->if_not_exists = if_not_exists;

    return true;
}
}
