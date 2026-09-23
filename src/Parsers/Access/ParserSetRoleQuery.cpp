#include <Parsers/Access/ParserSetRoleQuery.h>
#include <Parsers/Access/ASTSetRoleQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


namespace DB
{
namespace
{
    bool parseRoles(IParserBase::Pos & pos, Expected & expected, boost::intrusive_ptr<ASTRolesOrUsersSet> & roles)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            ParserRolesOrUsersSet roles_p;
            roles_p.allowRoles().allowAll();
            if (!roles_p.parse(pos, ast, expected))
                return false;

            roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(ast);
            roles->allow_users = false;
            return true;
        });
    }

    bool parseToUsers(IParserBase::Pos & pos, Expected & expected, boost::intrusive_ptr<ASTRolesOrUsersSet> & to_users)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            if (!ParserKeyword{Keyword::TO}.ignore(pos, expected))
                return false;

            ASTPtr ast;
            ParserRolesOrUsersSet users_p;
            users_p.allowUsers().allowCurrentUser();
            if (!users_p.parse(pos, ast, expected))
                return false;

            to_users = boost::static_pointer_cast<ASTRolesOrUsersSet>(ast);
            to_users->allow_roles = false;
            return true;
        });
    }
}


bool ParserSetRoleQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    using Kind = ASTSetRoleQuery::Kind;
    Kind kind = {};
    if (ParserKeyword{Keyword::SET_ROLE_DEFAULT}.ignore(pos, expected))
        kind = Kind::SET_ROLE_DEFAULT;
    else if (ParserKeyword{Keyword::SET_ROLE}.ignore(pos, expected))
        kind = Kind::SET_ROLE;
    else if (ParserKeyword{Keyword::SET_DEFAULT_ROLE}.ignore(pos, expected))
        kind = Kind::SET_DEFAULT_ROLE;
    else
        return false;

    boost::intrusive_ptr<ASTRolesOrUsersSet> roles;
    boost::intrusive_ptr<ASTRolesOrUsersSet> to_users;

    if ((kind == Kind::SET_ROLE) || (kind == Kind::SET_DEFAULT_ROLE))
    {
        if (!parseRoles(pos, expected, roles))
            return false;

        if (kind == Kind::SET_DEFAULT_ROLE)
        {
            if (!parseToUsers(pos, expected, to_users))
                return false;
        }
    }

    auto query = make_intrusive<ASTSetRoleQuery>();
    node = query;

    query->kind = kind;
    query->roles = std::move(roles);
    query->to_users = std::move(to_users);

    return true;
}
}

namespace DB
{

void registerStatementSetRole(StatementFactory & factory)
{
    factory.registerStatement("SET ROLE",
    {
        .description = R"DOCS_MD(
Activates roles for the current user.

```sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE {#set-default-role}

Sets default roles to a user.

Default roles are automatically activated at user login. You can set as default only the previously granted roles. If the role isn't granted to a user, ClickHouse throws an exception.

```sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

## Examples {#examples}

Set multiple default roles to a user:

```sql
SET DEFAULT ROLE role1, role2, ... TO user
```

Set all the granted roles as default to a user:

```sql
SET DEFAULT ROLE ALL TO user
```

Purge default roles from a user:

```sql
SET DEFAULT ROLE NONE TO user
```

Set all the granted roles as default except for specific roles `role1` and `role2`:

```sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```
)DOCS_MD",
        .syntax = R"(
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
)",
        .related = {"CREATE ROLE", "GRANT", "SET", "SHOW"},
    });
}

}
