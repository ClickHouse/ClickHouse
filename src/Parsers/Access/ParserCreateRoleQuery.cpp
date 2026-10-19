#include <Access/IAccessStorage.h>
#include <Parsers/Access/ParserCreateRoleQuery.h>
#include <Parsers/Access/ASTCreateRoleQuery.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ParserSettingsProfileElement.h>
#include <Parsers/Access/parseUserName.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>
#include <base/insertAtEnd.h>


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

            return parseRoleName(pos, expected, new_name);
        });
    }

    bool parseSettings(IParserBase::Pos & pos, Expected & expected, bool id_mode, boost::intrusive_ptr<ASTSettingsProfileElements> & settings)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            ParserSettingsProfileElements elements_p;
            elements_p.useIDMode(id_mode);
            if (!elements_p.parse(pos, ast, expected))
                return false;

            settings = boost::static_pointer_cast<ASTSettingsProfileElements>(ast);
            return true;
        });
    }

    bool parseAlterSettings(IParserBase::Pos & pos, Expected & expected, boost::intrusive_ptr<ASTAlterSettingsProfileElements> & alter_settings)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            ParserAlterSettingsProfileElements elements_p;
            if (!elements_p.parse(pos, ast, expected))
                return false;

            alter_settings = boost::static_pointer_cast<ASTAlterSettingsProfileElements>(ast);
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


bool ParserCreateRoleQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{Keyword::ATTACH_ROLE}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{Keyword::ALTER_ROLE}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{Keyword::CREATE_ROLE}.ignore(pos, expected))
            return false;
    }

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

    Strings names;
    if (!parseRoleNames(pos, expected, names))
        return false;

    String new_name;
    boost::intrusive_ptr<ASTSettingsProfileElements> settings;
    boost::intrusive_ptr<ASTAlterSettingsProfileElements> alter_settings;
    String cluster;
    String storage_name;

    while (true)
    {
        if (alter && new_name.empty() && (names.size() == 1) && parseRenameTo(pos, expected, new_name))
            continue;

        if (alter)
        {
            boost::intrusive_ptr<ASTAlterSettingsProfileElements> new_alter_settings;
            if (parseAlterSettings(pos, expected, new_alter_settings))
            {
                if (!alter_settings)
                    alter_settings = make_intrusive<ASTAlterSettingsProfileElements>();
                alter_settings->add(std::move(*new_alter_settings));
                continue;
            }
        }
        else
        {
            boost::intrusive_ptr<ASTSettingsProfileElements> new_settings;
            if (parseSettings(pos, expected, attach_mode, new_settings))
            {
                if (!settings)
                    settings = make_intrusive<ASTSettingsProfileElements>();
                settings->add(std::move(*new_settings));
                continue;
            }
        }

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        if (storage_name.empty() && ParserKeyword{Keyword::IN}.ignore(pos, expected) && parseAccessStorageName(pos, expected, storage_name))
            continue;

        break;
    }

    auto query = make_intrusive<ASTCreateRoleQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach_mode;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->names = std::move(names);
    query->new_name = std::move(new_name);
    query->settings = std::move(settings);
    query->alter_settings = std::move(alter_settings);
    query->storage_name = std::move(storage_name);

    return true;
}
}

namespace DB
{

void registerStatementRole(StatementFactory & factory)
{
    factory.registerStatement("CREATE ROLE",
    {
        .description = R"DOCS_MD(
Creates new [roles](/concepts/features/security/access-rights#role-management). Role is a set of [privileges](/reference/statements/grant#granting-privilege-syntax). A [user](/reference/statements/create/user) assigned a role gets all the privileges of this role.

Syntax:

```sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
```

`CREATE ROLE` requires the [CREATE ROLE](/reference/statements/grant#access-management) privilege. `OR REPLACE` throws away an existing role of the same name, along with the privileges granted to it, so it additionally requires the [DROP ROLE](/reference/statements/grant#access-management) privilege. The `DROP ROLE` privilege is required on every name listed in the statement whether or not that role already exists, so the statement cannot be used to find out which roles exist.

## Managing Roles {#managing-roles}

A user can be assigned multiple roles. Users can apply their assigned roles in arbitrary combinations by the [SET ROLE](/reference/statements/set-role) statement. The final scope of privileges is a combined set of all the privileges of all the applied roles. If a user has privileges granted directly to it's user account, they are also combined with the privileges granted by roles.

User can have default roles which apply at user login. To set default roles, use the [SET DEFAULT ROLE](/reference/statements/set-role#set-default-role) statement or the [ALTER USER](/reference/statements/alter/user) statement.

To revoke a role, use the [REVOKE](/reference/statements/revoke) statement.

To delete role, use the [DROP ROLE](/reference/statements/drop#drop-role) statement. The deleted role is being automatically revoked from all the users and roles to which it was assigned.

## Examples {#examples}

```sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

This sequence of queries creates the role `accountant` that has the privilege of reading data from the `db` database.

Assigning the role to the user `mira`:

```sql
GRANT accountant TO mira;
```

After the role is assigned, the user can apply it and execute the allowed queries. For example:

```sql
SET ROLE accountant;
SELECT * FROM db.*;
```
)DOCS_MD",
        .syntax = R"(
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
)",
        .parent = "CREATE",
        .related = {"ALTER ROLE", "CREATE USER", "GRANT", "SET ROLE", "DROP"},
    });

    factory.registerStatement("ALTER ROLE",
    {
        .description = R"DOCS_MD(
Changes roles.

Syntax:

```sql
ALTER ROLE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
```

`SET variable = value` is an alias for `MODIFY SETTING variable = value`: it changes a single setting in place while keeping the rest, unlike the bare `SETTINGS` clause which replaces the whole settings list and also removes all inherited (parent) profiles.
)DOCS_MD",
        .syntax = R"(
ALTER ROLE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
)",
        .parent = "ALTER",
        .related = {"CREATE ROLE", "ALTER", "SET ROLE", "GRANT"},
    });
}

}
