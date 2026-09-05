#include <Access/IAccessStorage.h>
#include <Parsers/Access/ParserCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTCreateSettingsProfileQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <Parsers/Access/ParserSettingsProfileElement.h>
#include <Parsers/Access/ParserRolesOrUsersSet.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
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

            return parseIdentifierOrStringLiteral(pos, expected, new_name);
        });
    }

    bool parseSettings(IParserBase::Pos & pos, Expected & expected, bool id_mode, boost::intrusive_ptr<ASTSettingsProfileElements> & settings)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            ParserSettingsProfileElements elements_p;
            elements_p.useInheritKeyword(true).useIDMode(id_mode);
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
            elements_p.useInheritKeyword(true);
            if (!elements_p.parse(pos, ast, expected))
                return false;

            alter_settings = boost::static_pointer_cast<ASTAlterSettingsProfileElements>(ast);
            return true;
        });
    }

    bool parseToRoles(IParserBase::Pos & pos, Expected & expected, bool id_mode, boost::intrusive_ptr<ASTRolesOrUsersSet> & roles)
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

            roles = boost::static_pointer_cast<ASTRolesOrUsersSet>(ast);
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


bool ParserCreateSettingsProfileQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{Keyword::ATTACH_SETTINGS_PROFILE}.ignore(pos, expected) && !ParserKeyword{Keyword::ATTACH_PROFILE}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{Keyword::ALTER_SETTINGS_PROFILE}.ignore(pos, expected) || ParserKeyword{Keyword::ALTER_PROFILE}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{Keyword::CREATE_SETTINGS_PROFILE}.ignore(pos, expected) && !ParserKeyword{Keyword::CREATE_PROFILE}.ignore(pos, expected))
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
    if (!parseIdentifiersOrStringLiterals(pos, expected, names))
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

    boost::intrusive_ptr<ASTRolesOrUsersSet> to_roles;
    parseToRoles(pos, expected, attach_mode, to_roles);

    if (cluster.empty())
        parseOnCluster(pos, expected, cluster);

    auto query = make_intrusive<ASTCreateSettingsProfileQuery>();
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
    query->to_roles = std::move(to_roles);
    query->storage_name = std::move(storage_name);

    return true;
}
}

namespace DB
{

void registerStatementSettingsProfile(StatementFactory & factory)
{
    factory.registerStatement("CREATE SETTINGS PROFILE",
    {
        .description = R"DOCS_MD(
Creates [settings profiles](/concepts/features/security/access-rights#settings-profiles-management) that can be assigned to a user or a role.

Syntax:

```sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]]
    [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

`ON CLUSTER` clause allows creating settings profiles on a cluster, see [Distributed DDL](/reference/statements/distributed-ddl).

`CREATE SETTINGS PROFILE` requires the [CREATE SETTINGS PROFILE](/reference/statements/grant#access-management) privilege. `OR REPLACE` throws away an existing profile of the same name, including which roles it applies to, so it additionally requires the [DROP SETTINGS PROFILE](/reference/statements/grant#access-management) privilege. The `DROP SETTINGS PROFILE` privilege is required whether or not the profile already exists, so the statement cannot be used to find out which profiles exist.

## Example {#example}

Create a user:
```sql
CREATE USER robin IDENTIFIED BY 'password';
```

Create the `max_memory_usage_profile` settings profile with value and constraints for the `max_memory_usage` setting and assign it to user `robin`:

```sql
CREATE
SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000
TO robin
```
)DOCS_MD",
        .syntax = R"(
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]]
    [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
)",
        .parent = "CREATE",
        .related = {"ALTER SETTINGS PROFILE", "CREATE USER", "CREATE ROLE", "SET", "DROP", "SHOW"},
    });

    factory.registerStatement("ALTER SETTINGS PROFILE",
    {
        .description = R"DOCS_MD(
Changes settings profiles.

Syntax:

```sql
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP ALL SETTINGS]
    [DROP ALL PROFILES]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

`ON CLUSTER` clause allows altering settings profiles on a cluster, see [Distributed DDL](/reference/statements/distributed-ddl).

## Replacing vs. modifying settings {#replacing-vs-modifying}

`ALTER SETTINGS PROFILE` supports two different ways of changing the settings and the parent (inherited) profiles of a profile. They behave very differently, so it is important to pick the right one.

### Replacing form: bare `SETTINGS` / `INHERIT` {#replacing-form}

A bare `SETTINGS` clause (without `ADD`, `MODIFY` or `DROP`) **replaces the entire settings list and all parent profiles** of the profile with exactly what you list. Anything previously present but not listed is silently dropped — there is no warning.

```sql
CREATE SETTINGS PROFILE OR REPLACE p
    SETTINGS max_execution_time = 10, enable_lazy_columns_replication = 1;

ALTER SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360;

SHOW CREATE SETTINGS PROFILE p;
-- → CREATE SETTINGS PROFILE p SETTINGS max_memory_usage = 16106127360
-- max_execution_time and enable_lazy_columns_replication are gone.
```

<Warning>
Because the bare `SETTINGS` form is a full replace, using it to "override one setting" on top of a populated base profile will drop every other setting (and every parent profile) on that profile. If you only want to change a single setting while keeping the rest, use the incremental `MODIFY`/`ADD`/`DROP` form described below.
</Warning>

This is the same behavior as `SETTINGS` in [`CREATE SETTINGS PROFILE`](/reference/statements/create/settings-profile): the clause defines the complete settings list.

### Incremental form: `ADD` / `MODIFY` / `DROP` {#incremental-form}

The `ADD`, `MODIFY` and `DROP` keywords change individual entries while leaving everything else on the profile untouched:

- `ADD SETTINGS variable = value [constraints]` — adds a setting that is not yet present.
- `MODIFY SETTINGS variable = value [constraints]` — replaces a single setting's entry. The whole entry (value and constraints) is overwritten, so re-specify `MIN`/`MAX`/`READONLY`/etc. if you want to keep them.
- `DROP SETTINGS variable [,...]` — removes the listed settings.
- `ADD PROFILES 'profile_name' [,...]` / `DROP PROFILES 'profile_name' [,...]` — add or remove parent (inherited) profiles.
- `DROP ALL SETTINGS` / `DROP ALL PROFILES` — remove all settings or all parent profiles.

Several of these clauses can be combined in a single statement, for example `DROP SETTINGS a ADD SETTINGS b = 1`.

`SET variable = value` is an alias for `MODIFY SETTINGS variable = value`. It is offered because `SET` feels natural and because typing the replacing `SETTINGS` clause when an incremental change was intended is a common mistake.

## Examples {#examples}

Override a single setting while preserving the rest of a populated profile:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 16106127360;
```

Add a new constrained setting and drop another one:

```sql
ALTER SETTINGS PROFILE my_profile
    DROP SETTINGS readonly
    ADD SETTINGS max_threads = 8 MIN 4 MAX 16 WRITABLE;
```

Manage parent profiles incrementally:

```sql
ALTER SETTINGS PROFILE my_profile ADD PROFILES p1;
ALTER SETTINGS PROFILE my_profile DROP PROFILES p1;
```

Always verify the result with [`SHOW CREATE SETTINGS PROFILE`](/reference/statements/show):

```sql
SHOW CREATE SETTINGS PROFILE my_profile;
```
## Incremental vs full replacement {#incremental-vs-full-replacement}

<Warning>
A bare `SETTINGS` clause **removes all existing settings and all inherited (parent) profiles** from the profile before applying the new ones.
</Warning>

To change a single setting while keeping the rest, use `ADD SETTINGS` or `MODIFY SETTINGS` (see examples below).

## ADD vs MODIFY {#add-vs-modify}

Both `ADD SETTINGS` and `MODIFY SETTINGS` preserve the other settings in the profile, but they treat an existing entry for the *same* setting differently:

- `ADD SETTINGS variable = value ...` first drops any existing entry for `variable` and then inserts the new one. It therefore **replaces the value together with all constraints** of that setting. Any previously defined `MIN`, `MAX`, or writability (`READONLY`/`WRITABLE`/`CONST`/`CHANGEABLE_IN_READONLY`) for `variable` that you do not repeat is discarded.
- `MODIFY SETTINGS variable = value ...` **merges field by field**: it overrides only the fields you actually specify (the value, or `MIN`, or `MAX`, or the writability) and keeps the other fields of that setting as they were.

<Tip>
In short, use `MODIFY SETTINGS` when you only want to tweak one aspect of a setting (e.g. just the value, while keeping an existing `MAX`); use `ADD SETTINGS` when you want to redefine a setting from scratch.
</Tip>

## Examples {#profile-examples}

Create a profile to use in the examples below:

```sql
CREATE SETTINGS PROFILE OR REPLACE p SETTINGS max_execution_time = 60;
```

### MODIFY SETTINGS {#example-modify-settings}

Add or change a single setting while keeping the others:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000;
SHOW CREATE SETTINGS PROFILE p;
-- CREATE SETTINGS PROFILE p SETTINGS
--     max_execution_time = 60,
--     max_memory_usage = 20000000000
```

Because `MODIFY` merges field by field, changing only the value of a setting keeps its existing constraints:

```sql
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 20000000000 MAX 30000000000;
ALTER SETTINGS PROFILE p MODIFY SETTINGS max_memory_usage = 25000000000;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_memory_usage = 25000000000 MAX 30000000000  -- the MAX constraint is preserved
```

### ADD SETTINGS {#example-add-settings}

Add a setting (also keeping the others), redefining it completely if it already exists:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 8 MAX 16 READONLY;
```

Unlike `MODIFY`, re-running `ADD` with only a value drops the previously defined constraints for that setting:

```sql
ALTER SETTINGS PROFILE p ADD SETTINGS max_threads = 4;
SHOW CREATE SETTINGS PROFILE p;
-- ... max_threads = 4   -- the MAX and READONLY constraints are gone
```

### DROP SETTINGS {#example-drop-settings}

Remove one or more named settings:

```sql
ALTER SETTINGS PROFILE p DROP SETTINGS max_threads;
```

Remove all settings at once:

```sql
ALTER SETTINGS PROFILE p DROP ALL SETTINGS;
```

### Working with inherited profiles {#example-profiles}

Add or remove parent (inherited) profiles without affecting the profile's own settings:

```sql
ALTER SETTINGS PROFILE p ADD PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP PROFILES base_profile;
ALTER SETTINGS PROFILE p DROP ALL PROFILES;
```
)DOCS_MD",
        .syntax = R"(
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]]
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...]]
    [DROP SETTINGS variable [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
)",
        .parent = "ALTER",
        .related = {"CREATE SETTINGS PROFILE", "ALTER", "SET", "SHOW"},
    });
}

}
