#include <Parsers/Access/ParserCheckGrantQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Parsers/Access/ASTCheckGrantQuery.h>
#include <Parsers/Access/parseAccessRightsElements.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


namespace DB
{

bool ParserCheckGrantQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::CHECK_GRANT}.ignore(pos, expected))
        return false;

    AccessRightsElements elements;
    if (!parseAccessRightsElementsWithoutOptions(pos, expected, elements))
        return false;

    elements.throwIfNotGrantable();

    auto query = make_intrusive<ASTCheckGrantQuery>();
    node = query;

    query->access_rights_elements = std::move(elements);

    return true;
}
}

namespace DB
{

void registerStatementCheckGrant(StatementFactory & factory)
{
    factory.registerStatement("CHECK GRANT",
    {
        .description = R"DOCS_MD(
The `CHECK GRANT` query is used to check whether the current user/role has been granted a specific privilege.

## Syntax {#syntax}

The basic syntax of the query is as follows:

```sql
CHECK GRANT privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*}
```

- `privilege` — Type of privilege.

## Examples {#examples}

If the user used to be granted the privilege, the response`check_grant` will be `1`. Otherwise, the response `check_grant` will be `0`.

If `table_1.col1` exists and current user is granted by privilege `SELECT`/`SELECT(con)` or role(with privilege), the response is `1`.
```sql
CHECK GRANT SELECT(col1) ON table_1;
```

```text
┌─result─┐
│      1 │
└────────┘
```
If `table_2.col2` doesn't exists, or current user is not granted by privilege `SELECT`/`SELECT(con)` or role(with privilege), the response is `0`.
```sql
CHECK GRANT SELECT(col2) ON table_2;
```

```text
┌─result─┐
│      0 │
└────────┘
```

## Wildcard {#wildcard}
Specifying privileges you can use asterisk (`*`) instead of a table or a database name. Please check [WILDCARD GRANTS](/reference/statements/grant#wildcard-grants) for wildcard rules.
)DOCS_MD",
        .syntax = R"(
CHECK GRANT privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*}
)",
        .related = {"GRANT", "REVOKE", "SHOW"},
    });
}

}
