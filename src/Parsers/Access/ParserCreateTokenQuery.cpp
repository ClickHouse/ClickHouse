#include <Parsers/Access/ParserCreateTokenQuery.h>

#include <Parsers/Access/ASTCreateTokenQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


namespace DB
{

bool ParserCreateTokenQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::CREATE_TOKEN}.ignore(pos, expected))
        return false;

    ASTPtr valid_until;
    bool valid_until_is_interval = false;
    AccessRightsElements grants;
    bool parsed_grants = false;

    /// Both clauses are optional and each of them can be specified at most once, in any order.
    while (true)
    {
        if (!valid_until && parseAuthenticationValidUntil(pos, expected, valid_until, valid_until_is_interval))
            continue;

        if (!parsed_grants && parseAuthenticationGrants(pos, expected, grants))
        {
            parsed_grants = true;
            continue;
        }

        break;
    }

    auto query = make_intrusive<ASTCreateTokenQuery>();
    query->setValidUntil(std::move(valid_until));
    query->valid_until_is_interval = valid_until_is_interval;
    query->grants = std::move(grants);

    node = query;
    return true;
}

void registerStatementCreateToken(StatementFactory & factory)
{
    factory.registerStatement("CREATE TOKEN",
    {
        .description = R"DOCS_MD(
Creates a token for the current user: the server generates a random secret, adds it to the current user as
an additional [authentication method](/reference/statements/create/user#identification), and returns it as
the result of the query. The secret is only ever shown by this query - it is stored hashed, so it cannot be
recovered afterwards.

Syntax:

```sql
CREATE TOKEN
    [{VALID UNTIL datetime | VALID FOR interval}]
    [GRANTS (privilege ON object [,...])]
```

This is a shorthand for `ALTER USER <current user> ADD IDENTIFIED WITH sha256_password BY '<random secret>'`
with the same [`VALID UNTIL`](/reference/statements/create/user#valid-until-clause) and
[`GRANTS`](/reference/statements/create/user#grants-clause) clauses, so a token behaves as a regular password
of the current user:

- it is tied to the user - it is displayed in `system.query_log` and `system.processes` as the user, it stops
  working when the user is deleted, and it loses access rights when the user loses them;
- it can be limited in time with `VALID UNTIL` or `VALID FOR`, and in privileges with `GRANTS`;
- it can be used with every authentication mechanism that accepts a password, e.g. the `password` parameter
  of the HTTP interface or the `--password` option of `clickhouse-client`.

Creating a token requires the `CREATE TOKEN` privilege, or the `ALTER USER` privilege on the current user.
It is a separate privilege because a token lowers the security level of the account it belongs to: a user
authenticated with a hardware key or a certificate can use it to create a long-lived password for the same
account. The same privilege authorizes the equivalent
`ALTER USER <current user> ADD IDENTIFIED ...` statement.

## Result {#result}

The query returns one row with two columns:

| Column        | Type            | Description                                                            |
|---------------|-----------------|------------------------------------------------------------------------|
| `token`       | `String`        | The generated secret.                                                   |
| `valid_until` | `DateTime64(0)` | When the token expires. `0` means that it never expires, the same encoding as the `valid_until` column of [`system.users`](/reference/system-tables/users). |

To get the secret without any formatting, select it with `FORMAT TSVRaw`:

```sql
CREATE TOKEN VALID FOR INTERVAL 30 DAY GRANTS (SELECT ON db.*) FORMAT TSVRaw
```

The secret is 32 characters long and is generated from a cryptographically secure random source, so it does
not have to be (and is not) checked against the
[password complexity rules](/reference/statements/create/user#identification) - those exist to
constrain passwords chosen by humans.

## VALID UNTIL and VALID FOR Clauses {#valid-until-clause}

Limit the lifetime of the token. They work exactly as the corresponding clauses of an authentication method
of [`CREATE USER`](/reference/statements/create/user#valid-until-clause): `VALID UNTIL` takes an absolute date
and time, `VALID FOR` takes an [interval](/reference/data-types/special-data-types/interval) which is added to
the current time when the query is executed.

Without either clause the token lives for
[`create_token_default_ttl_seconds`](/reference/settings/session-settings/create), which is
30 minutes by default, so a token that is not asked to live longer is short-lived. Set that setting to `0`, or
write `VALID UNTIL 'infinity'`, to create a token that never expires.

Examples:

- `CREATE TOKEN VALID UNTIL '2026-12-31'`
- `CREATE TOKEN VALID FOR INTERVAL 30 DAY`
- `CREATE TOKEN VALID UNTIL 'infinity'`

## GRANTS Clause {#grants-clause}

Limits the access rights of the sessions authenticated with the token to the intersection with the listed
privileges. It works exactly as the [`GRANTS` clause of `CREATE USER`](/reference/statements/create/user#grants-clause),
including its limitations - notably, the limit is enforced on the node which receives the query and is not
propagated to the other nodes of a cluster. The clause never adds any access rights: a privilege which is not
granted to the user stays unavailable to the token. Without the clause the token has the full access rights
of the user.

Examples:

- `CREATE TOKEN GRANTS (SELECT ON db.table)`
- `CREATE TOKEN VALID FOR INTERVAL 90 DAY GRANTS (SELECT ON db.table, INSERT ON db.table)`

Neither this limit nor the time limit applies to what the sessions of the token leave behind - see
[Security considerations](#security-considerations).

## Managing Tokens {#managing-tokens}

A token is an authentication method of the user, so it is listed by
[`SHOW CREATE USER`](/reference/statements/show#show-create-user) (with its `VALID UNTIL` and `GRANTS` clauses,
but without the secret) and in the `auth_type`, `auth_params` and `auth_grants` columns of the
[`system.users`](/reference/system-tables/users) table.

There is no statement which drops a single token. To revoke all the tokens of a user, replace its
authentication methods, e.g. with `ALTER USER <name> IDENTIFIED WITH ...`, or use
`ALTER USER <name> RESET AUTHENTICATION METHODS TO NEW` to keep only the most recently added one. The
number of authentication methods a user may have at once is limited by the
`max_authentication_methods_per_user` server setting.

The secret is generated and stored by the query itself, so a `CREATE TOKEN` whose result never reaches the
client - a connection lost while the row is being sent, or an `INTO OUTFILE` which the client cannot open -
leaves an authentication method behind which nobody can use and which still counts against that limit. Nobody
holds such a secret: it exists only while the query runs. A query rejected before it runs, including one whose
`FORMAT` clause names a format that cannot be used, adds nothing.

`CREATE TOKEN` does not support the `ON CLUSTER` clause. Use a replicated access storage (or run the query on
every node with the equivalent `ALTER USER ... ADD IDENTIFIED WITH sha256_hash` statement) to make a token work
across a cluster whose access entities are stored locally.

## Security Considerations {#security-considerations}

`VALID UNTIL` and `GRANTS` constrain the sessions which authenticate with the token. They do not constrain
what those sessions leave behind:

- The deadline is checked when a session authenticates with the token. A session which is already open, and a
  query which is already running, are not interrupted when the token expires.
- Everything a session creates outlives the token, and an object which does work on its own keeps doing it: a
  [refreshable materialized view](/reference/statements/create/view#refreshable-materialized-view) keeps
  refreshing, a materialized view attached to a streaming table engine such as
  [`Kafka`](/reference/engines/table-engines/integrations/kafka),
  [`RabbitMQ`](/reference/engines/table-engines/integrations/rabbitmq),
  [`NATS`](/reference/engines/table-engines/integrations/nats) or
  [`S3Queue`](/reference/engines/table-engines/integrations/s3queue) keeps consuming, and a dictionary with a
  [`LIFETIME`](/reference/statements/create/dictionary/lifetime) keeps reloading, long after the token has
  expired. A view does this work with the rights of its
  [`DEFINER`](/reference/statements/create/view#sql_security), which is by default the user which created the
  view - the full rights of the user, not the rights which the `GRANTS` clause of the token left it with.

A token which is allowed to create tables, views or dictionaries can therefore extend both of its limits in
practice: it can leave a persistent job behind which keeps running after the deadline and does, with the
rights of the user, what the token itself was not allowed to do. Grant a token only what the application
needs - usually `SELECT` and `INSERT` on named tables - and keep `CREATE TABLE`, `CREATE VIEW`,
`CREATE DICTIONARY` and the `ACCESS MANAGEMENT` privileges out of its `GRANTS` clause when the limits are
meant to hold.

A session which authenticated with a token that has a `GRANTS` clause cannot issue tokens at all: adding an
authentication method to an existing user is denied for such a session. The `GRANTS` clause of a new
authentication method is intersected at login with the access rights of the user, not with those of the
session which created the method, so it would otherwise be a way to widen the limit.
)DOCS_MD",
        .syntax = R"(
CREATE TOKEN
    [{VALID UNTIL datetime | VALID FOR interval}]
    [GRANTS (privilege ON object [,...])]
)",
        .parent = "CREATE",
        .related = {"CREATE USER", "ALTER USER", "GRANT", "SHOW"},
    });
}

}
