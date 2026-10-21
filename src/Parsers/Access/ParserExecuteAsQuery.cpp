#include <Parsers/Access/ParserExecuteAsQuery.h>

#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>


namespace DB
{

ParserExecuteAsQuery::ParserExecuteAsQuery(IParser & subquery_parser_)
    : subquery_parser(subquery_parser_)
{
}

bool ParserExecuteAsQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (!ParserKeyword{Keyword::EXECUTE_AS}.ignore(pos, expected))
        return false;

    ASTPtr target_user;
    if (!ParserUserNameWithHost(/*allow_query_parameter=*/ false).parse(pos, target_user, expected))
        return false;

    auto query = make_intrusive<ASTExecuteAsQuery>();
    node = query;

    query->set(query->target_user, target_user);

    if (ParserKeyword{Keyword::SETTINGS}.ignore(pos, expected))
    {
        auto target_settings = make_intrusive<ASTSetQuery>();
        target_settings->is_standalone = false;
        do
        {
            if (!ParserSetQuery::parseNameLiteralPair(target_settings->changes.emplace_back(), pos, expected))
                return false;
        } while (ParserToken{TokenType::Comma}.ignore(pos, expected));
        query->set(query->target_settings, target_settings);
    }

    /// support 1) EXECUTE AS <user1>  2) EXECUTE AS <user1> SELECT ...

    ASTPtr subquery;
    if (subquery_parser.parse(pos, subquery, expected))
    {
        if (auto * sub_output = dynamic_cast<ASTQueryWithOutput *>(subquery.get()))
        {
            /// Hoist output options (FORMAT, INTO OUTFILE, COMPRESSION, SETTINGS) from the subquery
            /// to the outer EXECUTE AS query. This is needed because the inner `executeQuery` is called
            /// with `QueryFlags{ .internal = true }` and returns raw blocks — the outer `executeQuery`
            /// pipeline reads these options from the top-level AST to apply formatting.
            /// Assign the fields here, but append them to `children` only below, after the subquery
            /// and in the canonical `output_option_members` order, so that a freshly parsed
            /// `EXECUTE AS ... <output options>` and its clone (see `ASTExecuteAsQuery::clone`, which
            /// appends `subquery` and then `cloneOutputOptions`) share the same child order and
            /// therefore the same tree hash.
            if (sub_output->out_file)
            {
                query->out_file = sub_output->out_file;
                query->setIsOutfileAppend(sub_output->isOutfileAppend());
                query->setIsOutfileTruncate(sub_output->isOutfileTruncate());
                query->setIsIntoOutfileWithStdout(sub_output->isIntoOutfileWithStdout());
            }
            if (sub_output->format_ast)
                query->format_ast = sub_output->format_ast;
            if (sub_output->compression)
                query->compression = sub_output->compression;
            if (sub_output->compression_level)
                query->compression_level = sub_output->compression_level;
            if (sub_output->settings_ast)
                query->settings_ast = sub_output->settings_ast;
            ASTQueryWithOutput::resetOutputASTIfExist(*sub_output);
        }

        query->set(query->subquery, subquery);

        /// Append the hoisted output options after the subquery, in the canonical order.
        ASTQueryWithOutput & out = *query;
        for (auto member : ASTQueryWithOutput::output_option_members)
            if (out.*member)
                out.children.push_back(out.*member);
    }

    return true;
}

std::map<String, Documentation> ParserExecuteAsQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["EXECUTE AS"] =
    {
        .description = R"DOCS_MD(
import { CloudNotSupportedBadge } from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

<CloudNotSupportedBadge />

Allows to execute queries on behalf of a different user.

## Syntax {#syntax}

```sql
EXECUTE AS target_user [SETTINGS name = value [, ...]];
EXECUTE AS target_user [SETTINGS name = value [, ...]] subquery;
```

The first form (without `subquery`) sets that all the following queries in the current session will be executed on behalf of the specified `target_user`.

The second form (with `subquery`) executes only the specified `subquery` on behalf of the specified `target_user`.

Settings changed in the current session or query carry over to `target_user` only where its settings constraints allow them.

In order to work both forms require config setting `access_control_improvements.allow_impersonate_user`
to be set to `1` and the `IMPERSONATE` privilege to be granted. For example, the following commands
```sql
GRANT IMPERSONATE ON user1 TO user2;
GRANT IMPERSONATE ON * TO user3;
```
allow user `user2` to execute commands `EXECUTE AS user1 ...` and also allow user `user3` to execute commands as any user.

While impersonating another user function [currentUser()](/reference/functions/regular-functions/other-functions#currentUser) returns the name of that other user,
and function [authenticatedUser()](/reference/functions/regular-functions/other-functions#authenticatedUser) returns the name of the user who has been actually authenticated.

## Setting values for the target user {#settings}

The `SETTINGS` clause gives values to custom settings that a settings profile of `target_user` declares `CONST`
without a value, for example as the key of a row policy. The values are applied after the profiles of `target_user`,
so neither the following queries nor a later `SET`, `SET profile` or `SETTINGS` clause can change them. It requires the
server setting `access_control_improvements.allow_execute_as_settings` (disabled by default). A setting may be given
only once, `target_user` must not be defined in the server configuration files, and the caller must be allowed to set
the same value for itself. A nested `EXECUTE AS` keeps the values: the next user must declare the same settings, and its
`SETTINGS` clause may only repeat them. Each value is a string, number, boolean or `NULL` literal, not a map.

The values also reach the remote nodes of a distributed query over a cluster with `<secret>`, which check them against
the same user there; the query fails if a remote node does not have the feature enabled or is of an older version.
Over a cluster without `<secret>` the remote node does not apply the constraints of the user. Other ways a query is run
again, for example `INSERT` into a `Distributed` table, `ON CLUSTER` queries or background operations, do not carry the
values; at most they pass a value on as an ordinary setting, subject to the constraints of the user they run as. Use
`getSetting` rather than `getSettingOrDefault` in such a row policy, so that a query without the value fails.

```sql
CREATE SETTINGS PROFILE tenant_lock SETTINGS SQL_tenant CONST;
CREATE USER tenant_reader SETTINGS PROFILE tenant_lock;
CREATE ROW POLICY tenant_rows ON events USING tenant = getSetting('SQL_tenant') TO tenant_reader;
GRANT SELECT ON events TO tenant_reader;
GRANT IMPERSONATE ON tenant_reader TO gateway;

-- As `gateway`: reads only the rows of `acme`, and `SET SQL_tenant = 'other'` in the session is refused.
EXECUTE AS tenant_reader SETTINGS SQL_tenant = 'acme';
```

## Examples {#examples}

```sql
SELECT currentUser(), authenticatedUser(); -- outputs "default    default"
CREATE USER james;
EXECUTE AS james SELECT currentUser(), authenticatedUser(); -- outputs "james    default"
```
)DOCS_MD",
        .syntax = R"(
EXECUTE AS target_user [SETTINGS name = value [, ...]]
EXECUTE AS target_user [SETTINGS name = value [, ...]] subquery
)",
        .related = {"GRANT", "CREATE USER", "SET ROLE"},
    };

    return documentation;
}

}
