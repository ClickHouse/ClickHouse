#include <Parsers/Access/ParserExecuteAsQuery.h>

#include <Parsers/Access/ASTExecuteAsQuery.h>
#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/Access/ParserUserNameWithHost.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/StatementFactory.h>
#include <Parsers/registerStatements.h>


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

}

namespace DB
{

void registerStatementExecuteAs(StatementFactory & factory)
{
    factory.registerStatement("EXECUTE AS",
    {
        .description = R"DOCS_MD(
import { CloudNotSupportedBadge } from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

<CloudNotSupportedBadge />

Allows to execute queries on behalf of a different user.

## Syntax {#syntax}

```sql
EXECUTE AS target_user;
EXECUTE AS target_user subquery;
```

The first form (without `subquery`) sets that all the following queries in the current session will be executed on behalf of the specified `target_user`.

The second form (with `subquery`) executes only the specified `subquery` on behalf of the specified `target_user`.

In order to work both forms require config setting `access_control_improvements.allow_impersonate_user`
to be set to `1` and the `IMPERSONATE` privilege to be granted. For example, the following commands
```sql
GRANT IMPERSONATE ON user1 TO user2;
GRANT IMPERSONATE ON * TO user3;
```
allow user `user2` to execute commands `EXECUTE AS user1 ...` and also allow user `user3` to execute commands as any user.

While impersonating another user function [currentUser()](/reference/functions/regular-functions/other-functions#currentUser) returns the name of that other user,
and function [authenticatedUser()](/reference/functions/regular-functions/other-functions#authenticatedUser) returns the name of the user who has been actually authenticated.

## Examples {#examples}

```sql
SELECT currentUser(), authenticatedUser(); -- outputs "default    default"
CREATE USER james;
EXECUTE AS james SELECT currentUser(), authenticatedUser(); -- outputs "james    default"
```
)DOCS_MD",
        .syntax = R"(
EXECUTE AS target_user
EXECUTE AS target_user subquery
)",
        .related = {"GRANT", "CREATE USER", "SET ROLE"},
    });
}

}
