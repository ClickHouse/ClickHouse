#include <Access/IAccessStorage.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Access/ASTCreateQueryMaskingRuleQuery.h>
#include <Parsers/Access/ParserCreateQueryMaskingRuleQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>


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

    bool parseStringLiteralAfterKeyword(IParserBase::Pos & pos, Expected & expected, Keyword keyword, std::optional<String> & value)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            ASTPtr ast;
            if (!ParserKeyword{keyword}.ignore(pos, expected) || !ParserStringLiteral{}.parse(pos, ast, expected))
                return false;

            value = ast->as<const ASTLiteral &>().value.safeGet<String>();
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

    bool parseInStorage(IParserBase::Pos & pos, Expected & expected, String & storage_name)
    {
        return IParserBase::wrapParseImpl(pos, [&]
        {
            return ParserKeyword{Keyword::IN}.ignore(pos, expected) && parseAccessStorageName(pos, expected, storage_name);
        });
    }
}


bool ParserCreateQueryMaskingRuleQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    bool alter = false;
    if (attach_mode)
    {
        if (!ParserKeyword{Keyword::ATTACH_QUERY_MASKING_RULE}.ignore(pos, expected))
            return false;
    }
    else
    {
        if (ParserKeyword{Keyword::ALTER_QUERY_MASKING_RULE}.ignore(pos, expected))
            alter = true;
        else if (!ParserKeyword{Keyword::CREATE_QUERY_MASKING_RULE}.ignore(pos, expected))
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

    String name;
    if (!parseIdentifierOrStringLiteral(pos, expected, name))
        return false;

    String new_name;
    std::optional<String> regexp;
    std::optional<String> replace;
    String cluster;
    String storage_name;

    while (true)
    {
        if (alter && new_name.empty() && parseRenameTo(pos, expected, new_name))
            continue;

        if (!regexp && parseStringLiteralAfterKeyword(pos, expected, Keyword::REGEXP, regexp))
            continue;

        if (!replace && parseStringLiteralAfterKeyword(pos, expected, Keyword::REPLACE, replace))
            continue;

        if (cluster.empty() && parseOnCluster(pos, expected, cluster))
            continue;

        if (storage_name.empty() && parseInStorage(pos, expected, storage_name))
            continue;

        break;
    }

    if (!alter && !regexp)
        return false;

    auto query = make_intrusive<ASTCreateQueryMaskingRuleQuery>();
    node = query;

    query->alter = alter;
    query->attach = attach_mode;
    query->if_exists = if_exists;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;
    query->cluster = std::move(cluster);
    query->name = std::move(name);
    query->new_name = std::move(new_name);
    query->storage_name = std::move(storage_name);
    query->regexp = std::move(regexp);
    query->replace = std::move(replace);

    return true;
}

std::map<String, Documentation> ParserCreateQueryMaskingRuleQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["CREATE QUERY MASKING RULE"] =
    {
        .description = R"DOCS_MD(
Creates a query masking rule. Like a rule of the [`query_masking_rules`](/reference/settings/server-settings/settings/query#query_masking_rules) server setting, it replaces every match of an [RE2](https://github.com/google/re2/wiki/Syntax) regular expression before the text is stored in the server logs, in [`system.query_log`](/reference/system-tables/query_log), [`system.text_log`](/reference/system-tables/text_log) and [`system.processes`](/reference/system-tables/processes), in exception messages, and in the logs sent to the client. This allows hiding sensitive data, such as tokens, names or emails, without access to the server configuration.

Syntax:

```sql
CREATE QUERY MASKING RULE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IN access_storage_type]
    REGEXP 'regexp'
    [REPLACE 'replacement']
```

`REPLACE` is the substitution string, `******` by default. It can reference capture groups of `REGEXP` as `\1` ... `\9`; a reference to a group the regular expression does not have is rejected.

A rule applies to the whole server, to the text of every user. The rules from the server configuration are applied first, then the SQL-defined rules in the order of their names. Rules are stored in the access storage like users and roles, so with a replicated access storage they apply on every replica. The `throw_on_match` option is only available in the server configuration, and the rules from the configuration are not listed by [`SHOW QUERY MASKING RULES`](/reference/statements/show#show-query-masking-rules).

The `REGEXP` is treated as a secret: it is shown as `'[HIDDEN]'` in the query logs, in `SHOW CREATE QUERY MASKING RULE` and in [`system.query_masking_rules`](/reference/system-tables/query_masking_rules), unless the display of secrets is enabled with [`display_secrets_in_show_and_select`](/reference/settings/server-settings/settings/other#display_secrets_in_show_and_select).

Each rule is one more regular expression replacement for every logged query and every log message, so keep the number of rules small.

`CREATE QUERY MASKING RULE` requires the [QUERY MASKING RULE ADMIN](/reference/statements/grant#access-management) privilege.

**Examples**

Hide the values of `token=...` in the logs:

```sql
CREATE QUERY MASKING RULE hide_tokens REGEXP 'token=[0-9a-f]+' REPLACE 'token=[REDACTED]';
```
)DOCS_MD",
        .syntax = R"(
CREATE QUERY MASKING RULE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [IN access_storage_type]
    REGEXP 'regexp'
    [REPLACE 'replacement']
)",
        .parent = "CREATE",
        .related = {"ALTER QUERY MASKING RULE", "DROP", "SHOW"},
    };

    documentation["ALTER QUERY MASKING RULE"] =
    {
        .description = R"DOCS_MD(
Changes a query masking rule created by [CREATE QUERY MASKING RULE](/reference/statements/create/query-masking-rule).

Syntax:

```sql
ALTER QUERY MASKING RULE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [REGEXP 'regexp']
    [REPLACE 'replacement']
```

The clauses that are not specified keep their values. Renaming a rule changes its position among the SQL-defined rules, which are applied in the order of their names.

`ALTER QUERY MASKING RULE` requires the [QUERY MASKING RULE ADMIN](/reference/statements/grant#access-management) privilege.

**Examples**

```sql
ALTER QUERY MASKING RULE hide_tokens REPLACE '[REDACTED]';
```
)DOCS_MD",
        .syntax = R"(
ALTER QUERY MASKING RULE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [REGEXP 'regexp']
    [REPLACE 'replacement']
)",
        .parent = "ALTER",
        .related = {"CREATE QUERY MASKING RULE", "ALTER", "SHOW"},
    };

    return documentation;
}

}
