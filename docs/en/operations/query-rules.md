---
description: 'Documentation for Query Rewrite Rules'
sidebar_label: 'Query Rewrite Rules'
sidebar_position: 89
slug: /operations/query-rules
title: 'Query Rewrite Rules'
doc_type: 'reference'
---

# Query Rewrite Rules {#query-rewrite-rules}

:::note
Query Rewrite Rules are an experimental feature. The `query_rules` setting is `EXPERIMENTAL`.
:::

Query Rewrite Rules provide capabilities to create, alter and drop rules which allow users to rewrite or reject specific queries.

These rules reuse query parameter feature for query matching which allow substitutions in queries source and resulting templates.

Matching is structural and **ignores aliases**: a rule whose source template is `SELECT 1 AS a` also matches `SELECT 1 AS b`. This is intentional — the source template matches the shape of a query rather than its output column names. The aliases in the result template still determine the rewritten query's output names.

The rules are applied in the order in which their names are listed in the `query_rules` setting (see [System setting](#system-setting) below). The order in which rules were created does not matter.

A `{name:Type}` placeholder uses the query parameter syntax, but here `Type` is a small matching vocabulary rather than an ordinary ClickHouse data type. The supported placeholder types are:

| Type | Matches |
|------|---------|
| `String` | a string literal |
| `Int` | an integer literal |
| `Expression` | an expression |
| `ExpressionList` | a list of expressions |
| `Subquery` | a subquery |

A placeholder with any other type (for example `{x:UInt64}` or `{d:Date}`) is rejected at `CREATE RULE` / `ALTER RULE` time, because it would be stored but never match any query.

Every `{name:Type}` placeholder referenced by a rule's result template must also appear in its source template, and a placeholder must not be repeated within the source template. A placeholder reused in the result template must declare the same `Type` as in the source template. Such rules are rejected at `CREATE RULE` / `ALTER RULE` time.

A rule template may itself be a `CREATE RULE` / `ALTER RULE` statement, but a `{name:Type}` placeholder inside such a nested rule template is not supported and is rejected at `CREATE RULE` / `ALTER RULE` time.

## Syntax {#syntax}

Query rewrite rules provide three types of queries:

### CREATE RULE {#create-rule}

Rewrite of rule:
```sql
CREATE RULE rule_name AS 
(
    any_query
) 
REWRITE TO 
(
    any_query
);
```
Rejection of rule:
```sql
CREATE RULE rule_name AS 
(
    any_query
) 
REJECT WITH 'Message';
```

### ALTER RULE {#alter-rule}

Rewrite of rule:
```sql
ALTER RULE rule_name AS 
(
    any_query
) 
REWRITE TO 
(
    any_query
);
```
Rejection of rule:
```sql
ALTER RULE rule_name AS 
(
    any_query
) 
REJECT WITH 'Message';
```

### DROP RULE {#drop-rule}

```sql
DROP RULE rule_name;
```

## Types of storages for query rewrite rules {#types-of-storages}

Query rewrite rules can either be stored on local disk or in ZooKeeper/Keeper. By default local storage is used.

To configure query rewrite rules storage you need to specify a type. This can be either local or keeper/zookeeper.

Config example:

```xml
<clickhouse>
  <query_rules_storage>
    <type>local</type>
    <path>/query_rules/</path>
    <update_timeout_ms>1000</update_timeout_ms>
  </query_rules_storage>
</clickhouse>
```

## System tables {#system-tables}

System table `system.query_rules` stores all created/altered query rules.
Format of table: `{name:String, rule:String}` where name is the rule name and rule is the whole query.

It only returns rows to users who hold at least one of the `CREATE RULE`, `ALTER RULE` or `DROP RULE` grants. A user without any of these grants sees no rows, so rule definitions (which may reference table names, filters and secrets) are not exposed to users who cannot manage rules.

The rewrite activity of a query is recorded in `system.query_log`: its `query` column holds the original query (before rewriting), and the `applied_rules` column lists the names of the rules that were applied to it, in the order they were applied (empty when no rule matched). A query rejected by a `REJECT` rule is recorded too, with the rejecting rule in `applied_rules` and the corresponding exception.

## System setting {#system-setting}

The `query_rules` setting lists the names of the query rewrite rules that are active for the query. It is a comma-separated list of rule names (identifiers or string literals), applied in the listed order, for example `query_rules = 'rule_1, rule_2'`. By default the list is empty and no rules are applied. If a listed rule does not exist, the query throws an exception. It is an `EXPERIMENTAL` setting.

`query_rules` is a session/profile-level setting: it is evaluated before a query's own `SETTINGS` clause is interpreted, so `SELECT ... SETTINGS query_rules = ...` does not affect whether rules are applied to that query. Set it at the session or profile level instead.

## Limitations {#limitations}

Rules are matched against the query **after** its query parameters (`{name:Type}` of a prepared statement) have been substituted, so a value supplied through a query parameter is matched as the literal it became. In particular, a `REJECT` rule that blocks a specific literal cannot be bypassed by passing that literal through a query parameter.

Matching is structural and performs no backtracking, and placeholders support only the limited type vocabulary listed above (`String`, `Int`, `Expression`, `ExpressionList`, `Subquery`).

## Access grants {#access-grants}

There are three access grants for query rewrite rules, each of these requires separate permission:
1. `CREATE RULE`
2. `ALTER RULE`
3. `DROP RULE`

## Examples {#examples}

Creation:
```sql
CREATE RULE rule_1 AS 
(
    SELECT date, sum(hits) FROM stats WHERE page = {name:String} GROUP BY date
) 
REWRITE TO 
(
    SELECT date, hits FROM totals WHERE page = {name:String}
);

CREATE RULE rule_2 AS 
(
    SELECT date, sum(hits) FROM stats WHERE page = {name:String} GROUP BY date
) 
REJECT WITH 'REJECT';

-- Activate rule_1 for the session (it is applied before normal query processing).
SET query_rules = 'rule_1';
```

Alteration:
```sql
ALTER RULE rule_1 AS (
    SELECT date, sum(hits) FROM stats WHERE date = {name2:String} AND page = {name:String} GROUP BY date
)
REWRITE TO 
(
    SELECT date, hits FROM totals WHERE page = {name:String} AND date = {name2:String}
);

ALTER RULE rule_1 AS (
    SELECT date, sum(hits) FROM stats WHERE date = {name2:String} AND page = {name:String} GROUP BY date
)
REJECT WITH 'REJECT';
```

Drop:
```sql
DROP RULE rule_1;
```
