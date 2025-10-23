---
description: 'Documentation for Query Rewrite Rules'
sidebar_label: 'Query Rewrite Rules'
sidebar_position: 89
slug: /operations/query-rules
title: 'Query Rewrite Rules'
doc_type: 'reference'
---

# Query Rewrite Rules

Query Rewrite Rules provide capabilities to create, alter and drop rules which allow users to rewrite or reject specific queries.

These rules reuse query parameter feature for query matching which allow substitutions in queries source and resulting templates.

The rules are being applied in the order of their creation.

## Syntax

Query rewrite rules provide three types of queries:
### CREATE RULE
Rewrite of rule:
```
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
```
CREATE RULE rule_name AS 
(
    any_query
) 
REJECT WITH 'Message';
```

### ALTER RULE
Rewrite of rule:
```
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
```
ALTER RULE rule_name AS 
(
    any_query
) 
REJECT WITH 'Message';
```

### DROP RULE
```
DROP RULE rule_name;
```

## Types of storages for query rewrite rules

Query rewrite rules can either be stored on local disk or in ZooKeeper/Keeper. By default local storage is used.

To configure query rewrite rules storage you need to specify a type. This can be either local or keeper/zookeeper.

Config example:

```
<clickhouse>
  <query_rules_storage>
    <type>local</type>
    <path>/query_rules/</path>
    <update_timeout_ms>1000</update_timeout_ms>
  </query_rules_storage>
</clickhouse>
```

## System tables

There are two system tables for query rewrite rules: system.query_rules and system.query_rules_log.

System table system.query_rules stores all created/altered query rules. 
Format of table: {name:String, rule:String} where name is rule name and rule is whole query.

System table system.query_rules_log stores logs of query rules. 
Format of table: {original_query:String, applied_rules:Array(String), resulting_query: String}.

## System setting

System boolean setting query_rules activates query rewrite rules.  By default value is false.

## Access grants

There are three access grants for query rewrite rules, each of these requires separate permission:
1. CREATE_RULE
2. ALTER_RULE
3. DROP_RULE

## Examples

Creation:
```
SET query_rules = 1;

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
```

Alteration:
```
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
```
DROP RULE rule_1;
```
