---
alias: []
description: 'Documentation for the MySQLDump format'
input_format: true
keywords: ['MySQLDump']
output_format: false
slug: /interfaces/formats/MySQLDump
title: 'MySQLDump'
---

| Input | Output  | Alias |
|-------|---------|-------|
| ✔     | ✗       |       |

## Description {#description}

ClickHouse supports reading MySQL [dumps](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html).

It reads all the data from `INSERT` queries belonging to a single table in the dump. 
If there is more than one table, by default it reads data from the first one.

:::note
This format supports schema inference: if the dump contains a `CREATE` query for the specified table, the structure is inferred from it, otherwise the schema is inferred from the data of `INSERT` queries.
:::

## Example Usage {#example-usage}

Given the following SQL dump file:

```sql title="dump.sql"
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test` (
  `x` int DEFAULT NULL,
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test` VALUES (1,NULL),(2,NULL),(3,NULL),(3,NULL),(4,NULL),(5,NULL),(6,7);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test 3` (
  `y` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test 3` VALUES (1);
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `test2` (
  `x` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
INSERT INTO `test2` VALUES (1),(2),(3);
```

We can run the following queries:

```sql title="Query"
DESCRIBE TABLE file(dump.sql, MySQLDump) 
SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```response title="Response"
┌─name─┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ x    │ Nullable(Int32) │              │                    │         │                  │                │
└──────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql title="Query"
SELECT *
FROM file(dump.sql, MySQLDump)
SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```response title="Response"
┌─x─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

## Format Settings {#format-settings}

You can specify the name of the table from which to read data from using the [`input_format_mysql_dump_table_name`](/operations/settings/settings-formats.md/#input_format_mysql_dump_table_name) setting.
If setting `input_format_mysql_dump_map_columns` is set to `1` and the dump contains a `CREATE` query for specified table or column names in the `INSERT` query, the columns from the input data will map to the columns from the table by name.
Columns with unknown names will be skipped if setting [`input_format_skip_unknown_fields`](/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to `1`.