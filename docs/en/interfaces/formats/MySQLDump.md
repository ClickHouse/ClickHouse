---
title : MySQLDump
slug : /en/interfaces/formats/MySQLDump
keywords : [MySQLDump]
---

## Description

ClickHouse supports reading MySQL [dumps](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html).
It reads all data from INSERT queries belonging to one table in dump. If there are more than one table, by default it reads data from the first one.
You can specify the name of the table from which to read data from using [input_format_mysql_dump_table_name](/docs/en/operations/settings/settings-formats.md/#input_format_mysql_dump_table_name) settings.
If setting [input_format_mysql_dump_map_columns](/docs/en/operations/settings/settings-formats.md/#input_format_mysql_dump_map_columns) is set to 1 and
dump contains CREATE query for specified table or column names in INSERT query the columns from input data will be mapped to the columns from the table by their names,
columns with unknown names will be skipped if setting [input_format_skip_unknown_fields](/docs/en/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) is set to 1.
This format supports schema inference: if the dump contains CREATE query for the specified table, the structure is extracted from it, otherwise schema is inferred from the data of INSERT queries.


## Example Usage

Examples:

File dump.sql:
```sql
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

Queries:

```sql
DESCRIBE TABLE file(dump.sql, MySQLDump) SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```text
┌─name─┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ x    │ Nullable(Int32) │              │                    │         │                  │                │
└──────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

```sql
SELECT *
FROM file(dump.sql, MySQLDump)
         SETTINGS input_format_mysql_dump_table_name = 'test2'
```

```text
┌─x─┐
│ 1 │
│ 2 │
│ 3 │
└───┘
```

## Format Settings
