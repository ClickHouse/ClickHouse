---
alias: []
description: 'MySQLDumpフォーマットのドキュメント'
input_format: true
keywords: ['MySQLDump']
output_format: false
slug: /interfaces/formats/MySQLDump
title: 'MySQLDump'
doc_type: 'reference'
---

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 説明
</div>

ClickHouse は MySQL の [ダンプ](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html) の読み込みをサポートしています。

ダンプ内では、単一のテーブルに属する `INSERT` クエリからすべてのデータを読み込みます。
複数のテーブルがある場合、デフォルトでは最初のテーブルのデータを読み込みます。

:::note
このフォーマットはスキーマ推論をサポートしています。ダンプに指定したテーブルの `CREATE` クエリが含まれている場合はそこから構造を推論し、含まれていない場合は `INSERT` クエリのデータからスキーマを推論します。
:::

<div id="example-usage">
  ## 使用例
</div>

次のSQLダンプファイルがあるとします：

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

以下のクエリを実行できます。

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

<div id="format-settings">
  ## フォーマット設定
</div>

[`input_format_mysql_dump_table_name`](/ja/operations/settings/settings-formats.md/#input_format_mysql_dump_table_name) 設定を使用すると、データの読み取り元となるテーブル名を指定できます。
`input_format_mysql_dump_map_columns` を `1` に設定し、ダンプに指定したテーブルの `CREATE` クエリまたは `INSERT` クエリ内のカラム名が含まれている場合、入力データのカラムはテーブルのカラム名に基づいて対応付けられます。
[`input_format_skip_unknown_fields`](/ja/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) 設定が `1` の場合、不明な名前のカラムはスキップされます。