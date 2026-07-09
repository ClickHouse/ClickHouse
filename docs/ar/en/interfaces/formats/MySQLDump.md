---
alias: []
description: 'توثيق لصيغة MySQLDump'
input_format: true
keywords: ['MySQLDump']
output_format: false
slug: /interfaces/formats/MySQLDump
title: 'MySQLDump'
doc_type: 'reference'
---

| الإدخال | المخرجات | الاسم المستعار |
| ------- | -------- | -------------- |
| ✔       | ✗        |                |

<div id="description">
  ## الوصف
</div>

يدعم ClickHouse قراءة [ملفات تفريغ](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html) MySQL.

يقرأ جميع البيانات من استعلامات `INSERT` الخاصة بجدول واحد داخل ملف التفريغ.
إذا كان هناك أكثر من جدول، فسيقرأ البيانات من الجدول الأول افتراضيًا.

:::note
يدعم هذا التنسيق استنتاج المخطط: إذا كان ملف التفريغ يحتوي على استعلام `CREATE` للجدول المحدد، فسيتم استنتاج البنية منه، وإلا فسيتم استنتاج المخطط من بيانات استعلامات `INSERT`.
:::

<div id="example-usage">
  ## مثال للاستخدام
</div>

بالنظر إلى ملف تفريغ SQL التالي:

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

يمكننا تنفيذ الاستعلامات التالية:

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
  ## إعدادات التنسيق
</div>

يمكنك تحديد اسم الجدول الذي ستُقرأ منه البيانات باستخدام الإعداد [`input_format_mysql_dump_table_name`](/ar/operations/settings/settings-formats.md/#input_format_mysql_dump_table_name).
إذا كان الإعداد `input_format_mysql_dump_map_columns` مضبوطًا على `1` وكان ملف التفريغ يحتوي على استعلام `CREATE` للجدول المحدد أو على أسماء الأعمدة في استعلام `INSERT`، فستُطابَق الأعمدة الواردة في بيانات الإدخال مع أعمدة الجدول حسب الاسم.
سيتم تخطي الأعمدة ذات الأسماء غير المعروفة إذا كان الإعداد [`input_format_skip_unknown_fields`](/ar/operations/settings/settings-formats.md/#input_format_skip_unknown_fields) مضبوطًا على `1`.