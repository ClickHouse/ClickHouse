---
description: '使用 format 实用工具处理 ClickHouse 数据格式的指南'
slug: /operations/utilities/clickhouse-format
title: 'clickhouse-format'
doc_type: 'reference'
---

支持对输入查询进行格式化。

选项：

* `--help` or`-h` — 显示帮助信息。
* `--query` — 对任意长度和复杂度的查询进行格式化。
* `--hilite` or `--highlight` — 使用 ANSI 终端转义序列添加语法高亮。
* `--oneline` — 格式化为单行。
* `--max_line_length` — 将长度小于指定值的查询格式化为单行。
* `--comments` — 在输出中保留注释。
* `--quiet` or `-q` — 仅检查语法，成功时不输出内容。
* `--multiquery` or `-n` — 允许在同一文件中包含多个查询。
* `--obfuscate` — 执行混淆而非格式化。
* `--seed <string>` — 用于决定混淆结果的任意字符串种子。
* `--backslash` — 在格式化后的查询每行末尾添加反斜杠。从网页或其他地方复制多行查询后，如果想在命令行中执行，这会很有用。
* `--semicolons_inline` — 在多查询模式下，将分号写在查询最后一行的末尾，而不是单独占一行。

<div id="examples">
  ## 示例
</div>

1. 格式化查询：

```bash title="Query"
$ clickhouse-format --query "select number from numbers(10) where number%2 order by number desc;"
```

```bash title="Response"
SELECT number
FROM numbers(10)
WHERE number % 2
ORDER BY number DESC
```

2. 高亮与单行：

```bash title="Query"
$ clickhouse-format --oneline --hilite <<< "SELECT sum(number) FROM numbers(5);"
```

```sql title="Response"
SELECT sum(number) FROM numbers(5)
```

3. 多重查询：

```bash title="Query"
$ clickhouse-format -n <<< "SELECT min(number) FROM numbers(5); SELECT max(number) FROM numbers(5);"
```

```sql title="Response"
SELECT min(number)
FROM numbers(5)
;

SELECT max(number)
FROM numbers(5)
;

```

4. 混淆：

```bash title="Query"
$ clickhouse-format --seed Hello --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

```sql title="Response"
SELECT treasury_mammoth_hazelnut BETWEEN nutmeg AND span, CASE WHEN chive >= 116 THEN switching ELSE ANYTHING END;
```

相同的查询，另一个种子字符串：

```bash title="Query"
$ clickhouse-format --seed World --obfuscate <<< "SELECT cost_first_screen BETWEEN a AND b, CASE WHEN x >= 123 THEN y ELSE NULL END;"
```

```sql title="Response"
SELECT horse_tape_summer BETWEEN folklore AND moccasins, CASE WHEN intestine >= 116 THEN nonconformist ELSE FORESTRY END;
```

5. 添加反斜杠：

```bash title="Query"
$ clickhouse-format --backslash <<< "SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 1 UNION DISTINCT SELECT 3);"
```

```sql title="Response"
SELECT * \
FROM  \
( \
    SELECT 1 AS x \
    UNION ALL \
    SELECT 1 \
    UNION DISTINCT \
    SELECT 3 \
)
```