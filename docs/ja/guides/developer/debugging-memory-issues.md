---
slug: /ja/guides/developer/debugging-memory-issues
sidebar_label: メモリ問題のデバッグ
sidebar_position: 1
description: メモリ問題のデバッグに役立つクエリ。
---

# メモリ問題のデバッグ

メモリの問題やメモリリークが発生した場合、どのクエリやリソースが大量のメモリを消費しているのかを知ることが役立ちます。以下のクエリは、デバッグに役立ち、最適化できるクエリ、データベース、テーブルを見つけるのに役立ちます。

**ピークメモリ使用量で現在実行中のプロセスを一覧表示**

```sql
SELECT
    initial_query_id,
    query,
    elapsed,
    formatReadableSize(memory_usage),
    formatReadableSize(peak_memory_usage),
FROM system.processes
ORDER BY peak_memory_usage DESC
LIMIT 100;
```

**メモリ使用量のメトリクスを一覧表示**

```sql
SELECT
    metric, description, formatReadableSize(value) size
FROM
    system.asynchronous_metrics
WHERE
    metric like '%Cach%'
    or metric like '%Mem%'
order by
    value desc;
```

**現在のメモリ使用量別にテーブルを一覧表示**

```sql
SELECT
    database,
    name,
    formatReadableSize(total_bytes)
FROM system.tables
WHERE engine IN ('Memory','Set','Join');
```

**マージによる総メモリ使用量を出力**

```sql
SELECT formatReadableSize(sum(memory_usage)) FROM system.merges;
```

**現在実行中のプロセスによる総メモリ使用量を出力**

```sql
SELECT formatReadableSize(sum(memory_usage)) FROM system.processes;
```

**Dictionary による総メモリ使用量を出力**

```sql
SELECT formatReadableSize(sum(bytes_allocated)) FROM system.dictionaries;
```

**主キーによる総メモリ使用量を出力**

```sql
SELECT
    sumIf(data_uncompressed_bytes, part_type = 'InMemory') as memory_parts,
    formatReadableSize(sum(primary_key_bytes_in_memory)) AS primary_key_bytes_in_memory,
    formatReadableSize(sum(primary_key_bytes_in_memory_allocated)) AS primary_key_bytes_in_memory_allocated
FROM system.parts;
```
