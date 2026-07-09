---
description: 'CoalescingMergeTree 继承自 MergeTree 引擎。其关键特性
  是能够在 parts 合并过程中自动保留每列最后一个非 NULL 值。'
sidebar_label: 'CoalescingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/coalescingmergetree
title: 'CoalescingMergeTree 表引擎'
keywords: ['CoalescingMergeTree']
show_related_blogs: true
doc_type: 'reference'
---

:::note 自 25.6 版本起可用
此表引擎在 OSS 和 Cloud 中自 25.6 版本起可用。
:::

该引擎继承自 [MergeTree](/zh/engines/table-engines/mergetree-family/mergetree)。关键区别在于数据 parts 的合并方式：对于 `CoalescingMergeTree` 表，ClickHouse 会将具有相同主键 (更准确地说，具有相同[排序键](../../../engines/table-engines/mergetree-family/mergetree.md)) 的所有行合并为一行，该行包含每一列最新的非 NULL 值。

这支持列级 upsert，也就是说，你可以只更新特定列，而不必更新整行。

`CoalescingMergeTree` 旨在与非键列中的 Nullable 类型配合使用。如果这些列不是 Nullable，则其行为与 [ReplacingMergeTree](/zh/engines/table-engines/mergetree-family/replacingmergetree) 相同。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CoalescingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

有关请求参数的说明，请参见[请求描述](../../../sql-reference/statements/create/table.md)。

<div id="parameters-of-coalescingmergetree">
  ### CoalescingMergeTree 的参数
</div>

<div id="columns">
  #### 列
</div>

`columns` - 可选。一个元组，包含要合并其值的列名。指定的列不能属于分区键或排序键。如果未指定 `columns`，ClickHouse 会合并所有不属于排序键的列中的值。

<div id="query-clauses">
  ### 查询子句
</div>

创建 `CoalescingMergeTree` 表时，需要使用与创建 `MergeTree` 表时相同的[子句](../../../engines/table-engines/mergetree-family/mergetree.md)。

<details markdown="1">
  <summary>已弃用的建表方法</summary>

  :::note
  请勿在新项目中使用此方法；如有可能，请将旧项目切换为上述方法。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] CoalescingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  除 `columns` 外，其他所有参数的含义都与 `MergeTree` 中相同。

  * `columns` — 一个元组，其中包含其值将被求和的列名。该参数为可选参数。有关说明，请参见上文。
</details>

<div id="usage-example">
  ## 使用示例
</div>

请看下表：

```sql
CREATE TABLE test_table
(
    key UInt64,
    value_int Nullable(UInt32),
    value_string Nullable(String),
    value_date Nullable(Date)
)
ENGINE = CoalescingMergeTree()
ORDER BY key
```

向其中插入数据：

```sql
INSERT INTO test_table VALUES(1, NULL, NULL, '2025-01-01'), (2, 10, 'test', NULL);
INSERT INTO test_table VALUES(1, 42, 'win', '2025-02-01');
INSERT INTO test_table(key, value_date) VALUES(2, '2025-02-01');
```

结果如下所示：

```sql
SELECT * FROM test_table ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   1 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-01-01 │
│   2 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-02-01 │
│   2 │        10 │ test         │       ᴺᵁᴸᴸ │
└─────┴───────────┴──────────────┴────────────┘
```

为获得正确的最终结果，建议使用以下查询：

```sql
SELECT * FROM test_table FINAL ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   2 │        10 │ test         │ 2025-02-01 │
└─────┴───────────┴──────────────┴────────────┘
```

使用 `FINAL` 修饰符会强制 ClickHouse 在查询时应用合并逻辑，确保每一列都能得到正确、合并后的“最新”值。对于从 CoalescingMergeTree 表中查询的场景，这是最安全、最准确的方法。

:::note

如果底层 parts 尚未完全合并，使用 `GROUP BY` 的方式可能会返回错误结果。

```sql
SELECT key, last_value(value_int), last_value(value_string), last_value(value_date)  FROM test_table GROUP BY key; -- Not recommended.
```

:::

<div id="tuple-element-aggregation">
  ## Tuple 元素聚合
</div>

启用 `allow_tuple_element_aggregation` 设置后，`Tuple` 列会被递归展平，因此每个叶子元素都可以独立参与合并。这使您能够在单个 `Tuple` 列中存储多个字段，并在合并过程中按元素分别合并它们——每个 `Nullable` 子列都会独立保留最新的非 NULL 值。

展平后的子列与普通列遵循相同的规则：

* 属于排序键或分区键中某个 `Tuple` 的子列不会参与合并。
* 如果指定了 `columns`，则只有所列 `Tuple` 列的子列会参与合并。

:::note
此设置不可变，必须在创建表时指定。
:::

```sql
CREATE TABLE coalescing_tuples
(
    key UInt64,
    data Tuple(
        value_a Nullable(UInt64),
        value_b Nullable(String),
        nested Tuple(
            value_c Nullable(UInt64)
        )
    )
) ENGINE = CoalescingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO coalescing_tuples VALUES (1, (100, NULL, (NULL)));
INSERT INTO coalescing_tuples VALUES (1, (NULL, 'hello', (42)));

SELECT key, data.value_a, data.value_b, data.nested.value_c FROM coalescing_tuples FINAL;
```

```text
┌─key─┬─data.value_a─┬─data.value_b─┬─data.nested.value_c─┐
│   1 │          100 │ hello        │                  42 │
└─────┴──────────────┴──────────────┴─────────────────────┘
```