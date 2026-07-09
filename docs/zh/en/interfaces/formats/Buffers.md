---
alias: []
description: 'Buffers 格式文档'
input_format: true
keywords: ['Buffers']
output_format: true
slug: /interfaces/formats/Buffers
title: 'Buffers'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

`Buffers` 是一种非常简单的二进制格式，用于**临时**数据交换，其中消费者和生产者都已知 schema 和列顺序。

与 [Native](./Native.md) 不同，它**不会**存储列名、列类型或任何额外的元数据。

在这种格式中，数据以二进制格式按[块](/zh/development/architecture#block)写入和读取。Buffers 采用与 [Native](./Native.md) 格式相同的按列二进制表示，并遵循相同的 Native 格式设置。

对于每个块，会写入以下序列：

1. 列数 (UInt64，小端序) 。
2. 行数 (UInt64，小端序) 。
3. 对于每一列：

* 序列化后的列数据总字节数 (UInt64，小端序) 。
* 序列化后的列数据字节，与 [Native](./Native.md) 格式中的内容完全一致。

<div id="example-usage">
  ## 使用示例
</div>

写入文件：

```sql
SELECT
    number AS num,
    number * number AS num_square
FROM numbers(10)
INTO OUTFILE 'squares.buffers'
FORMAT Buffers;
```

使用显式列类型读回：

```sql
SELECT
    *
FROM file(
    'squares.buffers',
    'Buffers',
    'col_1 UInt64, col_2 UInt64'
);
```

```txt
  ┌─col_1─┬─col_2─┐
  │     0 │     0 │
  │     1 │     1 │
  │     2 │     4 │
  │     3 │     9 │
  │     4 │    16 │
  │     5 │    25 │
  │     6 │    36 │
  │     7 │    49 │
  │     8 │    64 │
  │     9 │    81 │
  └───────┴───────┘
```

如果你有一个列类型相同的表，可以直接向其中插入数据：

```sql
CREATE TABLE number_squares
(
    a UInt64,
    b UInt64
) ENGINE = Memory;

INSERT INTO number_squares
FROM INFILE 'squares.buffers'
FORMAT Buffers;
```

查看该表：

```sql
SELECT * FROM number_squares;
```

```txt
  ┌─a─┬──b─┐
  │ 0 │  0 │
  │ 1 │  1 │
  │ 2 │  4 │
  │ 3 │  9 │
  │ 4 │ 16 │
  │ 5 │ 25 │
  │ 6 │ 36 │
  │ 7 │ 49 │
  │ 8 │ 64 │
  │ 9 │ 81 │
  └───┴────┘
```

<div id="format-settings">
  ## 格式设置
</div>
