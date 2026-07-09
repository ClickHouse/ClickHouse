---
description: '表相关文档'
keywords: ['压缩', 'codec', 'schema', 'DDL']
sidebar_label: '表'
sidebar_position: 36
slug: /sql-reference/statements/create/table
title: 'CREATE TABLE'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

创建新表。该查询可根据具体用例采用不同的语法形式。

默认情况下，表仅在当前服务器上创建。分布式 DDL 查询通过 `ON CLUSTER` 子句实现，相关内容[另行介绍](../../../sql-reference/distributed-ddl.md)。

<div id="syntax-forms">
  ## 语法形式
</div>

<div id="with-explicit-schema">
  ### 使用显式指定的 schema
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [COMMENT 'comment for column'] [compression_codec] [TTL expr1],
    name2 [type2] [NULL|NOT NULL] [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [COMMENT 'comment for column'] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
  [COMMENT 'comment for table']
```

在 `db` 数据库中创建一个名为 `table_name` 的表；如果未设置 `db`，则在当前数据库中创建。该表使用括号中指定的结构和 `engine` 引擎。
表的结构由列描述、二级索引、投影和约束组成。如果该引擎支持[主键](#primary-key)，则会将其标明为表引擎的参数。

最简单的情况下，列描述的形式为 `name type`。示例：`RegionID UInt32`。

也可以为默认值定义表达式 (见下文) 。

如有需要，可以指定主键，并包含一个或多个键表达式。

可以为列和表添加注释。

<div id="with-a-schema-similar-to-other-table">
  ### 使用现有表的 schema
</div>

ClickHouse 支持复制现有表的 schema 和数据。

要复制现有表的 schema：

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine]
```

这会创建一个与另一张表结构相同的表。

<div id="with-a-schema-and-data-cloned-from-another-table">
  ### 使用现有表的 schema 和数据
</div>

要复制现有表的 schema 和数据，可按以下方式操作：

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone CLONE AS [db.]table [ENGINE = engine]
```

这会创建一个与现有表具有相同 schema 和数据的表。新表创建后，`db.table` 的所有分区都会附加到该表。换句话说，`db.table` 的数据会在创建时克隆到 `db2.table_clone` 中。此查询等同于以下内容：

```sql
CREATE TABLE [IF NOT EXISTS] [db2.]table_clone AS [db.]table [ENGINE = engine];
ALTER TABLE [db2.]table_clone ATTACH PARTITION ALL FROM [db.]table;
```

对于这两项功能，你都可以为该表指定不同的引擎。如果未指定引擎，则会使用与原始表 (`db.table`) 相同的引擎。

<div id="from-a-table-function">
  ### 从表函数
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

创建一个表，其结果与指定的 [table function](/zh/sql-reference/table-functions) 相同。创建的表也会以与所指定的对应 table function 相同的方式运行。

<div id="from-select-query">
  ### 从 SELECT 查询
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name[(name1 [type1], name2 [type2], ...)] ENGINE = engine AS SELECT ...
```

创建一个表，其结构与 `SELECT` 查询结果类似，使用 `engine` 引擎，并用 `SELECT` 的数据填充该表。你也可以显式指定列的描述。

如果该表已存在，并且指定了 `IF NOT EXISTS`，则该查询不会执行任何操作。

查询中，`ENGINE` 子句后面还可以跟其他子句。有关如何创建表的详细信息，请参阅[表引擎](/zh/engines/table-engines)说明中的相关文档。

**示例**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1;
SELECT x, toTypeName(x) FROM t1;
```

```text title="Response"
┌─x─┬─toTypeName(x)─┐
│ 1 │ String        │
└───┴───────────────┘
```

<div id="null-or-not-null-modifiers">
  ## NULL 或 NOT NULL 修饰符
</div>

列定义中，数据类型后的 `NULL` 和 `NOT NULL` 修饰符用于控制该列是否可以为 [Nullable](/zh/sql-reference/data-types/nullable)。

如果该类型不是 `Nullable`，并且指定了 `NULL`，则会将其视为 `Nullable`；如果指定了 `NOT NULL`，则不会。例如，`INT NULL` 等同于 `Nullable(INT)`。如果该类型本身就是 `Nullable`，再指定 `NULL` 或 `NOT NULL` 修饰符时，则会抛出异常。

另请参见 [data&#95;type&#95;default&#95;nullable](../../../operations/settings/settings.md#data_type_default_nullable) 设置。

<div id="default_values">
  ## 默认值
</div>

列描述可以用 `DEFAULT expr`、`MATERIALIZED expr` 或 `ALIAS expr` 的形式指定默认值表达式。例如：`URLDomain String DEFAULT domain(URL)`。

表达式 `expr` 是可选的。如果省略，则必须显式指定列类型，默认值分别为：数值列为 `0`，字符串列为 `''` (空字符串) ，数组列为 `[]` (空数组) ，日期列为 `1970-01-01`，可空列为 `NULL`。

默认值列的列类型可以省略，此时会根据 `expr` 的类型自动推断。例如，列 `EventDate DEFAULT toDate(EventTime)` 的类型将为 Date。

如果同时指定了数据类型和默认值表达式，则会插入一个隐式类型转换函数，将该表达式转换为指定的类型。例如：`Hits UInt32 DEFAULT 0` 在内部表示为 `Hits UInt32 DEFAULT toUInt32(0)`。

默认值表达式 `expr` 可以引用表中的任意列和常量。ClickHouse 会检查对表结构的更改不会在表达式计算中引入循环。对于 INSERT，它会检查表达式是否可解析——也就是说，计算这些表达式所需的所有列都已传入。

<div id="default">
  ### DEFAULT
</div>

`DEFAULT expr`

普通默认值。如果在 `INSERT` 查询中未指定此类列的值，则会根据 `expr` 计算该值。

示例：

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime DEFAULT now(),
    updated_at_date Date DEFAULT toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id) VALUES (1);

SELECT * FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:06:46 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="materialized">
  ### MATERIALIZED
</div>

`MATERIALIZED expr`

物化表达式。插入行时，这类列的值会根据指定的物化表达式自动计算，且无法在 `INSERT` 时显式指定。

此外，此类默认值列不会包含在 `SELECT *` 的结果中。这是为了保持这样一个不变性：`SELECT *` 的结果始终都可以通过 `INSERT` 再次插入到表中。可以通过设置 `asterisk_include_materialized_columns` 禁用此行为。

示例：

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    updated_at DateTime MATERIALIZED now(),
    updated_at_date Date MATERIALIZED toDate(updated_at)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1);

SELECT * FROM test;
┌─id─┐
│  1 │
└────┘

SELECT id, updated_at, updated_at_date FROM test;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘

SELECT * FROM test SETTINGS asterisk_include_materialized_columns=1;
┌─id─┬──────────updated_at─┬─updated_at_date─┐
│  1 │ 2023-02-24 17:08:08 │      2023-02-24 │
└────┴─────────────────────┴─────────────────┘
```

<div id="ephemeral">
  ### EPHEMERAL
</div>

`EPHEMERAL [expr]`

临时列。此类列不会存储在表中，也不能对其执行 `SELECT`。临时列的唯一用途，是基于它们构建其他列的默认值表达式。

执行未显式指定列的插入时，会跳过此类列。这样做是为了保持这样一个不变性：`SELECT *` 的结果始终可以使用 `INSERT` 再次插入回表中。

示例：

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    unhexed String EPHEMERAL,
    hexed FixedString(4) DEFAULT unhex(unhexed)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test (id, unhexed) VALUES (1, '5a90b714');

SELECT
    id,
    hexed,
    hex(hexed)
FROM test
FORMAT Vertical;

Row 1:
──────
id:         1
hexed:      Z��
hex(hexed): 5A90B714
```

<div id="alias">
  ### ALIAS
</div>

`ALIAS expr`

计算列 (同义词) 。这种类型的列不会存储在表中，也无法向其 `INSERT` 值。

当 `SELECT` 查询显式引用这种类型的列时，其值会在查询时根据 `expr` 计算。默认情况下，`SELECT *` 不包含 ALIAS 列。可以通过设置 `asterisk_include_alias_columns` 来禁用此行为。

使用 ALTER 查询添加新列时，不会将旧数据写入这些列。相反，读取不包含这些新列值的旧数据时，默认会动态计算表达式。不过，如果执行这些表达式需要查询中未指定的其他列，则这些列也会被额外读取，但仅限于需要它们的数据块。

如果向表中添加了新列，但之后又修改了它的默认表达式，那么旧数据使用的值也会发生变化 (对于那些值未存储在磁盘上的数据) 。请注意，在执行后台合并时，如果参与合并的某个 parts 中缺少某列的数据，则会将该列的数据写入 merged part。

无法为嵌套数据结构中的元素设置默认值。

```sql
CREATE OR REPLACE TABLE test
(
    id UInt64,
    size_bytes Int64,
    size String ALIAS formatReadableSize(size_bytes)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO test VALUES (1, 4678899);

SELECT id, size_bytes, size FROM test;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘

SELECT * FROM test SETTINGS asterisk_include_alias_columns=1;
┌─id─┬─size_bytes─┬─size─────┐
│  1 │    4678899 │ 4.46 MiB │
└────┴────────────┴──────────┘
```

<div id="primary-key">
  ## 主键
</div>

您可以在创建表时定义[主键](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries)。主键可以通过以下两种方式指定：

* 在列列表中

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...,
    PRIMARY KEY(expr1[, expr2,...])
)
ENGINE = engine;
```

* 不在列列表中

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
PRIMARY KEY(expr1[, expr2,...]);
```

:::tip
不能在一次查询中同时使用这两种方式。
:::

<div id="constraints">
  ## 约束
</div>

除了列描述，还可以定义约束：

<div id="constraint">
  ### CONSTRAINT
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` 可以是任意布尔表达式。如果为该表定义了约束，那么在 `INSERT` 查询中，每一行都要检查所有这些约束。如果有任何约束不满足，服务器将抛出异常，并附带约束名称和检查表达式。

添加大量约束可能会对大型 `INSERT` 查询的性能造成负面影响。

可通过 [`system.constraints`](/zh/operations/system-tables/constraints) 表查看所有表中现有的约束。

<div id="assume">
  ### ASSUME
</div>

`ASSUME` 子句用于在表上定义一个假定为 true 的 `CONSTRAINT`。优化器随后可利用该约束提升 SQL 查询性能。

来看一个示例，展示在创建 `users_a` 表时如何使用 `ASSUME CONSTRAINT`：

```sql
CREATE TABLE users_a (
    uid Int16, 
    name String, 
    age Int16, 
    name_len UInt8 MATERIALIZED length(name), 
    CONSTRAINT c1 ASSUME length(name) = name_len
) 
ENGINE=MergeTree 
ORDER BY (name_len, name);
```

这里，`ASSUME CONSTRAINT` 用于断言 `length(name)` 函数始终等于 `name_len` 列中的值。这意味着，每当在查询中调用 `length(name)` 时，ClickHouse 都可以将其替换为 `name_len`，这样通常会更快，因为无需调用 `length()` 函数。

然后，在执行查询 `SELECT name FROM users_a WHERE length(name) < 5;` 时，ClickHouse 可以将其优化为 `SELECT name FROM users_a WHERE name_len < 5;`，这是因为有 `ASSUME CONSTRAINT`。这样可以让查询运行得更快，因为无需为每一行计算 `name` 的长度。

`ASSUME CONSTRAINT` **不会强制执行该约束**，它只是告知优化器该约束成立。如果该约束实际上并不成立，查询结果可能会不正确。因此，只有在你确信该约束成立时，才应使用 `ASSUME CONSTRAINT`。

<div id="ttl-expression">
  ## 生存时间 (TTL) 表达式
</div>

定义值的存储时间。只能为 MergeTree 家族表指定。详细说明请参见 [列和表的 TTL](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)。

<div id="column_compression_codec">
  ## 列压缩编解码器
</div>

默认情况下，ClickHouse 在自管理版本中使用 `lz4` 压缩，在 ClickHouse Cloud 中使用 `zstd`。

对于 `MergeTree` 引擎家族，您可以在服务器配置的 [compression](/zh/operations/server-configuration-parameters/settings#compression) 部分修改默认压缩方法。

您还可以在 `CREATE TABLE` 查询中为每一列单独定义压缩方法。

```sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9)),
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

可以指定 `Default` 编解码器，以表示使用默认压缩；具体行为可能取决于运行时中的不同设置 (以及数据本身的属性) 。
示例：`value UInt64 CODEC(Default)` —— 与不指定编解码器效果相同。

你也可以从该列中移除当前的 CODEC，并使用 config.xml 中的默认压缩：

```sql
ALTER TABLE codec_example MODIFY COLUMN float_value CODEC(Default);
```

编解码器可以串联组合，例如 `CODEC(Delta, Default)`。

:::tip
不能使用 `lz4` 这类外部工具解压 ClickHouse 数据库文件。请改用专用的 [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) 工具。
:::

以下表引擎支持压缩：

* [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) 家族。支持列压缩编解码器，并可通过 [compression](/zh/operations/server-configuration-parameters/settings#compression) 设置选择默认压缩方法。
* [Log](../../../engines/table-engines/log-family/index.md) 家族。默认使用 `lz4` 压缩方法，并支持列压缩编解码器。
* [Set](../../../engines/table-engines/special/set.md)。仅支持默认压缩方法。
* [Join](../../../engines/table-engines/special/join.md)。仅支持默认压缩方法。

ClickHouse 支持通用编解码器和专用编解码器。

<div id="general-purpose-codecs">
  ### 通用编解码器
</div>

<div id="none">
  #### NONE
</div>

`NONE` — 不压缩。

<div id="lz4">
  #### LZ4
</div>

`LZ4` — 默认使用的无损[数据压缩算法](https://github.com/lz4/lz4)，采用 LZ4 快速压缩。

<div id="lz4hc">
  #### LZ4HC
</div>

`LZ4HC[(level)]` — 支持级别配置的 LZ4 HC (高压缩) 算法。默认级别：9。设置 `level <= 0` 时，将使用默认级别。可用级别：[1, 12]。推荐级别范围：[4, 9]。

<div id="zstd">
  #### ZSTD
</div>

`ZSTD[(level)]` — [ZSTD 压缩算法](https://en.wikipedia.org/wiki/Zstandard)，支持配置 `level`。可选级别：[1, 22]。默认级别：1。

较高的压缩级别适用于非对称场景，例如压缩一次、反复解压。级别越高，压缩效果越好，但 CPU 使用率也越高。

<div id="zstd_qat">
  #### 已废弃：ZSTD_QAT
</div>

<CloudNotSupportedBadge />

<div id="deflate_qpl">
  #### 已废弃：DEFLATE_QPL
</div>

<CloudNotSupportedBadge />

<div id="specialized-codecs">
  ### 专用编解码器
</div>

这些编解码器旨在利用数据的特性来提升压缩效果。其中一些编解码器本身并不直接压缩数据，而是先对数据进行预处理，以便在第二阶段结合通用编解码器时获得更高的压缩率。

<div id="delta">
  #### Delta
</div>

`Delta(delta_bytes)` — 一种压缩方法，其中原始值会被替换为相邻两个值之差，但第一个值保持不变。`delta_bytes` 是原始值的最大大小，默认值为 `sizeof(type)`。将 `delta_bytes` 作为参数指定的做法已弃用，相关支持将在未来版本中移除。Delta 是一种数据预处理编解码器，也就是说，不能单独使用。

<div id="doubledelta">
  #### DoubleDelta
</div>

`DoubleDelta(bytes_size)` — 计算 delta 的 delta，并以紧凑的二进制形式写入。`bytes_size` 的含义与 [Delta](#delta) 编解码器中的 `delta_bytes` 类似。将 `bytes_size` 作为参数指定的做法已弃用，未来版本将移除对此的支持。对于步长固定的单调序列 (例如时间序列数据) ，可以获得最佳压缩率。可用于任何数值类型。它实现了 Gorilla TSDB 使用的算法，并将其扩展为支持 64 位类型。对于 32 位 delta，会额外使用 1 个比特：使用 5 比特前缀，而不是 4 比特前缀。更多信息请参阅 [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf) 一文中的 Compressing Time Stamps。DoubleDelta 是一种数据预处理编解码器，即不能单独使用。

<div id="gcd">
  #### GCD
</div>

`GCD()` - - 先计算列中各个值的最大公约数 (GCD) ，再将每个值除以该 GCD。可用于整数、小数和日期/时间列。该编解码器非常适合用于那些值以 GCD 的倍数变化 (增大或减小) 的列，例如 24、28、16、24、8、24 (GCD = 4) 。GCD 是一种数据预处理编解码器，也就是说，不能单独使用。

<div id="gorilla">
  #### Gorilla
</div>

`Gorilla(bytes_size)` — 计算当前浮点值与前一个浮点值的异或，并以紧凑的二进制形式写入。连续值之间的差异越小，也就是说该序列的值变化越慢，压缩率就越高。它实现了 Gorilla TSDB 使用的算法，并将其扩展为支持 64 位类型。`bytes_size` 的可能取值为：1、2、4、8；如果 `sizeof(type)` 等于 1、2、4 或 8，则默认值为 `sizeof(type)`。在所有其他情况下，值为 1。更多信息请参见 [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://doi.org/10.14778/2824032.2824078) 的第 4.1 节。

<div id="alp">
  #### ALP
</div>

<ExperimentalBadge />

`ALP(variant)` — 用于浮点数据的自适应无损压缩。支持 `Float32` 和 `Float64`。详见 [ALP: Adaptive lossless floating-point compression](https://ir.cwi.nl/pub/33334)。

该编解码器接受一个可选的变体参数：

* `ALP()` 或 `ALP(AUTO)` (默认) — 使用 STD，并根据估算的压缩后大小回退到 RD。
* `ALP(STD)` — 标准 ALP 变体。使用十进制幂将每个值表示为精确的缩放整数，然后使用 Frame-of-Reference 和位打包压缩生成的整数。无法表示的值会作为原始异常值存储。最适合源自小数的数值 (例如测量值、价格) 。
* `ALP(RD)` — Real Doubles 变体。重新解释每个值的位模式，并将其拆分为高位部分 (符号位 + 指数 + 尾数高位) 和低位部分。高位部分使用字典编码 (最多 8 个条目) ，低位部分使用位打包。最适合大量值共享相同高位比特的场景。

:::note
此编解码器为 Experimental，使用时需要设置 `SET allow_experimental_codecs = 1`。
:::

<div id="fpc">
  #### FPC
</div>

`FPC(level, float_size)` - 使用两种预测器中效果更好的一种，反复预测序列中的下一个浮点值，然后将实际值与预测值进行异或，并对结果进行前导零压缩。与 Gorilla 类似，在存储一系列变化缓慢的浮点值时，这种方式效率很高。对于 64 位值 (double) ，FPC 比 Gorilla 更快；对于 32 位值，实际效果则可能因情况而异。可选的 `level` 值为：1-28，默认值为 12。可选的 `float_size` 值为：4、8；如果类型是 Float，则默认值为 `sizeof(type)`。在其他所有情况下，其值为 4。有关该算法的详细说明，请参阅 [High Throughput Compression of Double-Precision Floating-Point Data](https://userweb.cs.txstate.edu/~burtscher/papers/dcc07a.pdf)。

<div id="t64">
  #### T64
</div>

`T64` —— 一种压缩方法，用于裁掉整数数据类型 (包括 `Enum`、`Date` 和 `DateTime`) 中未使用的高位。在算法的每一步中，该编解码器都会取一个包含 64 个值的块，将其放入一个 64×64 比特矩阵中，对矩阵进行转置，裁掉值中未使用的位，并将剩余部分作为一个序列返回。所谓未使用的位，是指在使用该压缩的整个数据分区片段中，在最大值与最小值之间没有变化的那些位。

`DoubleDelta` 和 `Gorilla` 编解码器在 Gorilla TSDB 中用作其压缩算法的组成部分。Gorilla 方法在一系列值及其时间戳缓慢变化的场景下非常有效。时间戳可通过 `DoubleDelta` 编解码器高效压缩，而值则可通过 `Gorilla` 编解码器高效压缩。例如，要创建一个存储效率高的表，可以按如下配置创建：

```sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

<div id="encryption-codecs">
  ### 加密编解码器
</div>

这些编解码器实际上并不压缩数据，而是对磁盘上的数据进行加密。只有在通过 [encryption](/zh/operations/server-configuration-parameters/settings#encryption) 设置指定了加密密钥时，这些编解码器才可用。请注意，加密只适合放在编解码器管道的末端，因为加密后的数据通常无法再进行有效压缩。

加密编解码器：

<div id="aes_128_gcm_siv">
  #### AES_128_GCM_SIV
</div>

`CODEC('AES-128-GCM-SIV')` — 使用 [RFC 8452](https://tools.ietf.org/html/rfc8452) 中定义的 GCM-SIV 模式，以 AES-128 对数据进行加密。

<div id="aes-256-gcm-siv">
  #### AES-256-GCM-SIV
</div>

`CODEC('AES-256-GCM-SIV')` — 使用 GCM-SIV 模式的 AES-256 加密数据。

这些编解码器使用固定 nonce，因此加密是确定性的。这使其与 [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md) 等支持去重的引擎兼容，但也有一个弱点：同一个数据块如果被加密两次，生成的密文将完全相同，因此能够读取磁盘的攻击者可以看出它们是相同的 (尽管只能看出这一点，无法获知其内容) 。

:::note
大多数引擎 (包括 &quot;*MergeTree&quot; 家族) 都会在磁盘上创建索引文件，且不会应用编解码器。这意味着如果加密列建立了索引，明文会出现在磁盘上。
:::

:::note
如果执行 SELECT 查询时在加密列中提到了某个特定值 (例如在 WHERE 子句中) ，该值可能会出现在 [system.query&#95;log](../../../operations/system-tables/query_log.md) 中。你可能需要禁用日志记录。
:::

**示例**

```sql
CREATE TABLE mytable
(
    x String CODEC(AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

:::note
如果需要启用压缩，必须明确指定。否则，数据只会被加密。
:::

**示例**

```sql
CREATE TABLE mytable
(
    x String CODEC(Delta, LZ4, AES_128_GCM_SIV)
)
ENGINE = MergeTree ORDER BY x;
```

<div id="temporary-tables">
  ## 临时表
</div>

:::note
请注意，临时表不会被复制。因此，插入临时表的数据无法保证在其他副本中可用。临时表的主要用例是在单个会话中，对小型外部数据集进行查询或连接。
:::

ClickHouse 支持临时表，具有以下特性：

* 临时表会在会话结束时消失，包括连接断开的情况。
* 如果未指定引擎，临时表会使用 Memory 表引擎；此外，它还可以使用除 Replicated 和 `KeeperMap` 引擎之外的任何表引擎。
* 临时表不能指定 DB。它是在 databases 之外创建的。
* 无法通过分布式 DDL 查询在所有 cluster 服务器上创建临时表 (使用 `ON CLUSTER`) ，因为该表仅存在于当前会话中。
* 如果临时表与另一个表同名，并且查询中只指定了表名而未指定 DB，则会使用临时表。
* 在分布式查询处理中，查询中使用的 Memory 引擎临时表会被传递到远程服务器。

要创建临时表，请使用以下语法：

```sql
CREATE [OR REPLACE] TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) [ENGINE = engine]
```

在大多数情况下，临时表不是手动创建的，而是在查询使用外部数据时，或用于分布式 `(GLOBAL) IN` 时自动创建。更多信息请参见相应章节。

也可以使用 [ENGINE = Memory](../../../engines/table-engines/special/memory.md) 的表来代替临时表。

<div id="replace-table">
  ## REPLACE TABLE
</div>

`REPLACE` 语句允许你[以原子方式](/zh/concepts/glossary#atomicity)更新表。

:::note
该语句支持 [`Atomic`](../../../engines/database-engines/atomic.md) 和 [`Replicated`](../../../engines/database-engines/replicated.md) 数据库引擎，
它们分别是 ClickHouse 和 ClickHouse Cloud 的默认数据库引擎。
:::

通常，如果你需要从表中删除部分数据，
可以创建一个新表，并使用不会检索到不需要数据的 `SELECT` 语句将其填充，
然后删除旧表并重命名新表。
下方示例演示了这种方法：

```sql
CREATE TABLE myNewTable AS myOldTable;

INSERT INTO myNewTable
SELECT * FROM myOldTable 
WHERE CounterID <12345;

DROP TABLE myOldTable;

RENAME TABLE myNewTable TO myOldTable;
```

除了上述方法，也可以使用 `REPLACE` (前提是使用默认数据库引擎) 达到相同效果：

```sql
REPLACE TABLE myOldTable
ENGINE = MergeTree()
ORDER BY CounterID 
AS
SELECT * FROM myOldTable
WHERE CounterID <12345;
```

<div id="syntax">
  ### 语法
</div>

```sql
{CREATE [OR REPLACE] | REPLACE} TABLE [db.]table_name
```

:::note
`CREATE` 语句的所有语法形式同样适用于此语句。对不存在的表执行 `REPLACE` 会报错。
:::

<div id="examples">
  ### 示例:
</div>

<Tabs>
  <TabItem value="clickhouse_replace_example" label="本地" default>
    参考下表：

    ```sql
    CREATE DATABASE base 
    ENGINE = Atomic;

    CREATE OR REPLACE TABLE base.t1
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    ┌─n─┬─s────┐
    │ 1 │ test │
    └───┴──────┘
    ```

    我们可以使用 `REPLACE` 语句清空所有数据：

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    ┌─n─┬─s──┐
    │ 2 │ \N │
    └───┴────┘
    ```

    或者，我们也可以使用 `REPLACE` 语句更改表结构：

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    ┌─n─┐
    │ 3 │
    └───┘
    ```
  </TabItem>

  <TabItem value="cloud_replace_example" label="Cloud">
    参考 ClickHouse Cloud 上的下表：

    ```sql
    CREATE DATABASE base;

    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64,
        s String
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (1, 'test');

    SELECT * FROM base.t1;

    1    test
    ```

    我们可以使用 `REPLACE` 语句清空所有数据：

    ```sql
    CREATE OR REPLACE TABLE base.t1 
    (
        n UInt64, 
        s Nullable(String)
    )
    ENGINE = MergeTree
    ORDER BY n;

    INSERT INTO base.t1 VALUES (2, null);

    SELECT * FROM base.t1;

    2    
    ```

    或者，我们也可以使用 `REPLACE` 语句更改表结构：

    ```sql
    REPLACE TABLE base.t1 (n UInt64) 
    ENGINE = MergeTree 
    ORDER BY n;

    INSERT INTO base.t1 VALUES (3);

    SELECT * FROM base.t1;

    3
    ```
  </TabItem>
</Tabs>

<div id="comment-clause">
  ## COMMENT 子句
</div>

在创建表时，可以为其添加注释。

**语法**

```sql
CREATE TABLE [db.]table_name
(
    name1 type1, name2 type2, ...
)
ENGINE = engine
COMMENT 'Comment'
```

:::note
`COMMENT` 子句必须在任何存储专用子句 (如 `PARTITION BY`、`ORDER BY` 和存储专用 `SETTINGS`) **之后**指定。

在 `COMMENT` 子句之后，只会解析查询专用的 `SETTINGS` (如 `max_threads` 等) ，不会再解析与存储相关的设置。

这意味着正确的子句顺序是：

* `ENGINE`
* 存储子句
* `COMMENT`
* 查询设置 (如有)
  :::

**示例**

```sql title="Query"
CREATE TABLE t1 (x String) ENGINE = Memory COMMENT 'The temporary table';
SELECT name, comment FROM system.tables WHERE name = 't1';
```

```text title="Response"
┌─name─┬─comment─────────────┐
│ t1   │ The temporary table │
└──────┴─────────────────────┘
```

<div id="related-content">
  ## 相关内容
</div>

* 博客：[借助 schema 和编解码器优化 ClickHouse](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema)
* 博客：[在 ClickHouse 中处理时间序列数据](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)