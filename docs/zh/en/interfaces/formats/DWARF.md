---
alias: []
description: 'DWARF 格式文档'
input_format: true
keywords: ['DWARF']
output_format: false
slug: /interfaces/formats/DWARF
title: 'DWARF'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 说明
</div>

`DWARF` 格式可从 ELF 文件 (可执行文件、库或目标文件) 中解析 DWARF 调试符号。
它与 `dwarfdump` 类似，但速度快得多 (每秒数百 MB) ，并且支持 SQL。
它会为 `.debug_info` 部分中的每个调试信息条目 (DIE) 输出一行，
还会包含 DWARF 编码中用于终止树状结构里子节点列表的 “null” 条目。

:::info
`.debug_info` 由 *units* 组成，它们对应编译单元：

* 每个 unit 都是一棵由 *DIE* 构成的树，并以 `compile_unit` DIE 作为根。
* 每个 DIE 都有一个 *标签* 和一个 *属性* 列表。
* 每个属性都有一个 *name* 和一个 *值* (以及一个 *form*，用于指定该值的编码方式) 。

这些 DIE 表示源代码中的各种对象，它们的 *标签* 会告诉你它具体是什么类型。例如，包括：

* 函数 (tag = `subprogram`) 
* 类/structs/枚举 (`class_type`/`structure_type`/`enumeration_type`) 
* 变量 (`variable`) 
* 函数参数 (`formal_parameter`) 。

这种树状结构反映了对应的源代码结构。例如，`class_type` DIE 可以包含表示该类方法的 `subprogram` DIE。
:::

`DWARF` 格式会输出以下列：

* `offset` - DIE 在 `.debug_info` 部分中的偏移量
* `size` - 编码后 DIE 的字节数 (包括属性) 
* `tag` - DIE 的类型；省略了惯用的 “DW&#95;TAG&#95;” 前缀
* `unit_name` - 包含此 DIE 的编译单元名称
* `unit_offset` - 包含此 DIE 的编译单元在 `.debug_info` 部分中的偏移量
* `ancestor_tags` - 当前 DIE 在树中各祖先的标签数组，顺序为从最内层到最外层
* `ancestor_offsets` - 各祖先的偏移量，与 `ancestor_tags` 一一对应
* 为方便使用，从属性数组中复制出的几个常见属性：
  * `name`
  * `linkage_name` - 经名称修饰的完全限定名称；通常只有函数才有 (但并非所有函数都有) 
  * `decl_file` - 声明此对象的源代码文件名
  * `decl_line` - 声明此对象时在源代码中的行号
* 用于描述属性的并行数组：
  * `attr_name` - 属性名称；省略了惯用的 “DW&#95;AT&#95;” 前缀
  * `attr_form` - 属性的编码和解释方式；省略了惯用的 DW&#95;FORM&#95; 前缀
  * `attr_int` - 属性的整数值；如果属性没有数值，则为 0
  * `attr_str` - 属性的字符串值；如果属性没有字符串值，则为空

<div id="example-usage">
  ## 示例用法
</div>

`DWARF` 格式可用于查找函数定义最多的编译单元 (包括模板实例化以及包含的头文件中的函数) ：

```sql title="Query"
SELECT
    unit_name,
    count() AS c
FROM file('programs/clickhouse', DWARF)
WHERE tag = 'subprogram' AND NOT has(attr_name, 'declaration')
GROUP BY unit_name
ORDER BY c DESC
LIMIT 3
```

```text title="Response"
┌─unit_name──────────────────────────────────────────────────┬─────c─┐
│ ./src/Core/Settings.cpp                                    │ 28939 │
│ ./src/AggregateFunctions/AggregateFunctionSumMap.cpp       │ 23327 │
│ ./src/AggregateFunctions/AggregateFunctionUniqCombined.cpp │ 22649 │
└────────────────────────────────────────────────────────────┴───────┘

3 rows in set. Elapsed: 1.487 sec. Processed 139.76 million rows, 1.12 GB (93.97 million rows/s., 752.77 MB/s.)
Peak memory usage: 271.92 MiB.
```

<div id="format-settings">
  ## 格式设置
</div>
