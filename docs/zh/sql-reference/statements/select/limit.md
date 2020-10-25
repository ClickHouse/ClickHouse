---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_title: LIMIT
---

# 限制条款 {#limit-clause}

`LIMIT m` 允许选择第一个 `m` 结果中的行。

`LIMIT n, m` 允许选择 `m` 跳过第一个结果后的行 `n` 行。 该 `LIMIT m OFFSET n` 语法是等效的。

`n` 和 `m` 必须是非负整数。

如果没有 [ORDER BY](../../../sql-reference/statements/select/order-by.md) 子句显式排序结果，结果的行选择可能是任意的和非确定性的。
