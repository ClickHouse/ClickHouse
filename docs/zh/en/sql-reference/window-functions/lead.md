---
description: 'lead 窗口函数文档'
sidebar_label: 'lead'
sidebar_position: 10
slug: /sql-reference/window-functions/lead
title: 'lead'
doc_type: 'reference'
---

返回在有序的窗口帧中，相对于当前行向后偏移若干行处计算得到的值。
此函数与 [`leadInFrame`](./leadInFrame.md) 类似，但始终使用 `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` 窗口帧。

**语法**

```sql
lead(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

有关 window function 语法的更多详细信息，请参见：[Window Functions - Syntax](./index.md/#syntax)。

**参数**

* `x` — 列名。
* `offset` — 要应用的偏移量。[(U)Int*](../data-types/int-uint.md)。 (可选，默认值为 `1`。) 
* `default` — 如果计算出的行超出窗口帧边界时返回的值。 (可选，省略时使用列类型的默认值。) 

**返回值**

* 在有序窗口帧中，当前行之后偏移相应行数处计算得到的值。

**示例**

此示例查看诺贝尔奖获奖者的[历史数据](https://www.kaggle.com/datasets/sazidthe1/nobel-prize-data)，并使用 `lead` 函数返回物理学类别中连续获奖者的列表。

```sql title="Query"
CREATE OR REPLACE VIEW nobel_prize_laureates
AS SELECT *
FROM file('nobel_laureates_data.csv');
```

```sql title="Query"
SELECT
    fullName,
    lead(year, 1, year) OVER (PARTITION BY category ORDER BY year ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS year,
    category,
    motivation
FROM nobel_prize_laureates
WHERE category = 'physics'
ORDER BY year DESC
LIMIT 9
```

```response title="Query"
   ┌─fullName─────────┬─year─┬─category─┬─motivation─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
1. │ Anne L Huillier  │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
2. │ Pierre Agostini  │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
3. │ Ferenc Krausz    │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
4. │ Alain Aspect     │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
5. │ Anton Zeilinger  │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
6. │ John Clauser     │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
7. │ Giorgio Parisi   │ 2021 │ physics  │ for the discovery of the interplay of disorder and fluctuations in physical systems from atomic to planetary scales                │
8. │ Klaus Hasselmann │ 2021 │ physics  │ for the physical modelling of Earths climate quantifying variability and reliably predicting global warming                        │
9. │ Syukuro Manabe   │ 2021 │ physics  │ for the physical modelling of Earths climate quantifying variability and reliably predicting global warming                        │
   └──────────────────┴──────┴──────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```