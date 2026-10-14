---
description: 'Documentation for the leadInFrame window function'
sidebar_label: 'leadInFrame'
sidebar_position: 10
slug: /sql-reference/window-functions/leadInFrame
title: 'leadInFrame'
doc_type: 'reference'
---

Returns a value evaluated at the row that is offset rows after the current row within the ordered frame.

:::warning
`leadInFrame` behavior differs from the standard SQL `lead` window function.
Clickhouse window function `leadInFrame` respects the window frame.
To get behavior identical to the `lead`, use `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
:::

**Syntax**

```sql
leadInFrame(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

For more detail on window function syntax see: [Window Functions - Syntax](./index.md/#syntax).

**Parameters**
- `x` — Column name.
- `offset` — Offset to apply. [(U)Int*](../data-types/int-uint.md). (Optional - `1` by default).
- `default` — Value to return if calculated row exceeds the boundaries of the window frame. (Optional - default value of column type when omitted).

**Returned value**

- value evaluated at the row that is offset rows after the current row within the ordered frame.

**Example**

This example looks at [historical data](https://www.kaggle.com/datasets/sazidthe1/nobel-prize-data) for Nobel Prize winners and uses the `leadInFrame` function to return a list of successive winners in the physics category.

```sql title="Query"
CREATE OR REPLACE VIEW nobel_prize_laureates
AS SELECT *
FROM file('nobel_laureates_data.csv');
```

```sql title="Query"
SELECT
    fullName,
    leadInFrame(year, 1, year) OVER (PARTITION BY category ORDER BY year ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS year,
    category,
    motivation
FROM nobel_prize_laureates
WHERE category = 'physics'
ORDER BY year DESC
LIMIT 9
```

```response title="Response"
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
