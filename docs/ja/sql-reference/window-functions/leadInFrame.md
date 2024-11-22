---
slug: /ja/sql-reference/window-functions/leadInFrame
sidebar_label: leadInFrame
sidebar_position: 10
---

# leadInFrame

順序付けられたフレーム内で、現在の行の後にあるオフセット行に評価された値を返します。

**構文**

```sql
leadInFrame(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

ウィンドウ関数の構文の詳細については、[ウィンドウ関数 - 構文](./index.md/#syntax) を参照してください。

**パラメーター**
- `x` — カラム名。
- `offset` — 適用するオフセット。[(U)Int*](../data-types/int-uint.md)。 （省略可能 - デフォルトは `1`）。
- `default` — 計算された行がウィンドウフレームの境界を超えた場合に返す値。 （省略可能 - 省略時はカラムタイプのデフォルト値）。

**返される値**

- 順序付けられたフレーム内で、現在の行の後にあるオフセット行に評価された値。

**例**

この例では、ノーベル賞受賞者の[歴史データ](https://www.kaggle.com/datasets/sazidthe1/nobel-prize-data)を調査し、`leadInFrame` 関数を使用して物理学カテゴリーの連続した受賞者のリストを返します。

クエリ:

```sql
CREATE OR REPLACE VIEW nobel_prize_laureates AS FROM file('nobel_laureates_data.csv') SELECT *;
```

```sql
FROM nobel_prize_laureates SELECT fullName, leadInFrame(year, 1, year) OVER (PARTITION BY category ORDER BY year) AS year, category, motivation WHERE category == 'physics' ORDER BY year DESC LIMIT 9;
```

結果:

```response
   ┌─fullName─────────┬─year─┬─category─┬─motivation─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
1. │ Pierre Agostini  │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
2. │ Ferenc Krausz    │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
3. │ Anne L Huillier  │ 2023 │ physics  │ for experimental methods that generate attosecond pulses of light for the study of electron dynamics in matter                     │
4. │ Alain Aspect     │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
5. │ Anton Zeilinger  │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
6. │ John Clauser     │ 2022 │ physics  │ for experiments with entangled photons establishing the violation of Bell inequalities and  pioneering quantum information science │
7. │ Syukuro Manabe   │ 2021 │ physics  │ for the physical modelling of Earths climate quantifying variability and reliably predicting global warming                        │
8. │ Klaus Hasselmann │ 2021 │ physics  │ for the physical modelling of Earths climate quantifying variability and reliably predicting global warming                        │
9. │ Giorgio Parisi   │ 2021 │ physics  │ for the discovery of the interplay of disorder and fluctuations in physical systems from atomic to planetary scales                │
   └──────────────────┴──────┴──────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```
