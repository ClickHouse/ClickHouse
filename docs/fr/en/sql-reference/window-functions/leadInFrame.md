---
description: 'Documentation de la fonction de fenêtre leadInFrame'
sidebar_label: 'leadInFrame'
sidebar_position: 10
slug: /sql-reference/window-functions/leadInFrame
title: 'leadInFrame'
doc_type: 'reference'
---

Renvoie une valeur évaluée sur la ligne située à `offset` lignes après la ligne en cours dans le cadre ordonné de la fenêtre.

:::warning
Le comportement de `leadInFrame` diffère de celui de la fonction de fenêtre SQL standard `lead`.
La fonction de fenêtre ClickHouse `leadInFrame` respecte le cadre de la fenêtre.
Pour obtenir un comportement identique à `lead`, utilisez `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
:::

**Syntaxe**

```sql
leadInFrame(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

Pour plus de détails sur la syntaxe des fonctions de fenêtre, voir : [Window Functions - Syntax](./index.md/#syntax).

**Paramètres**

* `x` — Nom de la colonne.
* `offset` — Décalage à appliquer. [(U)Int*](../data-types/int-uint.md). (Facultatif - `1` par défaut).
* `default` — Valeur à renvoyer si la ligne calculée dépasse les limites du cadre de fenêtre. (Facultatif - valeur par défaut du type de la colonne lorsqu&#39;il est omis).

**Valeur renvoyée**

* valeur évaluée sur la ligne située `offset` lignes après la ligne en cours dans le cadre ordonné.

**Exemple**

Cet exemple examine des [données historiques](https://www.kaggle.com/datasets/sazidthe1/nobel-prize-data) sur les lauréats du prix Nobel et utilise la fonction `leadInFrame` pour renvoyer une liste de lauréats successifs dans la catégorie de physique.

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