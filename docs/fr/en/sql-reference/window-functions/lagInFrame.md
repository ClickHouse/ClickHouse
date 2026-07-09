---
description: 'Documentation de la fonction de fenêtre lagInFrame'
sidebar_label: 'lagInFrame'
sidebar_position: 9
slug: /sql-reference/window-functions/lagInFrame
title: 'lagInFrame'
doc_type: 'reference'
---

Renvoie une valeur évaluée sur la ligne située à un décalage physique spécifié avant la ligne en cours au sein du frame ordonné.

:::warning
Le comportement de `lagInFrame` diffère de celui de la fonction de fenêtre SQL standard `lag`.
La fonction de fenêtre ClickHouse `lagInFrame` tient compte du frame de la fenêtre.
Pour obtenir un comportement identique à `lag`, utilisez `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
:::

**Syntaxe**

```sql
lagInFrame(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

Pour plus de détails sur la syntaxe des fonctions de fenêtre, voir : [Window Functions - Syntax](./index.md/#syntax).

**Paramètres**

* `x` — Nom de la colonne.
* `offset` — Décalage à appliquer. [(U)Int*](../data-types/int-uint.md). (Facultatif - `1` par défaut).
* `default` — Valeur à renvoyer si la ligne calculée dépasse les limites de la fenêtre. (Facultatif - valeur par défaut du type de colonne si omis).

**Valeur renvoyée**

* Valeur évaluée sur la ligne située à un décalage physique donné avant la ligne en cours dans la fenêtre ordonnée.

**Exemple**

Cet exemple examine les données historiques d&#39;une action donnée et utilise la fonction `lagInFrame` pour calculer un delta d&#39;un jour à l&#39;autre ainsi que la variation en pourcentage du cours de clôture de l&#39;action.

```sql title="Query"
CREATE TABLE stock_prices
(
    `date`   Date,
    `open`   Float32, -- opening price
    `high`   Float32, -- daily high
    `low`    Float32, -- daily low
    `close`  Float32, -- closing price
    `volume` UInt32   -- trade volume
)
Engine = Memory;

INSERT INTO stock_prices FORMAT Values
    ('2024-06-03', 113.62, 115.00, 112.00, 115.00, 438392000),
    ('2024-06-04', 115.72, 116.60, 114.04, 116.44, 403324000),
    ('2024-06-05', 118.37, 122.45, 117.47, 122.44, 528402000),
    ('2024-06-06', 124.05, 125.59, 118.32, 121.00, 664696000),
    ('2024-06-07', 119.77, 121.69, 118.02, 120.89, 412386000);
```

```sql title="Query"
SELECT
    date,
    close,
    lagInFrame(close, 1, close) OVER (ORDER BY date ASC
       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
     ) AS previous_day_close,
    COALESCE(ROUND(close - previous_day_close, 2)) AS delta,
    COALESCE(ROUND((delta / previous_day_close) * 100, 2)) AS percent_change
FROM stock_prices
ORDER BY date DESC
```

```response title="Response"
   ┌───────date─┬──close─┬─previous_day_close─┬─delta─┬─percent_change─┐
1. │ 2024-06-07 │ 120.89 │                121 │ -0.11 │          -0.09 │
2. │ 2024-06-06 │    121 │             122.44 │ -1.44 │          -1.18 │
3. │ 2024-06-05 │ 122.44 │             116.44 │     6 │           5.15 │
4. │ 2024-06-04 │ 116.44 │                115 │  1.44 │           1.25 │
5. │ 2024-06-03 │    115 │                115 │     0 │              0 │
   └────────────┴────────┴────────────────────┴───────┴────────────────┘
```