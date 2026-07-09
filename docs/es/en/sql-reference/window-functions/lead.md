---
description: 'Documentación de la función de ventana lead'
sidebar_label: 'lead'
sidebar_position: 10
slug: /sql-reference/window-functions/lead
title: 'lead'
doc_type: 'reference'
---

Devuelve un valor evaluado en la fila situada OFFSET filas después de la fila actual dentro del marco ordenado.
Esta función es similar a [`leadInFrame`](./leadInFrame.md), pero siempre usa el marco `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

**Sintaxis**

```sql
lead(x[, offset[, default]])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

Para obtener más información sobre la sintaxis de las funciones de ventana, consulte: [Funciones de ventana - Sintaxis](./index.md/#syntax).

**Parámetros**

* `x` — Nombre de la columna.
* `offset` — Desplazamiento que se aplica. [(U)Int*](../data-types/int-uint.md). (Opcional: `1` de forma predeterminada).
* `default` — Valor que se devolverá si la fila calculada excede los límites del marco de ventana. (Opcional: valor predeterminado del tipo de columna cuando se omite).

**Valor devuelto**

* valor evaluado en la fila situada `offset` filas después de la fila actual dentro del marco ordenado.

**Ejemplo**

Este ejemplo analiza [datos históricos](https://www.kaggle.com/datasets/sazidthe1/nobel-prize-data) de ganadores del Premio Nobel y utiliza la función `lead` para devolver una lista de ganadores consecutivos en la categoría de física.

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