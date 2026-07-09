---
description: 'Agrega arrays en un array más grande compuesto por esos arrays.'
keywords: ['groupArrayArray', 'array_concat_agg']
slug: /sql-reference/aggregate-functions/reference/grouparrayarray
title: 'groupArrayArray'
doc_type: 'reference'
---

Agrega arrays en un array más grande compuesto por esos arrays.
Combina la función [`groupArray`](/es/sql-reference/aggregate-functions/reference/grouparray) con el combinador [Array](/es/sql-reference/aggregate-functions/combinators#-array).

Alias: `array_concat_agg`

**Ejemplo**

Tenemos datos que capturan sesiones de navegación de usuarios. Cada sesión registra la secuencia de páginas que visitó un usuario específico.
Podemos usar la función `groupArrayArray` para analizar los patrones de visita de páginas de cada usuario.

```sql title="Setup"
CREATE TABLE website_visits (
    user_id UInt32,
    session_id UInt32,
    page_visits Array(String)
) ENGINE = Memory;

INSERT INTO website_visits VALUES
(101, 1, ['homepage', 'products', 'checkout']),
(101, 2, ['search', 'product_details', 'contact']),
(102, 1, ['homepage', 'about_us']),
(101, 3, ['blog', 'homepage']),
(102, 2, ['products', 'product_details', 'add_to_cart', 'checkout']);
```

```sql title="Query"
SELECT
    user_id,
    groupArrayArray(page_visits) AS user_session_page_sequences
FROM website_visits
GROUP BY user_id;
```

```sql title="Response"
   ┌─user_id─┬─user_session_page_sequences───────────────────────────────────────────────────────────────┐
1. │     101 │ ['homepage','products','checkout','search','product_details','contact','blog','homepage'] │
2. │     102 │ ['homepage','about_us','products','product_details','add_to_cart','checkout']             │
   └─────────┴───────────────────────────────────────────────────────────────────────────────────────────┘
```