---
description: 'Se utiliza con fines de prueba como el método más rápido para generar muchas filas.
  Similar a las tablas del sistema `system.zeros` y `system.zeros_mt`.'
sidebar_label: 'zeros'
sidebar_position: 145
slug: /sql-reference/table-functions/zeros
title: 'zeros'
doc_type: 'reference'
---

* `zeros(N)` – Devuelve una tabla con una única columna, &#39;zero&#39; (UInt8), que contiene el entero 0 `N` veces
* `zeros_mt(N)` – Igual que `zeros`, pero utiliza múltiples hilos.

Esta función se utiliza con fines de prueba como el método más rápido para generar muchas filas. Es similar a las tablas del sistema `system.zeros` y `system.zeros_mt`.

Las siguientes consultas son equivalentes:

```sql
SELECT * FROM zeros(10);
SELECT * FROM system.zeros LIMIT 10;
SELECT * FROM zeros_mt(10);
SELECT * FROM system.zeros_mt LIMIT 10;
```

```response
┌─zero─┐
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
└──────┘
```