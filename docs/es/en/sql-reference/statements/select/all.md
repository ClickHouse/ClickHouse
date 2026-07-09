---
description: 'Documentación de la cláusula ALL'
sidebar_label: 'ALL'
slug: /sql-reference/statements/select/all
title: 'Cláusula ALL'
doc_type: 'reference'
---

Si hay varias filas que coinciden en una tabla, `ALL` las devuelve todas. `SELECT ALL` es idéntico a `SELECT` sin `DISTINCT`. Si se especifican tanto `ALL` como `DISTINCT`, se producirá una excepción.

`ALL` puede especificarse dentro de las funciones de agregación, aunque no tiene ningún efecto práctico en el resultado de la consulta.

Por ejemplo:

```sql
SELECT sum(ALL number) FROM numbers(10);
```

Es equivalente a:

```sql
SELECT sum(number) FROM numbers(10);
```