---
description: 'Documentación de la sentencia USE'
sidebar_label: 'USE'
sidebar_position: 53
slug: /sql-reference/statements/use
title: 'Sentencia USE'
doc_type: 'reference'
---

```sql
USE [DATABASE] db
```

Permite establecer la base de datos actual de la sesión.

La base de datos actual se usa para buscar tablas si la base de datos no se define explícitamente en la consulta mediante un punto antes del nombre de la tabla.

Esta consulta no se puede realizar al usar el protocolo HTTP, ya que no existe el concepto de sesión.