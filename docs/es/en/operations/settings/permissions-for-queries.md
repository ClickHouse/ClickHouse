---
description: 'Configuración de permisos para consultas.'
sidebar_label: 'Permisos para consultas'
sidebar_position: 58
slug: /operations/settings/permissions-for-queries
title: 'Permisos para consultas'
doc_type: 'reference'
---

Las consultas en ClickHouse se pueden dividir en varios tipos:

1. Consultas de lectura de datos: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2. Consultas de escritura de datos: `INSERT`, `OPTIMIZE`.
3. Consultas de cambio de configuración: `SET`, `USE`.
4. Consultas [DDL](https://en.wikipedia.org/wiki/Data_definition_language): `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5. `KILL QUERY`.

La siguiente configuración regula los permisos del usuario según el tipo de consulta:

<div id="readonly">
  ## readonly
</div>

Restringe los permisos para las consultas de lectura de datos, escritura de datos y cambio de configuración.

Cuando se establece en 1, permite:

* Todos los tipos de consultas de lectura (como SELECT y consultas equivalentes).
* Consultas que solo modifican el contexto de la sesión (como USE).

Cuando se establece en 2, permite lo anterior más:

* SET y CREATE TEMPORARY TABLE

  :::tip
  Las consultas como EXISTS, DESCRIBE, EXPLAIN, SHOW PROCESSLIST, etc. son equivalentes a SELECT, porque solo hacen un select de tablas del sistema.
  :::

Valores posibles:

* 0 — Se permiten consultas de lectura de datos, escritura de datos y cambio de configuración.
* 1 — Solo se permiten consultas de lectura de datos.
* 2 — Se permiten consultas de lectura de datos y cambio de configuración.

Valor predeterminado: 0

:::note
Después de establecer `readonly = 1`, el usuario no puede cambiar la configuración de `readonly` ni la de `allow_ddl` en la sesión actual.

Al usar el método `GET` en la [interfaz HTTP](/es/interfaces/http), `readonly = 1` se establece automáticamente. Para modificar datos, use el método `POST`.

Establecer `readonly = 1` impide que el usuario cambie configuraciones. Existe una forma de impedir que el usuario cambie solo determinadas configuraciones. También existe una forma de permitir cambiar solo determinadas configuraciones bajo las restricciones de `readonly = 1`. Para obtener más detalles, consulte [restricciones de configuración](../../operations/settings/constraints-on-settings.md).
:::

<div id="allow_ddl">
  ## allow_ddl
</div>

Permite o deniega las consultas [DDL](https://en.wikipedia.org/wiki/Data_definition_language).

Valores posibles:

* 0 — las consultas DDL no están permitidas.
* 1 — las consultas DDL están permitidas.

Valor predeterminado: 1

:::note
No se puede ejecutar `SET allow_ddl = 1` si `allow_ddl = 0` en la sesión actual.
:::

:::note KILL QUERY
`KILL QUERY` se puede ejecutar con cualquier combinación de ajustes de readonly y allow&#95;ddl.
:::