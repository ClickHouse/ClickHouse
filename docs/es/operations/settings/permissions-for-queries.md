---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 58
toc_title: Permisos para consultas
---

# Permisos para consultas {#permissions_for_queries}

Las consultas en ClickHouse se pueden dividir en varios tipos:

1.  Leer consultas de datos: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2.  Escribir consultas de datos: `INSERT`, `OPTIMIZE`.
3.  Cambiar la consulta de configuración: `SET`, `USE`.
4.  [DDL](https://en.wikipedia.org/wiki/Data_definition_language) consulta: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5.  `KILL QUERY`.

La siguiente configuración regula los permisos de usuario según el tipo de consulta:

-   [sólo lectura](#settings_readonly) — Restricts permissions for all types of queries except DDL queries.
-   [Método de codificación de datos:](#settings_allow_ddl) — Restricts permissions for DDL queries.

`KILL QUERY` se puede realizar con cualquier configuración.

## sólo lectura {#settings_readonly}

Restringe los permisos para leer datos, escribir datos y cambiar las consultas de configuración.

Vea cómo las consultas se dividen en tipos [arriba](#permissions_for_queries).

Valores posibles:

-   0 — All queries are allowed.
-   1 — Only read data queries are allowed.
-   2 — Read data and change settings queries are allowed.

Después de configurar `readonly = 1` el usuario no puede cambiar `readonly` y `allow_ddl` configuración en la sesión actual.

Cuando se utiliza el `GET` método en el [Interfaz HTTP](../../interfaces/http.md), `readonly = 1` se establece automáticamente. Para modificar los datos, `POST` método.

Configuración `readonly = 1` prohibir al usuario cambiar todas las configuraciones. Hay una manera de prohibir al usuario
de cambiar sólo ajustes específicos, para más detalles ver [restricciones en la configuración](constraints-on-settings.md).

Valor predeterminado: 0

## Método de codificación de datos: {#settings_allow_ddl}

Permite o niega [DDL](https://en.wikipedia.org/wiki/Data_definition_language) consulta.

Vea cómo las consultas se dividen en tipos [arriba](#permissions_for_queries).

Valores posibles:

-   0 — DDL queries are not allowed.
-   1 — DDL queries are allowed.

No se puede ejecutar `SET allow_ddl = 1` si `allow_ddl = 0` para la sesión actual.

Valor predeterminado: 1

[Artículo Original](https://clickhouse.tech/docs/en/operations/settings/permissions_for_queries/) <!--hide-->
