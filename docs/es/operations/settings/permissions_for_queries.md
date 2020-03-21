# Permisos para consultas {#permissions-for-queries}

Las consultas en ClickHouse se pueden dividir en varios tipos:

1.  Leer consultas de datos: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2.  Escribir consultas de datos: `INSERT`, `OPTIMIZE`.
3.  Cambiar la consulta de configuración: `SET`, `USE`.
4.  [DDL](https://en.wikipedia.org/wiki/Data_definition_language) consulta: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5.  `KILL QUERY`.

La siguiente configuración regula los permisos de usuario según el tipo de consulta:

-   [sólo lectura](#settings_readonly) — Restringe los permisos para todos los tipos de consultas excepto las consultas DDL.
-   [Método de codificación de datos:](#settings_allow_ddl) — Restringe los permisos para consultas DDL.

`KILL QUERY` se puede realizar con cualquier configuración.

## sólo lectura {#settings-readonly}

Restringe los permisos para leer datos, escribir datos y cambiar las consultas de configuración.

Vea cómo las consultas se dividen en tipos [arriba](#permissions_for_queries).

Valores posibles:

-   0 — Se permiten todas las consultas.
-   1 — Sólo se permiten consultas de datos de lectura.
-   2 — Leer datos y cambiar las consultas de configuración están permitidos.

Después de configurar `readonly = 1` el usuario no puede cambiar `readonly` y `allow_ddl` configuración en la sesión actual.

Cuando se utiliza el `GET` método en el [Interfaz HTTP](../../interfaces/http.md), `readonly = 1` se establece automáticamente. Para modificar los datos, `POST` método.

Configuración `readonly = 1` prohibir al usuario cambiar todas las configuraciones. Hay una manera de prohibir al usuario
de cambiar sólo ajustes específicos, para más detalles ver [restricciones en la configuración](constraints_on_settings.md).

Valor predeterminado: 0

## Método de codificación de datos: {#settings-allow-ddl}

Permite o niega [DDL](https://en.wikipedia.org/wiki/Data_definition_language) consulta.

Vea cómo las consultas se dividen en tipos [arriba](#permissions_for_queries).

Valores posibles:

-   0 — No se permiten consultas DDL.
-   1 — Se permiten consultas DDL.

No se puede ejecutar `SET allow_ddl = 1` si `allow_ddl = 0` para la sesión actual.

Valor predeterminado: 1

[Artículo Original](https://clickhouse.tech/docs/es/operations/settings/permissions_for_queries/) <!--hide-->
