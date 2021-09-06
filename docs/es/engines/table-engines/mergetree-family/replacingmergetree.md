---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: ReplacingMergeTree
---

# ReplacingMergeTree {#replacingmergetree}

El motor difiere de [Método de codificación de datos:](mergetree.md#table_engines-mergetree) en que elimina las entradas duplicadas con el mismo valor de clave principal (o más exactamente, con el mismo [clave de clasificación](mergetree.md) valor).

La desduplicación de datos solo se produce durante una fusión. La fusión ocurre en segundo plano en un momento desconocido, por lo que no puede planificarla. Algunos de los datos pueden permanecer sin procesar. Aunque puede ejecutar una fusión no programada utilizando el `OPTIMIZE` consulta, no cuente con usarlo, porque el `OPTIMIZE` consulta leerá y escribirá una gran cantidad de datos.

Así, `ReplacingMergeTree` es adecuado para borrar datos duplicados en segundo plano para ahorrar espacio, pero no garantiza la ausencia de duplicados.

## Creación de una tabla {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Para obtener una descripción de los parámetros de solicitud, consulte [descripción de la solicitud](../../../sql-reference/statements/create.md).

**ReplacingMergeTree Parámetros**

-   `ver` — column with version. Type `UInt*`, `Date` o `DateTime`. Parámetro opcional.

    Al fusionar, `ReplacingMergeTree` de todas las filas con la misma clave primaria deja solo una:

    -   Último en la selección, si `ver` no establecido.
    -   Con la versión máxima, si `ver` indicado.

**Cláusulas de consulta**

Al crear un `ReplacingMergeTree` mesa de la misma [clausula](mergetree.md) se requieren, como al crear un `MergeTree` tabla.

<details markdown="1">

<summary>Método obsoleto para crear una tabla</summary>

!!! attention "Atención"
    No use este método en proyectos nuevos y, si es posible, cambie los proyectos antiguos al método descrito anteriormente.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
```

Todos los parámetros excepto `ver` el mismo significado que en `MergeTree`.

-   `ver` - columna con la versión. Parámetro opcional. Para una descripción, vea el texto anterior.

</details>

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/replacingmergetree/) <!--hide-->
