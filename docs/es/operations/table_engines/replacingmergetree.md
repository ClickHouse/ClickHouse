---
machine_translated: true
---

# ReplacingMergeTree {#replacingmergetree}

El motor difiere de [Método de codificación de datos:](mergetree.md#table_engines-mergetree) en que elimina las entradas duplicadas con el mismo valor de clave principal (o más exactamente, con el mismo [clave de clasificación](mergetree.md) de valor).

La desduplicación de datos solo se produce durante una fusión. La fusión se produce en segundo plano en un momento desconocido, por lo que no se puede planificar para ello. Algunos de los datos pueden permanecer sin procesar. Aunque puede ejecutar una fusión no programada utilizando el `OPTIMIZE` Consulta, no cuente con su uso, porque el `OPTIMIZE` consulta leerá y escribirá una gran cantidad de datos.

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

Para obtener una descripción de los parámetros de solicitud, consulte [descripción de la solicitud](../../query_language/create.md).

**ReplacingMergeTree Parámetros**

-   `ver` — columna con versión. Tipo `UInt*`, `Date` o `DateTime`. Parámetro opcional.

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

[Artículo Original](https://clickhouse.tech/docs/es/operations/table_engines/replacingmergetree/) <!--hide-->
