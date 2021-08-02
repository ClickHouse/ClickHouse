---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: GraphiteMergeTree
---

# GraphiteMergeTree {#graphitemergetree}

Este motor está diseñado para el adelgazamiento y la agregación / promedio (rollup) [Grafito](http://graphite.readthedocs.io/en/latest/index.html) datos. Puede ser útil para los desarrolladores que desean usar ClickHouse como almacén de datos para Graphite.

Puede usar cualquier motor de tabla ClickHouse para almacenar los datos de Graphite si no necesita un paquete acumulativo, pero si necesita un paquete acumulativo, use `GraphiteMergeTree`. El motor reduce el volumen de almacenamiento y aumenta la eficiencia de las consultas de Graphite.

El motor hereda propiedades de [Método de codificación de datos:](mergetree.md).

## Creación de una tabla {#creating-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Vea una descripción detallada del [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) consulta.

Una tabla para los datos de grafito debe tener las siguientes columnas para los siguientes datos:

-   Nombre métrico (sensor de grafito). Tipo de datos: `String`.

-   Tiempo de medición de la métrica. Tipo de datos: `DateTime`.

-   Valor de la métrica. Tipo de datos: cualquier numérico.

-   Versión de la métrica. Tipo de datos: cualquier numérico.

    ClickHouse guarda las filas con la versión más alta o la última escrita si las versiones son las mismas. Otras filas se eliminan durante la fusión de partes de datos.

Los nombres de estas columnas deben establecerse en la configuración acumulativa.

**GraphiteMergeTree parámetros**

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

**Cláusulas de consulta**

Al crear un `GraphiteMergeTree` mesa, la misma [clausula](mergetree.md#table_engine-mergetree-creating-a-table) se requieren, como al crear un `MergeTree` tabla.

<details markdown="1">

<summary>Método obsoleto para crear una tabla</summary>

!!! attention "Atención"
    No use este método en proyectos nuevos y, si es posible, cambie los proyectos antiguos al método descrito anteriormente.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    EventDate Date,
    Path String,
    Time DateTime,
    Value <Numeric_type>,
    Version <Numeric_type>
    ...
) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
```

Todos los parámetros excepto `config_section` el mismo significado que en `MergeTree`.

-   `config_section` — Name of the section in the configuration file, where are the rules of rollup set.

</details>

## Configuración acumulativa {#rollup-configuration}

La configuración del paquete acumulativo está definida por [graphite_rollup](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-graphite) parámetro en la configuración del servidor. El nombre del parámetro podría ser cualquiera. Puede crear varias configuraciones y usarlas para diferentes tablas.

Estructura de configuración Rollup:

      required-columns
      patterns

### Columnas requeridas {#required-columns}

-   `path_column_name` — The name of the column storing the metric name (Graphite sensor). Default value: `Path`.
-   `time_column_name` — The name of the column storing the time of measuring the metric. Default value: `Time`.
-   `value_column_name` — The name of the column storing the value of the metric at the time set in `time_column_name`. Valor predeterminado: `Value`.
-   `version_column_name` — The name of the column storing the version of the metric. Default value: `Timestamp`.

### Patrón {#patterns}

Estructura del `patterns` apartado:

``` text
pattern
    regexp
    function
pattern
    regexp
    age + precision
    ...
pattern
    regexp
    function
    age + precision
    ...
pattern
    ...
default
    function
    age + precision
    ...
```

!!! warning "Atención"
    Los patrones deben ser estrictamente ordenados:

      1. Patterns without `function` or `retention`.
      1. Patterns with both `function` and `retention`.
      1. Pattern `default`.

Al procesar una fila, ClickHouse comprueba las reglas en el `pattern` apartado. Cada uno de `pattern` (incluir `default` secciones pueden contener `function` parámetro para la agregación, `retention` parámetros o ambos. Si el nombre de la métrica coincide con `regexp`, las reglas de la `pattern` sección (o secciones); de lo contrario, las reglas de la `default` sección se utilizan.

Campos para `pattern` y `default` apartado:

-   `regexp`– A pattern for the metric name.
-   `age` – The minimum age of the data in seconds.
-   `precision`– How precisely to define the age of the data in seconds. Should be a divisor for 86400 (seconds in a day).
-   `function` – The name of the aggregating function to apply to data whose age falls within the range `[age, age + precision]`.

### Ejemplo de configuración {#configuration-example}

``` xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <regexp>click_cost</regexp>
        <function>any</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup>
```

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/graphitemergetree/) <!--hide-->
