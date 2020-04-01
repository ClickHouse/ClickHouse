---
machine_translated: true
---

# GraphiteMergeTree {#graphitemergetree}

Este motor está diseñado para el adelgazamiento y la agregación / promedio (rollup) [Grafito](http://graphite.readthedocs.io/en/latest/index.html) datos. Puede ser útil para los desarrolladores que desean usar ClickHouse como almacén de datos para Graphite.

Puede utilizar cualquier motor de tabla ClickHouse para almacenar los datos de grafito si no necesita un paquete acumulativo, pero si necesita un paquete acumulativo, use `GraphiteMergeTree`. El motor reduce el volumen de almacenamiento y aumenta la eficiencia de las consultas de Grafito.

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

Vea una descripción detallada del [CREAR TABLA](../../query_language/create.md#create-table-query) consulta.

Una tabla para los datos de grafito debe tener las siguientes columnas para los siguientes datos:

-   Nombre métrico (sensor de grafito). Tipo de datos: `String`.

-   Tiempo de medición de la métrica. Tipo de datos: `DateTime`.

-   Valor de la métrica. Tipo de datos: cualquier numérico.

-   Versión de la métrica. Tipo de datos: cualquier numérico.

    ClickHouse guarda las filas con la versión más alta o la última escrita si las versiones son las mismas. Otras filas se eliminan durante la fusión de partes de datos.

Los nombres de estas columnas deben establecerse en la configuración acumulativa.

**GraphiteMergeTree parámetros**

-   `config_section` — Nombre de la sección en el archivo de configuración, donde se establecen las reglas de acumulación.

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

-   `config_section` — Nombre de la sección en el archivo de configuración, donde se establecen las reglas de acumulación.

</details>

## Configuración acumulativa {#rollup-configuration}

La configuración del paquete acumulativo está definida por [graphite\_rollup](../server_settings/settings.md#server_settings-graphite_rollup) parámetro en la configuración del servidor. El nombre del parámetro podría ser cualquiera. Puede crear varias configuraciones y usarlas para diferentes tablas.

Estructura de configuración Rollup:

      required-columns
      patterns

### Columnas requeridas {#required-columns}

-   `path_column_name` — El nombre de la columna que almacena el nombre de la métrica (sensor de grafito). Valor predeterminado: `Path`.
-   `time_column_name` — El nombre de la columna que almacena el tiempo de medición de la métrica. Valor predeterminado: `Time`.
-   `value_column_name` — El nombre de la columna que almacena el valor de la métrica a la hora establecida en `time_column_name`. Valor predeterminado: `Value`.
-   `version_column_name` — El nombre de la columna que almacena la versión de la métrica. Valor predeterminado: `Timestamp`.

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

-   `regexp`– Un patrón para el nombre de la métrica.
-   `age` – La edad mínima de los datos en segundos.
-   `precision`– Cómo definir con precisión la edad de los datos en segundos. Debe ser un divisor para 86400 (segundos en un día).
-   `function` – El nombre de la función de agregación que se aplicará a los datos cuya antigüedad se encuentra dentro del intervalo `[age, age + precision]`.

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

[Artículo Original](https://clickhouse.tech/docs/es/operations/table_engines/graphitemergetree/) <!--hide-->
