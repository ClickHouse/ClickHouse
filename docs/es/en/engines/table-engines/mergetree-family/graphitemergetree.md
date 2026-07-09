---
description: 'Diseñado para reducir y agregar/promediar (rollup) datos de Graphite.'
sidebar_label: 'GraphiteMergeTree'
sidebar_position: 90
slug: /engines/table-engines/mergetree-family/graphitemergetree
title: 'motor de tabla GraphiteMergeTree'
doc_type: 'guide'
---

Este motor está diseñado para reducir y agregar/promediar (rollup) datos de [Graphite](http://graphite.readthedocs.io/en/latest/index.html). Puede ser útil para los desarrolladores que quieran usar ClickHouse como almacén de datos para Graphite.

Puede usar cualquier motor de tabla de ClickHouse para almacenar datos de Graphite si no necesita rollup, pero si necesita rollup, use `GraphiteMergeTree`. Este motor reduce el volumen de almacenamiento y aumenta la eficiencia de las consultas de Graphite.

Este motor hereda las propiedades de [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md).

<div id="creating-table">
  ## Creación de una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    Path String,
    Time DateTime,
    Value Float64,
    Version <Numeric_type>
    ...
) ENGINE = GraphiteMergeTree(config_section)
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Consulte una descripción detallada de la consulta [CREATE TABLE](/es/sql-reference/statements/create/table).

Una tabla para datos de Graphite debe tener las siguientes columnas:

* Nombre de la métrica (Graphite sensor). Tipo de dato: `String`.

* Momento de medición de la métrica. Tipo de dato: `DateTime`.

* Valor de la métrica. Tipo de dato: `Float64`.

* Versión de la métrica. Tipo de dato: cualquier tipo numérico (ClickHouse guarda las filas con la versión más alta o la última escrita si las versiones son iguales. Las demás filas se eliminan durante la fusión de las partes de datos).

Los nombres de estas columnas deben establecerse en la configuración de rollup.

**Parámetros de GraphiteMergeTree**

* `config_section` — Nombre de la sección del archivo de configuración donde se establecen las reglas de rollup.

**Cláusulas de la consulta**

Al crear una tabla `GraphiteMergeTree`, se requieren las mismas [cláusulas](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) que al crear una tabla `MergeTree`.

<details markdown="1">
  <summary>Método obsoleto para crear una tabla</summary>

  :::note
  No utilice este método en proyectos nuevos y, si es posible, cambie los proyectos antiguos al método descrito anteriormente.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      EventDate Date,
      Path String,
      Time DateTime,
      Value Float64,
      Version <Numeric_type>
      ...
  ) ENGINE [=] GraphiteMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, config_section)
  ```

  Todos los parámetros, excepto `config_section`, tienen el mismo significado que en `MergeTree`.

  * `config_section` — Nombre de la sección del archivo de configuración donde se establecen las reglas de rollup.
</details>

<div id="rollup-configuration">
  ## Configuración de rollup
</div>

Los ajustes de rollup se definen mediante el parámetro [graphite&#95;rollup](../../../operations/server-configuration-parameters/settings.md#graphite) en la configuración del servidor. El nombre del parámetro puede ser cualquiera. Puede crear varias configuraciones y usarlas para distintas tablas.

Estructura de la configuración de rollup:

columnas-obligatorias
patrones

<div id="required-columns">
  ### Columnas requeridas
</div>

<div id="path_column_name">
  #### `path_column_name`
</div>

`path_column_name` — Nombre de la columna que almacena el nombre de la métrica (Graphite sensor). Valor predeterminado: `Path`.

<div id="time_column_name">
  #### `time_column_name`
</div>

`time_column_name` — El nombre de la columna que almacena la hora de medición de la métrica. Valor predeterminado: `Time`.

<div id="value_column_name">
  #### `value_column_name`
</div>

`value_column_name` — El nombre de la columna que almacena el valor de la métrica en el instante indicado en `time_column_name`. Valor predeterminado: `Value`.

<div id="version_column_name">
  #### `version_column_name`
</div>

`version_column_name` — Nombre de la columna que almacena la versión de la métrica. Valor predeterminado: `Timestamp`.

<div id="patterns">
  ### Patrones
</div>

Estructura de la sección `patterns`:

```text
pattern
    rule_type
    regexp
    function
pattern
    rule_type
    regexp
    age + precision
    ...
pattern
    rule_type
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

:::important
Los patrones deben estar estrictamente ordenados:

1. Patrones sin `function` ni `retention`.
2. Patrones con `function` y `retention`.
3. Patrón `default`.
   :::

Al procesar una fila, ClickHouse comprueba las reglas de las secciones `pattern`. Cada una de las secciones `pattern` (incluida `default`) puede contener el parámetro `function` para la agregación, los parámetros `retention` o ambos. Si el nombre de la métrica coincide con `regexp`, se aplican las reglas de la sección `pattern` (o de las secciones correspondientes); de lo contrario, se usan las reglas de la sección `default`.

Campos de las secciones `pattern` y `default`:

* `rule_type` - tipo de regla. Se aplica solo a determinadas métricas. El motor lo usa para separar las métricas simples de las etiquetadas. Parámetro opcional. Valor predeterminado: `all`.
  No es necesario cuando el rendimiento no es crítico o cuando solo se usa un tipo de métricas, por ejemplo, métricas simples. De forma predeterminada, solo se crea un conjunto de reglas. En cambio, si se define cualquiera de los tipos especiales, se crean dos conjuntos distintos: uno para métricas simples (root.branch.leaf) y otro para métricas etiquetadas (root.branch.leaf;tag1=value1).
  Las reglas predeterminadas acaban en ambos conjuntos.
  Valores válidos:
  * `all` (predeterminado) - una regla universal, usada cuando se omite `rule_type`.
  * `plain` - una regla para métricas simples. El campo `regexp` se procesa como una expresión regular.
  * `tagged` - una regla para métricas etiquetadas (las métricas se almacenan en la base de datos con el formato `someName?tag1=value1&tag2=value2&tag3=value3`). La expresión regular debe estar ordenada por nombre de etiqueta; la primera etiqueta debe ser `__name__`, si existe. El campo `regexp` se procesa como una expresión regular.
  * `tag_list` - una regla para métricas etiquetadas; es un DSL simple para facilitar la descripción de métricas en formato graphite `someName;tag1=value1;tag2=value2`, `someName` o `tag1=value1;tag2=value2`. El campo `regexp` se convierte en una regla `tagged`. No es necesario ordenar por nombre de etiqueta, ya que se hará automáticamente. El valor de una etiqueta (pero no el nombre) puede definirse como una expresión regular, por ejemplo, `env=(dev|staging)`.
* `regexp` – Patrón para el nombre de la métrica (expresión regular o DSL).
* `age` – Antigüedad mínima de los datos en segundos.
* `precision`– Precisión con la que se define la antigüedad de los datos en segundos. Debe ser un divisor de 86400 (segundos en un día).
* `function` – Nombre de la función de agregación que debe aplicarse a los datos cuya antigüedad esté dentro del intervalo `[age, age + precision]`. Funciones aceptadas: min / max / any / avg. La media se calcula de forma imprecisa, como la media de las medias.

<div id="configuration-example">
  ### Ejemplo de configuración sin tipos de reglas
</div>

```xml
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

<div id="configuration-typed-example">
  ### Ejemplo de configuración con tipos de reglas
</div>

```xml
<graphite_rollup>
    <version_column_name>Version</version_column_name>
    <pattern>
        <rule_type>plain</rule_type>
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
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp>^((.*)|.)min\?</regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tagged</rule_type>
        <regexp><![CDATA[^someName\?(.*&)*tag1=value1(&|$)]]></regexp>
        <function>min</function>
        <retention>
            <age>0</age>
            <precision>5</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>60</precision>
        </retention>
    </pattern>
    <pattern>
        <rule_type>tag_list</rule_type>
        <regexp>someName;tag2=value2</regexp>
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

:::note
El rollup de datos se realiza durante las fusiones. Normalmente, las fusiones no se inician para las particiones antiguas, por lo que, para aplicar el rollup, es necesario forzar una fusión no programada mediante [optimize](../../../sql-reference/statements/optimize.md). También puede usar herramientas adicionales, por ejemplo, [graphite-ch-optimizer](https://github.com/innogames/graphite-ch-optimizer).
:::