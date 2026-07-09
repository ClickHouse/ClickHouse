---
alias: []
description: 'Documentación sobre el formato DWARF'
input_format: true
keywords: ['DWARF']
output_format: false
slug: /interfaces/formats/DWARF
title: 'DWARF'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✗      |       |

<div id="description">
  ## Descripción
</div>

El formato `DWARF` analiza símbolos de depuración DWARF de un archivo ELF (ejecutable, biblioteca o archivo objeto).
Es similar a `dwarfdump`, pero mucho más rápido (cientos de MB/s) y admite SQL.
Produce una fila por cada Debug Information Entry (DIE) en la sección `.debug_info`
e incluye entradas &quot;null&quot; que la codificación DWARF usa para terminar listas de hijos en el árbol.

:::info
`.debug_info` consta de *unidades*, que corresponden a unidades de compilación:

* Cada unidad es un árbol de *DIE*s, con un DIE `compile_unit` como raíz.
* Cada DIE tiene una *etiqueta* y una lista de *atributos*.
* Cada atributo tiene un *nombre* y un *valor* (y también una *forma*, que especifica cómo se codifica el valor).

Los DIE representan elementos del código fuente, y su *etiqueta* indica de qué tipo de elemento se trata. Por ejemplo, hay:

* funciones (tag = `subprogram`)
* clases/structs/enums (`class_type`/`structure_type`/`enumeration_type`)
* variables (`variable`)
* argumentos de función (`formal_parameter`).

La estructura en árbol refleja el código fuente correspondiente. Por ejemplo, un DIE `class_type` puede contener DIEs `subprogram` que representan métodos de la clase.
:::

El formato `DWARF` genera las siguientes columnas:

* `offset` - posición del DIE en la sección `.debug_info`
* `size` - número de bytes del DIE codificado (incluidos los atributos)
* `tag` - tipo del DIE; se omite el prefijo convencional &quot;DW&#95;TAG&#95;&quot;
* `unit_name` - nombre de la unidad de compilación que contiene este DIE
* `unit_offset` - posición de la unidad de compilación que contiene este DIE en la sección `.debug_info`
* `ancestor_tags` - array de etiquetas de los ancestros del DIE actual en el árbol, en orden de más interno a más externo
* `ancestor_offsets` - desplazamientos de los ancestros, paralelos a `ancestor_tags`
* algunos atributos comunes duplicados del array de atributos por comodidad:
  * `name`
  * `linkage_name` - nombre completamente calificado con name mangling; normalmente solo las funciones lo tienen (pero no todas las funciones)
  * `decl_file` - nombre del archivo de código fuente donde se declaró esta entidad
  * `decl_line` - número de línea en el código fuente donde se declaró esta entidad
* arrays paralelos que describen atributos:
  * `attr_name` - nombre del atributo; se omite el prefijo convencional &quot;DW&#95;AT&#95;&quot;
  * `attr_form` - cómo se codifica e interpreta el atributo; se omite el prefijo convencional DW&#95;FORM&#95;
  * `attr_int` - valor entero del atributo; 0 si el atributo no tiene un valor numérico
  * `attr_str` - valor de cadena del atributo; vacío si el atributo no tiene un valor de cadena

<div id="example-usage">
  ## Ejemplo de uso
</div>

El formato `DWARF` puede utilizarse para encontrar las unidades de compilación que tienen el mayor número de definiciones de funciones (incluidas las instanciaciones de plantillas y las funciones de los archivos de cabecera incluidos):

```sql title="Query"
SELECT
    unit_name,
    count() AS c
FROM file('programs/clickhouse', DWARF)
WHERE tag = 'subprogram' AND NOT has(attr_name, 'declaration')
GROUP BY unit_name
ORDER BY c DESC
LIMIT 3
```

```text title="Response"
┌─unit_name──────────────────────────────────────────────────┬─────c─┐
│ ./src/Core/Settings.cpp                                    │ 28939 │
│ ./src/AggregateFunctions/AggregateFunctionSumMap.cpp       │ 23327 │
│ ./src/AggregateFunctions/AggregateFunctionUniqCombined.cpp │ 22649 │
└────────────────────────────────────────────────────────────┴───────┘

3 rows in set. Elapsed: 1.487 sec. Processed 139.76 million rows, 1.12 GB (93.97 million rows/s., 752.77 MB/s.)
Peak memory usage: 271.92 MiB.
```

<div id="format-settings">
  ## Configuración del formato
</div>
