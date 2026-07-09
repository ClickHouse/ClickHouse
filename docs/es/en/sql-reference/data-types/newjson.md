---
description: 'Documentación del tipo de datos JSON en ClickHouse, que ofrece compatibilidad nativa
  para trabajar con datos JSON'
keywords: ['json', 'tipo de datos']
sidebar_label: 'JSON'
sidebar_position: 63
slug: /sql-reference/data-types/newjson
title: 'Tipo de datos JSON'
doc_type: 'reference'
---

import {CardSecondary} from '@clickhouse/click-ui/bundled';
import WhenToUseJson from '@site/docs/best-practices/_snippets/_when-to-use-json.md';
import Link from '@docusaurus/Link'

<Link to="/docs/best-practices/use-json-where-appropriate" style={{display: 'flex', textDecoration: 'none', width: 'fit-content'}}>
  <CardSecondary badgeState="success" badgeText="" description="Consulta nuestra guía de buenas prácticas sobre JSON para ver ejemplos, características avanzadas y aspectos a tener en cuenta al usar el tipo JSON." icon="book" infoText="Leer más" infoUrl="/docs/best-practices/use-json-where-appropriate" title="¿Buscas una guía?" />
</Link>

<br />

El tipo `JSON` almacena documentos en formato JavaScript Object Notation (JSON) en una sola columna.

:::note
En ClickHouse Open-Source, el tipo de datos JSON está marcado como listo para producción en la versión 25.3. No se recomienda usar este tipo en producción en versiones anteriores.
:::

Para declarar una columna de tipo `JSON`, puedes usar la siguiente sintaxis:

```sql
<column_name> JSON
(
    max_dynamic_paths=N,
    max_dynamic_types=M,
    some.path TypeName,
    SKIP path.to.skip,
    SKIP REGEXP 'paths_regexp'
)
```

Donde los parámetros de la sintaxis anterior se definen de la siguiente manera:

| Parámetro                   | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | Valor predeterminado |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------- |
| `max_dynamic_paths`         | Un parámetro opcional que indica cuántas rutas pueden almacenarse por separado como subcolumnas dentro de un único bloque de datos almacenado por separado (por ejemplo, dentro de una única parte de datos de una tabla MergeTree). <br /><br />Si se supera este límite, todas las demás rutas se almacenarán juntas en una sola estructura llamada [datos compartidos](#shared-data-structure).<br /><br />También hay [formas](#controlling-the-number-of-dynamic-paths) de cambiar el límite de rutas dinámicas sin modificar este parámetro. | `1024`               |
| `max_dynamic_types`         | Un parámetro opcional entre `1` y `255` que indica cuántos tipos de datos distintos pueden almacenarse por separado dentro de una única columna de ruta de tipo `Dynamic` en un único bloque de datos almacenado por separado (por ejemplo, dentro de una única parte de datos de una tabla MergeTree). <br /><br />Si se supera este límite, todos los tipos nuevos se almacenarán juntos en una sola estructura llamada `shared variant`.                                                                                                        | `32`                 |
| `some.path TypeName`        | Una indicación de tipo opcional para una ruta concreta del JSON. Estas rutas siempre se almacenarán como subcolumnas con el tipo especificado.                                                                                                                                                                                                                                                                                                                                                                                                     |                      |
| `SKIP path.to.skip`         | Una indicación opcional para una ruta concreta que debe omitirse durante el análisis del JSON. Estas rutas nunca se almacenarán en la columna JSON. Si la ruta especificada es un objeto JSON anidado, se omitirá todo el objeto anidado.                                                                                                                                                                                                                                                                                                          |                      |
| `SKIP REGEXP 'path_regexp'` | Una indicación opcional con una expresión regular que se utiliza para omitir rutas durante el análisis del JSON. Todas las rutas que coincidan con esta expresión regular nunca se almacenarán en la columna JSON.                                                                                                                                                                                                                                                                                                                                 |                      |

<WhenToUseJson />

<div id="creating-json">
  ## Crear `JSON`
</div>

En esta sección veremos las distintas formas de crear `JSON`.

<div id="using-json-in-a-table-column-definition">
  ### Uso de `JSON` en la definición de una columna de una tabla
</div>

```sql title="Query (Example 1)"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 1)"
┌─json────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"]}          │
│ {"f":"Hello, World!"}                       │
│ {"a":{"b":"43","e":"10"},"c":["4","5","6"]} │
└─────────────────────────────────────────────┘
```

```sql title="Query (Example 2)"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42}, "c" : [1, 2, 3]}'), ('{"f" : "Hello, World!"}'), ('{"a" : {"b" : 43, "e" : 10}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response (Example 2)"
┌─json──────────────────────────────┐
│ {"a":{"b":42},"c":["1","2","3"]}  │
│ {"a":{"b":0},"f":"Hello, World!"} │
│ {"a":{"b":43},"c":["4","5","6"]}  │
└───────────────────────────────────┘
```

<div id="using-cast-with-json">
  ### Uso de CAST con `::JSON`
</div>

Es posible convertir varios tipos mediante la sintaxis especial `::JSON`.

<div id="cast-from-string-to-json">
  #### CAST de `String` a `JSON`
</div>

```sql title="Query"
SELECT '{"a" : {"b" : 42},"c" : [1, 2, 3], "d" : "Hello, World!"}'::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

<div id="cast-from-tuple-to-json">
  #### CAST de `Tuple` a `JSON`
</div>

```sql title="Query"
SET enable_named_columns_in_function_tuple = 1;
SELECT (tuple(42 AS b) AS a, [1, 2, 3] AS c, 'Hello, World!' AS d)::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

<div id="cast-from-map-to-json">
  #### CAST de `Map` a `JSON`
</div>

```sql title="Query"
SET use_variant_as_common_type=1;
SELECT map('a', map('b', 42), 'c', [1,2,3], 'd', 'Hello, World!')::JSON AS json;
```

```text title="Response"
┌─json───────────────────────────────────────────────────┐
│ {"a":{"b":"42"},"c":["1","2","3"],"d":"Hello, World!"} │
└────────────────────────────────────────────────────────┘
```

:::note
Las rutas JSON se almacenan de forma aplanada. Esto significa que, cuando un objeto JSON se construye a partir de una ruta como `a.b.c`,
no es posible saber si debe construirse como `{ "a.b.c" : ... }` o como `{ "a": { "b": { "c": ... } } }`.
Nuestra implementación siempre asumirá lo segundo.

Por ejemplo:

```sql title="Query"
SELECT CAST('{"a.b.c" : 42}', 'JSON') AS json
```

devolverá:

```response title="Response"
   ┌─json───────────────────┐
1. │ {"a":{"b":{"c":"42"}}} │
   └────────────────────────┘
```

y **no**:

```sql
   ┌─json───────────┐
1. │ {"a.b.c":"42"} │
   └────────────────┘
```

:::

<div id="reading-json-paths-as-sub-columns">
  ## Leer rutas JSON como subcolumnas
</div>

El tipo `JSON` permite leer cada ruta como una subcolumna independiente.
Si el tipo de la ruta solicitada no se especifica en la declaración del tipo JSON,
la subcolumna de esa ruta siempre tendrá el tipo [Dynamic](/es/sql-reference/data-types/dynamic.md).

Por ejemplo:

```sql title="Query"
CREATE TABLE test (json JSON(a.b UInt32, SKIP a.e)) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : 42, "g" : 42.42}, "c" : [1, 2, 3], "d" : "2020-01-01"}'), ('{"f" : "Hello, World!", "d" : "2020-01-02"}'), ('{"a" : {"b" : 43, "e" : 10, "g" : 43.43}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────┐
│ {"a":{"b":42,"g":42.42},"c":["1","2","3"],"d":"2020-01-01"} │
│ {"a":{"b":0},"d":"2020-01-02","f":"Hello, World!"}          │
│ {"a":{"b":43,"g":43.43},"c":["4","5","6"]}                  │
└─────────────────────────────────────────────────────────────┘
```

```sql title="Query (Reading JSON paths as sub-columns)"
SELECT json.a.b, json.a.g, json.c, json.d FROM test;
```

```text title="Response (Reading JSON paths as sub-columns)"
┌─json.a.b─┬─json.a.g─┬─json.c──┬─json.d─────┐
│       42 │ 42.42    │ [1,2,3] │ 2020-01-01 │
│        0 │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ    │ 2020-01-02 │
│       43 │ 43.43    │ [4,5,6] │ ᴺᵁᴸᴸ       │
└──────────┴──────────┴─────────┴────────────┘
```

También puedes usar la función `getSubcolumn` para leer subcolumnas de tipo JSON:

```sql title="Query"
SELECT getSubcolumn(json, 'a.b'), getSubcolumn(json, 'a.g'), getSubcolumn(json, 'c'), getSubcolumn(json, 'd') FROM test;
```

```text title="Response"
┌─getSubcolumn(json, 'a.b')─┬─getSubcolumn(json, 'a.g')─┬─getSubcolumn(json, 'c')─┬─getSubcolumn(json, 'd')─┐
│                        42 │ 42.42                     │ [1,2,3]                 │ 2020-01-01              │
│                         0 │ ᴺᵁᴸᴸ                      │ ᴺᵁᴸᴸ                    │ 2020-01-02              │
│                        43 │ 43.43                     │ [4,5,6]                 │ ᴺᵁᴸᴸ                    │
└───────────────────────────┴───────────────────────────┴─────────────────────────┴─────────────────────────┘
```

Si la ruta solicitada no se encuentra en los datos, se rellenará con valores `NULL`:

```sql title="Query"
SELECT json.non.existing.path FROM test;
```

```text title="Response"
┌─json.non.existing.path─┐
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
│ ᴺᵁᴸᴸ                   │
└────────────────────────┘
```

Comprobemos los tipos de datos de las subcolumnas devueltas:

```sql title="Query"
SELECT toTypeName(json.a.b), toTypeName(json.a.g), toTypeName(json.c), toTypeName(json.d) FROM test;
```

```text title="Response"
┌─toTypeName(json.a.b)─┬─toTypeName(json.a.g)─┬─toTypeName(json.c)─┬─toTypeName(json.d)─┐
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
│ UInt32               │ Dynamic              │ Dynamic            │ Dynamic            │
└──────────────────────┴──────────────────────┴────────────────────┴────────────────────┘
```

Como podemos ver, para `a.b`, el tipo es `UInt32`, tal como lo especificamos en la declaración del tipo JSON,
y para todas las demás subcolumnas el tipo es `Dynamic`.

También es posible leer subcolumnas de un tipo `Dynamic` mediante la sintaxis especial `json.some.path.:TypeName`:

```sql title="Query"
SELECT
    json.a.g.:Float64,
    dynamicType(json.a.g),
    json.d.:Date,
    dynamicType(json.d)
FROM test
```

```text title="Response"
┌─json.a.g.:`Float64`─┬─dynamicType(json.a.g)─┬─json.d.:`Date`─┬─dynamicType(json.d)─┐
│               42.42 │ Float64               │     2020-01-01 │ Date                │
│                ᴺᵁᴸᴸ │ None                  │     2020-01-02 │ Date                │
│               43.43 │ Float64               │           ᴺᵁᴸᴸ │ None                │
└─────────────────────┴───────────────────────┴────────────────┴─────────────────────┘
```

Las subcolumnas de `Dynamic` pueden convertirse a cualquier tipo de dato. En este caso, se lanzará una excepción si el tipo interno de `Dynamic` no puede convertirse al tipo solicitado:

```sql title="Query"
SELECT json.a.g::UInt64 AS uint
FROM test;
```

```text title="Response"
┌─uint─┐
│   42 │
│    0 │
│   43 │
└──────┘
```

```sql title="Query"
SELECT json.a.g::UUID AS float
FROM test;
```

```text title="Response"
Received exception from server:
Code: 48. DB::Exception: Received from localhost:9000. DB::Exception:
Conversion between numeric types and UUID is not supported.
Probably the passed UUID is unquoted:
while executing 'FUNCTION CAST(__table1.json.a.g :: 2, 'UUID'_String :: 1) -> CAST(__table1.json.a.g, 'UUID'_String) UUID : 0'.
(NOT_IMPLEMENTED)
```

:::note
Para leer subcolumnas de forma eficiente desde partes Compact de MergeTree, asegúrate de que esté habilitada la configuración de MergeTree [write&#95;marks&#95;for&#95;substreams&#95;in&#95;compact&#95;parts](../../operations/settings/merge-tree-settings.md#write_marks_for_substreams_in_compact_parts).
:::

<div id="reading-json-sub-objects-as-sub-columns">
  ## Leer subobjetos JSON como subcolumnas
</div>

El tipo `JSON` admite la lectura de objetos anidados como subcolumnas de tipo `JSON` mediante la sintaxis especial `json.^some.path`:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : {"b" : {"c" : 42, "g" : 42.42}}, "c" : [1, 2, 3], "d" : {"e" : {"f" : {"g" : "Hello, World", "h" : [1, 2, 3]}}}}'), ('{"f" : "Hello, World!", "d" : {"e" : {"f" : {"h" : [4, 5, 6]}}}}'), ('{"a" : {"b" : {"c" : 43, "e" : 10, "g" : 43.43}}, "c" : [4, 5, 6]}');
SELECT json FROM test;
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":"42","g":42.42}},"c":["1","2","3"],"d":{"e":{"f":{"g":"Hello, World","h":["1","2","3"]}}}} │
│ {"d":{"e":{"f":{"h":["4","5","6"]}}},"f":"Hello, World!"}                                                 │
│ {"a":{"b":{"c":"43","e":"10","g":43.43}},"c":["4","5","6"]}                                               │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.^a.b, json.^d.e.f FROM test;
```

```text title="Response"
┌─json.^`a`.b───────────────────┬─json.^`d`.e.f──────────────────────────┐
│ {"c":"42","g":42.42}          │ {"g":"Hello, World","h":["1","2","3"]} │
│ {}                            │ {"h":["4","5","6"]}                    │
│ {"c":"43","e":"10","g":43.43} │ {}                                     │
└───────────────────────────────┴────────────────────────────────────────┘
```

:::note
Cuando las rutas se almacenan en [datos compartidos](#shared-data-structure) básicos (`map`), leer subcolumnas de subobjetos puede resultar ineficiente, ya que requiere recorrer toda la estructura de datos compartidos. Con la serialización de datos compartidos `map_with_buckets` o `advanced`, la lectura de subcolumnas desde los datos compartidos está altamente optimizada.
:::

<div id="reading-json-combined-sub-columns">
  ## Lectura de subcolumnas combinadas de JSON
</div>

El tipo `JSON` admite leer una ruta como una **subcolumna combinada** mediante la sintaxis especial `json.@some.path`.
Una subcolumna combinada para una ruta determinada devuelve:

* El valor literal almacenado en esa ruta como `Dynamic`, si la ruta tiene un valor literal.
* Un subobjeto JSON en esa ruta como `Dynamic`, si la ruta no tiene un valor literal pero sí subrutas anidadas.
* `NULL`, si en esa ruta no existe ni un valor literal ni ninguna subruta.

Esto resulta útil cuando una ruta puede contener un valor escalar o un objeto anidado en distintas filas, y es más práctico que consultar por separado la subcolumna literal (`json.a`) y la subcolumna de subobjeto (`json.^a`).

El siguiente ejemplo compara los tres tipos de subcolumnas para la ruta `a`:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES ('{"a" : 42, "b" : {"c" : 1, "d" : "Hello"}}'), ('{"a" : {"x": 1, "y": 2}, "b" : {"c" : 1}}'), ('{"c" : "World"}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────┐
│ {"a":42,"b":{"c":1,"d":"Hello"}}│
│ {"a":{"x":1,"y":2},"b":{"c":1}}│
│ {"c":"World"}                   │
└─────────────────────────────────┘
```

```sql title="Query"
SELECT
    json.a,
    dynamicType(json.a),
    json.^a,
    toTypeName(json.^a),
    json.@a,
    dynamicType(json.@a)
FROM test;
```

```text title="Response"
┌─json.a─┬─dynamicType(json.a)─┬─json.^a───────┬─toTypeName(json.^a)─┬─json.@a───────┬─dynamicType(json.@a)─┐
│ 42     │ Int64               │ {}            │ JSON                │ 42            │ Int64                │
│ NULL   │ None                │ {"x":1,"y":2} │ JSON                │ {"x":1,"y":2} │ JSON                 │
│ NULL   │ None                │ {}            │ JSON                │ NULL          │ None                 │
└────────┴─────────────────────┴───────────────┴─────────────────────┴───────────────┴──────────────────────┘
```

* Fila 1: `a` contiene el literal `42`. `json.a` lo devuelve como `Dynamic(Int64)`, `json.^a` devuelve un subobjeto vacío `{}` (sin claves anidadas en `a`) y `json.@a` devuelve el literal `42`.
* Fila 2: `a` contiene un objeto anidado. `json.a` devuelve `NULL` (no hay ningún valor literal en esa ruta), `json.^a` devuelve el subobjeto como `JSON` y `json.@a` también devuelve el subobjeto como `Dynamic(JSON)`.
* Fila 3: `a` no existe en absoluto. Tanto `json.a` como `json.@a` devuelven `NULL`, mientras que `json.^a` devuelve un `{}` vacío.

:::note
Cuando las rutas se almacenan en [datos compartidos](#shared-data-structure) básicos (`map`), leer subcolumnas combinadas puede ser ineficiente, ya que requiere recorrer toda la estructura de datos compartidos. Con la serialización de datos compartidos `map_with_buckets` o `advanced`, la lectura de subcolumnas desde los datos compartidos está muy optimizada.
:::

<div id="type-inference-for-paths">
  ## Inferencia de tipos para rutas
</div>

Durante el análisis de `JSON`, ClickHouse intenta detectar el tipo de datos más adecuado para cada ruta JSON.
Funciona de forma similar a la [inferencia automática del esquema a partir de los datos de entrada](/es/interfaces/schema-inference.md)
y se controla con los mismos ajustes:

* [input&#95;format&#95;try&#95;infer&#95;dates](/es/operations/settings/formats#input_format_try_infer_dates)
* [input&#95;format&#95;try&#95;infer&#95;datetimes](/es/operations/settings/formats#input_format_try_infer_datetimes)
* [schema&#95;inference&#95;make&#95;columns&#95;nullable](/es/operations/settings/formats#schema_inference_make_columns_nullable)
* [input&#95;format&#95;json&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/es/operations/settings/formats#input_format_json_try_infer_numbers_from_strings)
* [input&#95;format&#95;json&#95;infer&#95;incomplete&#95;types&#95;as&#95;strings](/es/operations/settings/formats#input_format_json_infer_incomplete_types_as_strings)
* [input&#95;format&#95;json&#95;read&#95;numbers&#95;as&#95;strings](/es/operations/settings/formats#input_format_json_read_numbers_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;strings](/es/operations/settings/formats#input_format_json_read_bools_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;numbers](/es/operations/settings/formats#input_format_json_read_bools_as_numbers)
* [input&#95;format&#95;json&#95;read&#95;arrays&#95;as&#95;strings](/es/operations/settings/formats#input_format_json_read_arrays_as_strings)
* [input&#95;format&#95;json&#95;infer&#95;array&#95;of&#95;dynamic&#95;from&#95;array&#95;of&#95;different&#95;types](/es/operations/settings/formats#input_format_json_infer_array_of_dynamic_from_array_of_different_types)

Veamos algunos ejemplos:

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=1, input_format_try_infer_datetimes=1;
```

```text title="Response"
┌─paths_with_types─────────────────┐
│ {'a':'Date','b':'DateTime64(9)'} │
└──────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : "2020-01-01", "b" : "2020-01-01 10:00:00"}'::JSON) AS paths_with_types settings input_format_try_infer_dates=0, input_format_try_infer_datetimes=0;
```

```text title="Response"
┌─paths_with_types────────────┐
│ {'a':'String','b':'String'} │
└─────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=1;
```

```text title="Response"
┌─paths_with_types───────────────┐
│ {'a':'Array(Nullable(Int64))'} │
└────────────────────────────────┘
```

```sql title="Query"
SELECT JSONAllPathsWithTypes('{"a" : [1, 2, 3]}'::JSON) AS paths_with_types settings schema_inference_make_columns_nullable=0;
```

```text title="Response"
┌─paths_with_types─────┐
│ {'a':'Array(Int64)'} │
└──────────────────────┘
```

<div id="handling-arrays-of-json-objects">
  ## Manejo de arrays de objetos JSON
</div>

Las rutas JSON que contienen un array de objetos se interpretan como el tipo `Array(JSON)` y se insertan en una columna `Dynamic` para esa ruta.
Para leer un array de objetos, puedes extraerlo de la columna `Dynamic` como una subcolumna:

```sql title="Query"
CREATE TABLE test (json JSON) ENGINE = Memory;
INSERT INTO test VALUES
('{"a" : {"b" : [{"c" : 42, "d" : "Hello", "f" : [[{"g" : 42.42}]], "k" : {"j" : 1000}}, {"c" : 43}, {"e" : [1, 2, 3], "d" : "My", "f" : [[{"g" : 43.43, "h" : "2020-01-01"}]],  "k" : {"j" : 2000}}]}}'),
('{"a" : {"b" : [1, 2, 3]}}'),
('{"a" : {"b" : [{"c" : 44, "f" : [[{"h" : "2020-01-02"}]]}, {"e" : [4, 5, 6], "d" : "World", "f" : [[{"g" : 44.44}]],  "k" : {"j" : 3000}}]}}');
SELECT json FROM test;
```

```text title="Response"
┌─json────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {"a":{"b":[{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}},{"c":"43"},{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}]}} │
│ {"a":{"b":["1","2","3"]}}                                                                                                                                               │
│ {"a":{"b":[{"c":"44","f":[[{"h":"2020-01-02"}]]},{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}]}}                                                │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

```sql title="Query"
SELECT json.a.b, dynamicType(json.a.b) FROM test;
```

```text title="Response"
┌─json.a.b──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─dynamicType(json.a.b)────────────────────────────────────┐
│ ['{"c":"42","d":"Hello","f":[[{"g":42.42}]],"k":{"j":"1000"}}','{"c":"43"}','{"d":"My","e":["1","2","3"],"f":[[{"g":43.43,"h":"2020-01-01"}]],"k":{"j":"2000"}}'] │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
│ [1,2,3]                                                                                                                                                           │ Array(Nullable(Int64))                                   │
│ ['{"c":"44","f":[[{"h":"2020-01-02"}]]}','{"d":"World","e":["4","5","6"],"f":[[{"g":44.44}]],"k":{"j":"3000"}}']                                                  │ Array(JSON(max_dynamic_types=16, max_dynamic_paths=256)) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────┘
```

Como habrás notado, los parámetros `max_dynamic_types`/`max_dynamic_paths` del tipo `JSON` anidado se han reducido con respecto a los valores predeterminados.
Esto es necesario para evitar que el número de subcolumnas crezca sin control en arrays anidados de objetos JSON.

Intentemos leer subcolumnas de una columna `JSON` anidada:

```sql title="Query"
SELECT json.a.b.:`Array(JSON)`.c, json.a.b.:`Array(JSON)`.f, json.a.b.:`Array(JSON)`.d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

Podemos evitar escribir los nombres de las subcolumnas de `Array(JSON)` mediante una sintaxis especial:

```sql title="Query"
SELECT json.a.b[].c, json.a.b[].f, json.a.b[].d FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c─┬─json.a.b.:`Array(JSON)`.f───────────────────────────────────┬─json.a.b.:`Array(JSON)`.d─┐
│ [42,43,NULL]              │ [[['{"g":42.42}']],NULL,[['{"g":43.43,"h":"2020-01-01"}']]] │ ['Hello',NULL,'My']       │
│ []                        │ []                                                          │ []                        │
│ [44,NULL]                 │ [[['{"h":"2020-01-02"}']],[['{"g":44.44}']]]                │ [NULL,'World']            │
└───────────────────────────┴─────────────────────────────────────────────────────────────┴───────────────────────────┘
```

El número de `[]` después de la ruta indica el nivel del array. Por ejemplo, `json.path[][]` se transformará en `json.path.:Array(Array(JSON))`

Veamos las rutas y los tipos dentro de nuestro `Array(JSON)`:

```sql title="Query"
SELECT DISTINCT arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b[]))) FROM test;
```

```text title="Response"
┌─arrayJoin(JSONAllPathsWithTypes(arrayJoin(json.a.b.:`Array(JSON)`)))──┐
│ ('c','Int64')                                                         │
│ ('d','String')                                                        │
│ ('f','Array(Array(JSON(max_dynamic_types=8, max_dynamic_paths=64)))') │
│ ('k.j','Int64')                                                       │
│ ('e','Array(Nullable(Int64))')                                        │
└───────────────────────────────────────────────────────────────────────┘
```

Leamos las subcolumnas de una columna `Array(JSON)`:

```sql title="Query"
SELECT json.a.b[].c.:Int64, json.a.b[].f[][].g.:Float64, json.a.b[].f[][].h.:Date FROM test;
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.c.:`Int64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.g.:`Float64`─┬─json.a.b.:`Array(JSON)`.f.:`Array(Array(JSON))`.h.:`Date`─┐
│ [42,43,NULL]                       │ [[[42.42]],[],[[43.43]]]                                     │ [[[NULL]],[],[['2020-01-01']]]                            │
│ []                                 │ []                                                           │ []                                                        │
│ [44,NULL]                          │ [[[NULL]],[[44.44]]]                                         │ [[['2020-01-02']],[[NULL]]]                               │
└────────────────────────────────────┴──────────────────────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘
```

También podemos leer las subcolumnas de subobjetos de una columna `JSON` anidada:

```sql title="Query"
SELECT json.a.b[].^k FROM test
```

```text title="Response"
┌─json.a.b.:`Array(JSON)`.^`k`─────────┐
│ ['{"j":"1000"}','{}','{"j":"2000"}'] │
│ []                                   │
│ ['{}','{"j":"3000"}']                │
└──────────────────────────────────────┘
```

<div id="handling-json-keys-with-nulls">
  ## Gestión de claves JSON con NULL
</div>

En nuestra implementación de JSON, `null` y la ausencia de valor se consideran equivalentes:

```sql title="Query"
SELECT '{}'::JSON AS json1, '{"a" : null}'::JSON AS json2, json1 = json2
```

```text title="Response"
┌─json1─┬─json2─┬─equals(json1, json2)─┐
│ {}    │ {}    │                    1 │
└───────┴───────┴──────────────────────┘
```

Esto significa que es imposible determinar si los datos JSON originales contenían alguna ruta con el valor NULL o si esa ruta no estaba presente en absoluto.

<div id="handling-json-keys-with-dots">
  ## Gestión de claves JSON con puntos
</div>

Internamente, la columna JSON almacena todas las rutas y sus valores de forma aplanada. Esto significa que, de forma predeterminada, estos 2 objetos se consideran iguales:

```json
{"a" : {"b" : 42}}
{"a.b" : 42}
```

Ambos se almacenarán internamente como un par de ruta `a.b` y valor `42`. Al dar formato a JSON, siempre construimos objetos anidados a partir de las partes de la ruta separadas por puntos:

```sql title="Query"
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a":{"b":"42"}} │ ['a.b']             │ ['a.b']             │
└──────────────────┴──────────────────┴─────────────────────┴─────────────────────┘
```

Como puedes ver, el JSON inicial `{"a.b" : 42}` ahora se muestra con el formato `{"a" : {"b" : 42}}`.

Esta limitación también hace que falle el análisis de objetos JSON válidos como este:

```sql title="Query"
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json;
```

```text title="Response"
Code: 117. DB::Exception: Cannot insert data into JSON column: Duplicate path found during parsing JSON object: a.b. You can enable setting type_json_skip_duplicated_paths to skip duplicated paths during insert: In scope SELECT CAST('{"a.b" : 42, "a" : {"b" : "Hello, World"}}', 'JSON') AS json. (INCORRECT_DATA)
```

Si quieres conservar las claves con puntos y evitar que se formateen como objetos anidados, puedes habilitar la
configuración [json&#95;type&#95;escape&#95;dots&#95;in&#95;keys](/es/operations/settings/formats#json_type_escape_dots_in_keys) (disponible a partir de la versión `25.8`). En este caso, durante el análisis, todos los puntos de las claves JSON se
escaparán como `%2E` y se restaurarán durante el formateo.

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a.b":"42"} │ ['a.b']             │ ['a%2Eb']           │
└──────────────────┴──────────────┴─────────────────────┴─────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, JSONAllPaths(json);
```

```text title="Response"
┌─json──────────────────────────────────┬─JSONAllPaths(json)─┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ ['a%2Eb','a.b']    │
└───────────────────────────────────────┴────────────────────┘
```

Para leer una clave con un punto escapado como subcolumna, debes usar un punto escapado en el nombre de la subcolumna:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┘
```

Nota: debido a las limitaciones del parser de identificadores y del analyzer, la subcolumna `` json.`a.b` `` es equivalente a la subcolumna `json.a.b` y no leerá la ruta con el punto escapado:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.`a.b`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┴──────────────┘
```

Además, si quieres especificar una pista para una ruta JSON que contiene claves con puntos (o usarla en las secciones `SKIP`/`SKIP REGEX`), debes usar puntos escapados en la pista:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(`a%2Eb` UInt8) as json, json.`a%2Eb`, toTypeName(json.`a%2Eb`);
```

```text title="Response"
┌─json────────────────────────────────┬─json.a%2Eb─┬─toTypeName(json.a%2Eb)─┐
│ {"a.b":42,"a":{"b":"Hello World!"}} │         42 │ UInt8                  │
└─────────────────────────────────────┴────────────┴────────────────────────┘
```

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON(SKIP `a%2Eb`) as json, json.`a%2Eb`;
```

```text title="Response"
┌─json───────────────────────┬─json.a%2Eb─┐
│ {"a":{"b":"Hello World!"}} │ ᴺᵁᴸᴸ       │
└────────────────────────────┴────────────┘
```

<div id="reading-json-type-from-data">
  ## Lectura del tipo JSON a partir de datos
</div>

Todos los formatos de texto
([`JSONEachRow`](/es/interfaces/formats/JSONEachRow),
[`TSV`](/es/interfaces/formats/TabSeparated),
[`CSV`](/es/interfaces/formats/CSV),
[`CustomSeparated`](/es/interfaces/formats/CustomSeparated),
[`Values`](/es/interfaces/formats/Values), etc.) permiten leer el tipo `JSON`.

Ejemplos:

```sql title="Query"
SELECT json FROM format(JSONEachRow, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP d.e, SKIP REGEXP \'b.*\')', '
{"json" : {"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}}
{"json" : {"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}}
{"json" : {"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}}
{"json" : {"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}}
{"json" : {"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}}
')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

Para formatos de texto como `CSV`/`TSV`/etc., `JSON` se interpreta a partir de una cadena que contiene el objeto JSON:

```sql title="Query"
SELECT json FROM format(TSV, 'json JSON(a.b.c UInt32, SKIP a.b.d, SKIP REGEXP \'b.*\')',
'{"a" : {"b" : {"c" : 1, "d" : [0, 1]}}, "b" : "2020-01-01", "c" : 42, "d" : {"e" : {"f" : ["s1", "s2"]}, "i" : [1, 2, 3]}}
{"a" : {"b" : {"c" : 2, "d" : [2, 3]}}, "b" : [1, 2, 3], "c" : null, "d" : {"e" : {"g" : 43}, "i" : [4, 5, 6]}}
{"a" : {"b" : {"c" : 3, "d" : [4, 5]}}, "b" : {"c" : 10}, "e" : "Hello, World!"}
{"a" : {"b" : {"c" : 4, "d" : [6, 7]}}, "c" : 43}
{"a" : {"b" : {"c" : 5, "d" : [8, 9]}}, "b" : {"c" : 11, "j" : [1, 2, 3]}, "d" : {"e" : {"f" : ["s3", "s4"], "g" : 44}, "h" : "2020-02-02 10:00:00"}}')
```

```text title="Response"
┌─json──────────────────────────────────────────────────────────┐
│ {"a":{"b":{"c":1}},"c":"42","d":{"i":["1","2","3"]}}          │
│ {"a":{"b":{"c":2}},"d":{"i":["4","5","6"]}}                   │
│ {"a":{"b":{"c":3}},"e":"Hello, World!"}                       │
│ {"a":{"b":{"c":4}},"c":"43"}                                  │
│ {"a":{"b":{"c":5}},"d":{"h":"2020-02-02 10:00:00.000000000"}} │
└───────────────────────────────────────────────────────────────┘
```

<div id="reaching-the-limit-of-dynamic-paths-inside-json">
  ## Alcanzar el límite de las rutas dinámicas dentro de JSON
</div>

El tipo de dato `JSON` solo puede almacenar internamente un número limitado de rutas como sub-columnas independientes.
De forma predeterminada, este límite es `1024`, pero puede cambiarlo en la declaración del tipo mediante el parámetro `max_dynamic_paths`.

Cuando se alcanza el límite, todas las rutas nuevas insertadas en una columna `JSON` se almacenan en una única estructura de datos compartida.
Sigue siendo posible leer esas rutas como sub-columnas,
pero puede ser menos eficiente ([consulte la sección sobre datos compartidos](#shared-data-structure)).
Este límite es necesario para evitar tener un número enorme de sub-columnas distintas que pueda hacer que la tabla quede inutilizable.

Veamos qué sucede cuando se alcanza el límite en algunos escenarios.

<div id="reaching-the-limit-during-data-parsing">
  ### Al alcanzar el límite durante el análisis de datos
</div>

Durante el análisis de objetos `JSON` de los datos, cuando se alcanza el límite para el bloque de datos actual,
todas las rutas nuevas se almacenarán en una estructura de datos compartida. Podemos usar las dos siguientes funciones de introspección: `JSONDynamicPaths`, `JSONSharedDataPaths`:

```sql title="Query"
SELECT json, JSONDynamicPaths(json), JSONSharedDataPaths(json) FROM format(JSONEachRow, 'json JSON(max_dynamic_paths=3)', '
{"json" : {"a" : {"b" : 42}, "c" : [1, 2, 3]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-01"}}
{"json" : {"a" : {"b" : 44}, "c" : [4, 5, 6]}}
{"json" : {"a" : {"b" : 43}, "d" : "2020-01-02", "e" : "Hello", "f" : {"g" : 42.42}}}
{"json" : {"a" : {"b" : 43}, "c" : [7, 8, 9], "f" : {"g" : 43.43}, "h" : "World"}}
')
```

```text title="Response"
┌─json───────────────────────────────────────────────────────────┬─JSONDynamicPaths(json)─┬─JSONSharedDataPaths(json)─┐
│ {"a":{"b":"42"},"c":["1","2","3"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-01"}                              │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"44"},"c":["4","5","6"]}                             │ ['a.b','c','d']        │ []                        │
│ {"a":{"b":"43"},"d":"2020-01-02","e":"Hello","f":{"g":42.42}}  │ ['a.b','c','d']        │ ['e','f.g']               │
│ {"a":{"b":"43"},"c":["7","8","9"],"f":{"g":43.43},"h":"World"} │ ['a.b','c','d']        │ ['f.g','h']               │
└────────────────────────────────────────────────────────────────┴────────────────────────┴───────────────────────────┘
```

Como podemos ver, tras insertar las rutas `e` y `f.g`, se alcanzó el límite
y se insertaron en una estructura de datos compartida.

<div id="during-merges-of-data-parts-in-mergetree-table-engines">
  ### Durante las fusiones de partes de datos en motores de tabla MergeTree
</div>

Durante la fusión de varias partes de datos en una tabla `MergeTree`, la columna `JSON` de la parte de datos resultante puede alcanzar el límite de rutas dinámicas
y no podrá almacenar todas las rutas de las partes de origen como subcolumnas.
En este caso, ClickHouse elige qué rutas permanecerán como subcolumnas después de la fusión y qué rutas se almacenarán en la estructura de datos compartida.
En la mayoría de los casos, ClickHouse intenta conservar las rutas que contienen
el mayor número de valores no nulos y mover las rutas menos frecuentes a la estructura de datos compartida. Sin embargo, esto depende de la implementación.

Veamos un ejemplo de una fusión de este tipo.
Primero, vamos a crear una tabla con una columna `JSON`, establecer el límite de rutas dinámicas en `3` y luego insertar valores con `5` rutas diferentes:

```sql title="Query"
CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths=3)) ENGINE=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as a) FROM numbers(5);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as b) FROM numbers(4);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as c) FROM numbers(3);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as d) FROM numbers(2);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as e) FROM numbers(1);
```

Cada inserción creará una parte de datos independiente en la que la columna `JSON` contendrá una única ruta:

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│       5 │ ['a']         │ []                │ all_1_1_0 │
│       4 │ ['b']         │ []                │ all_2_2_0 │
│       3 │ ['c']         │ []                │ all_3_3_0 │
│       2 │ ['d']         │ []                │ all_4_4_0 │
│       1 │ ['e']         │ []                │ all_5_5_0 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

Ahora, fusionemos todas las partes en una sola y veamos qué pasa:

```sql title="Query"
SELECT
    count(),
    groupArrayArrayDistinct(JSONDynamicPaths(json)) AS dynamic_paths,
    groupArrayArrayDistinct(JSONSharedDataPaths(json)) AS shared_data_paths,
    _part
FROM test
GROUP BY _part
ORDER BY _part ASC
```

```text title="Response"
┌─count()─┬─dynamic_paths─┬─shared_data_paths─┬─_part─────┐
│      15 │ ['a','b','c'] │ ['d','e']         │ all_1_5_2 │
└─────────┴───────────────┴───────────────────┴───────────┘
```

Como podemos ver, ClickHouse mantuvo las rutas más frecuentes, `a`, `b` y `c`, y trasladó las rutas `d` y `e` a una estructura de datos compartida.

<div id="shared-data-structure">
  ## Estructura de datos compartida
</div>

Como se describió en la sección anterior, cuando se alcanza el límite `max_dynamic_paths`, todas las rutas nuevas se almacenan en una única estructura de datos compartida.
En esta sección analizaremos en detalle la estructura de datos compartida y cómo se leen de ella las subcolumnas de las rutas.

Consulta la sección [&quot;funciones de introspección&quot;](/es/sql-reference/data-types/newjson#introspection-functions) para obtener más información sobre las funciones que se utilizan para inspeccionar el contenido de una columna JSON.

<div id="shared-data-structure-in-memory">
  ### Estructura de datos compartida en memoria
</div>

En memoria, la estructura de datos compartida es simplemente una subcolumna de tipo `Map(String, String)` que almacena la correspondencia entre una ruta JSON aplanada y un valor codificado en binario.
Para extraer de ella la subcolumna correspondiente a una ruta, simplemente iteramos por todas las filas de esta columna `Map` e intentamos encontrar la ruta solicitada y sus valores.

<div id="shared-data-structure-in-merge-tree-parts">
  ### Estructura de datos compartida en las partes de MergeTree
</div>

En las tablas [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md), almacenamos los datos en partes de datos que guardan todo en disco (local o remoto). Los datos en disco pueden almacenarse de forma diferente a como se almacenan en memoria.
Actualmente, hay 3 serializaciones distintas de la estructura de datos compartida en las partes de datos de MergeTree: `map`, `map_with_buckets`
y `advanced`.

La versión de serialización está controlada por la configuración de MergeTree
[object&#95;shared&#95;data&#95;serialization&#95;version](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version)
y [object&#95;shared&#95;data&#95;serialization&#95;version&#95;for&#95;zero&#95;level&#95;parts](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version_for_zero_level_parts)
(una parte de nivel cero es la parte que se crea al insertar datos en la tabla; durante la fusión, las partes tienen un nivel superior).

Nota: cambiar la serialización de la estructura de datos compartida solo se admite
para la [versión de serialización de objetos](../../operations/settings/merge-tree-settings.md#object_serialization_version) `v3`

<div id="shared-data-map">
  #### Map
</div>

En la versión de serialización `map`, los datos compartidos se serializan como una sola columna de tipo `Map(String, String)`, igual que se almacenan en
memoria. Para leer una subcolumna de una ruta con este tipo de serialización, ClickHouse lee toda la columna `Map` y
extrae en memoria la ruta solicitada.

Esta serialización es eficiente para escribir datos y leer toda la columna `JSON`, pero no lo es para leer subcolumnas de rutas.

<div id="shared-data-map-with-buckets">
  #### Map con buckets
</div>

En la versión de serialización `map_with_buckets`, los datos compartidos se serializan como `N` columnas (&quot;buckets&quot;) de tipo `Map(String, String)`.
Cada bucket de este tipo contiene solo un subconjunto de rutas. Para leer una subcolumna de ruta de este tipo de serialización, ClickHouse
lee toda la columna `Map` de un único bucket y extrae en memoria la ruta solicitada.

Esta serialización es menos eficiente para escribir datos y leer toda la columna `JSON`, pero es más eficiente para leer subcolumnas de rutas
porque solo lee datos de los buckets necesarios.

El número de buckets `N` está controlado por los ajustes de MergeTree [object&#95;shared&#95;data&#95;buckets&#95;for&#95;compact&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_compact_part) (8 por defecto)
y [object&#95;shared&#95;data&#95;buckets&#95;for&#95;wide&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_wide_part) (32 por defecto).
El valor máximo permitido para ambos ajustes es 256.

<div id="shared-data-advanced">
  #### Avanzado
</div>

En la versión de serialización `advanced`, los datos compartidos se serializan en una estructura de datos especial que maximiza el rendimiento
de la lectura de subcolumnas de rutas, ya que almacena información adicional que permite leer solo los datos de las rutas solicitadas.
Esta serialización también admite buckets, por lo que cada bucket contiene solo un subconjunto de rutas.

Esta serialización es bastante ineficiente para la escritura de datos (por lo que no se recomienda usarla para partes de nivel cero); leer la columna `JSON` completa es ligeramente menos eficiente en comparación con la serialización `map`, pero es muy eficiente para leer subcolumnas de rutas.

Nota: debido a que almacena información adicional dentro de la estructura de datos, el tamaño de almacenamiento en disco es mayor con esta serialización en comparación con
las serializaciones `map` y `map_with_buckets`.

Para obtener una visión más detallada de las nuevas serializaciones de datos compartidos y de los detalles de implementación, consulta la [entrada del blog](https://clickhouse.com/blog/json-data-type-gets-even-better).

<div id="controlling-the-number-of-dynamic-paths">
  ## Control del número de rutas dinámicas dentro de JSON en partes de MergeTree
</div>

La forma principal de establecer un límite para las rutas dinámicas en JSON es usar el parámetro `max_dynamic_paths` dentro de la declaración del tipo JSON.
Pero cambiar `max_dynamic_paths` para columnas existentes requiere ejecutar `ALTER TABLE <table> MODIFY COLUMN <column> JSON(max_dynamic_paths=K)`, lo que iniciará una mutación en segundo plano que reescribirá todas las partes existentes.
Esta mutación puede ser bastante costosa y afectar al rendimiento del servidor hasta que finalice. Para evitarlo, puedes usar estas 3 configuraciones, que pueden ayudarte a cambiar el límite de rutas dinámicas en tablas MergeTree para nuevas partes de datos:

* `merge_max_dynamic_subcolumns_in_wide_part` - una configuración de MergeTree que limita el número de subcolumnas dinámicas para cada columna JSON durante la fusión en una parte de datos Wide.
* `merge_max_dynamic_subcolumns_in_compact_part` - una configuración de MergeTree que limita el número de subcolumnas dinámicas para cada columna JSON durante la fusión en una parte de datos Compact.
* `max_dynamic_subcolumns_in_json_type_parsing` - una configuración de sesión que limita el número de subcolumnas dinámicas para cada columna JSON durante el análisis de datos JSON en una columna JSON.

Nota: el límite de rutas dinámicas no puede superar el valor especificado en el parámetro `max_dynamic_paths`, aunque los valores de las configuraciones descritas sean mayores.

<div id="introspection-functions">
  ## Funciones de introspección
</div>

Hay varias funciones que pueden ayudar a inspeccionar el contenido de la columna JSON:

* [`JSONAllPaths`](../functions/json-functions.md#JSONAllPaths)
* [`JSONAllPathsWithTypes`](../functions/json-functions.md#JSONAllPathsWithTypes)
* [`JSONAllValues`](../functions/json-functions.md#JSONAllValues)
* [`JSONDynamicPaths`](../functions/json-functions.md#JSONDynamicPaths)
* [`JSONDynamicPathsWithTypes`](../functions/json-functions.md#JSONDynamicPathsWithTypes)
* [`JSONSharedDataPaths`](../functions/json-functions.md#JSONSharedDataPaths)
* [`JSONSharedDataPathsWithTypes`](../functions/json-functions.md#JSONSharedDataPathsWithTypes)
* [`distinctDynamicTypes`](../aggregate-functions/reference/distinctDynamicTypes.md)
* [`distinctJSONPaths and distinctJSONPathsAndTypes`](../aggregate-functions/reference/distinctJSONPaths.md)

**Ejemplos**

Veamos el contenido del conjunto de datos [GH Archive](https://www.gharchive.org/) para la fecha `2020-01-01`:

```sql title="Query"
SELECT arrayJoin(distinctJSONPaths(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
```

```text title="Response"
┌─arrayJoin(distinctJSONPaths(json))─────────────────────────┐
│ actor.avatar_url                                           │
│ actor.display_login                                        │
│ actor.gravatar_id                                          │
│ actor.id                                                   │
│ actor.login                                                │
│ actor.url                                                  │
│ created_at                                                 │
│ id                                                         │
│ org.avatar_url                                             │
│ org.gravatar_id                                            │
│ org.id                                                     │
│ org.login                                                  │
│ org.url                                                    │
│ payload.action                                             │
│ payload.before                                             │
│ payload.comment._links.html.href                           │
│ payload.comment._links.pull_request.href                   │
│ payload.comment._links.self.href                           │
│ payload.comment.author_association                         │
│ payload.comment.body                                       │
│ payload.comment.commit_id                                  │
│ payload.comment.created_at                                 │
│ payload.comment.diff_hunk                                  │
│ payload.comment.html_url                                   │
│ payload.comment.id                                         │
│ payload.comment.in_reply_to_id                             │
│ payload.comment.issue_url                                  │
│ payload.comment.line                                       │
│ payload.comment.node_id                                    │
│ payload.comment.original_commit_id                         │
│ payload.comment.original_position                          │
│ payload.comment.path                                       │
│ payload.comment.position                                   │
│ payload.comment.pull_request_review_id                     │
...
│ payload.release.node_id                                    │
│ payload.release.prerelease                                 │
│ payload.release.published_at                               │
│ payload.release.tag_name                                   │
│ payload.release.tarball_url                                │
│ payload.release.target_commitish                           │
│ payload.release.upload_url                                 │
│ payload.release.url                                        │
│ payload.release.zipball_url                                │
│ payload.size                                               │
│ public                                                     │
│ repo.id                                                    │
│ repo.name                                                  │
│ repo.url                                                   │
│ type                                                       │
└─arrayJoin(distinctJSONPaths(json))─────────────────────────┘
```

```sql title="Query"
SELECT arrayJoin(distinctJSONPathsAndTypes(json))
FROM s3('s3://clickhouse-public-datasets/gharchive/original/2020-01-01-*.json.gz', JSONAsObject)
SETTINGS date_time_input_format = 'best_effort'
```

```text title="Response"
┌─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┐
│ ('actor.avatar_url',['String'])                             │
│ ('actor.display_login',['String'])                          │
│ ('actor.gravatar_id',['String'])                            │
│ ('actor.id',['Int64'])                                      │
│ ('actor.login',['String'])                                  │
│ ('actor.url',['String'])                                    │
│ ('created_at',['DateTime'])                                 │
│ ('id',['String'])                                           │
│ ('org.avatar_url',['String'])                               │
│ ('org.gravatar_id',['String'])                              │
│ ('org.id',['Int64'])                                        │
│ ('org.login',['String'])                                    │
│ ('org.url',['String'])                                      │
│ ('payload.action',['String'])                               │
│ ('payload.before',['String'])                               │
│ ('payload.comment._links.html.href',['String'])             │
│ ('payload.comment._links.pull_request.href',['String'])     │
│ ('payload.comment._links.self.href',['String'])             │
│ ('payload.comment.author_association',['String'])           │
│ ('payload.comment.body',['String'])                         │
│ ('payload.comment.commit_id',['String'])                    │
│ ('payload.comment.created_at',['DateTime'])                 │
│ ('payload.comment.diff_hunk',['String'])                    │
│ ('payload.comment.html_url',['String'])                     │
│ ('payload.comment.id',['Int64'])                            │
│ ('payload.comment.in_reply_to_id',['Int64'])                │
│ ('payload.comment.issue_url',['String'])                    │
│ ('payload.comment.line',['Int64'])                          │
│ ('payload.comment.node_id',['String'])                      │
│ ('payload.comment.original_commit_id',['String'])           │
│ ('payload.comment.original_position',['Int64'])             │
│ ('payload.comment.path',['String'])                         │
│ ('payload.comment.position',['Int64'])                      │
│ ('payload.comment.pull_request_review_id',['Int64'])        │
...
│ ('payload.release.node_id',['String'])                      │
│ ('payload.release.prerelease',['Bool'])                     │
│ ('payload.release.published_at',['DateTime'])               │
│ ('payload.release.tag_name',['String'])                     │
│ ('payload.release.tarball_url',['String'])                  │
│ ('payload.release.target_commitish',['String'])             │
│ ('payload.release.upload_url',['String'])                   │
│ ('payload.release.url',['String'])                          │
│ ('payload.release.zipball_url',['String'])                  │
│ ('payload.size',['Int64'])                                  │
│ ('public',['Bool'])                                         │
│ ('repo.id',['Int64'])                                       │
│ ('repo.name',['String'])                                    │
│ ('repo.url',['String'])                                     │
│ ('type',['String'])                                         │
└─arrayJoin(distinctJSONPathsAndTypes(json))──────────────────┘
```

<div id="alter-modify-column-to-json-type">
  ## ALTER MODIFY COLUMN al tipo JSON
</div>

Es posible modificar una tabla existente y cambiar el tipo de la columna al nuevo tipo `JSON`. En este momento, solo se admite `ALTER` desde un tipo `String`.

**Ejemplo**

```sql title="Query"
CREATE TABLE test (json String) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test VALUES ('{"a" : 42}'), ('{"a" : 43, "b" : "Hello"}'), ('{"a" : 44, "b" : [1, 2, 3]}'), ('{"c" : "2020-01-01"}');
ALTER TABLE test MODIFY COLUMN json JSON;
SELECT json, json.a, json.b, json.c FROM test;
```

```text title="Response"
┌─json─────────────────────────┬─json.a─┬─json.b──┬─json.c─────┐
│ {"a":"42"}                   │ 42     │ ᴺᵁᴸᴸ    │ ᴺᵁᴸᴸ       │
│ {"a":"43","b":"Hello"}       │ 43     │ Hello   │ ᴺᵁᴸᴸ       │
│ {"a":"44","b":["1","2","3"]} │ 44     │ [1,2,3] │ ᴺᵁᴸᴸ       │
│ {"c":"2020-01-01"}           │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ    │ 2020-01-01 │
└──────────────────────────────┴────────┴─────────┴────────────┘
```

<div id="lazy-type-hints">
  ## Indicaciones de tipo diferidas (Experimental)
</div>

:::note
Esta funcionalidad es experimental y requiere que la configuración `allow_experimental_json_lazy_type_hints` esté habilitada.
:::

Al añadir o modificar indicaciones de tipo en una columna JSON con `ALTER TABLE ... MODIFY COLUMN`, ClickHouse normalmente reescribe todas las partes de datos para materializar las nuevas indicaciones de tipo. En tablas con grandes volúmenes de datos históricos (cientos de terabytes), esto puede resultar extremadamente costoso.

Las **indicaciones de tipo diferidas** permiten añadir indicaciones de tipo como una operación solo de metadatos, sin reescribir los datos existentes:

* **Partes antiguas**: las indicaciones de tipo se aplican en tiempo de consulta mediante una conversión de `Dynamic` al tipo indicado
* **Partes nuevas**: las indicaciones de tipo se materializan durante las operaciones `INSERT`
* **Fusiones**: las indicaciones de tipo se materializan cuando las partes se fusionan

Esto significa que puede añadir indicaciones de tipo al instante, y los datos se convertirán gradualmente a medida que se produzcan las fusiones normales en segundo plano.

<div id="enabling-lazy-type-hints">
  ### Habilitar las indicaciones de tipo diferidas
</div>

```sql
SET allow_experimental_json_lazy_type_hints = 1;
```

<div id="lazy-type-hints-example">
  ### Ejemplo
</div>

```sql title="Query"
-- Create a table and insert data
CREATE TABLE test_lazy (json JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_lazy VALUES ('{"user_id": "123", "score": "95.5"}');

-- Enable experimental setting
SET allow_experimental_json_lazy_type_hints = 1;

-- Add type hints - this completes instantly without mutation
ALTER TABLE test_lazy MODIFY COLUMN json JSON(user_id UInt64, score Float64);

-- Query the data - type hints are applied at read time
SELECT json.user_id, toTypeName(json.user_id), json.score, toTypeName(json.score) FROM test_lazy;
```

```text title="Response"
┌─json.user_id─┬─toTypeName(json.user_id)─┬─json.score─┬─toTypeName(json.score)─┐
│          123 │ UInt64                   │       95.5 │ Float64                │
└──────────────┴──────────────────────────┴────────────┴────────────────────────┘
```

<div id="verifying-no-mutation-occurred">
  ### Verificar que no se haya producido ninguna mutación
</div>

Puede verificar que `ALTER` se completó sin generar ninguna mutación consultando la tabla `system.mutations`:

```sql
SELECT * FROM system.mutations WHERE table = 'test_lazy' AND NOT is_done;
```

Con las indicaciones de tipo diferidas habilitadas, esta consulta no devuelve ninguna fila, lo que confirma que la operación se limitó a los metadatos.

<div id="materializing-type-hints">
  ### Materialización de las indicaciones de tipo
</div>

Para materializar las indicaciones de tipo en los datos existentes, puede hacer una de estas opciones:

1. **Esperar a las fusiones en segundo plano**: ClickHouse materializará automáticamente las indicaciones de tipo cuando se fusionen las partes
2. **Forzar una fusión**: use `OPTIMIZE TABLE test_lazy FINAL` para fusionar todas las partes de inmediato
3. **Reescribir las partes**: use `ALTER TABLE test_lazy REWRITE PARTS` para reescribir las partes con los nuevos metadatos

<div id="lazy-type-hints-limitations">
  ### Limitaciones
</div>

* Esta funcionalidad es experimental y puede cambiar en futuras versiones
* La conversión de tipos en tiempo de consulta puede implicar una sobrecarga de rendimiento considerable en comparación con los tipos materializados previamente, especialmente en objetos JSON grandes
* Esta funcionalidad solo se aplica al modificar `typed_paths` (indicaciones de tipo); otros parámetros de JSON, como `max_dynamic_paths`, `SKIP` o `SKIP REGEXP`, siguen requiriendo mutaciones

<div id="comparison-between-values-of-the-json-type">
  ## Comparación entre valores del tipo JSON
</div>

Los objetos JSON se comparan de forma similar a los valores de tipo Map.

Por ejemplo:

```sql title="Query"
CREATE TABLE test (json1 JSON, json2 JSON) ENGINE=Memory;
INSERT INTO test FORMAT JSONEachRow
{"json1" : {}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : [1, 2, 3]}}
{"json1" : {"a" : 42}, "json2" : {"a" : "Hello"}}
{"json1" : {"a" : 42}, "json2" : {"b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 42, "b" : 42}}
{"json1" : {"a" : 42}, "json2" : {"a" : 41, "b" : 42}}

SELECT json1, json2, json1 < json2, json1 = json2, json1 > json2 FROM test;
```

```text title="Response"
┌─json1──────┬─json2───────────────┬─less(json1, json2)─┬─equals(json1, json2)─┬─greater(json1, json2)─┐
│ {}         │ {}                  │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {}                  │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"41"}          │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"42"}          │                  0 │                    1 │                     0 │
│ {"a":"42"} │ {"a":["1","2","3"]} │                  0 │                    0 │                     1 │
│ {"a":"42"} │ {"a":"Hello"}       │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"b":"42"}          │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"42","b":"42"} │                  1 │                    0 │                     0 │
│ {"a":"42"} │ {"a":"41","b":"42"} │                  0 │                    0 │                     1 │
└────────────┴─────────────────────┴────────────────────┴──────────────────────┴───────────────────────┘
```

**Nota:** cuando 2 rutas contienen valores de diferentes tipos de datos, se comparan según la [regla de comparación](/es/sql-reference/data-types/variant#comparing-values-of-variant-data) del tipo de dato `Variant`.

<div id="data-skipping-indexes-for-json">
  ## Índices de omisión de datos para JSON
</div>

Los [índices de omisión de datos](/es/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) se pueden usar con columnas `JSON` de tres formas:

1. **Índices en subcolumnas específicas** — cree un índice de omisión estándar sobre una ruta JSON conocida, igual que en una columna normal. Esto indexa los *valores* de esa ruta.
2. **Índices basados en rutas con `JSONAllPaths`** — indexe el *conjunto de rutas* presente en cada gránulo para omitir los gránulos que no puedan contener la ruta consultada.
3. **Índices basados en valores con `JSONAllValues`** — indexe *todos los valores* de todas las rutas JSON mediante un [índice de texto](/es/engines/table-engines/mergetree-family/textindexes.md) para acelerar la búsqueda de texto completo en cualquier subcolumna JSON con un solo índice.

<div id="json-indexes-on-subcolumns">
  ### Índices en subcolumnas específicas
</div>

Se puede crear un índice de omisión en cualquier subcolumna JSON usando la misma sintaxis que para las columnas normales.
Cualquier [tipo de índice compatible](/es/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) funciona (`minmax`, `set`, `bloom_filter`, `tokenbf_v1`, `ngrambf_v1`, etc.).

Hay dos formas de hacer referencia a una subcolumna JSON en una expresión de índice:

* **Ruta tipada** declarada en la indicación de tipo JSON: acceso directo por nombre: `json.a`.
* **Ruta dinámica** con conversión explícita: use la sintaxis de conversión `::`: `json.b::String`.

También se pueden usar expresiones que combinen varias subcolumnas, por ejemplo `json.a || json.b::String`.

<div id="json-indexes-on-subcolumns-example">
  #### Ejemplo
</div>

```sql title="Query"
CREATE TABLE sensor_data
(
    data JSON(sensor_id UInt32),
    INDEX idx_sensor data.sensor_id TYPE minmax GRANULARITY 1,
    INDEX idx_location data.location::String TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4);
INSERT INTO sensor_data SELECT toJSONString(map('sensor_id', number, 'location', 'room_' || toString(number))) FROM numbers(4, 4);
```

El índice `minmax` de la subcolumna tipada `data.sensor_id` limita el escaneo a los gránulos coincidentes:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.sensor_id < 2;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_sensor
        Description: minmax GRANULARITY 1
        Parts: 1/2
        Granules: 2/8
```

El índice `bloom_filter` sobre la subcolumna convertida `data.location::String` también funciona:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM sensor_data WHERE data.location::String = 'room_5';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx_location
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/8
```

<div id="json-indexes-jsonallpaths">
  ### Índices basados en rutas con JSONAllPaths
</div>

También se pueden crear [índices de omisión de datos](/es/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) en columnas `JSON` mediante la función [`JSONAllPaths`](/es/sql-reference/functions/json-functions#JSONAllPaths).
Esto funciona de forma similar a como se crean índices de omisión en columnas [`Map`](/es/sql-reference/data-types/map) con `mapKeys`: el índice almacena el conjunto de rutas JSON presentes en cada gránulo y lo utiliza para omitir los gránulos que no pueden contener la ruta consultada.

<div id="json-indexes-jsonallpaths-supported-types">
  #### Tipos de índice compatibles
</div>

`JSONAllPaths` puede usarse con los siguientes tipos de índices de omisión:

* [`bloom_filter`](/es/engines/table-engines/mergetree-family/mergetree#bloom-filter) — admite `equals`, `in` e `IS NOT NULL`.
* [`tokenbf_v1`](/es/engines/table-engines/mergetree-family/mergetree#token-bloom-filter) — admite `equals` e `IS NOT NULL`.
* [`ngrambf_v1`](/es/engines/table-engines/mergetree-family/mergetree#n-gram-bloom-filter) — admite `equals` e `IS NOT NULL`.
* [`text`](/es/engines/table-engines/mergetree-family/textindexes) (índice invertido) — admite `equals`, `in` e `IS NOT NULL`.

<div id="json-indexes-on-subcolumns-example">
  #### Ejemplo
</div>

```sql title="Query"
CREATE TABLE events
(
    data JSON,
    INDEX idx JSONAllPaths(data) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO events VALUES ('{"user": {"name": "Alice"}, "action": "login"}');
INSERT INTO events VALUES ('{"metric": {"cpu": 0.95}, "host": "srv1"}');
```

Puedes usar `EXPLAIN indexes = 1` para verificar que se está utilizando el índice de omisión. Cuando una ruta existe solo en una de las partes, el índice omite la otra parte:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name = 'Alice';
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

Cuando una ruta no existe en ninguna de las partes, se descartan todas las partes y los gránulos:

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.nonexistent = 1;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 0/2
        Granules: 0/2
```

`IS NOT NULL` también usa el índice: omite los gránulos en los que la ruta no existe (ya que el valor sería `NULL`):

```sql title="Query"
EXPLAIN indexes = 1 SELECT * FROM events WHERE data.user.name IS NOT NULL;
```

```text title="Response"
...
    Indexes:
      Skip
        Name: idx
        Description: bloom_filter GRANULARITY 1
        Parts: 1/2
        Granules: 1/2
```

<div id="json-indexes-jsonallpaths-how-it-works">
  #### Cómo funciona
</div>

La expresión `JSONAllPaths(json_column)` produce un `Array(String)` que contiene todas las rutas presentes en un valor JSON.
El índice de omisión almacena estas cadenas de ruta en su estructura de datos (filtro de Bloom o índice invertido).
Cuando una consulta filtra por `json.some.path`, el índice comprueba si la cadena `"some.path"` está presente en el índice de cada gránulo y omite los gránulos en los que no aparece.

<div id="json-indexes-jsonallpaths-safety-with-missing-paths">
  #### Seguridad con rutas ausentes
</div>

Cuando una ruta JSON no está presente en un gránulo, la subcolumna da como resultado:

* `NULL` para el tipo `Dynamic` (p. ej., `json.path`) y las subcolumnas de tipo `Nullable` (p. ej., `json.path.:Int64`) — las comparaciones con `NULL` siempre devuelven false, por lo que la omisión es segura.
* El valor predeterminado del tipo para las expresiones `CAST` no `Nullable` (p. ej., `json.path::Int64` produce `0` cuando falta la ruta) — la omisión es segura solo cuando el valor comparado es distinto del valor predeterminado. El índice gestiona esta distinción automáticamente.

<div id="json-indexes-jsonallvalues">
  ### Búsqueda de texto completo con JSONAllValues
</div>

Los [índices de texto](/es/engines/table-engines/mergetree-family/textindexes.md) pueden utilizarse para acelerar la búsqueda de texto completo en columnas JSON mediante la función [`JSONAllValues`](/es/sql-reference/functions/json-functions#JSONAllValues).
`JSONAllValues` devuelve todos los valores de una columna JSON como `Array(String)`, sobre los que puede crearse un índice de texto.
Un único índice sobre `JSONAllValues(json_column)` cubre todas las rutas JSON, lo que permite realizar búsquedas de texto completo en cualquier subcolumna sin crear índices separados para cada ruta.

Consulta [Índices basados en valores con JSONAllValues](/es/engines/table-engines/mergetree-family/textindexes.md#json-indexes-jsonallvalues) en la documentación de índices de texto para obtener más información y ver ejemplos.

<div id="tips-for-better-usage-of-the-json-type">
  ## Consejos para aprovechar mejor el tipo JSON
</div>

Antes de crear una columna `JSON` y cargar datos en ella, tenga en cuenta los siguientes consejos:

* Analice sus datos y proporcione tantas pistas de ruta con tipos como sea posible. Esto hará que el almacenamiento y la lectura sean mucho más eficientes.
* Piense qué rutas necesitará y cuáles no necesitará nunca. Especifique en la sección `SKIP` las rutas que no vaya a necesitar y, si hace falta, también en la sección `SKIP REGEXP`. Esto optimizará el almacenamiento.
* No configure el parámetro `max_dynamic_paths` con valores muy altos, ya que puede reducir la eficiencia del almacenamiento y la lectura.
  Aunque depende en gran medida de parámetros del sistema, como la memoria, la CPU, etc., como regla general no debería establecer `max_dynamic_paths` por encima de 10 000 para el almacenamiento en el sistema de archivos local ni de 1024 para el almacenamiento en el sistema de archivos remoto.

<div id="further-reading">
  ## Lecturas complementarias
</div>

* [Cómo creamos un nuevo y potente tipo de datos JSON para ClickHouse](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
* [El desafío de los mil millones de documentos JSON: ClickHouse vs. MongoDB, Elasticsearch y más](https://clickhouse.com/blog/json-bench-clickhouse-vs-mongodb-elasticsearch-duckdb-postgresql)