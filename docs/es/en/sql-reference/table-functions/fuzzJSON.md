---
description: 'Altera una cadena JSON con variaciones aleatorias.'
sidebar_label: 'fuzzJSON'
sidebar_position: 75
slug: /sql-reference/table-functions/fuzzJSON
title: 'fuzzJSON'
doc_type: 'reference'
---

Altera una cadena JSON con variaciones aleatorias.

<div id="syntax">
  ## Sintaxis
</div>

```sql
fuzzJSON({ named_collection [, option=value [,..]] | json_str[, random_seed] })
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento                          | Descripción                                                                                                  |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `named_collection`                 | Una [NAMED COLLECTION](/es/sql-reference/statements/create/named-collection.md).                                |
| `option=value`                     | Parámetros opcionales de la colección nombrada y sus valores.                                                |
| `json_str` (String)                | La cadena de origen que representa datos estructurados en formato JSON.                                      |
| `random_seed` (UInt64)             | Semilla aleatoria manual para producir resultados reproducibles.                                             |
| `reuse_output` (boolean)           | Reutiliza la salida de un proceso de fuzzing como entrada para el siguiente fuzzer.                          |
| `malform_output` (boolean)         | Genera una cadena que no puede analizarse como un objeto JSON.                                               |
| `max_output_length` (UInt64)       | Longitud máxima permitida de la cadena JSON generada o alterada.                                             |
| `probability` (Float64)            | La probabilidad de aplicar fuzzing a un campo JSON (un par clave-valor). Debe estar dentro del rango [0, 1]. |
| `max_nesting_level` (UInt64)       | La profundidad máxima permitida de las estructuras anidadas dentro de los datos JSON.                        |
| `max_array_size` (UInt64)          | El tamaño máximo permitido de un array JSON.                                                                 |
| `max_object_size` (UInt64)         | El número máximo permitido de campos en un solo nivel de un objeto JSON.                                     |
| `max_string_value_length` (UInt64) | La longitud máxima de un valor String.                                                                       |
| `min_key_length` (UInt64)          | La longitud mínima de la clave. Debe ser al menos 1.                                                         |
| `max_key_length` (UInt64)          | La longitud máxima de la clave. Debe ser mayor o igual que `min_key_length`, si se especifica.               |

<div id="returned_value">
  ## Valor devuelto
</div>

Un objeto de tipo tabla con una sola columna que contiene cadenas JSON alteradas.

<div id="usage-example">
  ## Ejemplo de uso
</div>

```sql
CREATE NAMED COLLECTION json_fuzzer AS json_str='{}';
SELECT * FROM fuzzJSON(json_fuzzer) LIMIT 3;
```

```text
{"52Xz2Zd4vKNcuP2":true}
{"UPbOhOQAdPKIg91":3405264103600403024}
{"X0QUWu8yT":[]}
```

```sql
SELECT * FROM fuzzJSON(json_fuzzer, json_str='{"name" : "value"}', random_seed=1234) LIMIT 3;
```

```text
{"key":"value", "mxPG0h1R5":"L-YQLv@9hcZbOIGrAn10%GA"}
{"BRE3":true}
{"key":"value", "SWzJdEJZ04nrpSfy":[{"3Q23y":[]}]}
```

```sql
SELECT * FROM fuzzJSON(json_fuzzer, json_str='{"students" : ["Alice", "Bob"]}', reuse_output=true) LIMIT 3;
```

```text
{"students":["Alice", "Bob"], "nwALnRMc4pyKD9Krv":[]}
{"students":["1rNY5ZNs0wU&82t_P", "Bob"], "wLNRGzwDiMKdw":[{}]}
{"xeEk":["1rNY5ZNs0wU&82t_P", "Bob"], "wLNRGzwDiMKdw":[{}, {}]}
```

```sql
SELECT * FROM fuzzJSON(json_fuzzer, json_str='{"students" : ["Alice", "Bob"]}', max_output_length=512) LIMIT 3;
```

```text
{"students":["Alice", "Bob"], "BREhhXj5":true}
{"NyEsSWzJdeJZ04s":["Alice", 5737924650575683711, 5346334167565345826], "BjVO2X9L":true}
{"NyEsSWzJdeJZ04s":["Alice", 5737924650575683711, 5346334167565345826], "BjVO2X9L":true, "k1SXzbSIz":[{}]}
```

```sql
SELECT * FROM fuzzJSON('{"id":1}', 1234) LIMIT 3;
```

```text
{"id":1, "mxPG0h1R5":"L-YQLv@9hcZbOIGrAn10%GA"}
{"BRjE":16137826149911306846}
{"XjKE":15076727133550123563}
```

```sql
SELECT * FROM fuzzJSON(json_nc, json_str='{"name" : "FuzzJSON"}', random_seed=1337, malform_output=true) LIMIT 3;
```

```text
U"name":"FuzzJSON*"SpByjZKtr2VAyHCO"falseh
{"name"keFuzzJSON, "g6vVO7TCIk":jTt^
{"DBhz":YFuzzJSON5}
```