---
description: 'Documentação do tipo de dado JSON no ClickHouse, com suporte nativo
  para trabalhar com dados JSON'
keywords: ['json', 'tipo de dado']
sidebar_label: 'JSON'
sidebar_position: 63
slug: /sql-reference/data-types/newjson
title: 'Tipo de dado JSON'
doc_type: 'reference'
---

import {CardSecondary} from '@clickhouse/click-ui/bundled';
import WhenToUseJson from '@site/docs/best-practices/_snippets/_when-to-use-json.md';
import Link from '@docusaurus/Link'

<Link to="/docs/best-practices/use-json-where-appropriate" style={{display: 'flex', textDecoration: 'none', width: 'fit-content'}}>
  <CardSecondary badgeState="success" badgeText="" description="Confira nosso guia de boas práticas sobre JSON para ver exemplos, recursos avançados e pontos a considerar ao usar o tipo JSON." icon="book" infoText="Leia mais" infoUrl="/docs/best-practices/use-json-where-appropriate" title="Está procurando um guia?" />
</Link>

<br />

O tipo `JSON` armazena documentos JavaScript Object Notation (JSON) em uma única coluna.

:::note
No ClickHouse de código aberto, o tipo de dados JSON é considerado pronto para produção a partir da versão 25.3. Não é recomendável usar esse tipo em produção em versões anteriores.
:::

Para declarar uma coluna do tipo `JSON`, você pode usar a seguinte sintaxe:

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

Em que os parâmetros da sintaxe acima são definidos da seguinte forma:

| Parâmetro                   | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           | Valor padrão |
| --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------ |
| `max_dynamic_paths`         | Um parâmetro opcional que indica quantos caminhos podem ser armazenados separadamente como subcolunas em um único bloco de dados armazenado separadamente (por exemplo, em uma única parte de dados de uma tabela MergeTree). <br /><br />Se esse limite for excedido, todos os demais caminhos serão armazenados juntos em uma única estrutura chamada [dados compartilhados](#shared-data-structure).<br /><br />Também há [formas](#controlling-the-number-of-dynamic-paths) de alterar o limite de caminhos dinâmicos sem mudar esse parâmetro. | `1024`       |
| `max_dynamic_types`         | Um parâmetro opcional entre `1` e `255` que indica quantos tipos de dados diferentes podem ser armazenados separadamente em uma única coluna de caminho do tipo `Dynamic` em um único bloco de dados armazenado separadamente (por exemplo, em uma única parte de dados de uma tabela MergeTree). <br /><br />Se esse limite for excedido, todos os novos tipos serão armazenados juntos em uma única estrutura chamada `shared variant`.                                                                                                           | `32`         |
| `some.path TypeName`        | Uma indicação de tipo opcional para um caminho específico no JSON. Esses caminhos sempre serão armazenados como subcolunas com o tipo especificado.                                                                                                                                                                                                                                                                                                                                                                                                 |              |
| `SKIP path.to.skip`         | Uma indicação opcional para um caminho específico que deve ser ignorado durante a análise do JSON. Esses caminhos nunca serão armazenados na coluna JSON. Se o caminho especificado for um objeto JSON aninhado, todo o objeto aninhado será ignorado.                                                                                                                                                                                                                                                                                              |              |
| `SKIP REGEXP 'path_regexp'` | Uma indicação opcional com uma expressão regular usada para ignorar caminhos durante a análise do JSON. Todos os caminhos que corresponderem a essa expressão regular nunca serão armazenados na coluna JSON.                                                                                                                                                                                                                                                                                                                                       |              |

<WhenToUseJson />

<div id="creating-json">
  ## Criando `JSON`
</div>

Nesta seção, vamos analisar as várias formas de criar `JSON`.

<div id="using-json-in-a-table-column-definition">
  ### Usando `JSON` na definição de uma coluna de tabela
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
  ### Usando CAST com `::JSON`
</div>

É possível fazer CAST de vários tipos usando a sintaxe especial `::JSON`.

<div id="cast-from-string-to-json">
  #### CAST de `String` para `JSON`
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
  #### CAST de `Tuple` para `JSON`
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
  #### CAST de `Map` para `JSON`
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
Os caminhos JSON são armazenados em formato achatado. Isso significa que, quando um objeto JSON é montado a partir de um caminho como `a.b.c`,
não é possível saber se o objeto deve ser construído como `{ "a.b.c" : ... }` ou `{ "a": { "b": { "c": ... } } }`.
Nossa implementação sempre assumirá a segunda opção.

Por exemplo:

```sql title="Query"
SELECT CAST('{"a.b.c" : 42}', 'JSON') AS json
```

retornará:

```response title="Response"
   ┌─json───────────────────┐
1. │ {"a":{"b":{"c":"42"}}} │
   └────────────────────────┘
```

e **não**:

```sql
   ┌─json───────────┐
1. │ {"a.b.c":"42"} │
   └────────────────┘
```

:::

<div id="reading-json-paths-as-sub-columns">
  ## Leitura de caminhos JSON como subcolunas
</div>

O tipo `JSON` permite ler cada caminho como uma subcoluna separada.
Se o tipo do caminho solicitado não estiver especificado na declaração do tipo JSON,
então a subcoluna desse caminho sempre terá o tipo [Dynamic](/pt-BR/sql-reference/data-types/dynamic.md).

Por exemplo:

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

Você também pode usar a função `getSubcolumn` para ler subcolunas do tipo JSON:

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

Se o caminho solicitado não for encontrado nos dados, ele será preenchido com valores `NULL`:

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

Vamos verificar os tipos de dados das subcolunas retornadas:

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

Como podemos ver, para `a.b`, o tipo é `UInt32`, como especificado na declaração do tipo JSON,
e, para todas as outras subcolunas, o tipo é `Dynamic`.

Também é possível ler subcolunas de um tipo `Dynamic` usando a sintaxe especial `json.some.path.:TypeName`:

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

As subcolunas de `Dynamic` podem ser convertidas para qualquer tipo de dado. Nesse caso, será lançada uma exceção se o tipo interno em `Dynamic` não puder ser convertido para o tipo solicitado:

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
Para ler subcolunas com eficiência em partes Compact do MergeTree, verifique se a configuração do MergeTree [write&#95;marks&#95;for&#95;substreams&#95;in&#95;compact&#95;parts](../../operations/settings/merge-tree-settings.md#write_marks_for_substreams_in_compact_parts) está habilitada.
:::

<div id="reading-json-sub-objects-as-sub-columns">
  ## Lendo sub-objetos JSON como subcolunas
</div>

O tipo `JSON` suporta a leitura de objetos aninhados como subcolunas do tipo `JSON`, usando a sintaxe especial `json.^some.path`:

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
Quando os caminhos são armazenados em [dados compartilhados](#shared-data-structure) básicos (`map`), a leitura de subcolunas de subobjetos pode ser ineficiente, pois exige percorrer toda a estrutura de dados compartilhados. Com a serialização de dados compartilhados `map_with_buckets` ou `advanced`, a leitura de subcolunas a partir dos dados compartilhados é altamente otimizada.
:::

<div id="reading-json-combined-sub-columns">
  ## Leitura de subcolunas combinadas de JSON
</div>

O tipo `JSON` oferece suporte à leitura de um caminho como uma **subcoluna combinada** usando a sintaxe especial `json.@some.path`.
Uma subcoluna combinada para um determinado caminho retorna:

* O valor literal armazenado nesse caminho como `Dynamic`, se o caminho tiver um valor literal.
* Um subobjeto JSON nesse caminho como `Dynamic`, se o caminho não tiver um valor literal, mas tiver subcaminhos aninhados.
* `NULL`, se não existir nem um valor literal nem quaisquer subcaminhos para esse caminho.

Isso é útil quando um caminho pode conter tanto um valor escalar quanto um objeto aninhado em diferentes linhas, e é mais conveniente do que consultar separadamente a subcoluna literal (`json.a`) e a subcoluna de subobjeto (`json.^a`).

O exemplo a seguir compara os três tipos de subcoluna para o caminho `a`:

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

* Linha 1: `a` contém o literal `42`. `json.a` o retorna como `Dynamic(Int64)`, `json.^a` retorna um subobjeto vazio `{}` (sem chaves aninhadas em `a`) e `json.@a` retorna o literal `42`.
* Linha 2: `a` contém um objeto aninhado. `json.a` retorna `NULL` (não há literal nesse caminho), `json.^a` retorna o subobjeto como `JSON` e `json.@a` também retorna o subobjeto como `Dynamic(JSON)`.
* Linha 3: `a` está completamente ausente. Tanto `json.a` quanto `json.@a` retornam `NULL`, enquanto `json.^a` retorna `{}` vazio.

:::note
Quando os caminhos são armazenados em [dados compartilhados](#shared-data-structure) básicos (`map`), a leitura de subcolunas combinadas pode ser ineficiente, pois exige a varredura de toda a estrutura de dados compartilhados. Com a serialização de dados compartilhados `map_with_buckets` ou `advanced`, a leitura de subcolunas a partir dos dados compartilhados é altamente otimizada.
:::

<div id="type-inference-for-paths">
  ## Inferência de tipos para caminhos
</div>

Durante a análise de `JSON`, o ClickHouse tenta detectar o tipo de dado mais apropriado para cada caminho JSON.
Isso funciona de forma semelhante à [inferência automática de esquema a partir dos dados de entrada](/pt-BR/interfaces/schema-inference.md),
e é controlado pelas mesmas configurações:

* [input&#95;format&#95;try&#95;infer&#95;dates](/pt-BR/operations/settings/formats#input_format_try_infer_dates)
* [input&#95;format&#95;try&#95;infer&#95;datetimes](/pt-BR/operations/settings/formats#input_format_try_infer_datetimes)
* [schema&#95;inference&#95;make&#95;columns&#95;nullable](/pt-BR/operations/settings/formats#schema_inference_make_columns_nullable)
* [input&#95;format&#95;json&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/pt-BR/operations/settings/formats#input_format_json_try_infer_numbers_from_strings)
* [input&#95;format&#95;json&#95;infer&#95;incomplete&#95;types&#95;as&#95;strings](/pt-BR/operations/settings/formats#input_format_json_infer_incomplete_types_as_strings)
* [input&#95;format&#95;json&#95;read&#95;numbers&#95;as&#95;strings](/pt-BR/operations/settings/formats#input_format_json_read_numbers_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;strings](/pt-BR/operations/settings/formats#input_format_json_read_bools_as_strings)
* [input&#95;format&#95;json&#95;read&#95;bools&#95;as&#95;numbers](/pt-BR/operations/settings/formats#input_format_json_read_bools_as_numbers)
* [input&#95;format&#95;json&#95;read&#95;arrays&#95;as&#95;strings](/pt-BR/operations/settings/formats#input_format_json_read_arrays_as_strings)
* [input&#95;format&#95;json&#95;infer&#95;array&#95;of&#95;dynamic&#95;from&#95;array&#95;of&#95;different&#95;types](/pt-BR/operations/settings/formats#input_format_json_infer_array_of_dynamic_from_array_of_different_types)

Veja alguns exemplos:

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
  ## Como lidar com arrays de objetos JSON
</div>

Caminhos JSON que contêm um array de objetos são processados como o tipo `Array(JSON)` e inseridos em uma coluna `Dynamic` para esse caminho.
Para ler um array de objetos, você pode extraí-lo da coluna `Dynamic` como uma subcoluna:

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

Como você deve ter notado, os parâmetros `max_dynamic_types`/`max_dynamic_paths` do tipo `JSON` aninhado foram reduzidos em relação aos valores padrão.
Isso é necessário para evitar que o número de subcolunas cresça de forma descontrolada em arrays aninhados de objetos JSON.

Vamos tentar ler subcolunas de uma coluna `JSON` aninhada:

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

Podemos evitar escrever os nomes das subcolunas de `Array(JSON)` usando uma sintaxe especial:

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

O número de `[]` após o caminho indica o nível do array. Por exemplo, `json.path[][]` será transformado em `json.path.:Array(Array(JSON))`

Vamos verificar os caminhos e tipos dentro do nosso `Array(JSON)`:

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

Vamos ler subcolunas de uma coluna `Array(JSON)`:

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

Também é possível ler subcolunas de subobjetos em uma coluna `JSON` aninhada:

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
  ## Manipulando chaves JSON com NULL
</div>

Na nossa implementação de JSON, `null` e a ausência de valor são considerados equivalentes:

```sql title="Query"
SELECT '{}'::JSON AS json1, '{"a" : null}'::JSON AS json2, json1 = json2
```

```text title="Response"
┌─json1─┬─json2─┬─equals(json1, json2)─┐
│ {}    │ {}    │                    1 │
└───────┴───────┴──────────────────────┘
```

Isso significa que é impossível determinar se os dados JSON originais continham algum caminho com o valor NULL ou se esse caminho simplesmente não existia.

<div id="handling-json-keys-with-dots">
  ## Tratamento de chaves JSON com pontos
</div>

Internamente, a coluna JSON armazena todos os caminhos e valores de forma achatada. Isso significa que, por padrão, estes 2 objetos são considerados iguais:

```json
{"a" : {"b" : 42}}
{"a.b" : 42}
```

Ambos serão armazenados internamente como um par formado pelo caminho `a.b` e pelo valor `42`. Durante a formatação do JSON, sempre criamos objetos aninhados com base nas partes do caminho separadas por ponto:

```sql title="Query"
SELECT '{"a" : {"b" : 42}}'::JSON AS json1, '{"a.b" : 42}'::JSON AS json2, JSONAllPaths(json1), JSONAllPaths(json2);
```

```text title="Response"
┌─json1────────────┬─json2────────────┬─JSONAllPaths(json1)─┬─JSONAllPaths(json2)─┐
│ {"a":{"b":"42"}} │ {"a":{"b":"42"}} │ ['a.b']             │ ['a.b']             │
└──────────────────┴──────────────────┴─────────────────────┴─────────────────────┘
```

Como você pode ver, o JSON inicial `{"a.b" : 42}` agora é formatado como `{"a" : {"b" : 42}}`.

Essa limitação também faz com que falhe o parsing de objetos JSON válidos como este:

```sql title="Query"
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json;
```

```text title="Response"
Code: 117. DB::Exception: Cannot insert data into JSON column: Duplicate path found during parsing JSON object: a.b. You can enable setting type_json_skip_duplicated_paths to skip duplicated paths during insert: In scope SELECT CAST('{"a.b" : 42, "a" : {"b" : "Hello, World"}}', 'JSON') AS json. (INCORRECT_DATA)
```

Se você quiser manter chaves com pontos e evitar formatá-las como objetos aninhados, poderá ativar a
configuração [json&#95;type&#95;escape&#95;dots&#95;in&#95;keys](/pt-BR/operations/settings/formats#json_type_escape_dots_in_keys) (disponível a partir da versão `25.8`). Nesse caso, durante o parsing, todos os pontos nas chaves JSON serão
escapados como `%2E` e desescapados novamente durante a formatação.

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

Para ler uma chave com ponto escapado como uma subcoluna, você precisa usar o ponto escapado no nome da subcoluna:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┘
```

Nota: devido às limitações do parser de identificadores e do analisador, a subcoluna `` json.`a.b` `` equivale à subcoluna `json.a.b` e não lê o caminho com ponto escapado:

```sql title="Query"
SET json_type_escape_dots_in_keys=1;
SELECT '{"a.b" : 42, "a" : {"b" : "Hello World!"}}'::JSON AS json, json.`a%2Eb`, json.`a.b`, json.a.b;
```

```text title="Response"
┌─json──────────────────────────────────┬─json.a%2Eb─┬─json.a.b─────┬─json.a.b─────┐
│ {"a.b":"42","a":{"b":"Hello World!"}} │ 42         │ Hello World! │ Hello World! │
└───────────────────────────────────────┴────────────┴──────────────┴──────────────┘
```

Além disso, se você quiser especificar uma indicação para um JSON path que contenha chaves com pontos (ou usá-la nas seções `SKIP`/`SKIP REGEX`), será necessário usar pontos escapados na indicação:

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
  ## Leitura do tipo JSON a partir de dados
</div>

Todos os formatos de texto
([`JSONEachRow`](/pt-BR/interfaces/formats/JSONEachRow),
[`TSV`](/pt-BR/interfaces/formats/TabSeparated),
[`CSV`](/pt-BR/interfaces/formats/CSV),
[`CustomSeparated`](/pt-BR/interfaces/formats/CustomSeparated),
[`Values`](/pt-BR/interfaces/formats/Values), etc.) suportam a leitura do tipo `JSON`.

Exemplos:

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

Para formatos de texto como `CSV`/`TSV`/etc, o `JSON` é interpretado a partir de uma string que contém o objeto JSON:

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
  ## Atingindo o limite de caminhos dinâmicos no JSON
</div>

O tipo de dado `JSON` pode armazenar internamente apenas um número limitado de caminhos como sub-colunas separadas.
Por padrão, esse limite é `1024`, mas você pode alterá-lo na declaração do tipo usando o parâmetro `max_dynamic_paths`.

Quando o limite é atingido, todos os novos caminhos inseridos em uma coluna `JSON` são armazenados em uma única estrutura de dados compartilhada.
Ainda é possível ler esses caminhos como sub-colunas,
mas isso pode ser menos eficiente ([veja a seção sobre dados compartilhados](#shared-data-structure)).
Esse limite é necessário para evitar um número enorme de sub-colunas diferentes, o que pode tornar a tabela inutilizável.

Vamos ver o que acontece quando o limite é atingido em alguns cenários diferentes.

<div id="reaching-the-limit-during-data-parsing">
  ### Ao atingir o limite durante a análise de dados
</div>

Durante a análise de objetos `JSON` nos dados, quando o limite é atingido para o bloco de dados atual,
todos os novos caminhos serão armazenados em uma estrutura de dados compartilhada. Podemos usar as duas funções de introspecção a seguir: `JSONDynamicPaths`, `JSONSharedDataPaths`

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

Como podemos ver, após a inserção dos caminhos `e` e `f.g`, o limite foi atingido,
e eles foram inseridos em uma estrutura de dados compartilhada.

<div id="during-merges-of-data-parts-in-mergetree-table-engines">
  ### Durante as mesclagens de partes de dados em motores de tabela MergeTree
</div>

Durante a mesclagem de várias partes de dados em uma tabela `MergeTree`, a coluna `JSON` na parte de dados resultante pode atingir o limite de caminhos dinâmicos
e não conseguir armazenar todos os caminhos das partes de origem como subcolunas.
Nesse caso, o ClickHouse escolhe quais caminhos permanecerão como subcolunas após a mesclagem e quais caminhos serão armazenados na estrutura de dados compartilhada.
Na maioria dos casos, o ClickHouse tenta manter os caminhos que contêm
o maior número de valores não nulos e mover os caminhos mais raros para a estrutura de dados compartilhada. No entanto, isso depende da implementação.

Vamos ver um exemplo desse tipo de mesclagem.
Primeiro, vamos criar uma tabela com uma coluna `JSON`, definir o limite de caminhos dinâmicos como `3` e, em seguida, inserir valores com `5` caminhos diferentes:

```sql title="Query"
CREATE TABLE test (id UInt64, json JSON(max_dynamic_paths=3)) ENGINE=MergeTree ORDER BY id;
SYSTEM STOP MERGES test;
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as a) FROM numbers(5);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as b) FROM numbers(4);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as c) FROM numbers(3);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as d) FROM numbers(2);
INSERT INTO test SELECT number, formatRow('JSONEachRow', number as e) FROM numbers(1);
```

Cada inserção criará uma parte de dados separada, com a coluna `JSON` contendo um único caminho:

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

Agora, vamos mesclar todas as partes em uma só e ver o que acontece:

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

Como podemos ver, o ClickHouse manteve os caminhos mais frequentes `a`, `b` e `c` e moveu os caminhos `d` e `e` para uma estrutura de dados compartilhada.

<div id="shared-data-structure">
  ## Estrutura de dados compartilhada
</div>

Conforme descrito na seção anterior, quando o limite de `max_dynamic_paths` é atingido, todos os novos caminhos são armazenados em uma única estrutura de dados compartilhada.
Nesta seção, vamos examinar os detalhes da estrutura de dados compartilhada e como lemos dela as subcolunas de caminhos.

Consulte a seção [&quot;funções de introspecção&quot;](/pt-BR/sql-reference/data-types/newjson#introspection-functions) para ver detalhes sobre as funções usadas para inspecionar o conteúdo de uma coluna JSON.

<div id="shared-data-structure-in-memory">
  ### Estrutura de dados compartilhada na memória
</div>

Na memória, a estrutura de dados compartilhada é apenas uma subcoluna do tipo `Map(String, String)` que armazena o mapeamento de um caminho JSON achatado para um valor codificado em binário.
Para extrair dela uma subcoluna de caminho, basta iterar por todas as linhas dessa coluna `Map` e tentar encontrar o caminho solicitado e seus valores.

<div id="shared-data-structure-in-merge-tree-parts">
  ### Estrutura de dados compartilhada em partes do MergeTree
</div>

Em tabelas [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md), armazenamos os dados em partes de dados que guardam tudo em disco (local ou remoto). Os dados em disco podem ser armazenados de forma diferente dos dados em memória.
Atualmente, há 3 serializações diferentes da estrutura de dados compartilhada em partes de dados do MergeTree: `map`, `map_with_buckets`
e `advanced`.

A versão de serialização é controlada pelas
configurações do MergeTree [object&#95;shared&#95;data&#95;serialization&#95;version](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version)
e [object&#95;shared&#95;data&#95;serialization&#95;version&#95;for&#95;zero&#95;level&#95;parts](../../operations/settings/merge-tree-settings.md#object_shared_data_serialization_version_for_zero_level_parts)
(parte de nível zero é a parte criada durante a inserção de dados na tabela; durante merges, as partes têm nível mais alto).

Nota: a alteração da serialização da estrutura de dados compartilhada tem suporte apenas
para a [object serialization version](../../operations/settings/merge-tree-settings.md#object_serialization_version) `v3`

<div id="shared-data-map">
  #### Map
</div>

Na versão de serialização `map`, os dados compartilhados são serializados como uma única coluna do tipo `Map(String, String)`, da mesma forma que são armazenados na
memória. Para ler a subcoluna de caminho desse tipo de serialização, o ClickHouse lê toda a coluna `Map` e
extrai o caminho solicitado na memória.

Essa serialização é eficiente para gravar dados e ler toda a coluna `JSON`, mas não é eficiente para ler subcolunas de caminhos.

<div id="shared-data-map-with-buckets">
  #### Map com buckets
</div>

Na versão de serialização `map_with_buckets`, os dados compartilhados são serializados como `N` colunas (&quot;buckets&quot;) do tipo `Map(String, String)`.
Cada bucket desse tipo contém apenas um subconjunto de caminhos. Para ler uma subcoluna de caminho desse tipo de serialização, o ClickHouse
lê a coluna `Map` inteira de um único bucket e extrai o caminho solicitado na memória.

Essa serialização é menos eficiente para gravar dados e ler a coluna `JSON` inteira, mas é mais eficiente para ler subcolunas de caminhos
porque lê dados apenas dos buckets necessários.

O número de buckets `N` é controlado pelas configurações do MergeTree [object&#95;shared&#95;data&#95;buckets&#95;for&#95;compact&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_compact_part) (8 por padrão)
e [object&#95;shared&#95;data&#95;buckets&#95;for&#95;wide&#95;part](../../operations/settings/merge-tree-settings.md#object_shared_data_buckets_for_wide_part) (32 por padrão).
O valor máximo permitido para ambas as configurações é 256.

<div id="shared-data-advanced">
  #### Avançado
</div>

Na versão de serialização `advanced`, os dados compartilhados são serializados em uma estrutura de dados especial que maximiza o desempenho
da leitura de subcolunas de caminhos ao armazenar informações adicionais que permitem ler apenas os dados dos caminhos solicitados.
Essa serialização também oferece suporte a buckets, portanto cada bucket contém apenas um subconjunto de caminhos.

Essa serialização é bastante ineficiente para gravação de dados (portanto, não é recomendável usá-la para partes de nível zero); a leitura da coluna `JSON` inteira é ligeiramente menos eficiente em comparação com a serialização `map`, mas ela é muito eficiente para ler subcolunas de caminhos.

Nota: devido ao armazenamento de informações adicionais dentro da estrutura de dados, o tamanho em disco é maior com essa serialização em comparação com as
serializações `map` e `map_with_buckets`.

Para uma visão geral mais detalhada das novas serializações de dados compartilhados e dos detalhes de implementação, leia o [post do blog](https://clickhouse.com/blog/json-data-type-gets-even-better).

<div id="controlling-the-number-of-dynamic-paths">
  ## Controlando o número de caminhos dinâmicos dentro de JSON em partes do MergeTree
</div>

A principal forma de definir um limite para caminhos dinâmicos em JSON é usar o parâmetro `max_dynamic_paths` na declaração do tipo JSON.
Mas alterar `max_dynamic_paths` para colunas existentes exige executar `ALTER TABLE <table> MODIFY COLUMN <column> JSON(max_dynamic_paths=K)`, o que iniciará uma mutação em segundo plano que reescreverá todas as partes existentes.
Essa mutação pode ser bastante pesada e pode afetar o desempenho do servidor até sua conclusão. Para evitar isso, você pode usar estas 3 configurações, que podem ajudar a alterar o limite de caminhos dinâmicos em tabelas MergeTree para novas partes de dados:

* `merge_max_dynamic_subcolumns_in_wide_part` - uma configuração do MergeTree que limita o número de subcolunas dinâmicas de cada coluna JSON durante a merge em uma parte de dados Wide.
* `merge_max_dynamic_subcolumns_in_compact_part` - uma configuração do MergeTree que limita o número de subcolunas dinâmicas de cada coluna JSON durante a merge em uma parte de dados Compact.
* `max_dynamic_subcolumns_in_json_type_parsing` - uma configuração de sessão que limita o número de subcolunas dinâmicas de cada coluna JSON durante o parsing de dados JSON em uma coluna JSON.

Nota: o limite de caminhos dinâmicos não pode exceder o valor especificado no parâmetro `max_dynamic_paths`, mesmo que os valores das configurações descritas sejam maiores.

<div id="introspection-functions">
  ## Funções de introspecção
</div>

Há várias funções que ajudam a inspecionar o conteúdo da coluna JSON:

* [`JSONAllPaths`](../functions/json-functions.md#JSONAllPaths)
* [`JSONAllPathsWithTypes`](../functions/json-functions.md#JSONAllPathsWithTypes)
* [`JSONAllValues`](../functions/json-functions.md#JSONAllValues)
* [`JSONDynamicPaths`](../functions/json-functions.md#JSONDynamicPaths)
* [`JSONDynamicPathsWithTypes`](../functions/json-functions.md#JSONDynamicPathsWithTypes)
* [`JSONSharedDataPaths`](../functions/json-functions.md#JSONSharedDataPaths)
* [`JSONSharedDataPathsWithTypes`](../functions/json-functions.md#JSONSharedDataPathsWithTypes)
* [`distinctDynamicTypes`](../aggregate-functions/reference/distinctDynamicTypes.md)
* [`distinctJSONPaths and distinctJSONPathsAndTypes`](../aggregate-functions/reference/distinctJSONPaths.md)

**Exemplos**

Vamos investigar o conteúdo do conjunto de dados [GH Archive](https://www.gharchive.org/) na data `2020-01-01`:

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
  ## ALTER MODIFY COLUMN para o tipo JSON
</div>

É possível alterar uma tabela existente e mudar o tipo da coluna para o novo tipo `JSON`. No momento, só há suporte a `ALTER` a partir do tipo `String`.

**Exemplo**

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
  ## Lazy Type Hints (Experimental)
</div>

:::note
Este recurso é experimental e exige que a configuração `allow_experimental_json_lazy_type_hints` esteja habilitada.
:::

Quando você adiciona ou modifica indicações de tipo em uma coluna JSON usando `ALTER TABLE ... MODIFY COLUMN`, o ClickHouse normalmente reescreve todas as partes de dados para materializar as novas indicações de tipo. Em tabelas com grandes volumes de dados históricos (centenas de terabytes), isso pode ser extremamente custoso.

**Lazy type hints** permitem adicionar indicações de tipo como uma operação somente de metadados, sem reescrever os dados existentes:

* **Partes antigas**: as indicações de tipo são aplicadas em tempo de consulta, convertendo de `Dynamic` para o tipo indicado
* **Partes novas**: as indicações de tipo são materializadas durante operações `INSERT`
* **Mesclagens**: as indicações de tipo são materializadas quando as partes são mescladas

Isso significa que você pode adicionar indicações de tipo instantaneamente, e os dados serão convertidos gradualmente à medida que ocorrerem as mesclagens normais em segundo plano.

<div id="enabling-lazy-type-hints">
  ### Ativando Lazy Type Hints
</div>

```sql
SET allow_experimental_json_lazy_type_hints = 1;
```

<div id="lazy-type-hints-example">
  ### Exemplo
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
  ### Verificando que não ocorreu nenhuma mutação
</div>

Você pode verificar se o `ALTER` foi concluído sem mutação consultando a tabela `system.mutations`:

```sql
SELECT * FROM system.mutations WHERE table = 'test_lazy' AND NOT is_done;
```

Com Lazy Type Hints ativados, esta consulta não retorna nenhuma linha, confirmando que a operação foi apenas de metadados.

<div id="materializing-type-hints">
  ### Materializando indicações de tipo
</div>

Para materializar indicações de tipo nos dados existentes, você pode:

1. **Aguardar as mesclagens em segundo plano**: o ClickHouse materializa automaticamente as indicações de tipo quando as partes são mescladas
2. **Forçar a mesclagem**: use `OPTIMIZE TABLE test_lazy FINAL` para mesclar todas as partes imediatamente
3. **Reescrever as partes**: use `ALTER TABLE test_lazy REWRITE PARTS` para reescrever as partes com os novos metadados

<div id="lazy-type-hints-limitations">
  ### Limitações
</div>

* Este recurso é experimental e pode mudar em versões futuras
* A conversão de tipos em tempo de consulta pode causar uma sobrecarga significativa de desempenho em comparação com tipos pré-materializados, especialmente em objetos JSON grandes
* O recurso se aplica apenas à modificação de `typed_paths` (indicações de tipo); outros parâmetros JSON, como `max_dynamic_paths`, `SKIP` ou `SKIP REGEXP`, ainda exigem mutações

<div id="comparison-between-values-of-the-json-type">
  ## Comparação entre valores do tipo JSON
</div>

Objetos JSON são comparados de maneira semelhante a Maps.

Por exemplo:

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

**Observação:** quando 2 caminhos contêm valores de tipos de dados diferentes, eles são comparados de acordo com a [regra de comparação](/pt-BR/sql-reference/data-types/variant#comparing-values-of-variant-data) do tipo de dado `Variant`.

<div id="data-skipping-indexes-for-json">
  ## Data skipping indexes para JSON
</div>

[Data skipping indexes](/pt-BR/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) podem ser usados com colunas `JSON` de três maneiras:

1. **Índices em subcolunas específicas** — crie um skip index padrão em um caminho JSON conhecido, assim como em uma coluna comum. Isso indexa os *valores* nesse caminho.
2. **Índices baseados em caminho com `JSONAllPaths`** — indexe o *conjunto de caminhos* presentes em cada grânulo para pular grânulos que não podem conter o caminho consultado.
3. **Índices baseados em valor com `JSONAllValues`** — indexe *todos os valores* em todos os caminhos JSON usando um [índice de texto](/pt-BR/engines/table-engines/mergetree-family/textindexes.md) para acelerar a busca de texto completo em qualquer subcoluna JSON com um único índice.

<div id="json-indexes-on-subcolumns">
  ### Índices em subcolunas específicas
</div>

Você pode criar um skip index em qualquer subcoluna JSON usando a mesma sintaxe das colunas comuns.
Qualquer [tipo de índice compatível](/pt-BR/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) funciona (`minmax`, `set`, `bloom_filter`, `tokenbf_v1`, `ngrambf_v1`, etc.).

Há duas maneiras de referenciar uma subcoluna JSON em uma expressão de índice:

* **Caminho tipado** declarado na indicação de tipo JSON — acesse diretamente pelo nome: `json.a`.
* **Caminho dinâmico** com conversão explícita — use a sintaxe de cast `::`: `json.b::String`.

Você também pode usar expressões que combinam várias subcolunas, por exemplo `json.a || json.b::String`.

<div id="json-indexes-on-subcolumns-example">
  #### Exemplo
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

O índice `minmax` sobre a subcoluna tipada `data.sensor_id` restringe a varredura aos grânulos correspondentes:

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

O índice `bloom_filter` na subcoluna com cast `data.location::String` também funciona:

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
  ### Índices baseados em caminhos com JSONAllPaths
</div>

[Data skipping indexes](/pt-BR/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-data_skipping-indexes) também podem ser criados em colunas `JSON` usando a função [`JSONAllPaths`](/pt-BR/sql-reference/functions/json-functions#JSONAllPaths).
Isso funciona de forma semelhante à criação de skip indexes em colunas [`Map`](/pt-BR/sql-reference/data-types/map) via `mapKeys` — o índice armazena o conjunto de caminhos JSON presentes em cada grânulo e o usa para ignorar grânulos que não podem conter o caminho consultado.

<div id="json-indexes-jsonallpaths-supported-types">
  #### Tipos de índice compatíveis
</div>

`JSONAllPaths` pode ser usado com os seguintes tipos de skip index:

* [`bloom_filter`](/pt-BR/engines/table-engines/mergetree-family/mergetree#bloom-filter) — suporta `equals`, `in` e `IS NOT NULL`.
* [`tokenbf_v1`](/pt-BR/engines/table-engines/mergetree-family/mergetree#token-bloom-filter) — suporta `equals` e `IS NOT NULL`.
* [`ngrambf_v1`](/pt-BR/engines/table-engines/mergetree-family/mergetree#n-gram-bloom-filter) — suporta `equals` e `IS NOT NULL`.
* [`text`](/pt-BR/engines/table-engines/mergetree-family/textindexes) (índice invertido) — suporta `equals`, `in` e `IS NOT NULL`.

<div id="json-indexes-on-subcolumns-example">
  #### Exemplo
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

Você pode usar `EXPLAIN indexes = 1` para verificar se o skip index está sendo usado. Quando um caminho existe apenas em uma parte, o índice ignora a outra parte:

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

Quando um caminho não existe em nenhuma parte, todas as partes e grânulos são ignorados:

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

`IS NOT NULL` também usa o índice — ele ignora os grânulos em que o caminho está ausente (já que o valor seria `NULL`):

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
  #### Como funciona
</div>

A expressão `JSONAllPaths(json_column)` retorna um `Array(String)` que contém todos os caminhos presentes em um valor JSON.
O skip index armazena essas strings de caminho em sua estrutura de dados (`filtro de Bloom` ou índice invertido).
Quando uma consulta filtra por `json.some.path`, o índice verifica se a string `"some.path"` está presente no índice de cada grânulo e ignora os grânulos em que ela está ausente.

<div id="json-indexes-jsonallpaths-safety-with-missing-paths">
  #### Segurança com caminhos ausentes
</div>

Quando um caminho JSON está ausente de um grânulo, a subcoluna é avaliada como:

* `NULL` para o tipo `Dynamic` (por exemplo, `json.path`) e subcolunas tipadas como `Nullable` (por exemplo, `json.path.:Int64`) — comparações com `NULL` sempre retornam `false`, portanto o skipping é seguro.
* O valor padrão do tipo para expressões `CAST` não `Nullable` (por exemplo, `json.path::Int64` produz `0` quando o caminho está ausente) — o skipping é seguro apenas quando o valor comparado difere do valor padrão. O índice lida automaticamente com essa distinção.

<div id="json-indexes-jsonallvalues">
  ### Busca de texto completo com JSONAllValues
</div>

[Índices de texto](/pt-BR/engines/table-engines/mergetree-family/textindexes.md) podem ser usados para acelerar a busca de texto completo em colunas JSON por meio da função [`JSONAllValues`](/pt-BR/sql-reference/functions/json-functions#JSONAllValues).
`JSONAllValues` retorna todos os valores de uma coluna JSON como `Array(String)`, que pode ser indexado por um índice de texto.
Um único índice em `JSONAllValues(json_column)` abrange todos os caminhos JSON, permitindo a busca de texto completo em qualquer subcoluna sem a necessidade de criar índices separados para cada caminho.

Consulte [Índices baseados em valor com JSONAllValues](/pt-BR/engines/table-engines/mergetree-family/textindexes.md#json-indexes-jsonallvalues) na documentação de índices de texto para mais detalhes e exemplos.

<div id="tips-for-better-usage-of-the-json-type">
  ## Dicas para melhor uso do tipo JSON
</div>

Antes de criar uma coluna `JSON` e carregar dados nela, considere as dicas a seguir:

* Analise seus dados e especifique o máximo possível de indicações de caminho com tipos. Isso tornará o armazenamento e a leitura muito mais eficientes.
* Pense em quais caminhos você vai precisar e em quais nunca vai precisar. Especifique os caminhos de que você não precisará na seção `SKIP` e, se necessário, na seção `SKIP REGEXP`. Isso melhorará o armazenamento.
* Não defina o parâmetro `max_dynamic_paths` com valores muito altos, pois isso pode tornar o armazenamento e a leitura menos eficientes.
  Embora isso dependa bastante de parâmetros do sistema, como memória, CPU etc., uma regra prática geral é não definir `max_dynamic_paths` acima de 10 000 para armazenamento no sistema de arquivos local e de 1024 para armazenamento no sistema de arquivos remoto.

<div id="further-reading">
  ## Leituras adicionais
</div>

* [Como criamos um novo e poderoso tipo de dados JSON no ClickHouse](https://clickhouse.com/blog/a-new-powerful-json-data-type-for-clickhouse)
* [O desafio do JSON com um bilhão de documentos: ClickHouse vs. MongoDB, Elasticsearch e outros](https://clickhouse.com/blog/json-bench-clickhouse-vs-mongodb-elasticsearch-duckdb-postgresql)