---
description: 'Documentação do tipo de dado Variant no ClickHouse'
sidebar_label: 'Variant(T1, T2, ...)'
sidebar_position: 40
slug: /sql-reference/data-types/variant
title: 'Variant(T1, T2, ...)'
doc_type: 'reference'
---

Esse tipo representa uma união de outros tipos de dado. O tipo `Variant(T1, T2, ..., TN)` significa que cada linha desse tipo
tem um valor do tipo `T1`, `T2`, ... `TN` ou nenhum deles (valor `NULL`).

A ordem dos tipos aninhados não importa: Variant(T1, T2) = Variant(T2, T1).
Os tipos aninhados podem ser quaisquer tipos, exceto os tipos Nullable(...), LowCardinality(Nullable(...)) e Variant(...).

:::note
Não é recomendável usar tipos semelhantes como variants (por exemplo, tipos numéricos diferentes como `Variant(UInt32, Int64)` ou tipos Date diferentes como `Variant(Date, DateTime)`),
porque trabalhar com valores desses tipos pode gerar ambiguidades. Por padrão, criar esse tipo `Variant` resultará em uma exceção, mas isso pode ser habilitado com a configuração `allow_suspicious_variant_types`
:::

<div id="creating-variant">
  ## Criando Variant
</div>

Usando o tipo `Variant` na definição da coluna de uma tabela:

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT v FROM test;
```

```text
┌─v─────────────┐
│ ᴺᵁᴸᴸ          │
│ 42            │
│ Hello, World! │
│ [1,2,3]       │
└───────────────┘
```

Usando CAST em colunas comuns:

```sql
SELECT toTypeName(variant) AS type_name, 'Hello, World!'::Variant(UInt64, String, Array(UInt64)) as variant;
```

```text
┌─type_name──────────────────────────────┬─variant───────┐
│ Variant(Array(UInt64), String, UInt64) │ Hello, World! │
└────────────────────────────────────────┴───────────────┘
```

Usando as funções `if/multiIf` quando os argumentos não têm um tipo comum (a configuração `use_variant_as_common_type` deve estar habilitada para isso):

```sql
SET use_variant_as_common_type = 1;
SELECT if(number % 2, number, range(number)) as variant FROM numbers(5);
```

```text
┌─variant───┐
│ []        │
│ 1         │
│ [0,1]     │
│ 3         │
│ [0,1,2,3] │
└───────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT multiIf((number % 4) = 0, 42, (number % 4) = 1, [1, 2, 3], (number % 4) = 2, 'Hello, World!', NULL) AS variant FROM numbers(4);
```

```text
┌─variant───────┐
│ 42            │
│ [1,2,3]       │
│ Hello, World! │
│ ᴺᵁᴸᴸ          │
└───────────────┘
```

Usando as funções &#39;array/map&#39; se os elementos do array/os valores do map não tiverem um tipo em comum (a configuração `use_variant_as_common_type` deve estar habilitada para isso):

```sql
SET use_variant_as_common_type = 1;
SELECT array(range(number), number, 'str_' || toString(number)) as array_of_variants FROM numbers(3);
```

```text
┌─array_of_variants─┐
│ [[],0,'str_0']    │
│ [[0],1,'str_1']   │
│ [[0,1],2,'str_2'] │
└───────────────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT map('a', range(number), 'b', number, 'c', 'str_' || toString(number)) as map_of_variants FROM numbers(3);
```

```text
┌─map_of_variants───────────────┐
│ {'a':[],'b':0,'c':'str_0'}    │
│ {'a':[0],'b':1,'c':'str_1'}   │
│ {'a':[0,1],'b':2,'c':'str_2'} │
└───────────────────────────────┘
```

<div id="reading-variant-nested-types-as-subcolumns">
  ## Leitura de tipos aninhados de Variant como subcolunas
</div>

O tipo Variant oferece suporte à leitura de um único tipo aninhado de uma coluna Variant usando o nome do tipo como subcoluna.
Assim, se você tiver a coluna `variant Variant(T1, T2, T3)`, poderá ler uma subcoluna do tipo `T2` usando a sintaxe `variant.T2`;
essa subcoluna terá o tipo `Nullable(T2)` se `T2` puder estar dentro de `Nullable`, e `T2` caso contrário. Essa subcoluna
terá o mesmo tamanho que a coluna `Variant` original e conterá valores `NULL` (ou valores vazios, se `T2` não puder estar dentro de `Nullable`)
em todas as linhas em que a coluna `Variant` original não tiver o tipo `T2`.

As subcolunas de Variant também podem ser lidas usando a função `variantElement(variant_column, type_name)`.

Exemplos:

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT v, v.String, v.UInt64, v.`Array(UInt64)` FROM test;
```

```text
┌─v─────────────┬─v.String──────┬─v.UInt64─┬─v.Array(UInt64)─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ          │     ᴺᵁᴸᴸ │ []              │
│ 42            │ ᴺᵁᴸᴸ          │       42 │ []              │
│ Hello, World! │ Hello, World! │     ᴺᵁᴸᴸ │ []              │
│ [1,2,3]       │ ᴺᵁᴸᴸ          │     ᴺᵁᴸᴸ │ [1,2,3]         │
└───────────────┴───────────────┴──────────┴─────────────────┘
```

```sql
SELECT toTypeName(v.String), toTypeName(v.UInt64), toTypeName(v.`Array(UInt64)`) FROM test LIMIT 1;
```

```text
┌─toTypeName(v.String)─┬─toTypeName(v.UInt64)─┬─toTypeName(v.Array(UInt64))─┐
│ Nullable(String)     │ Nullable(UInt64)     │ Array(UInt64)               │
└──────────────────────┴──────────────────────┴─────────────────────────────┘
```

```sql
SELECT v, variantElement(v, 'String'), variantElement(v, 'UInt64'), variantElement(v, 'Array(UInt64)') FROM test;
```

```text
┌─v─────────────┬─variantElement(v, 'String')─┬─variantElement(v, 'UInt64')─┬─variantElement(v, 'Array(UInt64)')─┐
│ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ                        │                        ᴺᵁᴸᴸ │ []                                 │
│ 42            │ ᴺᵁᴸᴸ                        │                          42 │ []                                 │
│ Hello, World! │ Hello, World!               │                        ᴺᵁᴸᴸ │ []                                 │
│ [1,2,3]       │ ᴺᵁᴸᴸ                        │                        ᴺᵁᴸᴸ │ [1,2,3]                            │
└───────────────┴─────────────────────────────┴─────────────────────────────┴────────────────────────────────────┘
```

Para saber qual variante está armazenada em cada linha, é possível usar a função `variantType(variant_column)`. Ela retorna um `Enum` com o nome do tipo Variant de cada linha (ou `'None'` se a linha for `NULL`).

Exemplo:

```sql
CREATE TABLE test (v Variant(UInt64, String, Array(UInt64))) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('Hello, World!'), ([1, 2, 3]);
SELECT variantType(v) FROM test;
```

```text
┌─variantType(v)─┐
│ None           │
│ UInt64         │
│ String         │
│ Array(UInt64)  │
└────────────────┘
```

```sql
SELECT toTypeName(variantType(v)) FROM test LIMIT 1;
```

```text
┌─toTypeName(variantType(v))──────────────────────────────────────────┐
│ Enum8('None' = -1, 'Array(UInt64)' = 0, 'String' = 1, 'UInt64' = 2) │
└─────────────────────────────────────────────────────────────────────┘
```

<div id="conversion-between-a-variant-column-and-other-columns">
  ## Conversão entre uma coluna do tipo Variant e outras colunas
</div>

Há 4 conversões possíveis para uma coluna do tipo `Variant`.

<div id="converting-a-string-column-to-a-variant-column">
  ### Convertendo uma coluna String em uma coluna Variant
</div>

A conversão de `String` para `Variant` é feita por meio do parsing de um valor do tipo `Variant` a partir do valor da string:

```sql
SELECT '42'::Variant(String, UInt64) AS variant, variantType(variant) AS variant_type
```

```text
┌─variant─┬─variant_type─┐
│ 42      │ UInt64       │
└─────────┴──────────────┘
```

```sql
SELECT '[1, 2, 3]'::Variant(String, Array(UInt64)) as variant, variantType(variant) as variant_type
```

```text
┌─variant─┬─variant_type──┐
│ [1,2,3] │ Array(UInt64) │
└─────────┴───────────────┘
```

````sql
SELECT CAST(map('key1', '42', 'key2', 'true', 'key3', '2020-01-01'), 'Map(String, Variant(UInt64, Bool, Date))') AS map_of_variants, mapApply((k, v) -> (k, variantType(v)), map_of_variants) AS map_of_variant_types```
````

```text
┌─map_of_variants─────────────────────────────┬─map_of_variant_types──────────────────────────┐
│ {'key1':42,'key2':true,'key3':'2020-01-01'} │ {'key1':'UInt64','key2':'Bool','key3':'Date'} │
└─────────────────────────────────────────────┴───────────────────────────────────────────────┘
```

Para desativar o parsing durante a conversão de `String` para `Variant`, é possível desativar a configuração `cast_string_to_dynamic_use_inference`:

```sql
SET cast_string_to_variant_use_inference = 0;
SELECT '[1, 2, 3]'::Variant(String, Array(UInt64)) as variant, variantType(variant) as variant_type
```

```text
┌─variant───┬─variant_type─┐
│ [1, 2, 3] │ String       │
└───────────┴──────────────┘
```

<div id="converting-an-ordinary-column-to-a-variant-column">
  ### Convertendo uma coluna comum em uma coluna Variant
</div>

É possível converter uma coluna comum do tipo `T` em uma coluna `Variant` contendo esse tipo:

```sql
SELECT toTypeName(variant) AS type_name, [1,2,3]::Array(UInt64)::Variant(UInt64, String, Array(UInt64)) as variant, variantType(variant) as variant_name
```

```text
┌─type_name──────────────────────────────┬─variant─┬─variant_name──┐
│ Variant(Array(UInt64), String, UInt64) │ [1,2,3] │ Array(UInt64) │
└────────────────────────────────────────┴─────────┴───────────────┘
```

Observação: a conversão do tipo `String` é sempre feita por meio de parsing; se você precisar converter uma coluna `String` para a variante `String` de um `Variant` sem parsing, poderá fazer o seguinte:

```sql
SELECT '[1, 2, 3]'::Variant(String)::Variant(String, Array(UInt64), UInt64) as variant, variantType(variant) as variant_type
```

```sql
┌─variant───┬─variant_type─┐
│ [1, 2, 3] │ String       │
└───────────┴──────────────┘
```

<div id="converting-a-variant-column-to-an-ordinary-column">
  ### Convertendo uma coluna Variant em uma coluna comum
</div>

É possível converter uma coluna `Variant` em uma coluna comum. Nesse caso, todas as variantes aninhadas serão convertidas para um tipo de destino:

```sql
CREATE TABLE test (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('42.42');
SELECT v::Nullable(Float64) FROM test;
```

```text
┌─CAST(v, 'Nullable(Float64)')─┐
│                         ᴺᵁᴸᴸ │
│                           42 │
│                        42.42 │
└──────────────────────────────┘
```

<div id="converting-a-variant-to-another-variant">
  ### Convertendo uma Variant em outra Variant
</div>

É possível converter uma coluna `Variant` em outra coluna `Variant`, mas somente se a coluna `Variant` de destino contiver todos os tipos aninhados da coluna `Variant` original:

```sql
CREATE TABLE test (v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO test VALUES (NULL), (42), ('String');
SELECT v::Variant(UInt64, String, Array(UInt64)) FROM test;
```

```text
┌─CAST(v, 'Variant(UInt64, String, Array(UInt64))')─┐
│ ᴺᵁᴸᴸ                                              │
│ 42                                                │
│ String                                            │
└───────────────────────────────────────────────────┘
```

<div id="reading-variant-type-from-the-data">
  ## Leitura do tipo Variant a partir dos dados
</div>

Todos os formatos de texto (TSV, CSV, CustomSeparated, Values, JSONEachRow etc.) oferecem suporte à leitura do tipo `Variant`. Durante o parsing dos dados, o ClickHouse tenta inserir o valor no tipo Variant mais apropriado.

Exemplo:

```sql
SELECT
    v,
    variantElement(v, 'String') AS str,
    variantElement(v, 'UInt64') AS num,
    variantElement(v, 'Float64') AS float,
    variantElement(v, 'DateTime') AS date,
    variantElement(v, 'Array(UInt64)') AS arr
FROM format(JSONEachRow, 'v Variant(String, UInt64, Float64, DateTime, Array(UInt64))', $$
{"v" : "Hello, World!"},
{"v" : 42},
{"v" : 42.42},
{"v" : "2020-01-01 00:00:00"},
{"v" : [1, 2, 3]}
$$)
```

```text
┌─v───────────────────┬─str───────────┬──num─┬─float─┬────────────────date─┬─arr─────┐
│ Hello, World!       │ Hello, World! │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │                ᴺᵁᴸᴸ │ []      │
│ 42                  │ ᴺᵁᴸᴸ          │   42 │  ᴺᵁᴸᴸ │                ᴺᵁᴸᴸ │ []      │
│ 42.42               │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │ 42.42 │                ᴺᵁᴸᴸ │ []      │
│ 2020-01-01 00:00:00 │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │ 2020-01-01 00:00:00 │ []      │
│ [1,2,3]             │ ᴺᵁᴸᴸ          │ ᴺᵁᴸᴸ │  ᴺᵁᴸᴸ │                ᴺᵁᴸᴸ │ [1,2,3] │
└─────────────────────┴───────────────┴──────┴───────┴─────────────────────┴─────────┘
```

<div id="comparing-values-of-variant-data">
  ## Comparando valores do tipo Variant
</div>

Valores do tipo `Variant` só podem ser comparados com valores do mesmo tipo `Variant`.

Por padrão, os operadores de comparação usam a [implementação padrão para Variant](#functions-with-variant-arguments),
aplicando a comparação separadamente a cada tipo Variant. Esse comportamento pode ser desativado com a configuração `use_variant_default_implementation_for_comparisons = 0`
para usar as regras nativas de comparação de Variant descritas abaixo. **Observe** que `ORDER BY` sempre usa comparação nativa.

**Regras nativas de comparação de Variant:**

O resultado do operador `<` para os valores `v1`, com tipo subjacente `T1`, e `v2`, com tipo subjacente `T2`, de um tipo `Variant(..., T1, ... T2, ...)` é definido da seguinte forma:

* Se `T1 = T2 = T`, o resultado será `v1.T < v2.T` (os valores subjacentes serão comparados).
* Se `T1 != T2`, o resultado será `T1 < T2` (os nomes dos tipos serão comparados).

Exemplos:

```sql
SET allow_suspicious_types_in_order_by = 1;
CREATE TABLE test (v1 Variant(String, UInt64, Array(UInt32)), v2 Variant(String, UInt64, Array(UInt32))) ENGINE=Memory;
INSERT INTO test VALUES (42, 42), (42, 43), (42, 'abc'), (42, [1, 2, 3]), (42, []), (42, NULL);
```

```sql
SELECT v2, variantType(v2) AS v2_type FROM test ORDER BY v2;
```

```text
┌─v2──────┬─v2_type───────┐
│ []      │ Array(UInt32) │
│ [1,2,3] │ Array(UInt32) │
│ abc     │ String        │
│ 42      │ UInt64        │
│ 43      │ UInt64        │
│ ᴺᵁᴸᴸ    │ None          │
└─────────┴───────────────┘
```

```sql
SELECT v1, variantType(v1) AS v1_type, v2, variantType(v2) AS v2_type, v1 = v2, v1 < v2, v1 > v2 FROM test;
```

```text
┌─v1─┬─v1_type─┬─v2──────┬─v2_type───────┬─equals(v1, v2)─┬─less(v1, v2)─┬─greater(v1, v2)─┐
│ 42 │ UInt64  │ 42      │ UInt64        │              1 │            0 │               0 │
│ 42 │ UInt64  │ 43      │ UInt64        │              0 │            1 │               0 │
│ 42 │ UInt64  │ abc     │ String        │              0 │            0 │               1 │
│ 42 │ UInt64  │ [1,2,3] │ Array(UInt32) │              0 │            0 │               1 │
│ 42 │ UInt64  │ []      │ Array(UInt32) │              0 │            0 │               1 │
│ 42 │ UInt64  │ ᴺᵁᴸᴸ    │ None          │              0 │            1 │               0 │
└────┴─────────┴─────────┴───────────────┴────────────────┴──────────────┴─────────────────┘

```

Se você precisar encontrar a linha com um valor específico de `Variant`, poderá fazer uma das seguintes opções:

* Faça CAST do valor para o tipo `Variant` correspondente:

```sql
SELECT * FROM test WHERE v2 == [1,2,3]::Array(UInt32)::Variant(String, UInt64, Array(UInt32));
```

```text
┌─v1─┬─v2──────┐
│ 42 │ [1,2,3] │
└────┴─────────┘
```

* Compare a subcoluna `Variant` com o tipo necessário:

```sql
SELECT * FROM test WHERE v2.`Array(UInt32)` == [1,2,3] -- or using variantElement(v2, 'Array(UInt32)')
```

```text
┌─v1─┬─v2──────┐
│ 42 │ [1,2,3] │
└────┴─────────┘
```

Às vezes, pode ser útil fazer uma verificação adicional do tipo Variant, pois subcolunas com tipos complexos como `Array/Map/Tuple` não podem estar dentro de `Nullable` e terão valores padrão em vez de `NULL` nas linhas com tipos diferentes:

```sql
SELECT v2, v2.`Array(UInt32)`, variantType(v2) FROM test WHERE v2.`Array(UInt32)` == [];
```

```text
┌─v2───┬─v2.Array(UInt32)─┬─variantType(v2)─┐
│ 42   │ []               │ UInt64          │
│ 43   │ []               │ UInt64          │
│ abc  │ []               │ String          │
│ []   │ []               │ Array(UInt32)   │
│ ᴺᵁᴸᴸ │ []               │ None            │
└──────┴──────────────────┴─────────────────┘
```

```sql
SELECT v2, v2.`Array(UInt32)`, variantType(v2) FROM test WHERE variantType(v2) == 'Array(UInt32)' AND v2.`Array(UInt32)` == [];
```

```text
┌─v2─┬─v2.Array(UInt32)─┬─variantType(v2)─┐
│ [] │ []               │ Array(UInt32)   │
└────┴──────────────────┴─────────────────┘
```

**Observação:** valores de variantes com tipos numéricos diferentes são considerados variantes distintas e não são comparados entre si; em vez disso, seus nomes de tipo são comparados.

Exemplo:

```sql
SET allow_suspicious_variant_types = 1;
CREATE TABLE test (v Variant(UInt32, Int64)) ENGINE=Memory;
INSERT INTO test VALUES (1::UInt32), (1::Int64), (100::UInt32), (100::Int64);
SELECT v, variantType(v) FROM test ORDER by v;
```

```text
┌─v───┬─variantType(v)─┐
│ 1   │ Int64          │
│ 100 │ Int64          │
│ 1   │ UInt32         │
│ 100 │ UInt32         │
└─────┴────────────────┘
```

**Nota:** por padrão, o tipo `Variant` não é permitido em chaves `GROUP BY`/`ORDER BY`; se quiser usá-lo, considere sua regra especial de comparação e habilite as configurações `allow_suspicious_types_in_group_by`/`allow_suspicious_types_in_order_by`.

<div id="jsonextract-functions-with-variant">
  ## Funções JSONExtract com Variant
</div>

Todas as funções `JSONExtract*` suportam o tipo `Variant`:

```sql
SELECT JSONExtract('{"a" : [1, 2, 3]}', 'a', 'Variant(UInt32, String, Array(UInt32))') AS variant, variantType(variant) AS variant_type;
```

```text
┌─variant─┬─variant_type──┐
│ [1,2,3] │ Array(UInt32) │
└─────────┴───────────────┘
```

```sql
SELECT JSONExtract('{"obj" : {"a" : 42, "b" : "Hello", "c" : [1,2,3]}}', 'obj', 'Map(String, Variant(UInt32, String, Array(UInt32)))') AS map_of_variants, mapApply((k, v) -> (k, variantType(v)), map_of_variants) AS map_of_variant_types
```

```text
┌─map_of_variants──────────────────┬─map_of_variant_types────────────────────────────┐
│ {'a':42,'b':'Hello','c':[1,2,3]} │ {'a':'UInt32','b':'String','c':'Array(UInt32)'} │
└──────────────────────────────────┴─────────────────────────────────────────────────┘
```

```sql
SELECT JSONExtractKeysAndValues('{"a" : 42, "b" : "Hello", "c" : [1,2,3]}', 'Variant(UInt32, String, Array(UInt32))') AS variants, arrayMap(x -> (x.1, variantType(x.2)), variants) AS variant_types
```

```text
┌─variants───────────────────────────────┬─variant_types─────────────────────────────────────────┐
│ [('a',42),('b','Hello'),('c',[1,2,3])] │ [('a','UInt32'),('b','String'),('c','Array(UInt32)')] │
└────────────────────────────────────────┴───────────────────────────────────────────────────────┘
```

<div id="functions-with-variant-arguments">
  ## Funções com argumentos Variant
</div>

A maioria das funções no ClickHouse oferece suporte automático a argumentos do tipo `Variant` por meio de uma **implementação padrão para Variant**.
A partir da versão `26.1`, quando uma função que não lida explicitamente com tipos Variant recebe uma coluna Variant, o ClickHouse:

1. Extrai cada tipo Variant da coluna Variant
2. Executa a função separadamente para cada tipo Variant
3. Combina os resultados adequadamente com base nos tipos de resultado

Isso permite usar funções regulares com colunas Variant sem tratamento especial.

**Exemplo:**

```sql
CREATE TABLE test (v Variant(UInt32, String)) ENGINE = Memory;
INSERT INTO test VALUES (42), ('hello'), (NULL);
SELECT *, toTypeName(v) FROM test WHERE v = 42;
```

```text
   ┌─v──┬─toTypeName(v)───────────┐
1. │ 42 │ Variant(String, UInt32) │
   └────┴─────────────────────────┘
```

O operador de comparação é aplicado automaticamente a cada tipo Variant separadamente, permitindo filtrar colunas Variant.

**Comportamento do tipo de resultado:**

O tipo de resultado depende do que a função retorna para cada variant:

* **Tipos de resultado diferentes**: `Variant(T1, T2, ...)`

  ```sql
  CREATE TABLE test2 (v Variant(UInt64, Float64)) ENGINE = Memory;
  INSERT INTO test2 VALUES (42::UInt64), (42.42);
  SELECT v + 1 AS result, toTypeName(result) FROM test2;
  ```

  ```text
  ┌─result─┬─toTypeName(plus(v, 1))──┐
  │     43 │ Variant(Float64, UInt64) │
  │  43.42 │ Variant(Float64, UInt64) │
  └────────┴─────────────────────────┘
  ```

* **Incompatibilidade de tipos**: `NULL` para variants incompatíveis

  ```sql
  CREATE TABLE test3 (v Variant(Array(UInt32), UInt32)) ENGINE = Memory;
  INSERT INTO test3 VALUES ([1,2,3]), (42);
  SELECT v + 10 AS result, toTypeName(result) FROM test3;
  ```

  ```text
  ┌─result─┬─toTypeName(plus(v, 10))─┐
  │   ᴺᵁᴸᴸ │ Nullable(UInt64)        │
  │     52 │ Nullable(UInt64)        │
  └────────┴─────────────────────────┘
  ```

:::note
**Tratamento de erros:** Quando uma função não consegue processar um tipo Variant, apenas erros relacionados a tipo (ILLEGAL&#95;TYPE&#95;OF&#95;ARGUMENT,
TYPE&#95;MISMATCH, CANNOT&#95;CONVERT&#95;TYPE, NO&#95;COMMON&#95;TYPE) são capturados e resultam em NULL para essas linhas. Outros erros, como
divisão por zero ou falta de memória, são gerados normalmente para evitar ocultar problemas reais silenciosamente.
:::

<div id="variant-type-mismatch-behavior">
  ### Comportamento em caso de incompatibilidade de tipo
</div>

A configuração `variant_throw_on_type_mismatch` controla o que acontece quando uma função é aplicada a uma coluna `Variant` e o tipo realmente armazenado em uma linha é incompatível com a função:

* `true` (padrão) — lançar uma exceção (`ILLEGAL_TYPE_OF_ARGUMENT`) na primeira linha incompatível.
* `false` — retornar `NULL` para as linhas incompatíveis e manter o resultado para as linhas compatíveis.

**Exemplo:**

```sql
CREATE TABLE test (v Variant(String, UInt64)) ENGINE = Memory;
INSERT INTO test VALUES ('hello'), (42), ('foo');

-- Default (throw on mismatch): length() does not accept UInt64, so the query throws.
SELECT length(v) FROM test;  -- throws ILLEGAL_TYPE_OF_ARGUMENT

-- With throw disabled: incompatible rows return NULL.
SET variant_throw_on_type_mismatch = false;
SELECT v, length(v) FROM test ORDER BY v::String NULLS LAST;
```

```text
┌─v─────┬─length(v)─┐
│ foo   │         3 │
│ hello │         5 │
│ 42    │      ᴺᵁᴸᴸ │
└───────┴───────────┘
```