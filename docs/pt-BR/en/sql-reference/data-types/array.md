---
description: 'Documentação do tipo de dado Array no ClickHouse'
sidebar_label: 'Array(T)'
sidebar_position: 32
slug: /sql-reference/data-types/array
title: 'Array(T)'
doc_type: 'referência'
---

Um array de itens do tipo `T`, com o índice inicial do array igual a 1. `T` pode ser qualquer tipo de dado, incluindo um array.

<div id="creating-an-array">
  ## Como criar um Array
</div>

Você pode usar uma função para criar um array:

```sql
array(T)
```

Você também pode usar `[]`.

```sql
[]
```

Exemplo de como criar um array:

```sql
SELECT array(1, 2) AS x, toTypeName(x)
```

```text
┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘
```

```sql
SELECT [1, 2] AS x, toTypeName(x)
```

```text
┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘
```

<div id="working-with-data-types">
  ## Trabalhando com tipos de dados
</div>

Ao criar um array na hora, o ClickHouse define automaticamente o tipo do argumento como o tipo de dado mais restrito capaz de armazenar todos os argumentos listados. Se houver valores [Nullable](/pt-BR/sql-reference/data-types/nullable) ou [NULL](/pt-BR/operations/settings/formats#input_format_null_as_default) literais, o tipo dos elementos do array também se torna [Nullable](../../sql-reference/data-types/nullable.md).

Se o ClickHouse não conseguir determinar o tipo de dado, ele gera uma exceção. Por exemplo, isso acontece ao tentar criar um array com strings e números ao mesmo tempo (`SELECT array(1, 'a')`).

Exemplos de detecção automática de tipo de dado:

```sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

```text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

Se você tentar criar um array de tipos de dados incompatíveis, o ClickHouse lança uma exceção:

```sql
SELECT array(1, 'a')
```

```text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

<div id="array-size">
  ## Tamanho do Array
</div>

É possível determinar o tamanho de um array usando a subcoluna `size0` sem ler a coluna inteira. Para arrays multidimensionais, você pode usar `sizeN-1`, em que `N` é a dimensão desejada.

**Exemplo**

```sql title="Query"
CREATE TABLE t_arr (`arr` Array(Array(Array(UInt32)))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_arr VALUES ([[[12, 13, 0, 1],[12]]]);

SELECT arr.size0, arr.size1, arr.size2 FROM t_arr;
```

```text title="Response"
┌─arr.size0─┬─arr.size1─┬─arr.size2─┐
│         1 │ [2]       │ [[4,1]]   │
└───────────┴───────────┴───────────┘
```

<div id="reading-nested-subcolumns-from-array">
  ## Lendo subcolunas aninhadas de Array
</div>

Se o tipo aninhado `T` dentro de `Array` tiver subcolunas (por exemplo, se for uma [tupla nomeada](./tuple.md)), você poderá ler essas subcolunas a partir de um tipo `Array(T)` com os mesmos nomes de subcoluna. O tipo de uma subcoluna será um `Array` do tipo da subcoluna original.

**Exemplo**

```sql
CREATE TABLE t_arr (arr Array(Tuple(field1 UInt32, field2 String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_arr VALUES ([(1, 'Hello'), (2, 'World')]), ([(3, 'This'), (4, 'is'), (5, 'subcolumn')]);
SELECT arr.field1, toTypeName(arr.field1), arr.field2, toTypeName(arr.field2) from t_arr;
```

```test
┌─arr.field1─┬─toTypeName(arr.field1)─┬─arr.field2────────────────┬─toTypeName(arr.field2)─┐
│ [1,2]      │ Array(UInt32)          │ ['Hello','World']         │ Array(String)          │
│ [3,4,5]    │ Array(UInt32)          │ ['This','is','subcolumn'] │ Array(String)          │
└────────────┴────────────────────────┴───────────────────────────┴────────────────────────┘
```