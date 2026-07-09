---
description: 'CoalescingMergeTree herda do motor MergeTree. Seu principal recurso
  é a capacidade de armazenar automaticamente o último valor não nulo de cada coluna durante as mesclagens de partes.'
sidebar_label: 'CoalescingMergeTree'
sidebar_position: 50
slug: /engines/table-engines/mergetree-family/coalescingmergetree
title: 'Motor de tabela CoalescingMergeTree'
keywords: ['CoalescingMergeTree']
show_related_blogs: true
doc_type: 'reference'
---

:::note Disponível a partir da versão 25.6
Este motor de tabela está disponível na versão 25.6 e posteriores, tanto em OSS quanto em Cloud.
:::

Este motor herda de [MergeTree](/pt-BR/engines/table-engines/mergetree-family/mergetree). A principal diferença está em como as partes de dados são mescladas: para tabelas `CoalescingMergeTree`, o ClickHouse substitui todas as linhas com a mesma chave primária (ou, mais precisamente, a mesma [chave de ordenação](../../../engines/table-engines/mergetree-family/mergetree.md)) por uma única linha que contém os valores não NULL mais recentes de cada coluna.

Isso permite upserts em nível de coluna, ou seja, você pode atualizar apenas colunas específicas em vez de linhas inteiras.

`CoalescingMergeTree` foi projetado para uso com tipos Nullable em colunas não chave. Se as colunas não forem Nullable, o comportamento será o mesmo de [ReplacingMergeTree](/pt-BR/engines/table-engines/mergetree-family/replacingmergetree).

<div id="creating-a-table">
  ## Criar uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = CoalescingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Para ver uma descrição dos parâmetros da requisição, consulte a [descrição da requisição](../../../sql-reference/statements/create/table.md).

<div id="parameters-of-coalescingmergetree">
  ### Parâmetros do CoalescingMergeTree
</div>

<div id="columns">
  #### Colunas
</div>

`columns` - Opcional. Uma tupla com os nomes das colunas cujos valores serão combinados. As colunas fornecidas não devem estar na partição nem na chave de ordenação. Se `columns` não for especificado, o ClickHouse combina os valores em todas as colunas que não fazem parte da chave de ordenação.

<div id="query-clauses">
  ### Cláusulas de consulta
</div>

Ao criar uma tabela `CoalescingMergeTree`, são necessárias as mesmas [cláusulas](../../../engines/table-engines/mergetree-family/mergetree.md) usadas na criação de uma tabela `MergeTree`.

<details markdown="1">
  <summary>Método obsoleto para criar uma tabela</summary>

  :::note
  Não use este método em novos projetos e, se possível, migre os projetos antigos para o método descrito acima.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] CoalescingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
  ```

  Todos os parâmetros, exceto `columns`, têm o mesmo significado de `MergeTree`.

  * `columns` — tupla com os nomes das colunas cujos valores serão somados. Parâmetro opcional. Para uma descrição, consulte o texto acima.
</details>

<div id="usage-example">
  ## Exemplo de uso
</div>

Considere a seguinte tabela:

```sql
CREATE TABLE test_table
(
    key UInt64,
    value_int Nullable(UInt32),
    value_string Nullable(String),
    value_date Nullable(Date)
)
ENGINE = CoalescingMergeTree()
ORDER BY key
```

Insira dados nele:

```sql
INSERT INTO test_table VALUES(1, NULL, NULL, '2025-01-01'), (2, 10, 'test', NULL);
INSERT INTO test_table VALUES(1, 42, 'win', '2025-02-01');
INSERT INTO test_table(key, value_date) VALUES(2, '2025-02-01');
```

O resultado ficará assim:

```sql
SELECT * FROM test_table ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   1 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-01-01 │
│   2 │      ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ         │ 2025-02-01 │
│   2 │        10 │ test         │       ᴺᵁᴸᴸ │
└─────┴───────────┴──────────────┴────────────┘
```

Consulta recomendada para obter o resultado correto e definitivo:

```sql
SELECT * FROM test_table FINAL ORDER BY key;
```

```text
┌─key─┬─value_int─┬─value_string─┬─value_date─┐
│   1 │        42 │ win          │ 2025-02-01 │
│   2 │        10 │ test         │ 2025-02-01 │
└─────┴───────────┴──────────────┴────────────┘
```

Usar o modificador `FINAL` força o ClickHouse a aplicar a lógica de merge no momento da consulta, garantindo que você obtenha o valor &quot;mais recente&quot; correto e consolidado para cada coluna. Esse é o método mais seguro e preciso ao consultar uma tabela CoalescingMergeTree.

:::note

Uma abordagem com `GROUP BY` pode retornar resultados incorretos se as partes de dados subjacentes não tiverem sido totalmente mescladas.

```sql
SELECT key, last_value(value_int), last_value(value_string), last_value(value_date)  FROM test_table GROUP BY key; -- Not recommended.
```

:::

<div id="tuple-element-aggregation">
  ## Agregação de elementos de Tuple
</div>

Quando a configuração `allow_tuple_element_aggregation` está habilitada, as colunas `Tuple` são achatadas recursivamente para que cada elemento terminal participe da coalescência de forma independente. Isso permite armazenar vários campos em uma única coluna `Tuple` e fazer com que eles sejam coalescidos elemento a elemento durante as mesclagens — cada subcoluna `Nullable` mantém, de forma independente, o valor não `NULL` mais recente.

As mesmas regras se aplicam às subcolunas achatadas e às colunas regulares:

* As subcolunas que pertencem a um `Tuple` na chave de ordenação ou na chave de partição são excluídas da coalescência.
* Se `columns` for especificado, apenas as subcolunas das colunas `Tuple` listadas serão coalescidas.

:::note
Essa configuração é imutável e deve ser especificada no momento da criação da tabela.
:::

```sql
CREATE TABLE coalescing_tuples
(
    key UInt64,
    data Tuple(
        value_a Nullable(UInt64),
        value_b Nullable(String),
        nested Tuple(
            value_c Nullable(UInt64)
        )
    )
) ENGINE = CoalescingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO coalescing_tuples VALUES (1, (100, NULL, (NULL)));
INSERT INTO coalescing_tuples VALUES (1, (NULL, 'hello', (42)));

SELECT key, data.value_a, data.value_b, data.nested.value_c FROM coalescing_tuples FINAL;
```

```text
┌─key─┬─data.value_a─┬─data.value_b─┬─data.nested.value_c─┐
│   1 │          100 │ hello        │                  42 │
└─────┴──────────────┴──────────────┴─────────────────────┘
```