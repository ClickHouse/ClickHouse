---
description: 'Página com detalhes sobre o analisador de consultas do ClickHouse'
keywords: ['analisador']
sidebar_label: 'Analisador'
slug: /operations/analyzer
title: 'Analisador'
doc_type: 'reference'
---

Na versão `24.3` do ClickHouse, o novo analisador de consultas foi ativado por padrão.
Você pode ler mais detalhes sobre como ele funciona [aqui](/pt-BR/guides/developer/understanding-query-execution-with-the-analyzer#analyzer).

<div id="known-incompatibilities">
  ## Incompatibilidades conhecidas
</div>

Apesar de corrigir um grande número de bugs e introduzir novas otimizações, esta versão também traz algumas mudanças incompatíveis no comportamento do ClickHouse. Leia as alterações abaixo para entender como reescrever suas consultas para o analisador.

<div id="invalid-queries-are-no-longer-optimized">
  ### Consultas inválidas não são mais otimizadas
</div>

A infraestrutura anterior de planejamento de consultas aplicava otimizações no nível da AST antes da etapa de validação da consulta.
As otimizações podiam reescrever a consulta inicial para torná-la válida e executável.

No analisador, a validação da consulta ocorre antes da etapa de otimização.
Isso significa que consultas inválidas que antes podiam ser executadas agora não têm mais suporte.
Nesses casos, a consulta deve ser corrigida manualmente.

<div id="example-1">
  #### Exemplo 1
</div>

A consulta a seguir usa a coluna `number` na lista de projeção quando apenas `toString(number)` fica disponível após a agregação.
No analisador antigo, `GROUP BY toString(number)` era otimizado para `GROUP BY number,`, o que tornava a consulta válida.

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

<div id="example-2">
  #### Exemplo 2
</div>

O mesmo problema ocorre nesta consulta. A coluna `number` é usada após a agregação com outra chave.
O analisador de consultas anterior corrigiu esta consulta movendo o filtro `number > 5` da cláusula `HAVING` para a cláusula `WHERE`.

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
GROUP BY n
HAVING number > 5
```

Para corrigir a consulta, você deve mover todas as condições aplicáveis a colunas não agregadas para a cláusula `WHERE`, em conformidade com a sintaxe SQL padrão:

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
WHERE number > 5
GROUP BY n
```

<div id="create-view-with-invalid-query">
  ### `CREATE VIEW` com uma consulta inválida
</div>

O analisador sempre faz a verificação de tipos.
Antes, era possível criar uma `VIEW` com uma consulta `SELECT` inválida.
A falha só aparecia no primeiro `SELECT` ou `INSERT` (no caso de `MATERIALIZED VIEW`).

Não é mais possível criar uma `VIEW` dessa maneira.

<div id="example-view">
  #### Exemplo
</div>

```sql
CREATE TABLE source (data String)
ENGINE=MergeTree
ORDER BY tuple();

CREATE VIEW some_view
AS SELECT JSONExtract(data, 'test', 'DateTime64(3)')
FROM source;
```

<div id="known-incompatibilities-of-the-join-clause">
  ### Incompatibilidades conhecidas da cláusula `JOIN`
</div>

<div id="join-using-column-from-projection">
  #### `JOIN` usando uma coluna de uma projeção
</div>

Por padrão, um alias da lista `SELECT` não pode ser usado como chave em `JOIN USING`.

Uma nova configuração, `analyzer_compatibility_join_using_top_level_identifier`, quando ativada, altera o comportamento de `JOIN USING` para priorizar a resolução de identificadores com base em expressões da lista de projeção da consulta `SELECT`, em vez de usar diretamente as colunas da tabela à esquerda.

Por exemplo:

```sql
SELECT a + 1 AS b, t2.s
FROM VALUES('a UInt64, b UInt64', (1, 1)) AS t1
JOIN VALUES('b UInt64, s String', (1, 'one'), (2, 'two')) t2
USING (b);
```

Com `analyzer_compatibility_join_using_top_level_identifier` definido como `true`, a condição de join é interpretada como `t1.a + 1 = t2.b`, em conformidade com o comportamento das versões anteriores.
O resultado será `2, 'two'`.
Quando a configuração é `false`, a condição de join assume por padrão `t1.b = t2.b`, e a consulta retornará `2, 'one'`.
Se `b` não estiver presente em `t1`, a consulta falhará com erro.

<div id="changes-in-behavior-with-join-using-and-aliasmaterialized-columns">
  #### Mudanças de comportamento com `JOIN USING` e colunas `ALIAS`/`MATERIALIZED`
</div>

No analisador, o uso de `*` em uma consulta `JOIN USING` que envolva colunas `ALIAS` ou `MATERIALIZED` incluirá essas colunas no conjunto de resultados por padrão.

Por exemplo:

```sql
CREATE TABLE t1 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1), (2);

CREATE TABLE t2 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (2), (3);

SELECT * FROM t1
FULL JOIN t2 USING (payload);
```

No analisador, o resultado desta consulta incluirá a coluna `payload`, bem como `id` de ambas as tabelas.
Em contraste, o analisador anterior só incluiria essas colunas `ALIAS` se configurações específicas (`asterisk_include_alias_columns` ou `asterisk_include_materialized_columns`) estivessem habilitadas,
e as colunas poderiam aparecer em uma ordem diferente.

Para garantir resultados consistentes e previsíveis, especialmente ao migrar consultas antigas para o analisador, é recomendável especificar explicitamente as colunas na cláusula `SELECT` em vez de usar `*`.

<div id="handling-of-type-modifiers-for-columns-in-using-clause">
  #### Tratamento de modificadores de tipo para colunas na cláusula `USING`
</div>

Na nova versão do analisador, as regras para determinar o supertipo comum de colunas especificadas na cláusula `USING` foram padronizadas para produzir resultados mais previsíveis,
especialmente ao lidar com modificadores de tipo como `LowCardinality` e `Nullable`.

* `LowCardinality(T)` e `T`: Quando uma coluna do tipo `LowCardinality(T)` é combinada com uma coluna do tipo `T`, o supertipo comum resultante será `T`, descartando efetivamente o modificador `LowCardinality`.
* `Nullable(T)` e `T`: Quando uma coluna do tipo `Nullable(T)` é combinada com uma coluna do tipo `T`, o supertipo comum resultante será `Nullable(T)`, garantindo que a propriedade `Nullable` seja preservada.

Por exemplo:

```sql
SELECT id, toTypeName(id)
FROM VALUES('id LowCardinality(String)', ('a')) AS t1
FULL OUTER JOIN VALUES('id String', ('b')) AS t2
USING (id);
```

Nesta consulta, o supertipo comum de `id` é determinado como `String`, descartando o modificador `LowCardinality` de `t1`.

<div id="projection-column-names-changes">
  ### Alterações nos nomes das colunas de projeção
</div>

Ao calcular os nomes da projeção, os aliases não são substituídos.

```sql
SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 0
FORMAT PrettyCompact

   ┌─x─┬─plus(plus(1, 1), 1)─┐
1. │ 2 │                   3 │
   └───┴─────────────────────┘

SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 1
FORMAT PrettyCompact

   ┌─x─┬─plus(x, 1)─┐
1. │ 2 │          3 │
   └───┴────────────┘
```

<div id="incompatible-function-arguments-types">
  ### Tipos incompatíveis nos argumentos de função
</div>

No analisador, a inferência de tipos ocorre durante a análise da consulta inicial.
Essa mudança significa que as verificações de tipo são feitas antes da avaliação em curto-circuito; portanto, os argumentos da função `if` devem sempre ter um supertipo comum.

Por exemplo, a consulta a seguir falha com `There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not`:

```sql
SELECT toTypeName(if(0, [2, 3, 4], 'String'))
```

<div id="heterogeneous-clusters">
  ### Clusters heterogêneos
</div>

O analisador altera significativamente o protocolo de comunicação entre os servidores do cluster. Portanto, é impossível executar consultas distribuídas em servidores com valores diferentes da configuração `enable_analyzer`.

<div id="mutations-are-interpreted-by-previous-analyzer">
  ### As mutações são interpretadas pelo analisador anterior
</div>

As mutações ainda usam o analisador antigo.
Isso significa que alguns novos recursos do ClickHouse SQL não podem ser usados em mutações. Por exemplo, a cláusula `QUALIFY`.
O status pode ser consultado [aqui](https://github.com/ClickHouse/ClickHouse/issues/61563).

<div id="unsupported-features">
  ### Recursos não suportados
</div>

Abaixo está a lista de recursos que o analisador ainda não suporta:

* Índice Annoy.
* Índice Hypothesis. Em desenvolvimento [aqui](https://github.com/ClickHouse/ClickHouse/pull/48381).
* Window view não é suportado. Não há planos de oferecer suporte a isso no futuro.

<div id="cloud-migration">
  ## Migração para Cloud
</div>

Estamos habilitando o novo analisador de consultas em todas as instâncias em que ele está desativado no momento para oferecer suporte a novas otimizações funcionais e de desempenho. Essa mudança aplica regras mais rigorosas de escopo em SQL, exigindo que os clientes atualizem manualmente as consultas que não estejam em conformidade.

<div id="migration-workflow">
  ### Fluxo de migração
</div>

1. Identifique a consulta filtrando `system.query_log` pelo `normalized_query_hash`:

```sql
SELECT query 
FROM clusterAllReplicas(default, system.query_log)
WHERE normalized_query_hash='{hash}' 
LIMIT 1 
SETTINGS skip_unavailable_shards=1
```

2. Execute a consulta com o analisador habilitado, adicionando estas configurações.

```sql
SETTINGS
    enable_analyzer=1,
    analyzer_compatibility_join_using_top_level_identifier=1
```

3. Refatore e verifique os resultados da consulta para garantir que correspondam à saída gerada quando o analisador estiver desabilitado.

Consulte as incompatibilidades mais frequentes encontradas durante os testes internos.

<div id="unknown-expression-identifier">
  ### Identificador de expressão desconhecido
</div>

Erro: `Unknown expression identifier ... in scope ... (UNKNOWN_IDENTIFIER)`. Código da exceção: 47

Causa: Consultas que dependem de comportamentos legados permissivos e fora do padrão, como referenciar aliases calculados em filtros, projeções de subconsulta ambíguas ou escopo &quot;dinâmico&quot; de CTE, agora são corretamente identificadas como inválidas e rejeitadas imediatamente.

Solução: Atualize seus padrões de SQL da seguinte forma:

* Lógica de filtro: Mova a lógica de WHERE para HAVING se estiver filtrando resultados, ou duplique a expressão em WHERE se estiver filtrando dados de origem.
* Escopo da subconsulta: Selecione explicitamente todas as colunas necessárias para a consulta externa.
* Chaves de junção: Use ON com expressões completas em vez de USING se a chave for um alias.
* Em consultas externas, refira-se ao alias da própria subconsulta/CTE, não às tabelas dentro dela.

<div id="non-aggregated-columns-in-group-by">
  ### Colunas não agregadas em GROUP BY
</div>

Erro: `Column ... is not under aggregate function and not in GROUP BY keys (NOT_AN_AGGREGATE)`. Código da exceção: 215

Causa: O analisador antigo permitia selecionar colunas que não estavam na cláusula GROUP BY (muitas vezes escolhendo um valor arbitrário). O analisador segue o SQL padrão: toda coluna selecionada deve ser uma agregação ou uma chave de agrupamento.

Solução: Envolva a coluna em `any()`, `argMax()` ou adicione-a ao GROUP BY.

```sql
/* ORIGINAL QUERY */
-- device_id is ambiguous
SELECT user_id, device_id FROM table GROUP BY user_id

/* FIXED QUERY */
SELECT user_id, any(device_id) FROM table GROUP BY user_id
-- OR
SELECT user_id, device_id FROM table GROUP BY user_id, device_id
```

<div id="duplicate-cte-names">
  ### Nomes de CTE duplicados
</div>

Erro: `CTE with name ... already exists (MULTIPLE_EXPRESSIONS_FOR_ALIAS)`. Código da exceção: 179

Causa: O analisador antigo permitia definir várias expressões de tabela comuns (WITH ...) com o mesmo nome, ocultando a definição anterior. O analisador proíbe essa ambiguidade.

Solução: Renomeie as CTEs duplicadas para que tenham nomes únicos.

```sql
/* ORIGINAL QUERY */
WITH 
  data AS (SELECT 1 AS id), 
  data AS (SELECT 2 AS id) -- Redefined
SELECT * FROM data;

/* FIXED QUERY */
WITH 
  raw_data AS (SELECT 1 AS id), 
  processed_data AS (SELECT 2 AS id)
SELECT * FROM processed_data;
```

<div id="ambiguous-column-identifiers">
  ### Identificadores de coluna ambíguos
</div>

Erro: `JOIN [JOIN TYPE] ambiguous identifier ... (AMBIGUOUS_IDENTIFIER)` Código da exceção: 207

Causa: A consulta faz referência a um nome de coluna presente em várias tabelas em um JOIN sem especificar a tabela de origem. O analisador antigo frequentemente inferia a coluna com base em lógica interna; o analisador exige um nome explícito.

Solução: Qualifique totalmente a coluna com table&#95;alias.column&#95;name.

```sql
/* ORIGINAL QUERY */
SELECT table1.ID AS ID FROM table1, table2 WHERE ID...

/* FIXED QUERY */
SELECT table1.ID AS ID_RENAMED FROM table1, table2 WHERE ID_RENAMED...
```

<div id="invalid-usage-of-final">
  ### Uso inválido de FINAL
</div>

Erro: `Table expression modifiers FINAL are not supported for subquery...` ou `Storage ... doesn't support FINAL` (`UNSUPPORTED_METHOD`). Códigos de exceção: 1, 181

Causa: FINAL é um modificador do armazenamento da tabela (especificamente [Shared]ReplacingMergeTree). O analisador rejeita FINAL quando ele é aplicado a:

* Subconsultas ou tabelas derivadas (por exemplo, FROM (SELECT ...) FINAL).
* Motores de tabela que não oferecem suporte a ele (por exemplo, SharedMergeTree).

Solução: Aplique FINAL apenas à tabela de origem dentro da subconsulta ou remova-o se o motor não oferecer suporte.

```sql
/* ORIGINAL QUERY */
SELECT * FROM (SELECT * FROM my_table) AS subquery FINAL ...

/* FIXED QUERY */
SELECT * FROM (SELECT * FROM my_table FINAL) AS subquery ...
```

<div id="countdistinct-case-insensitivity">
  ### Diferenciação entre maiúsculas e minúsculas na função `countDistinct()`
</div>

Erro: `Function with name countdistinct does not exist (UNKNOWN_FUNCTION)`. Código da exceção: 46

Causa: Os nomes de funções diferenciam maiúsculas de minúsculas ou são mapeados estritamente no analisador. `countdistinct` (tudo em minúsculas) não é mais reconhecido automaticamente.

Solução: Use o `countDistinct` padrão (camelCase) ou `uniq`, específico do ClickHouse.