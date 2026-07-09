---
description: 'Documentação do Kill'
sidebar_label: 'KILL'
sidebar_position: 46
slug: /sql-reference/statements/kill
title: 'Instruções KILL'
doc_type: 'reference'
---

Há dois tipos de instruções KILL: para encerrar uma consulta e para encerrar uma mutação

<div id="kill-query">
  ## KILL QUERY
</div>

```sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

Tenta encerrar à força as consultas em execução no momento.
As consultas a serem encerradas são selecionadas na tabela system.processes usando os critérios definidos na cláusula `WHERE` da consulta `KILL`.

Exemplos:

Primeiro, você precisará obter a lista de consultas incompletas. Esta consulta SQL as retorna, começando pelas que estão em execução há mais tempo:

Lista de um único nó do ClickHouse:

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM system.processes
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

Lista de um cluster do ClickHouse:

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM clusterAllReplicas(default, system.processes)
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

Interrompa a consulta:

```sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

:::tip
Se você estiver interrompendo uma consulta no ClickHouse Cloud ou em um cluster autogerenciado, use a opção `ON CLUSTER [cluster-name]` para garantir que a consulta seja interrompida em todas as réplicas.
:::

Usuários somente leitura só podem interromper as próprias consultas.

Por padrão, é usada a versão assíncrona das consultas (`ASYNC`), que não aguarda a confirmação de que as consultas foram interrompidas.

A versão síncrona (`SYNC`) aguarda que todas as consultas sejam interrompidas e exibe informações sobre cada processo à medida que ele é interrompido.
A resposta contém a coluna `kill_status`, que pode assumir os seguintes valores:

1. `finished` – A consulta foi encerrada com sucesso.
2. `waiting` – Aguardando a consulta terminar após o envio de um sinal de encerramento.
3. Os outros valores explicam por que a consulta não pode ser interrompida.

Uma consulta de teste (`TEST`) apenas verifica as permissões do usuário e exibe uma lista de consultas a serem interrompidas.

<div id="kill-mutation">
  ## KILL MUTATION
</div>

A presença de mutações demoradas ou incompletas geralmente indica que um serviço ClickHouse está com baixo desempenho. A natureza assíncrona das mutações pode fazer com que elas consumam todos os recursos disponíveis do sistema. Talvez seja necessário:

* Pausar todas as novas mutações, `INSERT`s e `SELECT`s e deixar que a fila de mutações seja processada até o fim.
* Ou encerrar manualmente algumas dessas mutações enviando um comando `KILL`.

```sql
KILL MUTATION
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

Tenta cancelar e remover [mutações](/pt-BR/sql-reference/statements/alter#mutations) que estão em execução no momento. As mutações a cancelar são selecionadas da tabela [`system.mutations`](/pt-BR/operations/system-tables/mutations) usando o filtro especificado pela cláusula `WHERE` da consulta `KILL`.

Uma consulta de teste (`TEST`) apenas verifica as permissões do usuário e exibe uma lista de mutações a interromper.

Exemplos:

Obtenha a `count()` do número de mutações incompletas:

Contagem de mutações de um único nó do ClickHouse:

```sql
SELECT count(*)
FROM system.mutations
WHERE is_done = 0;
```

Número de mutações em um cluster de réplicas do ClickHouse:

```sql
SELECT count(*)
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

Consulte a lista de mutações incompletas:

Lista de mutações de um único nó do ClickHouse:

```sql
SELECT mutation_id, *
FROM system.mutations
WHERE is_done = 0;
```

Lista de mutações de um cluster do ClickHouse:

```sql
SELECT mutation_id, *
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

Interrompa as mutações conforme necessário:

```sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

A consulta é útil quando uma mutação fica travada e não consegue ser concluída (por exemplo, se alguma função na consulta da mutação lança uma exceção ao ser aplicada aos dados contidos na tabela).

As alterações já feitas pela mutação não são revertidas.

:::note
A coluna `is_killed=1` (somente no ClickHouse Cloud) na tabela [system.mutations](/pt-BR/operations/system-tables/mutations) não significa necessariamente que a mutação tenha sido totalmente finalizada. É possível que uma mutação permaneça em um estado em que `is_killed=1` e `is_done=0` por um período prolongado. Isso pode acontecer se outra mutação de longa duração estiver bloqueando a mutação interrompida. Esta é uma situação normal.
:::