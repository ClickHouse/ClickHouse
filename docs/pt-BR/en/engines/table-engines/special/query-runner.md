---
description: 'Os registros inseridos em uma tabela `QueryRunner` representam consultas executadas pelo
  mecanismo, localmente ou em um cluster remoto, no modo "fire and forget".'
sidebar_label: 'QueryRunner'
sidebar_position: 55
slug: /engines/table-engines/special/query-runner
title: 'Mecanismo de tabela QueryRunner'
doc_type: 'reference'
---

<div id="queryrunner-table-engine">
  # Mecanismo de tabela QueryRunner
</div>

Os registros inseridos em uma tabela `QueryRunner` representam consultas executadas pelo mecanismo.
O mecanismo pode ser usado para execução assíncrona de consultas, execução em lote de consultas geradas,
direcionamento de consultas para clusters remotos, benchmarks, fuzzing e testes com tráfego espelho.

<div id="creating-a-table">
  ## Criando uma tabela
</div>

```sql
CREATE TABLE runner
(
    query String,
    database String,
    settings Map(LowCardinality(String), String)
)
ENGINE = QueryRunner
SETTINGS
    cluster = 'cluster_name',
    shard = '1',
    mode = 'asynchronous',
    threads = 4,
    max_queue_size = 1000
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }];
```

A tabela deve ser criada com um subconjunto das colunas permitidas: `query`, `database`, `settings`.
A coluna `query` é obrigatória, e as demais colunas são opcionais.

| Coluna     | Tipo                  | Significado                                                                                            |
| ---------- | --------------------- | ------------------------------------------------------------------------------------------------------ |
| `query`    | `String`              | A consulta a ser executada.                                                                            |
| `database` | `String`              | O banco de dados padrão da consulta. Se estiver vazio, o banco de dados padrão do servidor será usado. |
| `settings` | `Map(String, String)` | Configurações aplicadas à consulta.                                                                    |

<div id="engine-settings">
  ## Configurações do mecanismo
</div>

| Configuração     | Padrão           | Significado                                                                                                                                                                                                                                |
| ---------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cluster`        | `''`             | Nome do cluster para o qual as consultas serão enviadas. Se estiver vazio, as consultas serão executadas localmente.                                                                                                                       |
| `shard`          | `'1'`            | Índice com base em 1 do shard do cluster para o qual as consultas serão enviadas, ou `'random'` para escolher um shard aleatório por consulta, ou `'all'` para executar cada consulta em todos os shards. Requer a configuração `cluster`. |
| `mode`           | `'asynchronous'` | No modo `synchronous`, INSERT retorna depois que todas as consultas do lote inserido forem concluídas. No modo `asynchronous`, INSERT retorna assim que as consultas são enfileiradas.                                                     |
| `threads`        | `4`              | Número de threads em segundo plano que executam as consultas.                                                                                                                                                                              |
| `max_queue_size` | `1000`           | Número máximo de consultas na fila. Quando a fila está cheia, novas consultas inseridas são descartadas, e um erro é registrado.                                                                                                           |

<div id="details">
  ## Detalhes
</div>

A tabela permite apenas consultas `INSERT`.
As consultas são executadas no modo &quot;fire and forget&quot;: em caso de exceção, não há novas tentativas,
e os resultados das consultas `SELECT` são descartados (a única forma de preservar os resultados é `INSERT SELECT`).
O sucesso de cada consulta pode ser verificado na tabela `system.query_log`, em que as consultas iniciadas por
este mecanismo são marcadas com `is_internal = 1` no servidor de origem.

As consultas enfileiradas são mantidas na memória e não sobrevivem à reinicialização do servidor. Ao desligar o servidor
(ou ao executar `DROP`/`DETACH` da tabela), as consultas que ainda não começaram são descartadas. Das
consultas que já estão em execução, as que foram despachadas para um cluster são canceladas, enquanto as executadas
localmente são aguardadas até terminarem.

Quando uma consulta a ser executada é, ela própria, um `INSERT`, seus dados devem estar inline — `INSERT ... VALUES (...)`,
`INSERT ... SELECT ...` ou `INSERT ... FORMAT ...` com os dados no texto da consulta. Um `INSERT` que
espera receber os dados de um fluxo separado não tem suporte.

<div id="local-mode-and-sql-security">
  ## Modo local e SQL SECURITY
</div>

Sem a configuração `cluster`, as consultas são executadas no servidor local.
O usuário em nome do qual elas são executadas é determinado pela cláusula `SQL SECURITY`:

* `INVOKER` (padrão): as consultas são executadas em nome do usuário que realizou o `INSERT`.
* `DEFINER`: as consultas são executadas em nome do usuário especificado em `DEFINER`. Como as consultas inseridas são arbitrárias, conceder `INSERT` em uma tabela desse tipo delega todos os privilégios desse usuário.
* `NONE`: as consultas são executadas com acesso total, sem um usuário. Requer o privilégio `ALLOW_SQL_SECURITY_NONE` ao criar a tabela.

<div id="cluster-mode">
  ## Modo de cluster
</div>

Quando a configuração `cluster` é especificada, as consultas são enviadas ao cluster especificado.

O shard de destino é selecionado por `shard`: um índice fixo baseado em 1 (`'1'` por padrão), `'random'` para escolher um
shard aleatório para cada consulta, ou `'all'` para executar cada consulta em todos os shards do cluster. Uma réplica dentro do
shard é escolhida de acordo com a configuração `load_balancing` do servidor.

A coluna `database` define o banco de dados padrão da conexão com o servidor remoto. Como o
banco de dados padrão é definido uma vez por conexão, cada valor distinto de `database` usa seu próprio
pool de conexões, que é criado no primeiro uso e reutilizado durante todo o ciclo de vida da tabela.

`DEFINER` e `SQL SECURITY` têm efeito apenas no modo local, e combiná-los com a
configuração `cluster` é um erro. Nos servidores remotos, as consultas são autenticadas com as
credenciais da configuração do cluster e executadas como consultas iniciais normais: elas são registradas em
`system.query_log` com `is_initial_query = 1` e seu próprio `query_id` (não vinculado ao INSERT que
as gerou). No servidor de origem, as consultas despachadas são registradas em `system.query_log`
com `is_internal = 1`.

Como o mecanismo descarta os resultados das consultas, ele sempre executa as consultas despachadas com
`discard_query_data = 1`, de modo que os dados de resultado das consultas SELECT não são transferidos pela rede
(isso substitui qualquer valor de `discard_query_data` definido na coluna `settings`).

<div id="waiting-for-queries-to-finish">
  ## Aguardando o término das consultas
</div>

No modo assíncrono, a consulta a seguir pode ser usada para bloquear a execução até que todas as consultas enviadas à tabela até o momento tenham terminado:

```sql
SYSTEM WAIT QUERY RUNNER runner;
```

<div id="example">
  ## Exemplo
</div>

Executando novamente consultas `SELECT` recentes do log de consultas:

```sql
INSERT INTO runner (query, database, settings)
SELECT query, current_database, Settings
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND NOT is_internal AND query_kind = 'Select'
  AND event_time > now() - INTERVAL 1 HOUR;
```