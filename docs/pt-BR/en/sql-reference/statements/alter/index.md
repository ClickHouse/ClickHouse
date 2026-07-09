---
description: 'Documentação do ALTER'
sidebar_label: 'ALTER'
sidebar_position: 35
slug: /sql-reference/statements/alter/
title: 'ALTER'
doc_type: 'reference'
---

A maioria das consultas `ALTER TABLE` modifica configurações da tabela ou os dados:

| Modificador                                                                 |
| --------------------------------------------------------------------------- |
| [COLUMN](/pt-BR/sql-reference/statements/alter/column.md)                         |
| [PARTITION](/pt-BR/sql-reference/statements/alter/partition.md)                   |
| [DELETE](/pt-BR/sql-reference/statements/alter/delete.md)                         |
| [UPDATE](/pt-BR/sql-reference/statements/alter/update.md)                         |
| [ORDER BY](/pt-BR/sql-reference/statements/alter/order-by.md)                     |
| [INDEX](/pt-BR/sql-reference/statements/alter/skipping-index.md)                  |
| [CONSTRAINT](/pt-BR/sql-reference/statements/alter/constraint.md)                 |
| [TTL](/pt-BR/sql-reference/statements/alter/ttl.md)                               |
| [STATISTICS](/pt-BR/sql-reference/statements/alter/statistics.md)                 |
| [APPLY DELETED MASK](/pt-BR/sql-reference/statements/alter/apply-deleted-mask.md) |
| [APPLY PATCHES](/pt-BR/sql-reference/statements/alter/apply-patches.md)           |

:::note
A maioria das consultas `ALTER TABLE` tem suporte apenas para tabelas [*MergeTree](/pt-BR/engines/table-engines/mergetree-family/index.md), [Merge](/pt-BR/engines/table-engines/special/merge.md) e [Distributed](/pt-BR/engines/table-engines/special/distributed.md).
:::

Estas instruções `ALTER` manipulam views:

| Instrução                                                               | Descrição                                                                                 |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| [ALTER TABLE ... MODIFY QUERY](/pt-BR/sql-reference/statements/alter/view.md) | Modifica a estrutura de uma [visão materializada](/pt-BR/sql-reference/statements/create/view). |

Estas instruções `ALTER` modificam entidades relacionadas ao Controle de Acesso Baseado em Funções:

| Instrução                                                               |
| ----------------------------------------------------------------------- |
| [USER](/pt-BR/sql-reference/statements/alter/user.md)                         |
| [ROLE](/pt-BR/sql-reference/statements/alter/role.md)                         |
| [QUOTA](/pt-BR/sql-reference/statements/alter/quota.md)                       |
| [ROW POLICY](/pt-BR/sql-reference/statements/alter/row-policy.md)             |
| [SETTINGS PROFILE](/pt-BR/sql-reference/statements/alter/settings-profile.md) |

| Instrução                                                                     | Descrição                                                                                                |
| ----------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| [ALTER TABLE ... MODIFY COMMENT](/pt-BR/sql-reference/statements/alter/comment.md)  | Adiciona, modifica ou remove comentários da tabela, independentemente de já terem sido definidos ou não. |
| [ALTER NAMED COLLECTION](/pt-BR/sql-reference/statements/alter/named-collection.md) | Modifica [coleções nomeadas](/pt-BR/operations/named-collections.md).                                          |

<div id="mutations">
  ## Mutações
</div>

Consultas `ALTER` destinadas a manipular dados de tabelas são implementadas por meio de um mecanismo chamado &quot;mutações&quot;, principalmente [ALTER TABLE ... DELETE](/pt-BR/sql-reference/statements/alter/delete.md) e [ALTER TABLE ... UPDATE](/pt-BR/sql-reference/statements/alter/update.md). Elas são processos assíncronos em segundo plano, semelhantes a merges em tabelas [MergeTree](/pt-BR/engines/table-engines/mergetree-family/index.md), que produzem novas versões &quot;alteradas&quot; das partes.

Para tabelas `*MergeTree`, as mutações são executadas **reescrevendo partes de dados inteiras**.
Não há atomicidade — as partes são substituídas pelas partes alteradas assim que ficam prontas, e uma consulta `SELECT` iniciada durante uma mutação verá dados de partes que já foram alteradas junto com dados de partes que ainda não foram alteradas.

As mutações são totalmente ordenadas pela ordem em que são criadas e são aplicadas a cada parte nessa ordem. As mutações também são parcialmente ordenadas em relação às consultas `INSERT INTO`: os dados inseridos na tabela antes de a mutação ser submetida serão alterados, e os dados inseridos depois disso não serão alterados. Observe que as mutações não bloqueiam inserts de forma alguma.

Uma consulta de mutação retorna imediatamente após a entrada da mutação ser adicionada (no caso de tabelas replicadas, ao ZooKeeper; no caso de tabelas não replicadas, ao sistema de arquivos). A mutação em si é executada de forma assíncrona usando as configurações do perfil do sistema. Para acompanhar o progresso das mutações, você pode usar a tabela [`system.mutations`](/pt-BR/operations/system-tables/mutations). Uma mutação submetida com sucesso continuará sendo executada mesmo que os servidores ClickHouse sejam reiniciados. Não há como reverter a mutação depois que ela é submetida, mas, se a mutação ficar travada por algum motivo, ela pode ser cancelada com a consulta [`KILL MUTATION`](/pt-BR/sql-reference/statements/kill.md/#kill-mutation).

As entradas de mutações concluídas não são excluídas imediatamente (o número de entradas preservadas é determinado pelo parâmetro `finished_mutations_to_keep` do mecanismo de armazenamento). Entradas de mutação mais antigas são excluídas.

<div id="synchronicity-of-alter-queries">
  ## Sincronia das consultas ALTER
</div>

Para tabelas não replicadas, todas as consultas `ALTER` são executadas de forma síncrona. Para tabelas replicadas, a consulta apenas adiciona instruções para as ações correspondentes no `ZooKeeper`, e as próprias ações são executadas assim que possível. No entanto, a consulta pode aguardar a conclusão dessas ações em todas as réplicas.

Para consultas `ALTER` que criam mutações (por exemplo, entre elas `UPDATE`, `DELETE`, `MATERIALIZE INDEX`, `MATERIALIZE PROJECTION`, `MATERIALIZE COLUMN`, `APPLY DELETED MASK`, `APPLY PATCHES`, `CLEAR STATISTIC`, `MATERIALIZE STATISTIC`), a sincronia é definida pela configuração [mutations&#95;sync](/pt-BR/operations/settings/settings.md/#mutations_sync).

Para outras consultas `ALTER` que apenas modificam os metadados, você pode usar a configuração [alter&#95;sync](/pt-BR/operations/settings/settings#alter_sync) para definir a espera.

Você pode especificar por quanto tempo (em segundos) aguardar que réplicas inativas executem todas as consultas `ALTER` com a configuração [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/pt-BR/operations/settings/settings#replication_wait_for_inactive_replica_timeout).

:::note
Para todas as consultas `ALTER`, se `alter_sync = 2` e algumas réplicas permanecerem inativas por mais tempo do que o especificado na configuração `replication_wait_for_inactive_replica_timeout`, então uma exceção `UNFINISHED` é lançada.
:::

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [Como lidar com atualizações e exclusões no ClickHouse](https://clickhouse.com/blog/handling-updates-and-deletes-in-clickhouse)