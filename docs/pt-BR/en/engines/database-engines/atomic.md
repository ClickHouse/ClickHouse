---
description: 'O mecanismo `Atomic` oferece suporte a consultas `DROP TABLE` e `RENAME TABLE`
  sem bloqueio, e a consultas atômicas `EXCHANGE TABLES`. O mecanismo de banco de dados
  `Atomic` é usado por padrão.'
sidebar_label: 'Atomic'
sidebar_position: 10
slug: /engines/database-engines/atomic
title: 'Atomic'
doc_type: 'reference'
---

O mecanismo `Atomic` oferece suporte a consultas [`DROP TABLE`](#drop-detach-table) e [`RENAME TABLE`](#rename-table) sem bloqueio, além de consultas atômicas [`EXCHANGE TABLES`](#exchange-tables). O mecanismo de banco de dados `Atomic` é usado por padrão no ClickHouse de código aberto.

:::note
No ClickHouse Cloud, o [mecanismo de banco de dados `Shared`](/pt-BR/cloud/reference/shared-catalog#shared-database-engine) é usado por padrão e também oferece suporte às operações mencionadas acima.
:::

<div id="creating-a-database">
  ## Criando um banco de dados
</div>

```sql
CREATE DATABASE test [ENGINE = Atomic] [SETTINGS disk=...];
```

<div id="specifics-and-recommendations">
  ## Detalhes e recomendações
</div>

<div id="table-uuid">
  ### UUID da tabela
</div>

Cada tabela no banco de dados `Atomic` tem um [UUID](../../sql-reference/data-types/uuid.md) permanente e armazena seus dados no seguinte diretório:

```text
/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
```

Em que `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` é o UUID da tabela.

Por padrão, o UUID é gerado automaticamente. No entanto, os usuários podem informar explicitamente o UUID ao criar uma tabela, embora isso não seja recomendado.

Por exemplo:

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

:::note
Você pode usar a configuração [show&#95;table&#95;uuid&#95;in&#95;table&#95;create&#95;query&#95;if&#95;not&#95;nil](../../operations/settings/settings.md#show_table_uuid_in_table_create_query_if_not_nil) para exibir o UUID na consulta `SHOW CREATE`.
:::

<div id="rename-table">
  ### RENAME TABLE
</div>

As consultas [`RENAME`](../../sql-reference/statements/rename.md) não modificam o UUID nem movem os dados da tabela. Elas são executadas imediatamente e não esperam a conclusão de outras consultas que estejam usando a tabela.

<div id="drop-detach-table">
  ### DROP/DESANEXAR TABELA
</div>

Ao usar `DROP TABLE`, nenhum dado é removido. O mecanismo `Atomic` apenas marca a tabela como removida, movendo seus metadados para `/clickhouse_path/metadata_dropped/` e notificando a thread em segundo plano. O atraso antes da exclusão final dos dados da tabela é definido pela configuração [`database_atomic_delay_before_drop_table_sec`](../../operations/server-configuration-parameters/settings.md#database_atomic_delay_before_drop_table_sec).
Você pode especificar o modo síncrono usando o modificador `SYNC`. Para isso, use a configuração [`database_atomic_wait_for_drop_and_detach_synchronously`](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously). Nesse caso, `DROP` aguarda a conclusão de `SELECT`, `INSERT` e outras consultas em execução que estejam usando a tabela. A tabela será removida quando não estiver em uso.

<div id="exchange-tables">
  ### EXCHANGE TABLES/DICTIONARIES
</div>

A consulta [`EXCHANGE`](../../sql-reference/statements/exchange.md) troca tabelas ou dicionários atomicamente. Por exemplo, em vez desta operação não atômica:

```sql title="Non-atomic"
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```

você pode usar um atomic:

```sql title="Atomic"
EXCHANGE TABLES new_table AND old_table;
```

<div id="replicatedmergetree-in-atomic-database">
  ### ReplicatedMergeTree em banco de dados Atomic
</div>

Para tabelas [`ReplicatedMergeTree`](/pt-BR/engines/table-engines/mergetree-family/replication), recomenda-se não especificar os parâmetros do mecanismo para o caminho no ZooKeeper e o nome da réplica. Nesse caso, serão usados os parâmetros de configuração [`default_replica_path`](../../operations/server-configuration-parameters/settings.md#default_replica_path) e [`default_replica_name`](../../operations/server-configuration-parameters/settings.md#default_replica_name). Se quiser especificar explicitamente os parâmetros do mecanismo, recomenda-se usar a macro `{uuid}`. Isso garante que caminhos únicos sejam gerados automaticamente para cada tabela no ZooKeeper.

<div id="metadata-disk">
  ### Disco de metadados
</div>

Quando `disk` é especificado em `SETTINGS`, esse disco é usado para armazenar os arquivos de metadados da tabela.
Por exemplo:

```sql
CREATE TABLE db (n UInt64) ENGINE = Atomic SETTINGS disk=disk(type='local', path='/var/lib/clickhouse-disks/db_disk');
```

Se não for especificado, o disk definido em `database_disk.disk` será usado por padrão.

<div id="see-also">
  ## Veja também
</div>

* [system.databases](../../operations/system-tables/databases.md) tabela do sistema