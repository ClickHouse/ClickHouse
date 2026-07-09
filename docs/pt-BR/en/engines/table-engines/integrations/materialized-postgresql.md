---
description: 'Cria uma tabela no ClickHouse com um dump inicial de dados de uma
  tabela do PostgreSQL e inicia o processo de replicação.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 130
slug: /engines/table-engines/integrations/materialized-postgresql
title: 'Motor de tabela MaterializedPostgreSQL'
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql-table-engine">
  # Motor de tabela MaterializedPostgreSQL
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
Recomenda-se que os usuários do ClickHouse Cloud usem [ClickPipes](/pt-BR/integrations/clickpipes) para a replicação do PostgreSQL para o ClickHouse. Isso oferece suporte nativo a Change Data Capture (CDC) de alto desempenho para o PostgreSQL.
:::

Cria uma tabela no ClickHouse com um dump inicial de dados da tabela PostgreSQL e inicia o processo de replicação, ou seja, executa um job em segundo plano para aplicar novas alterações à medida que ocorrem na tabela PostgreSQL no banco de dados PostgreSQL remoto.

:::note
Este motor de tabela é experimental. Para usá-lo, defina `allow_experimental_materialized_postgresql_table` como 1 em seus arquivos de configuração ou usando o comando `SET`:

```sql
SET allow_experimental_materialized_postgresql_table=1
```

:::

Se for necessária mais de uma tabela, é altamente recomendável usar o [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md) mecanismo de banco de dados em vez do motor de tabela e usar a configuração `materialized_postgresql_tables_list`, que especifica as tabelas a serem replicadas (também será possível adicionar o `schema` do banco de dados). Isso é muito melhor em termos de CPU, além de exigir menos conexões e menos slots de replicação no banco de dados PostgreSQL remoto.

<div id="creating-a-table">
  ## Criar uma tabela
</div>

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_table', 'postgres_user', 'postgres_password')
PRIMARY KEY key;
```

**Parâmetros do mecanismo**

* `host:port` — Endereço do servidor PostgreSQL.
* `database` — Nome do banco de dados remoto.
* `table` — Nome da tabela remota.
* `user` — Usuário do PostgreSQL.
* `password` — Senha do usuário.

<div id="requirements">
  ## Requisitos
</div>

1. A configuração [wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) deve ter o valor `logical`, e o parâmetro `max_replication_slots` deve ter um valor de pelo menos `2` no arquivo de configuração do PostgreSQL.

2. Uma tabela com o motor `MaterializedPostgreSQL` deve ter uma chave primária — a mesma de um índice de replica identity (por padrão, a chave primária) de uma tabela do PostgreSQL (veja [detalhes sobre o índice de replica identity](../../../engines/database-engines/materialized-postgresql.md#requirements)).

3. Somente o banco de dados [Atomic](https://en.wikipedia.org/wiki/Atomicity_\(database_systems\)) é aceito.

4. O motor de tabela `MaterializedPostgreSQL` funciona apenas com versões do PostgreSQL &gt;= 11, pois a implementação exige a função [pg&#95;replication&#95;slot&#95;advance](https://pgpedia.info/p/pg_replication_slot_advance.html) do PostgreSQL.

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_version` — Contador de transações. Tipo: [UInt64](../../../sql-reference/data-types/int-uint.md).

* `_sign` — Marca de exclusão. Tipo: [Int8](../../../sql-reference/data-types/int-uint.md). Valores possíveis:
  * `1` — A linha não foi excluída,
  * `-1` — A linha foi excluída.

Essas colunas não precisam ser adicionadas quando uma tabela é criada. Elas ficam sempre acessíveis na consulta `SELECT`.
A coluna `_version` corresponde à posição `LSN` no `WAL`, portanto pode ser usada para verificar o quão atualizada está a replicação.

```sql
CREATE TABLE postgresql_db.postgresql_replica (key UInt64, value UInt64)
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgresql_replica', 'postgres_user', 'postgres_password')
PRIMARY KEY key;

SELECT key, value, _version FROM postgresql_db.postgresql_replica;
```

:::note
A replicação de valores [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) não é compatível. O valor padrão do tipo de dados será usado.
:::