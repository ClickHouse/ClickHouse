---
description: 'Cria um banco de dados no ClickHouse com tabelas de um banco de dados PostgreSQL.'
sidebar_label: 'MaterializedPostgreSQL'
sidebar_position: 60
slug: /engines/database-engines/materialized-postgresql
title: 'MaterializedPostgreSQL'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="materializedpostgresql">
  # MaterializedPostgreSQL
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::note
Recomenda-se que os usuários do ClickHouse Cloud usem [ClickPipes](/pt-BR/integrations/clickpipes) para a replicação do PostgreSQL para o ClickHouse. Ele oferece suporte nativo a Change Data Capture (CDC) de alto desempenho para PostgreSQL.
:::

Cria um banco de dados no ClickHouse com tabelas de um banco de dados PostgreSQL. Primeiro, um banco de dados com o mecanismo `MaterializedPostgreSQL` cria um snapshot do banco de dados PostgreSQL e carrega as tabelas necessárias. As tabelas necessárias podem incluir qualquer subconjunto de tabelas de qualquer subconjunto de esquemas do banco de dados especificado. Junto com o snapshot, o motor de banco de dados obtém o LSN e, assim que o dump inicial das tabelas é concluído, passa a extrair atualizações do WAL. Depois que o banco de dados é criado, tabelas adicionadas posteriormente ao banco de dados PostgreSQL não são incluídas automaticamente na replicação. Elas precisam ser adicionadas manualmente com a consulta `ATTACH TABLE db.table`.

A replicação é implementada com o PostgreSQL Logical Replication Protocol, que não permite replicar DDL, mas permite identificar se ocorreram mudanças incompatíveis com a replicação (alterações no tipo de coluna, adição/remoção de colunas). Essas mudanças são detectadas, e as tabelas correspondentes param de receber atualizações. Nesse caso, você deve usar as consultas `ATTACH`/ `DETACH PERMANENTLY` para recarregar a tabela por completo. Se o DDL não comprometer a replicação (por exemplo, ao renomear uma coluna), a tabela continuará recebendo atualizações (a inserção é feita por posição).

:::note
Este motor de banco de dados é experimental. Para usá-lo, defina `allow_experimental_database_materialized_postgresql` como 1 em seus arquivos de configuração ou usando o comando `SET`:

```sql
SET allow_experimental_database_materialized_postgresql=1
```

:::

<div id="creating-a-database">
  ## Criando um banco de dados
</div>

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedPostgreSQL('host:port', 'database', 'user', 'password') [SETTINGS ...]
```

**Parâmetros do mecanismo**

* `host:port` — endpoint do servidor PostgreSQL.
* `database` — nome do banco de dados PostgreSQL.
* `user` — usuário do PostgreSQL.
* `password` — senha do usuário.

<div id="example-of-use">
  ## Exemplo de uso
</div>

```sql
CREATE DATABASE postgres_db
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password');

SHOW TABLES FROM postgres_db;

┌─name───┐
│ table1 │
└────────┘

SELECT * FROM postgres_db.postgres_table;
```

<div id="dynamically-adding-table-to-replication">
  ## Adicionar dinamicamente novas tabelas à replicação
</div>

Depois que o banco de dados `MaterializedPostgreSQL` é criado, ele não detecta automaticamente novas tabelas no banco de dados PostgreSQL correspondente. Essas tabelas podem ser adicionadas manualmente:

```sql
ATTACH TABLE postgres_database.new_table;
```

:::warning
Antes da versão 22.1, ao adicionar uma tabela à replicação, um slot temporário de replicação não removido era deixado para trás (chamado `{db_name}_ch_replication_slot_tmp`). Se você estiver anexando tabelas em uma versão do ClickHouse anterior à 22.1, exclua-o manualmente (`SELECT pg_drop_replication_slot('{db_name}_ch_replication_slot_tmp')`). Caso contrário, o uso de disco aumentará. Esse problema foi corrigido na versão 22.1.
:::

<div id="dynamically-removing-table-from-replication">
  ## Removendo tabelas dinamicamente da replicação
</div>

É possível remover tabelas específicas da replicação:

```sql
DETACH TABLE postgres_database.table_to_remove PERMANENTLY;
```

<div id="schema">
  ## Schema do PostgreSQL
</div>

O [schema](https://www.postgresql.org/docs/9.1/ddl-schemas.html) do PostgreSQL pode ser configurado de 3 maneiras (a partir da versão 21.12).

1. Um schema para cada `MaterializedPostgreSQL` motor de banco de dados. Requer o uso da configuração `materialized_postgresql_schema`.
   As tabelas são acessadas apenas pelo nome da tabela:

```sql
CREATE DATABASE postgres_database
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema = 'postgres_schema';

SELECT * FROM postgres_database.table1;
```

2. Qualquer número de schemas com um conjunto específico de tabelas para um motor de banco de dados `MaterializedPostgreSQL`. É necessário usar a configuração `materialized_postgresql_tables_list`. Cada tabela é gravada junto com seu schema.
   As tabelas são acessadas usando ao mesmo tempo o nome do schema e o nome da tabela:

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_tables_list = 'schema1.table1,schema2.table2,schema1.table3',
         materialized_postgresql_tables_list_with_schema = 1;

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema2.table2`;
```

Mas, neste caso, todas as tabelas em `materialized_postgresql_tables_list` devem ser escritas com o nome do respectivo schema.
Requer `materialized_postgresql_tables_list_with_schema = 1`.

Aviso: neste caso, pontos no nome da tabela não são permitidos.

3. Qualquer número de schemas com o conjunto completo de tabelas para um motor de banco de dados `MaterializedPostgreSQL`. Requer o uso da configuração `materialized_postgresql_schema_list`.

```sql
CREATE DATABASE database1
ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
SETTINGS materialized_postgresql_schema_list = 'schema1,schema2,schema3';

SELECT * FROM database1.`schema1.table1`;
SELECT * FROM database1.`schema1.table2`;
SELECT * FROM database1.`schema2.table2`;
```

Aviso: neste caso, não são permitidos pontos no nome da tabela.

<div id="requirements">
  ## Requisitos
</div>

1. A configuração [wal&#95;level](https://www.postgresql.org/docs/current/runtime-config-wal.html) deve estar definida como `logical`, e o parâmetro `max_replication_slots` deve ter um valor mínimo de `2` no arquivo de configuração do PostgreSQL.

2. Cada tabela replicada deve ter uma das seguintes [replica identity](https://www.postgresql.org/docs/10/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY):

* chave primária (por padrão)

* índice

```bash
postgres# CREATE TABLE postgres_table (a Integer NOT NULL, b Integer, c Integer NOT NULL, d Integer, e Integer NOT NULL);
postgres# CREATE unique INDEX postgres_table_index on postgres_table(a, c, e);
postgres# ALTER TABLE postgres_table REPLICA IDENTITY USING INDEX postgres_table_index;
```

A chave primária é sempre verificada primeiro. Se ela não estiver presente, o índice definido como replica identity index será verificado.
Se o índice for usado como replica identity, deve haver apenas um índice desse tipo em uma tabela.
Você pode verificar qual tipo é usado em uma tabela específica com o seguinte comando:

```bash
postgres# SELECT CASE relreplident
          WHEN 'd' THEN 'default'
          WHEN 'n' THEN 'nothing'
          WHEN 'f' THEN 'full'
          WHEN 'i' THEN 'index'
       END AS replica_identity
FROM pg_class
WHERE oid = 'postgres_table'::regclass;
```

:::note
A replicação dos valores [**TOAST**](https://www.postgresql.org/docs/9.5/storage-toast.html) não é suportada. O valor padrão do tipo de dado será usado.
:::

<div id="settings">
  ## Configurações
</div>

<div id="materialized-postgresql-tables-list">
  ### `materialized_postgresql_tables_list`
</div>

Define uma lista de tabelas do banco de dados PostgreSQL separada por vírgulas, que serão replicadas pelo motor de banco de dados [MaterializedPostgreSQL](../../engines/database-engines/materialized-postgresql.md).

Cada tabela pode ter um subconjunto das colunas replicadas entre colchetes. Se esse subconjunto de colunas for omitido, todas as colunas da tabela serão replicadas.

```sql
    materialized_postgresql_tables_list = 'table1(co1, col2),table2,table3(co3, col5, col7)
```

Valor padrão: lista vazia — isso significa que todo o banco de dados PostgreSQL será replicado.

<div id="materialized-postgresql-schema">
  ### `materialized_postgresql_schema`
</div>

Valor padrão: string vazia. (Usa o schema padrão)

<div id="materialized-postgresql-schema-list">
  ### `materialized_postgresql_schema_list`
</div>

Valor padrão: lista vazia. (O schema padrão é usado)

<div id="materialized-postgresql-max-block-size">
  ### `materialized_postgresql_max_block_size`
</div>

Define o número de linhas coletadas em memória antes de gravar os dados na tabela do banco de dados PostgreSQL.

Valores possíveis:

* Inteiro positivo.

Valor padrão: `65536`.

<div id="materialized-postgresql-replication-slot">
  ### `materialized_postgresql_replication_slot`
</div>

Um slot de replicação criado pelo usuário. Deve ser usado em conjunto com `materialized_postgresql_snapshot`.

<div id="materialized-postgresql-snapshot">
  ### `materialized_postgresql_snapshot`
</div>

Uma string que identifica um snapshot a partir do qual será realizado o [dump inicial das tabelas do PostgreSQL](../../engines/database-engines/materialized-postgresql.md). Deve ser usada em conjunto com `materialized_postgresql_replication_slot`.

```sql
    CREATE DATABASE database1
    ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
    SETTINGS materialized_postgresql_tables_list = 'table1,table2,table3';

    SELECT * FROM database1.table1;
```

As configurações podem ser alteradas, se necessário, usando uma consulta DDL. No entanto, não é possível alterar a configuração `materialized_postgresql_tables_list`. Para atualizar a lista de tabelas nessa configuração, use a consulta `ATTACH TABLE`.

```sql
    ALTER DATABASE postgres_database MODIFY SETTING materialized_postgresql_max_block_size = <new_size>;
```

<div id="materialized_postgresql_use_unique_replication_consumer_identifier">
  ### `materialized_postgresql_use_unique_replication_consumer_identifier`
</div>

Use um identificador exclusivo de consumer de replicação. Padrão: `0`.
Se definido como `1`, permite configurar várias tabelas `MaterializedPostgreSQL` apontando para a mesma tabela `PostgreSQL`.

<div id="materialized-postgresql-use-extended-date-and-time-types">
  ### `materialized_postgresql_use_extended_date_and_time_types`
</div>

Mapeia os tipos `date` e `timestamp`/`timestamptz` do PostgreSQL para `Date32` e `DateTime64` no ClickHouse, que cobrem o intervalo de valores mais amplo desses tipos no PostgreSQL. Padrão: `1`.
Se definido como `0`, os tipos mais restritos `Date` e `DateTime` serão usados em vez disso (valores fora do intervalo deles ou com precisão de frações de segundo não podem ser representados).

Essa configuração controla apenas os tipos de coluna escolhidos pela inferência de tipos quando as tabelas aninhadas são criadas, portanto deve ser especificada no momento do `CREATE DATABASE`. Ela não pode ser alterada depois com `ALTER DATABASE ... MODIFY SETTING` (as tabelas aninhadas já criadas mantêm seus tipos de coluna fixos, e essa alteração é rejeitada); recrie o banco de dados para mudá-la. Ela não se aplica ao mecanismo de tabela `MaterializedPostgreSQL`, em que os tipos de coluna são declarados explicitamente.

<div id="notes">
  ## Notas
</div>

<div id="logical-replication-slot-failover">
  ### Failover do slot de replicação lógica
</div>

Os slots de replicação lógica existentes na primária não ficam disponíveis nas réplicas standby.
Portanto, se houver failover, a nova primária (a antiga standby física) não terá conhecimento de nenhum slot que existia na primária anterior. Isso fará com que a replicação do PostgreSQL seja interrompida.
Uma solução para isso é gerenciar os slots de replicação manualmente e definir um slot de replicação permanente (algumas informações podem ser encontradas [aqui](https://patroni.readthedocs.io/en/latest/SETTINGS.html)). Você precisará informar o nome do slot por meio da configuração `materialized_postgresql_replication_slot`, e ele deve ser exportado com a opção `EXPORT SNAPSHOT`. O identificador do snapshot precisa ser informado por meio da configuração `materialized_postgresql_snapshot`.

Observe que isso deve ser usado apenas se for realmente necessário. Se não houver uma necessidade real ou um entendimento claro do motivo, é melhor permitir que o engine de tabela crie e gerencie seu próprio slot de replicação.

**Exemplo (de [@bchrobot](https://github.com/bchrobot))**

1. Configure o slot de replicação no PostgreSQL.

   ```yaml
   apiVersion: "acid.zalan.do/v1"
   kind: postgresql
   metadata:
     name: acid-demo-cluster
   spec:
     numberOfInstances: 2
     postgresql:
       parameters:
         wal_level: logical
     patroni:
       slots:
         clickhouse_sync:
           type: logical
           database: demodb
           plugin: pgoutput
   ```

2. Aguarde até que o slot de replicação esteja pronto e, em seguida, inicie uma transação e exporte o identificador do snapshot da transação:

   ```sql
   BEGIN;
   SELECT pg_export_snapshot();
   ```

3. No ClickHouse, crie o banco de dados:

   ```sql
   CREATE DATABASE demodb
   ENGINE = MaterializedPostgreSQL('postgres1:5432', 'postgres_database', 'postgres_user', 'postgres_password')
   SETTINGS
     materialized_postgresql_replication_slot = 'clickhouse_sync',
     materialized_postgresql_snapshot = '0000000A-0000023F-3',
     materialized_postgresql_tables_list = 'table1,table2,table3';
   ```

4. Encerre a transação do PostgreSQL assim que a replicação para o banco de dados do ClickHouse for confirmada. Verifique se a replicação continua após o failover:

   ```bash
   kubectl exec acid-demo-cluster-0 -c postgres -- su postgres -c 'patronictl failover --candidate acid-demo-cluster-1 --force'
   ```

<div id="required-permissions">
  ### Permissões necessárias
</div>

1. [CREATE PUBLICATION](https://www.postgresql.org/docs/14/sql-createpublication.html) -- privilégio de criação de consulta.

2. [CREATE&#95;REPLICATION&#95;SLOT](https://www.postgresql.org/docs/10/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-SLOT) -- privilégio de replicação.

3. [pg&#95;drop&#95;replication&#95;slot](https://www.postgresql.org/docs/9.5/functions-admin.html#FUNCTIONS-REPLICATION) -- privilégio de replicação ou superuser.

4. [DROP PUBLICATION](https://www.postgresql.org/docs/10/sql-droppublication.html) -- proprietário da publication (`username` no próprio engine MaterializedPostgreSQL).

É possível evitar a execução dos comandos `2` e `3` e a necessidade dessas permissões. Use as configurações `materialized_postgresql_replication_slot` e `materialized_postgresql_snapshot`. Mas com muito cuidado.

Acesso às tabelas:

1. pg&#95;publication

2. pg&#95;replication&#95;slots

3. pg&#95;publication&#95;tables

<div id="backup-and-restore">
  ### Backup e restauração
</div>

É possível fazer backup de um banco de dados `MaterializedPostgreSQL`. Os dados de cada tabela replicada ficam em uma tabela `ReplacingMergeTree` aninhada, portanto `BACKUP DATABASE` captura esses dados ao delegar a operação à tabela aninhada.

```sql
BACKUP DATABASE postgres_db TO Disk('backups', 'postgres_db.zip');
```

Restaurar um banco de dados ou uma tabela `MaterializedPostgreSQL` **no mesmo local não é suportado**. Um objeto `MaterializedPostgreSQL` restaurado começa imediatamente a replicar a partir da fonte PostgreSQL ativa, portanto restaurar o snapshot do backup sobre ele misturaria o snapshot com o estado remoto atual. Por isso, o RESTORE falha por segurança nesse caso. Em vez disso, restaure os dados capturados em tabelas `ReplacingMergeTree` simples:

* Em um backup de banco de dados, a definição armazenada de cada tabela já é o `ReplacingMergeTree` aninhado sintético (não a engine `MaterializedPostgreSQL`), portanto cada tabela pode ser restaurada diretamente em uma tabela nova, que ainda não exista:

  ```sql
  RESTORE TABLE postgres_db.table1 AS restored_db.table1
  FROM Disk('backups', 'postgres_db.zip')
  SETTINGS allow_different_table_def = 1;
  ```

* Para um backup de tabela `MaterializedPostgreSQL` standalone, a definição armazenada é a própria engine `MaterializedPostgreSQL`. Crie antes uma tabela `ReplacingMergeTree` com a mesma estrutura da tabela aninhada (incluindo as colunas `_sign` e `_version`) e restaure nela:

  ```sql
  RESTORE TABLE src AS existing_replacing_mergetree
  FROM Disk('backups', 'table.zip')
  SETTINGS allow_different_table_def = 1;
  ```