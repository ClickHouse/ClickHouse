---
description: 'Documentação sobre Attach'
sidebar_label: 'ATTACH'
sidebar_position: 40
slug: /sql-reference/statements/attach
title: 'Instrução ATTACH'
doc_type: 'reference'
---

Anexa uma tabela ou um dicionário, por exemplo, ao mover um banco de dados para outro servidor.

**Sintaxe**

```sql
ATTACH TABLE|DICTIONARY|DATABASE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
```

A consulta não cria dados em disco, mas pressupõe que os dados já estejam nos locais apropriados e apenas adiciona ao servidor informações sobre a tabela, o dicionário ou o banco de dados especificado. Após executar a consulta `ATTACH`, o servidor passará a reconhecer a existência da tabela, do dicionário ou do banco de dados.

Se uma tabela foi desanexada anteriormente (consulta [DETACH](../../sql-reference/statements/detach.md)), ou seja, se sua estrutura já é conhecida, você pode usar a forma abreviada sem definir a estrutura.

<div id="attach-existing-table">
  ## Anexar tabela existente
</div>

**Sintaxe**

```sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

Esta consulta é usada ao iniciar o servidor. O servidor armazena os metadados das tabelas como arquivos com consultas `ATTACH`, que ele simplesmente executa na inicialização (com exceção de algumas tabelas de sistema, que são criadas explicitamente no servidor).

Se a tabela tiver sido desanexada permanentemente, ela não será anexada novamente na inicialização do servidor, então você precisará usar explicitamente a consulta `ATTACH`.

<div id="create-new-table-and-attach-data">
  ## Criar uma nova tabela e anexar os dados
</div>

<div id="with-specified-path-to-table-data">
  ### Com caminho especificado para os dados da tabela
</div>

A consulta cria uma nova tabela com a estrutura fornecida e anexa os dados da tabela do diretório informado em `user_files`.

**Sintaxe**

```sql
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
```

**Exemplo**

```sql title="Query"
DROP TABLE IF EXISTS test;
INSERT INTO TABLE FUNCTION file('01188_attach/test/data.TSV', 'TSV', 's String, n UInt8') VALUES ('test', 42);
ATTACH TABLE test FROM '01188_attach/test' (s String, n UInt8) ENGINE = File(TSV);
SELECT * FROM test;
```

```sql title="Response"
┌─s────┬──n─┐
│ test │ 42 │
└──────┴────┘
```

<div id="with-specified-table-uuid">
  ### Com UUID de tabela especificado
</div>

Esta consulta cria uma nova tabela com a estrutura fornecida e associa os dados da tabela com o UUID especificado.
Há suporte para isso no motor de banco de dados [Atomic](../../engines/database-engines/atomic.md).

**Sintaxe**

```sql
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
```

<div id="attach-mergetree-table-as-replicatedmergetree">
  ## Anexar tabela MergeTree como ReplicatedMergeTree
</div>

Permite anexar uma tabela MergeTree não replicada como ReplicatedMergeTree. A tabela ReplicatedMergeTree será criada com os valores das configurações `default_replica_path` e `default_replica_name`. Também é possível anexar uma tabela replicada como uma MergeTree comum.

Observe que os dados da tabela no ZooKeeper não são afetados por esta consulta. Isso significa que você precisa adicionar metadados no ZooKeeper usando `SYSTEM RESTORE REPLICA` ou limpá-los com `SYSTEM DROP REPLICA ... FROM ZKPATH ...` após o anexo.

Se você estiver tentando adicionar uma réplica a uma tabela ReplicatedMergeTree existente, lembre-se de que todos os dados locais da tabela MergeTree convertida serão desanexados.

**Sintaxe**

```sql
ATTACH TABLE [db.]name AS [NOT] REPLICATED
```

**Converter tabela para replicada**

```sql
DETACH TABLE test;
ATTACH TABLE test AS REPLICATED;
SYSTEM RESTORE REPLICA test;
```

**Converter tabela para não replicada**

Obtenha o caminho do ZooKeeper e o nome da réplica da tabela:

```sql title="Query"
SELECT replica_name, zookeeper_path FROM system.replicas WHERE table='test';
```

```sql title="Response"
┌─replica_name─┬─zookeeper_path─────────────────────────────────────────────┐
│ r1           │ /clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1 │
└──────────────┴────────────────────────────────────────────────────────────┘
```

Anexe a tabela como não replicada e exclua os dados da réplica no ZooKeeper:

```sql title="Query"
DETACH TABLE test;
ATTACH TABLE test AS NOT REPLICATED;
SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1';
```

<div id="attach-existing-dictionary">
  ## Anexar Dicionário Existente
</div>

Anexa um dicionário desanexado anteriormente.

**Sintaxe**

```sql
ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

<div id="attach-existing-database">
  ## Anexar Banco de Dados Existente
</div>

Anexa um banco de dados desanexado anteriormente.

**Sintaxe**

```sql
ATTACH DATABASE [IF NOT EXISTS] name [ENGINE=<database engine>] [ON CLUSTER cluster]
```