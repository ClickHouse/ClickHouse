---
description: 'Este motor permite integrar o ClickHouse ao RocksDB'
sidebar_label: 'EmbeddedRocksDB'
sidebar_position: 50
slug: /engines/table-engines/integrations/embedded-rocksdb
title: 'Motor de tabela EmbeddedRocksDB'
doc_type: 'referência'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="embeddedrocksdb-table-engine">
  # Motor de tabela EmbeddedRocksDB
</div>

<CloudNotSupportedBadge />

Este motor permite integrar o ClickHouse ao [RocksDB](http://rocksdb.org/).

<div id="creating-a-table">
  ## Criar uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = EmbeddedRocksDB([ttl, rocksdb_dir, read_only]) PRIMARY KEY(primary_key_name)
[ SETTINGS name=value, ... ]
```

Parâmetros do mecanismo:

* `ttl` - tempo de vida dos valores. O TTL é aceito em segundos. Se o TTL for 0, a instância regular do RocksDB será usada (sem TTL).
* `rocksdb_dir` - caminho para o diretório de um RocksDB existente ou caminho de destino do RocksDB criado. Abre a tabela com o `rocksdb_dir` especificado.
* `read_only` - quando `read_only` é definido como true, o modo somente leitura é usado. Para armazenamento com TTL, a compactação não será acionada (nem manual nem automaticamente), portanto nenhuma entrada expirada será removida.
* `primary_key_name` – qualquer nome de coluna na lista de colunas.
* a `primary key` deve ser especificada; ela suporta apenas uma coluna na chave primária. A chave primária será serializada em binário como uma `rocksdb key`.
* colunas diferentes da chave primária serão serializadas em binário como valor `rocksdb`, na ordem correspondente.
* consultas com filtragem por chave `equals` ou `in` serão otimizadas para busca de múltiplas chaves no `rocksdb`.

Configurações do mecanismo:

* `optimize_for_bulk_insert` – a tabela é otimizada para inserções em massa (o pipeline de insert criará arquivos SST e os importará para o banco de dados rocksdb, em vez de gravar em memtables); valor padrão: `1`.
* `bulk_insert_block_size` - tamanho mínimo dos arquivos SST (em número de linhas) criados pela inserção em massa; valor padrão: `1048449`.

Exemplo:

```sql
CREATE TABLE test
(
    `key` String,
    `v1` UInt32,
    `v2` String,
    `v3` Float32
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

<div id="metrics">
  ## Métricas
</div>

Há também a tabela `system.rocksdb`, que expõe estatísticas do RocksDB:

```sql
SELECT
    name,
    value
FROM system.rocksdb

┌─name──────────────────────┬─value─┐
│ no.file.opens             │     1 │
│ number.block.decompressed │     1 │
└───────────────────────────┴───────┘
```

<div id="configuration">
  ## Configuração
</div>

Você também pode alterar qualquer [opção do RocksDB](https://github.com/facebook/rocksdb/wiki/Option-String-and-Option-Map) usando a config:

```xml
<rocksdb>
    <options>
        <max_background_jobs>8</max_background_jobs>
    </options>
    <column_family_options>
        <num_levels>2</num_levels>
    </column_family_options>
    <tables>
        <table>
            <name>TABLE</name>
            <options>
                <max_background_jobs>8</max_background_jobs>
            </options>
            <column_family_options>
                <num_levels>2</num_levels>
            </column_family_options>
        </table>
    </tables>
</rocksdb>
```

Por padrão, a otimização trivial de contagem aproximada fica desativada, o que pode afetar o desempenho das consultas `count()`. Para ativar essa
otimização, defina `optimize_trivial_approximate_count_query = 1`. Além disso, essa configuração afeta `system.tables` no engine EmbeddedRocksDB;
ative essa configuração para ver valores aproximados de `total_rows` e `total_bytes`.

<div id="supported-operations">
  ## Operações suportadas
</div>

<div id="inserts">
  ### Inserções
</div>

Quando novas linhas são inseridas em `EmbeddedRocksDB`, se a chave já existir, o valor será atualizado; caso contrário, uma nova chave será criada.

Exemplo:

```sql
INSERT INTO test VALUES ('some key', 1, 'value', 3.2);
```

<div id="deletes">
  ### Exclusões
</div>

As linhas podem ser excluídas com a consulta `DELETE` ou com `TRUNCATE`.

```sql
DELETE FROM test WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
ALTER TABLE test DELETE WHERE key LIKE 'some%' AND v1 > 1;
```

```sql
TRUNCATE TABLE test;
```

<div id="updates">
  ### Atualizações
</div>

Os valores podem ser atualizados usando a consulta `ALTER TABLE`. A chave primária não pode ser atualizada.

```sql
ALTER TABLE test UPDATE v1 = v1 * 10 + 2 WHERE key LIKE 'some%' AND v3 > 3.1;
```

<div id="joins">
  ### Junções
</div>

Há suporte a uma junção `direct` especial com tabelas EmbeddedRocksDB.
Essa junção `direct` evita a criação de uma tabela hash na memória e acessa
os dados diretamente do EmbeddedRocksDB.

Em junções grandes, você pode notar um uso de memória muito menor com junções `direct`,
porque a tabela hash não é criada.

Para habilitar junções `direct`:

```sql
SET join_algorithm = 'direct, hash'
```

:::tip
Quando `join_algorithm` estiver definido como `direct, hash`, serão usadas junções do tipo direct quando possível e, caso contrário, hash.
:::

<div id="example">
  #### Exemplo
</div>

<div id="create-and-populate-an-embeddedrocksdb-table">
  ##### Criar e preencher uma tabela EmbeddedRocksDB
</div>

```sql
CREATE TABLE rdb
(
    `key` UInt32,
    `value` Array(UInt32),
    `value2` String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY key
```

```sql
INSERT INTO rdb
    SELECT
        toUInt32(sipHash64(number) % 10) AS key,
        [key, key+1] AS value,
        ('val2' || toString(key)) AS value2
    FROM numbers_mt(10);
```

<div id="create-and-populate-a-table-to-join-with-table-rdb">
  ##### Criar e preencher uma tabela para fazer junção com a tabela `rdb`
</div>

```sql
CREATE TABLE t2
(
    `k` UInt16
)
ENGINE = TinyLog
```

```sql
INSERT INTO t2 SELECT number AS k
FROM numbers_mt(10)
```

<div id="set-the-join-algorithm-to-direct">
  ##### Configure o algoritmo de junção para `direct`
</div>

```sql
SET join_algorithm = 'direct'
```

<div id="an-inner-join">
  ##### Um INNER JOIN
</div>

```sql
SELECT *
FROM
(
    SELECT k AS key
    FROM t2
) AS t2
INNER JOIN rdb ON rdb.key = t2.key
ORDER BY key ASC
```

```response
┌─key─┬─rdb.key─┬─value──┬─value2─┐
│   0 │       0 │ [0,1]  │ val20  │
│   2 │       2 │ [2,3]  │ val22  │
│   3 │       3 │ [3,4]  │ val23  │
│   6 │       6 │ [6,7]  │ val26  │
│   7 │       7 │ [7,8]  │ val27  │
│   8 │       8 │ [8,9]  │ val28  │
│   9 │       9 │ [9,10] │ val29  │
└─────┴─────────┴────────┴────────┘
```

<div id="more-information-on-joins">
  ### Mais informações sobre junções
</div>

* [configuração `join_algorithm`](/pt-BR/operations/settings/settings.md#join_algorithm)
* [cláusula JOIN](/pt-BR/sql-reference/statements/select/join.md)