---
description: 'A função de tabela `remote` permite acessar servidores remotos em tempo de execução,
  ou seja, sem criar uma tabela [Distributed](../../engines/table-engines/special/distributed.md). A função de tabela `remoteSecure` é igual
  à `remote`, mas usa uma conexão segura.'
sidebar_label: 'remote'
sidebar_position: 175
slug: /sql-reference/table-functions/remote
title: 'remote, remoteSecure'
doc_type: 'reference'
---

A função de tabela `remote` permite acessar servidores remotos em tempo de execução, ou seja, sem criar uma tabela [Distributed](../../engines/table-engines/special/distributed.md). A função de tabela `remoteSecure` é igual à `remote`, mas usa uma conexão segura.

Ambas as funções podem ser usadas em consultas `SELECT` e `INSERT`.

<div id="syntax">
  ## Sintaxe
</div>

```sql
remote(addresses_expr, [db, table, user [, password], sharding_key])
remote(addresses_expr, [db.table, user [, password], sharding_key])
remote(named_collection[, option=value [,..]])
remoteSecure(addresses_expr, [db, table, user [, password], sharding_key])
remoteSecure(addresses_expr, [db.table, user [, password], sharding_key])
remoteSecure(named_collection[, option=value [,..]])
```

<div id="parameters">
  ## Parâmetros
</div>

| Argumento        | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `addresses_expr` | Um endereço de servidor remoto ou uma expressão que gera vários endereços de servidores remotos. Formato: `host` ou `host:port`.<br /><br />    O `host` pode ser especificado como um nome de servidor ou como um endereço IPv4 ou IPv6. Um endereço IPv6 deve ser especificado entre `[]`.<br /><br />    O `port` é a porta TCP no servidor remoto. Se a porta for omitida, será usado [tcp&#95;port](../../operations/server-configuration-parameters/settings.md#tcp_port) do arquivo de configuração do servidor para a função de tabela `remote` (por padrão, 9000) e [tcp&#95;port&#95;secure](../../operations/server-configuration-parameters/settings.md#tcp_port_secure) para a função de tabela `remoteSecure` (por padrão, 9440).<br /><br />    Para endereços IPv6, a porta é obrigatória.<br /><br />    Se apenas o parâmetro `addresses_expr` for especificado, `db` e `table` usarão `system.one` por padrão.<br /><br />    Tipo: [String](../../sql-reference/data-types/string.md). |
| `db`             | Nome do banco de dados. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `table`          | Nome da tabela. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `user`           | Nome do usuário. Se não for especificado, `default` será usado. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `password`       | Senha do usuário. Se não for especificada, uma senha vazia será usada. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| `sharding_key`   | Chave de sharding para permitir a distribuição de dados entre nós. Por exemplo: `insert into remote('127.0.0.1:9000,127.0.0.2', db, table, 'default', rand())`. Tipo: [UInt32](../../sql-reference/data-types/int-uint.md).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |

Os argumentos também podem ser passados usando [coleções nomeadas](/pt-BR/operations/named-collections.md).

<div id="returned-value">
  ## Valor retornado
</div>

Uma tabela localizada em um servidor remoto.

<div id="usage">
  ## Uso
</div>

Como as funções de tabela `remote` e `remoteSecure` restabelecem a conexão a cada requisição, recomenda-se usar uma tabela `Distributed` em vez delas. Além disso, se os hostnames estiverem definidos, os nomes serão resolvidos, e os erros não serão contabilizados ao trabalhar com várias réplicas. Ao processar um grande número de consultas, sempre crie a tabela `Distributed` com antecedência e não use a função de tabela `remote`.

A função de tabela `remote` pode ser útil nos seguintes casos:

* Migração única de dados de um sistema para outro
* Acesso a um servidor específico para comparação de dados, depuração e testes, ou seja, conexões ad hoc.
* Consultas entre vários clusters do ClickHouse para fins de pesquisa.
* Requisições distribuídas pouco frequentes feitas manualmente.
* Requisições distribuídas em que o conjunto de servidores é redefinido a cada vez.

<div id="addresses">
  ### Endereços
</div>

```text
example01-01-1
example01-01-1:9440
example01-01-1:9000
localhost
127.0.0.1
[::]:9440
[::]:9000
[2a02:6b8:0:1111::11]:9000
```

Vários endereços podem ser separados por vírgulas. Nesse caso, o ClickHouse usará processamento distribuído e enviará a consulta para todos os endereços especificados (como shards que contêm dados diferentes). Exemplo:

```text
example01-01-1,example01-02-1
```

<div id="examples">
  ## Exemplos
</div>

<div id="selecting-data-from-a-remote-server">
  ### Selecionando dados de um servidor remoto:
</div>

```sql
SELECT * FROM remote('127.0.0.1', db.remote_engine_table) LIMIT 3;
```

Ou com [coleções nomeadas](/pt-BR/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = '127.0.0.1',
        database = 'db';
SELECT * FROM remote(creds, table='remote_engine_table') LIMIT 3;
```

<div id="inserting-data-into-a-table-on-a-remote-server">
  ### Inserção de dados em uma tabela em um servidor remoto:
</div>

```sql
CREATE TABLE remote_table (name String, value UInt32) ENGINE=Memory;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), 'remote_table') VALUES ('test', 42);
SELECT * FROM remote_table;
```

<div id="migration-of-tables-from-one-system-to-another">
  ### Migração de tabelas de um sistema para outro:
</div>

Este exemplo usa uma tabela de um conjunto de dados de exemplo.  O banco de dados é `imdb`, e a tabela é `actors`.

<div id="on-the-source-clickhouse-system-the-system-that-currently-hosts-the-data">
  #### No sistema ClickHouse de origem (o sistema que hospeda os dados atualmente)
</div>

* Verifique o nome do banco de dados de origem e da tabela (`imdb.actors`)

  ```sql
  show databases
  ```

  ```sql
  show tables in imdb
  ```

* Obtenha a instrução CREATE TABLE na origem:

```sql
  SELECT create_table_query
  FROM system.tables
  WHERE database = 'imdb' AND table = 'actors'
```

Resposta

```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
```

<div id="on-the-destination-clickhouse-system">
  #### No sistema ClickHouse de destino
</div>

* Crie o banco de dados de destino:

  ```sql
  CREATE DATABASE imdb
  ```

* Usando a instrução CREATE TABLE da origem, crie a tabela de destino:

  ```sql
  CREATE TABLE imdb.actors (`id` UInt32,
                            `first_name` String,
                            `last_name` String,
                            `gender` FixedString(1))
                  ENGINE = MergeTree
                  ORDER BY (id, first_name, last_name, gender);
  ```

<div id="back-on-the-source-deployment">
  #### De volta à implantação de origem
</div>

Insira dados no novo banco de dados e na nova tabela criados no sistema remoto. Você precisará do host, da porta, do nome de usuário, da senha, do banco de dados de destino e da tabela de destino.

```sql
INSERT INTO FUNCTION
remoteSecure('remote.clickhouse.cloud:9440', 'imdb.actors', 'USER', 'PASSWORD')
SELECT * from imdb.actors
```

<div id="globs-in-addresses">
  ## Globbing
</div>

Padrões entre `{ }` são usados para gerar um conjunto de shards e para especificar réplicas. Se houver vários pares de `{ }`, será gerado o produto cartesiano dos conjuntos correspondentes.

Há suporte para os seguintes tipos de padrão.

* `{a,b,c}` - Representa qualquer uma das strings alternativas `a`, `b` ou `c`. O padrão é substituído por `a` no endereço do primeiro shard, por `b` no endereço do segundo shard, e assim por diante. Por exemplo, `example0{1,2}-1` gera os endereços `example01-1` e `example02-1`.
* `{N..M}` - Um intervalo de números. Esse padrão gera endereços de shard com índices incrementais de `N` até `M` (inclusive). Por exemplo, `example0{1..2}-1` gera `example01-1` e `example02-1`.
* `{0n..0m}` - Um intervalo de números com zeros à esquerda. Esse padrão preserva os zeros à esquerda nos índices. Por exemplo, `example{01..03}-1` gera `example01-1`, `example02-1` e `example03-1`.
* `{a|b}` - Qualquer quantidade de variantes separadas por `|`. O padrão especifica réplicas. Por exemplo, `example01-{1|2}` gera as réplicas `example01-1` e `example01-2`.

A consulta será enviada para a primeira réplica saudável. No entanto, para `remote`, as réplicas são iteradas na ordem atualmente definida na configuração [load&#95;balancing](../../operations/settings/settings.md#load_balancing).
O número de endereços gerados é limitado pela configuração [table&#95;function&#95;remote&#95;max&#95;addresses](../../operations/settings/settings.md#table_function_remote_max_addresses).