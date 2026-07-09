---
description: 'Documentação do mecanismo de tabela MySQL'
sidebar_label: 'MySQL'
sidebar_position: 138
slug: /engines/table-engines/integrations/mysql
title: 'Mecanismo de tabela MySQL'
doc_type: 'reference'
---

O mecanismo MySQL permite executar consultas `SELECT` e `INSERT` em dados armazenados em um servidor MySQL remoto.

<div id="creating-a-table">
  ## Criando uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = MySQL({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
SETTINGS
    [ connection_pool_size=16, ]
    [ connection_max_tries=3, ]
    [ connection_wait_timeout=5, ]
    [ connection_auto_close=true, ]
    [ connect_timeout=10, ]
    [ read_write_timeout=300, ]
    [ enable_compression=false ]
;
```

Veja uma descrição detalhada da consulta [CREATE TABLE](/pt-BR/sql-reference/statements/create/table).

A estrutura da tabela pode ser diferente da estrutura da tabela MySQL original:

* Os nomes das colunas devem ser os mesmos da tabela MySQL original, mas você pode usar apenas algumas delas, em qualquer ordem.
* Os tipos das colunas podem ser diferentes dos da tabela MySQL original. O ClickHouse tenta [converter](../../../engines/database-engines/mysql.md#data_types-support) os valores para os tipos de dados do ClickHouse.
* A configuração [external&#95;table&#95;functions&#95;use&#95;nulls](/pt-BR/operations/settings/settings#external_table_functions_use_nulls) define como lidar com colunas Nullable. Valor padrão: 1. Se for 0, a função de tabela não cria colunas Nullable e insere valores padrão em vez de nulls. Isso também se aplica a valores NULL dentro de arrays.

**Parâmetros do mecanismo**

* `host:port` — Endereço do servidor MySQL.
* `database` — nome do banco de dados remoto.
* `table` — Nome da tabela remota ou uma consulta enviada ao MySQL como está (consulte [Passando uma consulta em vez de um nome de tabela](#passing-a-query)).
* `user` — usuário MySQL.
* `password` — senha do usuário.
* `replace_query` — Flag que converte consultas `INSERT INTO` em `REPLACE INTO`. Se `replace_query=1`, a consulta é substituída.
* `on_duplicate_clause` — A expressão `ON DUPLICATE KEY on_duplicate_clause` adicionada à consulta `INSERT`.
  Exemplo: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, em que `on_duplicate_clause` é `UPDATE c2 = c2 + 1`. Consulte a [documentação do MySQL](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) para ver quais valores de `on_duplicate_clause` você pode usar com a cláusula `ON DUPLICATE KEY`.
  Para especificar `on_duplicate_clause`, você precisa passar `0` para o parâmetro `replace_query`. Se passar `replace_query = 1` e `on_duplicate_clause` ao mesmo tempo, o ClickHouse gerará uma exceção.

Os argumentos também podem ser passados usando [coleções nomeadas](/pt-BR/operations/named-collections.md). Nesse caso, `host` e `port` devem ser especificados separadamente. Essa abordagem é recomendada para ambientes de produção.

Cláusulas `WHERE` simples, como `=, !=, >, >=, <, <=`, são executadas no servidor MySQL.

As demais condições e a restrição de amostragem `LIMIT` são executadas no ClickHouse somente depois que a consulta ao MySQL termina.

<div id="passing-a-query">
  ## Passando uma consulta em vez de um nome de tabela
</div>

Em vez de um nome de tabela, o argumento `table` pode ser uma consulta `SELECT` enviada ao MySQL sem modificações. A estrutura da tabela é inferida com base no resultado da consulta. A consulta pode ser escrita na forma de subconsulta ou encapsulada na função `query`:

```sql
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Isso é útil para delegar junções, agregações ou qualquer outro processamento ao MySQL. Essa tabela é somente leitura: não é permitido executar `INSERT` nela. A mesma sintaxe é compatível com a função de tabela [`mysql`](/pt-BR/sql-reference/table-functions/mysql).

:::note
A forma de subconsulta `(SELECT ...)` é analisada pelo ClickHouse e serializada novamente no dialeto do MySQL (identificadores entre crases) antes de ser enviada ao servidor. Portanto, ela deve ser um SQL do ClickHouse válido. Para passar uma sintaxe específica do MySQL que o ClickHouse não analisa, use a forma `query('...')`, cujo texto é enviado ao MySQL literalmente.

Qualquer `WHERE`, `LIMIT`, agregação etc. externo da consulta do ClickHouse ao redor **não** é delegado à consulta enviada — ele é aplicado no ClickHouse depois que o resultado completo da consulta é obtido. Para restringir os dados lidos do MySQL, coloque o filtro dentro da consulta enviada. Com [`external_table_strict_query = 1`](/pt-BR/operations/settings/settings#external_table_strict_query), um filtro externo que não possa ser delegado é rejeitado com uma exceção, em vez de ser aplicado localmente.
:::

Suporta múltiplas réplicas, que devem ser listadas com `|`. Por exemplo:

```sql
CREATE TABLE test_replicas (id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL(`mysql{2|3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

<div id="usage-example">
  ## Exemplo de uso
</div>

Crie uma tabela no MySQL:

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

Criar tabela no ClickHouse usando argumentos simples:

```sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

Ou usando [coleções nomeadas](/pt-BR/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL(creds, table='test')
```

Recuperando dados da tabela do MySQL:

```sql
SELECT * FROM mysql_table
```

```text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

<div id="mysql-settings">
  ## Configurações
</div>

As configurações padrão não são muito eficientes, pois nem sequer reutilizam as conexões. Essas configurações permitem aumentar o número de consultas que o servidor executa por segundo.

<div id="connection-auto-close">
  ### `connection_auto_close`
</div>

Permite fechar automaticamente a conexão após a execução da consulta, ou seja, desativar a reutilização da conexão.

Valores possíveis:

* 1 — O fechamento automático da conexão é permitido, portanto a reutilização da conexão fica desativada
* 0 — O fechamento automático da conexão não é permitido, portanto a reutilização da conexão fica ativada

Valor padrão: `1`.

<div id="connection-max-tries">
  ### `connection_max_tries`
</div>

Define o número de tentativas de repetição para o pool com failover.

Valores possíveis:

* Inteiro positivo.
* 0 — Não há tentativas de repetição para o pool com failover.

Valor padrão: `3`.

<div id="connection-pool-size">
  ### `connection_pool_size`
</div>

Tamanho do pool de conexões (se todas as conexões estiverem em uso, a consulta aguardará até que uma delas seja liberada).

Valores possíveis:

* Inteiro positivo.

Valor padrão: `16`.

<div id="connection-wait-timeout">
  ### `connection_wait_timeout`
</div>

Tempo limite (em segundos) para aguardar uma conexão livre (caso já haja `connection_pool_size` conexões ativas); 0 — não aguardar.

Valores possíveis:

* Inteiro positivo.

Valor padrão: `5`.

<div id="connect-timeout">
  ### `connect_timeout`
</div>

Tempo limite de conexão (em segundos).

Valores possíveis:

* Número inteiro positivo.

Valor padrão: `10`.

<div id="read-write-timeout">
  ### `read_write_timeout`
</div>

Tempo limite de leitura/gravação (em segundos).

Valores possíveis:

* Número inteiro positivo.

Valor padrão: `300`.

<div id="enable-compression">
  ### `enable_compression`
</div>

Habilita a compressão na conexão do protocolo MySQL.

Valor padrão: `false`.

Esta configuração se aplica a:

* o mecanismo de tabela `MySQL`;
* o mecanismo de banco de dados `MySQL`;
* a função de tabela `mysql`;
* coleções nomeadas usadas por integrações com MySQL.

Quando habilitada, o ClickHouse solicita o uso de compressão na conexão.

Exemplo:

```sql
CREATE TABLE mysql_engine_compression
(
    id UInt32,
    name String,
    age UInt32,
    money UInt32
)
ENGINE = MySQL('mysql80:3306', 'clickhouse', 'test_table', 'root', 'password')
SETTINGS enable_compression = 1;
```

<div id="see-also">
  ## Veja também
</div>

* [A função de tabela MySQL](../../../sql-reference/table-functions/mysql.md)
* [Usando o MySQL como origem de dicionário](/pt-BR/sql-reference/statements/create/dictionary/sources/mysql)