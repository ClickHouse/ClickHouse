---
description: 'Permite executar consultas `SELECT` e `INSERT` em dados armazenados em
  um servidor MySQL remoto.'
sidebar_label: 'mysql'
sidebar_position: 137
slug: /sql-reference/table-functions/mysql
title: 'mysql'
doc_type: 'reference'
---

Permite executar consultas `SELECT` e `INSERT` em dados armazenados em um servidor MySQL remoto.

<div id="syntax">
  ## Sintaxe
</div>

```sql
mysql({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento             | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host:port`           | Endereço do servidor MySQL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `database`            | Nome do banco de dados remoto.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `table`               | Nome da tabela remota ou uma consulta passada ao MySQL sem modificações (consulte [Passando uma consulta em vez de um nome de tabela](#passing-a-query)).                                                                                                                                                                                                                                                                                                                                                                            |
| `user`                | Usuário MySQL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `password`            | Senha do usuário.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `replace_query`       | Flag que converte consultas `INSERT INTO` em `REPLACE INTO`. Valores possíveis:<br />    - `0` - A consulta é executada como `INSERT INTO`.<br />    - `1` - A consulta é executada como `REPLACE INTO`.                                                                                                                                                                                                                                                                                                                             |
| `on_duplicate_clause` | A expressão `ON DUPLICATE KEY on_duplicate_clause` adicionada à consulta `INSERT`. Só pode ser especificada com `replace_query = 0` (se você passar `replace_query = 1` e `on_duplicate_clause` ao mesmo tempo, o ClickHouse gera uma exceção).<br />    Exemplo: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1;`<br />    Aqui, `on_duplicate_clause` é `UPDATE c2 = c2 + 1`. Consulte a documentação do MySQL para saber qual `on_duplicate_clause` pode ser usada com a cláusula `ON DUPLICATE KEY`. |

Os argumentos também podem ser passados usando [coleções nomeadas](/pt-BR/operations/named-collections.md). Nesse caso, `host` e `port` devem ser especificados separadamente. Essa abordagem é recomendada para o ambiente de produção.

Cláusulas `WHERE` simples, como `=, !=, >, >=, <, <=`, atualmente são executadas no servidor MySQL.

O restante das condições e a restrição de amostragem `LIMIT` são executados no ClickHouse somente após o término da consulta ao MySQL.

<div id="passing-a-query">
  ## Passar uma consulta em vez de um nome de tabela
</div>

Em vez de um nome de tabela, o terceiro argumento pode ser uma consulta `SELECT` passada ao MySQL como está. A estrutura da tabela resultante é inferida a partir do resultado da consulta. A consulta pode ser escrita como uma subconsulta ou encapsulada na função `query`:

```sql
SELECT * FROM mysql('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM mysql('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Isso é útil para delegar ao MySQL junções, agregações ou qualquer outro processamento. Essa tabela é somente leitura: `INSERT` nela não é permitido. A mesma sintaxe é compatível com o motor de tabela [`MySQL`](/pt-BR/engines/table-engines/integrations/mysql).

:::note
A forma de subconsulta `(SELECT ...)` é analisada pelo ClickHouse e serializada novamente no dialeto do MySQL (identificadores entre acentos graves) antes de ser enviada ao servidor. Portanto, ela deve ser válida em ClickHouse SQL. Para passar sintaxe específica do MySQL que o ClickHouse não analisa, use a forma `query('...')`, cujo texto é enviado ao MySQL literalmente.

Qualquer `WHERE`, `LIMIT`, agregação etc. externo da consulta ClickHouse circundante **não** é delegado para a consulta passada — ele é aplicado no ClickHouse depois que o resultado completo da consulta é buscado. Para restringir os dados lidos do MySQL, coloque o filtro dentro da consulta passada. Com [`external_table_strict_query = 1`](/pt-BR/operations/settings/settings#external_table_strict_query), um filtro externo que não pode ser delegado é rejeitado com uma exceção, em vez de ser aplicado localmente.
:::

Compatível com múltiplas réplicas, que devem ser listadas com `|`. Por exemplo:

```sql
SELECT name FROM mysql(`mysql{1|2|3}:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

ou

```sql
SELECT name FROM mysql(`mysql1:3306|mysql2:3306|mysql3:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

<div id="returned_value">
  ## Valor retornado
</div>

Um objeto de tabela com as mesmas colunas da tabela MySQL original.

:::note
Alguns tipos de dados do MySQL podem ser mapeados para diferentes tipos do ClickHouse — isso é controlado pela configuração em nível de consulta [mysql&#95;datatypes&#95;support&#95;level](/pt-BR/operations/settings/settings.md#mysql_datatypes_support_level)
:::

:::note
Na consulta `INSERT`, para distinguir a função de tabela `mysql(...)` de um nome de tabela com uma lista de nomes de colunas, você deve usar as palavras-chave `FUNCTION` ou `TABLE FUNCTION`. Veja os exemplos abaixo.
:::

<div id="examples">
  ## Exemplos
</div>

Tabela no MySQL:

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));

mysql> INSERT INTO test (`int_id`, `float`) VALUES (1,2);

mysql> SELECT * FROM test;
+--------+-------+
| int_id | float |
+--------+-------+
|      1 |     2 |
+--------+-------+
```

Selecionando dados do ClickHouse:

```sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

Ou com [coleções nomeadas](/pt-BR/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
SELECT * FROM mysql(creds, table='test');
```

```text
┌─int_id─┬─float─┐
│      1 │     2 │
└────────┴───────┘
```

<div id="enable-compression">
  ### `enable_compression`
</div>

Habilita a compressão para a conexão do protocolo MySQL.

Valor padrão: `false`.

Essa configuração se aplica a:

* a função de tabela `mysql`;
* o motor de tabela `MySQL`;
* o mecanismo de banco de dados `MySQL`;
* coleções nomeadas usadas por integrações com MySQL.

Quando ativada, o ClickHouse solicita compressão para a conexão.

Exemplo:

```sql
SELECT *
FROM mysql(
    'mysql80:3306',
    'clickhouse',
    'test_table',
    'root',
    'password',
    SETTINGS enable_compression = 1
);
```

Substituição e inserção:

```sql
INSERT INTO FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 1) (int_id, float) VALUES (1, 3);
INSERT INTO TABLE FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 0, 'UPDATE int_id = int_id + 1') (int_id, float) VALUES (1, 4);
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

```text
┌─int_id─┬─float─┐
│      1 │     3 │
│      2 │     4 │
└────────┴───────┘
```

Copiando dados de uma tabela MySQL para uma tabela do ClickHouse:

```sql
CREATE TABLE mysql_copy
(
   `id` UInt64,
   `datetime` DateTime('UTC'),
   `description` String,
)
ENGINE = MergeTree
ORDER BY (id,datetime);

INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password');
```

Ou, se estiver copiando apenas um lote incremental do MySQL com base no ID máximo atual:

```sql
INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password')
WHERE id > (SELECT max(id) FROM mysql_copy);
```

<div id="related">
  ## Veja também
</div>

* [O motor de tabela &#39;MySQL&#39;](../../engines/table-engines/integrations/mysql.md)
* [Usando o MySQL como fonte de dicionário](/pt-BR/sql-reference/statements/create/dictionary/sources/mysql)
* [mysql&#95;datatypes&#95;support&#95;level](/pt-BR/operations/settings/settings.md#mysql_datatypes_support_level)
* [mysql&#95;map&#95;fixed&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/pt-BR/operations/settings/settings.md#mysql_map_fixed_string_to_text_in_show_columns)
* [mysql&#95;map&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/pt-BR/operations/settings/settings.md#mysql_map_string_to_text_in_show_columns)
* [mysql&#95;max&#95;rows&#95;to&#95;insert](/pt-BR/operations/settings/settings.md#mysql_max_rows_to_insert)