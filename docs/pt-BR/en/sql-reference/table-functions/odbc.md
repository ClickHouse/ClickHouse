---
description: 'Retorna a tabela conectada via ODBC.'
sidebar_label: 'odbc'
sidebar_position: 150
slug: /sql-reference/table-functions/odbc
title: 'odbc'
doc_type: 'reference'
---

Retorna a tabela conectada via [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

<div id="syntax">
  ## Sintaxe
</div>

```sql
odbc(datasource, external_database, external_table)
odbc(datasource, external_table)
odbc(named_collection)
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento           | Descrição                                                            |
| ------------------- | -------------------------------------------------------------------- |
| `datasource`        | Nome da seção com as configurações de conexão no arquivo `odbc.ini`. |
| `external_database` | Nome de um banco de dados em um DBMS externo.                        |
| `external_table`    | Nome de uma tabela no `external_database`.                           |

Esses parâmetros também podem ser passados usando [coleções nomeadas](/pt-BR/operations/named-collections.md).

Para implementar conexões ODBC com segurança, o ClickHouse usa um programa separado, `clickhouse-odbc-bridge`. Se o driver for carregado diretamente pelo `clickhouse-server`, problemas no driver podem fazer o servidor ClickHouse falhar. O ClickHouse inicia automaticamente o `clickhouse-odbc-bridge` quando necessário. O programa ODBC bridge é instalado a partir do mesmo pacote que o `clickhouse-server`.

Os campos com valores `NULL` da tabela externa são convertidos nos valores padrão do tipo de dado base. Por exemplo, se um campo de uma tabela MySQL remota tiver o tipo `INT NULL`, ele será convertido em 0 (o valor padrão do tipo de dado `Int32` do ClickHouse).

<div id="usage-example">
  ## Exemplo de uso
</div>

**Obtendo dados da instalação local do MySQL via ODBC**

Este exemplo foi testado no Ubuntu Linux 18.04 e no servidor MySQL 5.7.

Certifique-se de que `unixODBC` e o MySQL Connector estejam instalados.

Por padrão (quando instalado a partir de pacotes), o ClickHouse é iniciado como o usuário `clickhouse`. Portanto, você precisa criar e configurar esse usuário no servidor MySQL.

```bash
$ sudo mysql
```

```sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

Em seguida, configure a conexão em `/etc/odbc.ini`.

```bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USERNAME = clickhouse
PASSWORD = clickhouse
```

Você pode verificar a conexão usando o utilitário `isql` da instalação do unixODBC.

```bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

Tabela no MySQL:

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

Obtendo dados da tabela MySQL no ClickHouse:

```sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

<div id="see-also">
  ## Veja também
</div>

* [Dicionários ODBC](/pt-BR/sql-reference/statements/create/dictionary/sources/odbc)
* [Mecanismo de tabela ODBC](/pt-BR/engines/table-engines/integrations/odbc).