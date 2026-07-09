---
description: 'Permite executar consultas `SELECT` e `INSERT` em dados armazenados
  em um servidor PostgreSQL remoto.'
sidebar_label: 'postgresql'
sidebar_position: 160
slug: /sql-reference/table-functions/postgresql
title: 'postgresql'
doc_type: 'reference'
---

Permite executar consultas `SELECT` e `INSERT` em dados armazenados em um servidor PostgreSQL remoto.

<div id="syntax">
  ## Sintaxe
</div>

```sql
postgresql({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento     | Descrição                                                                                                                                                    |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `host:port`   | Endereço do servidor PostgreSQL.                                                                                                                             |
| `database`    | nome do banco de dados remoto.                                                                                                                               |
| `table`       | Nome da tabela remota ou uma consulta passada ao PostgreSQL sem alterações (consulte [Passando uma consulta em vez de um nome de tabela](#passing-a-query)). |
| `user`        | Usuário do PostgreSQL.                                                                                                                                       |
| `password`    | Senha do usuário.                                                                                                                                            |
| `schema`      | Esquema de tabela diferente do padrão. Opcional.                                                                                                             |
| `on_conflict` | Estratégia de resolução de conflitos. Exemplo: `ON CONFLICT DO NOTHING`. Opcional.                                                                           |

Os argumentos também podem ser passados usando [coleções nomeadas](/pt-BR/operations/named-collections.md). Nesse caso, `host` e `port` devem ser especificados separadamente. Essa abordagem é recomendada para produção.

<div id="returned_value">
  ## Valor retornado
</div>

Um objeto de tabela com as mesmas colunas da tabela PostgreSQL original.

:::note
Na consulta `INSERT`, para distinguir a função de tabela `postgresql(...)` de um nome de tabela com uma lista de nomes de colunas, você deve usar as palavras-chave `FUNCTION` ou `TABLE FUNCTION`. Veja os exemplos abaixo.
:::

<div id="implementation-details">
  ## Detalhes de implementação
</div>

As consultas `SELECT` no PostgreSQL são executadas como `COPY (SELECT ...) TO STDOUT` em uma transação somente leitura do PostgreSQL, com `commit` após cada consulta `SELECT`.

Cláusulas `WHERE` simples, como `=`, `!=`, `>`, `>=`, `<`, `<=` e `IN`, são executadas no servidor PostgreSQL.

Todas as junções, agregações, ordenações, condições `IN [ array ]` e a restrição de amostragem `LIMIT` só são executadas no ClickHouse após a conclusão da consulta ao PostgreSQL.

<div id="passing-a-query">
  ## Passando uma consulta em vez de um nome de tabela
</div>

Em vez de um nome de tabela, o terceiro argumento pode ser uma consulta `SELECT` passada ao PostgreSQL como está. A estrutura da tabela resultante é inferida a partir do resultado da consulta. A consulta pode ser escrita como uma subconsulta ou encapsulada na função `query`:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM postgresql('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Isso é útil para delegar junções, agregações ou qualquer outro processamento ao PostgreSQL. Essa tabela é somente leitura: não é permitido executar `INSERT` nela. A mesma sintaxe é compatível com o motor de tabela [`PostgreSQL`](/pt-BR/engines/table-engines/integrations/postgresql).

:::note
A forma de subconsulta `(SELECT ...)` é analisada pelo ClickHouse e resserializada no dialeto do PostgreSQL (aspas de identificadores do PostgreSQL e escaping de literais de string) antes de ser enviada ao servidor. Portanto, ela deve ser um ClickHouse SQL válido. Para passar uma sintaxe específica do PostgreSQL que o ClickHouse não analisa, use a forma `query('...')`, cujo texto é enviado ao PostgreSQL literalmente.

Qualquer `WHERE`, `LIMIT`, agregação etc. externo da consulta ClickHouse em volta **não** é delegado à consulta fornecida — ele é aplicado no ClickHouse depois que o resultado completo da consulta é obtido. Para restringir os dados lidos do PostgreSQL, coloque o filtro dentro da consulta fornecida. Com [`external_table_strict_query = 1`](/pt-BR/operations/settings/settings#external_table_strict_query), um filtro externo que não pode ser delegado é rejeitado com uma exceção, em vez de ser aplicado localmente.
:::

As consultas `INSERT` no lado do PostgreSQL são executadas como `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` dentro de uma transação do PostgreSQL, com auto-commit após cada instrução `INSERT`.

Os tipos Array do PostgreSQL são convertidos em arrays do ClickHouse.

:::note
Cuidado: no PostgreSQL, uma coluna de tipo array, como Integer[], pode conter arrays com dimensões diferentes em linhas distintas, mas no ClickHouse só é permitido ter arrays multidimensionais com a mesma dimensão em todas as linhas.
:::

Suporta múltiplas réplicas, que devem ser listadas com `|`. Por exemplo:

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

or

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

Oferece suporte à prioridade de réplicas para a fonte de dicionário do PostgreSQL. Quanto maior o número no map, menor a prioridade. A prioridade mais alta é `0`.

<div id="examples">
  ## Exemplos
</div>

Tabela no PostgreSQL:

```text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# INSERT INTO test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> SELECT * FROM test;
  int_id | int_nullable | float | str  | float_nullable
 --------+--------------+-------+------+----------------
       1 |              |     2 | test |
(1 row)
```

Selecionando dados do ClickHouse com argumentos simples:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

Ou usando [coleções nomeadas](/pt-BR/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION mypg AS
        host = 'localhost',
        port = 5432,
        database = 'test',
        user = 'postgresql_user',
        password = 'password';
SELECT * FROM postgresql(mypg, table='test') WHERE str IN ('test');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Inserção:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Usando um esquema não padrão:

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

<div id="related">
  ## Veja também
</div>

* [O motor de tabela PostgreSQL](../../engines/table-engines/integrations/postgresql.md)
* [Usando o PostgreSQL como fonte de dicionário](/pt-BR/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="replicating-or-migrating-postgres-data-with-peerdb">
  ### Replicando ou migrando dados do Postgres com o PeerDB
</div>

> Além das funções de tabela, você também pode usar o [PeerDB](https://docs.peerdb.io/introduction) da ClickHouse para configurar um pipeline contínuo de dados do Postgres para o ClickHouse. O PeerDB é uma ferramenta projetada especificamente para replicar dados do Postgres para o ClickHouse usando CDC (captura de dados de alterações).