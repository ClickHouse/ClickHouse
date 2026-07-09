---
description: 'O motor PostgreSQL permite consultas `SELECT` e `INSERT` em dados armazenados
  em um servidor PostgreSQL remoto.'
sidebar_label: 'PostgreSQL'
sidebar_position: 160
slug: /engines/table-engines/integrations/postgresql
title: 'Motor de tabela PostgreSQL'
doc_type: 'guide'
---

O motor PostgreSQL permite consultas `SELECT` e `INSERT` em dados armazenados em um servidor PostgreSQL remoto.

:::note
No momento, apenas as versões 12 e posteriores do PostgreSQL são compatíveis com o motor de tabela.
:::

:::tip
Conheça nosso serviço [Managed Postgres](/pt-BR/docs/cloud/managed-postgres). Com armazenamento NVMe fisicamente co-localizado com a computação, ele oferece desempenho até 10x superior para cargas de trabalho limitadas por disco em comparação com alternativas que usam armazenamento conectado à rede, como o EBS, e permite replicar seus dados do Postgres para o ClickHouse usando o conector Postgres CDC no ClickPipes.
:::

<div id="creating-a-table">
  ## Criar uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 type1 [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 type2 [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = PostgreSQL({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

Consulte uma descrição detalhada da consulta [CREATE TABLE](/pt-BR/sql-reference/statements/create/table).

A estrutura da tabela pode ser diferente da estrutura da tabela PostgreSQL original:

* Os nomes das colunas devem ser os mesmos da tabela PostgreSQL original, mas você pode usar apenas algumas delas e em qualquer ordem.
* Os tipos das colunas podem ser diferentes dos da tabela PostgreSQL original. O ClickHouse tenta [converter](../../../engines/database-engines/postgresql.md#data_types-support) os valores para os tipos de dados do ClickHouse.
* A configuração [external&#95;table&#95;functions&#95;use&#95;nulls](/pt-BR/operations/settings/settings#external_table_functions_use_nulls) define como lidar com colunas Nullable. Valor padrão: 1. Se for 0, a função de tabela não cria colunas Nullable e insere valores padrão em vez de valores NULL. Isso também se aplica a valores NULL dentro de arrays.

**Parâmetros do mecanismo**

* `host:port` — Endereço do servidor PostgreSQL.
* `database` — Nome do banco de dados remoto.
* `table` — Nome da tabela remota ou uma consulta passada ao PostgreSQL como está (consulte [Passar uma consulta em vez de um nome de tabela](#passing-a-query)).
* `user` — Usuário do PostgreSQL.
* `password` — Senha do usuário.
* `schema` — Schema de tabela não padrão. Opcional.
* `on_conflict` — Estratégia de resolução de conflitos. Exemplo: `ON CONFLICT DO NOTHING`. Opcional. Observação: adicionar essa opção tornará a inserção menos eficiente.

[Coleções nomeadas](/pt-BR/operations/named-collections.md) (disponíveis desde a versão 21.11) são recomendadas para o ambiente de produção. Aqui está um exemplo:

```xml
<named_collections>
    <postgres_creds>
        <host>localhost</host>
        <port>5432</port>
        <user>postgres</user>
        <password>****</password>
        <schema>schema1</schema>
    </postgres_creds>
</named_collections>
```

Alguns parâmetros podem ser substituídos por argumentos de chave-valor:

```sql
SELECT * FROM postgresql(postgres_creds, table='table1');
```

<div id="implementation-details">
  ## Detalhes de implementação
</div>

As consultas `SELECT` no PostgreSQL são executadas como `COPY (SELECT ...) TO STDOUT` dentro de uma transação do PostgreSQL somente leitura, com commit após cada consulta `SELECT`.

Cláusulas `WHERE` simples, como `=`, `!=`, `>`, `>=`, `<`, `<=` e `IN`, são executadas no servidor PostgreSQL.

Todas as junções, agregações, ordenação, condições `IN [ array ]` e a restrição de amostragem `LIMIT` são executadas no ClickHouse somente depois que a consulta ao PostgreSQL termina.

<div id="passing-a-query">
  ## Usar uma consulta em vez de um nome de tabela
</div>

Em vez de um nome de tabela, o argumento `table` pode ser uma consulta `SELECT` enviada ao PostgreSQL como está. A estrutura da tabela é inferida a partir do resultado da consulta. A consulta pode ser escrita como uma subconsulta ou encapsulada na função `query`:

```sql
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Isso é útil para delegar junções, agregações ou qualquer outro processamento ao PostgreSQL. Essa tabela é somente leitura: `INSERT` nela não é permitido. A mesma sintaxe é compatível com a função de tabela [`postgresql`](/pt-BR/sql-reference/table-functions/postgresql).

:::note
A forma de subconsulta `(SELECT ...)` é analisada pelo ClickHouse e serializada novamente no dialeto do PostgreSQL (aspas de identificadores do PostgreSQL e escape de literais de string) antes de ser enviada ao servidor. Portanto, ela deve ser um ClickHouse SQL válido. Para passar uma sintaxe específica do PostgreSQL que o ClickHouse não analisa, use a forma `query('...')`, cujo texto é enviado ao PostgreSQL literalmente.

Qualquer `WHERE`, `LIMIT`, agregação etc. externo da consulta ClickHouse ao redor **não** é delegado à consulta fornecida — ele é aplicado no ClickHouse depois que o resultado completo da consulta é obtido. Para restringir os dados lidos do PostgreSQL, coloque o filtro dentro da consulta fornecida. Com [`external_table_strict_query = 1`](/pt-BR/operations/settings/settings#external_table_strict_query), um filtro externo que não pode ser delegado é rejeitado com uma exceção, em vez de ser aplicado localmente.
:::

As consultas `INSERT` no PostgreSQL são executadas como `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` dentro de uma transação do PostgreSQL, com commit automático após cada instrução `INSERT`.

Os tipos `Array` do PostgreSQL são convertidos em arrays do ClickHouse.

:::note
Tenha cuidado: no PostgreSQL, um dado de array, criado como `type_name[]`, pode conter arrays multidimensionais com números diferentes de dimensões em diferentes linhas da tabela na mesma coluna. Porém, no ClickHouse, só é permitido ter arrays multidimensionais com a mesma quantidade de dimensões em todas as linhas da tabela na mesma coluna.
:::

Oferece suporte a múltiplas réplicas, que devem ser listadas com `|`. Por exemplo:

```sql
CREATE TABLE test_replicas (id UInt32, name String) ENGINE = PostgreSQL(`postgres{2|3|4}:5432`, 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword');
```

Há suporte à prioridade de réplicas para a fonte de dicionário PostgreSQL. Quanto maior o número no map, menor a prioridade. A prioridade mais alta é `0`.

No exemplo abaixo, a réplica `example01-1` tem a maior prioridade:

```xml
<postgresql>
    <port>5432</port>
    <user>clickhouse</user>
    <password>qwerty</password>
    <replica>
        <host>example01-1</host>
        <priority>1</priority>
    </replica>
    <replica>
        <host>example01-2</host>
        <priority>2</priority>
    </replica>
    <db>db_name</db>
    <table>table_name</table>
    <where>id=10</where>
    <invalidate_query>SQL_QUERY</invalidate_query>
</postgresql>
</source>
```

<div id="usage-example">
  ## Exemplo de uso
</div>

<div id="table-in-postgresql">
  ### Tabela no PostgreSQL
</div>

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

<div id="creating-table-in-clickhouse-and-connecting-to--postgresql-table-created-above">
  ### Criando uma tabela no ClickHouse e conectando-a à tabela do PostgreSQL criada acima
</div>

Este exemplo usa o [motor de tabela PostgreSQL](/pt-BR/engines/table-engines/integrations/postgresql.md) para conectar a tabela do ClickHouse à tabela do PostgreSQL e executar instruções SELECT e INSERT no banco de dados PostgreSQL:

```sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-initial-data-from-postgresql-table-into-clickhouse-table-using-a-select-query">
  ### Inserindo dados iniciais de uma tabela PostgreSQL em uma tabela ClickHouse usando uma consulta SELECT
</div>

A [função de tabela PostgreSQL](/pt-BR/sql-reference/table-functions/postgresql.md) copia os dados do PostgreSQL para o ClickHouse. Isso costuma ser usado para melhorar o desempenho das consultas, consultando os dados ou realizando analytics no ClickHouse em vez de no PostgreSQL, e também pode ser usado para migrar dados do PostgreSQL para o ClickHouse. Como vamos copiar os dados do PostgreSQL para o ClickHouse, usaremos um engine de tabela MergeTree no ClickHouse e o chamaremos de postgresql&#95;copy:

```sql
CREATE TABLE default.postgresql_copy
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = MergeTree
ORDER BY (int_id);
```

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-incremental-data-from-postgresql-table-into-clickhouse-table">
  ### Inserindo dados incrementais da tabela PostgreSQL para a tabela ClickHouse
</div>

Se você estiver realizando uma sincronização contínua entre a tabela PostgreSQL e a tabela ClickHouse após o insert inicial, poderá usar uma cláusula WHERE no ClickHouse para inserir apenas os dados adicionados ao PostgreSQL com base em um timestamp ou em um ID de sequência exclusivo.

Isso exigiria acompanhar o ID máximo ou timestamp inserido anteriormente, como no exemplo a seguir:

```sql
SELECT max(`int_id`) AS maxIntID FROM default.postgresql_copy;
```

Em seguida, inserindo valores da tabela do PostgreSQL acima do máximo

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password')
WHERE int_id > (SELECT max(int_id) FROM default.postgresql_copy);
```

<div id="selecting-data-from-the-resulting-clickhouse-table">
  ### Selecionando dados da tabela do ClickHouse resultante
</div>

```sql
SELECT * FROM postgresql_copy WHERE str IN ('test');
```

```text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

<div id="using-non-default-schema">
  ### Usando schema não padrão
</div>

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

**Veja também**

* [A função de tabela `postgresql`](../../../sql-reference/table-functions/postgresql.md)
* [Usando o PostgreSQL como fonte de dicionário](/pt-BR/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [ClickHouse e PostgreSQL — uma combinação feita no paraíso dos dados — parte 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* Blog: [ClickHouse e PostgreSQL — uma combinação feita no paraíso dos dados — parte 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)