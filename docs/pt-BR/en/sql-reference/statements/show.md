---
description: 'Documentação do SHOW'
sidebar_label: 'SHOW'
sidebar_position: 37
slug: /sql-reference/statements/show
title: 'Instruções SHOW'
doc_type: 'reference'
---

:::note

`SHOW CREATE (TABLE|DATABASE|USER)` oculta informações sensíveis, a menos que as seguintes configurações estejam habilitadas:

* [`display_secrets_in_show_and_select`](../../operations/server-configuration-parameters/settings/#display_secrets_in_show_and_select) (configuração do servidor)
* [`format_display_secrets_in_show_and_select` ](../../operations/settings/formats/#format_display_secrets_in_show_and_select) (configuração de formato)

Além disso, o usuário deve ter o privilégio [`displaySecretsInShowAndSelect`](grant.md/#displaysecretsinshowandselect).
:::

<div id="show-create-table--dictionary--view--database">
  ## SHOW CREATE TABLE | DICTIONARY | VIEW | DATABASE
</div>

Estas instruções retornam uma única coluna do tipo String,
que contém a consulta `CREATE` usada para criar o objeto especificado.

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW [CREATE] TABLE | TEMPORARY TABLE | DICTIONARY | VIEW | DATABASE [db.]table|view [INTO OUTFILE filename] [FORMAT format]
```

:::note
Se você usar esta instrução para obter a consulta `CREATE` das tabelas de sistema,
receberá uma consulta *fictícia*, que apenas declara a estrutura da tabela,
mas não pode ser usada para criar uma tabela.
:::

<div id="show-databases">
  ## SHOW DATABASES
</div>

Esta instrução exibe uma lista de todos os bancos de dados.

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW DATABASES [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

É idêntico à consulta:

```sql
SELECT name FROM system.databases [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

<div id="examples">
  ### Exemplos
</div>

Neste exemplo, usamos `SHOW` para obter os nomes dos bancos de dados que contêm a sequência de caracteres &#39;de&#39; em seus nomes:

```sql title="Query"
SHOW DATABASES LIKE '%de%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

Também é possível fazer isso sem diferenciar maiúsculas de minúsculas:

```sql title="Query"
SHOW DATABASES ILIKE '%DE%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

Ou obtenha nomes de bancos de dados que não contenham &#39;de&#39;:

```sql title="Query"
SHOW DATABASES NOT LIKE '%de%'
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ system                         │
│ test                           │
│ tutorial                       │
└────────────────────────────────┘
```

Por fim, podemos obter os nomes de apenas os dois primeiros bancos de dados:

```sql title="Query"
SHOW DATABASES LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ default                        │
└────────────────────────────────┘
```

<div id="see-also">
  ### Veja também
</div>

* [`CREATE DATABASE`](/pt-BR/sql-reference/statements/create/database)

<div id="show-tables">
  ## SHOW TABLES
</div>

A instrução `SHOW TABLES` exibe uma lista de tabelas.

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW [FULL] [TEMPORARY] TABLES [{FROM | IN} <db>] [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Se a cláusula `FROM` não for especificada, a consulta retornará uma lista de tabelas do banco de dados atual.

Esta instrução é idêntica à consulta:

```sql
SELECT name FROM system.tables [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### Exemplos
</div>

Neste exemplo, usamos a instrução `SHOW TABLES` para encontrar todas as tabelas que contêm &#39;user&#39; nos nomes:

```sql title="Query"
SHOW TABLES FROM system LIKE '%user%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

Também podemos fazer isso sem diferenciar maiúsculas de minúsculas:

```sql title="Query"
SHOW TABLES FROM system ILIKE '%USER%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

Ou, para encontrar tabelas cujos nomes n&#39;o contêm a letra &#39;s&#39;:

```sql title="Query"
SHOW TABLES FROM system NOT LIKE '%s%'
```

```text title="Response"
┌─name─────────┐
│ metric_log   │
│ metric_log_0 │
│ metric_log_1 │
└──────────────┘
```

Por fim, podemos obter os nomes apenas das duas primeiras tabelas:

```sql title="Query"
SHOW TABLES FROM system LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ asynchronous_metric_log        │
└────────────────────────────────┘
```

<div id="see-also">
  ### Veja também
</div>

* [`Criar tabelas`](/pt-BR/sql-reference/statements/create/table)
* [`SHOW CREATE TABLE`](#show-create-table--dictionary--view--database)

<div id="show_columns">
  ## SHOW COLUMNS
</div>

A instrução `SHOW COLUMNS` exibe uma lista de colunas.

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW [EXTENDED] [FULL] COLUMNS {FROM | IN} <table> [{FROM | IN} <db>] [{[NOT] {LIKE | ILIKE} '<pattern>' | WHERE <expr>}] [LIMIT <N>] [INTO
OUTFILE <filename>] [FORMAT <format>]
```

O nome do banco de dados e da tabela pode ser especificado de forma abreviada como `<db>.<table>`,
o que significa que `FROM tab FROM db` e `FROM db.tab` são equivalentes.
Se nenhum banco de dados for especificado, a consulta retorna a lista de colunas do banco de dados atual.

Também há duas palavras-chave opcionais: `EXTENDED` e `FULL`. Atualmente, a palavra-chave `EXTENDED` não tem efeito
e existe para compatibilidade com o MySQL. A palavra-chave `FULL` faz com que a saída inclua as colunas `collation`, `comment` e `privilege`.

A instrução `SHOW COLUMNS` produz uma tabela de resultados com a seguinte estrutura:

| Coluna      | Descrição                                                                                                                                         | Tipo               |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `field`     | O nome da coluna                                                                                                                                  | `String`           |
| `type`      | O tipo de dado da coluna. Se a consulta foi feita por meio do MySQL wire protocol, será mostrado o nome de tipo equivalente no MySQL.             | `String`           |
| `null`      | `YES` se o tipo de dado da coluna for Nullable, `NO` caso contrário                                                                               | `String`           |
| `key`       | `PRI` se a coluna fizer parte da chave primária, `SOR` se a coluna fizer parte da chave de ordenação, vazio caso contrário                        | `String`           |
| `default`   | Expressão padrão da coluna se ela for do tipo `ALIAS`, `DEFAULT` ou `MATERIALIZED`; caso contrário, `NULL`.                                       | `Nullable(String)` |
| `extra`     | Informações adicionais, atualmente não usadas                                                                                                     | `String`           |
| `collation` | (somente se a palavra-chave `FULL` tiver sido especificada) Collation da coluna, sempre `NULL`, porque o ClickHouse não tem collations por coluna | `Nullable(String)` |
| `comment`   | (somente se a palavra-chave `FULL` tiver sido especificada) Comentário da coluna                                                                  | `String`           |
| `privilege` | (somente se a palavra-chave `FULL` tiver sido especificada) O privilégio que você tem nesta coluna, atualmente não disponível                     | `String`           |

<div id="examples">
  ### Exemplos
</div>

Neste exemplo, usaremos a instrução `SHOW COLUMNS` para obter informações sobre todas as colunas da tabela &#39;orders&#39;,
que começam com &#39;delivery&#95;&#39;:

```sql title="Query"
SHOW COLUMNS FROM 'orders' LIKE 'delivery_%'
```

```text title="Response"
┌─field───────────┬─type─────┬─null─┬─key─────┬─default─┬─extra─┐
│ delivery_date   │ DateTime │    0 │ PRI SOR │ ᴺᵁᴸᴸ    │       │
│ delivery_status │ Bool     │    0 │         │ ᴺᵁᴸᴸ    │       │
└─────────────────┴──────────┴──────┴─────────┴─────────┴───────┘
```

<div id="see-also">
  ### Veja também
</div>

* [`system.columns`](../../operations/system-tables/columns.md)

<div id="show-dictionaries">
  ## SHOW DICTIONARIES
</div>

A instrução `SHOW DICTIONARIES` exibe uma lista de [Dicionários](./create/dictionary/overview.md).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

Se a cláusula `FROM` não for especificada, a consulta retorna a lista de dicionários do banco de dados atual.

Você pode obter os mesmos resultados da consulta `SHOW DICTIONARIES` da seguinte forma:

```sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### Exemplos
</div>

A consulta a seguir seleciona as duas primeiras linhas da lista de tabelas do banco de dados `system`, cujos nomes contêm `reg`.

```sql title="Query"
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

```text title="Response"
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

<div id="show-index">
  ## SHOW INDEX
</div>

Exibe uma lista dos índices primários e data skipping indexes de uma tabela.

Esta instrução existe principalmente para compatibilidade com o MySQL. As tabelas de sistema [`system.tables`](../../operations/system-tables/tables.md) (para
chaves primárias) e [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md) (para data skipping indices)
fornecem informações equivalentes, mas de uma forma mais própria do ClickHouse.

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW [EXTENDED] {INDEX | INDEXES | INDICES | KEYS } {FROM | IN} <table> [{FROM | IN} <db>] [WHERE <expr>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

O nome do banco de dados e da tabela pode ser especificado na forma abreviada `<db>.<table>`, ou seja, `FROM tab FROM db` e `FROM db.tab` são
equivalentes. Se nenhum banco de dados for especificado, a consulta assume o banco de dados atual.

A palavra-chave opcional `EXTENDED` atualmente não tem efeito e existe para compatibilidade com o MySQL.

A instrução produz uma tabela de resultados com a seguinte estrutura:

| Coluna          | Descrição                                                                                                                                          | Tipo               |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| `table`         | O nome da tabela.                                                                                                                                  | `String`           |
| `non_unique`    | Sempre `1`, pois o ClickHouse não oferece suporte a restrições de unicidade.                                                                       | `UInt8`            |
| `key_name`      | O nome do índice; `PRIMARY` se o índice for um índice de chave primária.                                                                           | `String`           |
| `seq_in_index`  | Para um índice de chave primária, a posição da coluna a partir de `1`. Para um índice de data skipping: sempre `1`.                                | `UInt8`            |
| `column_name`   | Para um índice de chave primária, o nome da coluna. Para um índice de data skipping: `''` (string vazia); consulte o campo &quot;expression&quot;. | `String`           |
| `collation`     | A ordenação da coluna no índice: `A` se crescente, `D` se decrescente, `NULL` se não ordenado.                                                     | `Nullable(String)` |
| `cardinality`   | Uma estimativa da cardinalidade do índice (número de valores únicos no índice). Atualmente, sempre 0.                                              | `UInt64`           |
| `sub_part`      | Sempre `NULL`, porque o ClickHouse não oferece suporte a prefixos de índice como o MySQL.                                                          | `Nullable(String)` |
| `packed`        | Sempre `NULL`, porque o ClickHouse não oferece suporte a índices compactados (como o MySQL).                                                       | `Nullable(String)` |
| `null`          | Atualmente não utilizado                                                                                                                           |                    |
| `index_type`    | O tipo de índice, por exemplo, `PRIMARY`, `MINMAX`, `BLOOM_FILTER` etc.                                                                            | `String`           |
| `comment`       | Informações adicionais sobre o índice; atualmente, sempre `''` (string vazia).                                                                     | `String`           |
| `index_comment` | `''` (string vazia), porque índices no ClickHouse não podem ter um campo `COMMENT` (como no MySQL).                                                | `String`           |
| `visible`       | Se o índice estiver visível para o otimizador, sempre `YES`.                                                                                       | `String`           |
| `expression`    | Para um índice de data skipping, a expressão do índice. Para um índice de chave primária: `''` (string vazia).                                     | `String`           |

<div id="examples">
  ### Exemplos
</div>

Neste exemplo, usamos a instrução `SHOW INDEX` para obter informações sobre todos os índices da tabela &#39;tbl&#39;

```sql title="Query"
SHOW INDEX FROM 'tbl'
```

```text title="Response"
┌─table─┬─non_unique─┬─key_name─┬─seq_in_index─┬─column_name─┬─collation─┬─cardinality─┬─sub_part─┬─packed─┬─null─┬─index_type───┬─comment─┬─index_comment─┬─visible─┬─expression─┐
│ tbl   │          1 │ blf_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ BLOOM_FILTER │         │               │ YES     │ d, b       │
│ tbl   │          1 │ mm1_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ a, c, d    │
│ tbl   │          1 │ mm2_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ c, d, e    │
│ tbl   │          1 │ PRIMARY  │ 1            │ c           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ PRIMARY  │ 2            │ a           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ set_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ SET          │         │               │ YES     │ e          │
└───────┴────────────┴──────────┴──────────────┴─────────────┴───────────┴─────────────┴──────────┴────────┴──────┴──────────────┴─────────┴───────────────┴─────────┴────────────┘
```

<div id="see-also">
  ### Veja também
</div>

* [`system.tables`](../../operations/system-tables/tables.md)
* [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md)

<div id="show-processlist">
  ## SHOW PROCESSLIST
</div>

Exibe o conteúdo da tabela [`system.processes`](/pt-BR/operations/system-tables/processes), que contém uma lista das consultas que estão sendo processadas no momento, excluindo as consultas `SHOW PROCESSLIST`.

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

A consulta `SELECT * FROM system.processes` retorna dados sobre todas as consultas em execução no momento.

:::tip
Execute no Console:

```bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

:::

<div id="show-grants">
  ## SHOW GRANTS
</div>

A instrução `SHOW GRANTS` exibe os privilégios de um usuário.

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW GRANTS [FOR user1 [, user2 ...]] [WITH IMPLICIT] [FINAL]
```

Se o usuário não for especificado, a consulta retorna os privilégios do usuário atual.

O modificador `WITH IMPLICIT` permite mostrar os privilégios implícitos (por exemplo, `GRANT SELECT ON system.one`)

O modificador `FINAL` combina todos os privilégios do usuário e dos roles concedidos a ele (com herança)

<div id="show-create-user">
  ## SHOW CREATE USER
</div>

A instrução `SHOW CREATE USER` mostra os parâmetros usados na [criação do usuário](../../sql-reference/statements/create/user.md).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW CREATE USER [name1 [, name2 ...] | CURRENT_USER]
```

<div id="show-create-role">
  ## SHOW CREATE ROLE
</div>

A instrução `SHOW CREATE ROLE` mostra os parâmetros usados na [criação da role](../../sql-reference/statements/create/role.md).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW CREATE ROLE name1 [, name2 ...]
```

<div id="show-create-row-policy">
  ## SHOW CREATE ROW POLICY
</div>

A instrução `SHOW CREATE ROW POLICY` mostra os parâmetros usados na [criação da política de linha](../../sql-reference/statements/create/row-policy.md).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW CREATE [ROW] POLICY name ON [database1.]table1 [, [database2.]table2 ...]
```

<div id="show-create-quota">
  ## SHOW CREATE QUOTA
</div>

A instrução `SHOW CREATE QUOTA` mostra os parâmetros usados na [criação da QUOTA](../../sql-reference/statements/create/quota.md).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW CREATE QUOTA [name1 [, name2 ...] | CURRENT]
```

<div id="show-create-settings-profile">
  ## SHOW CREATE SETTINGS PROFILE
</div>

A instrução `SHOW CREATE SETTINGS PROFILE` mostra os parâmetros usados na [criação do perfil de configurações](../../sql-reference/statements/create/settings-profile.md).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW CREATE [SETTINGS] PROFILE name1 [, name2 ...]
```

<div id="show-users">
  ## SHOW USERS
</div>

A instrução `SHOW USERS` retorna uma lista com os nomes das [contas de usuário](../../guides/sre/user-management/index.md#user-account-management).
Para ver os parâmetros das contas de usuário, consulte a tabela do sistema [`system.users`](/pt-BR/operations/system-tables/users).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW USERS
```

<div id="show-roles">
  ## SHOW ROLES
</div>

A instrução `SHOW ROLES` retorna uma lista de [roles](../../guides/sre/user-management/index.md#role-management).
Para ver outros parâmetros,
consulte as tabelas de sistema [`system.roles`](/pt-BR/operations/system-tables/roles) e [`system.role_grants`](/pt-BR/operations/system-tables/role_grants).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW [CURRENT|ENABLED] ROLES
```

<div id="show-profiles">
  ## SHOW PROFILES
</div>

A instrução `SHOW PROFILES` retorna uma lista de [perfis de configurações](../../guides/sre/user-management/index.md#settings-profiles-management).
Para ver os parâmetros das contas de usuário, consulte a tabela do sistema [`settings_profiles`](/pt-BR/operations/system-tables/settings_profiles).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW [SETTINGS] PROFILES
```

<div id="show-policies">
  ## SHOW POLICIES
</div>

A instrução `SHOW POLICIES` retorna uma lista de [políticas de linha](../../guides/sre/user-management/index.md#row-policy-management) da tabela especificada.
Para ver os parâmetros das contas de usuário, consulte a tabela de sistema [`system.row_policies`](/pt-BR/operations/system-tables/row_policies).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW [ROW] POLICIES [ON [db.]table]
```

<div id="show-quotas">
  ## SHOW QUOTAS
</div>

A instrução `SHOW QUOTAS` retorna uma lista de [cotas](../../guides/sre/user-management/index.md#quotas-management).
Para ver os parâmetros das cotas, consulte a tabela de sistema [`system.quotas`](/pt-BR/operations/system-tables/quotas).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW QUOTAS
```

<div id="show-quota">
  ## SHOW QUOTA
</div>

A instrução `SHOW QUOTA` retorna o consumo de uma [quota](../../operations/quotas.md) de todos os usuários ou do usuário atual.
Para ver outros parâmetros, consulte as tabelas do sistema [`system.quotas_usage`](/pt-BR/operations/system-tables/quotas_usage) e [`system.quota_usage`](/pt-BR/operations/system-tables/quota_usage).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW [CURRENT] QUOTA
```

<div id="show-access">
  ## SHOW ACCESS
</div>

A instrução `SHOW ACCESS` mostra todos os [usuários](../../guides/sre/user-management/index.md#user-account-management), [roles](../../guides/sre/user-management/index.md#role-management), [perfis](../../guides/sre/user-management/index.md#settings-profiles-management) etc. e todos os [privilégios](../../sql-reference/statements/grant.md#privileges) concedidos a eles.

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW ACCESS
```

<div id="show-clusters">
  ## SHOW CLUSTER(S)
</div>

A instrução `SHOW CLUSTER(S)` retorna uma lista de clusters.
Todos os clusters disponíveis são listados na tabela [`system.clusters`](../../operations/system-tables/clusters.md).

:::note
A consulta `SHOW CLUSTER name` exibe `cluster`, `shard_num`, `replica_num`, `host_name`, `host_address` e `port` da tabela `system.clusters` para o nome de cluster especificado.
:::

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW CLUSTER '<name>'
SHOW CLUSTERS [[NOT] LIKE|ILIKE '<pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### Exemplos
</div>

```sql title="Query"
SHOW CLUSTERS;
```

```text title="Response"
┌─cluster──────────────────────────────────────┐
│ test_cluster_two_shards                      │
│ test_cluster_two_shards_internal_replication │
│ test_cluster_two_shards_localhost            │
│ test_shard_localhost                         │
│ test_shard_localhost_secure                  │
│ test_unavailable_shard                       │
└──────────────────────────────────────────────┘
```

```sql title="Query"
SHOW CLUSTERS LIKE 'test%' LIMIT 1;
```

```text title="Response"
┌─cluster─────────────────┐
│ test_cluster_two_shards │
└─────────────────────────┘
```

```sql title="Query"
SHOW CLUSTER 'test_shard_localhost' FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
cluster:                 test_shard_localhost
shard_num:               1
replica_num:             1
host_name:               localhost
host_address:            127.0.0.1
port:                    9000
```

<div id="show-settings">
  ## SHOW SETTINGS
</div>

A instrução `SHOW SETTINGS` retorna a lista de configurações do sistema e seus valores.
Ela seleciona dados da tabela [`system.settings`](../../operations/system-tables/settings.md).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW [CHANGED] SETTINGS LIKE|ILIKE <name>
```

<div id="clauses">
  ### Cláusulas
</div>

`LIKE|ILIKE` permite especificar um padrão de correspondência para o nome da configuração. Ele pode conter globs como `%` ou `_`. A cláusula `LIKE` diferencia maiúsculas de minúsculas; `ILIKE`, não.

Quando a cláusula `CHANGED` é usada, a consulta retorna apenas configurações alteradas em relação aos valores padrão.

<div id="examples">
  ### Exemplos
</div>

Consulta com a cláusula `LIKE`:

```sql title="Query"
SHOW SETTINGS LIKE 'send_timeout';
```

```text title="Response"
┌─name─────────┬─type────┬─value─┐
│ send_timeout │ Seconds │ 300   │
└──────────────┴─────────┴───────┘
```

Consulta usando a cláusula `ILIKE`:

```sql title="Query"
SHOW SETTINGS ILIKE '%CONNECT_timeout%'
```

```text title="Response"
┌─name────────────────────────────────────┬─type─────────┬─value─┐
│ connect_timeout                         │ Seconds      │ 10    │
│ connect_timeout_with_failover_ms        │ Milliseconds │ 50    │
│ connect_timeout_with_failover_secure_ms │ Milliseconds │ 100   │
└─────────────────────────────────────────┴──────────────┴───────┘
```

Consulta com a cláusula `CHANGED`:

```sql title="Query"
SHOW CHANGED SETTINGS ILIKE '%MEMORY%'
```

```text title="Response"
┌─name─────────────┬─type───┬─value───────┐
│ max_memory_usage │ UInt64 │ 10000000000 │
└──────────────────┴────────┴─────────────┘
```

<div id="show-setting">
  ## SHOW SETTING
</div>

A instrução `SHOW SETTING` exibe o valor da configuração para o nome de configuração especificado.

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW SETTING <name>
```

<div id="see-also">
  ### Veja também
</div>

* tabela [`system.settings`](../../operations/system-tables/settings.md)

<div id="show-filesystem-caches">
  ## SHOW FILESYSTEM CACHES
</div>

<div id="examples">
  ### Exemplos
</div>

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

<div id="see-also">
  ### Veja também
</div>

* tabela [`system.settings`](../../operations/system-tables/settings.md)

<div id="show-engines">
  ## SHOW ENGINES
</div>

A instrução `SHOW ENGINES` exibe o conteúdo da tabela [`system.table_engines`](../../operations/system-tables/table_engines.md),
que contém a descrição dos motores de tabela suportados pelo servidor e informações sobre o suporte a recursos.

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW ENGINES [INTO OUTFILE filename] [FORMAT format]
```

<div id="see-also">
  ### Veja também
</div>

* [system.table&#95;engines](../../operations/system-tables/table_engines.md) tabela

<div id="show-functions">
  ## SHOW FUNCTIONS
</div>

A instrução `SHOW FUNCTIONS` exibe o conteúdo da tabela [`system.functions`](../../operations/system-tables/functions.md).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW FUNCTIONS [LIKE | ILIKE '<pattern>']
```

Se qualquer uma das cláusulas `LIKE` ou `ILIKE` for especificada, a consulta retornará uma lista de funções de sistema cujos nomes correspondam ao `<pattern>` fornecido.

<div id="see-also">
  ### Veja também
</div>

* tabela [`system.functions`](../../operations/system-tables/functions.md)

<div id="show-merges">
  ## SHOW MERGES
</div>

A instrução `SHOW MERGES` retorna uma lista de merges.
Todos os merges são listados na tabela [`system.merges`](../../operations/system-tables/merges.md):

| Coluna              | Descrição                                                 |
| ------------------- | --------------------------------------------------------- |
| `table`             | Nome da tabela.                                           |
| `database`          | Nome do banco de dados ao qual a tabela pertence.         |
| `estimate_complete` | Tempo estimado para conclusão (em segundos).              |
| `elapsed`           | Tempo decorrido (em segundos) desde o início do merge.    |
| `progress`          | Percentual de trabalho concluído (de 0 a 100%).           |
| `is_mutation`       | 1 se este processo for uma mutação de uma parte.          |
| `size_compressed`   | Tamanho total dos dados comprimidos das partes mescladas. |
| `memory_usage`      | Consumo de memória do processo de merge.                  |

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW MERGES [[NOT] LIKE|ILIKE '<table_name_pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### Exemplos
</div>

```sql title="Query"
SHOW MERGES;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

```sql title="Query"
SHOW MERGES LIKE 'your_t%' LIMIT 1;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

<div id="show-create-masking-policy">
  ## SHOW CREATE MASKING POLICY
</div>

A instrução `SHOW CREATE MASKING POLICY` mostra os parâmetros usados na [criação da política de mascaramento](../../sql-reference/statements/create/masking-policy.md).

<div id="syntax">
  ### Sintaxe
</div>

```sql title="Syntax"
SHOW CREATE MASKING POLICY name ON [database.]table
```