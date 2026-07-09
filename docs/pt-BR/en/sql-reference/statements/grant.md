---
description: 'Documentação da instrução GRANT'
sidebar_label: 'GRANT'
sidebar_position: 38
slug: /sql-reference/statements/grant
title: 'Instrução GRANT'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="grant-statement">
  # Instrução GRANT
</div>

* Concede [privilégios](#privileges) a contas de usuário do ClickHouse ou a funções.
* Atribui funções a contas de usuário ou a outras funções.

Para revogar privilégios, use a instrução [REVOKE](../../sql-reference/statements/revoke.md). Você também pode listar os privilégios concedidos com a instrução [SHOW GRANTS](../../sql-reference/statements/show.md#show-grants).

<div id="granting-privilege-syntax">
  ## Sintaxe para conceder privilégios
</div>

```sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — Tipo de privilégio.
* `role` — Função de usuário do ClickHouse.
* `user` — Conta de usuário do ClickHouse.

A cláusula `WITH GRANT OPTION` concede a `user` ou `role` permissão para executar a consulta `GRANT`. Os usuários podem conceder privilégios com o mesmo escopo que possuem ou com escopo menor.
A cláusula `WITH REPLACE OPTION` substitui privilégios antigos por novos privilégios para `user` ou `role`; caso não seja especificada, ela acrescenta privilégios.

<div id="assigning-role-syntax">
  ## Sintaxe para atribuir função
</div>

```sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION] [WITH REPLACE OPTION]
```

* `role` — função de usuário do ClickHouse.
* `user` — conta de usuário do ClickHouse.

A cláusula `WITH ADMIN OPTION` concede o privilégio [ADMIN OPTION](#admin-option) a `user` ou `função`.
A cláusula `WITH REPLACE OPTION` substitui as funções antigas pela nova função para `user` ou `função`; se não for especificada, ela adiciona as funções.

<div id="grant-current-grants-syntax">
  ## Sintaxe de GRANT CURRENT GRANTS
</div>

```sql
GRANT CURRENT GRANTS{(privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}) | ON {db.table|db.*|*.*|table|*}} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — Tipo de privilégio.
* `role` — Função de usuário do ClickHouse.
* `user` — Conta de usuário do ClickHouse.

O uso da instrução `CURRENT GRANTS` permite conceder todos os privilégios especificados ao usuário ou à função especificados.
Se nenhum privilégio for especificado, o usuário ou a função especificados receberão todos os privilégios disponíveis para `CURRENT_USER`.

<div id="usage">
  ## Uso
</div>

Para usar `GRANT`, sua conta deve ter o privilégio `GRANT OPTION`. Você pode conceder privilégios apenas dentro do escopo dos privilégios da sua conta.

Por exemplo, o administrador concedeu privilégios à conta `john` com a consulta:

```sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

Isso significa que `john` tem permissão para executar:

* `SELECT x,y FROM db.table`.
* `SELECT x FROM db.table`.
* `SELECT y FROM db.table`.

`john` não pode executar `SELECT z FROM db.table`. `SELECT * FROM db.table` também não está disponível. Ao processar essa consulta, o ClickHouse não retorna nenhum dado, nem mesmo `x` e `y`. A única exceção é se a tabela contiver apenas as colunas `x` e `y`. Nesse caso, o ClickHouse retorna todos os dados.

Além disso, `john` tem o privilégio `GRANT OPTION`, portanto pode conceder a outros usuários privilégios com o mesmo escopo ou com escopo menor.

O acesso ao banco de dados `system` é sempre permitido (já que esse banco de dados é usado para processar consultas).

:::note
Embora existam muitas tabelas do sistema às quais novos usuários podem acessar por padrão, eles talvez não consigam acessar todas as tabelas do sistema por padrão sem concessões.
Além disso, o acesso a determinadas tabelas do sistema, como `system.zookeeper`, é restrito para usuários do Cloud por motivos de segurança.
:::

Você pode conceder vários privilégios a várias contas em uma única consulta. A consulta `GRANT SELECT, INSERT ON *.* TO john, robin` permite que as contas `john` e `robin` executem as consultas `INSERT` e `SELECT` em todas as tabelas de todos os bancos de dados no servidor.

<div id="wildcard-grants">
  ## Privilégios com curinga
</div>

Ao especificar privilégios, você pode usar um asterisco (`*`) no lugar do nome de uma tabela ou de um banco de dados. Por exemplo, a consulta `GRANT SELECT ON db.* TO john` permite que `john` execute a consulta `SELECT` em todas as tabelas do banco de dados `db`.
Além disso, você pode omitir o nome do banco de dados. Nesse caso, os privilégios são concedidos para o banco de dados atual.
Por exemplo, `GRANT SELECT ON * TO john` concede o privilégio em todas as tabelas do banco de dados atual, e `GRANT SELECT ON mytable TO john` concede o privilégio na tabela `mytable` do banco de dados atual.

:::note
A funcionalidade descrita abaixo está disponível a partir da versão 24.10 do ClickHouse.
:::

Você também pode colocar asteriscos no final do nome de uma tabela ou de um banco de dados. Essa funcionalidade permite conceder privilégios sobre um prefixo abstrato do caminho da tabela.
Exemplo: `GRANT SELECT ON db.my_tables* TO john`. Essa consulta permite que `john` execute a consulta `SELECT` em todas as tabelas do banco de dados `db` com o prefixo `my_tables*`.

Mais exemplos:

`GRANT SELECT ON db.my_tables* TO john`

```sql
SELECT * FROM db.my_tables -- granted
SELECT * FROM db.my_tables_0 -- granted
SELECT * FROM db.my_tables_1 -- granted

SELECT * FROM db.other_table -- not_granted
SELECT * FROM db2.my_tables -- not_granted
```

`GRANT SELECT ON db*.* TO john`

```sql
SELECT * FROM db.my_tables -- granted
SELECT * FROM db.my_tables_0 -- granted
SELECT * FROM db.my_tables_1 -- granted
SELECT * FROM db.other_table -- granted
SELECT * FROM db2.my_tables -- granted
```

Todas as tabelas recém-criadas dentro dos caminhos concedidos herdarão automaticamente todos os privilégios de seus respectivos caminhos pai.
Por exemplo, se você executar a consulta `GRANT SELECT ON db.* TO john` e depois criar uma nova tabela `db.new_table`, o usuário `john` poderá executar a consulta `SELECT * FROM db.new_table`.

Você pode especificar o asterisco **apenas** para os prefixos:

```sql
GRANT SELECT ON db.* TO john -- correct
GRANT SELECT ON db*.* TO john -- correct

GRANT SELECT ON *.my_table TO john -- wrong
GRANT SELECT ON foo*bar TO john -- wrong
GRANT SELECT ON *suffix TO john -- wrong
GRANT SELECT(foo) ON db.table* TO john -- wrong
```

<div id="privileges">
  ## Privilégios
</div>

Um privilégio é uma permissão concedida a um usuário para executar determinados tipos de consultas.

Os privilégios têm uma estrutura hierárquica, e o conjunto de consultas permitidas depende do escopo do privilégio.

A hierarquia de privilégios no ClickHouse é mostrada abaixo:

* [`ALL`](#all)
  * [`GERENCIAMENTO DE ACESSO`](#access-management)
    * `ALLOW SQL SECURITY NONE`
    * `ALTER QUOTA`
    * `ALTER ROLE`
    * `ALTER ROW POLICY`
    * `ALTER SETTINGS PROFILE`
    * `ALTER USER`
    * `CREATE QUOTA`
    * `CREATE ROLE`
    * `CREATE ROW POLICY`
    * `CREATE SETTINGS PROFILE`
    * `CREATE USER`
    * `DROP QUOTA`
    * `DROP ROLE`
    * `DROP ROW POLICY`
    * `DROP SETTINGS PROFILE`
    * `DROP USER`
    * `ROLE ADMIN`
    * `SHOW ACCESS`
      * `SHOW QUOTAS`
      * `SHOW ROLES`
      * `SHOW ROW POLICIES`
      * `SHOW SETTINGS PROFILES`
      * `SHOW USERS`
  * [`ALTER`](#alter)
    * `ALTER DATABASE`
      * `ALTER DATABASE SETTINGS`
    * `ALTER TABLE`
      * `ALTER COLUMN`
        * `ALTER ADD COLUMN`
        * `ALTER CLEAR COLUMN`
        * `ALTER COMMENT COLUMN`
        * `ALTER DROP COLUMN`
        * `ALTER MATERIALIZE COLUMN`
        * `ALTER MODIFY COLUMN`
        * `ALTER RENAME COLUMN`
      * `ALTER CONSTRAINT`
        * `ALTER ADD CONSTRAINT`
        * `ALTER DROP CONSTRAINT`
        * `ALTER MODIFY CONSTRAINT`
      * `ALTER DELETE`
      * `ALTER FETCH PARTITION`
      * `ALTER FREEZE PARTITION`
      * `ALTER INDEX`
        * `ALTER ADD INDEX`
        * `ALTER CLEAR INDEX`
        * `ALTER DROP INDEX`
        * `ALTER MATERIALIZE INDEX`
        * `ALTER ORDER BY`
        * `ALTER SAMPLE BY`
      * `ALTER MATERIALIZE TTL`
      * `ALTER MODIFY COMMENT`
      * `ALTER MOVE PARTITION`
      * `ALTER PROJECTION`
      * `ALTER SETTINGS`
      * `ALTER STATISTICS`
        * `ALTER ADD STATISTICS`
        * `ALTER DROP STATISTICS`
        * `ALTER MATERIALIZE STATISTICS`
        * `ALTER MODIFY STATISTICS`
      * `ALTER TTL`
      * `ALTER UPDATE`
      * `ALTER TABLE EXECUTE`
    * `ALTER VIEW`
      * `ALTER VIEW MODIFY QUERY`
      * `ALTER VIEW REFRESH`
      * `ALTER VIEW MODIFY SQL SECURITY`
  * [`BACKUP`](#backup)
  * [`CLUSTER`](#cluster)
  * [`CREATE`](#create)
    * `CREATE ARBITRARY TEMPORARY TABLE`
      * `CREATE TEMPORARY TABLE`
    * `CREATE DATABASE`
    * `CREATE DICTIONARY`
    * `CREATE FUNCTION`
    * `CREATE RESOURCE`
    * `CREATE TABLE`
    * `CREATE VIEW`
    * `CREATE WORKLOAD`
  * [`dictGet`](#dictget)
  * [`displaySecretsInShowAndSelect`](#displaysecretsinshowandselect)
  * [`DROP`](#drop)
    * `DROP DATABASE`
    * `DROP DICTIONARY`
    * `DROP FUNCTION`
    * `DROP RESOURCE`
    * `DROP TABLE`
    * `DROP VIEW`
    * `DROP WORKLOAD`
  * [`INSERT`](#insert)
  * [`INTROSPECTION`](#introspection)
    * `addressToLine`
    * `addressToLineWithInlines`
    * `addressToSymbol`
    * `demangle`
  * `KILL QUERY`
  * `KILL TRANSACTION`
  * `MOVE PARTITION BETWEEN SHARDS`
  * [`NAMED COLLECTION ADMIN`](#named-collection-admin)
    * `ALTER NAMED COLLECTION`
    * `CREATE NAMED COLLECTION`
    * `DROP NAMED COLLECTION`
    * `NAMED COLLECTION`
    * `SHOW NAMED COLLECTIONS`
    * `SHOW NAMED COLLECTIONS SECRETS`
  * [`OPTIMIZE`](#optimize)
  * [`SELECT`](#select)
  * [`SET DEFINER`](/pt-BR/sql-reference/statements/create/view#sql_security)
  * [`SHOW`](#show)
    * `SHOW COLUMNS`
    * `SHOW DATABASES`
    * `SHOW DICTIONARIES`
    * `SHOW TABLES`
  * `SHOW FILESYSTEM CACHES`
  * [`SOURCES`](#sources)
    * `AZURE`
    * `FILE`
    * `HDFS`
    * `HIVE`
    * `JDBC`
    * `KAFKA`
    * `MONGO`
    * `MYSQL`
    * `NATS`
    * `ODBC`
    * `POSTGRES`
    * `RABBITMQ`
    * `REDIS`
    * `REMOTE`
    * `S3`
    * `SQLITE`
    * `URL`
  * [`SYSTEM`](#system)
    * `SYSTEM CLEANUP`
    * `SYSTEM DROP CACHE`
      * `SYSTEM DROP COMPILED EXPRESSION CACHE`
      * `SYSTEM DROP CONNECTIONS CACHE`
      * `SYSTEM DROP DISTRIBUTED CACHE`
      * `SYSTEM DROP DNS CACHE`
      * `SYSTEM DROP FILESYSTEM CACHE`
      * `SYSTEM DROP FORMAT SCHEMA CACHE`
      * `SYSTEM DROP MARK CACHE`
      * `SYSTEM DROP MMAP CACHE`
      * `SYSTEM DROP PAGE CACHE`
      * `SYSTEM DROP PRIMARY INDEX CACHE`
      * `SYSTEM DROP QUERY CACHE`
      * `SYSTEM DROP S3 CLIENT CACHE`
      * `SYSTEM DROP SCHEMA CACHE`
      * `SYSTEM DROP UNCOMPRESSED CACHE`
    * `SYSTEM DROP PRIMARY INDEX CACHE`
    * `SYSTEM DROP REPLICA`
    * `SYSTEM FAILPOINT`
    * `SYSTEM FETCHES`
    * `SYSTEM FLUSH`
      * `SYSTEM FLUSH ASYNC INSERT QUEUE`
      * `SYSTEM FLUSH LOGS`
    * `SYSTEM JEMALLOC`
    * `SYSTEM KILL QUERY`
    * `SYSTEM KILL TRANSACTION`
    * `SYSTEM LISTEN`
    * `SYSTEM LOAD PRIMARY KEY`
    * `SYSTEM MERGES`
    * `SYSTEM MOVES`
    * `SYSTEM PULLING REPLICATION LOG`
    * `SYSTEM REDUCE BLOCKING PARTS`
    * `SYSTEM REPLICATION QUEUES`
    * `SYSTEM REPLICA READINESS`
    * `SYSTEM RESET DDL WORKER`
    * `SYSTEM RESTART DISK`
    * `SYSTEM RESTART REPLICA`
    * `SYSTEM RESTORE REPLICA`
    * `SYSTEM RELOAD`
      * `SYSTEM RELOAD ASYNCHRONOUS METRICS`
      * `SYSTEM RELOAD CONFIG`
        * `SYSTEM RELOAD DICTIONARY`
        * `SYSTEM RELOAD EMBEDDED DICTIONARIES`
        * `SYSTEM RELOAD FUNCTION`
        * `SYSTEM RELOAD MODEL`
        * `SYSTEM RELOAD USERS`
    * `SYSTEM SENDS`
      * `SYSTEM DISTRIBUTED SENDS`
      * `SYSTEM REPLICATED SENDS`
    * `SYSTEM SHUTDOWN`
    * `SYSTEM SYNC DATABASE REPLICA`
    * `SYSTEM SYNC FILE CACHE`
    * `SYSTEM SYNC FILESYSTEM CACHE`
    * `SYSTEM SYNC REPLICA`
    * `SYSTEM SYNC TRANSACTION LOG`
    * `SYSTEM THREAD FUZZER`
    * `SYSTEM TTL MERGES`
    * `SYSTEM UNFREEZE`
    * `SYSTEM UNLOAD PRIMARY KEY`
    * `SYSTEM VIEWS`
    * `SYSTEM VIRTUAL PARTS UPDATE`
    * `SYSTEM WAIT LOADING PARTS`
  * [`TABLE ENGINE`](#table-engine)
  * [`TRUNCATE`](#truncate)
  * `UNDROP TABLE`
* [`NONE`](#none)

Exemplos de como essa hierarquia é tratada:

* O privilégio `ALTER` inclui todos os outros privilégios `ALTER*`.
* `ALTER CONSTRAINT` inclui os privilégios `ALTER ADD CONSTRAINT`, `ALTER DROP CONSTRAINT` e `ALTER MODIFY CONSTRAINT`.

Os privilégios são aplicados em diferentes níveis. Saber em que nível um privilégio se aplica indica a sintaxe disponível para ele.

Níveis (do mais baixo para o mais alto):

* `COLUMN` — O privilégio pode ser concedido para coluna, tabela, banco de dados ou globalmente.
* `TABLE` — O privilégio pode ser concedido para tabela, banco de dados ou globalmente.
* `VIEW` — O privilégio pode ser concedido para view, banco de dados ou globalmente.
* `DICTIONARY` — O privilégio pode ser concedido para dicionário, banco de dados ou globalmente.
* `DATABASE` — O privilégio pode ser concedido para banco de dados ou globalmente.
* `GLOBAL` — O privilégio pode ser concedido apenas globalmente.
* `GROUP` — Agrupa privilégios de diferentes níveis. Quando um privilégio de nível `GROUP` é concedido, só são concedidos os privilégios do grupo que correspondem à sintaxe usada.

Exemplos de sintaxe permitida:

* `GRANT SELECT(x) ON db.table TO user`
* `GRANT SELECT ON db.* TO user`

Exemplos de sintaxe não permitida:

* `GRANT CREATE USER(x) ON db.table TO user`
* `GRANT CREATE USER ON db.* TO user`

O privilégio especial [ALL](#all) concede todos os privilégios a uma conta de usuário ou a uma função.

Por padrão, uma conta de usuário ou uma função não tem privilégios.

Se um usuário ou uma função não tiver privilégios, isso será exibido como o privilégio [NONE](#none).

Algumas consultas, por sua implementação, exigem um conjunto de privilégios. Por exemplo, para executar a instrução [RENAME](../../sql-reference/statements/optimize.md), você precisa dos seguintes privilégios: `SELECT`, `CREATE TABLE`, `INSERT` e `DROP TABLE`.

<div id="select">
  ### SELECT
</div>

Permite executar consultas [SELECT](../../sql-reference/statements/select/index.md).

Nível de privilégio: `COLUMN`.

**Descrição**

O usuário com esse privilégio pode executar consultas `SELECT` em uma lista específica de colunas na tabela e no banco de dados especificados. Se o usuário incluir outras colunas além das especificadas, a consulta não retornará dados.

Considere o seguinte privilégio:

```sql
GRANT SELECT(x,y) ON db.table TO john
```

Esse privilégio permite que `john` execute qualquer consulta `SELECT` que envolva dados das colunas `x` e/ou `y` em `db.table`, por exemplo, `SELECT x FROM db.table`. `john` não pode executar `SELECT z FROM db.table`. `SELECT * FROM db.table` também não fica disponível. Ao processar essa consulta, o ClickHouse não retorna dado algum, nem mesmo `x` e `y`. A única exceção é quando uma tabela contém apenas as colunas `x` e `y`; nesse caso, o ClickHouse retorna todos os dados.

<div id="insert">
  ### INSERT
</div>

Permite executar consultas [INSERT](../../sql-reference/statements/insert-into.md).

Nível de privilégio: `COLUMN`.

**Descrição**

O usuário que recebeu esse privilégio pode executar consultas `INSERT` em uma lista específica de colunas na tabela e no banco de dados especificados. Se o usuário incluir colunas diferentes das especificadas, a consulta não inserirá nenhum dado.

**Exemplo**

```sql
GRANT INSERT(x,y) ON db.table TO john
```

O privilégio concedido permite que `john` insira dados nas colunas `x` e/ou `y` da tabela `db.table`.

<div id="alter">
  ### ALTER
</div>

Permite executar consultas [ALTER](../../sql-reference/statements/alter/index.md) de acordo com a seguinte hierarquia de privilégios:

* `ALTER`. Nível: `COLUMN`.
  * `ALTER TABLE`. Nível: `GROUP`
  * `ALTER UPDATE`. Nível: `COLUMN`. Aliases: `UPDATE`
  * `ALTER DELETE`. Nível: `COLUMN`. Aliases: `DELETE`
  * `ALTER COLUMN`. Nível: `GROUP`
  * `ALTER ADD COLUMN`. Nível: `COLUMN`. Aliases: `ADD COLUMN`
  * `ALTER DROP COLUMN`. Nível: `COLUMN`. Aliases: `DROP COLUMN`
  * `ALTER MODIFY COLUMN`. Nível: `COLUMN`. Aliases: `MODIFY COLUMN`
  * `ALTER COMMENT COLUMN`. Nível: `COLUMN`. Aliases: `COMMENT COLUMN`
  * `ALTER CLEAR COLUMN`. Nível: `COLUMN`. Aliases: `CLEAR COLUMN`
  * `ALTER RENAME COLUMN`. Nível: `COLUMN`. Aliases: `RENAME COLUMN`
  * `ALTER INDEX`. Nível: `GROUP`. Aliases: `INDEX`
  * `ALTER ORDER BY`. Nível: `TABLE`. Aliases: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
  * `ALTER SAMPLE BY`. Nível: `TABLE`. Aliases: `ALTER MODIFY SAMPLE BY`, `MODIFY SAMPLE BY`
  * `ALTER ADD INDEX`. Nível: `TABLE`. Aliases: `ADD INDEX`
  * `ALTER DROP INDEX`. Nível: `TABLE`. Aliases: `DROP INDEX`
  * `ALTER MATERIALIZE INDEX`. Nível: `TABLE`. Aliases: `MATERIALIZE INDEX`
  * `ALTER CLEAR INDEX`. Nível: `TABLE`. Aliases: `CLEAR INDEX`
  * `ALTER CONSTRAINT`. Nível: `GROUP`. Aliases: `CONSTRAINT`
  * `ALTER ADD CONSTRAINT`. Nível: `TABLE`. Aliases: `ADD CONSTRAINT`
  * `ALTER DROP CONSTRAINT`. Nível: `TABLE`. Aliases: `DROP CONSTRAINT`
  * `ALTER MODIFY CONSTRAINT`. Nível: `TABLE`. Aliases: `MODIFY CONSTRAINT`
  * `ALTER TTL`. Nível: `TABLE`. Aliases: `ALTER MODIFY TTL`, `MODIFY TTL`
  * `ALTER MATERIALIZE TTL`. Nível: `TABLE`. Aliases: `MATERIALIZE TTL`
  * `ALTER SETTINGS`. Nível: `TABLE`. Aliases: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
  * `ALTER MOVE PARTITION`. Nível: `TABLE`. Aliases: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
  * `ALTER FETCH PARTITION`. Nível: `TABLE`. Aliases: `ALTER FETCH PART`, `FETCH PARTITION`, `FETCH PART`
  * `ALTER FREEZE PARTITION`. Nível: `TABLE`. Aliases: `FREEZE PARTITION`
  * `ALTER EXECUTE`. Nível: `TABLE`. Aliases: `ALTER TABLE EXECUTE`
  * `ALTER VIEW`. Nível: `GROUP`
  * `ALTER VIEW REFRESH`. Nível: `VIEW`. Aliases: `REFRESH VIEW`
  * `ALTER VIEW MODIFY QUERY`. Nível: `VIEW`. Aliases: `ALTER TABLE MODIFY QUERY`
  * `ALTER VIEW MODIFY SQL SECURITY`. Nível: `VIEW`. Aliases: `ALTER TABLE MODIFY SQL SECURITY`

Exemplos de como essa hierarquia é aplicada:

* O privilégio `ALTER` inclui todos os outros privilégios `ALTER*`.
* `ALTER CONSTRAINT` inclui os privilégios `ALTER ADD CONSTRAINT`, `ALTER DROP CONSTRAINT` e `ALTER MODIFY CONSTRAINT`.

**Notas**

* O privilégio `MODIFY SETTING` permite modificar as configurações do mecanismo da tabela. Não afeta configurações nem parâmetros de configuração do servidor.
* A operação `ATTACH` requer o privilégio [CREATE](#create).
* A operação `DETACH` requer o privilégio [DROP](#drop).
* Para interromper uma mutação com a consulta [KILL MUTATION](../../sql-reference/statements/kill.md#kill-mutation), você precisa ter o privilégio necessário para iniciar essa mutação. Por exemplo, se quiser interromper a consulta `ALTER UPDATE`, precisará do privilégio `ALTER UPDATE`, `ALTER TABLE` ou `ALTER`.

<div id="backup">
  ### BACKUP
</div>

Permite executar [`BACKUP`] em consultas. Para mais informações sobre backups, consulte [&quot;Backup e restauração&quot;](/pt-BR/operations/backup/overview).

<div id="create">
  ### CREATE
</div>

Permite executar instruções DDL [CREATE](../../sql-reference/statements/create/index.md) e [ATTACH](../../sql-reference/statements/attach.md) de acordo com a seguinte hierarquia de privilégios:

* `CREATE`. Nível: `GROUP`
  * `CREATE DATABASE`. Nível: `DATABASE`
  * `CREATE TABLE`. Nível: `TABLE`
    * `CREATE ARBITRARY TEMPORARY TABLE`. Nível: `GLOBAL`
      * `CREATE TEMPORARY TABLE`. Nível: `GLOBAL`
  * `CREATE VIEW`. Nível: `VIEW`
  * `CREATE DICTIONARY`. Nível: `DICTIONARY`

**Observações**

* Para excluir a tabela criada, o usuário precisa de [DROP](#drop).

<div id="cluster">
  ### CLUSTER
</div>

Permite executar consultas `ON CLUSTER`.

```sql title="Syntax"
GRANT CLUSTER ON *.* TO <username>
```

Por padrão, consultas com `ON CLUSTER` exigem que o usuário tenha a concessão `CLUSTER`.
Você receberá o seguinte erro se tentar usar `ON CLUSTER` em uma consulta sem antes conceder a concessão `CLUSTER`:

```text
Not enough privileges. To execute this query, it's necessary to have the grant CLUSTER ON *.*. 
```

O comportamento padrão pode ser alterado definindo a configuração `on_cluster_queries_require_cluster_grant`,
localizada na seção `access_control_improvements` de `config.xml` (veja abaixo), para `false`.

```yaml title="config.xml"
<access_control_improvements>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
</access_control_improvements>
```

<div id="drop">
  ### DROP
</div>

Permite executar consultas [DROP](../../sql-reference/statements/drop.md) e [DETACH](../../sql-reference/statements/detach.md) de acordo com a seguinte hierarquia de privilégios:

* `DROP`. Nível: `GROUP`
  * `DROP DATABASE`. Nível: `DATABASE`
  * `DROP TABLE`. Nível: `TABLE`
  * `DROP VIEW`. Nível: `VIEW`
  * `DROP DICTIONARY`. Nível: `DICTIONARY`

<div id="truncate">
  ### TRUNCATE
</div>

Permite executar consultas [TRUNCATE](../../sql-reference/statements/truncate.md).

Nível de privilégio: `TABLE`.

<div id="optimize">
  ### OPTIMIZE
</div>

Permite executar instruções [OPTIMIZE TABLE](../../sql-reference/statements/optimize.md).

Nível de privilégio: `TABLE`.

<div id="show">
  ### SHOW
</div>

Permite executar consultas `SHOW`, `DESCRIBE`, `USE` e `EXISTS` de acordo com a seguinte hierarquia de privilégios:

* `SHOW`. Nível: `GROUP`
  * `SHOW DATABASES`. Nível: `DATABASE`. Permite executar as consultas `SHOW DATABASES`, `SHOW CREATE DATABASE`, `USE <database>`.
  * `SHOW TABLES`. Nível: `TABLE`. Permite executar as consultas `SHOW TABLES`, `EXISTS <table>`, `CHECK <table>`.
  * `SHOW COLUMNS`. Nível: `COLUMN`. Permite executar as consultas `SHOW CREATE TABLE`, `DESCRIBE`.
  * `SHOW DICTIONARIES`. Nível: `DICTIONARY`. Permite executar as consultas `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY`, `EXISTS <dictionary>`.

**Notas**

Um usuário tem o privilégio `SHOW` se tiver qualquer outro privilégio sobre a tabela, o dicionário ou o banco de dados especificado.

<div id="kill-query">
  ### KILL QUERY
</div>

Permite executar consultas [KILL](../../sql-reference/statements/kill.md#kill-query) conforme a seguinte hierarquia de privilégios:

Nível de privilégio: `GLOBAL`.

**Notas**

O privilégio `KILL QUERY` permite que um usuário encerre consultas de outros usuários.

<div id="access-management">
  ### ACCESS MANAGEMENT
</div>

Permite que um usuário execute consultas que gerenciam usuários, funções e políticas de linha.

* `ACCESS MANAGEMENT`. Nível: `GROUP`
  * `CREATE USER`. Nível: `GLOBAL`
  * `ALTER USER`. Nível: `GLOBAL`
  * `DROP USER`. Nível: `GLOBAL`
  * `CREATE ROLE`. Nível: `GLOBAL`
  * `ALTER ROLE`. Nível: `GLOBAL`
  * `DROP ROLE`. Nível: `GLOBAL`
  * `ROLE ADMIN`. Nível: `GLOBAL`
  * `CREATE ROW POLICY`. Nível: `GLOBAL`. Aliases: `CREATE POLICY`
  * `ALTER ROW POLICY`. Nível: `GLOBAL`. Aliases: `ALTER POLICY`
  * `DROP ROW POLICY`. Nível: `GLOBAL`. Aliases: `DROP POLICY`
  * `CREATE QUOTA`. Nível: `GLOBAL`
  * `ALTER QUOTA`. Nível: `GLOBAL`
  * `DROP QUOTA`. Nível: `GLOBAL`
  * `CREATE SETTINGS PROFILE`. Nível: `GLOBAL`. Aliases: `CREATE PROFILE`
  * `ALTER SETTINGS PROFILE`. Nível: `GLOBAL`. Aliases: `ALTER PROFILE`
  * `DROP SETTINGS PROFILE`. Nível: `GLOBAL`. Aliases: `DROP PROFILE`
  * `SHOW ACCESS`. Nível: `GROUP`
    * `SHOW_USERS`. Nível: `GLOBAL`. Aliases: `SHOW CREATE USER`
    * `SHOW_ROLES`. Nível: `GLOBAL`. Aliases: `SHOW CREATE ROLE`
    * `SHOW_ROW_POLICIES`. Nível: `GLOBAL`. Aliases: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
    * `SHOW_QUOTAS`. Nível: `GLOBAL`. Aliases: `SHOW CREATE QUOTA`
    * `SHOW_SETTINGS_PROFILES`. Nível: `GLOBAL`. Aliases: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`
  * `ALLOW SQL SECURITY NONE`. Nível: `GLOBAL`. Aliases: `CREATE SQL SECURITY NONE`, `SQL SECURITY NONE`, `SECURITY NONE`

O privilégio `ROLE ADMIN` permite que um usuário atribua e revogue quaisquer funções, inclusive aquelas que não foram atribuídas a ele com a opção de admin.

<div id="system">
  ### SYSTEM
</div>

Permite que um usuário execute consultas [SYSTEM](../../sql-reference/statements/system.md) de acordo com a seguinte hierarquia de privilégios.

* `SYSTEM`. Nível: `GROUP`
  * `SYSTEM SHUTDOWN`. Nível: `GLOBAL`. Aliases: `SYSTEM KILL`, `SHUTDOWN`
  * `SYSTEM DROP CACHE`. Aliases: `DROP CACHE`
    * `SYSTEM DROP DNS CACHE`. Nível: `GLOBAL`. Aliases: `SYSTEM CLEAR DNS CACHE`, `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
    * `SYSTEM DROP MARK CACHE`. Nível: `GLOBAL`. Aliases: `SYSTEM CLEAR MARK CACHE`, `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
    * `SYSTEM DROP UNCOMPRESSED CACHE`. Nível: `GLOBAL`. Aliases: `SYSTEM CLEAR UNCOMPRESSED CACHE`, `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
  * `SYSTEM RELOAD`. Nível: `GROUP`
    * `SYSTEM RELOAD CONFIG`. Nível: `GLOBAL`. Aliases: `RELOAD CONFIG`
    * `SYSTEM RELOAD DICTIONARY`. Nível: `GLOBAL`. Aliases: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
      * `SYSTEM RELOAD EMBEDDED DICTIONARIES`. Nível: `GLOBAL`. Aliases: `RELOAD EMBEDDED DICTIONARIES`
  * `SYSTEM MERGES`. Nível: `TABLE`. Aliases: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
  * `SYSTEM TTL MERGES`. Nível: `TABLE`. Aliases: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
  * `SYSTEM FETCHES`. Nível: `TABLE`. Aliases: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
  * `SYSTEM MOVES`. Nível: `TABLE`. Aliases: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
  * `SYSTEM SENDS`. Nível: `GROUP`. Aliases: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
    * `SYSTEM DISTRIBUTED SENDS`. Nível: `TABLE`. Aliases: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
    * `SYSTEM REPLICATED SENDS`. Nível: `TABLE`. Aliases: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
  * `SYSTEM REPLICATION QUEUES`. Nível: `TABLE`. Aliases: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
  * `SYSTEM SYNC REPLICA`. Nível: `TABLE`. Aliases: `SYNC REPLICA`
  * `SYSTEM RESTART REPLICA`. Nível: `TABLE`. Aliases: `RESTART REPLICA`
  * `SYSTEM FLUSH`. Nível: `GROUP`
    * `SYSTEM FLUSH DISTRIBUTED`. Nível: `TABLE`. Aliases: `FLUSH DISTRIBUTED`
    * `SYSTEM FLUSH LOGS`. Nível: `GLOBAL`. Aliases: `FLUSH LOGS`

O privilégio `SYSTEM RELOAD EMBEDDED DICTIONARIES` é concedido implicitamente pelo privilégio `SYSTEM RELOAD DICTIONARY ON *.*`.

<div id="introspection">
  ### INTROSPECTION
</div>

Permite usar funções de [introspecção](../../operations/optimizing-performance/sampling-query-profiler.md).

* `INTROSPECTION`. Nível: `GROUP`. Aliases: `INTROSPECTION FUNCTIONS`
  * `addressToLine`. Nível: `GLOBAL`
  * `addressToLineWithInlines`. Nível: `GLOBAL`
  * `addressToSymbol`. Nível: `GLOBAL`
  * `demangle`. Nível: `GLOBAL`

<div id="sources">
  ### FONTES
</div>

Permite usar fontes de dados externas. Aplica-se a [motores de tabela](../../engines/table-engines/index.md) e [funções de tabela](/pt-BR/sql-reference/table-functions).

* `READ`. Nível: `GLOBAL_WITH_PARAMETER`
* `WRITE`. Nível: `GLOBAL_WITH_PARAMETER`

Parâmetros possíveis:

* `AZURE`
* `FILE`
* `HDFS`
* `HIVE`
* `JDBC`
* `KAFKA`
* `MONGO`
* `MYSQL`
* `NATS`
* `ODBC`
* `POSTGRES`
* `RABBITMQ`
* `REDIS`
* `REMOTE`
* `S3`
* `SQLITE`
* `URL`

:::note
A separação dos privilégios READ/WRITE para fontes está disponível a partir da versão 25.7 e apenas com a configuração de servidor
`access_control_improvements.enable_read_write_grants`

Caso contrário, use a sintaxe `GRANT AZURE ON *.* TO user`, que equivale ao novo `GRANT READ, WRITE ON AZURE TO user`
:::

Exemplos:

* Para criar uma tabela com o [motor de tabela MySQL](../../engines/table-engines/integrations/mysql.md), você precisa de `CREATE TABLE (ON db.table_name)` e dos privilégios `MYSQL`.
* Para usar a [função de tabela MySQL](../../sql-reference/table-functions/mysql.md), você precisa de `CREATE TEMPORARY TABLE` e dos privilégios `MYSQL`.

<div id="source-filter-grants">
  ### Concessões com filtro de origem
</div>

:::note
Este recurso está disponível a partir da versão 25.8 e somente com a configuração no nível do servidor
`access_control_improvements.enable_read_write_grants`
:::

Você pode conceder acesso a URIs de origem específicas usando filtros de expressão regular. Isso permite um controle mais granular sobre quais fontes de dados externas os usuários podem acessar.

**Sintaxe:**

```sql
GRANT READ ON S3('regexp_pattern') TO user
```

Esta concessão permitirá ao usuário ler apenas URIs do S3 que correspondam ao padrão de expressão regular especificado.

**Exemplos:**

Conceda acesso a caminhos específicos em buckets do S3:

```sql
-- Allow user to read only from s3://foo/ paths
GRANT READ ON S3('s3://foo/.*') TO john

-- Allow user to read from specific file patterns
GRANT READ ON S3('s3://mybucket/data/2024/.*\.parquet') TO analyst

-- Multiple filters can be granted to the same user
GRANT READ ON S3('s3://foo/.*') TO john
GRANT READ ON S3('s3://bar/.*') TO john
```

:::warning
O filtro da origem aceita **regexp** como parâmetro, portanto um GRANT
`GRANT READ ON URL('http://www.google.com') TO john;`

permitirá consultas

```sql
SELECT * FROM url('https://www.google.com');
SELECT * FROM url('https://www-google.com');
```

porque `.` é tratado como `qualquer caractere único` nas expressões regulares.
Isso pode resultar em uma vulnerabilidade potencial. A concessão correta deve ser

```sql
GRANT READ ON URL('https://www\.google\.com') TO john;
```

:::

**Concessão novamente com GRANT OPTION:**

Se a concessão original tiver `WITH GRANT OPTION`, ela poderá ser concedida novamente usando `GRANT CURRENT GRANTS`:

```sql
-- Original grant with GRANT OPTION
GRANT READ ON S3('s3://foo/.*') TO john WITH GRANT OPTION

-- John can now regrant this access to others
GRANT CURRENT GRANTS(READ ON S3) TO alice
```

**Limitações importantes:**

* **Revogações parciais não são permitidas:** Você não pode revogar um subconjunto de um padrão de filtro já concedido. É necessário revogar toda a permissão e concedê-la novamente com novos padrões, se necessário.
* **Permissões com curinga não são permitidas:** Você não pode usar `GRANT READ ON *('regexp')` nem padrões semelhantes compostos apenas por curingas. É necessário especificar uma fonte.

<div id="dictget">
  ### dictGet
</div>

* `dictGet`. Aliases: `dictHas`, `dictGetHierarchy`, `dictIsIn`

Permite ao usuário executar as funções [dictGet](/pt-BR/sql-reference/functions/ext-dict-functions#dictGet), [dictHas](../../sql-reference/functions/ext-dict-functions.md#dictHas), [dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictGetHierarchy), [dictIsIn](../../sql-reference/functions/ext-dict-functions.md#dictIsIn).

Nível de privilégio: `DICTIONARY`.

**Exemplos**

* `GRANT dictGet ON mydb.mydictionary TO john`
* `GRANT dictGet ON mydictionary TO john`

<div id="displaysecretsinshowandselect">
  ### displaySecretsInShowAndSelect
</div>

Permite que um usuário visualize segredos em consultas `SHOW` e `SELECT` se tanto a
[`display_secrets_in_show_and_select` server setting](../../operations/server-configuration-parameters/settings#display_secrets_in_show_and_select)
quanto a
[`format_display_secrets_in_show_and_select` format setting](../../operations/settings/formats#format_display_secrets_in_show_and_select)
estiverem ativadas.

<div id="named-collection-admin">
  ### NAMED COLLECTION ADMIN
</div>

Permite uma determinada operação em uma coleção nomeada especificada. Antes da versão 23.7, chamava-se NAMED COLLECTION CONTROL; após a 23.7, NAMED COLLECTION ADMIN foi adicionado, e NAMED COLLECTION CONTROL foi mantido como alias.

* `NAMED COLLECTION ADMIN`. Nível: `NAMED_COLLECTION`. Aliases: `NAMED COLLECTION CONTROL`
  * `CREATE NAMED COLLECTION`. Nível: `NAMED_COLLECTION`
  * `DROP NAMED COLLECTION`. Nível: `NAMED_COLLECTION`
  * `ALTER NAMED COLLECTION`. Nível: `NAMED_COLLECTION`
  * `SHOW NAMED COLLECTIONS`. Nível: `NAMED_COLLECTION`. Aliases: `SHOW NAMED COLLECTIONS`
  * `SHOW NAMED COLLECTIONS SECRETS`. Nível: `NAMED_COLLECTION`. Aliases: `SHOW NAMED COLLECTIONS SECRETS`
  * `NAMED COLLECTION`. Nível: `NAMED_COLLECTION`. Aliases: `NAMED COLLECTION USAGE, USE NAMED COLLECTION`

Ao contrário de todos os outros privilégios (CREATE, DROP, ALTER, SHOW), o privilégio NAMED COLLECTION foi adicionado somente na versão 23.7, enquanto todos os demais foram adicionados antes, na 22.12.

**Exemplos**

Supondo que uma coleção nomeada se chame abc, concedemos o privilégio CREATE NAMED COLLECTION ao usuário john.

* `GRANT CREATE NAMED COLLECTION ON abc TO john`

<div id="table-engine">
  ### TABLE ENGINE
</div>

Permite usar um motor de tabela específico ao criar uma tabela. Aplica-se aos [motores de tabela](../../engines/table-engines/index.md).

**Exemplos**

* `GRANT TABLE ENGINE ON * TO john`
* `GRANT TABLE ENGINE ON TinyLog TO john`

:::note
Por padrão, por motivos de compatibilidade retroativa, a criação de uma tabela com um motor de tabela específico ignora as concessões;
no entanto, você pode alterar esse comportamento definindo [`table_engines_require_grant` como true](https://github.com/ClickHouse/ClickHouse/blob/df970ed64eaf472de1e7af44c21ec95956607ebb/programs/server/config.xml#L853-L855)
em config.xml.
:::

Alguns motores de tabela com fontes externas podem exigir permissões `READ`/`WRITE` na fonte correspondente. Consulte [Fontes](#sources).

Por exemplo, para o motor de tabela AzureBlobStorage, a concessão a seguir pode ser necessária.

* `GRANT READ, WRITE ON AZURE TO john`

<div id="all">
  ### ALL
</div>

<CloudNotSupportedBadge />

Concede todos os privilégios sobre a entidade regulada a uma conta de usuário ou a uma função.

:::note
O privilégio `ALL` não tem suporte no ClickHouse Cloud, onde o usuário `default` tem permissões limitadas. Os usuários podem conceder o conjunto máximo de permissões a um usuário concedendo a `default_role`. Veja [aqui](/pt-BR/cloud/security/manage-cloud-users) para mais detalhes.
Os usuários também podem usar `GRANT CURRENT GRANTS` com o usuário `default` para obter um efeito semelhante ao de `ALL`.
:::

<div id="none">
  ### NONE
</div>

Não concede privilégios.

<div id="admin-option">
  ### ADMIN OPTION
</div>

O privilégio `ADMIN OPTION` permite que um usuário conceda sua função a outro usuário.