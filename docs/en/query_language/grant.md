# GRANT

Grants [privileges](#grant-privileges) to a ClickHouse user account or a role.

To revoke privileges, use the [REVOKE](revoke.md) statement. Also you can list granted privileges by the [SHOW GRANTS](show.md#show-grants) statement.

## Syntax {#grant-syntax}

```sql
GRANT privilege TO user|role [WITH GRANT OPTION]
```

- `privilege` — Type of privilege.
- `user` — ClickHouse user.
- `role` — ClickHouse user role.

The `WITH GRANT OPTION` clause sets `GRANT OPTION` privilege for `user`.


## Usage {#grant-usage}

To use `GRANT`, your account must have the `GRANT OPTION` privilege. You can grant privileges only inside the scope of your account privileges.

For example, administrator has granted privileges to the `vasia` account by the query:

```sql
GRANT SELECT(x,y) ON db.table TO vasya WITH GRANT OPTION
```

It means that `vasya` has the permission to perform:

- `SELECT x,y FROM db.table`.
- `SELECT x FROM db.table`.
- `SELECT y FROM db.table`.

`vasya` can't perform `SELECT z FROM db.table`. The `SELECT * FROM db.table` also is not available. ClickHouse doesn't return any data, even `x` and `y`.

Also `vasya` has the `GRANT OPTION` privilege, so it can grant other users with privileges of the same or the smaller scope.

Specifying privileges you can use asterisk (`*`) instead of a table or a database name. For example, the `GRANT SELECT ON db.* TO vasya` query allows `vasia` to perform the `SELECT` query over all the tables in `db` database. Also, you can omit database name. In this case privileges are granted for current database.

You can grant multiple privileges to multiple accounts in one query. The query `GRANT SELECT, INSERT ON *.* TO vasya, petya` allows accounts `vasia` and `petya` to perform the `INSERT` and `SELECT` queries over all the tables in all the databases on the server.


## Privileges {#grant-privileges}

Privilege is a permission to perform queries.

Privileges have hierarchic structure. A set of permitted queries depends on the privilege scope.

Top scope privileges:

- [SELECT](#grant-select)
- [INSERT](#grant-insert)
- [ALTER](#grant-alter)
- [CREATE](#grant-create)
- [DROP](#grant-drop)
- [TRUNCATE](#grant-truncate)
- [OPTIMIZE](#grant-optimize)
- [SHOW](#grant-show)
- [EXISTS](#grant-exists)
- [KILL](#grant-kill)
- [CREATE USER](#grant-create-user)
- [ROLE ADMIN](#grant-role-admin)
- [SYSTEM](#grant-system)
- [INTROSPECTION](#grant-introspection)
- [dictGet](#grant-dictget)
- [Table Functions](#grant-table-functions)

The special privilege [ALL](#grant-all) grants all the privileges to a user account or a role.

By default, a user account or a role has no privileges.

Some queries by their implementation require a set of privileges. For example, to perform the [RENAME](misc.md#misc_operations-rename) query you need the following privileges: `SELECT`, `CREATE TABLE`, `INSERT` and `DROP TABLE`.


### SELECT {#grant-select}

Allows to perform [SELECT](select.md) queries.

**Syntax**

```sql
SELECT (list, of, columns) ON db.table
```

**Description**

User granted with this privilege can perform `SELECT` queries over a specified list of columns in the specified table and database. If user includes other columns then specified a query returns no data. 

Consider the following privilege:

```sql
GRANT SELECT(x,y) ON db.table TO vasya
```

This privilege allows `vasya` to perform any `SELECT` query that involves data from the `x` and/or `y` columns in `db.table`. For example, `SELECT x FROM db.table`. `vasya` can't perform `SELECT z FROM db.table`. The `SELECT * FROM db.table` also is not available. ClickHouse doesn't return any data, even `x` and `y`.

### INSERT {#grant-insert}

Allows to perform [INSERT](insert.md) queries.

**Syntax**

```sql
INSERT (list, of, columns) ON db.table
```

**Description**

User granted with this privilege can perform `INSERT` queries over a specified list of columns in the specified table and database. If user includes other columns then specified a query doesn't insert any data. 

Consider the following privilege:

```sql
GRANT INSERT(x,y) ON db.table TO vasya
```

This privilege allows `vasya` to perform any `SELECT` query that involves data from the `x` and/or `y` columns in `db.table`. For example, `SELECT x FROM db.table`. `vasya` can't perform `SELECT z FROM db.table`. The `SELECT * FROM db.table` also is not available. ClickHouse doesn't return any data, even `x` and `y`.

### ALTER {#grant-alter}

Allows to perform [ALTER](alter.md) queries corresponding to the following hierarchy of privileges:

- `ALTER`
  - `ALTER TABLE`
    - `UPDATE`
    - `DELETE`
    - `ALTER COLUMN`
      - `ADD COLUMN`
      - `DROP COLUMN`
      - `MODIFY COLUMN`
      - `COMMENT COLUMN`
    - `INDEX`
      - `ALTER ORDER BY`
      - `ADD INDEX`
      - `DROP INDEX`
      - `MATERIALIZE INDEX`
      - `CLEAR INDEX`
    - `ALTER CONSTRAINT`
      - `ADD CONSTRAINT`
      - `DROP CONSTRAINT`
    - `MODIFY TTL`
    - `MODIFY SETTING`
    - `MOVE PARTITION`
    - `FETCH PARTITION`
    - `FREEZE PARTITION`
    - `ALTER VIEW`
      - `REFRESH VIEW`
      - `MODIFY VIEW QUERY`

The `ALTER` privilege includes all other privileges. `ALTER CONSTRAINT` includes only `ADD CONSTRAINT` and `DROP CONSTRAINT`.

**Syntax**

```sql
GRANT UPDATE(list, of, columns) ON db.table
```

**Notes**

- The `MODIFY SETTING` privilege allows to modify table engine settings. In doesn't affect session or server settings.

### CREATE {#grant-create}

- CREATE
    - CREATE DATABASE
    - CREATE TABLE
      - CREATE VIEW
    - CREATE TEMPORARY TABLE
    - CREATE DICTIONARY


Остальные права написаны на слайде, часто одни включают другие. Например, право `CREATE` дает право создавать, соответственно, хоть базу данных, хоть таблицу, хоть словарь. То есть если нужно указать более детализировано, можно указать более детализировано.

`CREATE`, которое наверху написано, дает право создавать базы данных, таблицы, словари, но не дает право создавать пользователей, потому что это явно сущность другого рода. Про них, соответственно, написано внизу. То есть право создавать пользователей, роли, <span style="color: red;">политики строк</span>, но я про это попозже поговорю. 

`ATTACH` и `DETACH` - их тоже тут нет. Они вошли в `CREATE` и `DROP`. Чтобы делать `DETACH`, нужно иметь право, соответственно, на `DROP` этого объекта.


### DROP {#grant-drop}

- DROP
  - DROP DATABASE
  - DROP TABLE
    - DROP VIEW
  - DROP DICTIONARY

`ATTACH` и `DETACH` - их тоже тут нет. Они вошли в `CREATE` и `DROP`. Чтобы делать `DETACH`, нужно иметь право, соответственно, на `DROP` этого объекта.

Право на удаление таблицы есть, его можно дать явно: `GRANT DROP <имя таблицы>`. Но оно не дается вместе с `CREATE`.
Администратор может написать команду `GRANT ALL PRIVILEGES ON db.table TO vasya`, и ты, соответственно, сможешь и создать таблицу, и удалить ее.

У нас эта неконсистентность есть еще с запросами `DROP DICTIONARY`. У меня был тест написан, я как раз у Саши спрашивал по поводу этой неконсистентности. Но Саши сейчас нет. Спрашивал по поводу текущей базы данных: указывать ее или нет для словарей.



### TRUNCATE {#grant-truncate}

- TRUNCATE
  - TRUNCATE TABLE
    - TRUNCATE VIEW

### OPTIMIZE {#grant-optimize}


### SHOW {#grant-show}

`SHOW` и `EXISTS` - это особые права. `SHOW` дается автоматически, если у тебя есть любое другое право, относящееся к данной таблице или к данному столбцу. То есть если ты даешь пользователю право `SELECT` столбцов `x`, `y` из какой-нибудь таблицы, это означает, что `SHOW` по отношению к этим столбцам у пользователя тоже есть. Это влияет, например, на то, что в таблице <span style="color: red;">system.columns</span> он эти столбцы тоже увидит. И с другими командами тоже так работает. Если пользователю дано хоть какое-нибудь право, относящееся к таблице, он получает право `SHOW` к этой таблице автоматом.

### EXISTS {#grant-exists}

### KILL {#grant-kill}

- KILL
  - KILL QUERY
  - KILL MUTATION


`KILL QUERY` соответствует тому, что было до этого. То есть это, по сути, право убивать запросы, созданные другим пользователем, потому что убивать свои собственные запросы вроде бы кто угодно может.

`KILL MUTATION` я сделал так, но не уверен, что это правильно. Может быть, это стоило куда-то в `ALTER` загнать. В том плане, чтобы давать право убивать мутацию, если пользователь имеет право запускать `ALTER` соответствующей <span style="color: red;">коммутации</span> (не уверен, на самом деле). Просто много получается прав, хочется по возможности их ужимать.


### CREATE USER {#grant-create-user}

- CREATE USER
  - DROP USER
  - CREATE ROLE
    - DROP ROLE
  - CREATE ROW POLICY
    - DROP ROW POLICY
  - CREATE QUOTA
    - DROP QUOTA

### ROLE ADMIN {#grant-role-admin}

???

### SYSTEM {#grant-system}

- SYSTEM
  - SHUTDOWN
  - DROP CACHE
  - RELOAD CONFIG
  - RELOAD DICTIONARY
  - STOP MERGES
  - STOP TTL MERGES
  - STOP FETCHES
  - STOP MOVES
  - STOP DISTRIBUTED_SENDS
  - STOP REPLICATED_SENDS
  - SYNC REPLICA
  - RESTART REPLICA
  - FLUSH DISTRIBUTED
  - FLUSH LOGS

`SYSTEM` - это, я думаю, понятно. То есть все это относится к команде `SYSTEM`, <span style="color: red;">которая начинается с `SYSTEM`</span>. Дальше пошли функции. 

### INTROSPECTION {#grant-introspection}

- INTROSPECTION
  - addressToLine()
  - addressToSymbol()
  - demangle()

### dictGet {#grant-dictget}

dictGet() - у нас так сложилось, что она используется для словарей. Причем там форма такая, что мы пишем, из какого словаря мы можем читать данные.
Особая история, потому что я, на самом деле, до конца, может быть, не очень хорошо сделал пока. То есть вот сейчас она работает именно так, как тут написано:

- `GRANT dictGet() ON mydb.mydictionary TO vasya;`
- `GRANT dictGet() ON mydictionary TO vasya;`
- `GRANT dictGet() ON 'no_database'.mydictionary TO vasya;`

Между последним и предпоследним, на самом деле, не очень для словарей хорошо получилось, потому что у нас есть словари, у которых нет базы данных. И эта ситуация должна отличаться от ситуации, когда база данных должна быть текущей. В общем, я сделал пока в пользу консистентности с прочими грантами, то есть если база данных не написана, то подразумевается в текущей. Поэтому получилась вот такая корявая запись, если нам все-таки надо дать право выполнять `dictGet()` для словаря, который из <span style="color: red;">xml</span> приехал, у которого нет базы данных. Может быть, стоит поменять как-то. То есть просто пишется `<no_database>`.

Если сделать наоборот, то оно немного неконсистентно. Потому что кроме `dictGet()` существует ведь еще тот же `SELECT` для таблицы, там вот так. Можно сделать, например, так, что при выполнении `GRANT` проверяется, есть ли у тебя словарь без базы данных. В зависимости от этого, оно либо так выполняется, либо эдак.




### Table Functions {#grant-table-functions}

- TABLE FUNCTIONS
  - file()
  - url()
  - input()
  - values()
  - numbers()
  - merge()
  - remote()
  - mysql()
  - odbc()
  - jdbc()
  - jdfs()
  - s3()

Еще про права на табличные функции. Это, по сути, права на создание временного storage с определенным движком. А есть еще `CREATE TEMPORARY TABLE`. Может быть, например, что кому-то запретили использовать табличную функцию <span style="color: red;">url()</span>. Сейчас они никак не связаны, то есть `CREATE TEMPORARY TABLE` управляет только командой `CREATE TEMPORARY TABLE`. Те временные таблицы, которые создаются табличными функциями, сейчас на них `CREATE TEMPORARY TABLE` никакого воздействия не оказывает.

В деталях. Если мы даем GRANT на `CREATE TEMPORARY TABLE`, то мы эти таблицы можем создавать в тех базах данных, на которые у нас есть доступ. Вроде логично.

### ALL {#grant-all}

Уже реализован механизм, позволяющий накручивать синонимы к этим правам. Например, вместо `ALL PRIVILEGES` можно писать просто `ALL`. То есть `GRANT ALL` и `GRANT ALL PRIVILEGES` - это одно и то же.
