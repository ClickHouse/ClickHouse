# REVOKE

Отзывает привилегии у пользователей или ролей.

## Синтаксис {#revoke-syntax}

**Отзыв привилегий у пользователей**

``` sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**Отзыв ролей у пользователей**

``` sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

## Описание {#revoke-description}

Для отзыва привилегий можно использовать привилегию более широкой области действия. Например, если у пользователя есть привилегия `SELECT (x,y)`, администратор может отозвать ее с помощью одного из запросов: `REVOKE SELECT(x,y) ...`, `REVOKE SELECT * ...` или даже `REVOKE ALL PRIVILEGES ...`.

### Частичный отзыв {#partial-revokes-dscr}

Вы можете отозвать часть привилегии. Например, если у пользователя есть привилегия `SELECT *.*`, вы можете отозвать привилегию на чтение данных из какой-то таблицы или базы данных.

## Примеры {#revoke-example}

Присвоить пользователю `john` привилегию на `SELECT` из всех баз данных кроме `accounts`:

``` sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

Присвоить пользователю `mira` привилегию на `SELECT` из всех столбцов таблицы `accounts.staff` кроме столбца `wage`:

``` sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

[Оригинальная статья](https://clickhouse.tech/docs/en/operations/settings/settings/) <!-- hide -->
