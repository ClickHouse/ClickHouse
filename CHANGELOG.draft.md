## RU

### Новые возможности:
* Добавлена поддержка ON выражений для JOIN ON синтаксиса:  
`JOIN ON Expr([table.]column, ...) = Expr([table.]column, ...) [AND Expr([table.]column, ...) = Expr([table.]column, ...) ...]`  
Выражение должно представлять из себя цепочку равенств, объединенных оператором AND. Каждая часть равенства может являться произвольным выражением над столбцами одной из таблиц. Поддержана возможность использования fully qualified имен столбцов (`table.name`, `database.table.name`, `table_alias.name`, `subquery_alias.name`) для правой таблицы. [#2742](https://github.com/yandex/ClickHouse/pull/2742)
