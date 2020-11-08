---
toc_priority: 53
toc_title: 'null'
---

# null {#null-function}

Создает временную таблицу указанной структуры [на движке Null](../../engines/table-engines/special/null.md). В соответствии с правилами работы движка Null, данные в таблице игнорируются, а сама таблица удаляется сразу после выполнения запроса. Функция используется для удобства написания тестов и демонстрационных примеров.

**Синтаксис** 

``` sql
null('structure')
```

**Параметр** 

-   `structure` — список колонок и их типов. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

Временная таблица указанной структуры на движке Null.

**Пример**

Один запрос с функцией `null`:

``` sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```
заменяет три запроса:

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

См. также: 

-   [Движок таблиц Null](../../engines/table-engines/special/null.md)

[Original article](https://clickhouse.tech/docs/en/sql-reference/table-functions/null/) <!--hide-->
