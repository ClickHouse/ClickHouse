## view {#view}

Преобразовывает подзапрос в таблицу. Функция реализовывает представления (смотрите [CREATE VIEW](https://clickhouse.tech/docs/ru/sql-reference/statements/create/view/#create-view)). Результирующая таблица не хранит данные, а только сохраняет указанный запрос `SELECT`. При чтении из таблицы, ClickHouse выполняет запрос и удаляет все ненужные столбцы из результата.

**Синтаксис**

``` sql
view(subquery)
```

**Аргументы**

-   `subquery` — запрос `SELECT`.

**Возвращаемое значение**

-   Таблица.

**Пример**

Входная таблица:

``` text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

Запрос:

``` sql
SELECT * FROM view(SELECT name FROM months);
```

Результат:

``` text
┌─name─────┐
│ January  │
│ February │
│ March    │
│ April    │
└──────────┘
```

Вы можете использовать функцию `view` как параметр табличных функций [remote](https://clickhouse.tech/docs/ru/sql-reference/table-functions/remote/#remote-remotesecure) и [cluster](https://clickhouse.tech/docs/ru/sql-reference/table-functions/cluster/#cluster-clusterallreplicas):

``` sql
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name));
```

``` sql
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name));
```

**Смотрите также**

-   [view](https://clickhouse.tech/docs/ru/engines/table-engines/special/view/#table_engines-view)

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/table-functions/view/) <!--hide-->
