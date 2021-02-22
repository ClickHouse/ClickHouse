---
toc_title: ALL
---

# ALL Clause {#select-all}

Поведение запроса `SELECT ALL` точно такое же, как и `SELECT` без аргумента `DISTINCT`.

-   если указан аргумент `ALL`, он будет игнорироваться.
-   если указаны оба аргумента: `ALL` и `DISTINCT`, функция вернет исключение.

`ALL` может быть указан внутри агрегатной функции, например, результат выполнения запроса:

```sql
SELECT sum(ALL number) FROM numbers(10);
```

равен результату выполнения запроса:

```sql
SELECT sum(number) FROM numbers(10);
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/select/all) <!--hide-->
