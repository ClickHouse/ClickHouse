---
toc_title: ALL
---

# ALL Clause {#select-all}

Результаты выполнения `SELECT ALL` точно такой же, как и `SELECT` с аргументом `DISTINCT`.

-   если указан аргумент `ALL`, он будет игнорирован.
-   если указаны оба аргумента: `ALL` и `DISTINCT`, будет вызвана критическая ошибка.

`ALL` также может быть указано внутри агрегатной функции с тем же эффектом(noop), например, результат выполнения :

```sql
SELECT sum(ALL number) FROM numbers(10);
```

равен результату выполнения:

```sql
SELECT sum(number) FROM numbers(10);
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/select/all) <!--hide-->