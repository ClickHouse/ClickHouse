---
toc_priority: 44
toc_title: DESCRIBE
---

# DESCRIBE TABLE Statement {#misc-describe-table}

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

Возвращает описание столбцов таблицы.

Результат запроса содержит столбцы (все столбцы имеют тип String):

-   `name` — имя столбца таблицы;
-   `type`— тип столбца;
-   `default_type` — в каком виде задано [выражение для значения по умолчанию](create.md#create-default-values): `DEFAULT`, `MATERIALIZED` или `ALIAS`. Столбец содержит пустую строку, если значение по умолчанию не задано.
-   `default_expression` — значение, заданное в секции `DEFAULT`;
-   `comment_expression` — комментарий к столбцу.

Вложенные структуры данных выводятся в «развёрнутом» виде. То есть, каждый столбец - по отдельности, с именем через точку.
