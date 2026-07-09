---
description: 'Документация по CHECK GRANT'
sidebar_label: 'CHECK GRANT'
sidebar_position: 56
slug: /sql-reference/statements/check-grant
title: 'Оператор CHECK GRANT'
doc_type: 'reference'
---

Запрос `CHECK GRANT` используется для проверки, выдана ли текущему пользователю или роли определённая привилегия.

<div id="syntax">
  ## Синтаксис
</div>

Синтаксис запроса выглядит следующим образом:

```sql
CHECK GRANT privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*}
```

* `privilege` — Тип привилегии.

<div id="examples">
  ## Примеры
</div>

Если пользователю ранее была выдана привилегия, значение `check_grant` в ответе будет равно `1`. В противном случае значение `check_grant` в ответе будет равно `0`.

Если `table_1.col1` существует и у текущего пользователя есть привилегия `SELECT`/`SELECT(con)` или роль (с этой привилегией), ответ будет `1`.

```sql
CHECK GRANT SELECT(col1) ON table_1;
```

```text
┌─result─┐
│      1 │
└────────┘
```

Если `table_2.col2` не существует или у текущего пользователя нет привилегии `SELECT`/`SELECT(con)` либо роли (с этой привилегией), ответ — `0`.

```sql
CHECK GRANT SELECT(col2) ON table_2;
```

```text
┌─result─┐
│      0 │
└────────┘
```

<div id="wildcard">
  ## Подстановочный знак
</div>

При указании привилегий можно использовать звёздочку (`*`) вместо имени таблицы или базы данных. Правила использования подстановочных знаков см. в разделе [WILDCARD GRANTS](../../sql-reference/statements/grant.md#wildcard-grants).