# EXISTS {#exists-operator}

Оператор `EXISTS` проверяет, сколько строк содержит результат выполнения подзапроса. Если результат пустой, то оператор возвращает `0`. В остальных случаях оператор возвращает `1`.

`EXISTS` может быть использован в секции [WHERE](../../sql-reference/statements/select/where.md).

!!! warning "Предупреждение"
    Ссылки на таблицы или столбцы основного запроса не поддерживаются в подзапросе. 

**Синтаксис**

```sql
WHERE EXISTS(subquery)
```

**Пример**

Запрос:

``` sql
SELECT 'Exists' WHERE EXISTS (SELECT * FROM numbers(10) WHERE number < 2);
SELECT 'Empty subquery' WHERE EXISTS (SELECT * FROM numbers(10) WHERE number > 12);
```

Первый запрос возвращает одну строку, а второй запрос не возвращает строк, так как результат его подзапроса пустой:

``` text
┌─'Exists'─┐
│ Exists   │
└──────────┘
```
