---
toc_priority: 7
toc_title: Политика доступа
---

# CREATE ROW POLICY {#create-row-policy-statement}

Создает [фильтр для строк](../../../operations/access-rights.md#row-policy-management), которые пользователь может прочесть из таблицы.

### Синтаксис {#create-row-policy-syntax}

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [ON CLUSTER cluster_name] ON [db.]table
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING condition]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

#### Секция AS {#create-row-policy-as}

С помощью данной секции можно создать политику разрешения или ограничения.

Политика разрешения предоставляет доступ к строкам. Разрешительные политики, которые применяются к одной таблице, объединяются с помощью логического оператора `OR`. Политики являются разрешительными по умолчанию.

Политика ограничения запрещает доступ к строкам. Ограничительные политики, которые применяются к одной таблице, объединяются логическим оператором `AND`.

Ограничительные политики применяются к строкам, прошедшим фильтр разрешительной политики. Если вы не зададите разрешительные политики, пользователь не сможет обращаться ни к каким строкам из таблицы.

#### Секция TO {#create-row-policy-to}

В секции `TO` вы можете перечислить как роли, так и пользователей. Например, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Ключевым словом `ALL` обозначаются все пользователи, включая текущего. Ключевые слова `ALL EXCEPT` позволяют исключить пользователей из списка всех пользователей. Например, `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

### Примеры

- `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO accountant, john@localhost`
- `CREATE ROW POLICY filter ON mydb.mytable FOR SELECT USING a<1000 TO ALL EXCEPT mira`

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/create/row-policy) 
<!--hide-->