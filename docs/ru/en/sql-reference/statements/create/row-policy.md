---
description: 'Документация по ROW POLICY'
sidebar_label: 'ROW POLICY'
sidebar_position: 41
slug: /sql-reference/statements/create/row-policy
title: 'CREATE ROW POLICY'
doc_type: 'reference'
---

Создаёт [row policy](../../../guides/sre/user-management/index.md#row-policy-management), то есть фильтр, определяющий, какие строки пользователь может читать из таблицы.

:::tip
ROW POLICY имеет смысл только для пользователей с доступом только для чтения. Если пользователь может изменять таблицу или копировать партиции между таблицами, это сводит на нет ограничения ROW POLICY.
:::

Синтаксис:

```sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name1 [ON CLUSTER cluster_name1] ON [db1.]table1|db1.*
        [, policy_name2 [ON CLUSTER cluster_name2] ON [db2.]table2|db2.* ...]
    [IN access_storage_type]
    [FOR SELECT] USING condition
    [AS {PERMISSIVE | RESTRICTIVE}]
    [TO {role1 [, role2 ...] | ALL | ALL EXCEPT role1 [, role2 ...]}]
```

<div id="using-clause">
  ## предложение USING
</div>

Позволяет задать условие для фильтрации строк. Пользователь увидит строку, если для неё значение условия вычисляется как ненулевое.

<div id="to-clause">
  ## предложение TO
</div>

В разделе `TO` можно указать список пользователей и ролей, для которых должна действовать эта политика. Например, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Ключевое слово `ALL` означает всех пользователей ClickHouse, включая текущего пользователя. Ключевое слово `ALL EXCEPT` позволяет исключить некоторых пользователей из списка всех пользователей, например, `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

<div id="as-clause">
  ## предложение AS
</div>

Для одной и той же таблицы и одного и того же пользователя одновременно может быть включено несколько политик. Поэтому нужен способ объединять условия из нескольких политик.

По умолчанию политики объединяются с помощью булевого оператора `OR`. Например, следующие политики:

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio
```

позволить пользователю `peter` видеть строки, в которых либо `b=1`, либо `c=2`.

Предложение `AS` задаёт, как политики должны комбинироваться с другими политиками. Политики могут быть как разрешающими, так и ограничивающими. По умолчанию политики являются разрешающими, то есть они комбинируются с помощью логического оператора `OR`.

В качестве альтернативы политику можно определить как ограничивающую. Ограничивающие политики комбинируются с помощью логического оператора `AND`.

Вот общая формула:

```text
row_is_visible = (one or more of the permissive policies' conditions are non-zero) AND
                 (all of the restrictive policies's conditions are non-zero)
```

Например, такие политики:

```sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

разрешить пользователю `peter` видеть строки только при одновременном выполнении условий `b=1` и `c=2`.

Политики на уровне базы данных объединяются с политиками таблиц.

Например, следующие политики:

```sql
CREATE ROW POLICY pol1 ON mydb.* USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

разрешает пользователю `peter` видеть строки таблицы table1, только если одновременно выполняются условия `b=1` и `c=2`, тогда как
для любой другой таблицы в mydb к пользователю будет применяться только политика `b=1`.

<div id="on-cluster-clause">
  ## Предложение ON CLUSTER
</div>

Позволяет создавать ROW POLICY в кластере, см. [Distributed DDL](../../../sql-reference/distributed-ddl.md).

<div id="examples">
  ## Примеры
</div>

`CREATE ROW POLICY filter1 ON mydb.mytable USING a<1000 TO accountant, john@localhost`

`CREATE ROW POLICY filter2 ON mydb.mytable USING a<1000 AND b=5 TO ALL EXCEPT mira`

`CREATE ROW POLICY filter3 ON mydb.mytable USING 1 TO admin`

`CREATE ROW POLICY filter4 ON mydb.* USING 1 TO admin`