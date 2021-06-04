---
toc_priority: 41
toc_title: "Политика доступа"
---

# CREATE ROW POLICY {#create-row-policy-statement}

Создает [политики доступа к строкам](../../../operations/access-rights.md#row-policy-management), т.е. фильтры, которые определяют, какие строки пользователь может читать из таблицы.

Синтаксис:

``` sql
CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name1 [ON CLUSTER cluster_name1] ON [db1.]table1 
        [, policy_name2 [ON CLUSTER cluster_name2] ON [db2.]table2 ...] 
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT] USING condition
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

## Секция USING {#create-row-policy-using}

Секция `USING` указывает условие для фильтрации строк. Пользователь может видеть строку, если это условие, вычисленное для строки, дает ненулевой результат.

## Секция TO {#create-row-policy-to}

В секции `TO` перечисляются пользователи и роли, для которых должна действовать политика. Например, `CREATE ROW POLICY ... TO accountant, john@localhost`.

Ключевым словом `ALL` обозначаются все пользователи, включая текущего. Ключевые слова `ALL EXCEPT` позволяют исключить пользователей из списка всех пользователей. Например, `CREATE ROW POLICY ... TO ALL EXCEPT accountant, john@localhost`

!!! note "Note"
    Если для таблицы не задано ни одной политики доступа к строкам, то любой пользователь может выполнить команду SELECT и получить все строки таблицы. Если определить хотя бы одну политику для таблицы, до доступ к строкам будет управляться этими политиками, причем для всех пользователей (даже для тех, для кого политики не определялись). Например, следующая политика

    `CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter`

    запретит пользователям `mira` и `peter` видеть строки с `b != 1`, и еще запретит всем остальным пользователям (например, пользователю `paul`) видеть какие-либо строки вообще из таблицы `mydb.table1`.
    
    Если это нежелательно, такое поведение можно исправить, определив дополнительную политику:

    `CREATE ROW POLICY pol2 ON mydb.table1 USING 1 TO ALL EXCEPT mira, peter`

## Секция AS {#create-row-policy-as}

Может быть одновременно активно более одной политики для одной и той же таблицы и одного и того же пользователя. Поэтому нам нужен способ комбинировать политики.

По умолчанию политики комбинируются с использованием логического оператора `OR`. Например, политики:

``` sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 TO peter, antonio
```

разрешат пользователю с именем `peter` видеть строки, для которых будет верно `b=1` или `c=2`.

Секция `AS` указывает, как политики должны комбинироваться с другими политиками. Политики могут быть или разрешительными (`PERMISSIVE`), или ограничительными (`RESTRICTIVE`). По умолчанию политики создаются разрешительными (`PERMISSIVE`); такие политики комбинируются с использованием логического оператора `OR`.

Ограничительные (`RESTRICTIVE`) политики комбинируются с использованием логического оператора `AND`.

Общая формула выглядит так:

```
строка_видима = (одна или больше permissive-политик дала ненулевой результат проверки условия) И
                (все restrictive-политики дали ненулевой результат проверки условия)
```

Например, политики

``` sql
CREATE ROW POLICY pol1 ON mydb.table1 USING b=1 TO mira, peter
CREATE ROW POLICY pol2 ON mydb.table1 USING c=2 AS RESTRICTIVE TO peter, antonio
```

разрешат пользователю с именем `peter` видеть только те строки, для которых будет одновременно `b=1` и `c=2`.

## Секция ON CLUSTER {#create-row-policy-on-cluster}

Секция `ON CLUSTER` позволяет создавать политики на кластере, см. [Распределенные DDL запросы](../../../sql-reference/distributed-ddl.md).

## Примеры

`CREATE ROW POLICY filter1 ON mydb.mytable USING a<1000 TO accountant, john@localhost`

`CREATE ROW POLICY filter2 ON mydb.mytable USING a<1000 AND b=5 TO ALL EXCEPT mira`

`CREATE ROW POLICY filter3 ON mydb.mytable USING 1 TO admin`

<!--hide-->