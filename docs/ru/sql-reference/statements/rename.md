---
toc_priority: 48
toc_title: RENAME
---

# RENAME Statement {#misc_operations-rename}

## RENAME DATABASE {#misc_operations-rename_database}
Переименовывает базу данных, поддерживается только для движка базы данных Atomic.

```
RENAME DATABASE atomic_database1 TO atomic_database2 [ON CLUSTER cluster]
```

## RENAME TABLE {#misc_operations-rename_table}
Переименовывает одну или несколько таблиц.

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

Переименовывание таблицы является лёгкой операцией. Если вы указали после `TO` другую базу данных, то таблица будет перенесена в эту базу данных. При этом, директории с базами данных должны быть расположены в одной файловой системе (иначе возвращается ошибка). В случае переименования нескольких таблиц в одном запросе — это неатомарная операция,  может выполнится частично, запросы в других сессиях могут получить ошибку `Table ... doesn't exist...`.
