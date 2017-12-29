# system.settings

Содержит информацию о настройках, используемых в данный момент.
То есть, используемых для выполнения запроса, с помощью которого вы читаете из таблицы system.settings.

Столбцы:

```text
name String   - имя настройки
value String  - значение настройки
changed UInt8 - была ли настройка явно задана в конфиге или изменена явным образом
```

Пример:

```sql
SELECT *
FROM system.settings
WHERE changed
```

```text
┌─name───────────────────┬─value───────┬─changed─┐
│ max_threads            │ 8           │       1 │
│ use_uncompressed_cache │ 0           │       1 │
│ load_balancing         │ random      │       1 │
│ max_memory_usage       │ 10000000000 │       1 │
└────────────────────────┴─────────────┴─────────┘
```
