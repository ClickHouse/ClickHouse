# system.detached_parts {#system_tables-detached_parts}

Содержит информацию об отсоединённых кусках таблиц семейства [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). Столбец `reason` содержит причину, по которой кусок был отсоединён. Для кусов, отсоединённых пользователем, `reason` содержит пустую строку.
Такие куски могут быть присоединены с помощью [ALTER TABLE ATTACH PARTITION\|PART](../../sql_reference/alter/#alter_attach-partition). Остальные столбцы описаны в [system.parts](#system_tables-parts).
Если имя куска некорректно, значения некоторых столбцов могут быть `NULL`. Такие куски могут быть удалены с помощью [ALTER TABLE DROP DETACHED PART](../../sql_reference/alter/#alter_drop-detached).

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/detached_parts) <!--hide-->
