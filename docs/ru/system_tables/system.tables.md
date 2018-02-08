# system.tables

Таблица содержит столбцы database, name, engine типа String.
Также таблица содержит три виртуальных столбца: metadata_modification_time типа DateTime, create_table_query и engine_full типа String.
Для каждой таблицы, о которой знает сервер, будет присутствовать соответствующая запись в таблице system.tables.
Эта системная таблица используется для реализации запросов SHOW TABLES.
