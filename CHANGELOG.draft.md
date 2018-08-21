## RU

## ClickHouse release 18.10.3, 2018-08-13

### Новые возможности:
* поддержка межсерверной репликации по HTTPS
* MurmurHash
* ODBCDriver2 с поддержкой NULL-ов
* поддержка UUID в ключевых колонках (экспериментально)

### Улучшения:
* добавлена поддержка SETTINGS для движка Kafka
* поддежка пустых кусков после мержей в движках Summing, Collapsing and VersionedCollapsing
* удаление старых записей о полностью выполнившихся мутациях
* исправлена логика REPLACE PARTITION для движка RplicatedMergeTree
* добавлена системная таблица system.merge_tree_settings
* в системную таблицу system.tables добавлены столбцы зависимостей: dependencies_database и dependencies_table
* заменен аллокатор, теперь используется jemalloc вместо tcmalloc
* улучшена валидация connection string ODBC
* удалена поддержка CHECK TABLE для распределенных таблиц
* добавлены stateful тесты (пока без данных)
* добавлена опция конфига max_partition_size_to_drop
* добавлена настройка output_format_json_escape_slashes
* добавлена настройка max_fetch_partition_retries_count
* добавлена настройка prefer_localhost_replica
* добавлены libressl, unixodbc и mariadb-connector-c как сабмодули

### Исправление ошибок:
* #2786
* #2777
* #2795
