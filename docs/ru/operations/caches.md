---
sidebar_position: 65
sidebar_label: Кеши
---

# Типы кеша {#cache-types}

При выполнении запросов ClickHouse использует различные типы кеша.

Основные типы кеша:

- `mark_cache` — кеш засечек, используемых движками таблиц семейства [MergeTree](../engines/table-engines/mergetree-family/mergetree.md).
- `uncompressed_cache` — кеш несжатых данных, используемых движками таблиц семейства [MergeTree](../engines/table-engines/mergetree-family/mergetree.md).

Дополнительные типы кеша:

- DNS-кеш.
- Кеш данных формата [regexp](../interfaces/formats.md#data-format-regexp).
- Кеш скомпилированных выражений.
- Кеш схем формата [Avro](../interfaces/formats.md#data-format-avro).
- Кеш данных в [словарях](../sql-reference/dictionaries/index.md).

Непрямое использование:

- Кеш страницы ОС.

Чтобы очистить кеш, используйте выражение [SYSTEM DROP ... CACHE](../sql-reference/statements/system.md).

