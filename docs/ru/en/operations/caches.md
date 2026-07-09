---
description: 'При выполнении запросов ClickHouse использует разные кэши.'
sidebar_label: 'Кэши'
sidebar_position: 65
slug: /operations/caches
title: 'Типы кэшей'
keywords: ['кэш']
doc_type: 'reference'
---

При выполнении запросов ClickHouse использует разные кэши, чтобы ускорить обработку запросов
и сократить количество чтений с диска и записей на диск.

Основные типы кэшей:

* `mark_cache` — кэш [меток](/ru/development/architecture#merge-tree), используемый движками таблиц семейства [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md).
* `uncompressed_cache` — кэш несжатых данных, используемый движками таблиц семейства [`MergeTree`](../engines/table-engines/mergetree-family/mergetree.md).
* Кэш страниц операционной системы (используется косвенно, для файлов с самими данными).

Также существует множество дополнительных типов кэшей:

* DNS-кэш.
* Кэш [Regexp](/ru/interfaces/formats/Regexp).
* Кэш скомпилированных выражений.
* Кэш [индекса векторного сходства](../engines/table-engines/mergetree-family/annindexes.md).
* Кэш [текстового индекса](../engines/table-engines/mergetree-family/textindexes.md#caching).
* Кэш схем [формата Avro](/ru/interfaces/formats/Avro).
* Кэш данных [Dictionaries](../sql-reference/statements/create/dictionary/overview.md).
* Кэш определения схемы.
* [Файловый кэш](storing-data.md) для S3, Azure, Local и других дисков.
* [Кэш страниц в пространстве пользователя](/ru/operations/userspace-page-cache)
* [Кэш запросов](query-cache.md).
* [Кэш условий запроса](query-condition-cache.md).
* Кэш схем форматов.

Если вам нужно очистить один из кэшей — для настройки производительности, устранения неполадок или обеспечения согласованности данных —
вы можете использовать оператор [`SYSTEM CLEAR ... CACHE`](../sql-reference/statements/system.md).