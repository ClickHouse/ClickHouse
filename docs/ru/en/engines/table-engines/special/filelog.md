---
description: 'Этот движок позволяет обрабатывать файлы журналов приложений как поток
  записей.'
sidebar_label: 'FileLog'
sidebar_position: 160
slug: /engines/table-engines/special/filelog
title: 'Движок таблицы FileLog'
doc_type: 'reference'
---

Этот движок позволяет обрабатывать файлы журналов приложений как поток записей.

`FileLog` позволяет:

* Подписываться на файлы журналов.
* Обрабатывать новые записи по мере их добавления в отслеживаемые файлы журналов.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = FileLog('path_to_logs', 'format_name') SETTINGS
    [poll_timeout_ms = 0,]
    [poll_max_batch_size = 0,]
    [max_block_size = 0,]
    [max_threads = 0,]
    [poll_directory_watch_events_backoff_init = 500,]
    [poll_directory_watch_events_backoff_max = 32000,]
    [poll_directory_watch_events_backoff_factor = 2,]
    [handle_error_mode = 'default']
```

Аргументы движка:

* `path_to_logs` – Путь к файлам журнала, которые нужно отслеживать. Это может быть путь к каталогу с файлами журнала или к одному файлу журнала. Обратите внимание, что ClickHouse разрешает только пути внутри каталога `user_files`.
* `format_name` - Формат записей. Обратите внимание, что FileLog обрабатывает каждую строку в файле как отдельную запись, поэтому не все форматы данных для него подходят.

Необязательные параметры:

* `poll_timeout_ms` - Тайм-аут одного опроса файла журнала. По умолчанию: [stream&#95;poll&#95;timeout&#95;ms](../../../operations/settings/settings.md#stream_poll_timeout_ms).
* `poll_max_batch_size` — Максимальное количество записей, получаемых за один опрос. По умолчанию: [max&#95;block&#95;size](/ru/operations/settings/settings#max_block_size).
* `max_block_size` — Максимальный размер батча (в записях) для одного опроса. По умолчанию: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size).
* `max_threads` - Максимальное количество потоков для разбора файлов; по умолчанию равно 0, что означает, что число будет равно max(1, physical&#95;cpu&#95;cores / 4).
* `poll_directory_watch_events_backoff_init` - Начальное значение паузы для потока наблюдения за каталогом. По умолчанию: `500`.
* `poll_directory_watch_events_backoff_max` - Максимальное значение паузы для потока наблюдения за каталогом. По умолчанию: `32000`.
* `poll_directory_watch_events_backoff_factor` - Коэффициент увеличения задержки; по умолчанию используется экспоненциальный рост. По умолчанию: `2`.
* `handle_error_mode` — Как обрабатывать ошибки в движке FileLog. Возможные значения: default (если не удаётся разобрать сообщение, будет сгенерировано исключение), stream (сообщение об исключении и исходное сообщение будут сохранены в виртуальных столбцах `_error` и `_raw_message`).

<div id="description">
  ## Описание
</div>

Доставленные записи отслеживаются автоматически, поэтому каждая запись в файле журнала учитывается только один раз.

`SELECT` не слишком полезен для чтения записей (кроме отладки), поскольку каждую запись можно прочитать только один раз. Гораздо практичнее создавать потоки в реальном времени с помощью [materialized views](../../../sql-reference/statements/create/view.md). Для этого:

1. Используйте движок для создания таблицы FileLog и рассматривайте её как поток данных.
2. Создайте таблицу с нужной структурой.
3. Создайте materialized view, которое преобразует данные из движка и помещает их в созданную ранее таблицу.

Когда `MATERIALIZED VIEW` подключается к движку, оно начинает собирать данные в фоновом режиме. Это позволяет непрерывно получать записи из файлов журналов и преобразовывать их в требуемый формат с помощью `SELECT`.
Одна таблица FileLog может иметь сколько угодно materialized views; они не читают данные из таблицы напрямую, а получают новые записи (блоками), благодаря чему можно записывать данные в несколько таблиц с разным уровнем детализации (с группировкой — агрегацией — и без неё).

Пример:

```sql
  CREATE TABLE logs (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = FileLog('user_files/my_app/app.log', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM logs GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

Чтобы прекратить получение потоковых данных или изменить логику преобразования, отсоедините materialized view:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

Если вы хотите изменить целевую таблицу с помощью `ALTER`, мы рекомендуем отключить materialized view, чтобы избежать расхождений между целевой таблицей и данными из представления.

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_filename` - Имя файла журнала. Тип данных: `LowCardinality(String)`.
* `_offset` - Смещение в файле журнала. Тип данных: `UInt64`.

Дополнительные виртуальные столбцы при `handle_error_mode='stream'`:

* `_raw_record` - Исходная запись, которую не удалось корректно разобрать. Тип данных: `Nullable(String)`.
* `_error` - Сообщение об исключении, возникшем при ошибке разбора. Тип данных: `Nullable(String)`.

Примечание: виртуальные столбцы `_raw_record` и `_error` заполняются только в случае исключения при разборе; если сообщение успешно разобрано, они всегда имеют значение `NULL`.