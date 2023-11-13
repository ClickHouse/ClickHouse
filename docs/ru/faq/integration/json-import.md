---
slug: /ru/faq/integration/json-import
title: Как импортировать JSON в ClickHouse?
sidebar_position: 11
---

# Как импортировать JSON в ClickHouse? {#how-to-import-json-into-clickhouse}

ClickHouse поддерживает широкий спектр [входных и выходных форматов данных](../../interfaces/formats.md). Среди них есть множество вариаций JSON, но чаще всего для импорта данных используют [JSONEachRow](../../interfaces/formats.md#jsoneachrow): один JSON-объект в строке, каждый объект с новой строки.

## Примеры {#examples}

С помощью [HTTP-интерфейса](../../interfaces/http.md):

``` bash
$ echo '{"foo":"bar"}' | curl 'http://localhost:8123/?query=INSERT%20INTO%20test%20FORMAT%20JSONEachRow' --data-binary @-
```

При помощи [интефейса CLI](../../interfaces/cli.md):

``` bash
$ echo '{"foo":"bar"}'  | clickhouse-client --query="INSERT INTO test FORMAT JSONEachRow"
```

Чтобы не вставлять данные вручную, используйте одну из [готовых библиотек](../../interfaces/index.md).

## Полезные настройки {#useful-settings}

-   `input_format_skip_unknown_fields` позволяет импортировать JSON, даже если он содержит дополнительные поля, которых нет в таблице (отбрасывая лишние поля).
-   `input_format_import_nested_json` позволяет импортировать вложенные JSON-объекты в столбцы типа [Nested](../../sql-reference/data-types/nested-data-structures/nested.md).

:::note Примечание
В HTTP-интерфейсе настройки передаются через параметры `GET` запроса, в `CLI` interface — как дополнительные аргументы командной строки, начинающиеся с `--`.
:::
