---
title: How to import JSON into ClickHouse?
toc_hidden: true
toc_priority: 11
---

# Как импортировать JSON в ClickHouse? {#how-to-import-json-into-clickhouse}

ClickHouse supports a wide range of [data formats for input and output](../../interfaces/formats.md). There are multiple JSON variations among them, but the most commonly used for data ingestion is [JSONEachRow](../../interfaces/formats.md#jsoneachrow). It expects one JSON object per row, each object separated by a newline.

## Примеры {#examples}

С помощью [HTTP-интерфейса](../../interfaces/http.md):

``` bash
$ echo '{"foo":"bar"}' | curl 'http://localhost:8123/?query=INSERT%20INTO%20test%20FORMAT%20JSONEachRow' --data-binary @-
```

При помощи [интефейса CLI](../../interfaces/cli.md):

``` bash
$ echo '{"foo":"bar"}'  | clickhouse-client ---query="INSERT INTO test FORMAT JSONEachRow"
```

Вместо того, чтобы вставлять данные вручную, модете использовать одну из [готовых библиотек](../../interfaces/index.md).

## Полезные настройки {#useful-settings}

-   `input_format_skip_unknown_fields` позволяет вставить JSON даже если есть дополнительные поля, которые не были представлены в табличной схеме (отбрасывая их).
-   `input_format_import_nested_json` позволяет вставить вложенные JSON-объекты в столбцы типа [Nested](../../sql-reference/data-types/nested-data-structures/nested.md).

!!! note "Note"
    Settings are specified as `GET` parameters for the HTTP interface or as additional command-line arguments prefixed with `--` for the `CLI` interface.
