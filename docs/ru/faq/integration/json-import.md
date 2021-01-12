---
title: How to import JSON into ClickHouse?
toc_hidden: true
toc_priority: 11
---

# Как импортировать JSON в ClickHouse? {#how-to-import-json-into-clickhouse}

ClickHouse поддерживаем широкий спектр [форматов данных на входе и выходе](../../interfaces/formats.md). Среди них есть множество вариаци JSON, но чаще всего для импорта данных используют [JSONeachRow](../../interfaces/formats.md#jsoneachrow): один JSON-объект в строке, между ними пустая строка.

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

-   `input_format_skip_unknown_fields` позволяет вставить JSON, даже если есть дополнительные поля, которые не были представлены в табличной схеме (отбрасывая их).
-   `input_format_import_nested_json` позволяет вставить вложенные JSON-объекты в столбцы типа [Nested](../../sql-reference/data-types/nested-data-structures/nested.md).

!!! note "Примечание"
    Настройки обозначаются как параметры `GET` для HTTP-интерфейса или как дополнительные аргументы командной строки `CLI` interface, начинающиеся с `--`.
