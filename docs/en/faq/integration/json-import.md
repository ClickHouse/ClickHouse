---
title: How to import JSON into ClickHouse?
toc_hidden: true
toc_priority: 11
---

# How to Import JSON Into ClickHouse? {#how-to-import-json-into-clickhouse}

ClickHouse supports a wide range of [data formats for input and output](../../interfaces/formats.md). There are multiple JSON variations among them, but the most commonly used for data ingestion is [JSONEachRow](../../interfaces/formats.md#jsoneachrow). It expects one JSON object per row, each object separated by a newline.

## Examples {#examples}

Using [HTTP interface](../../interfaces/http.md):

``` bash
$ echo '{"foo":"bar"}' | curl 'http://localhost:8123/?query=INSERT%20INTO%20test%20FORMAT%20JSONEachRow' --data-binary @-
```

Using [CLI interface](../../interfaces/cli.md):

``` bash
$ echo '{"foo":"bar"}'  | clickhouse-client ---query="INSERT INTO test FORMAT JSONEachRow"
```

Instead of inserting data manually, you might consider to use one of [client libraries](../../interfaces/index.md) instead.

## Useful Settings {#useful-settings}

-   `input_format_skip_unknown_fields` allows to insert JSON even if there were additional fields not present in table schema (by discarding them).
-   `input_format_import_nested_json` allows to insert nested JSON objects into columns of [Nested](../../sql-reference/data-types/nested-data-structures/nested.md) type.

!!! note "Note"
    Settings are specified as `GET` parameters for the HTTP interface or as additional command-line arguments prefixed with `--` for the `CLI` interface.
