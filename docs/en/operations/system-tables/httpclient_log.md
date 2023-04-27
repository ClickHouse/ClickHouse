---
slug: /en/operations/system-tables/httpclient_log
---
# httpclient_log

The `system.httpclient_log` table is created only if the [httpclient_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-httpclient-log) server setting is specified.

This table contains information about events that HTTP requests are issued from ClickHouse.

The `system.httpclient_log` table contains the following columns:

-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
-   `event_time` ([DateTime64](../../sql-reference/data-types/datetime.md)) — Event time in microseconds.
-   `client` - ([Enum8](../../sql-reference/data-types/enum.md)) — Type of the client that issues the request. Can have one of the following values:
    -   `AWS` — HTTP Client that access S3.
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — Identifier of the query that triggered this event.
-   `trace_id` ([UUID](../../sql-reference/data-types/uuid.md)) — Identifier of the current [trace](opentelemetry_span_log.md) that triggerd this event.
-   `span_id` ([UInt64](../../sql-reference/data-types/string.md)) — Identifier of current [span](opentelemetry_span_log.md) that triggered this event.
-   `duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Duration of this HTTP request.
-   `method` ([Enum8](../../sql-reference/data-types/string.md)) — HTTP method of this request. Can be one of the following values:
    -   `GET` — Current request is a HTTP GET request.
    -   `POST` — Current request is a HTTP POST request.
    -   `DELETE` — Current request is a HTTP DELETE request.
    -   `PUT` — Current request is a HTTP PUT request.
    -   `HEAD` — Current request is a HTTP HEAD request.
    -   `PATCH` — Current request is a HTTP PATCH request.
-   `uri` ([String](../../sql-reference/data-types/string.md)) — The uri that this request accessed.
-   `status` ([Int32](../../sql-reference/data-types/int-uint.md)) — The HTTP response status code this request.
-   `request_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The body size of this request.
-   `response_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The body size of the resposne of this request.
-   `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — The exception code of the occured error.
-   `exception` ([String](../../sql-reference/data-types/string.md)) — Text message of the occurred error.

The `system.httpclient_log` table is created after the first inserting data to the `MergeTree` table.

**Example**

``` sql
SELECT * FROM system.httpclient_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
event_date:     2023-03-15
event_time:     2023-03-15 17:19:12.631542
client:         AWS
query_id:       f0f26eb4-8e83-4b6c-85c0-3b3a0ea2ec59
trace_id:
span_id:        0
duration_ms:    2121
method:         GET
uri:            https://ch-nyc-taxi.s3.eu-west-3.amazonaws.com/tsv/trips_1.tsv.gz
status:         206
request_size:   0
response_size:  10485760
exception_code: 0
exception:
```
