---
description: 'Framing formats multiplex data, totals, extremes, progress, profile events, and server logs in a single response stream over HTTP'
sidebar_label: 'Framing formats'
sidebar_position: 22
slug: /interfaces/framing-formats
title: 'Framing formats'
doc_type: 'reference'
---

# Framing formats {#framing-formats}

A framing format multiplexes different response parts of the query in a single stream: chunks of data, totals and extremes, progress packets, profile events (metrics), and server logs - everything that the native protocol supports. This allows rich data exchange in the HTTP protocol.

Framing formats are independent of [output formats](/interfaces/formats): they encapsulate bytes produced by any output format, by separating and potentially encoding these chunks of bytes. The concatenation of the payloads of all `data`, `totals`, and `extremes` packets is exactly what the output format would have produced without framing. Auxiliary packets (progress, logs, profile events, exceptions) are represented as JSON.

Framing can also make an output format more expressive - this is the one deliberate exception to the rule above. The `JSONCompactEachRow` family of formats drops totals and extremes in its plain output, because their rows would be indistinguishable from ordinary data rows. Under a framing format the packet kind tells them apart, so these formats emit totals and extremes rows (in their usual row syntax) into the `totals` and `extremes` packets. For these formats the concatenation of the payloads of the `data` packets alone is exactly what the output format would have produced without framing, and the `totals` and `extremes` packets carry additional rows that the unframed output does not contain - so a client that reconstructs the unframed output from such a stream should concatenate only the `data` payloads.

The framing format is selected by the query-level setting `framing_output_format`. It currently applies to the HTTP protocol and is ignored for other interfaces.

Server logs are included as packets if the `send_logs_level` setting is set. Profile events are included if the `send_profile_events` setting is enabled (default). Progress and profile events packets are sent at most once in `interactive_delay` microseconds.

A successful stream ends with a final `progress` packet carrying the final counters (`result_rows`, `result_bytes`, `memory_usage`), like the final progress packet of the native protocol. These counters are known only after the query has finished, so no earlier `progress` packet carries them. The final `progress` packet is written after the trailing `log` and `profile_events` packets emitted by the query-finish logging (for example the "peak memory usage" log entry), so it is really the last packet of the stream. On failure, the `exception` packet is the last packet instead.

Anything a query enables only through its own `SETTINGS` clause - a framing format, `send_logs_level`, or `send_profile_events` - is not known until the query has been parsed, so the corresponding logs and profile events are captured only from query execution onwards. The logs and profile events of the parse, plan, and analysis phase are captured only when the setting comes from the session or the URL. In particular, a query that fails during analysis (before pipeline execution) - for example a reference to an unknown table - and enables `send_logs_level` only in its `SETTINGS` clause delivers just the `exception` packet, not the analysis-phase logs. Set `send_logs_level` on the session or the URL to capture those.

The same late-discovery caveat applies to `send_logs_source_regexp`: the log queue filters entries by source at the moment each entry is captured, so a regexp set only in the query's own `SETTINGS` clause takes effect from query execution onwards. The `log` packets of the parse, plan, and analysis phase are filtered by the session or URL value of the setting - they are unfiltered when it is not set there - so they may include sources that do not match the query-level regexp. Conversely, entries dropped by a narrower session or URL regexp are gone and are not recovered by a broader query-level one. Set `send_logs_source_regexp` on the session or the URL to filter the whole query lifecycle.

If an exception happens during query execution, it is sent as an `exception` packet (the last packet of the stream), regardless of the `http_write_exception_in_output_format` setting, so the client can always parse the response as a stream of packets.

There is one exception to this: if a packet write itself fails partway through (for example the connection is broken after some bytes of a packet have already reached the client), the framing fails closed and the stream is terminated without a final `exception` packet. It never retries a half-written packet, because re-emitting it would append a duplicate after the truncated bytes and corrupt the stream. In that situation the client observes a truncated response and an aborted HTTP connection rather than a well-formed terminal packet.

A framing format is applied to queries that produce no result stream as well - a successful `INSERT`, a DDL query, or any other query without output. Such a response carries no `data` packets, but still switches the response `Content-Type` to the framing format and streams the `progress`, `log`, and `profile_events` packets, matching the native protocol. The stream ends with a final `progress` packet carrying the final counters (for example `result_rows` and `result_bytes` with the number of written rows for an `INSERT`). Because no payload is formatted, the output format is irrelevant for such queries and does not affect the framed stream.

## Available framing formats {#available-framing-formats}

| Name                                              | Description                                                                            |
|---------------------------------------------------|----------------------------------------------------------------------------------------|
| [`None`](#framing-format-none)                                   | No framing: everything works as it is by default.                                       |
| [`EventStream`](#framing-format-eventstream)                     | HTTP server-sent events (`text/event-stream`).                                          |
| [`JSONEachPacketBase64`](#framing-format-jsoneachpacket)         | A JSON object per packet; the formatted data is base64-encoded.                         |
| [`JSONEachPacketString`](#framing-format-jsoneachpacket)         | A JSON object per packet; the formatted data is put into a JSON string.                 |

## None {#framing-format-none}

The default. Transparently routes everything applicable (data, totals, extremes, progress) to the output format, and ignores everything that is not applicable (metrics, logs). So everything works as it is by default, including formats that represent progress themselves, such as `JSONEachRowWithProgress`.

## EventStream {#framing-format-eventstream}

Frames packets as [HTTP server-sent events](https://html.spec.whatwg.org/multipage/server-sent-events.html) and sets the `Content-Type` of the response to `text/event-stream`. Every packet is sent as an event named after the packet kind: `data`, `totals`, `extremes`, `progress`, `log`, `profile_events`, `exception`. The bytes produced by the output format become the `data` fields of the event, one field per line of the payload (per the specification, the client joins consecutive `data` fields with a newline). Progress and other auxiliary packets are sent as JSON.

Because the client reconstructs the payload by joining the `data` fields with a newline and then stripping a single trailing newline, a payload that ends with a newline (the common case for line-based formats such as `JSONEachRow`, `TSV`, or `CSV`) is followed by an extra empty `data:` field, so the trailing newline survives the reconstruction. As a result, the concatenation of the reconstructed payloads of the `data`, `totals`, and `extremes` packets is exactly what the output format would have produced without framing.

Server-sent events is a text protocol that treats line breaks (including carriage returns, `\r`) as field delimiters, so a payload cannot be embedded as text if it may contain arbitrary bytes or raw carriage returns. `EventStream` base64-encodes the `data`, `totals`, and `extremes` payloads (each into a single `data:` field) in these cases:

- output formats that may produce non-UTF-8 bytes: binary formats such as `Native` or `RowBinary`, raw passthrough formats such as `RawBLOB` or `TSVRaw`, formats that write a literal setting value verbatim that is itself not valid UTF-8 (`CustomSeparated` with a delimiter such as `format_custom_row_after_delimiter`, or `SQLInsert` with an `output_format_sql_insert_table_name` that contains non-UTF-8 bytes), and formats that write the column names (and data type names) into the header verbatim when a name is not valid UTF-8 - a quoted identifier or an `Enum` element can contain arbitrary bytes (`TSKV`, `SQLInsert`, `Markdown`, `Pretty*`, `Vertical`, and the `*WithNames`/`*WithNamesAndTypes` variants of `TSV`, `CSV`, and `CustomSeparated`; the plain variants of the line-based formats write no header and are unaffected). The `CSV`-family headers flatten a `Tuple` column into its leaf fields (dotted names such as `t.a`, `t.b`) when `output_format_csv_header_serialize_tuple_into_separate_columns` is enabled (the default), so the flattened header names are validated too - a named `Tuple` element with non-UTF-8 bytes is detected even though that name never appears at the top level. The JSON output formats that emit header-derived names without UTF-8 validation when `output_format_json_validate_utf8 = 0` (the default) are covered the same way: `JSONEachRow` (and its aliases) and `JSONColumns` write the column names as object keys, `JSONObjectEachRow` writes them as the inner object keys (except the column selected by `format_json_object_each_row_column_for_object_name`, whose name is not emitted), the `JSONCompactEachRowWithNames`/`WithNamesAndTypes` variants write a header row of names (and type names), and `GeoJSON` writes the property column names as keys - so a non-UTF-8 name makes the framed JSON non-textual and is detected from the header (with UTF-8 validation on, or for the plain `JSONCompactEachRow` that writes no header, the output is textual and accepted). The full-document JSON formats (`JSON`, `JSONStrings`, `JSONCompact`, `JSONCompactStrings`, `JSONColumnsWithMetadata`) always validate the column names, but they also serialize the data type names into the `meta.type` fields, and those type names are only validated when at least one column's value type may itself emit invalid UTF-8; when every value type is guaranteed valid UTF-8, a non-UTF-8 named `Tuple` element or `Enum` value leaks into `meta.type`, which is likewise detected from the header. `XML` has the same carrier: it serializes the column names and type names into the `<name>` / `<type>` metadata elements through XML escaping only, and its UTF-8 validation is installed under the same condition, so a non-UTF-8 name or type name is likewise detected from the header;
- output formats that may emit raw carriage returns from the data: `CSV` (the CSV quoting passes `\r` in a `String` value through verbatim), `XML`, `Pretty`, `Vertical`, `Prometheus` (they write values without escaping `\r`), `SQLInsert` (the `output_format_sql_insert_table_name` setting and the column names are written verbatim), and `Markdown` with `output_format_markdown_escape_special_characters` enabled;
- output formats that may emit raw carriage returns because of the settings: `TSV` with `output_format_tsv_crlf_end_of_line` enabled (the `\r\n` row terminator), and `CustomSeparated` with a `CSV` or `XML` escaping rule or with delimiters containing `\r`.

When the payloads are base64-encoded, `EventStream` signals it by adding a `payload=base64` parameter to the `Content-Type` (`text/event-stream; charset=UTF-8; payload=base64`). The client then base64-decodes those payloads; the concatenation of the decoded payloads is exactly what the output format would have produced without framing. The auxiliary JSON packets (`progress`, `log`, `profile_events`, `exception`) are never encoded. Text output formats without raw carriage returns are still embedded as plain text (no `payload=base64`).

Byte-exact transport of arbitrary binary values is only guaranteed for the base64-encoded payloads. Escaping-based text formats (for example `TSV`, or the JSON formats with `output_format_json_validate_utf8 = 0`, the default) pass non-UTF-8 bytes of `String` values through verbatim; such payloads are embedded as plain text, and a client that decodes the stream as UTF-8 (for example a browser `EventSource`) replaces the invalid sequences. This matches the plain HTTP response of these formats, which declares `charset=UTF-8` just the same.

The `*WithProgress` output formats (`JSONEachRowWithProgress`, `JSONCompactEachRowWithProgress`) write progress as in-band rows that are part of their own output. A framing format delivers progress as separate `progress` packets instead, so it is not compatible with these output formats and rejects them - use the base output format (for example `JSONEachRow`) with framing, or the `None` framing with a `*WithProgress` format.

```bash
curl "http://localhost:8123/?framing_output_format=EventStream" -d "SELECT number FROM numbers(3) FORMAT JSONEachRow"
```

```text
event: data
data: {"number":"0"}
data: {"number":"1"}
data: {"number":"2"}
data: 

event: profile_events
data: [{"host_name":"localhost","current_time":"2026-07-11 00:00:00","thread_id":"0","type":"increment","name":"SelectedRows","value":"3"},{"host_name":"localhost","current_time":"2026-07-11 00:00:00","thread_id":"0","type":"increment","name":"SelectedBytes","value":"24"}]

event: progress
data: {"read_rows":"3","read_bytes":"24","total_rows_to_read":"3","result_rows":"3","result_bytes":"24","elapsed_ns":"1174415"}

```

`EventStream` integrates with the HTTP protocol and throws an exception when it is not applicable.

## JSONEachPacketBase64 and JSONEachPacketString {#framing-format-jsoneachpacket}

Every packet is a JSON object on a separate line (newline-delimited JSON, `application/x-ndjson`), containing the info about the packet. The bytes produced by the output format are put into the `data` field: base64-encoded in `JSONEachPacketBase64` (suitable for binary output formats), or as a JSON string in `JSONEachPacketString`.

Because `JSONEachPacketString` puts the payload bytes into a JSON string, it is meant for output formats that produce valid UTF-8 text. `String` and `FixedString` columns can hold arbitrary bytes, so text output formats such as `JSONEachRow`, `TSV` or `CSV` may emit invalid UTF-8 for such values - just as ClickHouse's own `JSONEachRow` does with the default `output_format_json_validate_utf8 = 0` - and in that case the resulting JSON string, and therefore the whole NDJSON stream, is not guaranteed to be valid UTF-8. `JSONEachPacketString` does not validate or re-encode the payload; use `JSONEachPacketBase64` for byte-exact transport of arbitrary bytes.

```bash
curl "http://localhost:8123/?framing_output_format=JSONEachPacketString" -d "SELECT number FROM numbers(3) FORMAT JSONEachRow"
```

```text
{"packet":"data","data":"{\"number\":\"0\"}\n{\"number\":\"1\"}\n{\"number\":\"2\"}\n"}
{"packet":"profile_events","profile_events":[{"host_name":"localhost","current_time":"2026-07-11 00:00:00","thread_id":"0","type":"increment","name":"SelectedRows","value":"3"}]}
{"packet":"progress","progress":{"read_rows":"3","read_bytes":"24","total_rows_to_read":"3","result_rows":"3","result_bytes":"24","elapsed_ns":"1265958"}}
```

With `JSONEachPacketBase64`, the same `data` packet looks like:

```text
{"packet":"data","data":"eyJudW1iZXIiOiIwIn0KeyJudW1iZXIiOiIxIn0KeyJudW1iZXIiOiIyIn0K"}
```

## Packet kinds {#framing-format-packet-kinds}

| Packet           | Contents                                                                                                       |
|------------------|----------------------------------------------------------------------------------------------------------------|
| `data`           | Bytes produced by the output format for the main result (including the format prefix and suffix).               |
| `totals`         | Bytes produced by the output format for the totals row (`WITH TOTALS`).                                         |
| `extremes`       | Bytes produced by the output format for the extremes (the `extremes` setting).                                  |
| `progress`       | Query progress as JSON: `read_rows`, `read_bytes`, `total_rows_to_read`, `result_rows`, `result_bytes`, `elapsed_ns`, `memory_usage` (zero fields are omitted). |
| `log`            | A server log entry as JSON: `event_time`, `host_name`, `query_id`, `thread_id`, `priority`, `source`, `text`.   |
| `profile_events` | An array of profile events as JSON: `host_name`, `current_time`, `thread_id`, `type` (`increment` or `gauge`), `name`, `value`. |
| `exception`      | The exception message as JSON.                                                                                   |

Unlike the `data`, `totals`, and `extremes` payloads (see the byte-exactness notes above), the string fields of the auxiliary packets (`query_id`, `text`, and `source` of `log`, `name` of `profile_events`, and the `exception` message) have no base64 escape hatch, and some of them (for example `query_id`, which is taken from the query) can hold arbitrary bytes. These fields are always sanitized to valid UTF-8, replacing invalid sequences with the replacement character (`U+FFFD`), so the auxiliary packets are always valid JSON.

Processing of multiple queries at once is not implemented yet, but the design allows it: every packet can be extended with the information about the query index along multiple queries.
