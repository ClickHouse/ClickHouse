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

If an exception happens during query execution, it is sent as an `exception` packet (the last packet of the stream), regardless of the `http_write_exception_in_output_format` setting, so the client can always parse the response as a stream of packets.

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

- output formats that may produce non-UTF-8 bytes: binary formats such as `Native` or `RowBinary`, raw passthrough formats such as `RawBLOB` or `TSVRaw`, and formats that write a literal setting value verbatim that is itself not valid UTF-8 (`CustomSeparated` with a delimiter such as `format_custom_row_after_delimiter`, or `SQLInsert` with an `output_format_sql_insert_table_name` that contains non-UTF-8 bytes);
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

event: progress
data: {"read_rows":"3","read_bytes":"24","total_rows_to_read":"3","result_rows":"3","result_bytes":"24","elapsed_ns":"1174415"}

event: profile_events
data: [{"host_name":"localhost","current_time":"2026-07-11 00:00:00","thread_id":"0","type":"increment","name":"SelectedRows","value":"3"},{"host_name":"localhost","current_time":"2026-07-11 00:00:00","thread_id":"0","type":"increment","name":"SelectedBytes","value":"24"}]

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
{"packet":"progress","progress":{"read_rows":"3","read_bytes":"24","total_rows_to_read":"3","result_rows":"3","result_bytes":"24","elapsed_ns":"1265958"}}
{"packet":"profile_events","profile_events":[{"host_name":"localhost","current_time":"2026-07-11 00:00:00","thread_id":"0","type":"increment","name":"SelectedRows","value":"3"}]}
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
