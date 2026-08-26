---
description: 'Framing formats multiplex data, totals, extremes, progress, profile events, and server logs in a single response stream over HTTP'
sidebarTitle: 'Framing formats'
slug: /interfaces/framing-formats
title: 'Framing formats'
doc_type: 'reference'
---

A framing format multiplexes different response parts of the query in a single stream: chunks of data, totals and extremes, progress packets, profile events (metrics), and server logs - everything that the native protocol supports. This allows rich data exchange in the HTTP protocol.

Framing formats are independent of [output formats](/reference/formats): they encapsulate bytes produced by any output format, by separating and potentially encoding these chunks of bytes. The concatenation of the payloads of all `data`, `totals`, and `extremes` packets is exactly what the output format would have produced without framing. Auxiliary packets (progress, logs, profile events, exceptions) are represented as JSON.

Framing can also make an output format more expressive - this is the one deliberate exception to the rule above. The `JSONCompactEachRow` family of formats drops totals and extremes in its plain output, because their rows would be indistinguishable from ordinary data rows. Under a framing format the packet kind tells them apart, so these formats emit totals and extremes rows (in their usual row syntax) into the `totals` and `extremes` packets. For these formats the concatenation of the payloads of the `data` packets alone is exactly what the output format would have produced without framing, and the `totals` and `extremes` packets carry additional rows that the unframed output does not contain - so a client that reconstructs the unframed output from such a stream should concatenate only the `data` payloads.

The framing format is selected by the query-level setting `framing_output_format`. It currently applies to the HTTP protocol and is ignored for other interfaces.

Server logs are included as packets if the `send_logs_level` setting is set. Profile events are included if the `send_profile_events` setting is enabled (default). Progress and profile events packets are sent at most once in `interactive_delay` microseconds.

A successful stream ends with a final `progress` packet carrying the final counters (`result_rows`, `result_bytes`, `memory_usage`), like the final progress packet of the native protocol. These counters are known only after the query has finished, so no earlier `progress` packet carries them. The final `progress` packet is written after the trailing `log` and `profile_events` packets emitted by the query-finish logging (for example the "peak memory usage" log entry), so it is really the last packet of the stream. On failure, the `exception` packet is the last packet instead, and the final-counters `progress` packet is not written at all - it is the success terminator of the stream - even when the failure happens after the query itself finished and the final counters were already known (for example, a failure while writing the query log).

Because this tail of the stream is written after the `QueryFinish` entry of `system.query_log` is recorded, the network-send profile events of the query (`NetworkSendBytes`, `NetworkSendElapsedMicroseconds`) do not include sending the trailing packets and closing the response - nor, when the response is buffered (`http_response_buffer_size` or `wait_end_of_query`), sending the buffered response body, which is transmitted only after the query has finished. This matches the native protocol, which sends its trailing logs and profile events after the query log entry as well.

Anything a query enables only through its own `SETTINGS` clause - a framing format, `send_logs_level`, or `send_profile_events` - is not known until the query has been parsed, so the corresponding logs and profile events are captured only from query execution onwards. The logs and profile events of the parse, plan, and analysis phase are captured only when the setting comes from the session or the URL. In particular, a query that fails during analysis (before pipeline execution) - for example a reference to an unknown table - and enables `send_logs_level` only in its `SETTINGS` clause delivers just the `exception` packet, not the analysis-phase logs. Set `send_logs_level` on the session or the URL to capture those.

The same late-discovery caveat applies to `send_logs_source_regexp`: the log queue filters entries by source at the moment each entry is captured, so a regexp set only in the query's own `SETTINGS` clause takes effect from query execution onwards. The `log` packets of the parse, plan, and analysis phase are filtered by the session or URL value of the setting - they are unfiltered when it is not set there - so they may include sources that do not match the query-level regexp. Conversely, entries dropped by a narrower session or URL regexp are gone and are not recovered by a broader query-level one. Set `send_logs_source_regexp` on the session or the URL to filter the whole query lifecycle.

If an exception happens during query execution, it is sent as an `exception` packet (the last packet of the stream), regardless of the `http_write_exception_in_output_format` setting, so the client can always parse the response as a stream of packets. Once the exception is recorded, the output format contributes no more payload bytes: a query that fails before producing any output delivers no `data` packet at all (not even the format's empty document skeleton), and a query that fails mid-stream leaves the concatenated payload truncated at the failure point, without the format's suffix - the payload of a failed query must not look like a complete document.

There is one exception to this: if a packet write itself fails partway through (for example the connection is broken after some bytes of a packet have already reached the client), the framing fails closed and the stream is terminated without a final `exception` packet. It never retries a half-written packet, because re-emitting it would append a duplicate after the truncated bytes and corrupt the stream. In that situation the client observes a truncated response and an aborted HTTP connection rather than a well-formed terminal packet. The same rule applies to a failure while the response stream itself is being closed (flushing the buffered results, finalizing the HTTP compression, closing the socket): by that point some or all of the success stream is already on the wire, so nothing is appended to it - neither an `exception` packet nor the generic HTTP error block - and the client observes a truncated response and an aborted connection. It also applies when the exception delivery itself fails: if writing the terminal `exception` packet fails (for example, while draining the trailing logs) after any part of the packet stream has been produced - whether it has already been transmitted or is still sitting in the server-side response buffers (`http_response_buffer_size`) - the stream is likewise terminated without appending anything, so a plain HTTP error body is never mixed into a partial packet stream. A failure while writing the string fields of the auxiliary `log`, `profile_events` and `exception` packets counts as a half-written packet as well, including a failure to write the last bytes of such a string: the stream then ends with that truncated packet and carries no terminator at all - neither an `exception` packet nor the final-counters `progress` packet - so a client that requires a terminator detects the failure even when the query itself succeeded.

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

Frames packets as [HTTP server-sent events](https://html.spec.whatwg.org/multipage/server-sent-events.html) and sets the `Content-Type` of the response to `text/event-stream; charset=UTF-8; payload=base64`. Every packet is sent as an event named after the packet kind: `data`, `totals`, `extremes`, `progress`, `log`, `profile_events`, `exception`. Progress and other auxiliary packets are sent as JSON.

Server-sent events is a text protocol that treats line breaks (including carriage returns, `\r`) as field delimiters, so the bytes produced by the output format are not embedded verbatim: a block of formatted data is base64-encoded into a single `data:` field, which decodes to the fully formatted payload, with all of its newlines. This is what the `payload=base64` parameter of the `Content-Type` says. The concatenation of the decoded payloads of the `data`, `totals`, and `extremes` packets is exactly what the output format would have produced without framing, byte for byte, for any output format - text, binary (`Native`, `RowBinary`), or raw passthrough (`RawBLOB`, `TSVRaw`) alike.

The auxiliary JSON packets (`progress`, `log`, `profile_events`, `exception`) are never encoded: they are written as a single `data:` field of JSON, which contains no line breaks.

The `*WithProgress` output formats (`JSONEachRowWithProgress`, `JSONCompactEachRowWithProgress`) write progress as in-band rows that are part of their own output. A framing format delivers progress as separate `progress` packets instead, so it is not compatible with these output formats and rejects them - use the base output format (for example `JSONEachRow`) with framing, or the `None` framing with a `*WithProgress` format.

```bash
curl "http://localhost:8123/?framing_output_format=EventStream" -d "SELECT number FROM numbers(3) FORMAT JSONEachRow"
```

```text
event: data
data: eyJudW1iZXIiOiIwIn0KeyJudW1iZXIiOiIxIn0KeyJudW1iZXIiOiIyIn0K

event: profile_events
data: [{"host_name":"localhost","current_time":"2026-07-11 00:00:00","thread_id":"0","type":"increment","name":"SelectedRows","value":"3"},{"host_name":"localhost","current_time":"2026-07-11 00:00:00","thread_id":"0","type":"increment","name":"SelectedBytes","value":"24"}]

event: progress
data: {"read_rows":"3","read_bytes":"24","total_rows_to_read":"3","result_rows":"3","result_bytes":"24","elapsed_ns":"1174415"}

```

`EventStream` integrates with the HTTP protocol and throws an exception when it is not applicable.

## JSONEachPacketBase64 and JSONEachPacketString {#framing-format-jsoneachpacket}

Every packet is a JSON object on a separate line (newline-delimited JSON, `application/x-ndjson`), containing the info about the packet. The bytes produced by the output format are put into the `data` field: base64-encoded in `JSONEachPacketBase64` (suitable for binary output formats), or as a JSON string in `JSONEachPacketString`.

The two variants encode the `data` field differently, so the `Content-Type` of the response tells them apart, as with `EventStream`: `JSONEachPacketBase64` sets `application/x-ndjson; charset=UTF-8; payload=base64`, and `JSONEachPacketString` sets `application/x-ndjson; payload=string`. A client can therefore learn from the response metadata alone whether the `data` field has to be base64-decoded. `charset=UTF-8` is promised only by `JSONEachPacketBase64`, because only base64 encoding makes the whole stream valid UTF-8 regardless of the payload bytes - see below.

Because `JSONEachPacketString` puts the payload bytes into a JSON string, it is meant for output formats that produce valid UTF-8 text. `String` and `FixedString` columns can hold arbitrary bytes, so text output formats such as `JSONEachRow`, `TSV` or `CSV` may emit invalid UTF-8 for such values - just as ClickHouse's own `JSONEachRow` does with the default `output_format_json_validate_utf8 = 0` - and in that case the resulting JSON string, and therefore the whole NDJSON stream, is not guaranteed to be valid UTF-8. `JSONEachPacketString` does not validate or re-encode the payload; use `JSONEachPacketBase64` for byte-exact transport of arbitrary bytes.

Output formats that knowably produce non-UTF-8 bytes are rejected by `JSONEachPacketString` up front with an error, before the query executes: binary formats (`Native`, `RowBinary`), raw passthrough formats (`RawBLOB`, `TSVRaw`), formats that write a non-UTF-8 column name, data type name, or `Tuple` element name from the query header into their output, and configurations whose settings-driven literals are written verbatim by the serializations and are not valid UTF-8 - the `format_csv_delimiter`, `format_tsv_null_representation` / `format_csv_null_representation`, and `bool_true_representation` / `bool_false_representation` settings.

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
