---
sidebar_label: FORMAT
---

# FORMAT Clause {#format-clause}

ClickHouse supports a wide range of [serialization formats](../../../interfaces/formats.md) that can be used on query results among other things. There are multiple ways to choose a format for `SELECT` output, one of them is to specify `FORMAT format` at the end of query to get resulting data in any specific format.

Specific format might be used either for convenience, integration with other systems or performance gain.

## Default Format {#default-format}

If the `FORMAT` clause is omitted, the default format is used, which depends on both the settings and the interface used for accessing the ClickHouse server. For the [HTTP interface](../../../interfaces/http.md) and the [command-line client](../../../interfaces/cli.md) in batch mode, the default format is `TabSeparated`. For the command-line client in interactive mode, the default format is `PrettyCompact` (it produces compact human-readable tables).

## Implementation Details {#implementation-details}

When using the command-line client, data is always passed over the network in an internal efficient format (`Native`). The client independently interprets the `FORMAT` clause of the query and formats the data itself (thus relieving the network and the server from the extra load).
