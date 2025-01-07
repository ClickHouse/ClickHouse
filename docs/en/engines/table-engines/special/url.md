---
slug: /en/engines/table-engines/special/url
sidebar_position: 80
sidebar_label:  URL
---

# URL Table Engine

Queries data to/from a remote HTTP/HTTPS server. This engine is similar to the [File](../../../engines/table-engines/special/file.md) engine.

Syntax: `URL(URL [,Format] [,CompressionMethod])`

- The `URL` parameter must conform to the structure of a Uniform Resource Locator. The specified URL must point to a server that uses HTTP or HTTPS. This does not require any additional headers for getting a response from the server.

- The `Format` must be one that ClickHouse can use in `SELECT` queries and, if necessary, in `INSERTs`. For the full list of supported formats, see [Formats](../../../interfaces/formats.md#formats).

    If this argument is not specified, ClickHouse detects the format automatically from the suffix of the `URL` parameter. If the suffix of `URL` parameter does not match any supported formats, it fails to create table. For example, for engine expression `URL('http://localhost/test.json')`, `JSON` format is applied.

- `CompressionMethod` indicates that whether the HTTP body should be compressed. If the compression is enabled, the HTTP packets sent by the URL engine contain 'Content-Encoding' header to indicate which compression method is used.

To enable compression, please first make sure the remote HTTP endpoint indicated by the `URL` parameter supports corresponding compression algorithm.

The supported `CompressionMethod` should be one of following:
- gzip or gz
- deflate
- brotli or br
- lzma or xz
- zstd or zst
- lz4
- bz2
- snappy
- none
- auto

If `CompressionMethod` is not specified, it defaults to `auto`. This means ClickHouse detects compression method from the suffix of `URL` parameter automatically. If the suffix matches any of compression method listed above, corresponding compression is applied or there won't be any compression enabled.

For example, for engine expression `URL('http://localhost/test.gzip')`, `gzip` compression method is applied, but for `URL('http://localhost/test.fr')`, no compression is enabled because the suffix `fr` does not match any compression methods above.

## Usage {#using-the-engine-in-the-clickhouse-server}

`INSERT` and `SELECT` queries are transformed to `POST` and `GET` requests,
respectively. For processing `POST` requests, the remote server must support
[Chunked transfer encoding](https://en.wikipedia.org/wiki/Chunked_transfer_encoding).

You can limit the maximum number of HTTP GET redirect hops using the [max_http_get_redirects](../../../operations/settings/settings.md#setting-max_http_get_redirects) setting.

## Example {#example}

**1.** Create a `url_engine_table` table on the server :

``` sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** Create a basic HTTP server using the standard Python 3 tools and
start it:

``` python3
from http.server import BaseHTTPRequestHandler, HTTPServer

class CSVHTTPServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/csv')
        self.end_headers()

        self.wfile.write(bytes('Hello,1\nWorld,2\n', "utf-8"))

if __name__ == "__main__":
    server_address = ('127.0.0.1', 12345)
    HTTPServer(server_address, CSVHTTPServer).serve_forever()
```

``` bash
$ python3 server.py
```

**3.** Request data:

``` sql
SELECT * FROM url_engine_table
```

``` text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

## Details of Implementation {#details-of-implementation}

- Reads and writes can be parallel
- Not supported:
    - `ALTER` and `SELECT...SAMPLE` operations.
    - Indexes.
    - Replication.

## PARTITION BY

`PARTITION BY` — Optional.  It is possible to create separate files by partitioning the data on a partition key. In most cases, you don't need a partition key, and if it is needed you generally don't need a partition key more granular than by month. Partitioning does not speed up queries (in contrast to the ORDER BY expression). You should never use too granular partitioning. Don't partition your data by client identifiers or names (instead, make client identifier or name the first column in the ORDER BY expression).

For partitioning by month, use the `toYYYYMM(date_column)` expression, where `date_column` is a column with a date of the type [Date](/docs/en/sql-reference/data-types/date.md). The partition names here have the `"YYYYMM"` format.

## Virtual Columns {#virtual-columns}

- `_path` — Path to the `URL`. Type: `LowCardinalty(String)`.
- `_file` — Resource name of the `URL`. Type: `LowCardinalty(String)`.
- `_size` — Size of the resource in bytes. Type: `Nullable(UInt64)`. If the size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_headers` - HTTP response headers. Type: `Map(LowCardinality(String), LowCardinality(String))`.

## Storage Settings {#storage-settings}

- [engine_url_skip_empty_files](/docs/en/operations/settings/settings.md#engine_url_skip_empty_files) - allows to skip empty files while reading. Disabled by default.
- [enable_url_encoding](/docs/en/operations/settings/settings.md#enable_url_encoding) - allows to enable/disable decoding/encoding path in uri. Enabled by default.
