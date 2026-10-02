---
description: 'Queries data to/from a remote HTTP/HTTPS server. This engine is similar
  to the File engine.'
sidebar_label: 'URL'
sidebar_position: 80
slug: /engines/table-engines/special/url
title: 'URL table engine'
doc_type: 'reference'
---

Queries data to/from a remote HTTP/HTTPS server. This engine is similar to the [File](../../../engines/table-engines/special/file.md) engine.

Syntax: `URL(URL [,Format] [,CompressionMethod])`

- The `URL` parameter must conform to the structure of a Uniform Resource Locator. For an `http`/`https` URL (the default backend), it must point to a server that uses HTTP or HTTPS, and getting a response from the server does not require any additional headers. A URL with a recognized non-HTTP scheme (`file://`, `s3://`, `az://`, `hdfs://`, …) is instead delegated to the matching engine — see [Dispatching by URL scheme](#scheme-dispatch) below.

- The `Format` must be one that ClickHouse can use in `SELECT` queries and, if necessary, in `INSERTs`. For the full list of supported formats, see [Formats](/interfaces/formats#formats-overview).

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

## Dispatching by URL scheme {#scheme-dispatch}

The `URL` engine is a unified wrapper on top of the other file- and object-storage engines: it dispatches to the right backend based on the URL scheme. `http`/`https` (and any unrecognized scheme) are served by the `URL` engine itself; `file://` is served by the [File](../../../engines/table-engines/special/file.md) engine; `s3://`, `gs://`, `gcs://`, `oss://` by the [S3](/engines/table-engines/integrations/s3) engine; `az://`, `azure://`, `abfss://`, `abfs://` by the [AzureBlobStorage](/engines/table-engines/integrations/azureBlobStorage) engine; and `hdfs://` by the [HDFS](/engines/table-engines/integrations/hdfs) engine.

Only the S3 schemes that the S3 URI mapper resolves to a concrete endpoint without extra configuration (`s3`, plus `gs`/`gcs`/`oss`) are dispatched. Other S3-compatible vendor schemes (`cos`, `obs`, `eos`, …) are region-specific and have no default endpoint mapping, so passing such a URL to the `URL` engine is treated as an unrecognized scheme and reported as an error; use the [S3](/engines/table-engines/integrations/s3) engine directly (with `url_scheme_mappers` configured) for those backends.

The [url_base](/operations/settings/settings.md#url_base) setting is applied before scheme dispatch, so a relative reference is first resolved against the base and then routed to the matching engine.

```sql
CREATE TABLE file_via_url (a UInt32, b String) ENGINE = URL('file://data.csv', CSV);
CREATE TABLE s3_via_url (a UInt32, b String) ENGINE = URL('s3://bucket/key.csv', CSV);
```

## Usage {#using-the-engine-in-the-clickhouse-server}

`INSERT` and `SELECT` queries are transformed to `POST` and `GET` requests,
respectively. For processing `POST` requests, the remote server must support
[Chunked transfer encoding](https://en.wikipedia.org/wiki/Chunked_transfer_encoding).

You can limit the maximum number of HTTP GET redirect hops using the [max_http_get_redirects](/operations/settings/settings#max_http_get_redirects) setting.

## Wildcards with HTTP index pages {#wildcards-with-http-index-pages}

When [allow_experimental_url_wildcard_from_index_pages](/operations/settings/settings.md#allow_experimental_url_wildcard_from_index_pages) is enabled, the `URL` table engine can expand wildcards by fetching HTTP index pages and extracting links from them.
This is the same mechanism as the [`url`](../../../sql-reference/table-functions/url.md#wildcards-with-http-index-pages) table function.

Expansion is limited by [max_http_index_page_size](/operations/server-configuration-parameters/settings.md#max_http_index_page_size) for each fetched index page and by [url_wildcard_max_directories_to_read](/operations/settings/settings.md#url_wildcard_max_directories_to_read) for recursive directory traversal.

## Example {#example}

**1.** Create a `url_engine_table` table on the server :

```sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** Create a basic HTTP server using the standard Python 3 tools and
start it:

```python3
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

```bash
$ python3 server.py
```

**3.** Request data:

```sql
SELECT * FROM url_engine_table
```

```text
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

## Virtual columns {#virtual-columns}

- `_path` — Path to the `URL`. Type: `LowCardinality(String)`.
- `_file` — Resource name of the `URL`. Type: `LowCardinality(String)`.
- `_size` — Size of the resource in bytes. Type: `Nullable(UInt64)`. If the size is unknown, the value is `NULL`.
- `_time` — Last modified time of the file. Type: `Nullable(DateTime)`. If the time is unknown, the value is `NULL`.
- `_headers` - HTTP response headers. Type: `Map(LowCardinality(String), LowCardinality(String))`.

## Resolving relative URLs {#resolving-relative-urls}

The [url_base](/operations/settings/settings.md#url_base) setting allows using a relative URL in the `URL` engine. When `url_base` is set, the URL passed to the engine is resolved against it per [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986). For a full description of the resolution rules, see the [url table function docs](../../../sql-reference/table-functions/url.md#resolving-relative-urls).

**Example**

```sql
SET url_base = 'http://127.0.0.1:12345/';
CREATE TABLE url_engine_table (word String, value UInt64) ENGINE = URL('hello.csv', CSV);
SELECT * FROM url_engine_table;
```

## Storage settings {#storage-settings}

- [engine_url_skip_empty_files](/operations/settings/settings.md#engine_url_skip_empty_files) - allows to skip empty files while reading. Disabled by default.
- [enable_url_encoding](/operations/settings/settings.md#enable_url_encoding) - allows to enable/disable decoding/encoding path in uri. Enabled by default.
- [url_base](/operations/settings/settings.md#url_base) - base URL for resolving relative URLs passed to the engine.
