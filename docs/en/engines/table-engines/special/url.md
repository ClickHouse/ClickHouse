---
toc_priority: 41
toc_title: URL
---

# URL Table Engine {#table_engines-url}

Queries data to/from a remote HTTP/HTTPS server. This engine is similar to the [File](../../../engines/table-engines/special/file.md) engine.

Syntax: `URL(URL, Format)`

## Usage {#using-the-engine-in-the-clickhouse-server}

The `format` must be one that ClickHouse can use in
`SELECT` queries and, if necessary, in `INSERTs`. For the full list of supported formats, see
[Formats](../../../interfaces/formats.md#formats).

The `URL` must conform to the structure of a Uniform Resource Locator. The specified URL must point to a server
that uses HTTP or HTTPS. This does not require any
additional headers for getting a response from the server.

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

-   Reads and writes can be parallel
-   Not supported:
    -   `ALTER` and `SELECT...SAMPLE` operations.
    -   Indexes.
    -   Replication.

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/url/) <!--hide-->
