<a name="table_engines-url"></a>

# URL(URL, Format)

This data source operates with data on remote HTTP/HTTPS server. The engine is
similar to [`File`](./file.md#).

## Usage in ClickHouse Server

```
URL(URL, Format)
```

`Format` should be supported for `SELECT` and/or `INSERT`. For the full list of
supported formats see [Formats](../../interfaces/formats.md#formats).

`URL` must match the format of Uniform Resource Locator. The specified
URL must address a server working with HTTP or HTTPS. The server shouldn't
require any additional HTTP-headers.

`INSERT` and `SELECT` queries are transformed into `POST` and `GET` requests
respectively. For correct `POST`-requests handling the remote server should support
[Chunked transfer encoding](https://ru.wikipedia.org/wiki/Chunked_transfer_encoding).

**Example:**

**1.** Create the `url_engine_table` table:

```sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** Implement simple http-server using python3:

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
python3 server.py
```

**3.** Query the data:

```sql
SELECT * FROM url_engine_table
```

```text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```


## Details of Implementation

- Reads and writes can be parallel
- Not supported:
  - `ALTER`
  - `SELECT ... SAMPLE`
  - Indices
  - Replication
