---
description: 'Allows processing files from URL in parallel from many nodes in a specified
  cluster.'
sidebar_label: 'urlCluster'
sidebar_position: 201
slug: /sql-reference/table-functions/urlCluster
title: 'urlCluster'
---

# urlCluster Table Function

Allows processing files from URL in parallel from many nodes in a specified cluster. On initiator it creates a connection to all nodes in the cluster, discloses asterisk in URL file path, and dispatches each file dynamically. On the worker node it asks the initiator about the next task to process and processes it. This is repeated until all tasks are finished.

**Syntax**

```sql
urlCluster(cluster_name, URL, format, structure)
```

**Arguments**

-   `cluster_name` — Name of a cluster that is used to build a set of addresses and connection parameters to remote and local servers.
- `URL` — HTTP or HTTPS server address, which can accept `GET` requests. Type: [String](../../sql-reference/data-types/string.md).
- `format` — [Format](/sql-reference/formats) of the data. Type: [String](../../sql-reference/data-types/string.md).
- `structure` — Table structure in `'UserID UInt64, Name String'` format. Determines column names and types. Type: [String](../../sql-reference/data-types/string.md).

**Returned value**

A table with the specified format and structure and with data from the defined `URL`.

**Examples**

Getting the first 3 lines of a table that contains columns of `String` and [UInt32](../../sql-reference/data-types/int-uint.md) type from HTTP-server which answers in [CSV](../../interfaces/formats.md#csv) format.

1. Create a basic HTTP server using the standard Python 3 tools and start it:

```python
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

```sql
SELECT * FROM urlCluster('cluster_simple','http://127.0.0.1:12345', CSV, 'column1 String, column2 UInt32')
```

## Globs in URL {#globs-in-url}

Patterns in curly brackets `{ }` are used to generate a set of shards or to specify failover addresses. Supported pattern types and examples see in the description of the [remote](remote.md#globs-in-addresses) function.
Character `|` inside patterns is used to specify failover addresses. They are iterated in the same order as listed in the pattern. The number of generated addresses is limited by [glob_expansion_max_elements](../../operations/settings/settings.md#glob_expansion_max_elements) setting.

**See Also**

-   [HDFS engine](../../engines/table-engines/special/url.md)
-   [URL table function](../../sql-reference/table-functions/url.md)
