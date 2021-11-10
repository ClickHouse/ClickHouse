# URL(URL,格式) {#table_engines-url}

用于管理远程 HTTP/HTTPS 服务器上的数据。该引擎类似
[文件](file.md) 引擎。

## 在 ClickHouse 服务器中使用引擎 {#zai-clickhouse-fu-wu-qi-zhong-shi-yong-yin-qing}

`Format` 必须是 ClickHouse 可以用于
`SELECT` 查询的一种格式，若有必要，还要可用于 `INSERT` 。有关支持格式的完整列表，请查看
[格式](../../../interfaces/formats.md#formats)。

`URL` 必须符合统一资源定位符的结构。指定的URL必须指向一个
HTTP 或 HTTPS 服务器。对于服务端响应，
不需要任何额外的 HTTP 头标记。

`INSERT` 和 `SELECT` 查询会分别转换为 `POST` 和 `GET` 请求。
对于 `POST` 请求的处理，远程服务器必须支持
[分块传输编码](https://en.wikipedia.org/wiki/Chunked_transfer_encoding)。

**示例：**

**1.** 在 Clickhouse 服务上创建一个 `url_engine_table` 表：

``` sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** 用标准的 Python 3 工具库创建一个基本的 HTTP 服务并
启动它：

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
python3 server.py
```

**3.** 查询请求:

``` sql
SELECT * FROM url_engine_table
```

    ┌─word──┬─value─┐
    │ Hello │     1 │
    │ World │     2 │
    └───────┴───────┘

## 功能实现 {#gong-neng-shi-xian}

-   读写操作都支持并发
-   不支持：
    -   `ALTER` 和 `SELECT...SAMPLE` 操作。
    -   索引。
    -   副本。

[来源文章](https://clickhouse.tech/docs/en/operations/table_engines/url/) <!--hide-->
