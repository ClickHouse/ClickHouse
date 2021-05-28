---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: URL
---

# URL (URL, Biçim) {#table_engines-url}

Uzak bir HTTP/HTTPS sunucusundaki verileri yönetir. Bu motor benzer
to the [Dosya](file.md) motor.

## ClickHouse sunucusunda motoru kullanma {#using-the-engine-in-the-clickhouse-server}

Bu `format` Clickhouse'un kullanabileceği bir tane olmalı
`SELECT` sorgular ve gerekirse `INSERTs`. Desteklenen formatların tam listesi için bkz.
[Biçimliler](../../../interfaces/formats.md#formats).

Bu `URL` tekdüzen bir kaynak Bulucu yapısına uygun olmalıdır. Belirtilen URL bir sunucuya işaret etmelidir
bu HTTP veya HTTPS kullanır. Bu herhangi bir gerektirmez
sunucudan yanıt almak için ek başlıklar.

`INSERT` ve `SELECT` sorgular dönüştürülür `POST` ve `GET` istemler,
sırasıyla. İşleme için `POST` istekleri, uzak sunucu desteklemesi gerekir
[Yığınlı aktarım kodlaması](https://en.wikipedia.org/wiki/Chunked_transfer_encoding).

Kullanarak HTTP get yönlendirme şerbetçiotu sayısını sınırlayabilirsiniz [max\_http\_get\_redirects](../../../operations/settings/settings.md#setting-max_http_get_redirects) ayar.

**Örnek:**

**1.** Create a `url_engine_table` sunucuda tablo :

``` sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** Standart Python 3 araçlarını kullanarak temel bir HTTP Sunucusu oluşturun ve
Başlat:

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

**3.** Veri iste:

``` sql
SELECT * FROM url_engine_table
```

``` text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

## Uygulama Detayları {#details-of-implementation}

-   Okuma ve yazma paralel olabilir
-   Desteklenmiyor:
    -   `ALTER` ve `SELECT...SAMPLE` harekat.
    -   Dizinler.
    -   Çoğalma.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/url/) <!--hide-->
