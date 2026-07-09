---
description: '원격 HTTP/HTTPS 서버와 데이터를 주고받습니다. 이 엔진은 File 엔진과
  유사합니다.'
sidebar_label: 'URL'
sidebar_position: 80
slug: /engines/table-engines/special/url
title: 'URL 테이블 엔진'
doc_type: '참고'
---

원격 HTTP/HTTPS 서버와 데이터를 주고받습니다. 이 엔진은 [File](../../../engines/table-engines/special/file.md) 엔진과 유사합니다.

구문: `URL(URL [,Format] [,CompressionMethod])`

* `URL` 매개변수는 Uniform Resource Locator 구조를 따라야 합니다. `http`/`https` URL(기본 백엔드)의 경우 HTTP 또는 HTTPS를 사용하는 서버를 가리켜야 하며, 서버에서 응답을 받는 데 추가 헤더가 필요하지 않아야 합니다. 인식되는 비HTTP 스킴(`file://`, `s3://`, `az://`, `hdfs://`, …)을 사용하는 URL은 대신 해당 엔진으로 전달됩니다. 자세한 내용은 아래의 [URL 스킴별 디스패치](#scheme-dispatch)를 참조하십시오.

* `Format`은 ClickHouse가 `SELECT` 쿼리에서 사용할 수 있어야 하며, 필요한 경우 `INSERTs`에서도 사용할 수 있어야 합니다. 지원되는 전체 포맷 목록은 [Formats](/ko/interfaces/formats#formats-overview)를 참조하십시오.

  이 인수가 지정되지 않으면 ClickHouse가 `URL` 매개변수의 접미사를 기반으로 포맷을 자동으로 감지합니다. `URL` 매개변수의 접미사가 지원되는 포맷과 일치하지 않으면 테이블 생성에 실패합니다. 예를 들어 엔진 표현식 `URL('http://localhost/test.json')`에서는 `JSON` 포맷이 적용됩니다.

* `CompressionMethod`는 HTTP 본문을 압축할지 여부를 나타냅니다. 압축이 활성화되면 URL 엔진이 전송하는 HTTP 패킷에는 사용된 압축 방식을 나타내는 &#39;Content-Encoding&#39; 헤더가 포함됩니다.

압축을 활성화하려면 먼저 `URL` 매개변수가 가리키는 원격 HTTP 엔드포인트가 해당 압축 알고리즘을 지원하는지 확인하십시오.

지원되는 `CompressionMethod`는 다음 중 하나여야 합니다:

* gzip or gz
* deflate
* brotli or br
* lzma or xz
* zstd or zst
* lz4
* bz2
* snappy
* none
* auto

`CompressionMethod`가 지정되지 않으면 기본값은 `auto`입니다. 즉, ClickHouse가 `URL` 매개변수의 접미사에서 압축 방식을 자동으로 감지합니다. 접미사가 위에 나열된 압축 방식 중 하나와 일치하면 해당 압축이 적용되고, 그렇지 않으면 압축이 적용되지 않습니다.

예를 들어 엔진 표현식 `URL('http://localhost/test.gzip')`에는 `gzip` 압축 방식이 적용되지만, `URL('http://localhost/test.fr')`에는 접미사 `fr`이 위의 어떤 압축 방식과도 일치하지 않으므로 압축이 적용되지 않습니다.

<div id="scheme-dispatch">
  ## URL 스킴별 디스패치
</div>

`URL` 엔진은 다른 파일 및 객체 스토리지 엔진을 통합하는 wrapper로, URL 스킴에 따라 적절한 백엔드로 디스패치합니다. `http`/`https`(및 인식되지 않는 모든 스킴)는 `URL` 엔진 자체에서 처리하고, `file://`는 [File](../../../engines/table-engines/special/file.md) 엔진에서, `s3://`, `gs://`, `gcs://`, `oss://`는 [S3](/ko/engines/table-engines/integrations/s3) 엔진에서, `az://`, `azure://`, `abfss://`, `abfs://`는 [AzureBlobStorage](/ko/engines/table-engines/integrations/azureBlobStorage) 엔진에서, `hdfs://`는 [HDFS](/ko/engines/table-engines/integrations/hdfs) 엔진에서 처리합니다.

추가 구성 없이 S3 URI mapper가 구체적인 엔드포인트로 해석할 수 있는 S3 스킴(`s3`와 `gs`/`gcs`/`oss`)만 디스패치됩니다. 그 외 S3-compatible 공급업체 스킴(`cos`, `obs`, `eos`, …)은 Region마다 다르고 기본 엔드포인트 매핑도 없으므로, 이러한 URL을 `URL` 엔진에 전달하면 인식되지 않는 스킴으로 처리되어 오류가 보고됩니다. 이러한 백엔드에는 [S3](/ko/engines/table-engines/integrations/s3) 엔진을 직접 사용하십시오(`url_scheme_mappers` 구성 필요).

[url&#95;base](/ko/operations/settings/settings.md#url_base) 설정은 스킴 디스패치 전에 적용되므로, 상대 참조는 먼저 base를 기준으로 해석된 뒤 해당 엔진으로 라우팅됩니다.

```sql
CREATE TABLE file_via_url (a UInt32, b String) ENGINE = URL('file://data.csv', CSV);
CREATE TABLE s3_via_url (a UInt32, b String) ENGINE = URL('s3://bucket/key.csv', CSV);
```

<div id="using-the-engine-in-the-clickhouse-server">
  ## 사용
</div>

`INSERT` 및 `SELECT` 쿼리는 각각 `POST` 및 `GET` 요청으로 변환됩니다.
`POST` 요청을 처리하려면 원격 서버에서
[청크 전송 인코딩](https://en.wikipedia.org/wiki/Chunked_transfer_encoding)을 지원해야 합니다.

[max&#95;http&#95;get&#95;redirects](/ko/operations/settings/settings#max_http_get_redirects) 설정을 사용하면 HTTP GET 리디렉션 홉의 최대 횟수를 제한할 수 있습니다.

<div id="wildcards-with-http-index-pages">
  ## HTTP 인덱스 페이지에서 와일드카드 사용
</div>

[allow&#95;experimental&#95;url&#95;wildcard&#95;from&#95;index&#95;pages](/ko/operations/settings/settings.md#allow_experimental_url_wildcard_from_index_pages)가 활성화되면 `URL` 테이블 엔진(table engine)은 HTTP 인덱스 페이지를 가져와 그 안의 링크를 추출해 와일드카드를 확장할 수 있습니다.
이는 [`url`](../../../sql-reference/table-functions/url.md#wildcards-with-http-index-pages) 테이블 함수(table function)와 동일한 메커니즘입니다.

확장은 가져오는 각 인덱스 페이지마다 [max&#95;http&#95;index&#95;page&#95;size](/ko/operations/server-configuration-parameters/settings.md#max_http_index_page_size)로 제한되며, 재귀적으로 디렉터리를 순회할 때는 [url&#95;wildcard&#95;max&#95;directories&#95;to&#95;read](/ko/operations/settings/settings.md#url_wildcard_max_directories_to_read)로 제한됩니다.

<div id="example">
  ## 예시
</div>

**1.** 서버에 `url_engine_table` 테이블을 생성합니다:

```sql
CREATE TABLE url_engine_table (word String, value UInt64)
ENGINE=URL('http://127.0.0.1:12345/', CSV)
```

**2.** 표준 Python 3 도구를 사용해 간단한 HTTP 서버를 만들고
시작하세요:

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

**3.** 데이터 요청:

```sql
SELECT * FROM url_engine_table
```

```text
┌─word──┬─value─┐
│ Hello │     1 │
│ World │     2 │
└───────┴───────┘
```

<div id="details-of-implementation">
  ## 구현 상세
</div>

* 읽기와 쓰기는 병렬로 수행할 수 있습니다
* 다음은 지원되지 않습니다:
  * `ALTER` 및 `SELECT...SAMPLE` 작업
  * 인덱스
  * 복제

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_path` — `URL` 경로입니다. 유형: `LowCardinality(String)`.
* `_file` — `URL` 리소스 이름입니다. 유형: `LowCardinality(String)`.
* `_size` — 리소스 크기(바이트)입니다. 유형: `Nullable(UInt64)`. 크기를 알 수 없으면 값은 `NULL`입니다.
* `_time` — 파일의 마지막 수정 시각입니다. 유형: `Nullable(DateTime)`. 시각을 알 수 없으면 값은 `NULL`입니다.
* `_headers` - HTTP 응답 헤더입니다. 유형: `Map(LowCardinality(String), LowCardinality(String))`.

<div id="resolving-relative-urls">
  ## 상대 URL 해석
</div>

[url&#95;base](/ko/operations/settings/settings.md#url_base) 설정을 사용하면 `URL` 엔진에서 상대 URL을 사용할 수 있습니다. `url_base`를 설정하면 엔진에 전달된 URL이 [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986)에 따라 이를 기준으로 해석됩니다. 해석 규칙에 대한 자세한 설명은 [url 테이블 함수 문서](../../../sql-reference/table-functions/url.md#resolving-relative-urls)를 참조하십시오.

**예시**

```sql
SET url_base = 'http://127.0.0.1:12345/';
CREATE TABLE url_engine_table (word String, value UInt64) ENGINE = URL('hello.csv', CSV);
SELECT * FROM url_engine_table;
```

<div id="storage-settings">
  ## 스토리지 설정
</div>

* [engine&#95;url&#95;skip&#95;empty&#95;files](/ko/operations/settings/settings.md#engine_url_skip_empty_files) - 읽는 중 빈 파일을 건너뛸 수 있습니다. 기본적으로 비활성화되어 있습니다.
* [enable&#95;url&#95;encoding](/ko/operations/settings/settings.md#enable_url_encoding) - URI의 경로 디코딩/인코딩을 활성화하거나 비활성화할 수 있습니다. 기본적으로 활성화되어 있습니다.
* [url&#95;base](/ko/operations/settings/settings.md#url_base) - 엔진에 전달된 상대 URL을 해석하기 위한 기준 URL입니다.