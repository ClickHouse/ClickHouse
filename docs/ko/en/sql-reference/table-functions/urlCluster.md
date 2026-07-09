---
description: '지정된 클러스터의 여러 노드에서 URL의 파일을 병렬로 처리할 수 있습니다.'
sidebar_label: 'urlCluster'
sidebar_position: 201
slug: /sql-reference/table-functions/urlCluster
title: 'urlCluster'
doc_type: 'reference'
---

지정된 클러스터의 여러 노드에서 URL의 파일을 병렬로 처리할 수 있습니다. 이니시에이터에서는 클러스터의 모든 노드에 대한 연결을 생성하고, URL 파일 경로의 애스터리스크를 전개한 뒤 각 파일을 동적으로 분배합니다. worker 노드에서는 이니시에이터에 다음으로 처리할 작업을 요청해 처리합니다. 이 과정은 모든 작업이 완료될 때까지 반복됩니다.

<div id="syntax">
  ## 구문
</div>

```sql
urlCluster(cluster_name, URL, format, structure)
```

<div id="arguments">
  ## 인수
</div>

| 인수             | 설명                                                                                                                      |
| -------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | 원격 및 로컬 서버의 주소 집합과 연결 매개변수를 구성하는 데 사용되는 클러스터 이름입니다.                                                                     |
| `URL`          | `GET` 요청을 수신할 수 있는 HTTP 또는 HTTPS 서버 주소입니다. 유형: [String](../../sql-reference/data-types/string.md).                      |
| `format`       | 데이터의 [포맷](/ko/sql-reference/formats)입니다. 유형: [String](../../sql-reference/data-types/string.md).                           |
| `structure`    | `'UserID UInt64, Name String'` 포맷의 테이블 구조입니다. 컬럼 이름과 타입을 결정합니다. 유형: [String](../../sql-reference/data-types/string.md). |

<div id="returned_value">
  ## 반환 값
</div>

지정된 포맷과 구조를 가지며, 지정한 `URL`의 데이터를 포함하는 테이블입니다.

<div id="examples">
  ## 예시
</div>

[CSV](/ko/interfaces/formats/CSV) 포맷으로 응답하는 HTTP 서버에서 `String` 및 [UInt32](../../sql-reference/data-types/int-uint.md) 타입의 컬럼이 있는 테이블의 처음 3줄을 가져옵니다.

1. 표준 Python 3 도구를 사용해 기본 HTTP 서버를 만들고 시작합니다:

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

<div id="globs-in-url">
  ## URL의 글롭 패턴
</div>

`{ }` 안의 패턴은 세그먼트 집합을 생성하거나 장애 조치 주소를 지정하는 데 사용됩니다. 지원되는 패턴 유형과 예시는 [remote](remote.md#globs-in-addresses) 함수 설명을 참조하십시오.
패턴 내부의 `|` 문자는 장애 조치 주소를 지정하는 데 사용됩니다. 주소는 패턴에 나열된 순서대로 차례로 사용됩니다. 생성되는 주소 수는 [glob&#95;expansion&#95;max&#95;elements](../../operations/settings/settings.md#glob_expansion_max_elements) 설정으로 제한됩니다.

<div id="related">
  ## 관련 항목
</div>

* [HDFS 엔진](/ko/engines/table-engines/integrations/hdfs)
* [URL 테이블 함수](/ko/engines/table-engines/special/url)