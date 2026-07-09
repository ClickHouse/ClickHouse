---
description: 'server 없이 데이터를 처리할 때 clickhouse-local을 사용하는 방법 안내'
sidebar_label: 'clickhouse-local'
sidebar_position: 60
slug: /operations/utilities/clickhouse-local
title: 'clickhouse-local'
doc_type: 'reference'
---

<div id="when-to-use-clickhouse-local-vs-clickhouse">
  ## `clickhouse-local`과 ClickHouse를 언제 사용해야 하는가
</div>

`clickhouse-local`은 전체 데이터베이스 서버를 설치하지 않고도 SQL을 사용해 로컬 및 원격 파일을 빠르게 처리해야 하는 개발자에게 적합한, 사용하기 쉬운 ClickHouse 버전입니다. `clickhouse-local`을 사용하면 개발자는 명령줄에서 직접 SQL 명령을 사용할 수 있으며([ClickHouse SQL 방언](../../sql-reference/index.md) 사용), 전체 ClickHouse를 설치하지 않고도 ClickHouse 기능에 간단하고 효율적으로 접근할 수 있습니다. `clickhouse-local`의 주요 장점 중 하나는 [clickhouse-client](/ko/operations/utilities/clickhouse-local)를 설치할 때 이미 함께 포함된다는 점입니다. 즉, 복잡한 설치 과정 없이도 `clickhouse-local`을 빠르게 시작할 수 있습니다.

`clickhouse-local`은 개발, 테스트, 파일 처리에는 매우 유용한 도구이지만, 엔드 사용자나 애플리케이션에 서비스를 제공하는 용도로는 적합하지 않습니다. 이러한 경우에는 오픈소스 [ClickHouse](/ko/install)를 사용하는 것이 좋습니다. ClickHouse는 대규모 분석 워크로드를 처리하도록 설계된 강력한 OLAP 데이터베이스입니다. 대규모 데이터셋에 대한 복잡한 쿼리를 빠르고 효율적으로 처리할 수 있으므로, 고성능이 중요한 운영 환경에 적합합니다. 또한 ClickHouse는 복제(replication), 세그먼트 분할(sharding), 고가용성과 같은 다양한 기능을 제공하며, 이러한 기능은 대규모 데이터셋을 처리하도록 스케일링하고 애플리케이션에 서비스를 제공하는 데 필수적입니다. 더 큰 데이터셋을 처리하거나 엔드 사용자 또는 애플리케이션에 서비스를 제공해야 한다면 `clickhouse-local` 대신 오픈소스 ClickHouse를 사용하는 것을 권장합니다.

아래 문서를 읽고 [로컬 파일 쿼리](#query_data_in_file) 또는 [S3의 Parquet 파일 읽기](#query-data-in-a-parquet-file-in-aws-s3)와 같은 `clickhouse-local`의 예시 사용 사례를 확인하십시오.

<div id="download-clickhouse-local">
  ## clickhouse-local 다운로드
</div>

`clickhouse-local`은 ClickHouse 서버와 `clickhouse-client`를 실행할 때 사용하는 것과 동일한 `clickhouse` 바이너리로 실행됩니다. 최신 버전을 다운로드하는 가장 쉬운 방법은 다음 명령을 사용하는 것입니다.

```bash
curl https://clickhouse.com/ | sh
```

:::note
방금 다운로드한 바이너리로는 다양한 ClickHouse 도구와 유틸리티를 실행할 수 있습니다. ClickHouse를 데이터베이스 서버로 실행하려면 [Quick Start](/ko/get-started/quick-start)를 참고하십시오.
:::

<div id="query_data_in_file">
  ## SQL을 사용해 파일의 데이터 쿼리하기
</div>

`clickhouse-local`의 일반적인 사용 방식 중 하나는 파일에 대해 애드혹 쿼리를 실행하는 것입니다. 즉, 데이터를 테이블에 삽입할 필요가 없습니다. `clickhouse-local`은 파일의 데이터를 임시 테이블로 스트리밍한 다음 SQL을 실행할 수 있습니다.

파일이 `clickhouse-local`과 동일한 머신에 있으면 로드할 파일만 지정하면 됩니다. 다음 `reviews.tsv` 파일에는 Amazon 상품 리뷰 샘플이 포함되어 있습니다:

```bash
./clickhouse local -q "SELECT * FROM 'reviews.tsv'"
```

이 명령은 다음의 축약형입니다:

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv')"
```

ClickHouse는 파일 이름 확장자를 보고 해당 파일이 탭으로 구분된 포맷임을 인식합니다. 포맷을 명시적으로 지정해야 한다면 [여러 ClickHouse 입력 형식](../../interfaces/formats.md) 중 하나를 추가하면 됩니다:

```bash
./clickhouse local -q "SELECT * FROM file('reviews.tsv', 'TabSeparated')"
```

`file` 테이블 함수는 테이블을 생성하며, `DESCRIBE`를 사용하면 자동으로 추론된 스키마를 확인할 수 있습니다:

```bash
./clickhouse local -q "DESCRIBE file('reviews.tsv')"
```

:::tip
파일 이름에 글롭 패턴을 사용할 수 있습니다([글롭 치환](/ko/sql-reference/table-functions/file.md/#globs-in-path) 참조).

예시:

```bash
./clickhouse local -q "SELECT * FROM 'reviews*.jsonl'"
./clickhouse local -q "SELECT * FROM 'review_?.csv'"
./clickhouse local -q "SELECT * FROM 'review_{1..3}.csv'"
```

:::

```response
marketplace    Nullable(String)
customer_id    Nullable(Int64)
review_id    Nullable(String)
product_id    Nullable(String)
product_parent    Nullable(Int64)
product_title    Nullable(String)
product_category    Nullable(String)
star_rating    Nullable(Int64)
helpful_votes    Nullable(Int64)
total_votes    Nullable(Int64)
vine    Nullable(String)
verified_purchase    Nullable(String)
review_headline    Nullable(String)
review_body    Nullable(String)
review_date    Nullable(Date)
```

평점이 가장 높은 제품을 찾아보겠습니다:

```bash
./clickhouse local -q "SELECT
    argMax(product_title,star_rating),
    max(star_rating)
FROM file('reviews.tsv')"
```

```response
Monopoly Junior Board Game    5
```

<div id="query-data-in-a-parquet-file-in-aws-s3">
  ## AWS S3의 Parquet 파일에서 데이터 쿼리하기
</div>

S3에 파일이 있으면 `clickhouse-local`과 `s3` 테이블 함수를 사용해 파일을 제자리에서 쿼리할 수 있습니다(데이터를 ClickHouse 테이블에 삽입하지 않음). 공개 버킷에 `house_0.parquet`라는 파일이 있으며, 이 파일에는 영국에서 거래된 주택의 가격 데이터가 들어 있습니다. 행 수가 얼마나 되는지 확인해 보겠습니다:

```bash
./clickhouse local -q "
SELECT count()
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

이 파일에는 2.7M개의 행이 포함되어 있습니다:

```response
2772030
```

ClickHouse가 파일에서 추론한 스키마(inferred schema)가 무엇인지 확인하는 것은 항상 유용합니다:

```bash
./clickhouse local -q "DESCRIBE s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')"
```

```response
price    Nullable(Int64)
date    Nullable(UInt16)
postcode1    Nullable(String)
postcode2    Nullable(String)
type    Nullable(String)
is_new    Nullable(UInt8)
duration    Nullable(String)
addr1    Nullable(String)
addr2    Nullable(String)
street    Nullable(String)
locality    Nullable(String)
town    Nullable(String)
district    Nullable(String)
county    Nullable(String)
```

가장 비싼 동네가 어디인지 살펴보겠습니다:

```bash
./clickhouse local -q "
SELECT
    town,
    district,
    count() AS c,
    round(avg(price)) AS price,
    bar(price, 0, 5000000, 100)
FROM s3('https://datasets-documentation.s3.eu-west-3.amazonaws.com/house_parquet/house_0.parquet')
GROUP BY
    town,
    district
HAVING c >= 100
ORDER BY price DESC
LIMIT 10"
```

```response
LONDON    CITY OF LONDON    886    2271305    █████████████████████████████████████████████▍
LEATHERHEAD    ELMBRIDGE    206    1176680    ███████████████████████▌
LONDON    CITY OF WESTMINSTER    12577    1108221    ██████████████████████▏
LONDON    KENSINGTON AND CHELSEA    8728    1094496    █████████████████████▉
HYTHE    FOLKESTONE AND HYTHE    130    1023980    ████████████████████▍
CHALFONT ST GILES    CHILTERN    113    835754    ████████████████▋
AMERSHAM    BUCKINGHAMSHIRE    113    799596    ███████████████▉
VIRGINIA WATER    RUNNYMEDE    356    789301    ███████████████▊
BARNET    ENFIELD    282    740514    ██████████████▊
NORTHWOOD    THREE RIVERS    184    731609    ██████████████▋
```

:::tip
파일 데이터를 ClickHouse에 삽입할 준비가 되면 ClickHouse 서버를 시작한 다음, `file` 및 `s3` 테이블 함수의 결과를 `MergeTree` 테이블에 삽입하십시오. 자세한 내용은 [Quick Start](/ko/get-started/quick-start)를 참조하십시오.
:::

<div id="format-conversions">
  ## 포맷 변환
</div>

`clickhouse-local`을 사용하여 서로 다른 포맷 간에 데이터를 변환할 수 있습니다. 예시:

```bash
$ clickhouse-local --input-format JSONLines --output-format CSV --query "SELECT * FROM table" < data.json > data.csv
```

포맷은 파일 확장자를 통해 자동으로 감지됩니다:

```bash
$ clickhouse-local --query "SELECT * FROM table" < data.json > data.csv
```

간단히 `--copy` 인수를 사용해 작성할 수 있습니다:

```bash
$ clickhouse-local --copy < data.json > data.csv
```

<div id="usage">
  ## 사용법
</div>

기본적으로 `clickhouse-local`은 동일한 호스트에서 실행 중인 ClickHouse 서버의 데이터에 접근할 수 있으며, 서버 구성에 의존하지 않습니다. 또한 `--config-file` 인수를 사용해 서버 구성을 로드할 수도 있습니다. 임시 데이터의 경우 기본적으로 고유한 임시 데이터 디렉터리가 생성됩니다.

기본 사용법(Linux):

```bash
$ clickhouse-local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

기본 사용법 (Mac):

```bash
$ ./clickhouse local --structure "table_structure" --input-format "format_of_incoming_data" --query "query"
```

:::note
`clickhouse-local`은 WSL2를 통해 Windows에서도 지원됩니다.
:::

인수:

* `-S`, `--structure` — 입력 데이터의 테이블 구조입니다.
* `--input-format` — 입력 형식이며, 기본값은 `TSV`입니다.
* `-F`, `--file` — 데이터 경로이며, 기본값은 `stdin`입니다.
* `-q`, `--query` — `;`를 구분자로 사용해 실행할 쿼리입니다. `--query`는 여러 번 지정할 수 있습니다. 예: `--query "SELECT 1" --query "SELECT 2"`. `--queries-file`과 동시에 사용할 수 없습니다.
* `--queries-file` - 실행할 쿼리가 들어 있는 파일 경로입니다. `--queries-file`은 여러 번 지정할 수 있습니다. 예: `--query queries1.sql --query queries2.sql`. `--query`와 동시에 사용할 수 없습니다.
* `--multiquery, -n` – 지정하면 세미콜론으로 구분된 여러 쿼리를 `--query` 옵션 뒤에 나열할 수 있습니다. 편의를 위해 `--query`를 생략하고 쿼리를 `--multiquery` 뒤에 직접 전달할 수도 있습니다.
* `-N`, `--table` — 출력 데이터를 저장할 테이블 이름이며, 기본값은 `table`입니다.
* `-f`, `--format`, `--output-format` — 출력 형식이며, 기본값은 `TSV`입니다.
* `-d`, `--database` — 기본 데이터베이스이며, 기본값은 `_local`입니다.
* `--stacktrace` — Exception이 발생한 경우 디버그 출력을 덤프할지 여부입니다.
* `--echo [ <bool> ]` — 실행 전에 각 쿼리를 출력합니다. 선택적 불리언 값을 받습니다. 대화형 모드에서는 기본적으로 활성화되고, 배치 모드에서는 비활성화됩니다. 참고: 이제 `--echo`는 선택적 값을 받으므로, 값 없이 사용한 `--echo` 바로 뒤에 위치 인수 쿼리를 두면 해당 쿼리가 값으로 처리됩니다. 대신 `--echo --query "..."`, `--echo -q "..."`, `--echo=false` 또는 파이프로 전달된 `stdin`을 사용하십시오.
* `--echo-formatted [ <bool> ]` — 출력되는 쿼리를 포맷합니다. 선택적 불리언 값을 받습니다. 대화형 모드에서는 기본적으로 활성화되고, 배치 모드에서는 비활성화됩니다.
* `--echo-query-id [ <bool> ]` — 실행 전에 `query_id`를 출력합니다. 선택적 불리언 값을 받습니다. 대화형 모드에서는 기본적으로 활성화되고, 배치 모드에서는 비활성화됩니다.
* `--echo-query-separator <string>` — 포맷된 출력 쿼리 앞에 이 구분자를 출력합니다(`--echo-formatted` 필요). 이렇게 하면 입력한 쿼리와 다시 포맷된 출력 쿼리를 더 쉽게 구분할 수 있습니다. 기본값은 빈 문자열(비활성화)입니다.
* `--highlight`, `--hilite` `<bool>` — 명령 프롬프트와 출력되는 쿼리의 syntax highlighting을 켜거나 끕니다. 기본적으로 활성화되어 있습니다. highlighting은 터미널에 출력할 때만 적용됩니다.
* `--hints <bool>` — 커서가 입력 끝에 있을 때 가장 잘 일치하는 제안에 대한 입력 중 자동완성 힌트(인라인 &quot;ghost&quot; 텍스트)를 표시합니다. 위/아래(또는 Ctrl-Up/Ctrl-Down)로 힌트를 이동하고, Tab 또는 Right로 인라인 힌트를 수락합니다. `Enter`는 힌트가 명시적으로 선택된 경우에만 이를 수락하고, 그렇지 않으면 쿼리를 실행합니다. `Tab`은 기존 완성 목록도 엽니다. `--highlight`(힌트에 색상이 필요함)와 제안 기능이 필요하므로 `--disable_suggestion`도 이를 비활성화합니다. 기본적으로 활성화되어 있습니다.
* `--verbose` — 쿼리 실행에 관한 더 자세한 정보를 표시합니다.
* `--logger.console` — Console에 로그를 기록합니다.
* `--logger.log` — 로그 파일 이름입니다.
* `--logger.level` — 로그 레벨입니다.
* `--ignore-error` — 쿼리가 실패해도 처리를 중단하지 않습니다.
* `-c`, `--config-file` — ClickHouse 서버와 동일한 포맷의 설정 파일 경로이며, 기본적으로 구성은 비어 있습니다.
* `--no-system-tables` — system tables를 ATTACH하지 않습니다.
* `--help` — `clickhouse-local`의 인수 참고입니다.
* `-V`, `--version` — 버전 정보를 출력하고 종료합니다.

또한 `--config-file` 대신 더 일반적으로 사용되는 각 ClickHouse 구성 변수용 인수도 있습니다.

<div id="commands">
  ## 명령어
</div>

<div id="ls-command">
  ### LS 명령
</div>

clickhouse-local에서 액세스할 수 있는 현재 작업 디렉터리의 모든 파일을 나열합니다.

다음과 같이 대화형 모드에서 실행할 수 있습니다:

```sql title="Query"
ClickHouse local version 26.3.1.1.

:) ls

SELECT _file AS file
FROM file('*', 'One')
ORDER BY file ASC
```

```text title="Response"
┌─file────────┐
│ file1.csv   │
│ file2.json  │
│ file3.xml   │
└─────────────┘
```

`-q` 인수를 사용해 쿼리로 실행할 수도 있습니다:

```sh
./clickhouse-local -q ls
```

```text title="Response"
file1.csv
file2.json
file3.xml
```

<div id="clear-command">
  ### CLEAR 명령
</div>

터미널 화면을 지웁니다(Linux의 `clear` 명령이나 많은 터미널에서의 Ctrl+L과 유사함). 이는 클라이언트 측 동작이며 SQL 엔진으로 전송되지 않습니다.

`clickhouse-local`에서는 **interactive** 모드와 **`-q`** 및 **`--queries-file`** 입력에서 이 메타 명령을 인식합니다(`-q`와 동일한 클라이언트 경로를 사용하며, `ls`와 같은 개념임). 따라서 `clear`만 단독으로 입력해도 `UNKNOWN_IDENTIFIER` 오류가 발생하지 않습니다. 원격 **`clickhouse-client --queries-file`** 의 동작은 변경되지 않습니다. 파일 내용은 SQL로만 실행되며(텍스트 수준의 메타 명령은 없음), 처리 방식도 그대로 유지됩니다.

`clickhouse-client`에서는 **interactive** 모드에서만 인식됩니다. **`-q`** 또는 쿼리 파일과 함께 사용할 경우 `clear`는 여전히 SQL로 구문 분석되므로, 자동화에서는 오타가 조용한 no-op으로 바뀌지 않고 기존 오류 동작이 유지됩니다.

지원되는 형식: `clear`, `CLEAR`, `/clear`(마지막의 선택적 `;`는 무시됨). 표준 출력이 터미널이 아닌 경우(예: 출력을 파이프로 전달할 때) 이 메타 명령은 인식되면 허용되지만 제어 시퀀스를 출력하지는 않습니다.

`clickhouse-local`에서 `-q`를 사용할 경우:

```sh
./clickhouse-local -q clear
```

<div id="examples">
  ## 예시
</div>

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local --structure "a Int64, b Int64" \
    --input-format "CSV" --query "SELECT * FROM table"
Read 2 rows, 32.00 B in 0.000 sec., 5182 rows/sec., 80.97 KiB/sec.
1   2
3   4
```

이전 예시는 다음과 동일합니다:

```bash title="Query"
$ echo -e "1,2\n3,4" | clickhouse-local -n --query "
    CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin);
    SELECT a, b FROM table;
    DROP TABLE table;"
Read 2 rows, 32.00 B in 0.000 sec., 4987 rows/sec., 77.93 KiB/sec.
1   2
3   4
```

`stdin` 또는 `--file` 인수를 사용할 필요가 없으며, [`file` 테이블 함수](../../sql-reference/table-functions/file.md)를 사용해 파일을 몇 개든 열 수 있습니다:

```bash title="Query"
$ echo 1 | tee 1.tsv
1

$ echo 2 | tee 2.tsv
2

$ clickhouse-local --query "
    select * from file('1.tsv', TSV, 'a int') t1
    cross join file('2.tsv', TSV, 'b int') t2"
1    2
```

이제 각 Unix 사용자에 대한 메모리 사용자를 출력합니다:

```bash title="Query"
$ ps aux | tail -n +2 | awk '{ printf("%s\t%s\n", $1, $4) }' \
    | clickhouse-local --structure "user String, mem Float64" \
        --query "SELECT user, round(sum(mem), 2) as memTotal
            FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty"
```

```text title="Response"
Read 186 rows, 4.15 KiB in 0.035 sec., 5302 rows/sec., 118.34 KiB/sec.
┏━━━━━━━━━━┳━━━━━━━━━━┓
┃ user     ┃ memTotal ┃
┡━━━━━━━━━━╇━━━━━━━━━━┩
│ bayonet  │    113.5 │
├──────────┼──────────┤
│ root     │      8.8 │
├──────────┼──────────┤
...
```

<div id="starting-listeners">
  ## TCP 및 HTTP 리스너 시작
</div>

`clickhouse-local`은 TCP(네이티브 프로토콜) 및 HTTP 연결을 허용하는 경량 서버로 전환할 수 있습니다. 이는 실행 중인 `clickhouse-local` 인스턴스의 데이터베이스와 테이블에 다른 ClickHouse 도구나 애플리케이션이 접근할 수 있도록 하려는 경우에 유용합니다. 각 수신 연결에는 자체 세션이 할당된다는 점에 유의하십시오. 따라서 대화형 `clickhouse-local` 세션의 임시 테이블과 세션 수준 설정은 외부 연결에서는 보이지 않습니다.

리스너를 열려면 `SYSTEM START LISTEN`을 사용하고, 닫으려면 `SYSTEM STOP LISTEN`을 사용하십시오:

```bash
clickhouse-local \
    --listen_host 127.0.0.1 \
    --tcp_port 9000 \
    --http_port 8123 \
    --query "
        SYSTEM START LISTEN TCP;
        SYSTEM START LISTEN HTTP;
        SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+42', LineAsString);
        SYSTEM STOP LISTEN TCP;
        SYSTEM STOP LISTEN HTTP;
    "
```

`--listen_host`, `--tcp_port`, `--http_port` 옵션은 바인드 주소와 포트를 설정합니다. 기본 포트는 TCP의 경우 `9000`, HTTP의 경우 `8123`입니다.

:::warning 보안
기본적으로 `clickhouse-local`은 임시 사용자 설정으로 실행되므로, 열리는 모든 리스너는 인증 없이 접근할 수 있습니다. `--config-file` 등을 통해 `users_config` 설정이 사용자 지정 `users.xml`을 가리키도록 하여 사용자와 액세스 제어를 명시적으로 구성한 경우가 아니라면, 루프백 주소(`127.0.0.1` 또는 `::1`)에 바인드하십시오. 인증 없이 비루프백 주소에서 수신 대기하면, 선택한 포트에 접근할 수 있는 누구에게나 로컬 인스턴스의 데이터가 노출됩니다.
:::

<div id="related-content-1">
  ## 관련 콘텐츠
</div>

* [clickhouse-local을 사용해 로컬 파일의 데이터를 추출, 변환, 쿼리하기](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)
* [ClickHouse로 데이터 가져오기 - Part 1](https://clickhouse.com/blog/getting-data-into-clickhouse-part-1)
* [방대한 실제 데이터 집합 살펴보기: ClickHouse의 100년이 넘는 기상 기록](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
* 블로그: [clickhouse-local을 사용해 로컬 파일의 데이터를 추출, 변환, 쿼리하기](https://clickhouse.com/blog/extracting-converting-querying-local-files-with-sql-clickhouse-local)