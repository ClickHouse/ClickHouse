---
description: 'clickhouse client command-line client 인터페이스 문서'
sidebar_label: 'ClickHouse 클라이언트'
sidebar_position: 18
slug: /interfaces/client
title: 'ClickHouse 클라이언트'
doc_type: '참고'
---

import Image from '@theme/IdealImage';
import cloud_connect_button from '@site/static/images/_snippets/cloud-connect-button.png';
import connection_details_native from '@site/static/images/_snippets/connection-details-native.png';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

ClickHouse는 ClickHouse 서버에서 SQL 쿼리를 직접 실행할 수 있는 네이티브 command-line client를 제공합니다.
라이브 쿼리 실행을 위한 interactive mode와 스크립팅 및 자동화를 위한 batch mode를 모두 지원합니다.
쿼리 결과는 터미널에 표시하거나 파일로 내보낼 수 있으며, Pretty, CSV, JSON 등을 포함한 모든 ClickHouse 출력 [포맷](formats.md)을 지원합니다.

이 클라이언트는 진행률 표시줄과 읽은 행 수, 처리된 바이트 수, 쿼리 실행 시간을 통해 쿼리 실행 상태를 실시간으로 보여줍니다.
또한 [command-line options](#command-line-options)와 [설정 파일](#configuration_files)을 모두 지원합니다.

<div id="install">
  ## 설치
</div>

ClickHouse를 다운로드하려면 다음 명령을 실행하세요:

```bash
curl https://clickhouse.com/ | sh
```

추가로 설치하려면 다음을 실행하세요:

```bash
sudo ./clickhouse install
```

더 많은 설치 옵션은 [ClickHouse 설치](../getting-started/install/install.mdx)를 참조하십시오.

서로 다른 클라이언트와 서버 버전도 호환되지만, 일부 기능은 이전 버전의 클라이언트에서 사용할 수 없을 수 있습니다. 클라이언트와 서버에는 동일한 버전을 사용할 것을 권장합니다.

<div id="run">
  ## 실행
</div>

:::note
ClickHouse를 다운로드만 하고 설치하지 않은 경우 `clickhouse-client` 대신 `./clickhouse client`를 사용하십시오.
:::

ClickHouse 서버에 연결하려면 다음을 실행하십시오.

```bash
$ clickhouse-client --host server

ClickHouse client version 24.12.2.29 (official build).
Connecting to server:9000 as user default.
Connected to ClickHouse server version 24.12.2.

:)
```

필요에 따라 추가 연결 세부 정보를 지정하십시오:

| Option                           | Description                                                                                                           |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `--port <port>`                  | ClickHouse 서버가 연결을 수신하는 포트입니다. 기본 포트는 9440(TLS) 및 9000(TLS 미사용)입니다. clickhouse client는 HTTP(S)가 아니라 네이티브 프로토콜을 사용합니다. |
| `-s [ --secure ]`                | TLS 사용 여부입니다(일반적으로 자동 감지됨).                                                                                           |
| `-u [ --user ] <username>`       | 연결에 사용할 데이터베이스 사용자입니다. 기본적으로 `default` 사용자로 연결됩니다.                                                                    |
| `--password <password>`          | 데이터베이스 사용자의 비밀번호입니다. 설정 파일에서 연결 비밀번호를 지정할 수도 있습니다. 비밀번호를 지정하지 않으면 클라이언트가 입력을 요청합니다.                                   |
| `-c [ --config ] <path-to-file>` | clickhouse client의 설정 파일 위치입니다. 기본 위치 중 하나에 없는 경우 지정합니다. [설정 파일](#configuration_files)을 참조하십시오.                       |
| `--connection <name>`            | [설정 파일](#connection-credentials)에 미리 구성된 연결 세부 정보의 이름입니다.                                                             |

명령줄 옵션의 전체 목록은 [명령줄 옵션](#command-line-options)을 참조하십시오.

<div id="connecting-cloud">
  ### ClickHouse Cloud에 연결하기
</div>

ClickHouse Cloud 서비스 정보는 ClickHouse Cloud 콘솔에서 확인할 수 있습니다. 연결할 서비스를 선택한 다음 **Connect**를 클릭하십시오:

<Image img={cloud_connect_button} size="md" alt="ClickHouse Cloud 서비스 연결 버튼" />

<br />

<br />

**Native**를 선택하면 예시 `clickhouse-client` 명령과 함께 연결 정보가 표시됩니다:

<Image img={connection_details_native} size="md" alt="ClickHouse Cloud 네이티브 TCP 연결 정보" />

<div id="connection-credentials">
  ### 설정 파일에 연결 정보 저장하기
</div>

하나 이상의 ClickHouse 서버에 대한 연결 정보를 [설정 파일](#configuration_files)에 저장할 수 있습니다.

형식은 다음과 같습니다:

```xml
<config>
    <connections_credentials>
        <connection>
            <name>default</name>
            <hostname>hostname</hostname>
            <port>9440</port>
            <secure>1</secure>
            <user>default</user>
            <password>password</password>
            <!-- <history_file></history_file> -->
            <!-- <history_max_entries></history_max_entries> -->
            <!-- <accept-invalid-certificate>false</accept-invalid-certificate> -->
            <!-- <prompt></prompt> -->
        </connection>
    </connections_credentials>
</config>
```

자세한 내용은 [설정 파일 섹션](#configuration_files)을 참조하십시오.

:::note
쿼리 구문에 집중할 수 있도록, 아래 예시에서는 연결 정보(`--host`, `--port` 등)를 생략합니다. 명령을 사용할 때는 해당 정보를 추가해야 합니다.
:::

<div id="interactive-mode">
  ## 대화형 모드
</div>

<div id="using-interactive-mode">
  ### 대화형 모드 사용
</div>

ClickHouse를 대화형 모드에서 실행하려면 다음 명령을 실행하십시오:

```bash
clickhouse-client
```

이렇게 하면 SQL 쿼리를 대화형으로 입력할 수 있는 Read-Eval-Print Loop (REPL)가 열립니다.
연결되면 쿼리를 입력할 수 있는 프롬프트가 표시됩니다:

```bash
ClickHouse client version 25.x.x.x
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 25.x.x.x

hostname :)
```

대화형 모드에서는 기본 출력 형식이 `PrettyCompact`입니다.
쿼리의 `FORMAT` 절에서 포맷을 변경하거나 `--format` 명령줄 옵션을 지정할 수 있습니다.
Vertical 형식을 사용하려면 `--vertical`을 사용하거나 쿼리 끝에 `\G`를 지정할 수 있습니다.
이 형식에서는 각 값이 별도의 줄에 출력되므로 열이 많은 테이블에 편리합니다.

대화형 모드에서는 기본적으로 `Enter`를 누르면 입력한 내용이 실행됩니다.
쿼리 끝에 세미콜론을 붙일 필요는 없습니다.

`-m, --multiline` 매개변수로 클라이언트를 시작할 수 있습니다.
여러 줄 쿼리를 입력하려면 줄 바꿈 앞에 백슬래시 `\`를 입력하십시오.
`Enter`를 누르면 쿼리의 다음 줄을 입력하라는 메시지가 표시됩니다.
쿼리를 실행하려면 끝에 세미콜론을 붙이고 `Enter`를 누르십시오.

clickhouse client는 `replxx`(`readline`과 유사)를 기반으로 하므로 익숙한 키보드 단축키를 사용할 수 있으며 이력도 유지됩니다.
이력은 기본적으로 `~/.clickhouse-client-history`에 기록됩니다.

클라이언트를 종료하려면 `Ctrl+D`를 누르거나, 쿼리 대신 다음 중 하나를 입력하십시오:

* `exit` 또는 `exit;`
* `quit` 또는 `quit;`
* `q`, `Q` 또는 `:q`
* `logout` 또는 `logout;`

<div id="getting-help">
  ### 도움말 보기
</div>

클라이언트를 벗어나지 않고도 함수, 테이블 엔진, 데이터 타입, 포맷, 설정 및 그 밖의 시스템 구성 요소에 대한 문서를 확인할 수 있습니다. `help` 다음에 이름을 입력하십시오(`/help`, `man`, `/man`도 동일하게 사용할 수 있습니다):

```text
help domainWithoutWWW
```

조회는 대소문자를 구분하지 않으며, [`system.documentation`](../operations/system-tables/documentation.md) 테이블을 조회합니다. 일치하는 문서는 터미널에서 Markdown으로 렌더링되며, 굵게/기울임꼴 텍스트, 테이블, 문법 강조가 적용된 코드 블록이 포함됩니다. 여러 구성 요소가 같은 이름을 공유하는 경우(예: `file`은 함수이면서 테이블 엔진이기도 함) 해당 항목이 모두 표시됩니다.

정확히 일치하는 이름이 없으면, 클라이언트는 비슷한 이름(오타 허용)과 문서에 해당 단어가 언급된 구성 요소를 나열합니다:

```text
help maxx_threads
```

`help`만 입력하면 간단한 사용법 요약이 출력됩니다.

<div id="processing-info">
  ### 쿼리 처리 정보
</div>

쿼리를 처리할 때 클라이언트는 다음을 표시합니다.

1. Progress. 기본적으로 초당 10회를 넘지 않도록 업데이트됩니다.
   쿼리가 매우 빠르면 진행 상황이 표시되기 전에 완료될 수 있습니다.
2. 디버깅용으로, 구문 분석 후 포맷팅된 쿼리.
3. 지정된 포맷의 결과.
4. 결과의 줄 수, 경과 시간, 쿼리 처리의 평균 속도.
   모든 데이터 양은 압축되지 않은 데이터를 기준으로 합니다.

오래 걸리는 쿼리는 `Ctrl+C`를 눌러 취소할 수 있습니다.
하지만 서버가 요청을 중단할 때까지는 잠시 기다려야 합니다.
일부 단계에서는 쿼리를 취소할 수 없습니다.
기다리지 않고 `Ctrl+C`를 두 번째로 누르면 클라이언트가 종료됩니다.

ClickHouse Client에서는 쿼리를 위해 외부 데이터(외부 임시 테이블)를 전달할 수 있습니다.
자세한 내용은 [쿼리 처리용 외부 데이터](../engines/table-engines/special/external-data.md) 섹션을 참조하십시오.

<div id="cli_aliases">
  ### 별칭
</div>

REPL에서는 다음 별칭을 사용할 수 있습니다:

* `\l` - SHOW DATABASES
* `\d` - SHOW TABLES
* `\c <DATABASE>` - USE DATABASE
* `.` - 마지막 쿼리를 다시 실행합니다

<div id="keyboard_shortcuts">
  ### 키보드 단축키
</div>

* `Alt (Option) + Shift + e` - 현재 쿼리로 편집기를 엽니다. 환경 변수 `EDITOR`로 사용할 편집기를 지정할 수 있습니다. 기본적으로 `vim`이 사용됩니다.
* `Alt (Option) + #` - 줄을 주석 처리합니다.
* `Ctrl + r` - 퍼지 이력 검색.

사용 가능한 모든 키보드 단축키의 전체 목록은 [replxx](https://github.com/AmokHuginnsson/replxx/blob/1f149bf/src/replxx_impl.cxx#L262)에서 확인할 수 있습니다.

:::tip
MacOS에서 메타 키(Option)가 올바르게 작동하도록 구성하려면 다음과 같이 하십시오.

iTerm2: Preferences -&gt; Profile -&gt; Keys -&gt; Left Option key로 이동한 다음 Esc+를 클릭하십시오
:::

<div id="batch-mode">
  ## 배치 모드
</div>

<div id="using-batch-mode">
  ### 배치 모드 사용
</div>

clickhouse client를 대화형으로 사용하는 대신 배치 모드로 실행할 수 있습니다.
배치 모드에서는 ClickHouse가 단일 쿼리를 실행한 후 즉시 종료되며, 대화형 프롬프트가 표시되거나 반복해서 입력을 받지 않습니다.

다음과 같이 단일 쿼리를 지정할 수 있습니다:

```bash
$ clickhouse-client "SELECT sum(number) FROM numbers(10)"
45
```

`--query` 명령줄 옵션을 사용할 수도 있습니다:

```bash
$ clickhouse-client --query "SELECT uniq(number) FROM numbers(10)"
10
```

`stdin`을 통해 쿼리를 제공할 수 있습니다:

```bash
$ echo "SELECT avg(number) FROM numbers(10)" | clickhouse-client
4.5
```

`messages` 테이블이 있다고 가정하면, 명령줄에서도 데이터를 삽입할 수 있습니다:

```bash
$ echo "Hello\nGoodbye" | clickhouse-client --query "INSERT INTO messages FORMAT CSV"
```

`--query`를 지정하면 모든 입력이 줄 바꿈(line feed) 뒤에 요청에 추가됩니다.

<div id="cloud-example">
  ### 원격 ClickHouse 서비스에 CSV 파일 삽입
</div>

이 예시에서는 샘플 데이터셋 CSV 파일 `cell_towers.csv`를 `default` 데이터베이스의 기존 테이블 `cell_towers`에 삽입합니다:

```bash
clickhouse-client --host HOSTNAME.clickhouse.cloud \
  --port 9440 \
  --user default \
  --password PASSWORD \
  --query "INSERT INTO cell_towers FORMAT CSVWithNames" \
  < cell_towers.csv
```

<div id="more-examples">
  ### 명령줄에서 데이터를 삽입하는 예시
</div>

명령줄에서 데이터를 삽입하는 방법은 여러 가지가 있습니다.
아래 예시는 배치 모드를 사용하여 2개의 CSV 데이터 행을 ClickHouse 테이블(table)에 삽입합니다:

```bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | \
  clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

아래 예시에서 `cat <<_EOF`는 `_EOF`가 다시 나타날 때까지 모든 내용을 읽는 Heredoc을 시작하고, затем 이를 출력합니다:

```bash
cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF
```

아래 예시에서는 `cat`을 사용해 file.csv의 내용을 stdout으로 출력한 다음, 이를 입력으로 `clickhouse-client`에 파이프로 전달합니다:

```bash
cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

배치 모드에서는 기본 데이터 [포맷](formats.md)이 `TabSeparated`입니다.
위 예시와 같이 쿼리의 `FORMAT` 절에서 포맷을 설정할 수 있습니다.

<div id="cli-queries-with-parameters">
  ## 매개변수가 있는 쿼리
</div>

쿼리에서 매개변수를 지정하고 명령줄 옵션으로 해당 값을 전달할 수 있습니다.
이렇게 하면 클라이언트 측에서 특정 동적 값을 사용해 쿼리를 포맷할 필요가 없습니다.
예시는 다음과 같습니다.

```bash
$ clickhouse-client --param_parName="[1, 2]" --query "SELECT {parName: Array(UInt16)}"
[1,2]
```

[대화형 세션](#interactive-mode)에서 매개변수를 설정할 수도 있습니다:

```text
$ clickhouse-client
ClickHouse client version 25.X.X.XXX (official build).

#highlight-next-line
:) SET param_parName='[1, 2]';

SET param_parName = '[1, 2]'

Query id: 7ac1f84e-e89a-4eeb-a4bb-d24b8f9fd977

Ok.

0 rows in set. Elapsed: 0.000 sec.

#highlight-next-line
:) SELECT {parName:Array(UInt16)}

SELECT {parName:Array(UInt16)}

Query id: 0358a729-7bbe-4191-bb48-29b063c548a7

   ┌─_CAST([1, 2]⋯y(UInt16)')─┐
1. │ [1,2]                    │
   └──────────────────────────┘

1 row in set. Elapsed: 0.006 sec.
```

<div id="cli-queries-with-parameters-syntax">
  ### 쿼리 구문
</div>

쿼리에서는 명령줄 매개변수로 채울 값을 다음 형식으로 중괄호 안에 넣습니다:

```sql
{<name>:<data type>}
```

| 매개변수        | 설명                                                                                                                                                                                                                                                                                                                                                    |
| ----------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`      | 플레이스홀더 식별자입니다. 해당 명령줄 옵션은 `--param_<name> = value`입니다.                                                                                                                                                                                                                                                                                                |
| `data type` | 매개변수의 [데이터 타입](../sql-reference/data-types/index.md)입니다. <br /><br />예를 들어, `(integer, ('string', integer))`와 같은 데이터 구조에는 `Tuple(UInt8, Tuple(String, UInt8))` 데이터 타입을 사용할 수 있습니다(다른 [integer](../sql-reference/data-types/int-uint.md) 타입도 사용할 수 있습니다). <br /><br />테이블 이름, 데이터베이스 이름, 컬럼 이름도 매개변수로 전달할 수 있으며, 이 경우 데이터 타입으로 `Identifier`를 사용해야 합니다. |

<div id="cli-queries-with-parameters-examples">
  ### 예시
</div>

```bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" \
    --query "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"

$ clickhouse-client --param_tbl="numbers" --param_db="system" --param_col="number" --param_alias="top_ten" \
    --query "SELECT {col:Identifier} as {alias:Identifier} FROM {db:Identifier}.{tbl:Identifier} LIMIT 10"
```

<div id="ai-sql-generation">
  ## AI 기반 SQL 생성
</div>

clickhouse client에는 자연어 설명을 바탕으로 SQL 쿼리를 생성하는 AI 지원 기능이 내장되어 있습니다. 이 기능을 사용하면 SQL에 대한 깊은 지식이 없어도 복잡한 쿼리를 작성할 수 있습니다.

`OPENAI_API_KEY` 또는 `ANTHROPIC_API_KEY` 환경 변수가 설정되어 있으면 AI 지원 기능이 별도 구성 없이 바로 작동합니다. 더 고급 구성이 필요하면 [구성](#ai-sql-generation-configuration) 섹션을 참조하십시오.

<div id="ai-sql-generation-usage">
  ### 사용 방법
</div>

AI SQL generation을 사용하려면 자연어 쿼리 앞에 `??`를 붙이십시오:

```bash
:) ?? show all users who made purchases in the last 30 days
```

AI는 다음을 수행합니다:

1. 데이터베이스 스키마(schema)를 자동으로 분석합니다
2. 발견된 테이블과 컬럼을 바탕으로 적절한 SQL을 생성합니다
3. 생성된 쿼리를 즉시 실행합니다

<div id="cli-queries-with-parameters-examples">
  ### 예시
</div>

```bash
:) ?? count orders by product category

Starting AI SQL generation with schema discovery...
──────────────────────────────────────────────────

🔍 list_databases
   ➜ system, default, sales_db

🔍 list_tables_in_database
   database: sales_db
   ➜ orders, products, categories

🔍 get_schema_for_table
   database: sales_db
   table: orders
   ➜ CREATE TABLE orders (order_id UInt64, product_id UInt64, quantity UInt32, ...)

✨ SQL query generated successfully!
──────────────────────────────────────────────────

SELECT
    c.name AS category,
    COUNT(DISTINCT o.order_id) AS order_count
FROM sales_db.orders o
JOIN sales_db.products p ON o.product_id = p.product_id
JOIN sales_db.categories c ON p.category_id = c.category_id
GROUP BY c.name
ORDER BY order_count DESC
```

<div id="ai-sql-generation-configuration">
  ### 구성
</div>

AI SQL 생성 기능을 사용하려면 ClickHouse Client 설정 파일에서 AI 프로바이더를 구성해야 합니다. OpenAI, Anthropic 또는 OpenAI 호환 API 서비스 중 하나를 사용할 수 있습니다.

<div id="ai-sql-generation-fallback">
  #### 환경 변수 기반 폴백
</div>

설정 파일에 AI 구성이 지정되어 있지 않으면, ClickHouse Client는 자동으로 환경 변수를 사용합니다:

1. 먼저 `OPENAI_API_KEY` 환경 변수를 확인합니다
2. 없으면 `ANTHROPIC_API_KEY` 환경 변수를 확인합니다
3. 둘 다 없으면 AI 기능이 비활성화됩니다

따라서 설정 파일 없이도 빠르게 설정할 수 있습니다:

```bash
# Using OpenAI
export OPENAI_API_KEY=your-openai-key
clickhouse-client

# Using Anthropic
export ANTHROPIC_API_KEY=your-anthropic-key
clickhouse-client
```

<div id="ai-sql-generation-configuration-file">
  #### 설정 파일
</div>

AI 설정을 더 세밀하게 제어하려면 다음 위치에 있는 ClickHouse Client 설정 파일에서 구성하십시오:

* `$XDG_CONFIG_HOME/clickhouse/config.xml` (`XDG_CONFIG_HOME`이 설정되지 않은 경우 `~/.config/clickhouse/config.xml`) (XML 포맷)
* `$XDG_CONFIG_HOME/clickhouse/config.yaml` (`XDG_CONFIG_HOME`이 설정되지 않은 경우 `~/.config/clickhouse/config.yaml`) (YAML 포맷)
* `~/.clickhouse-client/config.xml` (XML 포맷, 기존 위치)
* `~/.clickhouse-client/config.yaml` (YAML 포맷, 기존 위치)
* 또는 `--config-file`로 사용자 지정 위치를 지정하십시오

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <ai>
            <!-- 필수: API Key (또는 환경 변수로 설정) -->
            <api_key>your-api-key-here</api_key>

            <!-- 필수: 프로바이더 유형 (openai, anthropic) -->
            <provider>openai</provider>

            <!-- 사용할 모델 (기본값은 프로바이더에 따라 다름) -->
            <model>gpt-4o</model>

            <!-- 선택 사항: OpenAI 호환 서비스용 사용자 지정 API 엔드포인트 -->
            <!-- <base_url>https://openrouter.ai/api</base_url> -->

            <!-- 스키마 탐색 설정 -->
            <enable_schema_access>true</enable_schema_access>

            <!-- 생성 매개변수 -->
            <!-- 선택 사항: temperature는 여기에서 설정한 경우에만 모델로 전송됩니다.
                 일부 모델은 이 매개변수를 허용하지 않으므로 기본적으로 생략됩니다. -->
            <!-- <temperature>0.0</temperature> -->
            <max_tokens>1000</max_tokens>
            <timeout_seconds>30</timeout_seconds>
            <max_steps>10</max_steps>

            <!-- 선택 사항: 사용자 지정 시스템 프롬프트 -->
            <!-- <system_prompt>You are an expert ClickHouse SQL assistant...</system_prompt> -->
        </ai>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    ai:
      # 필수: API Key (또는 환경 변수로 설정)
      api_key: your-api-key-here

      # 필수: 프로바이더 유형 (openai, anthropic)
      provider: openai

      # 사용할 모델
      model: gpt-4o

      # 선택 사항: OpenAI 호환 서비스용 사용자 지정 API 엔드포인트
      # base_url: https://openrouter.ai/api

      # 스키마 접근 활성화 - AI가 데이터베이스/테이블 정보를 쿼리할 수 있도록 허용
      enable_schema_access: true

      # 생성 매개변수
      # temperature는 여기에서 설정한 경우에만 모델로 전송되며, 기본적으로는 생략됩니다.
      # 일부 모델은 이 매개변수를 허용하지 않기 때문입니다.
      # temperature: 0.0    # 무작위성 제어 (0.0 = 결정적)
      max_tokens: 1000      # 최대 응답 길이
      timeout_seconds: 30   # 요청 timeout
      max_steps: 10         # 최대 스키마 탐색 단계 수

      # 선택 사항: 사용자 지정 시스템 프롬프트
      # system_prompt: |
      #   귀하는 ClickHouse SQL 전문가 어시스턴트입니다. 자연어를 SQL로 변환하십시오.
      #   성능에 중점을 두고 ClickHouse 전용 최적화를 사용하십시오.
      #   설명 없이 항상 실행 가능한 SQL만 반환하십시오.
    ```
  </TabItem>
</Tabs>

<br />

**OpenAI 호환 API 사용(예: OpenRouter):**

```yaml
ai:
  provider: openai  # Use 'openai' for compatibility
  api_key: your-openrouter-api-key
  base_url: https://openrouter.ai/api/v1
  model: anthropic/claude-3.5-sonnet  # Use OpenRouter model naming
```

**최소 구성 예시:**

```yaml
# Minimal config - uses environment variable for API key
ai:
  provider: openai  # Will use OPENAI_API_KEY env var

# No config at all - automatic fallback
# (Empty or no ai section - will try OPENAI_API_KEY then ANTHROPIC_API_KEY)

# Only override model - uses env var for API key
ai:
  provider: openai
  model: gpt-3.5-turbo
```

<div id="ai-sql-generation-parameters">
  ### 매개변수
</div>

<details>
  <summary>필수 매개변수</summary>

  * `api_key` - AI 서비스용 API Key입니다. 환경 변수로 설정한 경우 생략할 수 있습니다:
    * OpenAI: `OPENAI_API_KEY`
    * Anthropic: `ANTHROPIC_API_KEY`
    * 참고: 구성 파일의 API Key가 환경 변수보다 우선합니다
  * `provider` - AI 프로바이더입니다: `openai` 또는 `anthropic`
    * 생략하면 사용 가능한 환경 변수를 기준으로 자동 폴백을 사용합니다
</details>

<details>
  <summary>모델 구성</summary>

  * `model` - 사용할 모델입니다(기본값: 프로바이더별 기본 모델)
    * OpenAI: `gpt-4o`, `gpt-4`, `gpt-3.5-turbo` 등
    * Anthropic: `claude-3-5-sonnet-20241022`, `claude-3-opus-20240229` 등
    * OpenRouter: `anthropic/claude-3.5-sonnet`과 같은 모델 이름을 사용합니다
</details>

<details>
  <summary>연결 설정</summary>

  * `base_url` - OpenAI 호환 서비스용 사용자 지정 API 엔드포인트입니다(선택 사항)
  * `timeout_seconds` - 요청 timeout 시간(초)입니다(기본값: `30`)
</details>

<details>
  <summary>스키마 탐색</summary>

  * `enable_schema_access` - AI가 데이터베이스 스키마를 탐색하도록 허용합니다(기본값: `true`)
  * `max_steps` - 스키마 탐색을 위한 최대 도구 호출 단계 수입니다(기본값: `10`)
</details>

<details>
  <summary>생성 매개변수</summary>

  * `temperature` - 샘플링 온도를 제어합니다. 0.0 = 결정적, 1.0 = 창의적입니다. 일부 모델은 이 매개변수를 허용하지 않으므로 기본적으로는 생략되며, 명시적으로 설정한 경우에만 모델로 전송됩니다.
  * `max_tokens` - 토큰 기준 최대 응답 길이입니다(기본값: `1000`)
  * `system_prompt` - AI용 사용자 지정 지침입니다(선택 사항)
</details>

<div id="ai-sql-generation-how-it-works">
  ### 작동 방식
</div>

AI SQL 생성기는 여러 단계에 걸쳐 작동합니다:

<VerticalStepper headerLevel="list">
  1. **스키마 탐색**

  AI는 기본 제공 도구를 사용해 데이터베이스를 탐색합니다

  * 사용 가능한 데이터베이스를 나열합니다
  * 관련 데이터베이스 내 테이블을 확인합니다
  * `CREATE TABLE` SQL 문을 통해 테이블 구조를 살펴봅니다

  2. **쿼리 생성**

  탐색한 스키마를 바탕으로 AI는 다음과 같은 SQL을 생성합니다:

  * 자연어로 표현한 의도에 맞습니다
  * 올바른 테이블명과 컬럼명을 사용합니다
  * 적절한 조인 및 집계를 적용합니다

  3. **실행**

  생성된 SQL은 자동으로 실행되며 결과가 표시됩니다
</VerticalStepper>

<div id="ai-sql-generation-limitations">
  ### 제한 사항
</div>

* 활성화된 인터넷 연결이 필요합니다
* API 사용에는 AI 프로바이더의 속도 제한과 비용이 적용됩니다
* 복잡한 쿼리는 여러 차례의 수정이 필요할 수 있습니다
* AI는 실제 데이터가 아니라 스키마 정보에만 읽기 전용으로 접근할 수 있습니다

<div id="ai-sql-generation-security">
  ### 보안
</div>

* API Key는 절대 ClickHouse 서버로 전송되지 않습니다
* AI는 실제 데이터가 아니라 스키마 정보(테이블/컬럼 이름 및 타입)만 확인합니다
* 생성된 모든 쿼리는 기존 데이터베이스 권한을 준수합니다

<div id="connection_string">
  ## 연결 문자열
</div>

<div id="connection-string-usage">
  ### 사용법
</div>

ClickHouse Client는 [MongoDB](https://www.mongodb.com/docs/manual/reference/connection-string/), [PostgreSQL](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING), [MySQL](https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html#connecting-using-uri)와 유사한 연결 문자열로 ClickHouse 서버에 연결할 수도 있습니다. 구문은 다음과 같습니다:

```text
clickhouse:[//[user[:password]@][hosts_and_ports]][/database][?query_parameters]
```

| 구성 요소(모두 선택 사항)    | 설명                                                                                                  | 기본값              |
| ------------------ | --------------------------------------------------------------------------------------------------- | ---------------- |
| `user`             | 데이터베이스 사용자 이름입니다.                                                                                   | `default`        |
| `password`         | 데이터베이스 사용자 비밀번호입니다. `:`가 지정되어 있고 비밀번호가 비어 있으면 클라이언트가 사용자 비밀번호 입력을 요청합니다.                            | -                |
| `hosts_and_ports`  | 호스트와 선택적 포트의 목록입니다. `host[:port] [, host:[port]], ...`                                              | `localhost:9000` |
| `database`         | 데이터베이스 이름입니다.                                                                                       | `default`        |
| `query_parameters` | 키-값 쌍의 목록입니다. `param1=value1[,&param2=value2], ...` 일부 매개변수는 값이 필요하지 않습니다. 매개변수 이름과 값은 대소문자를 구분합니다. | -                |

<div id="connection-string-notes">
  ### 참고 사항
</div>

연결 문자열에 username, password 또는 데이터베이스를 지정한 경우 `--user`, `--password` 또는 `--database`로는 지정할 수 없습니다(반대의 경우도 마찬가지입니다).

host 구성 요소는 호스트명, IPv4 주소 또는 IPv6 주소일 수 있습니다.
IPv6 주소는 `[]`로 감싸야 합니다:

```text
clickhouse://[2001:db8::1234]
```

연결 문자열에는 여러 호스트를 포함할 수 있습니다.
ClickHouse Client는 이 호스트들에 순서대로(왼쪽에서 오른쪽으로) 연결을 시도합니다.
연결이 설정되면 나머지 호스트에는 더 이상 연결을 시도하지 않습니다.

연결 문자열은 `clickHouse-client`의 첫 번째 인수로 지정해야 합니다.
연결 문자열은 `--host` 및 `--port`를 제외한 임의 개수의 다른 [명령줄 옵션](#command-line-options)과 함께 사용할 수 있습니다.

`query_parameters`에 허용되는 키는 다음과 같습니다.

| 키                 | 설명                                                                                         |
| ----------------- | ------------------------------------------------------------------------------------------ |
| `secure` (or `s`) | 지정하면 클라이언트는 보안 연결(TLS)을 통해 서버에 연결합니다. [명령줄 옵션](#command-line-options)의 `--secure`를 참조하십시오. |

**퍼센트 인코딩**

다음 매개변수에 포함된 미국 외 ASCII 문자, 공백 및 특수 문자는 [퍼센트 인코딩](https://en.wikipedia.org/wiki/URL_encoding)해야 합니다.

* `user`
* `password`
* `hosts`
* `database`
* `query parameters`

<div id="cli-queries-with-parameters-examples">
  ### 예시
</div>

`localhost`의 9000 포트에 연결한 다음 쿼리 `SELECT 1`을 실행합니다.

```bash
clickhouse-client clickhouse://localhost:9000 --query "SELECT 1"
```

`localhost`에 사용자 `john`, 비밀번호 `secret`, 호스트 `127.0.0.1`, 포트 `9000`으로 연결합니다

```bash
clickhouse-client clickhouse://john:secret@127.0.0.1:9000
```

`default` 사용자로 `localhost`에 연결하고, 호스트는 IPv6 주소 `[::1]`, 포트는 `9000`을 사용합니다.

```bash
clickhouse-client clickhouse://[::1]:9000
```

`localhost`의 9000번 포트에 여러 줄 모드로 연결합니다.

```bash
clickhouse-client clickhouse://localhost:9000 '-m'
```

사용자 `default`로 `localhost`의 9000번 포트에 연결합니다.

```bash
clickhouse-client clickhouse://default@localhost:9000

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --user default
```

포트 9000의 `localhost`에 연결하고, 기본 데이터베이스로 `my_database`를 사용합니다.

```bash
clickhouse-client clickhouse://localhost:9000/my_database

# equivalent to:
clickhouse-client clickhouse://localhost:9000 --database my_database
```

포트 9000의 `localhost`에 연결하고, 연결 문자열에 지정된 `my_database` 데이터베이스를 기본으로 사용하며, 축약형 `s` 매개변수로 보안 연결을 사용합니다.

```bash
clickhouse-client clickhouse://localhost/my_database?s

# equivalent to:
clickhouse-client clickhouse://localhost/my_database -s
```

기본 포트, `default` 사용자 및 기본 데이터베이스를 사용해 기본 호스트에 연결합니다.

```bash
clickhouse-client clickhouse:
```

기본 호스트의 기본 포트에 `my_user` 사용자로 비밀번호 없이 연결합니다.

```bash
clickhouse-client clickhouse://my_user@

# Using a blank password between : and @ means to asking the user to enter the password before starting the connection.
clickhouse-client clickhouse://my_user:@
```

이메일 주소를 사용자 이름으로 사용해 `localhost`에 연결합니다. `@` 기호는 퍼센트 인코딩되어 `%40`으로 변환됩니다.

```bash
clickhouse-client clickhouse://some_user%40some_mail.com@localhost:9000
```

`192.168.1.15`, `192.168.1.25` 두 호스트 중 하나에 연결하세요.

```bash
clickhouse-client clickhouse://192.168.1.15,192.168.1.25
```

<div id="query-id-format">
  ## 쿼리 ID 포맷
</div>

대화형 모드에서 ClickHouse Client는 각 쿼리의 쿼리 ID를 표시합니다. 기본적으로 ID는 다음과 같이 포맷됩니다:

```sql
Query id: 927f137d-00f1-4175-8914-0dd066365e96
```

사용자 지정 포맷은 설정 파일의 `query_id_formats` 태그 내에서 지정할 수 있습니다. 포맷 문자열의 `{query_id}` 플레이스홀더는 쿼리 ID로 대체됩니다. 태그 안에는 여러 개의 포맷 문자열을 지정할 수 있습니다.
이 기능은 쿼리 프로파일링을 더 쉽게 할 수 있도록 URL을 생성하는 데 사용할 수 있습니다.

**예시**

```xml
<config>
  <query_id_formats>
    <speedscope>http://speedscope-host/#profileURL=qp%3Fid%3D{query_id}</speedscope>
  </query_id_formats>
</config>
```

위 구성에서는 쿼리 ID가 다음 포맷으로 표시됩니다:

```response
speedscope:http://speedscope-host/#profileURL=qp%3Fid%3Dc8ecc783-e753-4b38-97f1-42cddfb98b7d
```

<div id="configuration_files">
  ## 설정 파일
</div>

ClickHouse Client는 다음 파일 중 존재하는 첫 번째 파일을 사용합니다.

* `-c [ -C, --config, --config-file ]` 매개변수로 지정한 파일
* `./clickhouse-client.[xml|yaml|yml]`
* `$XDG_CONFIG_HOME/clickhouse/config.[xml|yaml|yml]` (`XDG_CONFIG_HOME`이 설정되지 않은 경우 `~/.config/clickhouse/config.[xml|yaml|yml]`)
* `~/.clickhouse-client/config.[xml|yaml|yml]`
* `/etc/clickhouse-client/config.[xml|yaml|yml]`

ClickHouse 리포지토리에서 예시 설정 파일을 참조하십시오: [`clickhouse-client.xml`](https://github.com/ClickHouse/ClickHouse/blob/master/programs/client/clickhouse-client.xml)

<Tabs>
  <TabItem value="xml" label="XML" default>
    ```xml
    <config>
        <user>username</user>
        <password>password</password>
        <secure>true</secure>
        <openSSL>
          <client>
            <caConfig>/etc/ssl/cert.pem</caConfig>
          </client>
        </openSSL>
    </config>
    ```
  </TabItem>

  <TabItem value="yaml" label="YAML">
    ```yaml
    user: username
    password: 'password'
    secure: true
    openSSL:
      client:
        caConfig: '/etc/ssl/cert.pem'
    ```
  </TabItem>
</Tabs>

<div id="environment-variable-options">
  ## 환경 변수 옵션
</div>

사용자 이름, 비밀번호, 호스트는 환경 변수 `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_HOST`를 통해 설정할 수 있습니다.
명령줄 인수 `--user`, `--password`, `--host` 또는 [연결 문자열](#connection_string)(지정된 경우)이 환경 변수보다 우선 적용됩니다.

<div id="command-line-options">
  ## 명령줄 옵션
</div>

모든 명령줄 옵션은 명령줄에서 직접 지정하거나 [설정 파일](#configuration_files)에 기본값으로 지정할 수 있습니다.

<div id="command-line-options-general">
  ### 일반 옵션
</div>

| 옵션                                                  | 설명                                                                                | 기본값                      |
| --------------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------ |
| `-c [ -C, --config, --config-file ] <path-to-file>` | 클라이언트의 설정 파일이 기본 위치에 없으면 해당 파일의 위치를 지정합니다. [설정 파일](#configuration_files)을 참조하십시오. | -                        |
| `--help`                                            | 사용법 요약을 출력한 후 종료합니다. `--verbose`와 함께 사용하면 쿼리 설정을 포함한 모든 옵션을 표시합니다.                | -                        |
| `--history_file <path-to-file>`                     | 명령 이력이 저장된 파일의 경로입니다.                                                             | -                        |
| `--history_max_entries`                             | 이력 파일에 저장할 수 있는 최대 항목 수입니다.                                                       | `1000000` (100만)         |
| `--prompt <prompt>`                                 | 사용자 지정 프롬프트를 지정합니다.                                                               | server의 `display_name` 값 |
| `--verbose`                                         | 출력 상세 수준을 높입니다.                                                                   | -                        |
| `-V [ --version ]`                                  | 버전을 출력한 후 종료합니다.                                                                  | -                        |

<div id="command-line-options-connection">
  ### 연결 옵션
</div>

| Option                               | Description                                                                                                                                                                                                                                                        | Default                                                                                         |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| `--connection <name>`                | 설정 파일에 미리 구성된 연결 정보의 이름입니다. [연결 자격 증명](#connection-credentials)을 참조하십시오.                                                                                                                                                                                           | -                                                                                               |
| `-d [ --database ] <database>`       | 이 연결의 기본 데이터베이스로 사용할 데이터베이스를 선택합니다.                                                                                                                                                                                                                                | 서버 설정의 현재 데이터베이스(`default`가 기본값)                                                                |
| `-h [ --host ] <host>`               | 연결할 ClickHouse 서버의 호스트명입니다. 호스트명, IPv4 주소 또는 IPv6 주소를 사용할 수 있습니다. 여러 인수로 여러 호스트를 전달할 수 있습니다.                                                                                                                                                                       | `localhost`                                                                                     |
| `--jwt <value>`                      | 인증에 JSON Web Token (JWT)을 사용합니다. <br /><br />서버 JWT 인증은 ClickHouse Cloud에서만 사용할 수 있습니다.                                                                                                                                                                            | -                                                                                               |
| `login`                              | IdP를 통해 인증할 수 있도록 device grant OAuth flow를 시작합니다. <br /><br />ClickHouse Cloud 호스트의 경우 OAuth 변수는 자동으로 추론됩니다. 그렇지 않은 경우에는 `--oauth-url`, `--oauth-client-id`, `--oauth-audience`를 지정해야 합니다.                                                                         | -                                                                                               |
| `--no-warnings`                      | 클라이언트가 서버에 연결할 때 `system.warnings`의 경고를 표시하지 않습니다.                                                                                                                                                                                                                 | -                                                                                               |
| `--no-server-client-version-message` | 클라이언트가 서버에 연결할 때 서버와 클라이언트 간 버전 불일치 메시지를 숨깁니다.                                                                                                                                                                                                                     | -                                                                                               |
| `--password <password>`              | 데이터베이스 사용자의 비밀번호입니다. 설정 파일에서도 연결의 비밀번호를 지정할 수 있습니다. 비밀번호를 지정하지 않으면 클라이언트가 입력을 요청합니다.                                                                                                                                                                               | -                                                                                               |
| `--port <port>`                      | 서버가 연결을 수락하는 포트입니다. 기본 포트는 9440(TLS) 및 9000(TLS 미사용)입니다. <br /><br />참고: 클라이언트는 HTTP(S)가 아니라 네이티브 프로토콜을 사용합니다.                                                                                                                                                     | `--secure`를 지정한 경우 `9440`, 그렇지 않으면 `9000`입니다. 호스트명이 `.clickhouse.cloud`로 끝나면 기본값은 항상 `9440`입니다. |
| `-s [ --secure ]`                    | TLS 사용 여부를 지정합니다. <br /><br />포트 9440(기본 보안 포트) 또는 ClickHouse Cloud에 연결할 때는 자동으로 활성화됩니다. <br /><br />[설정 파일](#configuration_files)에서 CA 인증서를 구성해야 할 수 있습니다. 사용 가능한 구성 설정은 [서버 측 TLS 구성](../operations/server-configuration-parameters/settings.md#openssl)과 동일합니다. | 포트 9440 또는 ClickHouse Cloud에 연결할 때 자동으로 활성화됩니다                                                  |
| `--ssh-key-file <path-to-file>`      | 서버 인증에 사용할 SSH private key가 포함된 파일입니다.                                                                                                                                                                                                                             | -                                                                                               |
| `--ssh-key-passphrase <value>`       | `--ssh-key-file`에 지정한 SSH private key의 패스프레이스입니다.                                                                                                                                                                                                                  | -                                                                                               |
| `--tls-sni-override <server name>`   | TLS를 사용하는 경우 핸드셰이크에서 전달할 서버 이름(SNI)입니다.                                                                                                                                                                                                                            | `-h` 또는 `--host`로 지정한 호스트입니다.                                                                   |
| `-u [ --user ] <username>`           | 연결에 사용할 데이터베이스 사용자입니다.                                                                                                                                                                                                                                             | `default`                                                                                       |

:::note
클라이언트는 `--host`, `--port`, `--user`, `--password` 옵션 대신 [연결 문자열](#connection_string)도 지원합니다.
:::

<div id="command-line-options-query">
  ### 쿼리 옵션
</div>

| 옵션                              | 설명                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--param_<name>=<value>`        | [매개변수가 있는 쿼리](#cli-queries-with-parameters)의 매개변수에 대한 치환 값입니다.                                                                                                                                                                                                                                                                                                                                                                    |
| `-q [ --query ] <query>`        | 배치 모드에서 실행할 쿼리입니다. 여러 번 지정할 수 있으며(`--query "SELECT 1" --query "SELECT 2"`), 세미콜론으로 구분된 여러 쿼리를 한 번에 지정할 수도 있습니다(`--query "SELECT 1; SELECT 2;"`). 후자의 경우 `VALUES` 이외의 포맷을 사용하는 `INSERT` 쿼리는 빈 줄로 구분해야 합니다. <br /><br />매개변수 없이 단일 쿼리를 지정할 수도 있습니다: `clickhouse-client "SELECT 1"` <br /><br />`--queries-file`과 함께 사용할 수 없습니다.                                                                                                     |
| `--queries-file <path-to-file>` | 쿼리가 포함된 파일의 경로입니다. `--queries-file`은 여러 번 지정할 수 있습니다. 예: `--queries-file queries1.sql --queries-file queries2.sql`. <br /><br />`--query`와 함께 사용할 수 없습니다.                                                                                                                                                                                                                                                                         |
| `-m [ --multiline ]`            | 지정하면 여러 줄 쿼리를 허용합니다(Enter를 눌러도 쿼리가 전송되지 않음). 쿼리는 세미콜론으로 끝날 때만 전송됩니다.                                                                                                                                                                                                                                                                                                                                                              |
| `--inline-insert-data`          | 데이터를 Native 형식의 블록으로 변환하지 않고, `INSERT ... VALUES`(및 기타 인라인 포맷)를 쿼리 텍스트에 있는 그대로 전송합니다. 서버가 인라인 데이터를 직접 파싱하므로, 테이블 구조와 컬럼 기본값을 클라이언트로 다시 보내는 round-trip을 피할 수 있습니다. 이는 네이티브 프로토콜을 통해 작은 삽입을 많이 수행할 때 성능을 높일 수 있습니다. [`send_table_structure_on_insert_with_inline_data`](/ko/operations/settings/settings#send_table_structure_on_insert_with_inline_data)를 자동으로 `0`으로 설정합니다. 인라인 데이터와 외부 데이터(`stdin` 또는 `INFILE`의 데이터)는 함께 사용할 수 없습니다. |

<div id="command-line-options-query-settings">
  ### 쿼리 설정
</div>

쿼리 설정은 클라이언트의 명령줄 옵션으로 지정할 수 있습니다. 예시는 다음과 같습니다.

```bash
$ clickhouse-client --max_threads 1
```

[설정](../operations/settings/settings.md)에서 설정 목록을 확인하십시오.

<div id="command-line-options-formatting">
  ### 포맷 옵션
</div>

| Option                            | Description                                                                                                                                                                                                                                                                                                                                 | Default                                  |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `-f [ --format ] <format>`        | 지정한 포맷으로 결과를 출력합니다. <br /><br />지원되는 포맷 목록은 [입력 및 출력 데이터용 포맷](formats.md)을 참조하십시오.                                                                                                                                                                                                                                                          | `TabSeparated`                           |
| `--pager <command>`               | 모든 출력을 이 명령으로 파이프합니다. 일반적으로 `less`(예: 넓은 결과 집합을 표시할 때 `less -S`) 또는 이와 유사한 명령을 사용합니다.                                                                                                                                                                                                                                                       | -                                        |
| `-E [ --vertical ]`               | 결과를 출력할 때 [Vertical 형식](/ko/interfaces/formats/Vertical)을 사용합니다. 이는 `–-format Vertical`과 동일합니다. 이 포맷에서는 각 값이 별도의 줄에 출력되므로 넓은 테이블을 표시할 때 유용합니다.                                                                                                                                                                                                 | -                                        |
| `--echo [ <bool> ]`               | 실행 전에 각 쿼리를 출력합니다. 선택적 불리언 값을 받을 수 있습니다.                                                                                                                                                                                                                                                                                                    | 대화형 모드에서는 `true`, 비대화형(배치) 모드에서는 `false` |
| `--echo-formatted [ <bool> ]`     | 에코로 출력되는 쿼리를 포맷합니다. 선택적 불리언 값을 받을 수 있습니다.                                                                                                                                                                                                                                                                                                   | 대화형 모드에서는 `true`, 비대화형(배치) 모드에서는 `false` |
| `--echo-query-id [ <bool> ]`      | 실행 전에 쿼리 id를 출력합니다. 선택적 불리언 값을 받을 수 있습니다.                                                                                                                                                                                                                                                                                                   | 대화형 모드에서는 `true`, 비대화형(배치) 모드에서는 `false` |
| `--echo-query-separator <string>` | 포맷된 에코 쿼리 앞에 이 구분자를 출력합니다(`--echo-formatted` 필요). 이렇게 하면 직접 입력한 쿼리와 다시 포맷된 에코 출력을 더 쉽게 구분할 수 있습니다.                                                                                                                                                                                                                                          | 비어 있음(비활성화됨)                             |
| `--highlight [ --hilite ] <bool>` | 명령 프롬프트와 에코로 출력되는 쿼리의 구문 강조를 전환합니다.                                                                                                                                                                                                                                                                                                         | `true`                                   |
| `--hints <bool>`                  | 커서가 입력 끝에 있을 때 가장 잘 일치하는 제안에 대한 입력 중 자동 완성 힌트(인라인 &quot;ghost&quot; 텍스트)를 표시합니다. 위/아래(또는 Ctrl-Up/Ctrl-Down)로 힌트를 탐색하고, Tab 또는 Right로 인라인 힌트를 수락할 수 있습니다. `Enter`는 힌트가 명시적으로 선택된 경우에만 이를 수락하고, 그렇지 않으면 쿼리를 실행합니다. `Tab`은 기존 완성 목록도 엽니다. 이 기능을 사용하려면 `--highlight`(힌트에 색상이 필요함)와 제안 기능이 필요하며, 따라서 `--disable_suggestion`을 사용하면 이 기능도 비활성화됩니다. | `true`                                   |

<div id="command-line-options-execution-details">
  ### 실행 세부 정보
</div>

| 옵션                               | 설명                                                                                                                                                                                                                                                                             | 기본값                                   |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------- |
| `--chime [N]`                    | 쿼리가 최소 `N`초 이상 실행된 뒤 종료되면(성공 및 오류 모두 포함) `BEL` 제어 문자를 `stderr`에 기록합니다. `stderr`가 터미널(TTY)에 연결된 경우에만 출력됩니다. `stderr`를 리디렉션하면(예: `2>err.log`) 출력이 억제되지만, `stdout`을 리디렉션하는 경우(예: `> result.tsv`)에는 영향을 받지 않습니다. 값 없이 `--chime`를 지정하면 기본 임계값을 사용합니다. 비활성화하려면 `--chime 0`으로 설정하십시오. | `5`초                                  |
| `--enable-progress-table-toggle` | 제어 키(Space)를 눌러 진행률 테이블 표시를 전환할 수 있도록 합니다. 진행률 테이블 출력이 활성화된 대화형 모드에서만 적용됩니다.                                                                                                                                                                                                   | `enabled`                             |
| `--hardware-utilization`         | 진행률 표시줄에 하드웨어 사용률 정보를 출력합니다.                                                                                                                                                                                                                                                   | -                                     |
| `--memory-usage`                 | 지정하면 비대화형 모드에서 메모리 사용량을 `stderr`에 출력합니다. <br /><br />가능한 값: <br />• `none` - 메모리 사용량을 출력하지 않음 <br />• `default` - 바이트 수를 출력함 <br />• `readable` - 사람이 읽기 쉬운 포맷으로 메모리 사용량을 출력함                                                                                                  | -                                     |
| `--print-profile-events`         | `ProfileEvents` 패킷을 출력합니다.                                                                                                                                                                                                                                                     | -                                     |
| `--progress`                     | 쿼리 실행 진행률을 출력합니다. <br /><br />가능한 값: <br />• `tty\|on\|1\|true\|yes` - 대화형 모드에서 터미널로 출력합니다 <br />• `err` - 비대화형 모드에서 `stderr`로 출력합니다 <br />• `off\|0\|false\|no` - 진행률 출력을 비활성화합니다                                                                                             | 대화형 모드에서는 `tty`, 비대화형(배치) 모드에서는 `off` |
| `--progress-table`               | 쿼리 실행 중 변경되는 메트릭이 포함된 진행률 테이블을 출력합니다. <br /><br />가능한 값: <br />• `tty\|on\|1\|true\|yes` - 대화형 모드에서 터미널로 출력합니다 <br />• `err` - 비대화형 모드에서 `stderr`로 출력합니다 <br />• `off\|0\|false\|no` - 진행률 테이블을 비활성화합니다                                                                        | 대화형 모드에서는 `tty`, 비대화형(배치) 모드에서는 `off` |
| `--stacktrace`                   | 예외의 스택 트레이스를 출력합니다.                                                                                                                                                                                                                                                            | -                                     |
| `-t [ --time ]`                  | 비대화형 모드에서 `stderr`에 쿼리 실행 시간을 출력합니다(벤치마크용).                                                                                                                                                                                                                                    | -                                     |