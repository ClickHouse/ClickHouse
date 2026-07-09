---
description: '`executable` 테이블 함수는 **stdout**으로 행을 출력하는 스크립트에서 정의한 사용자 정의 함수(UDF)의 출력을 기반으로 테이블을 생성합니다.'
keywords: ['udf', '사용자 정의 함수', 'clickhouse', '실행형', '테이블', '함수']
sidebar_label: 'executable'
sidebar_position: 50
slug: /engines/table-functions/executable
title: 'executable'
doc_type: '참고'
---

`executable` 테이블 함수는 **stdout**으로 행을 출력하는 스크립트에서 정의한 사용자 정의 함수(UDF)의 출력을 기반으로 테이블을 생성합니다. 실행형 스크립트는 `users_scripts` 디렉터리에 저장되며, 어떤 소스에서든 데이터를 읽을 수 있습니다. 실행형 스크립트를 실행하는 데 필요한 모든 패키지가 ClickHouse 서버에 설치되어 있는지 확인하십시오. 예를 들어 Python 스크립트인 경우, 서버에 필요한 Python 패키지가 설치되어 있는지 확인하십시오.

선택적으로 하나 이상의 입력 쿼리를 포함할 수 있으며, 그 결과는 스크립트가 읽을 수 있도록 **stdin**으로 스트리밍됩니다.

:::note
일반 UDF 함수와 `executable` 테이블 함수 및 `Executable` 테이블 엔진의 주요 차이점은, 일반 UDF 함수는 행 수를 변경할 수 없다는 점입니다. 예를 들어 입력이 100행이면 결과도 반드시 100행을 반환해야 합니다. `executable` 테이블 함수 또는 `Executable` 테이블 엔진을 사용하면 복잡한 집계를 포함해 원하는 모든 데이터 변환을 스크립트에서 수행할 수 있습니다.
:::

<div id="syntax">
  ## 구문
</div>

`executable` 테이블 함수는 3개의 매개변수가 필요하며, 선택 사항으로 입력 쿼리 목록도 받을 수 있습니다.

```sql
executable(script_name, format, structure, [input_query...] [,SETTINGS ...])
```

* `script_name`: 스크립트 파일 이름입니다. `user_scripts` 폴더(`user_scripts_path` 설정의 기본 폴더)에 저장됩니다.
* `format`: 생성된 테이블의 포맷
* `structure`: 생성된 테이블의 테이블 스키마
* `input_query`: 결과가 **stdin**을 통해 스크립트에 전달되는 선택적 쿼리(또는 컬렉션이나 쿼리)

:::note
동일한 입력 쿼리로 같은 스크립트를 반복해서 호출할 경우 [`Executable` 테이블 엔진](../../engines/table-engines/special/executable.md) 사용을 고려하십시오.
:::

다음 Python 스크립트의 이름은 `generate_random.py`이며 `user_scripts` 폴더에 저장되어 있습니다. 이 스크립트는 숫자 `i`를 읽은 뒤, 각 문자열 앞에 탭으로 구분된 숫자를 붙여 무작위 문자열 `i`개를 출력합니다:

```python
#!/usr/local/bin/python3.9

import sys
import string
import random

def main():

    # Read input value
    for number in sys.stdin:
        i = int(number)

        # Generate some random rows
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # Flush results to stdout
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

스크립트를 실행해 무작위 문자열 10개를 생성해 보겠습니다:

```sql
SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', (SELECT 10))
```

응답은 다음과 같습니다:

```response
┌─id─┬─random─────┐
│  0 │ xheXXCiSkH │
│  1 │ AqxvHAoTrl │
│  2 │ JYvPCEbIkY │
│  3 │ sWgnqJwGRm │
│  4 │ fTZGrjcLon │
│  5 │ ZQINGktPnd │
│  6 │ YFSvGGoezb │
│  7 │ QyMJJZOOia │
│  8 │ NfiyDDhmcI │
│  9 │ REJRdJpWrg │
└────┴────────────┘
```

<div id="settings">
  ## 설정
</div>

* `send_chunk_header` - 처리할 데이터 청크를 보내기 전에 행 수를 전송할지 여부를 제어합니다. 기본값은 `false`입니다.
* `pool_size` — 풀 크기입니다. `pool_size`에 0을 지정하면 풀 크기 제한이 없습니다. 기본값은 `16`입니다.
* `max_command_execution_time` — 데이터 블록 처리 시 실행형 스크립트 명령의 최대 실행 시간입니다. 초 단위로 지정합니다. 기본값은 10입니다.
* `command_termination_timeout` — 실행형 스크립트에는 기본 읽기-쓰기 루프가 포함되어야 합니다. 테이블 함수가 제거되면 파이프가 닫히며, 실행 파일은 종료할 때까지 `command_termination_timeout`초 동안 기다립니다. 이 시간이 지나도 종료되지 않으면 ClickHouse가 자식 프로세스에 SIGTERM 신호를 보냅니다. 초 단위로 지정합니다. 기본값은 10입니다.
* `command_read_timeout` - 명령의 stdout에서 데이터를 읽을 때의 timeout이며, 밀리초 단위입니다. 기본값은 10000입니다.
* `command_write_timeout` - 명령의 stdin에 데이터를 쓸 때의 timeout이며, 밀리초 단위입니다. 기본값은 10000입니다.

<div id="passing-query-results-to-a-script">
  ## 쿼리 결과를 스크립트로 전달하기
</div>

[쿼리 결과를 스크립트로 전달하는 방법](../../engines/table-engines/special/executable.md#passing-query-results-to-a-script)은 `Executable` 테이블 엔진의 예시를 참고하십시오. 다음은 `executable` 테이블 함수를 사용해 해당 예시와 동일한 스크립트를 실행하는 방법입니다:

```sql
SELECT * FROM executable(
    'sentiment.py',
    TabSeparated,
    'id UInt64, sentiment Float32',
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```