---
slug: /sql-reference/statements/create/dictionary/sources/executable-file
title: '실행형 파일 딕셔너리 소스'
sidebar_position: 3
sidebar_label: '실행형 파일'
description: 'ClickHouse에서 실행형 파일을 딕셔너리 소스로 구성합니다.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

실행형 파일을 사용하는 방식은 [딕셔너리가 메모리에 저장되는 방식](../layouts/)에 따라 달라집니다. 딕셔너리가 `cache` 및 `complex_key_cache`로 저장된 경우, ClickHouse는 실행형 파일의 STDIN으로 요청을 보내 필요한 키를 조회합니다. 그렇지 않으면 ClickHouse는 실행형 파일을 실행하고, 그 출력을 딕셔너리 데이터로 간주합니다.

설정 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE(
        command 'cat /opt/dictionaries/os.tsv'
        format 'TabSeparated'
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <source>
        <executable>
            <command>cat /opt/dictionaries/os.tsv</command>
            <format>TabSeparated</format>
            <implicit_key>false</implicit_key>
        </executable>
    </source>
    ```
  </TabItem>
</Tabs>

설정 필드:

| Setting                       | Description                                                                                                                                                                                                                                                                                                               |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `command`                     | 실행형 파일의 절대 경로 또는 파일 이름입니다(`command`의 디렉터리가 `PATH`에 있는 경우).                                                                                                                                                                                                                                                                |
| `format`                      | 파일 포맷입니다. [Formats](/ko/sql-reference/formats)에 설명된 모든 포맷이 지원됩니다.                                                                                                                                                                                                                                                            |
| `command_termination_timeout` | 실행형 스크립트에는 메인 읽기-쓰기 루프가 포함되어야 합니다. 딕셔너리가 삭제되면 파이프가 닫히고, ClickHouse가 자식 프로세스에 SIGTERM 신호를 보내기 전까지 실행형 파일은 종료할 수 있도록 `command_termination_timeout`초의 시간이 주어집니다. 초 단위로 지정합니다. 기본값은 `10`입니다. 선택 사항입니다.                                                                                                                        |
| `command_read_timeout`        | 명령의 stdout에서 데이터를 읽을 때 적용되는 timeout이며, 밀리초 단위입니다. 기본값은 `10000`입니다. 선택 사항입니다.                                                                                                                                                                                                                                              |
| `command_write_timeout`       | 명령의 stdin에 데이터를 쓸 때 적용되는 timeout이며, 밀리초 단위입니다. 기본값은 `10000`입니다. 선택 사항입니다.                                                                                                                                                                                                                                                 |
| `implicit_key`                | 실행형 소스 파일은 값만 반환할 수 있으며, 요청된 키와의 대응 관계는 결과의 행 순서에 따라 암묵적으로 결정됩니다. 기본값은 `false`입니다.                                                                                                                                                                                                                                        |
| `execute_direct`              | `execute_direct` = `1`이면 `command`는 [user&#95;scripts&#95;path](/ko/operations/server-configuration-parameters/settings#user_scripts_path)로 지정된 user&#95;scripts 폴더에서 검색됩니다. 추가 스크립트 인수는 공백으로 구분해 지정할 수 있습니다. 예: `script_name arg1 arg2`. `execute_direct` = `0`이면 `command`는 `bin/sh -c`의 인수로 전달됩니다. 기본값은 `0`입니다. 선택 사항입니다. |
| `send_chunk_header`           | 프로세스로 데이터 청크를 보내기 전에 행 수를 보낼지 여부를 제어합니다. 기본값은 `false`입니다. 선택 사항입니다.                                                                                                                                                                                                                                                       |

해당 딕셔너리 소스는 XML 구성으로만 설정할 수 있습니다. 실행형 소스를 사용하는 딕셔너리는 DDL로 생성할 수 없도록 비활성화되어 있습니다. 그렇지 않으면 DB 사용자가 ClickHouse 노드에서 임의의 바이너리를 실행할 수 있기 때문입니다.