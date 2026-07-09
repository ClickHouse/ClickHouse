---
slug: /sql-reference/statements/create/dictionary/sources/executable-pool
title: '실행형 풀 딕셔너리 소스'
sidebar_position: 4
sidebar_label: '실행형 풀'
description: 'ClickHouse에서 실행형 풀을 딕셔너리 소스로 설정합니다.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

실행형 풀은 프로세스 풀에서 데이터를 로드할 수 있게 합니다.
이 소스는 소스에서 모든 데이터를 로드해야 하는 딕셔너리 레이아웃에서는 작동하지 않습니다.

실행형 풀은 딕셔너리가 다음 레이아웃 중 하나를 사용해 [저장될 때](../layouts/#storing-dictionaries-in-memory) 작동합니다.

* `cache`
* `complex_key_cache`
* `ssd_cache`
* `complex_key_ssd_cache`
* `direct`
* `complex_key_direct`

실행형 풀은 지정된 명령으로 프로세스 풀을 생성하고, 프로세스가 종료될 때까지 계속 실행 상태로 유지합니다. 프로그램은 STDIN에서 읽을 수 있는 동안 데이터를 읽고 결과를 STDOUT으로 출력해야 합니다. 또한 STDIN에서 다음 데이터 블록을 기다릴 수 있습니다. ClickHouse는 데이터 블록 하나를 처리한 뒤 STDIN을 닫지 않고, 필요할 때 다른 데이터 청크를 파이프로 전달합니다. 실행형 스크립트는 이러한 데이터 처리 방식에 맞게 준비되어 있어야 합니다. 즉, STDIN을 폴링하고 STDOUT으로 데이터를 조기에 플러시해야 합니다.

설정 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(EXECUTABLE_POOL(
        command 'while read key; do printf "$key\tData for key $key\n"; done'
        format 'TabSeparated'
        pool_size 10
        max_command_execution_time 10
        implicit_key false
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <source>
        <executable_pool>
            <command><command>while read key; do printf "$key\tData for key $key\n"; done</command</command>
            <format>TabSeparated</format>
            <pool_size>10</pool_size>
            <max_command_execution_time>10<max_command_execution_time>
            <implicit_key>false</implicit_key>
        </executable_pool>
    </source>
    ```
  </TabItem>
</Tabs>

설정 필드:

| 설정                            | 설명                                                                                                                                                                                                                                                                                                                             |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `command`                     | 실행 파일의 절대 경로 또는 파일 이름입니다(프로그램 디렉터리가 `PATH`에 포함된 경우).                                                                                                                                                                                                                                                                           |
| `format`                      | 파일 포맷입니다. [포맷](/ko/sql-reference/formats)에 설명된 모든 포맷이 지원됩니다.                                                                                                                                                                                                                                                                      |
| `pool_size`                   | 풀 크기입니다. `pool_size`에 0을 지정하면 풀 크기에 제한이 없습니다. 기본값은 `16`입니다.                                                                                                                                                                                                                                                                    |
| `command_termination_timeout` | 실행형 스크립트에는 기본 읽기-쓰기 루프가 포함되어 있어야 합니다. 딕셔너리가 제거되면 파이프가 닫히고, ClickHouse가 자식 프로세스에 SIGTERM 신호를 보내기 전까지 실행 파일은 `command_termination_timeout`초 안에 종료해야 합니다. 초 단위로 지정합니다. 기본값은 `10`입니다. 선택 사항입니다.                                                                                                                                    |
| `max_command_execution_time`  | 데이터 블록을 처리할 때 실행형 스크립트 명령의 최대 실행 시간입니다. 초 단위로 지정합니다. 기본값은 `10`입니다. 선택 사항입니다.                                                                                                                                                                                                                                                   |
| `command_read_timeout`        | 명령의 stdout에서 데이터를 읽을 때의 타임아웃이며, 밀리초 단위입니다. 기본값은 `10000`입니다. 선택 사항입니다.                                                                                                                                                                                                                                                          |
| `command_write_timeout`       | 명령의 stdin에 데이터를 쓸 때의 타임아웃이며, 밀리초 단위입니다. 기본값은 `10000`입니다. 선택 사항입니다.                                                                                                                                                                                                                                                             |
| `implicit_key`                | 실행형 소스 파일은 값만 반환할 수 있으며, 요청된 키와의 대응 관계는 결과의 행 순서에 따라 암묵적으로 결정됩니다. 기본값은 `false`입니다. 선택 사항입니다.                                                                                                                                                                                                                                   |
| `execute_direct`              | `execute_direct` = `1`이면 `command`는 [user&#95;scripts&#95;path](/ko/operations/server-configuration-parameters/settings#user_scripts_path)로 지정된 user&#95;scripts 폴더 안에서 검색됩니다. 공백 구분자를 사용해 추가 스크립트 인수를 지정할 수 있습니다. 예: `script_name arg1 arg2`. `execute_direct` = `0`이면 `command`는 `bin/sh -c`의 인수로 전달됩니다. 기본값은 `1`입니다. 선택 사항입니다. |
| `send_chunk_header`           | 프로세스로 데이터 청크를 보내기 전에 행 수를 보낼지 여부를 제어합니다. 기본값은 `false`입니다. 선택 사항입니다.                                                                                                                                                                                                                                                            |

이 딕셔너리 소스는 XML 구성으로만 설정할 수 있습니다. 그렇지 않으면 DB 사용자가 ClickHouse 노드에서 임의의 바이너리를 실행할 수 있으므로, DDL을 통한 실행형 소스 딕셔너리 생성은 비활성화되어 있습니다.