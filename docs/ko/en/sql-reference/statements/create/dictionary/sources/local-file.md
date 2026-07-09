---
slug: /sql-reference/statements/create/dictionary/sources/local-file
title: '로컬 파일 딕셔너리 소스'
sidebar_position: 2
sidebar_label: '로컬 파일'
description: 'ClickHouse에서 로컬 파일을 딕셔너리 소스로 설정합니다.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

로컬 파일 소스는 로컬 파일 시스템의 파일에서 딕셔너리 데이터를 로드합니다. 이 방식은 TSV, CSV 또는 기타 [지원되는 포맷](/ko/sql-reference/formats)의 플랫 파일로 저장할 수 있는 작고 정적인 lookup 테이블에 유용합니다.

설정 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(FILE(path './user_files/os.tsv' format 'TabSeparated'))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <source>
      <file>
        <path>/opt/dictionaries/os.tsv</path>
        <format>TabSeparated</format>
      </file>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

설정 필드:

| Setting  | Description                                                    |
| -------- | -------------------------------------------------------------- |
| `path`   | 파일의 절대 경로입니다.                                                  |
| `format` | 파일 포맷입니다. [Formats](/ko/sql-reference/formats)에 설명된 모든 포맷을 지원합니다. |

소스가 `FILE`인 딕셔너리를 DDL 명령(`CREATE DICTIONARY ...`)으로 생성하는 경우, DB 사용자가 ClickHouse 노드의 임의 파일에 접근하지 못하도록 소스 파일은 `user_files` 디렉터리에 있어야 합니다.

**관련 항목**

* [딕셔너리 함수](/ko/sql-reference/table-functions/dictionary)