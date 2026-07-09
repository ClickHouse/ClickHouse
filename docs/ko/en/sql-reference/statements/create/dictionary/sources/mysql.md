---
slug: /sql-reference/statements/create/dictionary/sources/mysql
title: 'MySQL 딕셔너리 소스'
sidebar_position: 7
sidebar_label: 'MySQL'
description: 'ClickHouse에서 MySQL을 딕셔너리 소스로 구성합니다.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

설정 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        port 3306
        user 'clickhouse'
        password 'qwerty'
        replica(host 'example01-1' priority 1)
        replica(host 'example01-2' priority 1)
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
        enable_compression 1
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <source>
      <mysql>
          <port>3306</port>
          <user>clickhouse</user>
          <password>qwerty</password>
          <replica>
              <host>example01-1</host>
              <priority>1</priority>
          </replica>
          <replica>
              <host>example01-2</host>
              <priority>1</priority>
          </replica>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
          <enable_compression>1</enable_compression>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

설정 필드:

| 설정                        | 설명                                                                                                                                                                                |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `port`                    | MySQL server의 포트입니다. 모든 레플리카에 대해 지정할 수도 있고, 각 레플리카별로 `<replica>` 내부에 개별 지정할 수도 있습니다.                                                                                              |
| `user`                    | MySQL 사용자 이름입니다. 모든 레플리카에 대해 지정할 수도 있고, 각 레플리카별로 `<replica>` 내부에 개별 지정할 수도 있습니다.                                                                                                  |
| `password`                | MySQL 사용자 비밀번호입니다. 모든 레플리카에 대해 지정할 수도 있고, 각 레플리카별로 `<replica>` 내부에 개별 지정할 수도 있습니다.                                                                                                |
| `replica`                 | 레플리카 구성 섹션입니다. 여러 개를 둘 수 있습니다.                                                                                                                                                    |
| `replica/host`            | MySQL 호스트입니다.                                                                                                                                                                     |
| `replica/priority`        | 레플리카 우선순위입니다. 연결을 시도할 때 ClickHouse는 우선순위 순서대로 레플리카를 순회합니다. 숫자가 낮을수록 우선순위가 높습니다.                                                                                                   |
| `db`                      | 데이터베이스 이름입니다.                                                                                                                                                                     |
| `table`                   | 테이블 이름입니다.                                                                                                                                                                        |
| `where`                   | 선택 조건입니다. 조건 구문은 MySQL의 `WHERE` 절과 동일하며, 예를 들어 `id > 10 AND id < 20`와 같습니다. 선택 사항입니다.                                                                                             |
| `invalidate_query`        | 딕셔너리 상태를 확인하는 쿼리입니다. 선택 사항입니다. 자세한 내용은 [LIFETIME를 사용한 딕셔너리 데이터 갱신](../lifetime.md) 섹션을 참조하십시오.                                                                                    |
| `fail_on_connection_loss` | 연결이 끊어졌을 때 server의 동작을 제어합니다. `true`이면 클라이언트와 server 사이의 연결이 끊어질 경우 즉시 예외가 발생합니다. `false`이면 server는 오류를 보고하기 전에 최소 3번 데이터 가져오기를 재시도합니다. 재시도하면 응답 시간이 늘어날 수 있습니다. 기본값은 `false`입니다. |
| `query`                   | 사용자 지정 쿼리입니다. 선택 사항입니다.                                                                                                                                                           |
| `enable_compression`      | MySQL protocol 연결에 대해 zlib 압축을 활성화합니다. `1`로 설정하면 ClickHouse가 MySQL server에 protocol 수준 압축을 요청합니다. `<replica>` 내부에서 레플리카별로 설정할 수도 있습니다. 기본값은 `0`입니다.                               |

:::note
`table` 또는 `where` 필드는 `query` 필드와 함께 사용할 수 없습니다. 또한 `table` 또는 `query` 필드 중 하나는 반드시 선언해야 합니다.
:::

:::note
명시적인 `secure` 매개변수는 없습니다. SSL 연결을 설정할 때는 보안이 필수입니다.
:::

MySQL은 로컬 호스트에서 소켓을 통해 연결할 수 있습니다. 이를 위해 `host`와 `socket`을 설정하십시오.

설정 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MYSQL(
        host 'localhost'
        socket '/path/to/socket/file.sock'
        user 'clickhouse'
        password 'qwerty'
        db 'db_name'
        table 'table_name'
        where 'id=10'
        invalidate_query 'SQL_QUERY'
        fail_on_connection_loss 'true'
        query 'SELECT id, value_1, value_2 FROM db_name.table_name'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <source>
      <mysql>
          <host>localhost</host>
          <socket>/path/to/socket/file.sock</socket>
          <user>clickhouse</user>
          <password>qwerty</password>
          <db>db_name</db>
          <table>table_name</table>
          <where>id=10</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
          <fail_on_connection_loss>true</fail_on_connection_loss>
          <query>SELECT id, value_1, value_2 FROM db_name.table_name</query>
      </mysql>
    </source>
    ```
  </TabItem>
</Tabs>