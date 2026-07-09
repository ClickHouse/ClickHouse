---
slug: /sql-reference/statements/create/dictionary/sources/mongodb
title: 'MongoDB 딕셔너리 소스'
sidebar_position: 9
sidebar_label: 'MongoDB'
description: 'ClickHouse에서 MongoDB를 딕셔너리 소스로 구성합니다.'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

설정 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(MONGODB(
        host 'localhost'
        port 27017
        user ''
        password ''
        db 'test'
        collection 'dictionary_source'
        options 'ssl=true'
    ))
    ```

    또는 URI를 사용할 수 있습니다:

    ```sql
    SOURCE(MONGODB(
        uri 'mongodb://localhost:27017/clickhouse'
        collection 'dictionary_source'
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <source>
        <mongodb>
            <host>localhost</host>
            <port>27017</port>
            <user></user>
            <password></password>
            <db>test</db>
            <collection>dictionary_source</collection>
            <options>ssl=true</options>
        </mongodb>
    </source>
    ```

    또는 URI를 사용할 수 있습니다:

    ```xml
    <source>
        <mongodb>
            <uri>mongodb://localhost:27017/test?ssl=true</uri>
            <collection>dictionary_source</collection>
        </mongodb>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

설정 필드:

| 설정           | 설명                                                     |
| ------------ | ------------------------------------------------------ |
| `host`       | MongoDB 호스트입니다.                                        |
| `port`       | MongoDB 서버의 포트입니다.                                     |
| `user`       | MongoDB 사용자 이름입니다.                                     |
| `password`   | MongoDB 사용자의 비밀번호입니다.                                  |
| `db`         | 데이터베이스 이름입니다.                                          |
| `collection` | collection 이름입니다.                                      |
| `options`    | MongoDB connection string 옵션입니다. 선택 사항입니다.             |
| `uri`        | 연결을 설정하기 위한 URI입니다(개별 `host`/`port`/`db` 필드 대신 사용 가능). |

[엔진에 대한 자세한 정보](/ko/engines/table-engines/integrations/mongodb)