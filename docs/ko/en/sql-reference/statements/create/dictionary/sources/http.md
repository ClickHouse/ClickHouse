---
slug: /sql-reference/statements/create/dictionary/sources/http
title: 'HTTP(S) 딕셔너리 소스'
sidebar_position: 5
sidebar_label: 'HTTP(S)'
description: 'ClickHouse에서 HTTP 또는 HTTPS endpoint를 딕셔너리 소스로 구성합니다.'
doc_type: '참고'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

HTTP(S) 서버 사용 방식은 [딕셔너리가 메모리에 저장되는 방식](../layouts/)에 따라 달라집니다. 딕셔너리가 `cache` 및 `complex_key_cache`를 사용해 저장된 경우, ClickHouse는 `POST` 메서드로 요청을 보내 필요한 키를 가져옵니다.

설정 예시:

<Tabs>
  <TabItem value="ddl" label="DDL" default>
    ```sql
    SOURCE(HTTP(
        url 'http://[::1]/os.tsv'
        format 'TabSeparated'
        credentials(user 'user' password 'password')
        headers(header(name 'API-KEY' value 'key'))
    ))
    ```
  </TabItem>

  <TabItem value="xml" label="설정 파일">
    ```xml
    <source>
        <http>
            <url>http://[::1]/os.tsv</url>
            <format>TabSeparated</format>
            <credentials>
                <user>user</user>
                <password>password</password>
            </credentials>
            <headers>
                <header>
                    <name>API-KEY</name>
                    <value>key</value>
                </header>
            </headers>
        </http>
    </source>
    ```
  </TabItem>
</Tabs>

<br />

ClickHouse가 HTTPS 리소스에 액세스하려면 서버 구성에서 [openSSL을 구성](/ko/operations/server-configuration-parameters/settings#openssl)해야 합니다.

설정 필드:

| Setting       | Description                                                    |
| ------------- | -------------------------------------------------------------- |
| `url`         | 소스 URL입니다.                                                     |
| `format`      | 파일 포맷입니다. [Formats](/ko/sql-reference/formats)에 설명된 모든 포맷을 지원합니다. |
| `credentials` | Basic HTTP 인증입니다. 선택 사항입니다.                                    |
| `user`        | 인증에 필요한 사용자 이름입니다.                                             |
| `password`    | 인증에 필요한 비밀번호입니다.                                               |
| `headers`     | HTTP 요청에 사용되는 모든 사용자 지정 HTTP 헤더 항목입니다. 선택 사항입니다.               |
| `header`      | 단일 HTTP 헤더 항목입니다.                                              |
| `name`        | 요청에 전송할 헤더의 식별자 이름입니다.                                         |
| `value`       | 특정 식별자 이름에 설정할 값입니다.                                           |

DDL 명령(`CREATE DICTIONARY ...`)을 사용해 딕셔너리를 생성할 때는 데이터베이스 사용자가 임의의 HTTP 서버에 액세스하지 못하도록, HTTP 딕셔너리의 원격 호스트를 config의 `remote_url_allow_hosts` 섹션 내용과 대조해 확인합니다.