---
description: '쿼리 권한에 대한 설정입니다.'
sidebar_label: '쿼리별 권한'
sidebar_position: 58
slug: /operations/settings/permissions-for-queries
title: '쿼리별 권한'
doc_type: '참고'
---

ClickHouse의 쿼리는 여러 유형으로 나눌 수 있습니다.

1. 데이터 읽기 쿼리: `SELECT`, `SHOW`, `DESCRIBE`, `EXISTS`.
2. 데이터 쓰기 쿼리: `INSERT`, `OPTIMIZE`.
3. 설정 변경 쿼리: `SET`, `USE`.
4. [DDL](https://en.wikipedia.org/wiki/Data_definition_language) 쿼리: `CREATE`, `ALTER`, `RENAME`, `ATTACH`, `DETACH`, `DROP` `TRUNCATE`.
5. `KILL QUERY`.

다음 설정은 쿼리 유형에 따라 사용자 권한을 제어합니다.

<div id="readonly">
  ## readonly
</div>

데이터 읽기, 데이터 쓰기, 설정 변경 쿼리에 대한 권한을 제한합니다.

1로 설정하면 다음이 허용됩니다.

* 모든 유형의 읽기 쿼리(예: SELECT 및 이에 준하는 쿼리)
* 세션 Context만 변경하는 쿼리(예: USE)

2로 설정하면 위 항목에 더해 다음이 허용됩니다.

* SET 및 CREATE TEMPORARY TABLE

  :::tip
  EXISTS, DESCRIBE, EXPLAIN, SHOW PROCESSLIST 등의 쿼리는 system table에서 SELECT만 수행하므로 SELECT와 동일합니다.
  :::

가능한 값:

* 0 — 읽기, 쓰기, 설정 변경 쿼리가 허용됩니다.
* 1 — 데이터 읽기 쿼리만 허용됩니다.
* 2 — 데이터 읽기 및 설정 변경 쿼리가 허용됩니다.

기본값: 0

:::note
`readonly = 1`로 설정한 후에는 현재 세션에서 사용자가 `readonly` 및 `allow_ddl` 설정을 변경할 수 없습니다.

[HTTP 인터페이스](/ko/interfaces/http)에서 `GET` 메서드를 사용하면 `readonly = 1`이 자동으로 설정됩니다. 데이터를 수정하려면 `POST` 메서드를 사용하십시오.

`readonly = 1`로 설정하면 사용자가 설정을 변경할 수 없습니다. 특정 설정만 변경하지 못하게 하는 방법도 있습니다. 또한 `readonly = 1` 제한하에서 특정 설정만 변경할 수 있게 하는 방법도 있습니다. 자세한 내용은 [설정에 대한 제약 조건](../../operations/settings/constraints-on-settings.md)을 참조하십시오.
:::

<div id="allow_ddl">
  ## allow_ddl
</div>

[DDL](https://en.wikipedia.org/wiki/Data_definition_language) 쿼리를 허용하거나 금지합니다.

가능한 값:

* 0 — DDL 쿼리가 허용되지 않습니다.
* 1 — DDL 쿼리가 허용됩니다.

기본값: 1

:::note
현재 session에서 `allow_ddl = 0`이면 `SET allow_ddl = 1`을 실행할 수 없습니다.
:::

:::note KILL QUERY
`KILL QUERY`는 readonly 및 allow&#95;ddl 설정의 어떤 조합이든 수행할 수 있습니다.
:::