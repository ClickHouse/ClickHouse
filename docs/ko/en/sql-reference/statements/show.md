---
description: 'SHOW에 대한 문서'
sidebar_label: 'SHOW'
sidebar_position: 37
slug: /sql-reference/statements/show
title: 'SHOW SQL 문'
doc_type: 'reference'
---

:::note

`SHOW CREATE (TABLE|DATABASE|USER)`는 다음 설정을 켜지 않으면 시크릿을 숨깁니다:

* [`display_secrets_in_show_and_select`](../../operations/server-configuration-parameters/settings/#display_secrets_in_show_and_select) (서버 설정)
* [`format_display_secrets_in_show_and_select` ](../../operations/settings/formats/#format_display_secrets_in_show_and_select) (포맷 설정)

또한 사용자에게는 [`displaySecretsInShowAndSelect`](grant.md/#displaysecretsinshowandselect) 권한이 있어야 합니다.
:::

<div id="show-create-table--dictionary--view--database">
  ## SHOW CREATE TABLE | DICTIONARY | VIEW | DATABASE
</div>

이 SQL 문은 `String` 타입의 단일 컬럼을 반환하며,
해당 컬럼에는 지정된 객체를 생성하는 데 사용된 `CREATE` 쿼리가 포함됩니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW [CREATE] TABLE | TEMPORARY TABLE | DICTIONARY | VIEW | DATABASE [db.]table|view [INTO OUTFILE filename] [FORMAT format]
```

:::note
이 문을 사용해 시스템 테이블의 `CREATE` 쿼리를 가져오면,
테이블 구조만 선언할 뿐
실제로 테이블을 생성하는 데는 사용할 수 없는 *가짜* 쿼리가 반환됩니다.
:::

<div id="show-databases">
  ## SHOW DATABASES
</div>

이 문은 모든 데이터베이스 목록을 표시합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW DATABASES [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

다음 쿼리와 동일합니다:

```sql
SELECT name FROM system.databases [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE filename] [FORMAT format]
```

<div id="examples">
  ### 예시
</div>

이 예시에서는 이름에 &#39;de&#39;라는 문자열이 포함된 데이터베이스 이름을 확인하기 위해 `SHOW`를 사용합니다:

```sql title="Query"
SHOW DATABASES LIKE '%de%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

대소문자를 구분하지 않는 방식으로도 할 수 있습니다:

```sql title="Query"
SHOW DATABASES ILIKE '%DE%'
```

```text title="Response"
┌─name────┐
│ default │
└─────────┘
```

또는 이름에 &#39;de&#39;가 없는 데이터베이스 이름을 가져옵니다:

```sql title="Query"
SHOW DATABASES NOT LIKE '%de%'
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ system                         │
│ test                           │
│ tutorial                       │
└────────────────────────────────┘
```

마지막으로, 처음 두 개의 데이터베이스 이름만 조회할 수 있습니다:

```sql title="Query"
SHOW DATABASES LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ default                        │
└────────────────────────────────┘
```

<div id="see-also">
  ### 관련 항목
</div>

* [`CREATE DATABASE`](/ko/sql-reference/statements/create/database)

<div id="show-tables">
  ## SHOW TABLES
</div>

`SHOW TABLES` 문은 테이블 목록을 표시합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW [FULL] [TEMPORARY] TABLES [{FROM | IN} <db>] [[NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

`FROM` 절을 지정하지 않으면 현재 데이터베이스의 테이블 목록을 반환합니다.

이 문은 다음 쿼리와 동일합니다:

```sql
SELECT name FROM system.tables [WHERE name [NOT] LIKE | ILIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### 예시
</div>

이 예시에서는 이름에 &#39;user&#39;가 포함된 모든 테이블(table)을 찾기 위해 `SHOW TABLES` 문을 사용합니다:

```sql title="Query"
SHOW TABLES FROM system LIKE '%user%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

대소문자를 구분하지 않는 방식으로도 할 수 있습니다.

```sql title="Query"
SHOW TABLES FROM system ILIKE '%USER%'
```

```text title="Response"
┌─name─────────────┐
│ user_directories │
│ users            │
└──────────────────┘
```

또는 이름에 &#39;s&#39;가 들어 있지 않은 테이블을 찾으려면 다음과 같이 합니다:

```sql title="Query"
SHOW TABLES FROM system NOT LIKE '%s%'
```

```text title="Response"
┌─name─────────┐
│ metric_log   │
│ metric_log_0 │
│ metric_log_1 │
└──────────────┘
```

마지막으로, 처음 두 개 테이블의 이름만 가져올 수 있습니다:

```sql title="Query"
SHOW TABLES FROM system LIMIT 2
```

```text title="Response"
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ asynchronous_metric_log        │
└────────────────────────────────┘
```

<div id="see-also">
  ### 관련 항목
</div>

* [`Create Tables`](/ko/sql-reference/statements/create/table)
* [`SHOW CREATE TABLE`](#show-create-table--dictionary--view--database)

<div id="show_columns">
  ## SHOW COLUMNS
</div>

`SHOW COLUMNS` 문은 컬럼 목록을 보여줍니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW [EXTENDED] [FULL] COLUMNS {FROM | IN} <table> [{FROM | IN} <db>] [{[NOT] {LIKE | ILIKE} '<pattern>' | WHERE <expr>}] [LIMIT <N>] [INTO
OUTFILE <filename>] [FORMAT <format>]
```

데이터베이스 이름과 테이블 이름은 `<db>.<table>` 형태의 축약형으로 지정할 수 있습니다.
즉, `FROM tab FROM db`와 `FROM db.tab`은 동일합니다.
데이터베이스를 지정하지 않으면 쿼리는 현재 데이터베이스의 컬럼 목록을 반환합니다.

선택적으로 사용할 수 있는 키워드 `EXTENDED`와 `FULL`도 있습니다. `EXTENDED` 키워드는 현재 아무런 효과가 없으며,
MySQL 호환성을 위해 존재합니다. `FULL` 키워드를 사용하면 출력에 collation, comment, privilege 컬럼이 포함됩니다.

`SHOW COLUMNS` 문은 다음 구조의 결과 테이블을 생성합니다:

| 컬럼          | 설명                                                                                         | 유형                 |
| ----------- | ------------------------------------------------------------------------------------------ | ------------------ |
| `field`     | 컬럼 이름                                                                                      | `String`           |
| `type`      | 컬럼의 데이터 타입입니다. 쿼리가 MySQL wire 프로토콜을 통해 실행된 경우 MySQL의 해당 데이터 타입 이름이 표시됩니다.                  | `String`           |
| `null`      | 컬럼 데이터 타입이 Nullable이면 `YES`, 그렇지 않으면 `NO`                                                  | `String`           |
| `key`       | 컬럼이 프라이머리 키의 일부이면 `PRI`, 정렬 키의 일부이면 `SOR`, 그렇지 않으면 빈 값                                     | `String`           |
| `default`   | 컬럼 타입이 `ALIAS`, `DEFAULT`, 또는 `MATERIALIZED`이면 해당 컬럼의 기본 표현식이고, 그렇지 않으면 `NULL`입니다.         | `Nullable(String)` |
| `extra`     | 추가 정보입니다. 현재는 사용되지 않습니다                                                                    | `String`           |
| `collation` | (`FULL` 키워드를 지정한 경우에만) 컬럼의 collation이며, ClickHouse는 컬럼별 collations를 지원하지 않으므로 항상 `NULL`입니다 | `Nullable(String)` |
| `comment`   | (`FULL` 키워드를 지정한 경우에만) 컬럼 주석                                                               | `String`           |
| `privilege` | (`FULL` 키워드를 지정한 경우에만) 이 컬럼에 대한 권한이며, 현재는 사용할 수 없습니다                                       | `String`           |

<div id="examples">
  ### 예시
</div>

이 예시에서는 `SHOW COLUMNS` 구문을 사용하여 `orders` 테이블에서 `delivery&#95;`로 시작하는 모든 컬럼의 정보를 가져옵니다:

```sql title="Query"
SHOW COLUMNS FROM 'orders' LIKE 'delivery_%'
```

```text title="Response"
┌─field───────────┬─type─────┬─null─┬─key─────┬─default─┬─extra─┐
│ delivery_date   │ DateTime │    0 │ PRI SOR │ ᴺᵁᴸᴸ    │       │
│ delivery_status │ Bool     │    0 │         │ ᴺᵁᴸᴸ    │       │
└─────────────────┴──────────┴──────┴─────────┴─────────┴───────┘
```

<div id="see-also">
  ### 관련 항목
</div>

* [`system.columns`](../../operations/system-tables/columns.md)

<div id="show-dictionaries">
  ## SHOW DICTIONARIES
</div>

`SHOW DICTIONARIES` 문은 [Dictionaries](./create/dictionary/overview.md) 목록을 표시합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

`FROM` 절을 지정하지 않으면 현재 데이터베이스의 딕셔너리 목록이 반환됩니다.

다음과 같이 `SHOW DICTIONARIES` 쿼리와 같은 결과를 얻을 수 있습니다:

```sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

<div id="examples">
  ### 예시
</div>

다음 쿼리는 `system` 데이터베이스의 테이블 목록에서 이름에 `reg`가 포함된 처음 2개 행을 선택합니다.

```sql title="Query"
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

```text title="Response"
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

<div id="show-index">
  ## SHOW INDEX
</div>

테이블의 프라이머리 인덱스와 데이터 스키핑 인덱스 목록을 표시합니다.

이 문은 주로 MySQL과의 호환성을 위해 제공됩니다. 시스템 테이블(system tables)인 [`system.tables`](../../operations/system-tables/tables.md) (프라이머리 키용) 및 [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md) (데이터 스키핑 인덱스용)에서도
동일한 정보를 확인할 수 있지만, ClickHouse에 더 적합한 네이티브 방식으로 제공됩니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW [EXTENDED] {INDEX | INDEXES | INDICES | KEYS } {FROM | IN} <table> [{FROM | IN} <db>] [WHERE <expr>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

데이터베이스와 테이블 이름은 축약형 `<db>.<table>`로 지정할 수 있습니다. 즉, `FROM tab FROM db`와 `FROM db.tab`은
동일합니다. 데이터베이스를 지정하지 않으면 쿼리는 현재 데이터베이스를 사용합니다.

선택적 키워드 `EXTENDED`는 현재 아무런 효과가 없으며, MySQL 호환성을 위해 존재합니다.

이 구문은 다음 구조의 결과 테이블을 생성합니다:

| 컬럼              | 설명                                                                                                    | 유형                 |
| --------------- | ----------------------------------------------------------------------------------------------------- | ------------------ |
| `table`         | 테이블 이름입니다.                                                                                            | `String`           |
| `non_unique`    | ClickHouse는 유일성 제약 조건을 지원하지 않으므로 항상 `1`입니다.                                                           | `UInt8`            |
| `key_name`      | 인덱스 이름입니다. 인덱스가 primary key 인덱스인 경우 `PRIMARY`입니다.                                                     | `String`           |
| `seq_in_index`  | primary key 인덱스에서는 `1`부터 시작하는 컬럼의 위치입니다. data skipping index에서는 항상 `1`입니다.                            | `UInt8`            |
| `column_name`   | primary key 인덱스에서는 컬럼 이름입니다. data skipping index에서는 `''`(빈 문자열)이며, &quot;expression&quot; 필드를 참조하십시오. | `String`           |
| `collation`     | 인덱스에서 컬럼의 정렬 방식입니다. 오름차순이면 `A`, 내림차순이면 `D`, 정렬되지 않으면 `NULL`입니다.                                       | `Nullable(String)` |
| `cardinality`   | 인덱스 cardinality(인덱스 내 고유 값 수)의 추정치입니다. 현재는 항상 0입니다.                                                   | `UInt64`           |
| `sub_part`      | ClickHouse는 MySQL과 같은 인덱스 프리픽스를 지원하지 않으므로 항상 `NULL`입니다.                                               | `Nullable(String)` |
| `packed`        | ClickHouse는 패킹 인덱스(MySQL과 같은)를 지원하지 않으므로 항상 `NULL`입니다.                                                | `Nullable(String)` |
| `null`          | 현재 사용되지 않습니다.                                                                                         |                    |
| `index_type`    | 인덱스 유형입니다. 예: `PRIMARY`, `MINMAX`, `BLOOM_FILTER` 등입니다.                                               | `String`           |
| `comment`       | 인덱스에 대한 추가 정보이며, 현재는 항상 `''`(빈 문자열)입니다.                                                               | `String`           |
| `index_comment` | ClickHouse의 인덱스는 `COMMENT` 필드(MySQL과 같은)를 가질 수 없으므로 `''`(빈 문자열)입니다.                                   | `String`           |
| `visible`       | 인덱스가 옵티마이저에 표시되는지 여부이며, 항상 `YES`입니다.                                                                  | `String`           |
| `expression`    | data skipping index에서는 인덱스 표현식입니다. primary key 인덱스에서는 `''`(빈 문자열)입니다.                                 | `String`           |

<div id="examples">
  ### 예시
</div>

이 예시에서는 `SHOW INDEX` 문을 사용하여 테이블 &#39;tbl&#39;의 모든 인덱스 정보를 확인합니다

```sql title="Query"
SHOW INDEX FROM 'tbl'
```

```text title="Response"
┌─table─┬─non_unique─┬─key_name─┬─seq_in_index─┬─column_name─┬─collation─┬─cardinality─┬─sub_part─┬─packed─┬─null─┬─index_type───┬─comment─┬─index_comment─┬─visible─┬─expression─┐
│ tbl   │          1 │ blf_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ BLOOM_FILTER │         │               │ YES     │ d, b       │
│ tbl   │          1 │ mm1_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ a, c, d    │
│ tbl   │          1 │ mm2_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ MINMAX       │         │               │ YES     │ c, d, e    │
│ tbl   │          1 │ PRIMARY  │ 1            │ c           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ PRIMARY  │ 2            │ a           │ A         │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ PRIMARY      │         │               │ YES     │            │
│ tbl   │          1 │ set_idx  │ 1            │ 1           │ ᴺᵁᴸᴸ      │ 0           │ ᴺᵁᴸᴸ     │ ᴺᵁᴸᴸ   │ ᴺᵁᴸᴸ │ SET          │         │               │ YES     │ e          │
└───────┴────────────┴──────────┴──────────────┴─────────────┴───────────┴─────────────┴──────────┴────────┴──────┴──────────────┴─────────┴───────────────┴─────────┴────────────┘
```

<div id="see-also">
  ### 관련 항목
</div>

* [`system.tables`](../../operations/system-tables/tables.md)
* [`system.data_skipping_indices`](../../operations/system-tables/data_skipping_indices.md)

<div id="show-processlist">
  ## SHOW PROCESSLIST
</div>

현재 처리 중인 쿼리 목록이 들어 있는 [`system.processes`](/ko/operations/system-tables/processes) 테이블의 내용을 출력합니다. 단, `SHOW PROCESSLIST` 쿼리는 제외됩니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

`SELECT * FROM system.processes` 쿼리는 현재 실행 중인 모든 쿼리의 정보를 반환합니다.

:::tip
콘솔에서 실행하세요:

```bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

:::

<div id="show-grants">
  ## SHOW GRANTS
</div>

`SHOW GRANTS` 문은 사용자의 권한을 표시합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW GRANTS [FOR user1 [, user2 ...]] [WITH IMPLICIT] [FINAL]
```

사용자를 지정하지 않으면, 쿼리는 현재 사용자의 권한을 반환합니다.

`WITH IMPLICIT` 수정자를 사용하면 암시적으로 부여된 권한 부여를 표시할 수 있습니다(예: `GRANT SELECT ON system.one`)

`FINAL` 수정자는 사용자와 해당 사용자에게 부여된 역할의 모든 권한 부여를 머지합니다(상속 포함)

<div id="show-create-user">
  ## SHOW CREATE USER
</div>

`SHOW CREATE USER` 문은 [사용자 생성](../../sql-reference/statements/create/user.md) 시 사용된 매개변수를 표시합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW CREATE USER [name1 [, name2 ...] | CURRENT_USER]
```

<div id="show-create-role">
  ## SHOW CREATE ROLE
</div>

`SHOW CREATE ROLE` 문은 [역할 생성](../../sql-reference/statements/create/role.md) 시 사용한 매개변수를 표시합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW CREATE ROLE name1 [, name2 ...]
```

<div id="show-create-row-policy">
  ## SHOW CREATE ROW POLICY
</div>

`SHOW CREATE ROW POLICY` 문은 [ROW POLICY 생성](../../sql-reference/statements/create/row-policy.md) 시 사용된 매개변수를 표시합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW CREATE [ROW] POLICY name ON [database1.]table1 [, [database2.]table2 ...]
```

<div id="show-create-quota">
  ## SHOW CREATE QUOTA
</div>

`SHOW CREATE QUOTA` 문은 [QUOTA를 생성](../../sql-reference/statements/create/quota.md)할 때 사용된 매개변수를 표시합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW CREATE QUOTA [name1 [, name2 ...] | CURRENT]
```

<div id="show-create-settings-profile">
  ## SHOW CREATE SETTINGS PROFILE
</div>

`SHOW CREATE SETTINGS PROFILE` 문은 [설정 프로필 생성](../../sql-reference/statements/create/settings-profile.md) 시 사용된 매개변수를 표시합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW CREATE [SETTINGS] PROFILE name1 [, name2 ...]
```

<div id="show-users">
  ## SHOW USERS
</div>

`SHOW USERS` 문은 [사용자 계정](../../guides/sre/user-management/index.md#user-account-management) 이름 목록을 반환합니다.
사용자 계정의 매개변수를 확인하려면 시스템 테이블 [`system.users`](/ko/operations/system-tables/users)를 참조하십시오.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW USERS
```

<div id="show-roles">
  ## SHOW ROLES
</div>

`SHOW ROLES` 문은 [역할](../../guides/sre/user-management/index.md#role-management) 목록을 반환합니다.
다른 매개변수는
시스템 테이블 [`system.roles`](/ko/operations/system-tables/roles) 및 [`system.role_grants`](/ko/operations/system-tables/role_grants`)를 참조하십시오.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW [CURRENT|ENABLED] ROLES
```

<div id="show-profiles">
  ## SHOW PROFILES
</div>

`SHOW PROFILES` 문은 [설정 프로필](../../guides/sre/user-management/index.md#settings-profiles-management) 목록을 반환합니다.
사용자 계정의 매개변수를 보려면 시스템 테이블(system table) [`settings_profiles`](/ko/operations/system-tables/settings_profiles)를 참조하십시오.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW [SETTINGS] PROFILES
```

<div id="show-policies">
  ## SHOW POLICIES
</div>

`SHOW POLICIES` 문은 지정한 테이블의 [행 정책](../../guides/sre/user-management/index.md#row-policy-management) 목록을 반환합니다.
사용자 계정 매개변수를 확인하려면 시스템 테이블(system table) [`system.row_policies`](/ko/operations/system-tables/row_policies)를 참조하십시오.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW [ROW] POLICIES [ON [db.]table]
```

<div id="show-quotas">
  ## SHOW QUOTAS
</div>

`SHOW QUOTAS` 문은 [쿼터](../../guides/sre/user-management/index.md#quotas-management) 목록을 반환합니다.
쿼터 매개변수를 확인하려면 시스템 테이블(system table) [`system.quotas`](/ko/operations/system-tables/quotas)를 참조하십시오.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW QUOTAS
```

<div id="show-quota">
  ## SHOW QUOTA
</div>

`SHOW QUOTA` 문은 모든 사용자 또는 현재 사용자의 [쿼터](../../operations/quotas.md) 활용 정보를 반환합니다.
다른 매개변수를 보려면 시스템 테이블 [`system.quotas_usage`](/ko/operations/system-tables/quotas_usage) 및 [`system.quota_usage`](/ko/operations/system-tables/quota_usage)를 참조하십시오.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW [CURRENT] QUOTA
```

<div id="show-access">
  ## SHOW ACCESS
</div>

`SHOW ACCESS` 문은 모든 [사용자](../../guides/sre/user-management/index.md#user-account-management), [역할](../../guides/sre/user-management/index.md#role-management), [프로필](../../guides/sre/user-management/index.md#settings-profiles-management) 등과 각 항목에 대한 모든 [권한 부여](../../sql-reference/statements/grant.md#privileges)를 표시합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW ACCESS
```

<div id="show-clusters">
  ## SHOW CLUSTER(S)
</div>

`SHOW CLUSTER(S)` 문은 클러스터 목록을 반환합니다.
사용 가능한 모든 클러스터는 [`system.clusters`](../../operations/system-tables/clusters.md) 테이블에 나열됩니다.

:::note
`SHOW CLUSTER name` 쿼리는 지정된 클러스터 이름에 대해 `system.clusters` 테이블의 `cluster`, `shard_num`, `replica_num`, `host_name`, `host_address`, `port`를 표시합니다.
:::

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW CLUSTER '<name>'
SHOW CLUSTERS [[NOT] LIKE|ILIKE '<pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### 예시
</div>

```sql title="Query"
SHOW CLUSTERS;
```

```text title="Response"
┌─cluster──────────────────────────────────────┐
│ test_cluster_two_shards                      │
│ test_cluster_two_shards_internal_replication │
│ test_cluster_two_shards_localhost            │
│ test_shard_localhost                         │
│ test_shard_localhost_secure                  │
│ test_unavailable_shard                       │
└──────────────────────────────────────────────┘
```

```sql title="Query"
SHOW CLUSTERS LIKE 'test%' LIMIT 1;
```

```text title="Response"
┌─cluster─────────────────┐
│ test_cluster_two_shards │
└─────────────────────────┘
```

```sql title="Query"
SHOW CLUSTER 'test_shard_localhost' FORMAT Vertical;
```

```text title="Response"
Row 1:
──────
cluster:                 test_shard_localhost
shard_num:               1
replica_num:             1
host_name:               localhost
host_address:            127.0.0.1
port:                    9000
```

<div id="show-settings">
  ## SHOW SETTINGS
</div>

`SHOW SETTINGS` 문은 시스템 설정과 해당 값을 목록으로 반환합니다.
이 문은 [`system.settings`](../../operations/system-tables/settings.md) 테이블에서 데이터를 조회합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW [CHANGED] SETTINGS LIKE|ILIKE <name>
```

<div id="clauses">
  ### 절
</div>

`LIKE|ILIKE`를 사용하면 설정 이름과 일치하는 패턴을 지정할 수 있습니다. 이 패턴에는 `%` 또는 `_`와 같은 글롭 패턴이 포함될 수 있습니다. `LIKE` 절은 대소문자를 구분하고, `ILIKE`는 대소문자를 구분하지 않습니다.

`CHANGED` 절을 사용하면 기본값에서 변경된 설정만 반환됩니다.

<div id="examples">
  ### 예시
</div>

`LIKE` 절을 사용한 쿼리:

```sql title="Query"
SHOW SETTINGS LIKE 'send_timeout';
```

```text title="Response"
┌─name─────────┬─type────┬─value─┐
│ send_timeout │ Seconds │ 300   │
└──────────────┴─────────┴───────┘
```

`ILIKE` 절을 사용한 쿼리:

```sql title="Query"
SHOW SETTINGS ILIKE '%CONNECT_timeout%'
```

```text title="Response"
┌─name────────────────────────────────────┬─type─────────┬─value─┐
│ connect_timeout                         │ Seconds      │ 10    │
│ connect_timeout_with_failover_ms        │ Milliseconds │ 50    │
│ connect_timeout_with_failover_secure_ms │ Milliseconds │ 100   │
└─────────────────────────────────────────┴──────────────┴───────┘
```

`CHANGED` 절을 사용하는 쿼리:

```sql title="Query"
SHOW CHANGED SETTINGS ILIKE '%MEMORY%'
```

```text title="Response"
┌─name─────────────┬─type───┬─value───────┐
│ max_memory_usage │ UInt64 │ 10000000000 │
└──────────────────┴────────┴─────────────┘
```

<div id="show-setting">
  ## SHOW SETTING
</div>

`SHOW SETTING` 문은 지정한 설정 이름의 설정 값을 출력합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW SETTING <name>
```

<div id="see-also">
  ### 관련 항목
</div>

* [`system.settings`](../../operations/system-tables/settings.md) 테이블

<div id="show-filesystem-caches">
  ## SHOW FILESYSTEM CACHES
</div>

<div id="examples">
  ### 예시
</div>

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

<div id="see-also">
  ### 관련 항목
</div>

* [`system.settings`](../../operations/system-tables/settings.md) 테이블

<div id="show-engines">
  ## SHOW ENGINES
</div>

`SHOW ENGINES` 문은 [`system.table_engines`](../../operations/system-tables/table_engines.md) 테이블의 내용을 출력합니다. 이 테이블에는 서버가 지원하는 테이블 엔진의 설명과 각 기능의 지원 정보가 포함되어 있습니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW ENGINES [INTO OUTFILE filename] [FORMAT format]
```

<div id="see-also">
  ### 관련 항목
</div>

* [system.table&#95;engines](../../operations/system-tables/table_engines.md) 테이블

<div id="show-functions">
  ## SHOW FUNCTIONS
</div>

`SHOW FUNCTIONS` 문은 [`system.functions`](../../operations/system-tables/functions.md) 테이블의 내용을 출력합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW FUNCTIONS [LIKE | ILIKE '<pattern>']
```

`LIKE` 또는 `ILIKE` 절 중 하나를 지정하면, 쿼리는 이름이 지정된 `<pattern>`과 일치하는 시스템 함수 목록을 반환합니다.

<div id="see-also">
  ### 관련 항목
</div>

* [`system.functions`](../../operations/system-tables/functions.md) 테이블

<div id="show-merges">
  ## SHOW MERGES
</div>

`SHOW MERGES` 문은 머지 목록을 반환합니다.
모든 머지는 [`system.merges`](../../operations/system-tables/merges.md) 테이블에 나열됩니다.

| Column              | Description                   |
| ------------------- | ----------------------------- |
| `table`             | 테이블 이름입니다.                    |
| `database`          | 테이블이 속한 데이터베이스의 이름입니다.        |
| `estimate_complete` | 완료까지의 예상 시간(초)입니다.            |
| `elapsed`           | 머지가 시작된 후 경과한 시간(초)입니다.       |
| `progress`          | 완료된 작업의 비율(0~100%)입니다.        |
| `is_mutation`       | 이 프로세스가 파트 mutation인 경우 1입니다. |
| `size_compressed`   | 병합된 파트의 압축 데이터 총크기입니다.        |
| `memory_usage`      | 머지 프로세스의 메모리 사용량입니다.          |

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW MERGES [[NOT] LIKE|ILIKE '<table_name_pattern>'] [LIMIT <N>]
```

<div id="examples">
  ### 예시
</div>

```sql title="Query"
SHOW MERGES;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

```sql title="Query"
SHOW MERGES LIKE 'your_t%' LIMIT 1;
```

```text title="Response"
┌─table──────┬─database─┬─estimate_complete─┬─elapsed─┬─progress─┬─is_mutation─┬─size_compressed─┬─memory_usage─┐
│ your_table │ default  │              0.14 │    0.36 │    73.01 │           0 │        5.40 MiB │    10.25 MiB │
└────────────┴──────────┴───────────────────┴─────────┴──────────┴─────────────┴─────────────────┴──────────────┘
```

<div id="show-create-masking-policy">
  ## SHOW CREATE MASKING POLICY
</div>

`SHOW CREATE MASKING POLICY` 문은 [마스킹 정책 생성](../../sql-reference/statements/create/masking-policy.md) 시 사용된 매개변수를 표시합니다.

<div id="syntax">
  ### 구문
</div>

```sql title="Syntax"
SHOW CREATE MASKING POLICY name ON [database.]table
```