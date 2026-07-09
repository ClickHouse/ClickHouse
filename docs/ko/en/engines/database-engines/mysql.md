---
description: '원격 MySQL 서버의 데이터베이스에 연결하고 `INSERT` 및 `SELECT` 쿼리를 실행하여
  ClickHouse와 MySQL 간에 데이터를 교환할 수 있습니다.'
sidebar_label: 'MySQL'
sidebar_position: 50
slug: /engines/database-engines/mysql
title: 'MySQL'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mysql-database-engine">
  # MySQL 데이터베이스 엔진
</div>

<CloudNotSupportedBadge />

원격 MySQL 서버의 데이터베이스에 연결하여 ClickHouse와 MySQL 간에 데이터를 교환하기 위한 `INSERT` 및 `SELECT` 쿼리를 수행할 수 있습니다.

`MySQL` 데이터베이스 엔진은 쿼리를 MySQL 서버용으로 변환하므로 `SHOW TABLES` 또는 `SHOW CREATE TABLE`과 같은 작업을 수행할 수 있습니다.

다음 쿼리는 수행할 수 없습니다.

* `RENAME`
* `CREATE TABLE`
* `ALTER`

<div id="creating-a-database">
  ## 데이터베이스 생성
</div>

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', ['database' | database], 'user', 'password')
[SETTINGS enable_compression=0]
```

**엔진 매개변수**

* `host:port` — MySQL 서버의 주소입니다.
* `database` — 원격 데이터베이스 이름입니다.
* `user` — MySQL 사용자입니다.
* `password` — 사용자 비밀번호입니다.

**설정**

<div id="enable-compression">
  ### `enable_compression`
</div>

MySQL 프로토콜 connection에 대해 zlib 압축을 활성화합니다. `1`로 설정하면 ClickHouse가 MySQL 서버에 프로토콜 수준의 압축을 요청합니다.

기본값: `0`.

예시:

```sql
CREATE DATABASE mysql_db
ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
SETTINGS enable_compression = 1;
```

<div id="data_types-support">
  ## 데이터 타입 지원
</div>

| MySQL                            | ClickHouse                                                   |
| -------------------------------- | ------------------------------------------------------------ |
| UNSIGNED TINYINT                 | [UInt8](../../sql-reference/data-types/int-uint.md)          |
| TINYINT                          | [Int8](../../sql-reference/data-types/int-uint.md)           |
| UNSIGNED SMALLINT                | [UInt16](../../sql-reference/data-types/int-uint.md)         |
| SMALLINT                         | [Int16](../../sql-reference/data-types/int-uint.md)          |
| UNSIGNED INT, UNSIGNED MEDIUMINT | [UInt32](../../sql-reference/data-types/int-uint.md)         |
| INT, MEDIUMINT                   | [Int32](../../sql-reference/data-types/int-uint.md)          |
| UNSIGNED BIGINT                  | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| BIGINT                           | [Int64](../../sql-reference/data-types/int-uint.md)          |
| FLOAT                            | [Float32](../../sql-reference/data-types/float.md)           |
| DOUBLE                           | [Float64](../../sql-reference/data-types/float.md)           |
| DATE                             | [Date](../../sql-reference/data-types/date.md)               |
| DATETIME, TIMESTAMP              | [DateTime](../../sql-reference/data-types/datetime.md)       |
| BINARY                           | [FixedString](../../sql-reference/data-types/fixedstring.md) |

그 외의 모든 MySQL 데이터 타입은 [String](../../sql-reference/data-types/string.md)으로 변환됩니다.

[Nullable](../../sql-reference/data-types/nullable.md)도 지원됩니다.

<div id="global-variables-support">
  ## 전역 변수 지원
</div>

호환성을 높이기 위해 전역 변수를 MySQL 스타일인 `@@identifier` 형식으로 참조할 수 있습니다.

다음 변수를 지원합니다.

* `version`
* `max_allowed_packet`

:::note
현재 이 변수들은 자리만 마련된 항목이며, 실제로는 아무 기능과도 연결되어 있지 않습니다.
:::

예시:

```sql
SELECT @@version;
```

<div id="examples-of-use">
  ## 사용 예시
</div>

MySQL 테이블:

```text
mysql> USE test;
Database changed

mysql> CREATE TABLE `mysql_table` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into mysql_table (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from mysql_table;
+------+-----+
| int_id | value |
+------+-----+
|      1 |     2 |
+------+-----+
1 row in set (0,00 sec)
```

MySQL 서버와 데이터를 주고받는 ClickHouse 데이터베이스:

```sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password') SETTINGS read_write_timeout=10000, connect_timeout=100;
```

```sql
SHOW DATABASES
```

```text
┌─name─────┐
│ default  │
│ mysql_db │
│ system   │
└──────────┘
```

```sql
SHOW TABLES FROM mysql_db
```

```text
┌─name─────────┐
│  mysql_table │
└──────────────┘
```

```sql
SELECT * FROM mysql_db.mysql_table
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```

```sql
INSERT INTO mysql_db.mysql_table VALUES (3,4)
```

```sql
SELECT * FROM mysql_db.mysql_table
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```