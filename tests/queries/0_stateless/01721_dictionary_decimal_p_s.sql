-- Tags: no-parallel

set allow_experimental_bigint_types=1;
drop database if exists db_01721;
drop table if exists db_01721.table_decimal_dict;
drop dictionary if exists db_01721.decimal_dict;


create database db_01721;

CREATE TABLE db_01721.table_decimal_dict(
KeyField UInt64,
Decimal32_ Decimal(5,4),
Decimal64_ Decimal(18,8),
Decimal128_ Decimal(25,8),
Decimal256_ Decimal(76,37)
)
ENGINE = Memory;

insert into db_01721.table_decimal_dict
select number,
       number / 3,
       number / 3,
       number / 3,
       number / 3
from numbers(5000);


CREATE DICTIONARY IF NOT EXISTS db_01721.decimal_dict (
	KeyField UInt64 DEFAULT 9999999,
	Decimal32_ Decimal(5,4) DEFAULT 0.11,
	Decimal64_ Decimal(18,8) DEFAULT 0.11,
	Decimal128_ Decimal(25,8) DEFAULT 0.11
--	,Decimal256_ Decimal256(37) DEFAULT 0.11
)
PRIMARY KEY KeyField
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_decimal_dict' DB 'db_01721'))
LIFETIME(0) LAYOUT(SPARSE_HASHED);

select '-------- 42 --------';

SELECT * from db_01721.table_decimal_dict where KeyField = 42;

SELECT * from db_01721.decimal_dict	where KeyField = 42;

SELECT dictGet('db_01721.decimal_dict', 'Decimal32_', toUInt64(42)),
       dictGet('db_01721.decimal_dict', 'Decimal64_', toUInt64(42)),
       dictGet('db_01721.decimal_dict', 'Decimal128_', toUInt64(42))
       -- ,dictGet('db_01721.decimal_dict', 'Decimal256_', toUInt64(42))
;


select '-------- 4999 --------';

SELECT * from db_01721.table_decimal_dict where KeyField = 4999;

SELECT * from db_01721.decimal_dict	where KeyField = 4999;

SELECT dictGet('db_01721.decimal_dict', 'Decimal32_', toUInt64(4999)),
       dictGet('db_01721.decimal_dict', 'Decimal64_', toUInt64(4999)),
       dictGet('db_01721.decimal_dict', 'Decimal128_', toUInt64(4999))
       --,dictGet('db_01721.decimal_dict', 'Decimal256_', toUInt64(4999))
;

select '-------- 5000 --------';

SELECT * from db_01721.table_decimal_dict where KeyField = 5000;

SELECT * from db_01721.decimal_dict	where KeyField = 5000;

SELECT dictGet('db_01721.decimal_dict', 'Decimal32_', toUInt64(5000)),
       dictGet('db_01721.decimal_dict', 'Decimal64_', toUInt64(5000)),
       dictGet('db_01721.decimal_dict', 'Decimal128_', toUInt64(5000))
       --,dictGet('db_01721.decimal_dict', 'Decimal256_', toUInt64(5000))
;

drop table if exists table_decimal_dict;
drop dictionary if exists cache_dict;
drop database if exists db_01721;

