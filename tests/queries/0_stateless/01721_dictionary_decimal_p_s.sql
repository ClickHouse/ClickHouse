-- Tags: no-parallel

drop table if exists table_decimal_dict;
drop dictionary if exists decimal_dict;


CREATE TABLE table_decimal_dict(
KeyField UInt64,
Decimal32_ Decimal(5,4),
Decimal64_ Decimal(18,8),
Decimal128_ Decimal(25,8),
Decimal256_ Decimal(76,37)
)
ENGINE = Memory;

insert into table_decimal_dict
select number,
       number / 3,
       number / 3,
       number / 3,
       number / 3
from numbers(5000);


CREATE DICTIONARY IF NOT EXISTS decimal_dict (
	KeyField UInt64 DEFAULT 9999999,
	Decimal32_ Decimal(5,4) DEFAULT 0.11,
	Decimal64_ Decimal(18,8) DEFAULT 0.11,
	Decimal128_ Decimal(25,8) DEFAULT 0.11
--	,Decimal256_ Decimal256(37) DEFAULT 0.11
)
PRIMARY KEY KeyField
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_decimal_dict' DB current_database()))
LIFETIME(0) LAYOUT(SPARSE_HASHED);

select '-------- 42 --------';

SELECT * from table_decimal_dict where KeyField = 42;

SELECT * from decimal_dict	where KeyField = 42;

SELECT dictGet('decimal_dict', 'Decimal32_', toUInt64(42)),
       dictGet('decimal_dict', 'Decimal64_', toUInt64(42)),
       dictGet('decimal_dict', 'Decimal128_', toUInt64(42))
       -- ,dictGet('decimal_dict', 'Decimal256_', toUInt64(42))
;


select '-------- 4999 --------';

SELECT * from table_decimal_dict where KeyField = 4999;

SELECT * from decimal_dict	where KeyField = 4999;

SELECT dictGet('decimal_dict', 'Decimal32_', toUInt64(4999)),
       dictGet('decimal_dict', 'Decimal64_', toUInt64(4999)),
       dictGet('decimal_dict', 'Decimal128_', toUInt64(4999))
       --,dictGet('decimal_dict', 'Decimal256_', toUInt64(4999))
;

select '-------- 5000 --------';

SELECT * from table_decimal_dict where KeyField = 5000;

SELECT * from decimal_dict	where KeyField = 5000;

SELECT dictGet('decimal_dict', 'Decimal32_', toUInt64(5000)),
       dictGet('decimal_dict', 'Decimal64_', toUInt64(5000)),
       dictGet('decimal_dict', 'Decimal128_', toUInt64(5000))
       --,dictGet('decimal_dict', 'Decimal256_', toUInt64(5000))
;

drop dictionary if exists decimal_dict;
drop table if exists table_decimal_dict;
