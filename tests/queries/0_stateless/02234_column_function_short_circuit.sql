DROP TABLE IF EXISTS dict_table;
DROP TABLE IF EXISTS data_table;
DROP DICTIONARY IF EXISTS dict;

create table dict_table
(
    `strField` String,
    `dateField` Date,
    `float64Field` Float64
) Engine Log();

insert into dict_table values ('SomeStr', toDate('2021-01-01'), 1.1), ('SomeStr2', toDate('2021-01-02'), 2.2);

create dictionary dict
(
    `strField` String,
    `dateField` Date,
    `float64Field` Float64
)
PRIMARY KEY strField, dateField
SOURCE (CLICKHOUSE(TABLE 'dict_table'))
LIFETIME(MIN 300 MAX 360)
LAYOUT (COMPLEX_KEY_HASHED());

create table data_table
(
    `float64Field1` Float64,
    `float64Field2` Float64,
    `strField1` String,
    `strField2` String
) Engine Log();

insert into data_table values (1.1, 1.2, 'SomeStr', 'SomeStr'), (2.1, 2.2, 'SomeStr2', 'SomeStr2');

select round(
        float64Field1 * if(strField1 != '', 1.0, dictGetFloat64('dict', 'float64Field', (strField1, toDate('2021-01-01'))))
        + if(strField2 != '', 1.0, dictGetFloat64('dict', 'float64Field', (strField2, toDate('2021-01-01')))) * if(isFinite(float64Field2), float64Field2, 0),
    2)
from data_table;

DROP DICTIONARY dict;
DROP TABLE dict_table;
DROP TABLE data_table;
