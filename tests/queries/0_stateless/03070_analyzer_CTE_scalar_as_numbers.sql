-- https://github.com/ClickHouse/ClickHouse/issues/8259
with
    (select 25) as something
select *, something
from numbers(toUInt64(assumeNotNull(something)));
