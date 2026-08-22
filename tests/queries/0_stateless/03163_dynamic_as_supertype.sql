SET allow_experimental_dynamic_type=1;
SET allow_suspicious_types_in_order_by=1;
SELECT if(number % 2, number::Dynamic(max_types=3), ('str_' || toString(number))::Dynamic(max_types=2)) AS d, toTypeName(d), dynamicType(d) FROM numbers(4);
CREATE TABLE dynamic_test_1 (d Dynamic(max_types=3)) ENGINE = Memory;
INSERT INTO dynamic_test_1 VALUES ('str_1'), (42::UInt64);
CREATE TABLE dynamic_test_2 (d Dynamic(max_types=5)) ENGINE = Memory;
INSERT INTO dynamic_test_2 VALUES ('str_2'), (43::UInt64), ('2020-01-01'::Date), ([1, 2, 3]);
SELECT * FROM (SELECT d, dynamicType(d) FROM dynamic_test_1 UNION ALL SELECT d, dynamicType(d) FROM dynamic_test_2) order by d;

