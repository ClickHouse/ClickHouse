-- https://github.com/ClickHouse/ClickHouse/issues/33000
SET enable_analyzer=1;

SET param_test_a=30;

WITH {test_a:UInt32} as column SELECT column as number FROM numbers(2) FORMAT TSVWithNames;

WITH {test_a:UInt32} as column SELECT {test_a:UInt32} as number FROM numbers(2) FORMAT TSVWithNames;

WITH {test_a:UInt32} as column SELECT column FROM numbers(2) FORMAT TSVWithNames;
