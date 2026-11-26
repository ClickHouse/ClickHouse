SET input_format_json_empty_as_default = 1, allow_experimental_variant_type = 1;

-- Simple types
-- { echoOn }
SELECT x FROM format(JSONEachRow, 'x Date', '{"x":""}');
SELECT x FROM format(JSONEachRow, 'x Date32', '{"x":""}');
SELECT toTimeZone(x, 'UTC') FROM format(JSONEachRow, 'x DateTime', '{"x":""}');
SELECT toTimeZone(x, 'UTC') FROM format(JSONEachRow, 'x DateTime64', '{"x":""}');
SELECT x FROM format(JSONEachRow, 'x IPv4', '{"x":""}');
SELECT x FROM format(JSONEachRow, 'x IPv6', '{"x":""}');
SELECT x FROM format(JSONEachRow, 'x UUID', '{"x":""}');
-- { echoOff }

-- Simple type AggregateFunction
DROP TABLE IF EXISTS table1;
CREATE TABLE table1(col AggregateFunction(uniq, UInt64)) ENGINE=Memory();
DROP TABLE IF EXISTS table2;
CREATE TABLE table2(UserID UInt64) ENGINE=Memory();

INSERT INTO table1 SELECT uniqState(UserID) FROM table2;
INSERT INTO table1 SELECT x FROM format(JSONEachRow, 'x AggregateFunction(uniq, UInt64)' AS T, '{"x":""}');

-- { echoOn }
SELECT COUNT(DISTINCT col) FROM table1;
-- { echoOff }

DROP TABLE table1;
DROP TABLE table2;

-- The setting input_format_defaults_for_omitted_fields determines the default value if enabled.
CREATE TABLE table1(address IPv6 DEFAULT toIPv6('2001:db8:3333:4444:5555:6666:7777:8888')) ENGINE=Memory();

SET input_format_defaults_for_omitted_fields = 0;
INSERT INTO table1 FORMAT JSONEachRow {"address":""};

SET input_format_defaults_for_omitted_fields = 1;
INSERT INTO table1 FORMAT JSONEachRow {"address":""};

-- { echoOn }
SELECT * FROM table1 ORDER BY address ASC;
-- { echoOff }

DROP TABLE table1;

-- Nullable
-- { echoOn }
SELECT x FROM format(JSONEachRow, 'x Nullable(IPv6)', '{"x":""}');

-- Compound types
SELECT x FROM format(JSONEachRow, 'x Array(UUID)', '{"x":["00000000-0000-0000-0000-000000000000","b15f852c-c41a-4fd6-9247-1929c841715e",""]}');
SELECT x FROM format(JSONEachRow, 'x Array(Nullable(IPv6))', '{"x":["",""]}');
SELECT x FROM format(JSONEachRow, 'x Tuple(Date, IPv4, String)', '{"x":["", "", "abc"]}');
SELECT x FROM format(JSONEachRow, 'x Map(String, IPv6)', '{"x":{"abc": ""}}');
SELECT x FROM format(JSONEachRow, 'x Variant(Date, UUID)', '{"x":""}');

-- Deep composition
SELECT x FROM format(JSONEachRow, 'x Array(Array(IPv6))', '{"x":[["2001:db8:3333:4444:CCCC:DDDD:EEEE:FFFF", ""], ["", "2001:db8:3333:4444:5555:6666:7777:8888"]]}');
SELECT x FROM format(JSONEachRow, 'x Variant(Date, Array(UUID))', '{"x":["", "b15f852c-c41a-4fd6-9247-1929c841715e"]}');
SELECT x FROM format(JSONEachRow, 'x Tuple(Array(UUID), Tuple(UUID, Map(String, IPv6)))', '{"x":[[""], ["",{"abc":""}]]}');
SELECT x FROM format(JSONEachRow, 'x Map(Tuple(Date,IPv4), Variant(UUID,IPv6))', '{"x":{["",""]:""}}');
