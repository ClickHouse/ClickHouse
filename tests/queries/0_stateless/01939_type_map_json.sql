SELECT map(1, 2, 3, 4) AS m FORMAT JSONEachRow;
SELECT map(1, 2, 3, 4) AS m, toJSONString(m) AS s, isValidJSON(s);

SELECT map('key1', number, 'key2', number * 2) AS m FROM numbers(1, 1) FORMAT JSONEachRow;
SELECT map('key1', number, 'key2', number * 2) AS m, toJSONString(m) AS s, isValidJSON(s) FROM numbers(1, 1);

SELECT map('key1', number, 'key2', number * 2) AS m FROM numbers(1, 1)
    FORMAT JSONEachRow
    SETTINGS output_format_json_quote_64bit_integers = 0;

SELECT map('key1', number, 'key2', number * 2) AS m, toJSONString(m) AS s, isValidJSON(s) FROM numbers(1, 1)
    SETTINGS output_format_json_quote_64bit_integers = 0;

CREATE TEMPORARY TABLE map_json (m1 Map(String, UInt64), m2 Map(UInt32, UInt32));

INSERT INTO map_json FORMAT JSONEachRow {"m1" : {"k1" : 1, "k2" : 2}, "m2" : {"1" : 2, "2" : 3}};

SELECT m1, m2 FROM map_json FORMAT JSONEachRow;
SELECT m1, m2 FROM map_json FORMAT JSONEachRow SETTINGS output_format_json_quote_64bit_integers = 0;
