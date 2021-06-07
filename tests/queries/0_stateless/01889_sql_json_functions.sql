SELECT '--JSON_VALUE--';
SELECT JSON_VALUE('$.hello', '{"hello":1}');
SELECT JSON_VALUE('$.hello', '{"hello":1.2}');
SELECT JSON_VALUE('$.hello', '{"hello":true}');
SELECT JSON_VALUE('$.hello', '{"hello":"world"}');
SELECT JSON_VALUE('$.hello', '{"hello":null}');
SELECT JSON_VALUE('$.hello', '{"hello":["world","world2"]}');
SELECT JSON_VALUE('$.hello', '{"hello":{"world":"!"}}');
SELECT JSON_VALUE('$.hello', '{hello:world}'); -- invalid json => default value (empty string)
SELECT JSON_VALUE('$.hello', '');

SELECT '--JSON_QUERY--';
SELECT JSON_QUERY('$.hello', '{"hello":1}');
SELECT JSON_QUERY('$.hello', '{"hello":1.2}');
SELECT JSON_QUERY('$.hello', '{"hello":true}');
SELECT JSON_QUERY('$.hello', '{"hello":"world"}');
SELECT JSON_QUERY('$.hello', '{"hello":null}');
SELECT JSON_QUERY('$.hello', '{"hello":["world","world2"]}');
SELECT JSON_QUERY('$.hello', '{"hello":{"world":"!"}}');
SELECT JSON_QUERY('$.hello', '{hello:{"world":"!"}}}'); -- invalid json => default value (empty string)
SELECT JSON_QUERY('$.hello', '');

SELECT '--JSON_EXISTS--';
SELECT JSON_EXISTS('$.hello', '{"hello":1}');
SELECT JSON_EXISTS('$.world', '{"hello":1,"world":2}');
SELECT JSON_EXISTS('$.world', '{"hello":{"world":1}}');
SELECT JSON_EXISTS('$.hello.world', '{"hello":{"world":1}}');
SELECT JSON_EXISTS('$.hello', '{hello:world}'); -- invalid json => default value (zero integer)
SELECT JSON_EXISTS('$.hello', '');
SELECT JSON_EXISTS('$.hello[*]', '{"hello":["world"]}');
SELECT JSON_EXISTS('$.hello[0]', '{"hello":["world"]}');
SELECT JSON_EXISTS('$.hello[1]', '{"hello":["world"]}');
SELECT JSON_EXISTS('$.a[*].b', '{"a":[{"b":1},{"c":2}]}');
SELECT JSON_EXISTS('$.a[*].f', '{"a":[{"b":1},{"c":2}]}');
SELECT JSON_EXISTS('$.a[*][0].h', '{"a":[[{"b":1}, {"g":1}],[{"h":1},{"y":1}]]}');