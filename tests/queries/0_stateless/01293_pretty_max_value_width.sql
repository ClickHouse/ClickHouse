SELECT 'привет' AS x, 'мир' AS y FORMAT Pretty;

SET output_format_pretty_max_value_width = 5;
SELECT 'привет' AS x, 'мир' AS y FORMAT Pretty;
SELECT 'привет' AS x, 'мир' AS y FORMAT PrettyCompact;
SELECT 'привет' AS x, 'мир' AS y FORMAT PrettySpace;

SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT Pretty;
SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT PrettyCompact;
SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT PrettySpace;

SET output_format_pretty_max_value_width = 6;

SELECT 'привет' AS x, 'мир' AS y FORMAT Pretty;
SELECT 'привет' AS x, 'мир' AS y FORMAT PrettyCompact;
SELECT 'привет' AS x, 'мир' AS y FORMAT PrettySpace;

SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT Pretty;
SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT PrettyCompact;
SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT PrettySpace;

SET output_format_pretty_max_value_width = 1;

SELECT 'привет' AS x, 'мир' AS y FORMAT Pretty;
SELECT 'привет' AS x, 'мир' AS y FORMAT PrettyCompact;
SELECT 'привет' AS x, 'мир' AS y FORMAT PrettySpace;

SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT Pretty;
SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT PrettyCompact;
SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT PrettySpace;

SET output_format_pretty_max_value_width = 0;

SELECT 'привет' AS x, 'мир' AS y FORMAT Pretty;
SELECT 'привет' AS x, 'мир' AS y FORMAT PrettyCompact;
SELECT 'привет' AS x, 'мир' AS y FORMAT PrettySpace;

SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT Pretty;
SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT PrettyCompact;
SELECT * FROM VALUES('x String, y String', ('привет', 'мир'), ('мир', 'привет')) FORMAT PrettySpace;

SET output_format_pretty_color = 0;
SELECT 'привет' AS x, 'мир' AS y FORMAT Pretty;
