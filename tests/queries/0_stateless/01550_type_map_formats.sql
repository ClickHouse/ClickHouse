SET allow_experimental_map_type = 1;
SET output_format_write_statistics = 0;

DROP TABLE IF EXISTS map_formats;
CREATE TABLE map_formats (seq UInt32, orders Map(UInt32), prices Map(Float32), phones Map(String), birthday Map(Date)) ENGINE = Memory;

INSERT INTO map_formats VALUES(1, {'apple': 1, 'banana': 2}, {'apple': 8.4, 'mango': 5.2}, {'fire': '119', 'medical': '120'}, {'Jay': '2000-01-01'});
INSERT INTO map_formats VALUES(2, {'mango': 5, 'pear': 3}, {'orange': 3.4, 'meal': 15.2}, {'mobile': '10086'}, {'George': '2001-03-23'});

SELECT 'JSON';
SELECT * FROM map_formats ORDER BY seq FORMAT JSON;
SELECT 'JSONEachRow';
SELECT * FROM map_formats ORDER BY seq FORMAT JSONEachRow;
SELECT 'CSV';
SELECT * FROM map_formats ORDER BY seq FORMAT CSV;
SELECT 'TSV';
SELECT * FROM map_formats ORDER BY seq FORMAT TSV;
SELECT 'TSKV';
SELECT * FROM map_formats ORDER BY seq FORMAT TSKV;

DROP TABLE map_formats;
