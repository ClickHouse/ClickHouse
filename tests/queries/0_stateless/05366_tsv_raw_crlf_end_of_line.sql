-- input_format_tsv_crlf_end_of_line must remove the '\r' of a CRLF line ending in the TSVRaw formats,
-- as it already does in TSV. 

SET input_format_tsv_crlf_end_of_line = 1;

SELECT 'TSVWithNames (baseline)';
SELECT x, hex(s) FROM format(TSVWithNames, 'x UInt8, s String', 'x\ts\r\n1\tab\r\n2\tcd\r\n') ORDER BY x;

SELECT 'TSVRawWithNames';
SELECT x, hex(s) FROM format(TSVRawWithNames, 'x UInt8, s String', 'x\ts\r\n1\tab\r\n2\tcd\r\n') ORDER BY x;

SELECT 'TSVRaw, string in the last column';
SELECT x, hex(s) FROM format(TSVRaw, 'x UInt8, s String', '1\tab\r\n2\tcd\r\n') ORDER BY x;

SELECT 'TSVRaw, number in the last column';
SELECT s, x FROM format(TSVRaw, 's String, x UInt8', 'ab\t1\r\ncd\t2\r\n') ORDER BY x;

SELECT 'TSVRaw, date in the last column';
SELECT s, d FROM format(TSVRaw, 's String, d Date', 'ab\t2017-08-01\r\n') ORDER BY s;

SELECT 'TSVRaw, nullable last column';
SELECT x, s FROM format(TSVRaw, 'x UInt8, s Nullable(String)', '1\tab\r\n2\t\\N\r\n') ORDER BY x;

SELECT 'TSVRaw, a carriage return inside a field is kept';
SELECT hex(s) FROM format(TSVRaw, 's String', 'a\rb\r\n');

SELECT 'TSVRaw, no line ending at the end of the input';
SELECT x, hex(s) FROM format(TSVRaw, 'x UInt8, s String', '1\tab\r\n2\tcd') ORDER BY x;

SELECT 'TSVRawWithNamesAndTypes';
SELECT x, hex(s) FROM format(TSVRawWithNamesAndTypes, 'x\ts\r\nUInt8\tString\r\n1\tab\r\n') ORDER BY x;
