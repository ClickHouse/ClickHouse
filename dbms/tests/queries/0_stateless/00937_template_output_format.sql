DROP TABLE IF EXISTS template;
CREATE TABLE template (s1 String, s2 String, `s 3` String, "s 4" String, n UInt64, d Date) ENGINE = Memory;
INSERT INTO template VALUES
('qwe,rty', 'as"df''gh', '', 'zx\ncv\tbn m', 123, '2016-01-01'),('as"df''gh', '', 'zx\ncv\tbn m', 'qwe,rty', 456, '2016-01-02'),('', 'zx\ncv\tbn m', 'qwe,rty', 'as"df''gh',  9876543210, '2016-01-03'),('zx\ncv\tbn m', 'qwe,rty', 'as"df''gh', '', 789, '2016-01-04');

SELECT * FROM template WITH TOTALS LIMIT 4 FORMAT Template SETTINGS
extremes = 1,
format_schema = '{prefix} \n${data:None}\n------\n${totals:}\n------\n${min}\n------\n${max}\n${rows:Escaped} rows\nbefore limit ${rows_before_limit:XML}\nread ${rows_read:Escaped} $$ suffix $$',
format_schema_rows = 'n:\t${n:JSON}, s1:\t${s1:Escaped}, s2:\t${s2:Quoted}, s3:\t${`s 3`:JSON}, s4:\t${"s 4":CSV}, d:\t${d:Escaped}, n:\t${n:Raw}\t',
format_schema_rows_between_delimiter = ';\n';

DROP TABLE template;
