CREATE TABLE datetime64_cmp
(
	dt6 DateTime64(6, 'UTC'),
	dt3 DateTime64(3, 'UTC')
) ENGINE = Memory;

INSERT INTO datetime64_cmp
VALUES ('2019-09-16 19:20:33.123000', '2019-09-16 19:20:33.123'), ('2019-09-16 19:20:33.123456', '2015-05-18 07:40:29.123'),  ('2015-05-18 07:40:29.123456', '2019-09-16 19:20:33.123');

-- Compare equal and unequal values of different precicion/scale
SELECT
	dt6, dt3,
	dt6 >  dt3,
	dt6 >= dt3,
	dt6 =  dt3,
	dt6 <= dt3,
	dt6 <  dt3,
	dt6 != dt3
FROM datetime64_cmp
ORDER BY
	dt6, dt3;
