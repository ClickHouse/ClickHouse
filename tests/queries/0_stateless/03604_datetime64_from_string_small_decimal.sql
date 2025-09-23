DROP TABLE IF EXISTS datetimes64_unixts;

CREATE TABLE datetimes64_unixts
(
    id UInt64,
    some_dt64_0 DateTime64(0, 'UTC'),
    some_dt64_1 DateTime64(1, 'UTC'),
    some_dt64_2 DateTime64(2, 'UTC'),
    some_dt64_3 DateTime64(3, 'UTC'),
    some_dt64_4 DateTime64(4, 'UTC'),
    some_dt64_5 DateTime64(5, 'UTC'),
    some_dt64_6 DateTime64(6, 'UTC'),
    some_dt64_7 DateTime64(7, 'UTC'),
    some_dt64_8 DateTime64(8, 'UTC'),
    some_dt64_9 DateTime64(9, 'UTC')
) ENGINE = Memory;

INSERT INTO datetimes64_unixts (
    id,
    some_dt64_0, some_dt64_1, some_dt64_2, some_dt64_3, some_dt64_4,
    some_dt64_5, some_dt64_6, some_dt64_7, some_dt64_8, some_dt64_9
) FORMAT TSV
1	123	456	789	1011	2025	877	8	9999	033	842
2	123	233	456	7890	-123	345	6789	553	999	7
3	99.1	250.5	3333.77	4444.9	123.45	678.9	8888.01	777.6	942.42	9999.123456789
4	0.1	12.34	56.7	-89.01	234.5	678.01	987.65	543.21	111.11	222.22
5	2025-08-31	2025-09-01	2024-12-25	2025-01-01	2023-07-04	2022-11-11	2020-02-29	2019-01-01	2018-12-12	2017-06-06
6	2025-08-31T13:45:30	2025-08-31T23:59:59	2024-02-29T00:00:00	2025-01-01T12:34:56	2023-12-31T23:59:59	2022-06-15T08:00:00	2021-03-01T01:23:45	2019-10-10T10:10:10	2018-05-05T05:05:05	2017-01-01T00:00:01



SELECT *
FROM datetimes64_unixts
ORDER BY id;

DROP TABLE IF EXISTS datetimes64_unixts;

