-- init
CREATE TABLE data
(
    `id` UInt32,
    `n` UInt32,
    `s` String,
    `d` Decimal32(4)
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO data (*)
VALUES (1, 20, 'bsdf', -18731.5032), (2, 30, 'cdsf', 65289.5061), (3, 10, 'asdf', -87586.1517);

-- tests
select anyOrderBy(n, 'asc') from data;
select anyOrderBy(n, 'desc') from data;
select anyOrderBy(s, ' Asc') from data;
select anyOrderBy(s, ' DESC  ') from data;
select anyOrderBy(d, 'asc') from data;
select anyOrderBy(d, ' ') from data;

-- cleanup
DROP TABLE data;
