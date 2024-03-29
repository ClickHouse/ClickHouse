drop table if exists tt;

CREATE TABLE tt
(
    `x` UInt32,
    `y` UInt32,
    INDEX mm_x_y (x, y) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY x
SETTINGS index_granularity = 3;

Insert into tt values(4, 4), (6 ,6), (8, 8), (12, 12), (14, 14), (16, 16);

select '1/2 marks filtered by minmax index, read_rows should be 3';
select * from tt where pointInPolygon((x, y), [(4., 4.), (8., 4.), (8., 8.), (4., 8.)]) format JSON;
select * from tt where pointInPolygon((x, y), [(10., 13.), (14., 14.), (14, 10)])  format JSON;

select '2/2 marks filtered by minmax index, read_rows should be 0';
select * from tt where pointInPolygon((x, y), [(0., 0.), (2., 2.), (2., 0.)]) format JSON;
select * from tt where pointInPolygon((x, y), [(4., 0.), (8., 4.), (4., 8.), (0., 4.)], [(3., 3.), (3., 5.), (5., 5.), (5., 3.)]) format JSON;

drop table if exists tt;
