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

Insert into tt values(1,2),(2,3),(3,4),(4,5),(5,6);

select * from tt where pointInPolygon((x, y), [(2, 0), (3.5, 4), (3.5, 0)]);

--select * from tt where pointInPolygon((x, y), [(0, 0), (2, 3), (4, 0)]);

select * from tt where pointInPolygon((x, y), [(3, 0), (4, 4), (4, 1)]);

drop table if exists tt;
