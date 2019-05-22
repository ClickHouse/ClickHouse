DROP TABLE IF EXISTS array;
CREATE TABLE array (arr Array(Nullable(Float64))) ENGINE = Memory;
INSERT INTO array(arr) values ([1,2]),([3,4]),([5,6]),([7,8]);

select * from array where arr > [12.2];
select * from array where arr > [null, 12.2];
select * from array where arr > [null, 12];

DROP TABLE array;
