create table Position(Group Int32, Order Int32, Id Nullable(Int32)) ENGINE = Memory();
insert into Position values (7, 10, 5), (7, 20, 6), (7, 30, NULL), (7, 40, NULL);

SELECT
    	p.Id,
    	FIRST_VALUE(p.Id) OVER(ORDER BY p.Order DESC)
    FROM
    	Position p;

SELECT
    	p.Id,
    	FIRST_VALUE_RESPECT_NULLS(p.Id) OVER(ORDER BY p.Order DESC)
    FROM
    	Position p;

SELECT
    	p.Id,
    	FIRST_VALUE_IGNORE_NULLS(p.Id) OVER(ORDER BY p.Order DESC)
    FROM
    	Position p;

DROP TABLE Position;