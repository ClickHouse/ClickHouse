-- Tags: no-fasttest
-- Tag no-fasttest: needs s2

select s2CellsIntersect(9926595209846587392, 9926594385212866560);
select s2CellsIntersect(9926595209846587392, 9937259648002293760);


SELECT s2CellsIntersect(9926595209846587392, 9223372036854775806); -- { serverError 36 }
