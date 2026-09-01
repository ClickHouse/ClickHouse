-- { echoOn }

SET transform_null_in = 0;

SET enable_analyzer = 1;

SELECT nullIn(x, (y, 1)), notNullIn(x, (y, 1)), globalNullIn(x, (y, 1)), globalNotNullIn(x, (y, 1)) FROM (SELECT materialize(NULL) AS x, materialize(NULL) AS y);
SELECT nullIn(x, if(number = 0, y, 1)), notNullIn(x, if(number = 0, y, 1)) FROM (SELECT number, materialize(NULL) AS x, materialize(NULL) AS y FROM numbers(1));
