SET allow_experimental_analyzer = 1;
SELECT age('year', toDate(materialize(_CAST('2018-01-01', 'LowCardinality(String)'))), toDate('2017-01-01')) GROUP BY _CAST(17167, 'Date');
SELECT age('quarter', toDate(materialize(_CAST('2018-01-01', 'LowCardinality(String)'))), toDate('2017-01-01')) GROUP BY _CAST(17167, 'Date');
