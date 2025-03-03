SET enable_analyzer=1;
CREATE OR REPLACE VIEW param_test AS SELECT {test_str:String} as s_result;
WITH 'OK' AS s SELECT * FROM param_test(test_str=s);
WITH (SELECT 123) AS s SELECT * FROM param_test(test_str=s);
WITH (SELECT 100 + 20 + 3) AS s SELECT * FROM param_test(test_str=s);
WITH (SELECT number FROM numbers(123, 1)) AS s SELECT * FROM param_test(test_str=s);
WITH CAST(123, 'String') AS s SELECT * FROM param_test(test_str=s);
