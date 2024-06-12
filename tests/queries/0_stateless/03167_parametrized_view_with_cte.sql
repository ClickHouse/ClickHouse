SET allow_experimental_analyzer=1;
CREATE OR REPLACE VIEW param_test AS SELECT {test_str:String} as s_result;
WITH 'OK' AS s SELECT * FROM param_test(test_str=s);
WITH CAST(123, String) AS s SELECT * FROM param_test(test_str=s);
