-- VisitParam with basic type
SELECT visitParamExtractInt('{"myparam":-1}', 'myparam');
SELECT visitParamExtractUInt('{"myparam":-1}', 'myparam');
SELECT visitParamExtractFloat('{"myparam":null}', 'myparam');
SELECT visitParamExtractFloat('{"myparam":-1}', 'myparam');
SELECT visitParamExtractBool('{"myparam":true}', 'myparam');
SELECT visitParamExtractString('{"myparam":"test_string"}', 'myparam');
SELECT visitParamExtractString('{"myparam":"test\\"string"}', 'myparam');
-- VisitParam with complex type
SELECT visitParamExtractRaw('{"myparam":"test_string"}', 'myparam');
SELECT visitParamExtractRaw('{"myparam": "test_string"}', 'myparam');
SELECT visitParamExtractRaw('{"myparam": "test\\"string"}', 'myparam');
SELECT visitParamExtractRaw('{"myparam": "test\\"string", "other":123}', 'myparam');
SELECT visitParamExtractRaw('{"myparam": ["]", "2", "3"], "other":123}', 'myparam');
SELECT visitParamExtractRaw('{"myparam": {"nested" : [1,2,3]}, "other":123}', 'myparam');
-- jsonAny
SELECT jsonAny('{"myparam":"test_string"}', 'myparam');
SELECT jsonAny('{"myparam": "test_string"}', 'myparam');
SELECT jsonAny('{"myparam": 5}', 'myparam');
SELECT jsonAny('{"myparam": ["]", "2", "3"], "other":123}', 'myparam');
SELECT jsonAny('{"myparam": {"nested": [1,2,3]}, "other":123}', 'myparam');
SELECT jsonAny('{"myparam": {"nested": [1,2,3]}, "other":123}', 'myparam.nested');
SELECT jsonAny('{"myparam": {"nested": [1,2,3]}, "other":123}', 'myparam.not_there');
SELECT jsonAny('{"myparam": [{"array_test": 1}, {"array_test": 2}, {"array_test": 3}], "other":123}', 'myparam.array_test');
SELECT jsonAny('{"myparam": {"nested": [1,2,3]}, "other":123}', '.myparam');
SELECT jsonAny('{"myparam": {"nested": {"hh": "abc"}}, "other":123}', 'myparam.nested.hh');
SELECT jsonAny('{"myparam": {"nested": {"hh": "abc"}}, "other":123}', 'myparam..hh');
SELECT jsonAny('{"myparam": {"nested": {"hh": "abc"}}, "other":123}', 'myparam.');
-- jsonAnyArray
SELECT jsonAnyArray('{"myparam": "test_string"}', 'myparam');
SELECT jsonAnyArray('{"myparam": 5}', 'myparam');
SELECT jsonAnyArray('{"myparam": {"nested": [1,2,3]}, "other":123}', 'myparam');
SELECT jsonAnyArray('{"myparam": {"nested": [1,2,3]}, "other":123}', 'myparam.nested');
SELECT jsonAnyArray('{"myparam": [{"array_test": 1}, {"array_test": 2}, {"array_test": 3}], "other":123}', 'myparam.array_test');
SELECT jsonAnyArray('{"myparam": []}', 'myparam');
-- jsonAll
SELECT jsonAll('{"myparam": [{"A": 1, "B": [{"leaf": 14},{"leaf": 15},{"leaf": 16}]}, {"A": 2, "B": [{"leaf": 24},{"leaf": 25},{"leaf": 26}]}, {"A": 3, "B": [{"leaf": 34},{"leaf": 35},{"leaf": 36}]}]}', 'myparam.A');
SELECT jsonAll('{"myparam": [{"A": 1, "B": [{"leaf": 14},{"leaf": 15},{"leaf": 16}]}, {"A": 2, "B": [{"leaf": 24},{"leaf": 25},{"leaf": 26}]}, {"A": 3, "B": [{"leaf": 34},{"leaf": 35},{"leaf": 36}]}]}', 'myparam.B.leaf');
-- jsonAllArrays
SELECT jsonAllArrays('{"myparam": [{"A": 1, "B": [17,18,19]}, {"A": 2, "B": [27,28,29]}, {"A": 3, "B": [37,38,39]}]}', 'myparam.B');
-- jsonCount
SELECT jsonCount('{"myparam": 5}', 'myparam');
SELECT jsonCount('{"myparam": 5}', 'notthere');
SELECT jsonCount('{"myparam": [{"A": 1, "B": [17,18,19]}, {"A": 2, "B": [27,28,29]}, {"A": 3, "B": [37,38,39]}]}', 'myparam.B');
