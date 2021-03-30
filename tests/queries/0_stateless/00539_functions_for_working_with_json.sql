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
SELECT visitParamExtractRaw('{"myparam": "{"}', 'myparam');
SELECT visitParamExtractRaw('{"myparam": "["}', 'myparam');
SELECT visitParamExtractRaw('{"myparam": ["]", "2", "3"], "other":123}', 'myparam');
SELECT visitParamExtractRaw('{"myparam": {"nested" : [1,2,3]}, "other":123}', 'myparam');

SELECT JSONExtractInt('{"myparam":-1}', 'myparam');
SELECT JSONExtractUInt('{"myparam":-1}', 'myparam');
SELECT JSONExtractFloat('{"myparam":null}', 'myparam');
SELECT JSONExtractFloat('{"myparam":-1}', 'myparam');
SELECT JSONExtractBool('{"myparam":true}', 'myparam');
SELECT JSONExtractString('{"myparam":"test_string"}', 'myparam');
SELECT JSONExtractString('{"myparam":"test\\"string"}', 'myparam');
