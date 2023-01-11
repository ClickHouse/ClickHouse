SELECT arrayShuffle([]);
SELECT arrayShuffle([], 0xbad_cafe);
SELECT arrayShuffle([9223372036854775808]);
SELECT arrayShuffle([9223372036854775808], 0xbad_cafe);
SELECT arrayShuffle([1,2,3,4,5,6,7,8,9,10], 0xbad_cafe);
SELECT arrayShuffle([1,2,3,4,5,6,7,8,9,10.1], 0xbad_cafe);
SELECT arrayShuffle([1,2,3,4,5,6,7,8,9,9223372036854775808], 0xbad_cafe);
SELECT arrayShuffle([1,2,3,4,5,6,7,8,9,NULL], 0xbad_cafe);
SELECT arrayShuffle([toFixedString('123', 3), toFixedString('456', 3), toFixedString('789', 3), toFixedString('ABC', 3), toFixedString('000', 3)], 0xbad_cafe);
SELECT arrayShuffle([toFixedString('123', 3), toFixedString('456', 3), toFixedString('789', 3), toFixedString('ABC', 3), NULL], 0xbad_cafe);
SELECT arrayShuffle(['storage','tiger','imposter','terminal','uniform','sensation'], 0xbad_cafe);
SELECT arrayShuffle(['storage','tiger',NULL,'terminal','uniform','sensation'], 0xbad_cafe);
SELECT arrayShuffle([NULL]);
SELECT arrayShuffle([NULL,NULL]);
SELECT arrayShuffle([[1,2,3,4],[-1,-2,-3,-4],[10,20,30,40],[100,200,300,400,500,600,700,800,900],[2,4,8,16,32,64]], 0xbad_cafe);
SELECT arrayShuffle([[1,2,3,4],[NULL,-2,-3,-4],[10,20,30,40],[100,200,300,400,500,600,700,800,900],[2,4,8,16,32,64]], 0xbad_cafe);
SELECT arrayShuffle(groupArray(x),0xbad_cafe) FROM (SELECT number as x from system.numbers LIMIT 100);
SELECT arrayShuffle(groupArray(toUInt64(x)),0xbad_cafe) FROM (SELECT number as x from system.numbers LIMIT 100);
SELECT arrayShuffle(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayShuffle([1], 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayShuffle([1], 1.1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayShuffle([1], 0xcafe, 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }