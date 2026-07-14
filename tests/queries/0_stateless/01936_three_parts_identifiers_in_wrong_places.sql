SET enable_analyzer = 1;

SELECT dictGet(t.nest.a, concat(currentDatabase(), '.dict.dict'), 's', number) FROM numbers(5); -- { serverError INVALID_IDENTIFIER }

SELECT dictGetFloat64(t.b.s, 'database_for_dict.dict1', dictGetFloat64('Ta\0', toUInt64('databas\0_for_dict.dict1databas\0_for_dict.dict1', dictGetFloat64('', '', toUInt64(1048577), toDate(NULL)), NULL), toDate(dictGetFloat64(257, 'database_for_dict.dict1database_for_dict.dict1', '', toUInt64(NULL), 2, toDate(NULL)), '2019-05-2\0')), NULL, toUInt64(dictGetFloat64('', '', toUInt64(-9223372036854775808), toDate(NULL)), NULL)); -- { serverError INVALID_IDENTIFIER }

SELECT NULL AND (2147483648 AND NULL) AND -2147483647, toUUID(((1048576 AND NULL) AND (2147483647 AND 257 AND NULL AND -2147483649) AND NULL) IN (test_01103.t1_distr.id), '00000000-e1fe-11e\0-bb8f\0853d60c00749'), stringToH3('89184926cc3ffff89184926cc3ffff89184926cc3ffff89184926cc3ffff89184926cc3ffff89184926cc3ffff89184926cc3ffff89184926cc3ffff'); -- { serverError INVALID_IDENTIFIER }

SELECT 'still alive';
