-- Tags: no-fasttest
-- Tag no-fasttest: depends on cld2 and nlp-data

SET allow_experimental_nlp_functions = 1;

SELECT detectLanguage('Они сошлись. Волна и камень, Стихи и проза, лед и пламень, Не столь различны меж собой.');
SELECT detectLanguage('Sweet are the uses of adversity which, like the toad, ugly and venomous, wears yet a precious jewel in his head.');
SELECT detectLanguage('A vaincre sans peril, on triomphe sans gloire.');
SELECT detectLanguage('二兎を追う者は一兎をも得ず');
SELECT detectLanguage('有情饮水饱，无情食饭饥。');
SELECT detectLanguage('*****///// _____ ,,,,,,,, .....');
SELECT detectLanguageMixed('二兎を追う者は一兎をも得ず二兎を追う者は一兎をも得ず A vaincre sans peril, on triomphe sans gloire.');
SELECT detectLanguageMixed('어디든 가치가 있는 곳으로 가려면 지름길은 없다');
SELECT detectLanguageMixed('*****///// _____ ,,,,,,,, .....');

SELECT detectCharset('Plain English');
SELECT detectLanguageUnknown('Plain English');

SELECT detectTonality('милая кошка');
SELECT detectTonality('ненависть к людям');
SELECT detectTonality('обычная прогулка по ближайшему парку');

SELECT detectProgrammingLanguage('#include <iostream>');
