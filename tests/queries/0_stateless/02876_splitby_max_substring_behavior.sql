SELECT '-- splitByAlpha';
SELECT splitByAlpha('ab.cd.ef.gh') SETTINGS splitby_max_substring_behavior = '';
SELECT splitByAlpha('ab.cd.ef.gh', -1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByAlpha('ab.cd.ef.gh', 0) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByAlpha('ab.cd.ef.gh', 1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByAlpha('ab.cd.ef.gh', 2) SETTINGS splitby_max_substring_behavior = '';

SELECT splitByAlpha('ab.cd.ef.gh') SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByAlpha('ab.cd.ef.gh', -1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByAlpha('ab.cd.ef.gh', 0) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByAlpha('ab.cd.ef.gh', 1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByAlpha('ab.cd.ef.gh', 2) SETTINGS splitby_max_substring_behavior = 'python';

SELECT splitByAlpha('ab.cd.ef.gh') SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByAlpha('ab.cd.ef.gh', -1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByAlpha('ab.cd.ef.gh', 0) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByAlpha('ab.cd.ef.gh', 1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByAlpha('ab.cd.ef.gh', 2) SETTINGS splitby_max_substring_behavior = 'spark';

SELECT '-- splitByNonAlpha';
SELECT splitByNonAlpha('128.0.0.1') SETTINGS splitby_max_substring_behavior = '';
SELECT splitByNonAlpha('128.0.0.1', -1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByNonAlpha('128.0.0.1', 0) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByNonAlpha('128.0.0.1', 1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByNonAlpha('128.0.0.1', 2) SETTINGS splitby_max_substring_behavior = '';

SELECT splitByNonAlpha('128.0.0.1') SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByNonAlpha('128.0.0.1', -1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByNonAlpha('128.0.0.1', 0) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByNonAlpha('128.0.0.1', 1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByNonAlpha('128.0.0.1', 2) SETTINGS splitby_max_substring_behavior = 'python';

SELECT splitByNonAlpha('128.0.0.1') SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByNonAlpha('128.0.0.1', -1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByNonAlpha('128.0.0.1', 0) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByNonAlpha('128.0.0.1', 1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByNonAlpha('128.0.0.1', 2) SETTINGS splitby_max_substring_behavior = 'spark';

SELECT '-- splitByWhitespace';
SELECT splitByWhitespace('Nein, nein, nein! Doch!') SETTINGS splitby_max_substring_behavior = '';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', -1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', 0) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', 1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', 2) SETTINGS splitby_max_substring_behavior = '';

SELECT splitByWhitespace('Nein, nein, nein! Doch!') SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', -1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', 0) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', 1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', 2) SETTINGS splitby_max_substring_behavior = 'python';

SELECT splitByWhitespace('Nein, nein, nein! Doch!') SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', -1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', 0) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', 1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', 2) SETTINGS splitby_max_substring_behavior = 'spark';

SELECT '-- splitByChar';
SELECT splitByChar('=', 'a==b=c=d') SETTINGS splitby_max_substring_behavior = '';
SELECT splitByChar('=', 'a==b=c=d', -1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByChar('=', 'a==b=c=d', 0) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByChar('=', 'a==b=c=d', 1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByChar('=', 'a==b=c=d', 2) SETTINGS splitby_max_substring_behavior = '';

SELECT splitByChar('=', 'a==b=c=d') SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByChar('=', 'a==b=c=d', -1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByChar('=', 'a==b=c=d', 0) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByChar('=', 'a==b=c=d', 1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByChar('=', 'a==b=c=d', 2) SETTINGS splitby_max_substring_behavior = 'python';

SELECT splitByChar('=', 'a==b=c=d') SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByChar('=', 'a==b=c=d', -1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByChar('=', 'a==b=c=d', 0) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByChar('=', 'a==b=c=d', 1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByChar('=', 'a==b=c=d', 2) SETTINGS splitby_max_substring_behavior = 'spark';

SELECT '-- splitByString';

SELECT splitByString('==', 'a==b=c=d') SETTINGS splitby_max_substring_behavior = '';
SELECT splitByString('==', 'a==b=c=d', -1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByString('==', 'a==b=c=d', 0) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByString('==', 'a==b=c=d', 1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByString('==', 'a==b=c=d', 2) SETTINGS splitby_max_substring_behavior = '';

SELECT splitByString('==', 'a==b=c=d') SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByString('==', 'a==b=c=d', -1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByString('==', 'a==b=c=d', 0) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByString('==', 'a==b=c=d', 1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByString('==', 'a==b=c=d', 2) SETTINGS splitby_max_substring_behavior = 'python';

SELECT splitByString('==', 'a==b=c=d') SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByString('==', 'a==b=c=d', -1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByString('==', 'a==b=c=d', 0) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByString('==', 'a==b=c=d', 1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByString('==', 'a==b=c=d', 2) SETTINGS splitby_max_substring_behavior = 'spark';

SELECT splitByString('', 'a==b=c=d') SETTINGS splitby_max_substring_behavior = '';
SELECT splitByString('', 'a==b=c=d', -1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByString('', 'a==b=c=d', 0) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByString('', 'a==b=c=d', 1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByString('', 'a==b=c=d', 2) SETTINGS splitby_max_substring_behavior = '';

SELECT splitByString('', 'a==b=c=d') SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByString('', 'a==b=c=d', -1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByString('', 'a==b=c=d', 0) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByString('', 'a==b=c=d', 1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByString('', 'a==b=c=d', 2) SETTINGS splitby_max_substring_behavior = 'python';

SELECT splitByString('', 'a==b=c=d') SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByString('', 'a==b=c=d', -1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByString('', 'a==b=c=d', 0) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByString('', 'a==b=c=d', 1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByString('', 'a==b=c=d', 2) SETTINGS splitby_max_substring_behavior = 'spark';

SELECT '-- splitByRegexp';

SELECT splitByRegexp('\\d+', 'a12bc23de345f') SETTINGS splitby_max_substring_behavior = '';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', -1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', 0) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', 1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', 2) SETTINGS splitby_max_substring_behavior = '';

SELECT splitByRegexp('\\d+', 'a12bc23de345f') SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', -1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', 0) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', 1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', 2) SETTINGS splitby_max_substring_behavior = 'python';

SELECT splitByRegexp('\\d+', 'a12bc23de345f') SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', -1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', 0) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', 1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByRegexp('\\d+', 'a12bc23de345f', 2) SETTINGS splitby_max_substring_behavior = 'spark';

SELECT splitByRegexp('', 'a12bc23de345f') SETTINGS splitby_max_substring_behavior = '';
SELECT splitByRegexp('', 'a12bc23de345f', -1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByRegexp('', 'a12bc23de345f', 0) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByRegexp('', 'a12bc23de345f', 1) SETTINGS splitby_max_substring_behavior = '';
SELECT splitByRegexp('', 'a12bc23de345f', 2) SETTINGS splitby_max_substring_behavior = '';

SELECT splitByRegexp('', 'a12bc23de345f') SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByRegexp('', 'a12bc23de345f', -1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByRegexp('', 'a12bc23de345f', 0) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByRegexp('', 'a12bc23de345f', 1) SETTINGS splitby_max_substring_behavior = 'python';
SELECT splitByRegexp('', 'a12bc23de345f', 2) SETTINGS splitby_max_substring_behavior = 'python';

SELECT splitByRegexp('', 'a12bc23de345f') SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByRegexp('', 'a12bc23de345f', -1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByRegexp('', 'a12bc23de345f', 0) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByRegexp('', 'a12bc23de345f', 1) SETTINGS splitby_max_substring_behavior = 'spark';
SELECT splitByRegexp('', 'a12bc23de345f', 2) SETTINGS splitby_max_substring_behavior = 'spark';
