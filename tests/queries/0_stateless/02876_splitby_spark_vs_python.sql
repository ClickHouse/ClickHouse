SELECT 'splitByAlpha';
SELECT splitByAlpha('ab.cd.ef.gh', 2) settings split_tokens_like_python = 0;
SELECT splitByAlpha('ab.cd.ef.gh', 2) settings split_tokens_like_python = 1;

SELECT 'splitByNonAlpha';
SELECT splitByNonAlpha('128.0.0.1', 2) settings split_tokens_like_python = 0;
SELECT splitByNonAlpha('128.0.0.1', 2) settings split_tokens_like_python = 1;

SELECT 'splitByWhitespace';
SELECT splitByWhitespace('Nein, nein, nein! Doch!', 2) settings split_tokens_like_python = 0;
SELECT splitByWhitespace('Nein, nein, nein! Doch!', 2) settings split_tokens_like_python = 1;

SELECT 'splitByChar';
SELECT splitByChar('=', 'a=b=c=d', 2) SETTINGS split_tokens_like_python = 0;
SELECT splitByChar('=', 'a=b=c=d', 2) SETTINGS split_tokens_like_python = 1;

SELECT 'splitByString';
SELECT splitByString('', 'a==b==c==d', 2) SETTINGS split_tokens_like_python = 0;
SELECT splitByString('', 'a==b==c==d', 2) SETTINGS split_tokens_like_python = 1;
SELECT splitByString('==', 'a==b==c==d', 2) SETTINGS split_tokens_like_python = 0;
SELECT splitByString('==', 'a==b==c==d', 2) SETTINGS split_tokens_like_python = 1;

SELECT 'splitByRegexp';
SELECT splitByRegexp('', 'a12bc23de345f', 2) SETTINGS split_tokens_like_python = 0;
SELECT splitByRegexp('', 'a12bc23de345f', 2) SETTINGS split_tokens_like_python = 1;
SELECT splitByRegexp('\\d+', 'a12bc23de345f', 2) SETTINGS split_tokens_like_python = 0;
SELECT splitByRegexp('\\d+', 'a12bc23de345f', 2) SETTINGS split_tokens_like_python = 1;
