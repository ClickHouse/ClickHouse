-- Tags: no-fasttest

SET session_timezone='Europe/Madrid'; -- disable time zone randomization in CI
SELECT if(length(x) = 26, ULIDStringToDateTime(x, 'Europe/Madrid'), toDateTime('2024-02-21 12:00:00', 'Europe/Madrid')) AS datetime
FROM values('x String', '01HQ3KJJKHRWP357YVYBX32WHY', '01HQ3KJJKH')
