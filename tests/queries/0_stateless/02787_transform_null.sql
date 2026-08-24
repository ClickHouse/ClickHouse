SELECT transform(0, [0, 1], ['ZERO', 'ONE'], 'DEFAULT') AS result;
SELECT transform(0, [0, 1], ['ZERO', 'ONE'], NULL) AS result;

SELECT CASE 1
  WHEN 0 THEN 'ZERO'
  WHEN 1 THEN 'ONE'
  ELSE 'NONE'
END AS result;

SELECT CASE 1
  WHEN 0 THEN NULL
  WHEN 1 THEN 'ONE'
  ELSE 'NONE'
END AS result;

select 
    case 1 
        when 1 then 'a' 
        else 'b'
    end value;

select 
    case 1 
        when 1 then 'a' 
    end value;

SELECT
    d,
    toInt16OrNull(d),
    caseWithExpression(d, 'a', 3, toInt16OrZero(d)) AS case_zero,
    caseWithExpression(d, 'a', 3, toInt16OrNull(d)) AS case_null,
    if(d = 'a', 3, toInt16OrZero(d)) AS if_zero,
    if(d = 'a', 3, toInt16OrNull(d)) AS if_null
FROM
(
    SELECT arrayJoin(['', '1', 'a']) AS d
)
ORDER BY
    case_zero ASC,
    d ASC;
