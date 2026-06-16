select
  toYYYYMM(date) as date_,
  n
from (select
  [toDate('20230815'), toDate('20230816')] as date,
  [1, 2] as n
) as data
array join date, n
where date_ >= 202303;
