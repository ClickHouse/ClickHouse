SELECT if(d = 4, d, 1)
FROM
(
  SELECT materialize(1::BFloat16) as d
);
