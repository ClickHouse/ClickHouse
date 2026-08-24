SELECT round((countIf(rating = 5)) - (countIf(rating < 5)), 4) as nps,
       dense_rank() OVER (ORDER BY nps DESC)   as rank
FROM (select number as rating, number%3 rest_id from numbers(10))
group by rest_id
order by rank;
