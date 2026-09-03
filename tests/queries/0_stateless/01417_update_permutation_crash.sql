select tuple(1, 1, number) as t from numbers_mt(1000001) order by t, number limit 1;
