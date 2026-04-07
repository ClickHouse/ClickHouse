SET optimize_rewrite_array_exists_to_has = 0;

SELECT arrayExists(date -> (date = '2022-07-31'), [toDate('2022-07-31')]) AS date_exists;
