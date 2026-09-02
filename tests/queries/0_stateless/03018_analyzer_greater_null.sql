SELECT
    max(NULL > 255) > NULL AS a,
    count(NULL > 1.) > 1048577
FROM numbers(10);
