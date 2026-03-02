SELECT
    multiIf((
        SELECT count(*)
        FROM store_sales
        WHERE (ss_quantity >= 1) AND (ss_quantity <= 20)
    ) > 74129, (
        SELECT avg(ss_ext_discount_amt)
        FROM store_sales
        WHERE (ss_quantity >= 1) AND (ss_quantity <= 20)
    ), (
        SELECT avg(ss_net_paid)
        FROM store_sales
        WHERE (ss_quantity >= 1) AND (ss_quantity <= 20)
    )) AS bucket1,
    multiIf((
        SELECT count(*)
        FROM store_sales
        WHERE (ss_quantity >= 21) AND (ss_quantity <= 40)
    ) > 122840, (
        SELECT avg(ss_ext_discount_amt)
        FROM store_sales
        WHERE (ss_quantity >= 21) AND (ss_quantity <= 40)
    ), (
        SELECT avg(ss_net_paid)
        FROM store_sales
        WHERE (ss_quantity >= 21) AND (ss_quantity <= 40)
    )) AS bucket2,
    multiIf((
        SELECT count(*)
        FROM store_sales
        WHERE (ss_quantity >= 41) AND (ss_quantity <= 60)
    ) > 56580, (
        SELECT avg(ss_ext_discount_amt)
        FROM store_sales
        WHERE (ss_quantity >= 41) AND (ss_quantity <= 60)
    ), (
        SELECT avg(ss_net_paid)
        FROM store_sales
        WHERE (ss_quantity >= 41) AND (ss_quantity <= 60)
    )) AS bucket3,
    multiIf((
        SELECT count(*)
        FROM store_sales
        WHERE (ss_quantity >= 61) AND (ss_quantity <= 80)
    ) > 10097, (
        SELECT avg(ss_ext_discount_amt)
        FROM store_sales
        WHERE (ss_quantity >= 61) AND (ss_quantity <= 80)
    ), (
        SELECT avg(ss_net_paid)
        FROM store_sales
        WHERE (ss_quantity >= 61) AND (ss_quantity <= 80)
    )) AS bucket4,
    multiIf((
        SELECT count(*)
        FROM store_sales
        WHERE (ss_quantity >= 81) AND (ss_quantity <= 100)
    ) > 165306, (
        SELECT avg(ss_ext_discount_amt)
        FROM store_sales
        WHERE (ss_quantity >= 81) AND (ss_quantity <= 100)
    ), (
        SELECT avg(ss_net_paid)
        FROM store_sales
        WHERE (ss_quantity >= 81) AND (ss_quantity <= 100)
    )) AS bucket5
FROM reason
WHERE r_reason_sk = 1;