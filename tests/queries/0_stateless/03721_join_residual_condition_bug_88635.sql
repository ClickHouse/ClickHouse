SET enable_analyzer = 1;

SELECT 1 FROM numbers(3) tx
JOIN numbers(3) ty
    ON tx.number = ty.number
JOIN numbers(3) tz
    ON tz.number = ty.number AND if(tx.number % 2 == 0, 1, 2) != if(tz.number % 2 == 0, 1, 3)
SETTINGS query_plan_use_logical_join_step = 0;
