-- Bug 37909

SELECT
    v_date AS vDate,
    round(sum(v_share)) AS v_sum
FROM
(
    WITH
        (
            SELECT rand() % 10000
        ) AS dummy_1,
        (
            SELECT rand() % 10000
        ) AS dummy_2,
        (
            SELECT rand() % 10000
        ) AS dummy_3,
        _v AS
        (
            SELECT
                xxHash64(rand()) % 100000 AS d_id,
                toDate(parseDateTimeBestEffort('2022-01-01') + (rand() % 2600000)) AS v_date
            FROM numbers(1000000)
            ORDER BY d_id ASC
        ),
        _i AS
        (
            SELECT xxHash64(rand()) % 40000 AS d_id
            FROM numbers(1000000)
        ),
        not_i AS
        (
            SELECT
                NULL AS v_date,
                d_id,
                0 AS v_share
            FROM _i
            LIMIT 100
        )
    SELECT *
    FROM
    (
        SELECT
            d_id,
            v_date,
            v_share
        FROM not_i
        UNION ALL
        SELECT
            d_id,
            v_date,
            1 AS v_share
        FROM
        (
            SELECT
                d_id,
                arrayJoin(groupArray(v_date)) AS v_date
            FROM
            (
                SELECT
                    v_date,
                    d_id
                FROM _v
                UNION ALL
                SELECT
                    NULL AS v_date,
                    d_id
                FROM _i
            )
            GROUP BY d_id
        )
    )
    WHERE (v_date >= '2022-05-08') AND (v_date <= '2022-06-07')
)
/* WHERE (v_date >= '2022-05-08') AND (v_date <= '2022-06-07') placing condition has same effect */
GROUP BY vDate
ORDER BY vDate ASC
SETTINGS enable_analyzer = 1; -- the query times out if enable_analyzer = 0
