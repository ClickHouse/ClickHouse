CREATE TABLE left (g UInt32, i UInt32)
  ORDER BY (g, i);

INSERT INTO left VALUES
(0, 1), (0, 2), (0, 3), (0, 4), (0, 5), (2, 0);

CREATE TABLE right (g UInt32, i UInt32)
  ORDER BY (g, i);

INSERT INTO right VALUES
(0,0), (0, 3), (0, 4), (0, 6), (1, 0);

SET enable_analyzer = 1;

SELECT 'run enable-analyzer=1';
with differences as
    (
        (
            select g, i from left
            where g BETWEEN 0 and 10
            EXCEPT ALL
            select g, i from right
            where g BETWEEN 0 and 10
        )
        UNION ALL
        (
            select g, i from right
            where g BETWEEN 0 and 10
            EXCEPT ALL
            select g, i from left
            where g BETWEEN 0 and 10
        )
    ),
diff_counts as
    (
        select g, count(*) from differences group by g
    )
select * from diff_counts
ORDER BY g;

SELECT 'run enable-analyzer=1 ignore';
with differences as
    (
        (
            select g, i from left
            where g BETWEEN 0 and 10
            EXCEPT ALL
            select g, i from right
            where g BETWEEN 0 and 10
        )
        UNION ALL
        (
            select g, i from right
            where g BETWEEN 0 and 10
            EXCEPT ALL
            select g, i from left
            where g BETWEEN 0 and 10
        )
    ),
diff_counts as
    (
        select g, count(ignore(*)) from differences group by g
    )
select * from diff_counts
ORDER BY g;
