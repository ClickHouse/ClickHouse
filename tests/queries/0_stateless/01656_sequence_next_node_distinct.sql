SET allow_experimental_funnel_functions = 1;

DROP TABLE IF EXISTS events_demo;

CREATE TABLE events_demo (
  id UInt32,
  dt DateTime,
  action Nullable(String)
) ENGINE = MergeTree()
ORDER BY
  (id, dt);

INSERT INTO
  events_demo (id, dt, action)
VALUES
  (1, '2025-06-17 09:00:00', 'A'),
  (1, '2025-06-17 09:00:10', 'B'),
  (1, '2025-06-17 09:00:20', 'B'),
  (1, '2025-06-17 09:00:30', 'C'),
  (1, '2025-06-17 09:00:40', NULL),
  (2, '2025-06-17 08:00:00', 'X'),
  (2, '2025-06-17 08:00:00', 'X'),
  (2, '2025-06-17 08:00:10', 'A'),
  (2, '2025-06-17 08:00:20', NULL),
  (2, '2025-06-17 08:00:30', 'Y'),
  (2, '2025-06-17 08:00:30', 'Y'),
  (2, '2025-06-17 08:00:40', 'A'),
  (2, '2025-06-17 08:00:50', 'Y'),
  (1, '2025-06-17 09:00:40', NULL);

SELECT
  DISTINCT '(forward, head, A->B)',
  id,
  sequenceNextNodeDistinct('forward', 'head', 4) (
    dt,
    action,
    action = 'A',
    toNullable(1 IS NOT NULL)
    AND (NOT toNullable(isNullable(1)))
  ) IGNORE NULLS AS next_node
FROM
  events_demo
GROUP BY
  * WITH ROLLUP WITH TOTALS
ORDER BY
  ALL ASC NULLS LAST;
