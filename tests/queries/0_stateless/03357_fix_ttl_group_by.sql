

CREATE TABLE simple_test_ttl_group_by (
  event_date DateTime,
  user_id Int32,
  item_id Int32,
  val Float32,
  rolled_by_item_id DateTime DEFAULT event_date,
  rolled_by_user_id DateTime DEFAULT event_date
) ENGINE = MergeTree()
      PRIMARY KEY (event_date, user_id, item_id)
      TTL rolled_by_item_id + INTERVAL 1 SECOND
          GROUP BY event_date, user_id, item_id
          SET val = sum(val),
          rolled_by_item_id = max(rolled_by_item_id) + INTERVAL 10 YEAR,
      rolled_by_user_id + INTERVAL 3 SECOND
          GROUP BY event_date, user_id
          SET val = sum(val),
              rolled_by_user_id = max(rolled_by_user_id) + INTERVAL 10 YEAR
      SETTINGS merge_with_ttl_timeout = 30;

-- Here will be generated 2 users with 5 events for 3 items
INSERT INTO simple_test_ttl_group_by
SELECT
    now() AS event_date,  -- Указываем фиксированное время
    number % 2 AS user_id,
    user_id * 10 + (number % 3) AS item_id,
    number AS val,
    event_date AS rolled_by_item_id,
    event_date AS rolled_by_user_id
FROM numbers(10);

-- Check before ttls: all ttl_info are equal to event_date
SELECT 'CHECK BEFORE BOTH TTL';
SELECT count() FROM simple_test_ttl_group_by;
with now() as event_date
SELECT
    toYear(group_by_ttl_info.min[1]) == toYear(toDate(event_date)),
    toYear(group_by_ttl_info.min[2]) == toYear(toDate(event_date)),
    toYear(group_by_ttl_info.max[1]) == toYear(toDate(event_date)),
    toYear(group_by_ttl_info.max[2]) == toYear(toDate(event_date))
FROM system.parts
WHERE active = 1 AND table = 'simple_test_ttl_group_by' and database = currentDatabase();

SELECT sleep(1);
OPTIMIZE TABLE simple_test_ttl_group_by FINAL;

-- Check after first ttl: first ttl_info are in 10 years, second are equal to date
SELECT 'CHECK AFTER FIRST TTL';
SELECT count() FROM simple_test_ttl_group_by;
with now() as event_date
SELECT
    toYear(group_by_ttl_info.min[1]) == toYear(toDate(event_date) + INTERVAL 10 YEAR),
    toYear(group_by_ttl_info.min[2]) == toYear(toDate(event_date)),
    toYear(group_by_ttl_info.max[1]) == toYear(toDate(event_date) + INTERVAL 10 YEAR),
    toYear(group_by_ttl_info.max[2]) == toYear(toDate(event_date))
FROM system.parts
WHERE active = 1 AND table = 'simple_test_ttl_group_by' and database = currentDatabase();

SELECT sleep(1);
SELECT sleep(1);

OPTIMIZE TABLE simple_test_ttl_group_by FINAL;

-- Check after second ttl: second ttl are equal to event_date in 10 years
SELECT 'CHECK AFTER SECOND TTL';
SELECT count() FROM simple_test_ttl_group_by;
with now() as event_date
SELECT
    toYear(group_by_ttl_info.min[1]) == toYear(toDate(event_date) + INTERVAL 10 YEAR),
    toYear(group_by_ttl_info.min[2]) == toYear(toDate(event_date) + INTERVAL 10 YEAR),
    toYear(group_by_ttl_info.max[1]) == toYear(toDate(event_date) + INTERVAL 10 YEAR),
    toYear(group_by_ttl_info.max[2]) == toYear(toDate(event_date) + INTERVAL 10 YEAR)
FROM system.parts
WHERE active = 1 AND table = 'simple_test_ttl_group_by' and database = currentDatabase();

DROP TABLE  simple_test_ttl_group_by;