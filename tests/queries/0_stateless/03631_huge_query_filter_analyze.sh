#!/usr/bin/env bash
# Tags: long, no-msan, no-tsan, no-asan, no-ubsan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query "

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS events;

CREATE TABLE users
(
    org_user_id UInt32,
    org_id UInt16,
    is_archived UInt8 DEFAULT 0,
    is_inactive UInt8 DEFAULT 0,
    user_last_seen DateTime64(3, 'UTC'),
    group_ids Array(UInt32),
    group_state_ids Map(String, UInt32)
)
ENGINE = MergeTree()
ORDER BY (org_id, org_user_id);

CREATE TABLE events
(
    org_user_id UInt32,
    org_id UInt16,
    source_id UInt32,
    event_time DateTime64(3, 'UTC'),
    is_archived UInt8 DEFAULT 0,
    metrics Array(Tuple(metric_keys UInt8, metric_values UInt32))
)
ENGINE = MergeTree()
ORDER BY (org_id, org_user_id);
"




HUGE_QUERY="
SELECT ignore(*) AS count
FROM users
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(((materialize(CAST(JSONExtract('{"1": {"2": 42}}', 'Map(String, Map(String, UInt32))'), 'Map(UInt8, Map(UInt8, UInt32))'))[events.source_id])[metric.metric_keys]) * metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.556', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.556', 3, 'UTC')) AND (events.source_id IN (10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008, 10009, 10010, 10011, 10012, 10013, 10014, 10015, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, 10024, 10025, 10026, 10027, 10028, 10029, 10030, 10031, 10032, 10033, 10034, 10035, 10036, 10037, 10038, 10039, 10040, 10041, 10042)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS score_points_abc123def456 ON users.org_user_id = score_points_abc123def456.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10034)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.557', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.557', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10034, 15)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_xyz789abc123 ON users.org_user_id = event_count_xyz789abc123.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10034)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10034, 48)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_qwe456rty789 ON users.org_user_id = event_count_qwe456rty789.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10040)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10040, 2)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_asd123fgh456 ON users.org_user_id = event_count_asd123fgh456.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10040)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10040, 123)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_zxc789vbn012 ON users.org_user_id = event_count_zxc789vbn012.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10040)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10040, 122)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_poi098uyt765 ON users.org_user_id = event_count_poi098uyt765.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10037)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10037, 15)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_lkj432hgf876 ON users.org_user_id = event_count_lkj432hgf876.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10038)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10038, 15)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_mnb543vcx098 ON users.org_user_id = event_count_mnb543vcx098.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10005)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10005, 4)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_qaz123wsx456 ON users.org_user_id = event_count_qaz123wsx456.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10026)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10026, 15)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_edc789rfv345 ON users.org_user_id = event_count_edc789rfv345.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10039)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10039, 4)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_tgb678yhn012 ON users.org_user_id = event_count_tgb678yhn012.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10039)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10039, 3)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_ujm890ik123 ON users.org_user_id = event_count_ujm890ik123.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10001, 10002, 10003, 10004)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN ((10001, 12), (10002, 12), (10003, 12), (10004, 12))) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_plm456okn789 ON users.org_user_id = event_count_plm456okn789.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10001, 10002, 10003, 10004)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN ((10001, 103), (10002, 103), (10003, 103), (10004, 103))) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_vfr567cde234 ON users.org_user_id = event_count_vfr567cde234.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10001, 10002, 10003, 10004)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN ((10001, 121), (10002, 121), (10003, 121), (10004, 121))) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_bgtyhn890 ON users.org_user_id = event_count_bgtyhn890.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10001, 10002, 10003, 10004)) AND (events.event_time >= toDateTime64('2025-01-16T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN ((10001, 13), (10002, 13), (10003, 13), (10004, 13))) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_nhgfds123456 ON users.org_user_id = event_count_nhgfds123456.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10034)) AND (events.event_time >= toDateTime64('2025-06-18T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10034, 4)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_qwerty789012 ON users.org_user_id = event_count_qwerty789012.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10034)) AND (events.event_time >= toDateTime64('2025-06-18T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10034, 15)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_asdfgh345678 ON users.org_user_id = event_count_asdfgh345678.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10040)) AND (events.event_time >= toDateTime64('2025-06-18T22:06:24.558', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.558', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10040, 1)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_zxcvbn901234 ON users.org_user_id = event_count_zxcvbn901234.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10040)) AND (events.event_time >= toDateTime64('2025-06-18T22:06:24.559', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.559', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10040, 122)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_poiuyt567890 ON users.org_user_id = event_count_poiuyt567890.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10039)) AND (events.event_time >= toDateTime64('2025-06-18T22:06:24.559', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.559', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10039, 4)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_lkjhgf234567 ON users.org_user_id = event_count_lkjhgf234567.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10039)) AND (events.event_time >= toDateTime64('2025-06-18T22:06:24.559', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.559', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10039, 3)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_mnbvcx890123 ON users.org_user_id = event_count_mnbvcx890123.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10001, 10002, 10003, 10004)) AND (events.event_time >= toDateTime64('2025-04-23T22:06:24.559', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.559', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN ((10001, 13), (10002, 13), (10003, 13), (10004, 13))) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_qazwsx456789 ON users.org_user_id = event_count_qazwsx456789.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10005)) AND (events.event_time >= toDateTime64('2025-04-23T22:06:24.559', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.559', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10005, 5)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_edcrfv012345 ON users.org_user_id = event_count_edcrfv012345.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10034)) AND (events.event_time >= toDateTime64('2025-06-18T22:06:24.559', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.559', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10034, 48)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_tgbnhy678901 ON users.org_user_id = event_count_tgbnhy678901.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10040)) AND (events.event_time >= toDateTime64('2025-06-18T22:06:24.559', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.559', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10040, 2)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_yujmki234567 ON users.org_user_id = event_count_yujmki234567.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10040)) AND (events.event_time >= toDateTime64('2025-06-18T22:06:24.559', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.559', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10040, 123)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_plokij890123 ON users.org_user_id = event_count_plokij890123.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10037)) AND (events.event_time >= toDateTime64('2025-06-18T22:06:24.559', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.559', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10037, 15)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_vfrcdx456789 ON users.org_user_id = event_count_vfrcdx456789.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10001, 10002, 10003, 10004)) AND (events.event_time >= toDateTime64('2025-04-23T22:06:24.559', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.559', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN ((10001, 12), (10002, 12), (10003, 12), (10004, 12))) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_bgtyhnju012345 ON users.org_user_id = event_count_bgtyhnju012345.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10001, 10002, 10003, 10004)) AND ((events.source_id, metric.metric_keys) IN ((10001, 114), (10002, 114), (10003, 114), (10004, 114))) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_nhyujm678901 ON users.org_user_id = event_count_nhyujm678901.org_user_id
ALL LEFT JOIN
(
    SELECT
        org_user_id,
        sum(metric.metric_values) AS result
    FROM events
    ARRAY JOIN metrics AS metric
    WHERE (events.is_archived = 0) AND (events.org_id = 1234) AND (events.source_id IN (10005)) AND (events.event_time >= toDateTime64('2025-04-23T22:06:24.559', 3, 'UTC')) AND (events.event_time < toDateTime64('2025-07-16T22:06:24.559', 3, 'UTC')) AND ((events.source_id, metric.metric_keys) IN (10005, 4)) AND ((events.source_id, metric.metric_keys) NOT IN ((10031, 15), (10029, 15), (10002, 15), (10030, 15), (10037, 15), (10038, 15), (10039, 15), (10028, 15), (10040, 3), (10041, 15), (10027, 15), (10001, 15), (10003, 15), (10004, 15), (10042, 15)))
    GROUP BY org_user_id
) AS event_count_mkloiu234567 ON users.org_user_id = event_count_mkloiu234567.org_user_id
WHERE (users.is_archived = 0) AND (users.org_id = 1234) AND (users.is_inactive = 0) AND ((users.user_last_seen >= toDateTime64('2025-01-17T22:06:24.562', 3, 'UTC')) AND (((event_count_xyz789abc123.result IS NOT NULL) AND (event_count_xyz789abc123.result >= 1)) OR ((event_count_qwe456rty789.result IS NOT NULL) AND (event_count_qwe456rty789.result >= 1)) OR ((event_count_asd123fgh456.result IS NOT NULL) AND (event_count_asd123fgh456.result >= 1)) OR ((event_count_zxc789vbn012.result IS NOT NULL) AND (event_count_zxc789vbn012.result >= 1)) OR ((event_count_poi098uyt765.result IS NOT NULL) AND (event_count_poi098uyt765.result >= 1)) OR ((event_count_lkj432hgf876.result IS NOT NULL) AND (event_count_lkj432hgf876.result >= 1)) OR ((event_count_mnb543vcx098.result IS NOT NULL) AND (event_count_mnb543vcx098.result >= 1)) OR ((event_count_qaz123wsx456.result IS NOT NULL) AND (event_count_qaz123wsx456.result >= 1)) OR ((event_count_edc789rfv345.result IS NOT NULL) AND (event_count_edc789rfv345.result >= 1)) OR ((event_count_tgb678yhn012.result IS NOT NULL) AND (event_count_tgb678yhn012.result >= 1)) OR ((event_count_ujm890ik123.result IS NOT NULL) AND (event_count_ujm890ik123.result >= 1)) OR ((event_count_plm456okn789.result IS NOT NULL) AND (event_count_plm456okn789.result >= 1)) OR ((event_count_vfr567cde234.result IS NOT NULL) AND (event_count_vfr567cde234.result >= 1)) OR ((event_count_bgtyhn890.result IS NOT NULL) AND (event_count_bgtyhn890.result >= 1)) OR ((event_count_nhgfds123456.result IS NOT NULL) AND (event_count_nhgfds123456.result >= 1))) AND (NOT ((users.user_last_seen >= toDateTime64('2025-01-17T22:06:24.562', 3, 'UTC')) AND (((event_count_qwerty789012.result IS NOT NULL) AND (event_count_qwerty789012.result >= 1) AND ((event_count_qwerty789012.result IS NULL) OR (event_count_qwerty789012.result <= 2))) OR ((event_count_asdfgh345678.result IS NOT NULL) AND (event_count_asdfgh345678.result >= 2) AND ((event_count_asdfgh345678.result IS NULL) OR (event_count_asdfgh345678.result <= 3))) OR ((event_count_zxcvbn901234.result IS NOT NULL) AND (event_count_zxcvbn901234.result >= 2)) OR ((event_count_poiuyt567890.result IS NOT NULL) AND (event_count_poiuyt567890.result >= 2)) OR ((event_count_lkjhgf234567.result IS NOT NULL) AND (event_count_lkjhgf234567.result >= 3)) OR ((event_count_mnbvcx890123.result IS NOT NULL) AND (event_count_mnbvcx890123.result >= 5)) OR ((event_count_qazwsx456789.result IS NOT NULL) AND (event_count_qazwsx456789.result >= 2)) OR ((event_count_edcrfv012345.result IS NOT NULL) AND (event_count_edcrfv012345.result >= 2))))) AND (NOT ((users.user_last_seen >= toDateTime64('2025-01-17T22:06:24.563', 3, 'UTC')) AND (score_points_abc123def456.result IS NOT NULL) AND (score_points_abc123def456.result >= 11) AND ((score_points_abc123def456.result IS NULL) OR (score_points_abc123def456.result <= 50)) AND (((event_count_tgbnhy678901.result IS NOT NULL) AND (event_count_tgbnhy678901.result >= 3)) OR ((event_count_asdfgh345678.result IS NOT NULL) AND (event_count_asdfgh345678.result >= 1) AND ((event_count_asdfgh345678.result IS NULL) OR (event_count_asdfgh345678.result <= 3))) OR ((event_count_yujmki234567.result IS NOT NULL) AND (event_count_yujmki234567.result >= 2)) OR ((event_count_plokij890123.result IS NOT NULL) AND (event_count_plokij890123.result >= 2)) OR ((event_count_poiuyt567890.result IS NOT NULL) AND (event_count_poiuyt567890.result >= 2)) OR ((event_count_vfrcdx456789.result IS NOT NULL) AND (event_count_vfrcdx456789.result >= 2)) OR ((event_count_lkjhgf234567.result IS NOT NULL) AND (event_count_lkjhgf234567.result >= 2)) OR ((event_count_bgtyhnju012345.result IS NOT NULL) AND (event_count_bgtyhnju012345.result >= 1)) OR ((event_count_nhyujm678901.result IS NOT NULL) AND (event_count_nhyujm678901.result >= 1)) OR ((event_count_mkloiu234567.result IS NOT NULL) AND (event_count_mkloiu234567.result >= 1))))) AND hasAll(users.group_ids, _CAST([5000001], 'Array(UInt32)')) AND (users.org_user_id IN (
    SELECT org_user_id
    FROM users
    WHERE (users.org_id = 1234) AND hasAll(users.group_ids, _CAST([5000001], 'Array(UInt32)'))
)) AND (NOT hasAny(mapValues(users.group_state_ids), _CAST([5000002], 'Array(UInt32)'))))
"

${CLICKHOUSE_CLIENT} --query="$HUGE_QUERY SETTINGS enable_analyzer=0"
${CLICKHOUSE_CLIENT} --query="$HUGE_QUERY SETTINGS enable_analyzer=1"
${CLICKHOUSE_CLIENT} --query="EXPLAIN PLAN actions = 1, description = 1 $HUGE_QUERY SETTINGS enable_analyzer=0 FORMAT Null"
${CLICKHOUSE_CLIENT} --query="EXPLAIN PLAN actions = 1, description = 1 $HUGE_QUERY SETTINGS enable_analyzer=1 FORMAT Null"

