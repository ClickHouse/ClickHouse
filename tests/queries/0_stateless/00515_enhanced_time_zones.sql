SET allow_deprecated_snowflake_conversion_functions = 1;

SELECT addMonths(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 1, 'Asia/Kolkata');
SELECT addMonths(toDateTime('2017-11-05 10:37:47', 'Asia/Kolkata'), 1);
SELECT addMonths(toTimeZone(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 'Asia/Kolkata'), 1);

SELECT addMonths(toDateTime('2017-11-05 08:07:47'), 1);
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), 1);
SELECT addMonths(toDateTime('2017-11-05 08:07:47'), materialize(1));
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), materialize(1));

SELECT addMonths(toDateTime('2017-11-05 08:07:47'), -1);
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), -1);
SELECT addMonths(toDateTime('2017-11-05 08:07:47'), materialize(-1));
SELECT addMonths(materialize(toDateTime('2017-11-05 08:07:47')), materialize(-1));

SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Istanbul');
SELECT toUnixTimestamp(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 'Asia/Istanbul');

SELECT toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul');
SELECT toTimeZone(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 'Asia/Kolkata');
SELECT toString(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'));
SELECT toString(toTimeZone(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 'Asia/Kolkata'));
SELECT toString(toDateTime('2017-11-05 08:07:47', 'Asia/Istanbul'), 'Asia/Kolkata');

SELECT '-- Test const timezone arguments --';

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (val Int64, tz String) engine=Log;
INSERT INTO tab VALUES (42, 'Asia/Singapore') (43, 'Asia/Tokyo');

SELECT val FROM tab WHERE now(tz) != toDateTime('2000-01-01 00:00:00') ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 0; -- { serverError ILLEGAL_COLUMN }
SELECT val FROM tab WHERE now(tz) != toDateTime('2000-01-01 00:00:00') ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 1;

SELECT val FROM tab WHERE now64(9, tz) != toDateTime64('2000-01-01 00:00:00', 6) ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 0; -- { serverError ILLEGAL_COLUMN }
SELECT val FROM tab WHERE now64(9, tz) != toDateTime64('2000-01-01 00:00:00', 6) ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 1;

SELECT val FROM tab WHERE nowInBlock(tz) != toDateTime('2000-01-01 00:00:00') ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 0; -- { serverError ILLEGAL_COLUMN }
SELECT val FROM tab WHERE nowInBlock(tz) != toDateTime('2000-01-01 00:00:00') ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 1;

SELECT val FROM tab WHERE toTimeZone(toDateTime(val), tz) != toDateTime('2023-06-11 14:14:14') ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 0; -- { serverError ILLEGAL_COLUMN }
SELECT val FROM tab WHERE toTimeZone(toDateTime(val), tz) != toDateTime('2023-06-11 14:14:14') ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 1;

SELECT val FROM tab WHERE fromUnixTimestamp64Milli(val, tz) != toDateTime64('2023-06-11 14:14:14', 6) ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 0; -- { serverError ILLEGAL_COLUMN }
SELECT val FROM tab WHERE fromUnixTimestamp64Milli(val, tz) != toDateTime64('2023-06-11 14:14:14', 6) ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 1;

SELECT val FROM tab WHERE fromUnixTimestamp64Micro(val, tz) != toDateTime64('2023-06-11 14:14:14', 6) ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 0; -- { serverError ILLEGAL_COLUMN }
SELECT val FROM tab WHERE fromUnixTimestamp64Micro(val, tz) != toDateTime64('2023-06-11 14:14:14', 6) ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 1;

SELECT val FROM tab WHERE fromUnixTimestamp64Nano(val, tz) != toDateTime64('2023-06-11 14:14:14', 6) ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 0; -- { serverError ILLEGAL_COLUMN }
SELECT val FROM tab WHERE fromUnixTimestamp64Nano(val, tz) != toDateTime64('2023-06-11 14:14:14', 6) ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 1;

SELECT val FROM tab WHERE snowflakeToDateTime(val, tz) != toDateTime('2023-06-11 14:14:14') ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 0; -- { serverError ILLEGAL_COLUMN }
SELECT val FROM tab WHERE snowflakeToDateTime(val, tz) != toDateTime('2023-06-11 14:14:14') ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 1;

SELECT val FROM tab WHERE snowflakeToDateTime64(val, tz) != toDateTime64('2023-06-11 14:14:14', 6) ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 0; -- { serverError ILLEGAL_COLUMN }
SELECT val FROM tab WHERE snowflakeToDateTime64(val, tz) != toDateTime64('2023-06-11 14:14:14', 6) ORDER BY val SETTINGS allow_nonconst_timezone_arguments = 1;

-- test for a related bug:

DROP TABLE tab;

SET allow_nonconst_timezone_arguments = 1;

CREATE TABLE tab (`country` LowCardinality(FixedString(7)) DEFAULT 'unknown', `city` LowCardinality(String) DEFAULT 'unknown', `region` LowCardinality(String) DEFAULT 'unknown', `continent` LowCardinality(FixedString(7)) DEFAULT 'unknown', `is_eu_country` Bool, `date` DateTime CODEC(DoubleDelta, LZ4), `viewer_date` DateTime ALIAS toTimezone(date, timezone), `device_browser` LowCardinality(String) DEFAULT 'unknown', `metro_code` LowCardinality(String) DEFAULT 'unknown', `domain` String DEFAULT 'unknown', `device_platform` LowCardinality(String) DEFAULT 'unknown', `device_type` LowCardinality(String) DEFAULT 'unknown', `device_vendor` LowCardinality(String) DEFAULT 'unknown', `ip` FixedString(39) DEFAULT 'unknown', `lat` Decimal(8, 6) CODEC(T64), `lng` Decimal(9, 6) CODEC(T64), `asset_id` String DEFAULT 'unknown', `is_personalized` Bool, `metric` String, `origin` String DEFAULT 'unknown', `product_id` UInt64 CODEC(T64), `referer` String DEFAULT 'unknown', `server_side` Int8 CODEC(T64), `third_party_id` String DEFAULT 'unknown', `partner_slug` LowCardinality(FixedString(10)) DEFAULT 'unknown', `user_agent` String DEFAULT 'unknown', `user_id` UUID, `zip` FixedString(10) DEFAULT 'unknown', `timezone` LowCardinality(String), `as_organization` LowCardinality(String) DEFAULT 'unknown', `content_cat` Array(String), `playback_method` LowCardinality(String) DEFAULT 'unknown', `store_id` LowCardinality(String) DEFAULT 'unknown', `store_url` String DEFAULT 'unknown', `timestamp` Nullable(DateTime), `ad_count` Int8 CODEC(T64), `ad_type` LowCardinality(FixedString(10)) DEFAULT 'unknown', `ad_categories` Array(FixedString(8)), `blocked_ad_categories` Array(FixedString(8)), `break_max_ad_length` Int8 CODEC(T64), `break_max_ads` Int8 CODEC(T64), `break_max_duration` Int8 CODEC(T64), `break_min_ad_length` Int8 CODEC(T64), `break_position` LowCardinality(FixedString(18)) DEFAULT 'unknown', `media_playhead` String DEFAULT 'unknown', `placement_type` Int8 CODEC(T64), `transaction_id` String, `universal_ad_id` Array(String), `client_ua` LowCardinality(String) DEFAULT 'unknown', `device_ip` FixedString(39) DEFAULT 'unknown', `device_ua` LowCardinality(String) DEFAULT 'unknown', `ifa` String, `ifa_type` LowCardinality(String) DEFAULT 'unknown', `vast_lat` Decimal(8, 6) CODEC(T64), `vast_long` Decimal(9, 6) CODEC(T64), `server_ua` String DEFAULT 'unknown', `app_bundle` String DEFAULT 'unknown', `page_url` String DEFAULT 'unknown', `api_framework` Array(UInt8), `click_type` LowCardinality(String), `extensions` Array(String), `media_mime` Array(String), `om_id_partner` LowCardinality(String) DEFAULT 'unknown', `player_capabilities` Array(FixedString(12)), `vast_versions` Array(UInt8), `verification_vendors` Array(String), `ad_play_head` String DEFAULT 'unknown', `ad_serving_id` String DEFAULT 'unknown', `asset_uri` String DEFAULT 'unknown', `content_id` String DEFAULT 'unknown', `content_uri` String DEFAULT 'unknown', `inventory_state` Array(FixedString(14)), `player_size` Array(UInt8), `player_state` Array(FixedString(12)), `pod_sequence` Int8 CODEC(T64), `click_position` Array(UInt32), `error_code` Int16 CODEC(T64), `error_reason` Int8 CODEC(T64), `gdpr_consent` String DEFAULT 'unknown', `limited_tracking` Bool, `regulations` String DEFAULT 'unknown', `content_category` Array(String), PROJECTION projection_TPAG_VAST_date (SELECT * ORDER BY toYYYYMMDD(date), metric, product_id, asset_id)) ENGINE = MergeTree ORDER BY (product_id, metric, asset_id, toYYYYMMDD(date));
DETACH TABLE tab;

SET allow_nonconst_timezone_arguments = 0;

-- ATTACH TABLE doesn't check the default expressions
ATTACH TABLE tab;
DROP TABLE tab;

-- CREATE TABLE does check the default expressions, so the following is expected to fail:
CREATE TABLE tab (`country` LowCardinality(FixedString(7)) DEFAULT 'unknown', `city` LowCardinality(String) DEFAULT 'unknown', `region` LowCardinality(String) DEFAULT 'unknown', `continent` LowCardinality(FixedString(7)) DEFAULT 'unknown', `is_eu_country` Bool, `date` DateTime CODEC(DoubleDelta, LZ4), `viewer_date` DateTime ALIAS toTimezone(date, timezone), `device_browser` LowCardinality(String) DEFAULT 'unknown', `metro_code` LowCardinality(String) DEFAULT 'unknown', `domain` String DEFAULT 'unknown', `device_platform` LowCardinality(String) DEFAULT 'unknown', `device_type` LowCardinality(String) DEFAULT 'unknown', `device_vendor` LowCardinality(String) DEFAULT 'unknown', `ip` FixedString(39) DEFAULT 'unknown', `lat` Decimal(8, 6) CODEC(T64), `lng` Decimal(9, 6) CODEC(T64), `asset_id` String DEFAULT 'unknown', `is_personalized` Bool, `metric` String, `origin` String DEFAULT 'unknown', `product_id` UInt64 CODEC(T64), `referer` String DEFAULT 'unknown', `server_side` Int8 CODEC(T64), `third_party_id` String DEFAULT 'unknown', `partner_slug` LowCardinality(FixedString(10)) DEFAULT 'unknown', `user_agent` String DEFAULT 'unknown', `user_id` UUID, `zip` FixedString(10) DEFAULT 'unknown', `timezone` LowCardinality(String), `as_organization` LowCardinality(String) DEFAULT 'unknown', `content_cat` Array(String), `playback_method` LowCardinality(String) DEFAULT 'unknown', `store_id` LowCardinality(String) DEFAULT 'unknown', `store_url` String DEFAULT 'unknown', `timestamp` Nullable(DateTime), `ad_count` Int8 CODEC(T64), `ad_type` LowCardinality(FixedString(10)) DEFAULT 'unknown', `ad_categories` Array(FixedString(8)), `blocked_ad_categories` Array(FixedString(8)), `break_max_ad_length` Int8 CODEC(T64), `break_max_ads` Int8 CODEC(T64), `break_max_duration` Int8 CODEC(T64), `break_min_ad_length` Int8 CODEC(T64), `break_position` LowCardinality(FixedString(18)) DEFAULT 'unknown', `media_playhead` String DEFAULT 'unknown', `placement_type` Int8 CODEC(T64), `transaction_id` String, `universal_ad_id` Array(String), `client_ua` LowCardinality(String) DEFAULT 'unknown', `device_ip` FixedString(39) DEFAULT 'unknown', `device_ua` LowCardinality(String) DEFAULT 'unknown', `ifa` String, `ifa_type` LowCardinality(String) DEFAULT 'unknown', `vast_lat` Decimal(8, 6) CODEC(T64), `vast_long` Decimal(9, 6) CODEC(T64), `server_ua` String DEFAULT 'unknown', `app_bundle` String DEFAULT 'unknown', `page_url` String DEFAULT 'unknown', `api_framework` Array(UInt8), `click_type` LowCardinality(String), `extensions` Array(String), `media_mime` Array(String), `om_id_partner` LowCardinality(String) DEFAULT 'unknown', `player_capabilities` Array(FixedString(12)), `vast_versions` Array(UInt8), `verification_vendors` Array(String), `ad_play_head` String DEFAULT 'unknown', `ad_serving_id` String DEFAULT 'unknown', `asset_uri` String DEFAULT 'unknown', `content_id` String DEFAULT 'unknown', `content_uri` String DEFAULT 'unknown', `inventory_state` Array(FixedString(14)), `player_size` Array(UInt8), `player_state` Array(FixedString(12)), `pod_sequence` Int8 CODEC(T64), `click_position` Array(UInt32), `error_code` Int16 CODEC(T64), `error_reason` Int8 CODEC(T64), `gdpr_consent` String DEFAULT 'unknown', `limited_tracking` Bool, `regulations` String DEFAULT 'unknown', `content_category` Array(String), PROJECTION projection_TPAG_VAST_date (SELECT * ORDER BY toYYYYMMDD(date), metric, product_id, asset_id)) ENGINE = MergeTree ORDER BY (product_id, metric, asset_id, toYYYYMMDD(date)); -- { serverError ILLEGAL_COLUMN }
