DROP TABLE IF EXISTS ttl_default_matcher_late_cycle;

SET asterisk_include_alias_columns = 0;

CREATE TABLE ttl_default_matcher_late_cycle
(
    ts DateTime,
    a UInt8,
    b String DEFAULT toJSONString(tuple(* EXCEPT b)) TTL ts,
    x String ALIAS b
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_default_matcher_late_cycle VALUES (toDateTime('2000-01-01 00:00:00'), 1, 'old');

SET asterisk_include_alias_columns = 1;

OPTIMIZE TABLE ttl_default_matcher_late_cycle FINAL;
OPTIMIZE TABLE ttl_default_matcher_late_cycle FINAL;

SELECT b FROM ttl_default_matcher_late_cycle; -- { serverError CYCLIC_ALIASES }

DROP TABLE ttl_default_matcher_late_cycle;
