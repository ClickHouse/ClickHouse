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

-- TTL materialization during a merge expands `b`'s matcher under the background
-- merge context (server defaults), where alias columns are excluded from `*`, so no
-- cycle is formed and `OPTIMIZE` must succeed.
OPTIMIZE TABLE ttl_default_matcher_late_cycle FINAL;
OPTIMIZE TABLE ttl_default_matcher_late_cycle FINAL;

SET asterisk_include_alias_columns = 1;

-- With `asterisk_include_alias_columns = 1` the matcher in `b`'s default expands to
-- include the alias `x` (which aliases `b`), forming a `b` -> `x` -> `b` cycle. Reading the
-- alias column `x` forces this expansion at query time under both the old analyzer
-- (with `optimize_respect_aliases`) and the new analyzer, so the late cycle is detected.
SELECT x FROM ttl_default_matcher_late_cycle SETTINGS optimize_respect_aliases = 1; -- { serverError CYCLIC_ALIASES }

DROP TABLE ttl_default_matcher_late_cycle;
