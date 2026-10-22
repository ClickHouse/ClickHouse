-- Matcher expansion in stored expressions is settings-independent: TTL materialization
-- during a merge and later alias reads expand `*` identically no matter which
-- `asterisk_include_*` settings the session (or the background merge context) has.
DROP TABLE IF EXISTS ttl_default_matcher;

SET asterisk_include_alias_columns = 0;

CREATE TABLE ttl_default_matcher
(
    ts DateTime('UTC'),
    a UInt8,
    b String DEFAULT toJSONString(tuple(* EXCEPT b)) TTL ts,
    x String ALIAS b
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_default_matcher VALUES (toDateTime('2000-01-01 00:00:00', 'UTC'), 1, 'old');

-- TTL materialization during the merge recomputes the expired `b` from its DEFAULT;
-- `*` never includes the alias `x`, so no cycle can form.
OPTIMIZE TABLE ttl_default_matcher FINAL;
OPTIMIZE TABLE ttl_default_matcher FINAL;

SELECT x FROM ttl_default_matcher SETTINGS optimize_respect_aliases = 1;

SET asterisk_include_alias_columns = 1;

-- The alias read is unaffected by the session setting.
SELECT x FROM ttl_default_matcher SETTINGS optimize_respect_aliases = 1;

DROP TABLE ttl_default_matcher;
