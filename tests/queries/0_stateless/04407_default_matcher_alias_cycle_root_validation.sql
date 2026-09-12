-- Matcher expansion in stored expressions is settings-independent: reading an ALIAS
-- whose expression uses `*` — directly and through the `merge` table function under the
-- old analyzer with `optimize_respect_aliases` — returns the same value regardless of
-- `asterisk_include_materialized_columns`.
DROP TABLE IF EXISTS default_matcher_alias_root_read;

SET asterisk_include_materialized_columns = 0;

CREATE TABLE default_matcher_alias_root_read
(
    a UInt8,
    x String ALIAS toJSONString(tuple(* EXCEPT x)),
    m String MATERIALIZED x
)
ENGINE = Memory;

INSERT INTO default_matcher_alias_root_read (a) VALUES (1);

SELECT x FROM default_matcher_alias_root_read
SETTINGS allow_experimental_analyzer = 0, optimize_respect_aliases = 1;

SET asterisk_include_materialized_columns = 1;

-- `*` never includes the MATERIALIZED column `m`, so no `x` -> `m` -> `x` cycle can
-- form and the read does not depend on the session setting.
SELECT x FROM default_matcher_alias_root_read
SETTINGS allow_experimental_analyzer = 0, optimize_respect_aliases = 1;

SELECT x FROM merge(currentDatabase(), '^default_matcher_alias_root_read$')
SETTINGS allow_experimental_analyzer = 0, optimize_respect_aliases = 1;

DROP TABLE default_matcher_alias_root_read;
