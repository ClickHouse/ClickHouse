-- Matcher expansion in stored expressions (DEFAULT/MATERIALIZED/ALIAS and skip indexes)
-- must not depend on the caller's `asterisk_include_alias_columns` /
-- `asterisk_include_materialized_columns` settings: otherwise the same table could store
-- different physical values for the same row depending only on the inserter's session,
-- and background rematerialization (running under server-default settings) could produce
-- yet another variant.
DROP TABLE IF EXISTS matcher_expansion_settings_independent;

CREATE TABLE matcher_expansion_settings_independent
(
    a UInt8,
    x UInt8 ALIAS a + 100,
    m UInt8 MATERIALIZED greatest(* EXCEPT m)
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO matcher_expansion_settings_independent (a) SETTINGS asterisk_include_alias_columns = 0 VALUES (1);
INSERT INTO matcher_expansion_settings_independent (a) SETTINGS asterisk_include_alias_columns = 1 VALUES (1);

-- Both rows must store the same value of `m`, regardless of the INSERT-time setting.
SELECT a, m FROM matcher_expansion_settings_independent ORDER BY a, m;

-- Background rematerialization must use the same frozen expansion as the inserts.
SET asterisk_include_alias_columns = 1;
SET asterisk_include_materialized_columns = 1;
ALTER TABLE matcher_expansion_settings_independent MATERIALIZE COLUMN m SETTINGS mutations_sync = 1;

SELECT a, m FROM matcher_expansion_settings_independent ORDER BY a, m;

DROP TABLE matcher_expansion_settings_independent;
