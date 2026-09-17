-- Tags: memory-engine

SET allow_experimental_user_defined_types = 0;

SELECT
    'before',
    (SELECT count() FROM system.databases WHERE name = 'udt_04658_missing_db'),
    (SELECT count() FROM system.tables WHERE name IN ('udt_04658_table_guard', 'udt_04658_string_table_guard', 'udt_04658_ttl_guard')),
    (SELECT count() FROM system.functions WHERE name IN (
        'udt_04658_function_guard',
        'udt_04658_dynamic_function_guard',
        'udt_04658_dynamic_cluster_function_guard')),
    (SELECT count() FROM system.row_policies WHERE short_name = 'udt_04658_policy_guard'),
    (SELECT count() FROM system.data_type_families WHERE name IN ('UDT04658Basic', 'UDT04658Recursive', 'UDT04658Attached', 'UDT04658Missing')),
    (SELECT count() FROM system.backups WHERE position(name, 'udt_04658_backup_guard') != 0)
FORMAT TSVRaw;

SELECT 'statements' FORMAT TSVRaw;

CREATE TYPE UDT04658Basic AS UInt64; -- { serverError SUPPORT_IS_DISABLED }

CREATE TYPE IF NOT EXISTS udt_04658_missing_db.UDT04658Recursive(T TYPE, N UInt16)
    ON CLUSTER udt_04658_missing_cluster
    DECREASES N
    AS TYPE_IF(N = 0, T, udt_04658_missing_db.UDT04658Recursive(T, N - 1))
    COMMENT 'must not persist'; -- { serverError SUPPORT_IS_DISABLED }

ATTACH TYPE IF NOT EXISTS udt_04658_missing_db.UDT04658Attached
    UUID '11111111-1111-1111-1111-111111111111'
    REVISION 1
    ON CLUSTER udt_04658_missing_cluster
    AS UInt64
    DEFINITION HASH '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
    COMMENT 'must not attach'; -- { serverError SUPPORT_IS_DISABLED }

DROP TYPE UDT04658Basic RESTRICT; -- { serverError SUPPORT_IS_DISABLED }
DROP TYPE IF EXISTS udt_04658_missing_db.UDT04658Missing
    ON CLUSTER udt_04658_missing_cluster RESTRICT; -- { serverError SUPPORT_IS_DISABLED }

ALTER TYPE UDT04658Basic RENAME TO UDT04658Renamed; -- { serverError SUPPORT_IS_DISABLED }
ALTER TYPE UDT04658Basic COMMENT 'must not persist'; -- { serverError SUPPORT_IS_DISABLED }
ALTER TYPE IF EXISTS udt_04658_missing_db.UDT04658Missing
    ON CLUSTER udt_04658_missing_cluster RENAME TO UDT04658Renamed; -- { serverError SUPPORT_IS_DISABLED }

SHOW TYPES; -- { serverError SUPPORT_IS_DISABLED }
SHOW TYPES FROM udt_04658_missing_db LIKE 'UDT04658%'; -- { serverError SUPPORT_IS_DISABLED }

SHOW CREATE TYPE udt_04658_missing_db.UDT04658Missing; -- { serverError SUPPORT_IS_DISABLED }
DESCRIBE TYPE udt_04658_missing_db.UDT04658Missing; -- { serverError SUPPORT_IS_DISABLED }

PHYSICALIZE TYPE REFERENCES OBJECT TABLE udt_04658_missing_db.missing_table
    ON CLUSTER udt_04658_missing_cluster DROP UNUSED TYPES DRY RUN; -- { serverError SUPPORT_IS_DISABLED }
PHYSICALIZE TYPE REFERENCES CLOSURE OF VIEW udt_04658_missing_db.missing_view
    DROP UNUSED TYPES DRY RUN; -- { serverError SUPPORT_IS_DISABLED }
PHYSICALIZE TYPE REFERENCES DATABASE udt_04658_missing_db
    ON CLUSTER udt_04658_missing_cluster DRY RUN; -- { serverError SUPPORT_IS_DISABLED }
PHYSICALIZE TYPE REFERENCES APPLY TOKEN ''; -- { serverError SUPPORT_IS_DISABLED }
PHYSICALIZE TYPE REFERENCES APPLY TOKEN 'udt-04658-plan-token'; -- { serverError SUPPORT_IS_DISABLED }

-- Repeating the same output path asserts that the first rejected statement did
-- not create even an empty client-side output file.
SHOW CREATE TYPE udt_04658_missing_db.UDT04658Missing
    INTO OUTFILE 'udt_04658_must_not_exist.tsv'; -- { serverError SUPPORT_IS_DISABLED }
SHOW CREATE TYPE udt_04658_missing_db.UDT04658Missing
    INTO OUTFILE 'udt_04658_must_not_exist.tsv'; -- { serverError SUPPORT_IS_DISABLED }

-- definition-only authority enables the database-local definitions lifecycle when the gate is
-- on. Keep one output-free round trip here so the disabled assertions cannot
-- hide integration failures in the Atomic durability path.
SET allow_experimental_user_defined_types = 1;
CREATE TYPE UDT04658EnabledSmoke AS UInt64;
DROP TYPE UDT04658EnabledSmoke RESTRICT;
SET allow_experimental_user_defined_types = 0;

SELECT 'casts' FORMAT TSVRaw;

SET enable_analyzer = 0;
SELECT CAST(1 AS udt_04658_missing_db.UDT04658Missing); -- { serverError SUPPORT_IS_DISABLED }
SELECT CAST(1, materialize('udt_04658_missing_db.UDT04658Missing')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET enable_analyzer = 1;
SELECT CAST(1 AS udt_04658_missing_db.UDT04658Missing); -- { serverError SUPPORT_IS_DISABLED }
SELECT CAST(1, materialize('udt_04658_missing_db.UDT04658Missing')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT CAST(1 AS Array(udt_04658_missing_db.UDT04658Missing)); -- { serverError SUPPORT_IS_DISABLED }
SELECT CAST(1, 'udt_04658_missing_db.UDT04658Missing'); -- { serverError SUPPORT_IS_DISABLED }
SELECT CAST(1, 'Array(udt_04658_missing_db.UDT04658Missing)'); -- { serverError SUPPORT_IS_DISABLED }
SELECT 1::udt_04658_missing_db.UDT04658Missing; -- { serverError SUPPORT_IS_DISABLED }
SELECT accurateCast(1, 'udt_04658_missing_db.UDT04658Missing'); -- { serverError SYNTAX_ERROR }
SELECT accurateCastOrNull(1, 'udt_04658_missing_db.UDT04658Missing'); -- { serverError SYNTAX_ERROR }
-- This separate compatibility function is not a parser and feature gate CAST boundary. Its type lookup
-- remains physical-factory-only, while persisted uses are still rejected by the
-- pre-side-effect guard covered by the unit gate.
SELECT accurateCastOrDefault(1, 'udt_04658_missing_db.UDT04658Missing', 0); -- { serverError SYNTAX_ERROR }

SELECT 'side-effect-guards' FORMAT TSVRaw;

CREATE TABLE udt_04658_table_guard
(
    x UInt64 DEFAULT CAST(1 AS udt_04658_missing_db.UDT04658Missing)
)
ENGINE = Memory; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE udt_04658_string_table_guard
(
    x UInt64 DEFAULT CAST(1, 'udt_04658_missing_db.UDT04658Missing')
)
ENGINE = Memory; -- { serverError SUPPORT_IS_DISABLED }

CREATE TABLE udt_04658_ttl_guard
(
    d DateTime,
    x UInt64
)
ENGINE = MergeTree
ORDER BY x
TTL d GROUP BY CAST(x AS udt_04658_missing_db.UDT04658Missing) SET x = x; -- { serverError SUPPORT_IS_DISABLED }

CREATE FUNCTION udt_04658_function_guard
    ON CLUSTER udt_04658_missing_cluster
    AS x -> CAST(x, 'udt_04658_missing_db.UDT04658Missing'); -- { serverError SUPPORT_IS_DISABLED }

-- Nonconstant CAST targets are deliberately deferred while the feature is
-- disabled. With UDT analysis enabled, persisted expressions reject them at
-- the execution boundary before either local or ON CLUSTER side effects.
SET allow_experimental_user_defined_types = 1;

CREATE FUNCTION udt_04658_dynamic_function_guard
    AS x -> CAST(x, materialize('udt_04658_missing_db.UDT04658Missing')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

CREATE FUNCTION udt_04658_dynamic_cluster_function_guard
    ON CLUSTER udt_04658_missing_cluster
    AS x -> accurateCastOrDefault(x, materialize('udt_04658_missing_db.UDT04658Missing'), 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SET allow_experimental_user_defined_types = 0;

CREATE ROW POLICY udt_04658_policy_guard
    ON udt_04658_missing_db.missing_table
    USING CAST(1 AS udt_04658_missing_db.UDT04658Missing)
    TO ALL; -- { serverError SUPPORT_IS_DISABLED }

BACKUP TABLE system.one
    PARTITION CAST(1 AS udt_04658_missing_db.UDT04658Missing)
    TO File('udt_04658_backup_guard'); -- { serverError SUPPORT_IS_DISABLED }

BACKUP TABLE system.one
    TO File('udt_04658_backup_guard')
    SETTINGS cluster_host_ids = [CAST(1 AS udt_04658_missing_db.UDT04658Missing)]; -- { serverError SUPPORT_IS_DISABLED }

SELECT
    'after',
    (SELECT count() FROM system.databases WHERE name = 'udt_04658_missing_db'),
    (SELECT count() FROM system.tables WHERE name IN ('udt_04658_table_guard', 'udt_04658_string_table_guard', 'udt_04658_ttl_guard')),
    (SELECT count() FROM system.functions WHERE name IN (
        'udt_04658_function_guard',
        'udt_04658_dynamic_function_guard',
        'udt_04658_dynamic_cluster_function_guard')),
    (SELECT count() FROM system.row_policies WHERE short_name = 'udt_04658_policy_guard'),
    (SELECT count() FROM system.data_type_families WHERE name IN ('UDT04658Basic', 'UDT04658Recursive', 'UDT04658Attached', 'UDT04658Missing')),
    (SELECT count() FROM system.backups WHERE position(name, 'udt_04658_backup_guard') != 0)
FORMAT TSVRaw;
