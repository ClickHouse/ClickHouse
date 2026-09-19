-- Tags: no-replicated-database

DROP TABLE IF EXISTS tuple_element_codec_gate;
DROP TABLE IF EXISTS tuple_element_codec_root_only;
DROP TABLE IF EXISTS tuple_element_codec_full_attach;
DROP TABLE IF EXISTS tuple_element_codec_as_source;
DROP TABLE IF EXISTS tuple_element_codec_clone_source;
DROP TABLE IF EXISTS tuple_element_codec_existing_destination;
DROP TABLE IF EXISTS tuple_element_codec_inherited_gate;
DROP TABLE IF EXISTS tuple_element_codec_nested_inherited_gate;
DROP TABLE IF EXISTS tuple_element_codec_symbolic_restatement;
DROP TEMPORARY TABLE IF EXISTS tuple_element_codec_existing_temporary;

-- Root-only codecs are outside the experimental gate.
CREATE TABLE tuple_element_codec_root_only
(
    value Tuple(number UInt64, text String) CODEC(ZSTD)
)
ENGINE = MergeTree ORDER BY tuple();
DROP TABLE tuple_element_codec_root_only;

CREATE TABLE tuple_element_codec_gate
(
    id UInt64,
    value Tuple(number UInt64 CODEC(Delta, ZSTD), text String)
)
ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

SET enable_tuple_element_codecs = 1;
SET allow_suspicious_codecs = 1;

CREATE TABLE tuple_element_codec_gate
(
    id UInt64,
    value Tuple(number UInt64 CODEC(Delta), retained UInt64 CODEC(Delta), text String)
)
ENGINE = MergeTree ORDER BY id;

SET enable_tuple_element_codecs = 0;
SET allow_suspicious_codecs = 0;

-- Loading persisted metadata is compatibility-safe and must not require the session gate.
INSERT INTO tuple_element_codec_gate VALUES (1, (1, 2, 'one'));
DETACH TABLE tuple_element_codec_gate;
ATTACH TABLE tuple_element_codec_gate;
SELECT value.number FROM tuple_element_codec_gate FORMAT Null;

-- A full ATTACH definition is fresh user-supplied metadata, unlike the short ATTACH above.
ATTACH TABLE tuple_element_codec_full_attach UUID '50280000-0000-0000-0000-000000000001'
(
    value Tuple(number UInt64 CODEC(ZSTD), text String)
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- CREATE AS and CLONE AS copy the complete codec policy from their source. They must
-- not bypass the gate merely because their columns were not written in this query.
CREATE TABLE tuple_element_codec_as_source AS tuple_element_codec_gate; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tuple_element_codec_clone_source CLONE AS tuple_element_codec_gate; -- { serverError BAD_ARGUMENTS }

-- An unused copied definition does not require admission when IF NOT EXISTS makes the query a no-op.
CREATE TABLE tuple_element_codec_existing_destination (n UInt64)
ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE IF NOT EXISTS tuple_element_codec_existing_destination AS tuple_element_codec_gate;
CREATE TABLE IF NOT EXISTS tuple_element_codec_existing_destination CLONE AS tuple_element_codec_gate;
ATTACH TABLE IF NOT EXISTS tuple_element_codec_existing_destination UUID '50280000-0000-0000-0000-000000000002'
(
    value Tuple(number UInt64 CODEC(ZSTD), text String)
)
ENGINE = MergeTree ORDER BY tuple();
SELECT n FROM tuple_element_codec_existing_destination FORMAT Null;

CREATE TEMPORARY TABLE tuple_element_codec_existing_temporary (n UInt64);
CREATE TEMPORARY TABLE IF NOT EXISTS tuple_element_codec_existing_temporary AS tuple_element_codec_gate;
SELECT n FROM tuple_element_codec_existing_temporary FORMAT Null;
DROP TEMPORARY TABLE tuple_element_codec_existing_temporary;
DROP TABLE tuple_element_codec_existing_destination;

-- Restating Delta, which is stored as Delta(8) for UInt64, is semantically unchanged
-- and remains allowed without either admission gate.
ALTER TABLE tuple_element_codec_gate
    MODIFY COLUMN value Tuple(number UInt64 CODEC(Delta), retained UInt64 CODEC(Delta), text String);

-- Property-only changes retain already-persisted codecs without applying the current
-- session's admission policy to the tuple-element declarations.
ALTER TABLE tuple_element_codec_gate MODIFY COLUMN value COMMENT 'retained';

-- Admission checks for a changed tuple declaration do not spill over to a retained
-- suspicious declaration in the same column policy.
SET enable_tuple_element_codecs = 1;
ALTER TABLE tuple_element_codec_gate
    MODIFY COLUMN value Tuple(number UInt64 CODEC(ZSTD), retained UInt64, text String);

-- The current declaration is now ZSTD, so changing it to suspicious Delta is a
-- genuine declaration change and must use the disabled session admission policy.
ALTER TABLE tuple_element_codec_gate
    MODIFY COLUMN value Tuple(number UInt64 CODEC(Delta), retained UInt64, text String); -- { serverError BAD_ARGUMENTS }
SET enable_tuple_element_codecs = 0;

-- A real type change still checks the retained Delta declaration against the
-- resulting type, but does so as trusted metadata rather than re-admitting it.
ALTER TABLE tuple_element_codec_gate
    MODIFY COLUMN value Tuple(number UInt64, retained Int64, text String);

-- Root codec changes also remain independent of the tuple-element gate.
ALTER TABLE tuple_element_codec_gate MODIFY COLUMN value CODEC(ZSTD);

ALTER TABLE tuple_element_codec_gate
    MODIFY COLUMN value Tuple(number UInt64 CODEC(LZ4), retained Int64, text String); -- { serverError BAD_ARGUMENTS }

ALTER TABLE tuple_element_codec_gate ADD COLUMN
    added Tuple(number UInt64 CODEC(ZSTD), text String); -- { serverError BAD_ARGUMENTS }

SET enable_tuple_element_codecs = 1;
ALTER TABLE tuple_element_codec_gate ADD COLUMN
    added Tuple(number UInt64 CODEC(ZSTD), text String);
SET enable_tuple_element_codecs = 0;

-- Removal does not introduce metadata and remains allowed with the gate disabled.
ALTER TABLE tuple_element_codec_gate
    MODIFY COLUMN value Tuple(number UInt64 REMOVE CODEC, retained Int64, text String);

DROP TABLE tuple_element_codec_gate;

-- Removing an override makes the inherited root codec effective for future writes.
-- The inherited codec must pass its dedicated gate in the current session.
SET enable_tuple_element_codecs = 1;
SET enable_alp_codec = 1;
CREATE TABLE tuple_element_codec_inherited_gate
(
    value Tuple(a Float64 CODEC(LZ4), b Float64 CODEC(LZ4)) CODEC(ALP)
)
ENGINE = MergeTree ORDER BY tuple();

CREATE TABLE tuple_element_codec_nested_inherited_gate
(
    value Tuple(
        g Tuple(a Float64 CODEC(LZ4), b Float64) CODEC(ALP),
        z UInt64
    )
)
ENGINE = MergeTree ORDER BY tuple();

SET enable_alp_codec = 0;
ALTER TABLE tuple_element_codec_inherited_gate
    MODIFY COLUMN value Tuple(a Float64 REMOVE CODEC, b Float64 CODEC(LZ4)); -- { serverError BAD_ARGUMENTS }

SET enable_alp_codec = 1;
ALTER TABLE tuple_element_codec_inherited_gate
    MODIFY COLUMN value Tuple(a Float64 REMOVE CODEC, b Float64 CODEC(LZ4));
DROP TABLE tuple_element_codec_inherited_gate;

-- REMOVE CODEC remains allowed with the Tuple feature gate disabled. The existing
-- inherited declaration is checked only by its own codec gate.
SET enable_tuple_element_codecs = 0;
ALTER TABLE tuple_element_codec_nested_inherited_gate MODIFY COLUMN value Tuple(
    g Tuple(a Float64 REMOVE CODEC, b Float64),
    z UInt64
);
DROP TABLE tuple_element_codec_nested_inherited_gate;
SET enable_alp_codec = 0;

-- A shadowed type-dependent codec keeps its symbolic form. Removing an override
-- may normalize it, but restating the same declaration does not require the Tuple gate.
SET enable_tuple_element_codecs = 1;
SET allow_suspicious_codecs = 1;
CREATE TABLE tuple_element_codec_symbolic_restatement
(
    value Tuple(
        g Tuple(
            a UInt64 CODEC(LZ4),
            b UInt64 CODEC(LZ4)
        ) CODEC(Delta),
        z UInt64
    )
)
ENGINE = MergeTree ORDER BY tuple();

SET enable_tuple_element_codecs = 0;
SET allow_suspicious_codecs = 0;
ALTER TABLE tuple_element_codec_symbolic_restatement MODIFY COLUMN value Tuple(
    g Tuple(
        a UInt64 REMOVE CODEC,
        b UInt64 CODEC(LZ4)
    ) CODEC(Delta),
    z UInt64
); -- { serverError BAD_ARGUMENTS }

SET allow_suspicious_codecs = 1;
ALTER TABLE tuple_element_codec_symbolic_restatement MODIFY COLUMN value Tuple(
    g Tuple(
        a UInt64 REMOVE CODEC,
        b UInt64 CODEC(LZ4)
    ) CODEC(Delta),
    z UInt64
);
DROP TABLE tuple_element_codec_symbolic_restatement;
SET allow_suspicious_codecs = 0;
