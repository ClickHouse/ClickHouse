-- Tags: no-replicated-database

DROP TABLE IF EXISTS tuple_element_codec_gate;
DROP TABLE IF EXISTS tuple_element_codec_root_only;
DROP TABLE IF EXISTS tuple_element_codec_full_attach;
DROP TABLE IF EXISTS tuple_element_codec_as_source;
DROP TABLE IF EXISTS tuple_element_codec_clone_source;
DROP TABLE IF EXISTS tuple_element_codec_existing_destination;
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
