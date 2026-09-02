DROP TABLE IF EXISTS tuple_element_codec_gate;
DROP TABLE IF EXISTS tuple_element_codec_root_only;

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
    value Tuple(number UInt64 CODEC(Delta), retained UInt64 CODEC(Delta), text String),
    root_codec UInt64 CODEC(Delta)
)
ENGINE = MergeTree ORDER BY id;

SET enable_tuple_element_codecs = 0;
SET allow_suspicious_codecs = 0;

-- Loading persisted metadata is compatibility-safe and must not require the session gate.
INSERT INTO tuple_element_codec_gate VALUES (1, (1, 2, 'one'), 1);
DETACH TABLE tuple_element_codec_gate;
ATTACH TABLE tuple_element_codec_gate;
SELECT value.number FROM tuple_element_codec_gate FORMAT Null;

-- Restating Delta, which is stored as Delta(8) for UInt64, is semantically unchanged
-- and remains allowed without either admission gate.
ALTER TABLE tuple_element_codec_gate
    MODIFY COLUMN value Tuple(number UInt64 CODEC(Delta), retained UInt64 CODEC(Delta), text String);

-- Property-only changes retain already-persisted codecs without applying the current
-- session's admission policy to either tuple-element or root declarations.
ALTER TABLE tuple_element_codec_gate MODIFY COLUMN value COMMENT 'retained';
ALTER TABLE tuple_element_codec_gate MODIFY COLUMN value DEFAULT tuple(0, 0, '');
ALTER TABLE tuple_element_codec_gate MODIFY COLUMN value TTL toDateTime(id) + INTERVAL 100 YEAR;
ALTER TABLE tuple_element_codec_gate MODIFY COLUMN value SETTINGS (max_compress_block_size = 65536);
ALTER TABLE tuple_element_codec_gate MODIFY COLUMN root_codec COMMENT 'retained';
ALTER TABLE tuple_element_codec_gate MODIFY COLUMN root_codec DEFAULT 0;
ALTER TABLE tuple_element_codec_gate MODIFY COLUMN root_codec SETTINGS (max_compress_block_size = 65536);

-- An explicitly supplied root codec still uses the current session policy.
ALTER TABLE tuple_element_codec_gate MODIFY COLUMN root_codec CODEC(Delta); -- { serverError BAD_ARGUMENTS }

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
