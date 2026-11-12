-- Controls come first: on an unfixed binary the first witness throws and the runner stops the
-- file, so this ordering keeps the diff self-describing.

-- A column that is not an ALIAS accepts a codec.
CREATE TABLE t_plain (event String, type_uid UInt32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_plain MODIFY COLUMN type_uid UInt32 CODEC(T64, LZ4);
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_plain' AND name = 'type_uid';

-- DEFAULT and MATERIALIZED sources already accepted the very same ALTER.
CREATE TABLE t_default (event String, type_uid UInt32 DEFAULT JSONExtractUInt(event, 'type_uid'))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_default MODIFY COLUMN type_uid UInt32 MATERIALIZED JSONExtractUInt(event, 'type_uid') CODEC(T64, LZ4);
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_default' AND name = 'type_uid';

CREATE TABLE t_mat (event String, type_uid UInt32 MATERIALIZED JSONExtractUInt(event, 'type_uid'))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mat MODIFY COLUMN type_uid UInt32 MATERIALIZED JSONExtractUInt(event, 'type_uid') CODEC(T64, LZ4);
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_mat' AND name = 'type_uid';

-- Turning a physical column into an ALIAS is fine as long as no codec is involved.
CREATE TABLE t_mat_to_alias (event String, type_uid UInt32 MATERIALIZED JSONExtractUInt(event, 'type_uid'))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mat_to_alias MODIFY COLUMN type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid');
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_mat_to_alias' AND name = 'type_uid';

-- The reported case: ALIAS -> MATERIALIZED together with a codec.
CREATE TABLE t_alias_to_mat (event String, type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid'))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_alias_to_mat MODIFY COLUMN type_uid UInt32 MATERIALIZED JSONExtractUInt(event, 'type_uid') CODEC(T64, LZ4);
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_alias_to_mat' AND name = 'type_uid';

-- The same defect with DEFAULT as the target kind, which the issue does not mention.
CREATE TABLE t_alias_to_default (event String, type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid'))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_alias_to_default MODIFY COLUMN type_uid UInt32 DEFAULT JSONExtractUInt(event, 'type_uid') CODEC(T64, LZ4);
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_alias_to_default' AND name = 'type_uid';

-- The type may be omitted; the column still becomes physical.
CREATE TABLE t_alias_no_type (event String, type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid'))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_alias_no_type MODIFY COLUMN type_uid MATERIALIZED JSONExtractUInt(event, 'type_uid') CODEC(T64, LZ4);
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_alias_no_type' AND name = 'type_uid';

-- Same fix on a replicated table.
CREATE TABLE t_alias_to_mat_rep (event String, type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid'))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04819_alias_codec', 'r1') ORDER BY tuple();
ALTER TABLE t_alias_to_mat_rep MODIFY COLUMN type_uid UInt32 MATERIALIZED JSONExtractUInt(event, 'type_uid') CODEC(T64, LZ4);
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_alias_to_mat_rep' AND name = 'type_uid';

-- The column stays an ALIAS, so the codec is still refused.
CREATE TABLE t_alias_to_alias (event String, type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid'))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_alias_to_alias MODIFY COLUMN type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid') CODEC(T64, LZ4); -- { serverError BAD_ARGUMENTS }

-- Without a default specifier the ALIAS is preserved, so the codec is still refused.
CREATE TABLE t_alias_no_specifier (event String, type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid'))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_alias_no_specifier MODIFY COLUMN type_uid UInt32 CODEC(T64, LZ4); -- { serverError BAD_ARGUMENTS }
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_alias_no_specifier' AND name = 'type_uid';

-- AUTO_INCREMENT carries no expression and also preserves the ALIAS.
CREATE TABLE t_alias_auto_increment (event String, type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid'))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_alias_auto_increment MODIFY COLUMN type_uid UInt32 AUTO_INCREMENT CODEC(T64, LZ4); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_alias_auto_increment MODIFY COLUMN type_uid UInt32 AUTO_INCREMENT;
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_alias_auto_increment' AND name = 'type_uid';

-- EPHEMERAL is not physical, so a codec is still refused for an ALIAS column.
CREATE TABLE t_alias_to_ephemeral (event String, type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid'))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_alias_to_ephemeral MODIFY COLUMN type_uid UInt32 EPHEMERAL 1 CODEC(T64, LZ4); -- { serverError BAD_ARGUMENTS }

-- A codec on an EPHEMERAL column of a non-ALIAS source keeps being accepted, in both directions.
CREATE TABLE t_plain_to_ephemeral (event String, type_uid UInt32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_plain_to_ephemeral MODIFY COLUMN type_uid UInt32 EPHEMERAL 1 CODEC(T64, LZ4);
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_plain_to_ephemeral' AND name = 'type_uid';

CREATE TABLE t_mat_codec_to_ephemeral
(event String, type_uid UInt32 MATERIALIZED JSONExtractUInt(event, 'type_uid') CODEC(T64, LZ4))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mat_codec_to_ephemeral MODIFY COLUMN type_uid UInt32 EPHEMERAL 1;
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_mat_codec_to_ephemeral' AND name = 'type_uid';

-- The reverse direction, physical -> ALIAS with a codec, stays rejected.
CREATE TABLE t_plain_to_alias (event String, type_uid UInt32) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_plain_to_alias MODIFY COLUMN type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid') CODEC(T64, LZ4); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_mat_codec_to_alias
(event String, type_uid UInt32 MATERIALIZED JSONExtractUInt(event, 'type_uid') CODEC(T64, LZ4))
ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE t_mat_codec_to_alias MODIFY COLUMN type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid'); -- { serverError BAD_ARGUMENTS }

-- Data is readable through a column that the ALTER made physical.
INSERT INTO t_alias_to_mat VALUES ('{"type_uid":7}');
SELECT event, type_uid FROM t_alias_to_mat;

-- Below, only AlterCommands::validate can reject: max_query_size = 0 skips the CREATE-query
-- revalidation, and a replicated table reaches the other one only in a Replicated database.
-- The setting is session-scoped, so this section must stay last.
SET max_query_size = 0;

CREATE TABLE t_alias_rep_unvalidated (event String, type_uid UInt32 ALIAS JSONExtractUInt(event, 'type_uid'))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04819_alias_codec_no_revalidation', 'r1') ORDER BY tuple();
ALTER TABLE t_alias_rep_unvalidated MODIFY COLUMN type_uid UInt32 CODEC(T64, LZ4); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_alias_rep_unvalidated MODIFY COLUMN type_uid UInt32 AUTO_INCREMENT CODEC(T64, LZ4); -- { serverError BAD_ARGUMENTS }
SELECT default_kind, default_expression, compression_codec FROM system.columns
WHERE database = currentDatabase() AND table = 't_alias_rep_unvalidated' AND name = 'type_uid';

DROP TABLE t_plain;
DROP TABLE t_default;
DROP TABLE t_mat;
DROP TABLE t_mat_to_alias;
DROP TABLE t_alias_to_mat;
DROP TABLE t_alias_to_default;
DROP TABLE t_alias_no_type;
DROP TABLE t_alias_to_mat_rep;
DROP TABLE t_alias_to_alias;
DROP TABLE t_alias_no_specifier;
DROP TABLE t_alias_auto_increment;
DROP TABLE t_alias_to_ephemeral;
DROP TABLE t_plain_to_ephemeral;
DROP TABLE t_mat_codec_to_ephemeral;
DROP TABLE t_plain_to_alias;
DROP TABLE t_mat_codec_to_alias;
DROP TABLE t_alias_rep_unvalidated;
