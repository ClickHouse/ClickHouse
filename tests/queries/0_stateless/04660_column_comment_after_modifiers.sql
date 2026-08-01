-- The modifiers of a column declaration can be written in any order, and each of them at most once.
-- In particular, `COMMENT` is accepted after `CODEC`, `STATISTICS`, `TTL`, `COLLATE`, `PRIMARY KEY`
-- and per-column `SETTINGS`, not only before them.

SET allow_experimental_statistics = 1;

DROP TABLE IF EXISTS t_column_comment_order;

CREATE TABLE t_column_comment_order
(
    a UInt64 CODEC(ZSTD) COMMENT 'a comment',
    b UInt64 COMMENT 'b comment' CODEC(ZSTD),
    c UInt64 DEFAULT 1 CODEC(ZSTD) COMMENT 'c comment',
    d DateTime,
    e UInt64 CODEC(ZSTD) TTL d + INTERVAL 1 DAY COMMENT 'e comment',
    f UInt64 SETTINGS (max_compress_block_size = 1024) COMMENT 'f comment',
    g UInt64 STATISTICS(tdigest) COMMENT 'g comment',
    h String EPHEMERAL COMMENT 'h comment'
)
ENGINE = MergeTree ORDER BY a;

SHOW CREATE TABLE t_column_comment_order FORMAT TSVRaw;

SELECT name, comment FROM system.columns WHERE database = currentDatabase() AND table = 't_column_comment_order' ORDER BY name;

-- The same for ALTER.
ALTER TABLE t_column_comment_order MODIFY COLUMN a UInt64 CODEC(LZ4) COMMENT 'a new comment';
ALTER TABLE t_column_comment_order ADD COLUMN i UInt64 CODEC(ZSTD) COMMENT 'i comment';
ALTER TABLE t_column_comment_order ADD COLUMN j UInt64 TTL d + INTERVAL 2 DAY COMMENT 'j comment';
ALTER TABLE t_column_comment_order ADD COLUMN k UInt64 SETTINGS (max_compress_block_size = 1024) COMMENT 'k comment';

-- A trailing `SETTINGS` clause without parentheses still belongs to the query, not to the column.
ALTER TABLE t_column_comment_order MODIFY COLUMN b UInt64 COMMENT 'b new comment' SETTINGS mutations_sync = 2;
ALTER TABLE t_column_comment_order MODIFY COLUMN b UInt64 CODEC(ZSTD) COMMENT 'b newer comment' SETTINGS mutations_sync = 2;

-- Type-less `MODIFY COLUMN`: a leading `SETTINGS`, `STATISTICS` or `COLLATE` is a modifier, not a data type,
-- so the modifiers can be reordered here as well.
ALTER TABLE t_column_comment_order MODIFY COLUMN f SETTINGS (max_compress_block_size = 2048) COMMENT 'f new comment';
ALTER TABLE t_column_comment_order MODIFY COLUMN g STATISTICS(tdigest) COMMENT 'g new comment';
ALTER TABLE t_column_comment_order MODIFY COLUMN e CODEC(LZ4) COMMENT 'e new comment';
ALTER TABLE t_column_comment_order MODIFY COLUMN f SETTINGS (max_compress_block_size = 4096) COMMENT 'f newer comment' SETTINGS mutations_sync = 2;

SELECT name, comment FROM system.columns WHERE database = currentDatabase() AND table = 't_column_comment_order' ORDER BY name;

DROP TABLE t_column_comment_order;

-- A column-level `PRIMARY KEY` followed by a comment.
DROP TABLE IF EXISTS t_column_comment_order_pk;

CREATE TABLE t_column_comment_order_pk (a UInt64 PRIMARY KEY COMMENT 'a comment', b UInt64) ENGINE = MergeTree;

SHOW CREATE TABLE t_column_comment_order_pk FORMAT TSVRaw;

DROP TABLE t_column_comment_order_pk;

-- Formatting normalizes the order, and the result is parsed back.
SELECT formatQuery('CREATE TABLE t (a UInt64 CODEC(ZSTD) COMMENT \'a comment\') ENGINE = Memory');
SELECT formatQuery('CREATE TABLE t (a UInt64 STATISTICS(tdigest) COMMENT \'a comment\') ENGINE = MergeTree ORDER BY a');
SELECT formatQuery('CREATE TABLE t (a DateTime TTL a + toIntervalDay(1) COMMENT \'a comment\') ENGINE = MergeTree ORDER BY a');
SELECT formatQuery('CREATE TABLE t (a String COLLATE utf8_bin COMMENT \'a comment\') ENGINE = Memory');
SELECT formatQuery('CREATE TABLE t (a String TTL now() COLLATE utf8_bin) ENGINE = MergeTree ORDER BY a');
SELECT formatQuery('ALTER TABLE t MODIFY COLUMN a UInt64 CODEC(ZSTD) COMMENT \'a comment\'');
SELECT formatQuery('ALTER TABLE t MODIFY COLUMN a SETTINGS (max_compress_block_size = 1024) COMMENT \'a comment\'');
SELECT formatQuery('ALTER TABLE t MODIFY COLUMN a STATISTICS(tdigest) COMMENT \'a comment\'');
SELECT formatQuery('ALTER TABLE t MODIFY COLUMN a COLLATE utf8_bin COMMENT \'a comment\'');

-- Formatting is idempotent: the canonical order prints `COLLATE` last, and it is parsed back.
SELECT formatQuery(formatQuery('CREATE TABLE t (a String COLLATE utf8_bin COMMENT \'a comment\') ENGINE = Memory'));
SELECT formatQuery(formatQuery('CREATE TABLE t (a String TTL now() COLLATE utf8_bin) ENGINE = MergeTree ORDER BY a'));

-- Every modifier is still accepted at most once.
CREATE TABLE t_column_comment_order_bad (a UInt64 COMMENT 'x' COMMENT 'y') ENGINE = Memory; -- { clientError SYNTAX_ERROR }
CREATE TABLE t_column_comment_order_bad (a UInt64 CODEC(ZSTD) COMMENT 'x' CODEC(LZ4)) ENGINE = Memory; -- { clientError SYNTAX_ERROR }
CREATE TABLE t_column_comment_order_bad (a UInt64 SETTINGS (max_compress_block_size = 1024) COMMENT 'x' SETTINGS (max_compress_block_size = 2048)) ENGINE = Memory; -- { clientError SYNTAX_ERROR }
CREATE TABLE t_column_comment_order_bad (a String COLLATE utf8_bin COMMENT 'x' COLLATE binary) ENGINE = Memory; -- { clientError SYNTAX_ERROR }
