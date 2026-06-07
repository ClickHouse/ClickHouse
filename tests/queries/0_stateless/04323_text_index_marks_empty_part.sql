DROP TABLE IF EXISTS t_text_idx_empty;

CREATE TABLE t_text_idx_empty
(
    s FixedString(37),
    INDEX idx s TYPE text(tokenizer = array()) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS prewarm_mark_cache = true;

INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number), 37) FROM numbers(5);
INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number + 5), 37) FROM numbers(5);
INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number + 10), 37) FROM numbers(5);
INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number + 15), 37) FROM numbers(5);
INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number + 20), 37) FROM numbers(5);
INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number + 25), 37) FROM numbers(5);
INSERT INTO t_text_idx_empty SELECT toFixedString(toString(number + 30), 37) FROM numbers(5);

OPTIMIZE TABLE t_text_idx_empty FINAL;
ALTER TABLE t_text_idx_empty DELETE WHERE 1 SETTINGS mutations_sync = 2;

SYSTEM PREWARM MARK CACHE t_text_idx_empty;

SELECT count() FROM t_text_idx_empty;
SELECT count() FROM t_text_idx_empty WHERE has(['anything'], s);

DROP TABLE t_text_idx_empty;
