-- A mutation of a persistent `Join` rewrites the table directory and replaces the in-memory state.
-- Both must end up describing the same rows: the rewrite is done first and the new state is
-- published only afterwards, so a reload can never disagree with what queries see.

DROP TABLE IF EXISTS mutated_join;
CREATE TABLE mutated_join (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k) SETTINGS persistent = 1;

INSERT INTO mutated_join VALUES (1, 10), (2, 20);
INSERT INTO mutated_join VALUES (3, 30), (4, 40);

ALTER TABLE mutated_join DELETE WHERE k = 2 SETTINGS mutations_sync = 2;
SELECT 'after delete', k, v FROM mutated_join ORDER BY k;

DETACH TABLE mutated_join;
ATTACH TABLE mutated_join;
SELECT 'after reattach', k, v FROM mutated_join ORDER BY k;

-- A second mutation must replace the rewritten file rather than accumulate next to it.
ALTER TABLE mutated_join DELETE WHERE k = 3 SETTINGS mutations_sync = 2;
SELECT 'after second delete', k, v FROM mutated_join ORDER BY k;

DETACH TABLE mutated_join;
ATTACH TABLE mutated_join;
SELECT 'after reattach', k, v FROM mutated_join ORDER BY k;

DROP TABLE mutated_join;
