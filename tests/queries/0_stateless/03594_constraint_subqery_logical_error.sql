-- Tags: no-random-detach, no-async-insert, no-flaky-check
-- no-random-detach: random DETACH/ATTACH races with INSERT and produces flaky
-- 'Unexpected packet from server (got Progress)' over the native protocol.
-- no-flaky-check: the same 'Unexpected packet from server (got Progress)' flake on the
-- INSERT expecting an exception pre-exists on the base branch independently of random
-- DETACH/ATTACH (seen on unrelated PRs where reruns pass), so flaky-check reruns of this
-- tag-only edit keep tripping on it.
-- - no-async-insert -- with wait_for_async_insert=0 the INSERT is fire-and-forget, so the constraint error is raised in the background flush and never reaches the client, breaking the { serverError } assertion.

CREATE TABLE check_constraint (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO TABLE check_constraint (c0) VALUES (1);
ALTER TABLE check_constraint ADD CONSTRAINT c0 CHECK (SELECT 1);
INSERT INTO TABLE check_constraint (c0) VALUES (1); -- { serverError UNKNOWN_IDENTIFIER }
SELECT 1 FROM check_constraint WHERE 1 = 1 SETTINGS optimize_substitute_columns = 1, convert_query_to_cnf = 1;

CREATE TABLE assume_constraint (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE assume_constraint ADD CONSTRAINT c0 ASSUME (SELECT 1);
INSERT INTO TABLE assume_constraint (c0) VALUES (1);
SELECT 1 FROM assume_constraint WHERE 1 = 1 SETTINGS optimize_substitute_columns = 1, convert_query_to_cnf = 1;
