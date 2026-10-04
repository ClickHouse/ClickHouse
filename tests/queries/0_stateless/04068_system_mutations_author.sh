#!/usr/bin/env bash
# Tags: zookeeper

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# User names are global, so they are suffixed with the test database to let the test run in parallel
# with itself. `clickhouse-test` replaces the database name with `default` in the output, which keeps
# the reference file deterministic.
USER="test_mutations_author_user_${CLICKHOUSE_DATABASE}"
# A backslash in the name checks that the author is escaped correctly in the serialized mutation entry.
SPECIAL='`test_mutations\\author_special_'"${CLICKHOUSE_DATABASE}"'`'

$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS test_mutations_author_regular;
DROP TABLE IF EXISTS test_mutations_author_replicated;
DROP USER IF EXISTS \`$USER\`;
DROP USER IF EXISTS $SPECIAL;

CREATE TABLE test_mutations_author_regular (id UInt64, value String) ENGINE = MergeTree ORDER BY id SETTINGS persist_mutation_author = 1;
CREATE TABLE test_mutations_author_replicated (id UInt64, value String) ENGINE = ReplicatedMergeTree ('/clickhouse/{database}/test_mutations_author_replicated', '1') ORDER BY id SETTINGS persist_mutation_author = 1;
CREATE USER \`$USER\` IDENTIFIED WITH no_password;
GRANT ALTER UPDATE ON *.* TO \`$USER\`;
CREATE USER $SPECIAL IDENTIFIED WITH no_password;
GRANT ALTER UPDATE ON *.* TO $SPECIAL;

INSERT INTO test_mutations_author_regular VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO test_mutations_author_replicated VALUES (4, 'e'), (5, 'f'), (6, 'g');

SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'mutations' AND name = 'author';

-- default user mutations
ALTER TABLE test_mutations_author_regular UPDATE value = 'x' WHERE id = 1 SETTINGS mutations_sync = 1;
ALTER TABLE test_mutations_author_replicated UPDATE value = 'y' WHERE id = 4 SETTINGS mutations_sync = 1;

SELECT database, table, is_done, author FROM system.mutations
WHERE database = currentDatabase() AND table IN ('test_mutations_author_regular', 'test_mutations_author_replicated')
ORDER BY table, mutation_id;

-- custom user mutation
EXECUTE AS \`$USER\` ALTER TABLE test_mutations_author_regular UPDATE value = 'z' WHERE id = 2 SETTINGS mutations_sync = 1;

SELECT database, table, is_done, author FROM system.mutations
WHERE database = currentDatabase() AND table = 'test_mutations_author_regular'
ORDER BY mutation_id;

-- user with special character (backslash) in name - tests escaping
EXECUTE AS $SPECIAL ALTER TABLE test_mutations_author_regular UPDATE value = 'w' WHERE id = 3 SETTINGS mutations_sync = 1;
EXECUTE AS $SPECIAL ALTER TABLE test_mutations_author_replicated UPDATE value = 'w' WHERE id = 5 SETTINGS mutations_sync = 1;

SELECT database, table, is_done, author FROM system.mutations
WHERE database = currentDatabase() AND table IN ('test_mutations_author_regular', 'test_mutations_author_replicated')
ORDER BY table, mutation_id;

-- reload mutations from disk
DETACH TABLE test_mutations_author_regular;
ATTACH TABLE test_mutations_author_regular;

-- reload mutations from ZooKeeper
DETACH TABLE test_mutations_author_replicated;
ATTACH TABLE test_mutations_author_replicated;

-- Do not check \`is_done\` here: right after ATTACH of a replicated table the mutation state
-- may not be recomputed yet, so \`is_done\` is transiently 0.
SELECT database, table, author FROM system.mutations
WHERE database = currentDatabase() AND table IN ('test_mutations_author_regular', 'test_mutations_author_replicated')
ORDER BY table, mutation_id;

-- when \`persist_mutation_author\` is disabled (the default), the author is not recorded
-- and the mutation entry keeps the old format
DROP TABLE IF EXISTS test_mutations_author_disabled;
CREATE TABLE test_mutations_author_disabled (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_mutations_author_disabled VALUES (7, 'h');
ALTER TABLE test_mutations_author_disabled UPDATE value = 'v' WHERE id = 7 SETTINGS mutations_sync = 1;

SELECT database, table, is_done, author FROM system.mutations
WHERE database = currentDatabase() AND table = 'test_mutations_author_disabled';

DROP TABLE test_mutations_author_disabled;
DROP TABLE test_mutations_author_regular;
DROP TABLE test_mutations_author_replicated;
DROP USER \`$USER\`;
DROP USER $SPECIAL;
"
