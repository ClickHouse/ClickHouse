#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the `mongodb` table function and the `MongoDB` table engine need USE_MONGODB
# no-replicated-database: a named collection is server-global, not database-scoped

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A named collection is server-global, so the name carries the test database to keep concurrent runs
# of this test apart.
NC="nc_05251_${CLICKHOUSE_DATABASE}"
NC_HOST="nc_05251_host_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
-- \`create_table_query\` is rendered by the masker that also renders the query log, the server log and
-- \`SHOW CREATE\`, and creating a view or a MongoDB table contacts no server, so every row below is a
-- network-free assertion on what those sinks print.

DROP NAMED COLLECTION IF EXISTS ${NC};
DROP NAMED COLLECTION IF EXISTS ${NC_HOST};
CREATE NAMED COLLECTION ${NC} AS uri = 'mongodb://usr:COLLPW@127.0.0.1:27017/db', collection = 'c';
CREATE NAMED COLLECTION ${NC_HOST} AS host = '127.0.0.1', port = 27017, user = 'usr', password = 'COLLPW', database = 'db', collection = 'c';

-- The credential must be hidden in the URI form of both surfaces, at every arity.
CREATE VIEW f01 AS SELECT * FROM mongodb('mongodb://usr:MONGOPW@127.0.0.1:27017/db', 'c', 'x String');
CREATE VIEW f02 AS SELECT * FROM mongodb('mongodb://usr:MONGOPW@127.0.0.1:27017/db', 'c', 'x String', '_id');
CREATE VIEW f03 AS SELECT * FROM mongodb('mongodb://usr:MONGOPW@127.0.0.1:27017/db', 'c', structure = 'x String');
CREATE VIEW f04 AS SELECT * FROM mongodb('mongodb://usr:MONGOPW@127.0.0.1:27017/db', 'c', 'x String', oid_columns = '_id');
CREATE TABLE f05 (x String) ENGINE = MongoDB('mongodb://usr:MONGOPW@127.0.0.1:27017/db', 'c', '_id');
-- A URI built from an expression cannot be read here, so it is hidden whole rather than printed.
CREATE TABLE f06 (x String) ENGINE = MongoDB(concat('mongodb://usr:', 'MONGOPW', '@127.0.0.1:27017/db'), 'c', '_id');
-- An override is hidden at its own position wherever it is written, and no other argument is
-- overwritten by the masked copy: \`structure\` and \`collection\` must read back unchanged.
CREATE VIEW f07 AS SELECT * FROM mongodb(${NC}, structure = 'x String', uri = 'mongodb://usr:MONGOPW@127.0.0.1:27017/db', collection = 'c');
CREATE VIEW f08 AS SELECT * FROM mongodb(${NC}, collection = 'c', structure = 'x String', uri = 'mongodb://usr:MONGOPW@127.0.0.1:27017/db');
-- Every occurrence is hidden, not just the first: the effective override is the last one.
CREATE VIEW f09 AS SELECT * FROM mongodb(${NC}, uri = 'mongodb://usr:MONGOPW@127.0.0.1:27017/db', uri = 'mongodb://usr:MONGOPW2@127.0.0.1:27017/db', collection = 'c', structure = 'x String');
-- An override key is evaluated as a constant expression, so a key that is not a plain literal here can
-- still name \`uri\` or \`password\`. Such a value is hidden and the key expression stays visible.
CREATE VIEW f10 AS SELECT * FROM mongodb(${NC}, concat('pass', 'word') = 'MONGOPW', collection = 'c', structure = 'x String');
CREATE VIEW f11 AS SELECT * FROM mongodb(${NC}, concat('u', 'ri') = 'mongodb://usr:MONGOPW@127.0.0.1:27017/db', collection = 'c', structure = 'x String');
-- An override VALUE is evaluated the same way, so a \`uri\` override that cannot be read here is still
-- accepted and effective. It is hidden whole rather than printed.
CREATE VIEW f12 AS SELECT * FROM mongodb(${NC}, uri = concat('mongodb://usr:', 'MONGOPW', '@127.0.0.1:27017/db'), collection = 'c', structure = 'x String');
-- A post-collection argument that is not a \`key = value\` override is accepted and ignored, so it is
-- hidden whole rather than printed, both on its own and next to an effective \`uri\` override.
CREATE TABLE f13 (x String) ENGINE = MongoDB(${NC}, concat('mongodb://usr:', 'MONGOPW', '@127.0.0.1:27017/db'));
CREATE VIEW f14 AS SELECT * FROM mongodb(${NC}, concat('mongodb://usr:', 'MONGOPW', '@127.0.0.1:27017/db'), structure = 'x String');
CREATE VIEW f15 AS SELECT * FROM mongodb(${NC}, concat('mongodb://usr:', 'MONGOPW', '@127.0.0.1:27017/db'), uri = 'mongodb://usr:MONGOPW2@127.0.0.1:27017/db', structure = 'x String');

-- Controls: masked before this change too, and their render must not move.
CREATE TABLE c01 (x String) ENGINE = MongoDB('mongodb://usr:MONGOPW@127.0.0.1:27017/db', 'c');
CREATE VIEW c02 AS SELECT * FROM mongodb(${NC}, uri = 'mongodb://usr:MONGOPW@127.0.0.1:27017/db', collection = 'c', structure = 'x String');
CREATE VIEW c03 AS SELECT * FROM mongodb('127.0.0.1:27017', 'db', 'c', 'usr', 'MONGOPW', 'x String');
CREATE TABLE c04 (x String) ENGINE = MongoDB('127.0.0.1:27017', 'db', 'c', 'usr', 'MONGOPW');
-- In the \`host:port\` form only the password is hidden: the user and the trailing positional stay visible.
CREATE VIEW c05 AS SELECT * FROM mongodb('127.0.0.1:27017', 'db', 'c', 'usr', 'MONGOPW', structure = 'x String', options = 'ssl=false', oid_columns = '_id');
CREATE VIEW c06 AS SELECT * FROM mongodb(${NC_HOST}, password = 'MONGOPW', structure = 'x String');
-- A URI that carries no credential keeps its text, at the arity this change newly scans and at the
-- arity that was scanned before it.
CREATE TABLE c07 (x String) ENGINE = MongoDB('mongodb://127.0.0.1:27017/db', 'c', '_id');
CREATE TABLE c08 (x String) ENGINE = MongoDB('mongodb://127.0.0.1:27017/db', 'c');
-- Argument 0 of the \`host:port\` forms is a destination, not a credential, so one that cannot be read
-- here keeps its text and only the password is hidden. The table function rejects a bare expression
-- argument (\`Code: 36\`), so the engine is the surface on which this shape is accepted.
CREATE TABLE c09 (x String) ENGINE = MongoDB(concat('127.0.0.1', ':27017'), 'db', 'c', 'usr', 'MONGOPW');
CREATE TABLE c10 (x String) ENGINE = MongoDB(concat('127.0.0.1', ':27017'), 'db', 'c', 'usr', 'MONGOPW', 'ssl=false', '_id');
-- A well-formed override of a key that is not a credential keeps both its key and its value.
CREATE VIEW c11 AS SELECT * FROM mongodb(${NC}, oid_columns = '_id', structure = 'x String');

SELECT name, replaceAll(create_table_query, '\n', ' ') FROM system.tables
WHERE database = currentDatabase() ORDER BY name;

DROP VIEW f01; DROP VIEW f02; DROP VIEW f03; DROP VIEW f04; DROP TABLE f05; DROP TABLE f06;
DROP VIEW f07; DROP VIEW f08; DROP VIEW f09; DROP VIEW f10; DROP VIEW f11; DROP VIEW f12;
DROP TABLE f13; DROP VIEW f14; DROP VIEW f15;
DROP TABLE c01; DROP VIEW c02; DROP VIEW c03; DROP TABLE c04; DROP VIEW c05; DROP VIEW c06;
DROP TABLE c07; DROP TABLE c08; DROP TABLE c09; DROP TABLE c10; DROP VIEW c11;
DROP NAMED COLLECTION ${NC};
DROP NAMED COLLECTION ${NC_HOST};
"
