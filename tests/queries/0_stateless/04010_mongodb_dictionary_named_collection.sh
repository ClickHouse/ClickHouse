#!/usr/bin/env bash
# Tags: no-fasttest
# Verify that MongoDB dictionary source works with named collections
# without throwing "Unexpected key `name` in named collection" (issue #97840)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NC_URI="test_mongo_nc_uri_${CLICKHOUSE_DATABASE}"
NC_HOST="test_mongo_nc_host_${CLICKHOUSE_DATABASE}"
DICT_URI="test_mongo_dict_uri_${CLICKHOUSE_DATABASE}"
DICT_HOST="test_mongo_dict_host_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${NC_URI} AS uri = 'mongodb://localhost:27017/testdb';

CREATE DICTIONARY ${DICT_URI}
(
    id UInt64,
    name String
)
PRIMARY KEY id
SOURCE(MONGODB(NAME ${NC_URI} COLLECTION 'test_coll'))
LIFETIME(MIN 100 MAX 5400)
LAYOUT(HASHED());

SELECT * FROM ${DICT_URI}; -- { serverError DICTIONARIES_WAS_NOT_LOADED }

DROP DICTIONARY IF EXISTS ${DICT_URI};
DROP NAMED COLLECTION IF EXISTS ${NC_URI};
"

$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${NC_HOST} AS host = 'localhost', port = '27017', db = 'testdb', user = '', password = '';

CREATE DICTIONARY ${DICT_HOST}
(
    id UInt64,
    name String
)
PRIMARY KEY id
SOURCE(MONGODB(NAME ${NC_HOST} COLLECTION 'test_coll'))
LIFETIME(MIN 100 MAX 5400)
LAYOUT(HASHED());

SELECT * FROM ${DICT_HOST}; -- { serverError DICTIONARIES_WAS_NOT_LOADED }

DROP DICTIONARY IF EXISTS ${DICT_HOST};
DROP NAMED COLLECTION IF EXISTS ${NC_HOST};
"

# Verify that 'options' is rejected when 'uri' is used, since options are part of the URI.
NC_OPT="test_mongo_nc_opt_${CLICKHOUSE_DATABASE}"
DICT_OPT="test_mongo_dict_opt_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${NC_OPT} AS uri = 'mongodb://localhost:27017/testdb', options = 'ssl=false';

CREATE DICTIONARY ${DICT_OPT}
(
    id UInt64,
    name String
)
PRIMARY KEY id
SOURCE(MONGODB(NAME ${NC_OPT} COLLECTION 'test_coll'))
LIFETIME(MIN 100 MAX 5400)
LAYOUT(HASHED());

SELECT * FROM ${DICT_OPT}; -- { serverError BAD_ARGUMENTS }

DROP DICTIONARY IF EXISTS ${DICT_OPT};
DROP NAMED COLLECTION IF EXISTS ${NC_OPT};
"
