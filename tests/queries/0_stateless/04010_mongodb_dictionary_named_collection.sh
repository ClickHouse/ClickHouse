#!/usr/bin/env bash
# Tags: no-fasttest
# Verify that MongoDB dictionary source works with named collections
# without throwing "Unexpected key `name` in named collection" (issue #97840)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NC_NAME="test_mongo_nc_${CLICKHOUSE_DATABASE}"
DICT_NAME="test_mongo_dict_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${NC_NAME} AS uri = 'mongodb://localhost:27017/testdb', options = 'ssl=false';

CREATE DICTIONARY ${DICT_NAME}
(
    id UInt64,
    name String
)
PRIMARY KEY id
SOURCE(MONGODB(NAME ${NC_NAME} COLLECTION 'test_coll'))
LIFETIME(MIN 100 MAX 5400)
LAYOUT(HASHED());

SELECT * FROM ${DICT_NAME}; -- { serverError DICTIONARIES_WAS_NOT_LOADED }

DROP DICTIONARY IF EXISTS ${DICT_NAME};
DROP NAMED COLLECTION IF EXISTS ${NC_NAME};

SELECT 'OK';
"

