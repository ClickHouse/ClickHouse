#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

u1="${CLICKHOUSE_TEST_UNIQUE_NAME}_collection1"
u2="${CLICKHOUSE_TEST_UNIQUE_NAME}_collection2"
u3="${CLICKHOUSE_TEST_UNIQUE_NAME}_collection3"

${CLICKHOUSE_CLIENT}  -m --query "

DROP NAMED COLLECTION IF EXISTS $u1;
DROP NAMED COLLECTION IF EXISTS $u2;
DROP NAMED COLLECTION IF EXISTS $u3;

CREATE NAMED COLLECTION IF NOT EXISTS $u1 AS
    url = 'http://127.0.0.1:8123?query=select+1' NOT OVERRIDABLE,
    format = 'RawBLOB' OVERRIDABLE;

CREATE NAMED COLLECTION IF NOT EXISTS $u2 AS
    url = 'http://127.0.0.1:8123?query=select+1',
    format = 'RawBLOB';

SET allow_named_collection_override_by_default=1;
SELECT 'allow_named_collection_override_by_default=1 u1';
SELECT * FROM url($u1);
SELECT * FROM url($u1, headers('Accept'='text/csv; charset=utf-8'));
SELECT * FROM url($u1, url='http://127.0.0.1:8123?query=select+2'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url($u1, format='CSV');
SELECT 'allow_named_collection_override_by_default=1 u2';
SELECT * FROM url($u2);
SELECT * FROM url($u2, headers('Accept'='text/csv; charset=utf-8'));
SELECT * FROM url($u2, url='http://127.0.0.1:8123?query=select+2');
SELECT * FROM url($u2, format='CSV');

SET allow_named_collection_override_by_default=0;
SELECT 'allow_named_collection_override_by_default=0 u1';
SELECT * FROM url($u1);
SELECT * FROM url($u1, headers('Accept'='text/csv; charset=utf-8')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url($u1, url='http://127.0.0.1:8123?query=select+2'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url($u1, format='CSV');
SELECT 'allow_named_collection_override_by_default=0 u2';
SELECT * FROM url($u2);
SELECT * FROM url($u2, headers('Accept'='text/csv; charset=utf-8')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url($u2, url='http://127.0.0.1:8123?query=select+2'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url($u2, format='CSV'); -- { serverError BAD_ARGUMENTS }

SELECT 'Test ALTER';

CREATE NAMED COLLECTION IF NOT EXISTS $u3 AS
    url = 'http://127.0.0.1:8123?query=select+1' NOT OVERRIDABLE,
    format = 'RawBLOB' OVERRIDABLE;

ALTER NAMED COLLECTION $u3 SET
    url = 'http://127.0.0.1:8123?query=select+2' OVERRIDABLE,
    format = 'RawBLOB' NOT OVERRIDABLE;

SELECT * FROM url($u3);
SELECT * FROM url($u3, url='http://127.0.0.1:8123?query=select+1');
SELECT * FROM url($u3, format='CSV'); -- { serverError BAD_ARGUMENTS }

DROP NAMED COLLECTION IF EXISTS $u1;
DROP NAMED COLLECTION IF EXISTS $u2;
DROP NAMED COLLECTION IF EXISTS $u2;

SELECT 'Test XML collections';

SET allow_named_collection_override_by_default=1;
SELECT 'allow_named_collection_override_by_default=1 url_override1';
SELECT * FROM url(url_override1);
SELECT * FROM url(url_override1, headers('Accept'='text/csv; charset=utf-8'));
SELECT * FROM url(url_override1, url='http://127.0.0.1:8123?query=select+2'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url(url_override1, format='CSV');
SELECT 'allow_named_collection_override_by_default=1 url_override2';
SELECT * FROM url(url_override2);
SELECT * FROM url(url_override2, headers('Accept'='text/csv; charset=utf-8'));
SELECT * FROM url(url_override2, url='http://127.0.0.1:8123?query=select+2');
SELECT * FROM url(url_override2, format='CSV');

SET allow_named_collection_override_by_default=0;
SELECT 'allow_named_collection_override_by_default=0 url_override1';
SELECT * FROM url(url_override1);
SELECT * FROM url(url_override1, headers('Accept'='text/csv; charset=utf-8')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url(url_override1, url='http://127.0.0.1:8123?query=select+2'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url(url_override1, format='CSV');
SELECT 'allow_named_collection_override_by_default=0 url_override2';
SELECT * FROM url(url_override2);
SELECT * FROM url(url_override2, headers('Accept'='text/csv; charset=utf-8')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url(url_override2, url='http://127.0.0.1:8123?query=select+2'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM url(url_override2, format='CSV'); -- { serverError BAD_ARGUMENTS }
"
