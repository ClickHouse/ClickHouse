#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
CREATE TEMPORARY TABLE t_src (id UInt32, val String) ENGINE = Memory;
INSERT INTO t_src VALUES (1,'a'), (2,'b'), (3,'c');

CREATE TEMPORARY VIEW tview_basic AS
SELECT id, upper(val) AS u
FROM t_src
WHERE id <= 2;

SELECT * FROM tview_basic ORDER BY id;

EXISTS TEMPORARY VIEW tview_basic;
SHOW TEMPORARY VIEW tview_basic;
"

$CLICKHOUSE_CLIENT -nm -q "
CREATE TEMPORARY TABLE t_src (id UInt32, val String) ENGINE = Memory;
INSERT INTO t_src VALUES (1,'a'), (2,'b'), (3,'c');

CREATE TEMPORARY VIEW tview_basic AS
SELECT id, upper(val) AS u
FROM t_src
WHERE id <= 2;

SELECT * FROM tview_basic ORDER BY id;

EXISTS TEMPORARY VIEW tview_basic;
SHOW TEMPORARY VIEW tview_basic;
"

$CLICKHOUSE_CLIENT -nm -q "
SHOW TEMPORARY VIEW tview_basic; -- { serverError UNKNOWN_TABLE }
"
