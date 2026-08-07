#!/usr/bin/env bash
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT "

DROP TABLE IF EXISTS 02703_rptable;
DROP TABLE IF EXISTS 02703_rptable_another;
CREATE TABLE 02703_rptable (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;

INSERT INTO 02703_rptable VALUES (1, 10), (2, 20), (3, 30), (4, 40);

CREATE TABLE 02703_rptable_another ENGINE = MergeTree ORDER BY x AS SELECT * FROM 02703_rptable;


DROP ROW POLICY IF EXISTS 02703_filter_policy_${CLICKHOUSE_DATABASE}_1 ON ${CLICKHOUSE_DATABASE}.02703_rptable;
DROP ROW POLICY IF EXISTS 02703_filter_policy_${CLICKHOUSE_DATABASE}_2 ON ${CLICKHOUSE_DATABASE}.*;
DROP ROW POLICY IF EXISTS 02703_filter_policy_${CLICKHOUSE_DATABASE}_3 ON ${CLICKHOUSE_DATABASE}.02703_rptable;
DROP ROW POLICY IF EXISTS 02703_filter_policy_${CLICKHOUSE_DATABASE}_4 ON ${CLICKHOUSE_DATABASE}.02703_rptable;
DROP ROW POLICY IF EXISTS 02703_filter_policy_${CLICKHOUSE_DATABASE}_5 ON ${CLICKHOUSE_DATABASE}.*;

-- the test assumes users_without_row_policies_can_read_rows is true

SELECT 'None';
SELECT * FROM 02703_rptable;

CREATE ROW POLICY 02703_filter_policy_${CLICKHOUSE_DATABASE}_1 ON ${CLICKHOUSE_DATABASE}.02703_rptable USING x=1 AS permissive TO ALL;
SELECT 'R1: x == 1';
SELECT * FROM 02703_rptable;

CREATE ROW POLICY 02703_filter_policy_${CLICKHOUSE_DATABASE}_2 ON ${CLICKHOUSE_DATABASE}.* USING x=2 AS permissive TO ALL;
SELECT 'R1, R2: (x == 1) OR (x == 2)';
SELECT * FROM 02703_rptable;

SELECT 'R1, R2: (x == 2) FROM ANOTHER';
SELECT * FROM 02703_rptable_another;

CREATE ROW POLICY 02703_filter_policy_${CLICKHOUSE_DATABASE}_3 ON ${CLICKHOUSE_DATABASE}.02703_rptable USING x=3 AS permissive TO ALL;
SELECT 'R1, R2, R3: (x == 1) OR (x == 2) OR (x == 3)';
SELECT * FROM 02703_rptable;

CREATE ROW POLICY 02703_filter_policy_${CLICKHOUSE_DATABASE}_4 ON ${CLICKHOUSE_DATABASE}.02703_rptable USING x<=2 AS restrictive TO ALL;
SELECT 'R1, R2, R3, R4: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2)';
SELECT * FROM 02703_rptable;

CREATE ROW POLICY 02703_filter_policy_${CLICKHOUSE_DATABASE}_5 ON ${CLICKHOUSE_DATABASE}.* USING y>=20 AS restrictive TO ALL;
SELECT 'R1, R2, R3, R4, R5: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2) AND (y >= 20)';
SELECT * FROM 02703_rptable;

CREATE TABLE 02703_after_rp ENGINE = MergeTree ORDER BY x AS SELECT * FROM 02703_rptable;
SELECT * FROM 02703_after_rp;

-- does not matter if policies or table are created first
SELECT 'R1, R2, R3, R4, R5: (x == 2) AND (y >= 20) FROM AFTER_RP';
SELECT * FROM 02703_after_rp;

SELECT 'R1, R2, R3, R4, R5: (x == 2) AND (y >= 20) FROM ANOTHER';
SELECT * FROM 02703_rptable_another;

DROP ROW POLICY 02703_filter_policy_${CLICKHOUSE_DATABASE}_1 ON ${CLICKHOUSE_DATABASE}.02703_rptable;
SELECT 'R2, R3, R4, R5: ((x == 2) OR (x == 3)) AND (x <= 2) AND (y >= 20)';
SELECT * FROM 02703_rptable;

DROP ROW POLICY 02703_filter_policy_${CLICKHOUSE_DATABASE}_2 ON ${CLICKHOUSE_DATABASE}.*;
SELECT 'R3, R4, R5: (x == 3) AND (x <= 2) AND (y >= 20)';
SELECT * FROM 02703_rptable;

DROP ROW POLICY 02703_filter_policy_${CLICKHOUSE_DATABASE}_3 ON ${CLICKHOUSE_DATABASE}.02703_rptable;
SELECT 'R4, R5: (x <= 2) AND (y >= 20)';
SELECT * FROM 02703_rptable;

DROP ROW POLICY 02703_filter_policy_${CLICKHOUSE_DATABASE}_4 ON ${CLICKHOUSE_DATABASE}.02703_rptable;
SELECT 'R5: (x >= 2)';
SELECT * FROM 02703_rptable;

CREATE TABLE 02703_unexpected_columns (xx UInt8, yy UInt8) ENGINE = MergeTree ORDER BY xx;
SELECT 'Policy not applicable';
SELECT * FROM 02703_unexpected_columns; -- { serverError UNKNOWN_IDENTIFIER } -- Missing columns: 'x' while processing query

DROP ROW POLICY 02703_filter_policy_${CLICKHOUSE_DATABASE}_5 ON ${CLICKHOUSE_DATABASE}.*;
SELECT 'None';
SELECT * FROM 02703_rptable;

SELECT 'No problematic policy, select works';
SELECT 'Ok' FROM ${CLICKHOUSE_DATABASE}.02703_unexpected_columns;

DROP TABLE 02703_rptable;
DROP TABLE 02703_rptable_another;
DROP TABLE 02703_unexpected_columns;

"
