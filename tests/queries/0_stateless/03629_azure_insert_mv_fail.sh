#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-distributed-cache
# Tag no-fasttest: requires Azure
# Tag no-shared-merge-tree: does not support replication
# Tag no-distributed-cache: Not supported auth type

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
drop table if exists t0;
drop table if exists t1;
"

${CLICKHOUSE_CLIENT} -nm --query "
CREATE TABLE t0 (c0 Int) ENGINE = Memory;
CREATE TABLE t1 (c0 Int) ENGINE = AzureBlobStorage('http://127.0.0.1:10000/devstoreaccount1', 'cont', 'f0', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', CSV);

CREATE MATERIALIZED VIEW v0 TO t1 (c0 Int) AS (SELECT 1 c0 FROM t0 JOIN t1 ON TRUE);
"

${CLICKHOUSE_CLIENT} -nm --materialized_views_ignore_errors=1 --query "
    INSERT INTO TABLE t0 (c0) VALUES (1);
"

${CLICKHOUSE_CLIENT} --query "
drop table if exists t0;
drop table if exists t1;
"
