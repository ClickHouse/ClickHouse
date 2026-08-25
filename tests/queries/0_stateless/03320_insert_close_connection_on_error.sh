#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: relies on system.errors

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -nm -q "
    drop table if exists in_dist;
    drop table if exists in;
    drop table if exists mv;
    drop table if exists proxy;

    -- we need INSERT into distributed table to trigger multiple INSERTs from one query
    create table in_dist (dummy Int) engine=Distributed('test_shard_localhost', currentDatabase(), in);

    create table in (dummy Int) engine=Null();
    -- Distributed table that will reject any INSERTs
    create table proxy engine=Distributed('test_shard_localhost', system, one);
    create materialized view mv to proxy as select * from in;
"

tmp_file=$(mktemp "$CUR_DIR/clickhouse.XXXXXX.tsv")
trap 'rm $tmp_file' EXIT
yes 1 | head -n 10 > "$tmp_file"

iterations=10
for _ in $(seq 1 $iterations); do
    errors_before=$($CLICKHOUSE_CLIENT -q "select value from system.errors where name = 'UNEXPECTED_PACKET_FROM_CLIENT'")
    opts=(
        # split file into blocks of 1 row
        # (NOTE: cannot be set in a query, since INFILE uses client context)
        --max_block_size=1

        # disable squshing, so that the block will be processed block-by-block
        --min_insert_block_size_rows=0
        --min_insert_block_size_bytes=0

        # disable randomization
        --prefer_localhost_replica=1
        --distributed_foreground_insert=0
    )
    $CLICKHOUSE_CLIENT "${opts[@]}" -nm -q "
        insert into in_dist from infile '$tmp_file'; -- { serverError NOT_IMPLEMENTED }
    "
    errors_after=$($CLICKHOUSE_CLIENT -q "select value from system.errors where name = 'UNEXPECTED_PACKET_FROM_CLIENT'")
    if [[ "$errors_before" != "$errors_after" ]]; then
        # retry
        continue
    fi
done

if [[ "$errors_before" != "$errors_after" ]]; then
    echo "UNEXPECTED_PACKET_FROM_CLIENT does not match ($errors_before vs $errors_after) after $iterations iterations"
fi
