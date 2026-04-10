#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest - CountMinSketch is not compiled

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
drop table if exists mt;
create table mt (key Int, value String) engine=MergeTree() order by key settings
  index_granularity=100,
  index_granularity_bytes=10e6,
  min_bytes_for_wide_part=0,
  -- otherwise sparse info will be different, since for INSERTs the sparse ratio is calculated for the whole block, while for mutations for each granula (FIXME?)
  ratio_of_defaults_for_sparse_serialization=1,
  -- This uncovers the bug
  auto_statistics_types='uniq,minmax,countmin,tdigest'
;
insert into mt select number, repeat('a', number) from numbers(10e3) settings max_block_size=1e6;
-- { echo }
select count() from mt;
"

hashes="$($CLICKHOUSE_CLIENT -q "select (hash_of_all_files, hash_of_uncompressed_files, uncompressed_hash_of_compressed_files) from system.parts where database = currentDatabase() and table = 'mt'")"
$CLICKHOUSE_CLIENT -nm -q "
-- { echo }
alter table mt rewrite parts settings mutations_sync=2;
select count() from mt;
detach table mt;
attach table mt;
select count() from mt;
"

new_hashes="$($CLICKHOUSE_CLIENT -q "select (hash_of_all_files, hash_of_uncompressed_files, uncompressed_hash_of_compressed_files) from system.parts where database = currentDatabase() and table = 'mt' and active")"
if [ "$hashes" != "$new_hashes" ]; then
  echo "Hashes does not matches: '$hashes' vs '$new_hashes'"
else
  echo "Hashes are the same"
fi
