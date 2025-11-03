#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# Firstly write parts with use_const_adaptive_granularity=0 and then enable it and check that index_granularity_bytes_in_memory_allocated=25 (sizeof constant granularity)

$CLICKHOUSE_CLIENT --allow_experimental_full_text_index=1 -nm -q "
drop table if exists test_materialize;
create table test_materialize
(
  key Int,
  value String,
  vec Array(Float32),

  index val_idx1 value type minmax,
  index val_idx2 value type set(10),
  index val_idx3 value type bloom_filter,
  index val_idx4 value type ngrambf_v1(3, 256, 2, 0),
  index val_idx5 value type tokenbf_v1(256, 2, 0),
  index val_idx6 value type text(tokenizer = ngrams(3)),

  -- NOTE: vector similarity index is not stable
  -- index vec_idx vec type vector_similarity('hnsw', 'L2Distance', 2),
)
engine=MergeTree()
order by key
settings
  index_granularity=100,
  index_granularity_bytes=10e6,
  use_const_adaptive_granularity=false,
  enable_index_granularity_compression=false,
  min_bytes_for_wide_part=0,
  -- otherwise sparse info will be different, since for INSERTs the sparse ratio is calculated for the whole block, while for mutations for each granula (FIXME?)
  ratio_of_defaults_for_sparse_serialization=1,
  -- there was a bug with checksums, fixed in https://github.com/ClickHouse/ClickHouse/pull/89381
  auto_statistics_types=''
;
insert into test_materialize select number, repeat('a', number), [1 + number/10e3, 0 + number/10e3] from numbers(10e3) settings max_block_size=1e6;
-- { echo }
select count() from test_materialize;
"

hashes="$($CLICKHOUSE_CLIENT -q "select (hash_of_all_files, hash_of_uncompressed_files, uncompressed_hash_of_compressed_files) from system.parts where database = currentDatabase() and table = 'test_materialize'")"
$CLICKHOUSE_CLIENT --allow_experimental_full_text_index=1 -nm -q "
-- { echo }
select rows, index_granularity_bytes_in_memory_allocated>25 from system.parts where database = currentDatabase() and table = 'test_materialize' order by 1;
alter table test_materialize modify setting use_const_adaptive_granularity;
alter table test_materialize rewrite parts settings mutations_sync=2;
select rows, index_granularity_bytes_in_memory_allocated from system.parts where database = currentDatabase() and table = 'test_materialize' and active order by 1;
select count() from test_materialize;
select * from system.mutations where database = currentDatabase() and not is_done format Vertical;
detach table test_materialize;
attach table test_materialize;
"
new_hashes="$($CLICKHOUSE_CLIENT -q "select (hash_of_all_files, hash_of_uncompressed_files, uncompressed_hash_of_compressed_files) from system.parts where database = currentDatabase() and table = 'test_materialize' and active")"
if [ "$hashes" != "$new_hashes" ]; then
  echo "Hashes does not matches: '$hashes' vs '$new_hashes'"
else
  echo "Hashes are the same"
fi
