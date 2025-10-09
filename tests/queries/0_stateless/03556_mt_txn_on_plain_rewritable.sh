#!/usr/bin/env bash
# Tags: no-random-settings, no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-random-settings: enable after root causing flakiness

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} --query "
drop table if exists test;
create table test (key Int) engine=MergeTree() order by ()
SETTINGS table_disk = true,
  disk = disk(
      name = 03556_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/03556/${CLICKHOUSE_DATABASE}/');
insert into test settings implicit_transaction=1 values (1);
detach table test;
attach table test;
"
