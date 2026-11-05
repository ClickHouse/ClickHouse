#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
drop table if exists \`04338_t\`;
create table \`04338_t\` (a Int32) engine = MergeTree() order by tuple();
"

backup_id=${CLICKHOUSE_TEST_UNIQUE_NAME}
backup_name="Disk('backups', '$backup_id')";

# Request a few non-default backup-specific settings, including a string-valued one.
${CLICKHOUSE_CLIENT} --query "
backup table ${CLICKHOUSE_DATABASE}.\`04338_t\` to $backup_name
settings deduplicate_files=0, allow_s3_native_copy=0, compression_method='zstd';
" | grep -o "BACKUP_CREATED"

# The requested settings are observable in system.backups.settings, while the secret
# 'password' and the duplicative 'id' are never exposed. A Disk backup has no
# engine-specific (e.g. S3) settings, so engine_settings is empty. String-valued settings are
# stored verbatim, without SQL-literal quoting, so settings['compression_method'] = 'zstd' holds.
${CLICKHOUSE_CLIENT} -m --query "
select
    settings['deduplicate_files'],
    settings['allow_s3_native_copy'],
    mapContains(settings, 'password'),
    mapContains(settings, 'id'),
    length(engine_settings),
    settings['compression_method'] = 'zstd'
from system.backups where name='Disk(\'backups\', \'$backup_id\')'
"
