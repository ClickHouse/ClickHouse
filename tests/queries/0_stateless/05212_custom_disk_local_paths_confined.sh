#!/usr/bin/env bash
# Tags: no-fasttest, no-distributed-cache
# no-fasttest: the S3 cases need the minio instance.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
server_path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.disks WHERE name = 'default'")
inside="${CLICKHOUSE_DISKS_FILES}/${name}"
outside="${server_path}${name}_outside"
s3_args="endpoint = 'http://localhost:11111/test/${name}/', access_key_id = 'clickhouse', secret_access_key = 'clickhouse'"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_outside (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(name = '${name}_outside', type = 'local_blob_storage', path = '${outside}/'); -- { serverError BAD_ARGUMENTS }
SELECT 'data path outside, disk registered', count() FROM system.disks WHERE name = '${name}_outside';
"
test -d "${outside}" && echo "data path outside, directory created" || echo "data path outside, directory not created"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_meta_outside (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(name = '${name}_meta_outside', type = 'local_blob_storage', path = '${inside}_meta/', metadata_path = '${outside}_meta/'); -- { serverError BAD_ARGUMENTS }
SELECT 'metadata path outside, disk registered', count() FROM system.disks WHERE name = '${name}_meta_outside';

CREATE TABLE t_s3_meta_outside (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(name = '${name}_s3_meta_outside', type = 's3', ${s3_args}, metadata_path = '${outside}_s3/'); -- { serverError BAD_ARGUMENTS }
SELECT 's3 metadata path outside, disk registered', count() FROM system.disks WHERE name = '${name}_s3_meta_outside';

CREATE TABLE t_escaping_name (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(name = '../${name}_escape', type = 's3', ${s3_args}); -- { serverError BAD_ARGUMENTS }
SELECT 'escaping name, disk registered', count() FROM system.disks WHERE name = '../${name}_escape';

CREATE TABLE t_cache_outside (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(type = cache, name = '${name}_cache_outside', path = '${name}_cache_outside', max_size = '10Mi',
    disk = disk(name = '${name}_nested_outside', type = 'local_blob_storage', path = '${outside}_nested/')); -- { serverError BAD_ARGUMENTS }
SELECT 'nested data path outside, disk registered', count() FROM system.disks WHERE name = '${name}_nested_outside';

CREATE TABLE t_encrypted_outside (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(type = encrypted, name = '${name}_encrypted_outside', disk = 'local_disk', path = '../../${name}_escape/', key = '1234567812345678'); -- { serverError BAD_ARGUMENTS }
SELECT 'encrypted path outside wrapped disk, disk registered', count() FROM system.disks WHERE name = '${name}_encrypted_outside';

CREATE TABLE t_encrypted_default (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(type = encrypted, name = '${name}_encrypted_default', disk = 'default', path = '${name}_encrypted/', key = '1234567812345678'); -- { serverError BAD_ARGUMENTS }
SELECT 'encrypted over local disk outside base directory, disk registered', count() FROM system.disks WHERE name = '${name}_encrypted_default';

CREATE TABLE t_inside (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(name = '${name}_inside', type = 'local_blob_storage', path = '${inside}/');
INSERT INTO t_inside VALUES (1), (2);
SELECT 'data path inside', count() FROM t_inside;

CREATE TABLE t_s3 (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(name = '${name}_s3', type = 's3', ${s3_args});
INSERT INTO t_s3 VALUES (1), (2);
SELECT 's3 with default metadata path', count() FROM t_s3;

CREATE TABLE t_cache (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(type = cache, name = '${name}_cache', path = '${name}_cache', max_size = '10Mi', disk = 'local_disk');
INSERT INTO t_cache VALUES (1), (2);
SELECT 'cache over config disk', count() FROM t_cache;
"
