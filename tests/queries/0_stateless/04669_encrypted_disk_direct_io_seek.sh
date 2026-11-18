#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: depends on OpenSSL
# Tag no-object-storage: O_DIRECT applies to local disks only
# Tag no-replicated-database, no-shared-merge-tree: custom disk

# Reading a Compact part from an `encrypted` disk with O_DIRECT used to fail with
# "ReadBufferFromEncryptedFile: Wrong file position ... in the inner buffer", because
# `ReadBufferFromFileDescriptor::seek` returned the offset rounded down to the O_DIRECT
# alignment instead of the position the buffer was left at.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiline -q """
DROP TABLE IF EXISTS t_encrypted_direct_io SYNC;

CREATE TABLE t_encrypted_direct_io (id UInt64, a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS disk = disk(
    type = encrypted,
    disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_DATABASE}_encrypted_direct_io/'),
    algorithm = 'AES_128_CTR',
    key_hex = '00112233445566778899aabbccddeeff'),
    min_bytes_for_wide_part = '1G';

-- A single Compact part large enough that reading the second column has to seek to an
-- offset that is not a multiple of the O_DIRECT alignment.
INSERT INTO t_encrypted_direct_io SELECT number, number, sipHash64(number) FROM numbers(200000);

SELECT count(), sum(a), sum(b) FROM t_encrypted_direct_io
SETTINGS min_bytes_to_use_direct_io = 1, max_threads = 1;

DROP TABLE t_encrypted_direct_io SYNC;
"""
