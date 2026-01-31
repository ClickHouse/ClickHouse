#!/usr/bin/env bash
# Tags: long, no-parallel, no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: plain rewritable should not be shared between replicas
# Tag no-parallel: self-concurrency in flaky check (pre-configured RW disk cannot be
#                  shared across the tables from different test instances)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This test uses table_disk = 1, so data paths remain the same after
# table recreation. In flaky check, when the test is running multiple
# times on the same server with different MT settings, this may lead
# to reading cached files from the previous run, and attempting to
# deserialize them according to the current run settings, and results
# in unexpected query results / LOGICAL_ERRORs.
#
# So, uncompressed cache usage is disabled for the whole test.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --use_uncompressed_cache 0"

function wait_background_refresh()
{
    local in_sync

    for _ in {0..100}; do
        in_sync=$(
            ${CLICKHOUSE_CLIENT} -q "
                SELECT count() > 1 AND groupArraySortedIf(10)(name, table = '03787_writer') = groupArraySortedIf(10)(name, table = '03787_reader')
                FROM system.parts
                WHERE database = currentDatabase()
                  AND table IN ('03787_writer', '03787_reader')
                  AND active = 1"
        )
        if [[ "$in_sync" == "1" ]]; then
            return
        fi

        sleep 0.2
    done

    echo "Failed to wait for reader table sync" >&2
    return 1
}

# reader table recreated with different disks, since readonly disk keeps metadata after table drop
${CLICKHOUSE_CLIENT} -nm -q "
    DROP TABLE IF EXISTS 03787_writer SYNC;
    DROP TABLE IF EXISTS 03787_reader SYNC;

    CREATE TABLE 03787_writer (key Int32, val String) ENGINE = MergeTree() ORDER BY key
    SETTINGS table_disk = 1, disk = 'disk_s3_plain_rewritable_03787_writer';

    CREATE TABLE 03787_reader (key Int32, val String) ENGINE = MergeTree() ORDER BY key
    SETTINGS table_disk = 1,
             refresh_parts_interval = 1,
             disk = disk(
                readonly = true,
                name = disk_s3_plain_rewritable_03787_reader_${CLICKHOUSE_DATABASE}_init,
                type = s3_plain_rewritable,
                endpoint = 'http://localhost:11111/test/test_03787_read_write/',
                access_key_id = clickhouse,
                secret_access_key = clickhouse
            );
"

${CLICKHOUSE_CLIENT} -q "INSERT INTO 03787_writer SELECT number, 'val-' || number FROM numbers(5);"

wait_background_refresh || exit 1

echo "# Initial data on writer"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM 03787_writer ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;"

echo "# Initial data on reader"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM 03787_reader ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;"

${CLICKHOUSE_CLIENT} -nm -q "
    ALTER TABLE 03787_writer ADD COLUMN dt Date SETTINGS alter_sync = 2;
    ALTER TABLE 03787_writer UPDATE dt = toDate('2025-01-01') + key WHERE 1 = 1 SETTINGS mutations_sync = 2;
"

wait_background_refresh || exit 1

echo "# Data with new column on writer"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM 03787_writer ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;"

echo "# Data with new column on reader before recreate"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM 03787_reader ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;"

${CLICKHOUSE_CLIENT} -nm -q "
    DROP TABLE IF EXISTS 03787_reader SYNC;

    CREATE TABLE 03787_reader (key Int32, val String, dt Date) ENGINE = MergeTree() ORDER BY key
    SETTINGS table_disk = 1,
             refresh_parts_interval = 1,
             disk = disk(
                readonly = true,
                name = disk_s3_plain_rewritable_03787_reader_${CLICKHOUSE_DATABASE}_mutation,
                type = s3_plain_rewritable,
                endpoint = 'http://localhost:11111/test/test_03787_read_write/',
                access_key_id = clickhouse,
                secret_access_key = clickhouse
            );
"

echo "# Data with new column on reader after recreate"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM 03787_reader ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;"

${CLICKHOUSE_CLIENT} -nm -q "ALTER TABLE 03787_writer DROP COLUMN dt SETTINGS alter_sync = 2;"

wait_background_refresh || exit 1

echo "# Data without column on writer"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM 03787_writer ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;"

echo "# Data without column on reader before recreate"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM 03787_reader ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;"

${CLICKHOUSE_CLIENT} -nm -q "
    DROP TABLE IF EXISTS 03787_reader SYNC;

    CREATE TABLE 03787_reader (key Int32, val String) ENGINE = MergeTree() ORDER BY key
    SETTINGS table_disk = 1,
             refresh_parts_interval = 1,
             disk = disk(
                readonly = true,
                name = disk_s3_plain_rewritable_03787_reader_${CLICKHOUSE_DATABASE}_drop,
                type = s3_plain_rewritable,
                endpoint = 'http://localhost:11111/test/test_03787_read_write/',
                access_key_id = clickhouse,
                secret_access_key = clickhouse
            );
"

echo "# Data with new column on reader after recreate"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM 03787_reader ORDER BY key FORMAT TabSeparatedWithNamesAndTypes;"

${CLICKHOUSE_CLIENT} -nm -q "
    DROP TABLE IF EXISTS 03787_writer SYNC;
    DROP TABLE IF EXISTS 03787_reader SYNC;
"
