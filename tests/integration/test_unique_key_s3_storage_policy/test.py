import logging

import pytest

from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_policy.xml"],
    stay_alive=True,
    with_minio=True,
)

UK_SETTINGS = {"allow_experimental_unique_key": "1"}

EXPECTED_ROWS = "10\ta\n20\tb\n30\tc\n"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_unique_key_sst_roundtrip_on_s3(started_cluster):
    # UNIQUE KEY on S3: SST sidecar read/write through IDataPartStorage.
    # The SST reader/writer goes through IDataPartStorage::readFile/writeFile,
    # so it works on any disk type. This test verifies the round-trip on S3.
    node.query("DROP TABLE IF EXISTS uk_s3")
    node.query(
        """
        CREATE TABLE uk_s3 (id UInt64, v String)
        ENGINE = MergeTree
        UNIQUE KEY (id)
        ORDER BY (id)
        SETTINGS disk = 's3_disk', min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1
        """,
        settings=UK_SETTINGS,
    )
    node.query(
        "INSERT INTO uk_s3 VALUES (10, 'a'), (20, 'b'), (30, 'c')",
        settings=UK_SETTINGS,
    )
    assert node.query("SELECT id, v FROM uk_s3 ORDER BY id") == EXPECTED_ROWS

    # DETACH + ATTACH: load-time validation reads the SST sidecar back from
    # S3 through IDataPartStorage.
    node.query("DETACH TABLE uk_s3 SYNC")
    node.query("ATTACH TABLE uk_s3", settings=UK_SETTINGS)
    assert (
        node.query(
            """
            SELECT count() FROM system.parts
            WHERE database = currentDatabase() AND table = 'uk_s3' AND active
            """
        )
        == "1\n"
    )
    assert node.query("SELECT id, v FROM uk_s3 ORDER BY id") == EXPECTED_ROWS

    node.query("DROP TABLE uk_s3")


def test_unique_key_on_s3_survives_restart(started_cluster):
    # Restart the whole node: parts are re-loaded from S3 and the SST sidecar
    # is re-read on startup.
    node.query("DROP TABLE IF EXISTS uk_s3")
    node.query(
        """
        CREATE TABLE uk_s3 (id UInt64, v String)
        ENGINE = MergeTree
        UNIQUE KEY (id)
        ORDER BY (id)
        SETTINGS disk = 's3_disk', min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1
        """,
        settings=UK_SETTINGS,
    )
    node.query(
        "INSERT INTO uk_s3 VALUES (10, 'a'), (20, 'b'), (30, 'c')",
        settings=UK_SETTINGS,
    )

    node.restart_clickhouse()
    assert node.query("SELECT id, v FROM uk_s3 ORDER BY id") == EXPECTED_ROWS

    node.query("DROP TABLE uk_s3")


def test_unique_key_extend_policy_with_s3(started_cluster):
    # A local UNIQUE KEY table widened onto a policy that also contains an
    # S3 volume. `default_with_s3` keeps the `default` volume/disk of the old
    # policy, so the generic compatibility check passes.
    node.query("DROP TABLE IF EXISTS uk_local")
    node.query(
        """
        CREATE TABLE uk_local (id UInt64, v String)
        ENGINE = MergeTree
        UNIQUE KEY (id)
        ORDER BY (id)
        SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1
        """,
        settings=UK_SETTINGS,
    )
    node.query(
        "INSERT INTO uk_local VALUES (10, 'a'), (20, 'b'), (30, 'c')",
        settings=UK_SETTINGS,
    )

    node.query(
        "ALTER TABLE uk_local MODIFY SETTING storage_policy = 'default_with_s3'",
        settings=UK_SETTINGS,
    )

    # Existing data stays readable after the policy change.
    assert node.query("SELECT id, v FROM uk_local ORDER BY id") == EXPECTED_ROWS

    # New inserts keep working under the widened policy (parts go to the
    # first volume, which is still the local `default` disk).
    node.query("INSERT INTO uk_local VALUES (40, 'd')", settings=UK_SETTINGS)
    assert node.query("SELECT count() FROM uk_local") == "4\n"

    node.query("DROP TABLE uk_local")
