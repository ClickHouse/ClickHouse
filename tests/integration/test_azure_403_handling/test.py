#!/usr/bin/env python3
"""Regression tests: Azure 403 is retried and never misreported as POTENTIALLY_BROKEN_DATA_PART."""
import os

import pytest

from helpers.cluster import ClickHouseCluster


AZURITE_ACCOUNT = "devstoreaccount1"
AZURITE_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
CONTAINER = "cont"

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[os.path.join(SCRIPT_DIR, "configs", "azure_disk.xml")],
    with_azurite=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True, scope="function")
def reset_failpoint_between_tests(started_cluster):
    try:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")
    except Exception:
        pass
    yield
    try:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")
    except Exception:
        pass


def _create_table(endpoint, name, blob):
    node.query(
        f"""
        CREATE TABLE {name} (k UInt64, v String)
        ENGINE = AzureBlobStorage(
            '{endpoint}', '{CONTAINER}', '{blob}',
            '{AZURITE_ACCOUNT}', '{AZURITE_KEY}', 'CSV'
        )
        """
    )


def _drop_caches():
    node.query("SYSTEM DROP FILESYSTEM CACHE")
    node.query("SYSTEM DROP MARK CACHE")


def test_sanity_check(started_cluster):
    endpoint = started_cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    _create_table(endpoint, "t", "basic.csv")
    node.query("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")

    assert node.query("SELECT count() FROM t").strip() == "3"
    assert node.query("SELECT v FROM t WHERE k = 2").strip() == "b"


def test_azure_403_is_retried_not_broken_part(started_cluster):
    endpoint = started_cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    _create_table(endpoint, "t_403", "failpoint.csv")
    node.query("INSERT INTO t_403 VALUES (1, 'a'), (2, 'b')")

    assert node.query("SELECT count() FROM t_403").strip() == "2"

    _drop_caches()

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        err = node.query_and_get_error("SELECT count() FROM t_403")
        assert "403" in err or "Forbidden" in err, f"expected 403 error, got:\n{err}"
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")


def test_azure_403_at_merge_not_broken_part(started_cluster):
    node.query(
        """
        CREATE TABLE t_merge (k UInt64, v String)
        ENGINE = MergeTree() ORDER BY k
        SETTINGS storage_policy = 'azure_policy', min_bytes_for_wide_part = 0
        """
    )
    for i in range(3):
        node.query(
            f"INSERT INTO t_merge SELECT number + {i * 100}, toString(number) FROM numbers(100)"
        )
    assert node.query("SELECT count() FROM t_merge").strip() == "300"

    _drop_caches()

    node.query("SYSTEM ENABLE FAILPOINT azure_inject_forbidden_response")
    try:
        err = node.query_and_get_error("OPTIMIZE TABLE t_merge FINAL")
        assert "POTENTIALLY_BROKEN_DATA_PART" not in err, (
            f"unexpected broken-part error:\n{err}"
        )
    finally:
        node.query("SYSTEM DISABLE FAILPOINT azure_inject_forbidden_response")
