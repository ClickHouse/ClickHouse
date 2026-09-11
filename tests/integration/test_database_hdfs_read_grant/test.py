"""A `HDFS` database must not serve a table from its cache unchecked.

The cache is keyed on the table name alone, so an entry one user resolved is handed to every later
caller. Both the read source grant and the remote host filter therefore have to be checked above it.
The same shape was fixed for the `Filesystem` database in
https://github.com/ClickHouse/ClickHouse/issues/118042.
"""

import pytest

from helpers.cluster import ClickHouseCluster, is_arm
from helpers.config_manager import ConfigManager

if is_arm():
    pytestmark = pytest.mark.skip

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_hdfs=True, stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_hdfs_database_cache_is_checked(started_cluster):
    hdfs_api = started_cluster.hdfs_api
    for name in ("warm.tsv", "cold.tsv", "cold2.tsv"):
        hdfs_api.write_data("/" + name, "7\n")

    # The engine rejects a source that carries a path, so table names here are relative, which is the
    # form the `EXISTS TABLE` arms below need.
    node.query("CREATE DATABASE hdfs_db ENGINE = HDFS('hdfs://hdfs1:9000')")
    # The source grant is the variable under test, so the table grant is deliberately broad and cannot
    # confound the result. It also shows that it does not confer the source grant by itself.
    node.query("CREATE USER u")
    node.query("GRANT SELECT ON *.* TO u")

    # A user holding the grant resolves the name, which is what caches the storage for it.
    assert node.query("SELECT * FROM hdfs_db.`warm.tsv`") == "7\n"

    # `tryGetTable` reports a denied resolution as `UNKNOWN_TABLE` (code 60). What this asserts is that
    # the cached name is refused exactly like the name that was never resolved.
    error = node.query_and_get_error("SELECT * FROM hdfs_db.`warm.tsv`", user="u")
    assert "Code: 60." in error, error
    error = node.query_and_get_error("SELECT * FROM hdfs_db.`cold.tsv`", user="u")
    assert "Code: 60." in error, error

    # `EXISTS TABLE` needs only `SHOW TABLES`, so it must answer alike for a cached name and for one
    # that was never resolved, or it reports which names other users have read.
    assert node.query("EXISTS TABLE hdfs_db.`warm.tsv`", user="u") == "1\n"
    assert node.query("EXISTS TABLE hdfs_db.`never.tsv`", user="u") == "1\n"
    # A name that forms no usable URL still answers 0, and answers it to everyone: the two lines above
    # are a measurement, and this probe does not depend on the grant.
    assert node.query("EXISTS TABLE hdfs_db.`hdfs://hdfs1:9000`", user="u") == "0\n"
    assert node.query("EXISTS TABLE hdfs_db.`hdfs://hdfs1:9000`") == "0\n"

    node.query("GRANT READ ON HDFS TO u")

    # With the grant the cached name is served, while a name that was never resolved is not: only the
    # latter calls the table function, which also requires `CREATE TEMPORARY TABLE`. That contrast is
    # what shows the query above was answered from the cache and not by resolving the file again.
    assert node.query("SELECT * FROM hdfs_db.`warm.tsv`", user="u") == "7\n"
    error = node.query_and_get_error("SELECT * FROM hdfs_db.`cold2.tsv`", user="u")
    assert "Code: 60." in error, error
    node.query("GRANT CREATE TEMPORARY TABLE ON *.* TO u")
    assert node.query("SELECT * FROM hdfs_db.`cold2.tsv`", user="u") == "7\n"

    # A grant restricted by URL must keep working: the filter is matched against the URI the table
    # function reports, which for HDFS is the host of the table and not its path.
    node.query("CREATE USER f")
    node.query("GRANT SELECT ON *.* TO f")
    node.query("GRANT READ ON HDFS('hdfs://hdfs1:9000') TO f")
    assert node.query("SELECT * FROM hdfs_db.`warm.tsv`", user="f") == "7\n"

    # The host filter is re-read on `SYSTEM RELOAD CONFIG`, which keeps the cache, so a cached table
    # must stop being served once its host is no longer allowed. These run as the fully granted user,
    # so the grant cannot be the cause.
    with ConfigManager() as cm:
        cm.add_main_config(node, "configs/allowlist.xml")
        error = node.query_and_get_error("SELECT * FROM hdfs_db.`warm.tsv`")
        assert "Code: 60." in error, error
        assert node.query("EXISTS TABLE hdfs_db.`warm.tsv`") == "0\n"
        # A path that reports the refusal instead of masking it names the URL that was rejected.
        error = node.query_and_get_error("INSERT INTO hdfs_db.`warm.tsv` VALUES (1)")
        assert "UNACCEPTABLE_URL" in error, error
        # The name that is not in the cache is refused the same way, as it already was.
        error = node.query_and_get_error("SELECT * FROM hdfs_db.`cold.tsv`")
        assert "Code: 60." in error, error

    # Removing the file and reloading again restores the answer, so the refusals above came from the
    # filter and not from an evicted or poisoned cache entry.
    assert node.query("SELECT * FROM hdfs_db.`warm.tsv`") == "7\n"

    node.query("DROP DATABASE hdfs_db SYNC")
    node.query("DROP USER u, f")
