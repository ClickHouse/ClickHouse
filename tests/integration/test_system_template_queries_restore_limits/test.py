import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/backups_disk.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def make_backup(name):
    node.query(
        f"INSERT INTO system.template_queries (name, category, description, query) VALUES ('{name}_seed', 'test', '', 'SELECT 1')"
    )
    node.query(f"BACKUP TABLE system.template_queries TO File('/backups/{name}') FORMAT Null")
    node.query(f"DELETE FROM system.template_queries WHERE name = '{name}_seed'")
    return node.exec_in_container(
        ["bash", "-c", f"find /backups/{name} -name template_queries.tsv"]
    ).strip()


def restore_error(name):
    return node.query_and_get_error(
        f"RESTORE TABLE system.template_queries FROM File('/backups/{name}') FORMAT Null"
    )


def test_restore_stops_at_row_limit(started_cluster):
    name = "rows_limit"
    path = make_backup(name)
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"awk 'BEGIN {{ for (i = 0; i <= 10000; ++i) printf \"{name}_%d\\ttest\\t\\tSELECT 1\\t0\\n\", i; print \"malformed\" }}' > {path}",
        ]
    )
    assert "Too many user template queries" in restore_error(name)
    assert node.query(f"SELECT count() FROM system.template_queries WHERE startsWith(name, '{name}')") == "0\n"


def test_restore_refuses_oversized_field(started_cluster):
    name = "field_limit"
    path = make_backup(name)
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"awk 'BEGIN {{ q = \"x\"; for (i = 0; i < 23; ++i) q = q q; printf \"{name}_big\\ttest\\t\\t%s\\t0\\n\", q }}' > {path}",
        ]
    )
    assert "A template query field is longer than the maximum of 1048576 bytes" in restore_error(name)
    assert node.query(f"SELECT count() FROM system.template_queries WHERE startsWith(name, '{name}')") == "0\n"
