import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
import os.path


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('instance', stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_version_1():
    node.stop_clickhouse()
    node.clear_dir_in_container("/var/lib/clickhouse/access")
    node.copy_dir_to_container(os.path.join(SCRIPT_DIR, "version_1"), "/var/lib/clickhouse/access")
    node.start_clickhouse()

    assert node.query("SELECT name, select_filter, kind FROM system.row_policies ORDER BY name") == TSV([['p0 ON mydb.mytable', 'a = 0', 'permissive'],
                                                                                                         ['p1 ON mydb.mytable', 'a = 1000', 'permissive'],
                                                                                                         ['p2 ON mydb.mytable', 'a = 2000', 'restrictive'],
                                                                                                         ['p3 ON mydb.mytable', 'a = 3000', 'simple']])

    u0_id = node.query("SELECT id FROM system.users WHERE name='u0'").rstrip()
    p0_id = node.query("SELECT id FROM system.row_policies WHERE name='p0 ON mydb.mytable'").rstrip()

    assert node.read_file(os.path.join("/var/lib/clickhouse/access", p0_id + ".sql")) == \
        f"ATTACH ROW POLICY p0 ON mydb.mytable FOR SELECT USING a = 0 TO ID('{u0_id}');\n"

    node.query("ALTER ROW POLICY p0 ON mydb.mytable FOR SELECT USING a=1")
    assert node.read_file(os.path.join("/var/lib/clickhouse/access", p0_id + ".sql")) == \
        f"SET rbac_version = 2;\n"\
        f"ATTACH ROW POLICY p0 ON mydb.mytable FOR SELECT USING a = 1 AS permissive TO ID('{u0_id}');\n"

    node.query("CREATE ROW POLICY p4 ON mydb.mytable FOR SELECT USING a=4000")
    p4_id = node.query("SELECT id FROM system.row_policies WHERE name='p4 ON mydb.mytable'").rstrip()
    assert node.read_file(os.path.join("/var/lib/clickhouse/access", p4_id + ".sql")) == \
        f"SET rbac_version = 2;\n"\
        f"ATTACH ROW POLICY p4 ON mydb.mytable FOR SELECT USING a = 4000 AS permissive;\n"
