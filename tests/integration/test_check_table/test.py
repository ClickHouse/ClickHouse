import concurrent
import pytest
import uuid
import time

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.corrupt_part_data_on_disk import corrupt_part_data_on_disk
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def remove_checksums_on_disk(node, database, table, part_name):
    part_path = node.query(
        f"SELECT path FROM system.parts WHERE database = '{database}' AND table = '{table}' AND name = '{part_name}'"
    ).strip()
    node.exec_in_container(
        ["bash", "-c", "rm -r {p}/checksums.txt".format(p=part_path)], privileged=True
    )


def remove_part_from_disk(node, table, part_name):
    part_path = node.query(
        "SELECT path FROM system.parts WHERE table = '{}' and name = '{}'".format(
            table, part_name
        )
    ).strip()
    if not part_path:
        raise Exception("Part " + part_name + "doesn't exist")
    node.exec_in_container(
        ["bash", "-c", "rm -r {p}/*".format(p=part_path)], privileged=True
    )

# TODO: move to the end and fix https://github.com/ClickHouse/ClickHouse/issues/87916
def test_check_all_tables(started_cluster):
    def _create_table(database, table, engine=None):
        if engine is None:
            engine = "MergeTree() PARTITION BY toYYYYMM(date) ORDER BY id SETTINGS min_bytes_for_wide_part=0"

        node1.query(
            f"CREATE TABLE {database}.{table} (date Date, id UInt32, value Int32) ENGINE = {engine}"
        )
        node1.query(
            f"INSERT INTO {database}.{table} VALUES (toDate('2019-02-01'), 1, 10), (toDate('2019-02-01'), 2, 12)"
        )
        node1.query(f"SYSTEM STOP MERGES {database}.{table}")

    for database in ["db1", "db2", "db3"]:
        node1.query(f"CREATE DATABASE {database}")
        for table in ["table1", "table2"]:
            _create_table(database, table)

    remove_checksums_on_disk(node1, "db1", "table2", "201902_1_1_0")

    # These tables should not be checked
    _create_table("db1", "table_memory", "Memory()")
    _create_table("db1", "table_log", "TinyLog()")

    node1.query("SYSTEM ENABLE FAILPOINT check_table_query_delay_for_part")

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []

        check_future = executor.submit(
            lambda: node1.query(
                "CHECK ALL TABLES",
                settings={"check_query_single_value_result": 0, "max_threads": 16},
            )
        )

        futures.append(executor.submit(lambda: node1.query("DROP TABLE db3.table2")))
        futures.append(executor.submit(lambda: node1.query("DROP DATABASE db2")))

        for future in concurrent.futures.as_completed(futures):
            future.result()

        check_result = check_future.result()

    # Do not check databases created in other tests
    check_result = [
        res.split("\t")
        for res in check_result.split("\n")
        if res and not res.startswith("default")
    ]

    checked_tables = {f"{res[0]}.{res[1]}" for res in check_result}
    assert all(
        table in checked_tables for table in ["db1.table1", "db1.table2", "db3.table1"]
    ), checked_tables

    for check_result_row in check_result:
        assert len(check_result_row) == 5, check_result_row

        db, table, part, flag, message = check_result_row
        if db == "db1" and table == "table2" and part == "201902_1_1_0":
            assert flag == "1"
            assert message == "Checksums recounted and written to disk."
        else:
            assert flag == "1"
            assert message == ""

    for database in ["db1", "db3"]:
        node1.query(f"DROP DATABASE {database} SYNC")
    node1.query("SYSTEM DISABLE FAILPOINT check_table_query_delay_for_part")

@pytest.mark.parametrize("merge_tree_settings", [""])
def test_check_normal_table_corruption(started_cluster, merge_tree_settings):
    node1.query("DROP TABLE IF EXISTS non_replicated_mt")

    node1.query(
        f"""
        CREATE TABLE non_replicated_mt(date Date, id UInt32, value Int32)
        ENGINE = MergeTree() PARTITION BY toYYYYMM(date) ORDER BY id
        {merge_tree_settings};
    """
    )

    node1.query(
        "INSERT INTO non_replicated_mt VALUES (toDate('2019-02-01'), 1, 10), (toDate('2019-02-01'), 2, 12)"
    )
    assert (
        node1.query(
            "CHECK TABLE non_replicated_mt PARTITION 201902",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201902_1_1_0\t1\t\n"
    )

    remove_checksums_on_disk(node1, "default", "non_replicated_mt", "201902_1_1_0")

    assert (
        node1.query(
            "CHECK TABLE non_replicated_mt",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        ).strip()
        == "201902_1_1_0\t1\tChecksums recounted and written to disk."
    )

    assert node1.query("SELECT COUNT() FROM non_replicated_mt") == "2\n"

    remove_checksums_on_disk(node1, "default", "non_replicated_mt", "201902_1_1_0")

    assert (
        node1.query(
            "CHECK TABLE non_replicated_mt PARTITION 201902",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        ).strip()
        == "201902_1_1_0\t1\tChecksums recounted and written to disk."
    )

    assert node1.query("SELECT COUNT() FROM non_replicated_mt") == "2\n"

    corrupt_part_data_on_disk(
        node1, "non_replicated_mt", "201902_1_1_0", database="default"
    )

    assert node1.query(
        "CHECK TABLE non_replicated_mt",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ).strip().split("\t")[0:2] == ["201902_1_1_0", "0"]

    assert node1.query(
        "CHECK TABLE non_replicated_mt",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ).strip().split("\t")[0:2] == ["201902_1_1_0", "0"]

    node1.query(
        "INSERT INTO non_replicated_mt VALUES (toDate('2019-01-01'), 1, 10), (toDate('2019-01-01'), 2, 12)"
    )

    assert (
        node1.query(
            "CHECK TABLE non_replicated_mt PARTITION 201901",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201901_2_2_0\t1\t\n"
    )

    corrupt_part_data_on_disk(
        node1, "non_replicated_mt", "201901_2_2_0", database="default"
    )

    remove_checksums_on_disk(node1, "default", "non_replicated_mt", "201901_2_2_0")

    assert node1.query(
        "CHECK TABLE non_replicated_mt PARTITION 201901",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ).strip().split("\t")[0:2] == ["201901_2_2_0", "0"]


@pytest.mark.parametrize("merge_tree_settings, zk_path_suffix", [("", "_0")])
def test_check_replicated_table_simple(
    started_cluster, merge_tree_settings, zk_path_suffix
):
    for node in [node1, node2]:
        node.query("DROP TABLE IF EXISTS replicated_mt SYNC")

    for node in [node1, node2]:
        node.query(
            """
        CREATE TABLE replicated_mt(date Date, id UInt32, value Int32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_mt_{zk_path_suffix}', '{replica}')
        PARTITION BY toYYYYMM(date) ORDER BY id
        {merge_tree_settings}
            """.format(
                replica=node.name,
                zk_path_suffix=zk_path_suffix,
                merge_tree_settings=merge_tree_settings,
            )
        )

    node1.query(
        "INSERT INTO replicated_mt VALUES (toDate('2019-02-01'), 1, 10), (toDate('2019-02-01'), 2, 12)"
    )
    node2.query("SYSTEM SYNC REPLICA replicated_mt")

    assert node1.query("SELECT count() from replicated_mt") == "2\n"
    assert node2.query("SELECT count() from replicated_mt") == "2\n"

    assert (
        node1.query(
            "CHECK TABLE replicated_mt",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201902_0_0_0\t1\t\n"
    )
    assert (
        node2.query(
            "CHECK TABLE replicated_mt",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201902_0_0_0\t1\t\n"
    )

    node2.query(
        "INSERT INTO replicated_mt VALUES (toDate('2019-01-02'), 3, 10), (toDate('2019-01-02'), 4, 12)"
    )
    node1.query("SYSTEM SYNC REPLICA replicated_mt")
    assert node1.query("SELECT count() from replicated_mt") == "4\n"
    assert node2.query("SELECT count() from replicated_mt") == "4\n"

    assert (
        node1.query(
            "CHECK TABLE replicated_mt PARTITION 201901",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201901_0_0_0\t1\t\n"
    )
    assert (
        node2.query(
            "CHECK TABLE replicated_mt PARTITION 201901",
            settings={"check_query_single_value_result": 0, "max_threads": 1},
        )
        == "201901_0_0_0\t1\t\n"
    )

    assert sorted(
        node2.query(
            "CHECK TABLE replicated_mt",
            settings={"check_query_single_value_result": 0},
        ).split("\n")
    ) == ["", "201901_0_0_0\t1\t", "201902_0_0_0\t1\t"]

    with pytest.raises(QueryRuntimeException) as exc:
        node2.query(
            "CHECK TABLE replicated_mt PART '201801_0_0_0'",
            settings={"check_query_single_value_result": 0},
        )
    assert "NO_SUCH_DATA_PART" in str(exc.value)

    assert (
        node2.query(
            "CHECK TABLE replicated_mt PART '201902_0_0_0'",
            settings={"check_query_single_value_result": 0},
        )
        == "201902_0_0_0\t1\t\n"
    )


@pytest.mark.parametrize(
    "merge_tree_settings, zk_path_suffix, part_file_ext",
    [
        (
            "",
            "_0",
            ".bin",
        )
    ],
)
def test_check_replicated_table_corruption(
    started_cluster, merge_tree_settings, zk_path_suffix, part_file_ext
):
    for node in [node1, node2]:
        node.query_with_retry("DROP TABLE IF EXISTS replicated_mt_1 SYNC")

    for node in [node1, node2]:
        node.query_with_retry(
            """
        CREATE TABLE replicated_mt_1(date Date, id UInt32, value Int32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/replicated_mt_1_{zk_path_suffix}', '{replica}')
        PARTITION BY toYYYYMM(date) ORDER BY id
        {merge_tree_settings}
            """.format(
                replica=node.name,
                merge_tree_settings=merge_tree_settings,
                zk_path_suffix=zk_path_suffix,
            )
        )

    node1.query(
        "INSERT INTO replicated_mt_1 VALUES (toDate('2019-02-01'), 1, 10), (toDate('2019-02-01'), 2, 12)"
    )
    node1.query(
        "INSERT INTO replicated_mt_1 VALUES (toDate('2019-01-02'), 3, 10), (toDate('2019-01-02'), 4, 12)"
    )
    node2.query("SYSTEM SYNC REPLICA replicated_mt_1")

    assert node1.query("SELECT count() from replicated_mt_1") == "4\n"
    assert node2.query("SELECT count() from replicated_mt_1") == "4\n"

    part_name = node1.query_with_retry(
        "SELECT name from system.parts where table = 'replicated_mt_1' and partition_id = '201901' and active = 1"
    ).strip()

    corrupt_part_data_on_disk(
        node1, "replicated_mt_1", part_name, part_file_ext, database="default"
    )

    assert node1.query(
        "CHECK TABLE replicated_mt_1 PARTITION 201901",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ) == "{p}\t0\tPart {p} looks broken. Removing it and will try to fetch.\n".format(
        p=part_name
    )

    node1.query_with_retry("SYSTEM SYNC REPLICA replicated_mt_1")
    assert node1.query(
        "CHECK TABLE replicated_mt_1 PARTITION 201901",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ) == "{}\t1\t\n".format(part_name)
    assert node1.query("SELECT count() from replicated_mt_1") == "4\n"

    remove_part_from_disk(node2, "replicated_mt_1", part_name)
    assert node2.query(
        "CHECK TABLE replicated_mt_1 PARTITION 201901",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ) == "{p}\t0\tPart {p} looks broken. Removing it and will try to fetch.\n".format(
        p=part_name
    )

    node1.query("SYSTEM SYNC REPLICA replicated_mt_1")
    assert node1.query(
        "CHECK TABLE replicated_mt_1 PARTITION 201901",
        settings={"check_query_single_value_result": 0, "max_threads": 1},
    ) == "{}\t1\t\n".format(part_name)
    assert node1.query("SELECT count() from replicated_mt_1") == "4\n"

@pytest.mark.parametrize("engine", ["ReplicatedMergeTree"])
def test_check_replicated_does_not_block_shutdown(started_cluster, engine):
    part_count = 100
    table_name = f"replicated_check_{engine}"
    node1.query(
        f"""
            CREATE TABLE {table_name}(id UInt32, value Int32)
            ENGINE = {engine}('/clickhouse/tables/{{database}}/{table_name}', '{node1.name}')
            PARTITION BY id
            ORDER BY tuple()
            AS SELECT number, number FROM numbers({part_count})
        """
    )

    query_id = uuid.uuid4().hex

    def wait_check_table_and_restart_keeper():
        assert_eq_with_retry(
            node1,
            f"SELECT query_id FROM system.processes WHERE query_id = '{query_id}'",
            query_id,
        )

        cluster.kill_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
        err = node1.query_and_get_error(
            f"INSERT INTO {table_name} SELECT number, number FROM numbers(10) SETTINGS insert_keeper_max_retries=0"
        )
        assert err != ""
        cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])

        # Wait for keeper reconnection
        node1.query(
            f"INSERT INTO {table_name} SELECT number, number FROM numbers(10) SETTINGS insert_keeper_max_retries=100"
        )

    node1.query("SYSTEM ENABLE FAILPOINT check_table_query_delay_for_part")

    def run_check_table_and_measure_time():
        start_time = time.time()
        # Killing ZooKeeper mid-check makes the query abort quickly instead of scanning all parts.
        # Depending on which condition wins once the session is lost, the abort surfaces either as a
        # transient ZooKeeper error rethrown from the check (retryable, e.g. connection loss), or as
        # ABORTED once the replica's partial shutdown stops the check. Either way the query must fail:
        # the data is healthy, so a "0" (error) result would falsely report it as broken.
        # The point of this test is that CHECK TABLE does not block until every part is checked.
        try:
            result = node1.query(
                f"CHECK TABLE {table_name} SETTINGS max_threads=1, check_query_single_value_result = 1", query_id=query_id
            )
            assert False, f"CHECK TABLE was expected to fail, but returned: {result!r}"
        except QueryRuntimeException as e:
            assert (
                "KEEPER_EXCEPTION" in str(e) or "Coordination" in str(e) or "ABORTED" in str(e)
            ), str(e)
        end_time = time.time()
        avg_sleep = 0.5
        # The process should have stopped before checking all parts
        assert end_time - start_time < (part_count * avg_sleep)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = [
            executor.submit(run_check_table_and_measure_time),
            executor.submit(wait_check_table_and_restart_keeper),
        ]

        for future in concurrent.futures.as_completed(futures):
            future.result()

    node1.query("SYSTEM DISABLE FAILPOINT check_table_query_delay_for_part")
    node1.query(f"DROP TABLE {table_name} SYNC")


def test_check_cancelled_in_json_prefix_read_keeps_part(started_cluster):
    # A Wide part without columns_substreams.txt (written before that file existed) is checked by
    # reading its JSON structure prefix, where a cancelled query interrupts the check. That says nothing
    # about the data, so CHECK TABLE must fail without detaching the part and fetching it again.
    table_name = "replicated_json_check_cancelled"
    node1.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
    node1.query(
        f"""
        CREATE TABLE {table_name} (id UInt64, j JSON)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table_name}', '{node1.name}') ORDER BY id
        SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
        """
    )
    node1.query(
        f"INSERT INTO {table_name} SELECT number, toJSONString(map(concat('key_', toString(number)), number))::JSON FROM numbers(1000)"
    )
    part_path = node1.query(
        f"SELECT path FROM system.parts WHERE table = '{table_name}' AND active"
    ).strip()
    node1.query(f"DETACH TABLE {table_name}")
    node1.exec_in_container(
        ["bash", "-c", f"rm {part_path}/columns_substreams.txt"], privileged=True
    )
    node1.query(f"ATTACH TABLE {table_name}")

    # The failpoint sleeps for a random time under a second before the part check. The timeout leaves the
    # check time to start and usually cancels the query during the sleep, so the check meets the
    # cancellation at its first cancellable point, the structure prefix read. A short sleep lets it finish.
    node1.query("SYSTEM ENABLE FAILPOINT check_table_query_delay_for_part")
    try:
        for _ in range(10):
            answer, error = node1.query_and_get_answer_with_error(
                f"CHECK TABLE {table_name}",
                settings={
                    "max_threads": 1,
                    "max_execution_time": 0.1,
                    "check_query_single_value_result": 1,
                },
            )
            assert ("TIMEOUT_EXCEEDED" in error) if error else answer == "1\n", (
                answer,
                error,
            )
    finally:
        node1.query("SYSTEM DISABLE FAILPOINT check_table_query_delay_for_part")

    assert (
        node1.query(f"SELECT count() FROM system.detached_parts WHERE table = '{table_name}'")
        == "0\n"
    )
    assert node1.query(f"SELECT count() FROM {table_name}") == "1000\n"

    # Without a check that was interrupted before reaching a verdict, the assertions above hold on any build.
    logger = f"{table_name} (ReplicatedMergeTreePartCheckThread)"
    started = int(node1.count_in_log(f"{logger}: Checking data of part all_0_0_0"))
    finished = int(node1.count_in_log(f"{logger}: Part all_0_0_0 looks"))
    assert started > finished, (started, finished)

    node1.query(f"DROP TABLE {table_name} SYNC")


def test_cancelled_size_calculation_keeps_part_sizes(started_cluster):
    # The column sizes of a Wide part without columns_substreams.txt are calculated on first use by reading
    # each JSON structure prefix, where a cancelled query interrupts the calculation. A later calculation
    # must not count the columns read before the interruption twice.
    #
    # The failpoint sleeps before each part of a t_slowdown_system_parts* table, so the query is cancelled
    # before that part's sizes are calculated. The other table is read first in the same query because a
    # prefix read polls for cancellation only some time after the thread's first one.
    tables = ["json_sizes_first", "t_slowdown_system_parts_json_sizes"]
    for table, rows in zip(tables, [1, 1000]):
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node1.query(
            f"""
            CREATE TABLE {table} (k String, j JSON) ENGINE = MergeTree ORDER BY k
            SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0
            """
        )
        node1.query(
            f"INSERT INTO {table} SELECT toString(number), toJSONString(map(concat('key_', toString(number)), number))::JSON FROM numbers({rows})"
        )
        part_path = node1.query(
            f"SELECT path FROM system.parts WHERE table = '{table}' AND active"
        ).strip()
        node1.query(f"DETACH TABLE {table}")
        node1.exec_in_container(
            ["bash", "-c", f"rm {part_path}/columns_substreams.txt"], privileged=True
        )
        node1.query(f"ATTACH TABLE {table}")

    where = "WHERE database = currentDatabase() AND active AND table IN ({})"
    in_tables = where.format(", ".join(f"'{table}'" for table in tables))
    totals = f"SELECT table, data_compressed_bytes, data_uncompressed_bytes, marks_bytes FROM system.parts {in_tables} ORDER BY table"
    sleeps = "SELECT sum(value) FROM system.events WHERE event = 'SystemPartsEnumerationSlowdownSleeps'"
    sleeps_before = int(node1.query(sleeps))
    node1.query("SYSTEM ENABLE FAILPOINT slowdown_system_parts_enumeration")
    try:
        for _ in range(3):
            # Reattaching makes the sizes of the first table uncalculated again.
            node1.query(f"DETACH TABLE {tables[0]}")
            node1.query(f"ATTACH TABLE {tables[0]}")
            error = node1.query_and_get_error(
                totals, settings={"max_execution_time": 0.2}
            )
            assert "TIMEOUT_EXCEEDED" in error, error
    finally:
        node1.query("SYSTEM DISABLE FAILPOINT slowdown_system_parts_enumeration")

    # A query reached the sleep before the second table's part, and that part's sizes still had to be
    # calculated afterwards, so the calculation was interrupted rather than skipped or completed.
    assert int(node1.query(sleeps)) > sleeps_before
    query_id = uuid.uuid4().hex
    node1.query(
        f"SELECT data_compressed_bytes FROM system.parts {where.format(repr(tables[1]))}",
        query_id=query_id,
    )
    node1.query("SYSTEM FLUSH LOGS query_log")
    file_opens = node1.query(
        f"SELECT ProfileEvents['FileOpen'] FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
    )
    assert int(file_opens) > 0

    per_column = f"SELECT table, sum(column_data_compressed_bytes), sum(column_data_uncompressed_bytes), sum(column_marks_bytes) FROM system.parts_columns {in_tables} GROUP BY table ORDER BY table"
    assert node1.query(totals) == node1.query(per_column)

    for table in tables:
        node1.query(f"DROP TABLE {table} SYNC")
