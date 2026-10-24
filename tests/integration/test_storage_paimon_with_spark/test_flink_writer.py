"""Live Flink writer feeding Keeper-backed incremental reads.

Flink is the dominant real-world Paimon writer. The static fixture
(test_reader_compat.py) proves ClickHouse reads Flink-written metadata; this
module drives a *live* Flink SQL client commit-by-commit, so the incremental
consumer sees Flink snapshots arrive over time — the streaming half of the
Flink journey that a checked-in fixture cannot exercise. The CDC-into-
primary-key half remains blocked on
https://github.com/ClickHouse/ClickHouse/issues/113360.
"""

import os
import shutil
import time

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check
from helpers.paimon_utils import (
    create_paimon_table,
    drop_tables_at_exit,
    get_uuid_str,
    id_array_str,
    incremental_settings,
    upload_paimon_table,
    wait_until_query_result,
)
from helpers.s3_tools import prepare_s3_bucket

FLINK_WAREHOUSE = "/tmp/flink_wh"


@pytest.fixture(scope="module")
def started_cluster_paimon_flink():
    try:
        cluster = ClickHouseCluster(__file__, name="paimon-flink")
        cluster.base_cmd.extend(
            [
                "--file",
                os.path.join(
                    get_docker_compose_path(), "docker_compose_flink_writer.yml"
                ),
            ]
        )
        cluster.add_instance(
            "node1",
            main_configs=["configs/config.d/named_collections.xml"],
            user_configs=["configs/users.d/users.xml"],
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.start()
        prepare_s3_bucket(cluster)

        from helpers.s3_tools import S3Uploader

        cluster.default_s3_uploader = S3Uploader(
            cluster.minio_client, cluster.minio_bucket
        )

        # The sidecar downloads the Paimon Flink jars at start and leaves a
        # ready or failure marker. Only a startup failure with Maven genuinely
        # unreachable is skippable (an outage); anything else — broken compose,
        # wrong jar coordinates, start-cluster failure — must fail loudly.
        flink = cluster.get_instance_docker_id("flink-writer")
        deadline = time.monotonic() + 120
        while True:
            markers = run_and_check(
                [
                    "docker",
                    "exec",
                    flink,
                    "bash",
                    "-c",
                    "ls /tmp/flink_ready /tmp/flink_failed 2>/dev/null; true",
                ],
                nothrow=True,
            )
            if "flink_ready" in markers:
                break
            timed_out = time.monotonic() > deadline
            if "flink_failed" in markers or timed_out:
                # Probe Maven from inside the container: the downloads run
                # there, so a container-only DNS/egress failure must classify
                # as an outage (skip), which a host-side probe cannot see.
                # An inconclusive probe (docker exec itself failing) is not
                # treated as an outage: fail loudly below.
                probe = run_and_check(
                    [
                        "docker",
                        "exec",
                        flink,
                        "bash",
                        "-c",
                        "curl -sfI --max-time 10 https://repo1.maven.org"
                        " >/dev/null 2>&1 && echo MAVEN_OK || echo MAVEN_FAIL",
                    ],
                    nothrow=True,
                )
                if "MAVEN_FAIL" in probe:
                    pytest.skip(
                        "Flink sidecar failed to start: Maven unreachable "
                        "from inside the container"
                    )
                logs = run_and_check(
                    ["docker", "logs", "--tail", "30", flink], nothrow=True
                )
                pytest.fail(
                    f"Flink sidecar failed to start (Maven is reachable from "
                    f"inside the container, so this is not an outage; probe: "
                    f"{probe!r}); container log tail:\n{logs}"
                )
            time.sleep(3)
        yield cluster
    finally:
        cluster.shutdown()


def _flink_sql(started_cluster, statements):
    """Run statements in a fresh Flink SQL client session (catalogs are
    session-scoped, so every batch re-declares the Paimon catalog)."""
    flink = started_cluster.get_instance_docker_id("flink-writer")
    script = (
        "SET 'table.dml-sync' = 'true';\n"
        f"CREATE CATALOG p WITH ('type' = 'paimon', 'warehouse' = 'file:{FLINK_WAREHOUSE}');\n"
        "USE CATALOG p;\n" + statements
    )
    run_and_check(
        [
            "docker",
            "exec",
            flink,
            "bash",
            "-c",
            "cat > /tmp/stmt.sql << 'FLINKSQL'\n" + script + "\nFLINKSQL",
        ]
    )
    run_and_check(
        ["docker", "exec", flink, "/opt/flink/bin/sql-client.sh", "-f", "/tmp/stmt.sql"]
    )


def _pull_and_upload(started_cluster, table_name):
    """Copy the table out of the Flink container into the standard local
    warehouse path, then upload in commit order."""
    flink = started_cluster.get_instance_docker_id("flink-writer")
    local_dir = f"/var/lib/clickhouse/user_files/paimon_data/default.db/{table_name}"
    shutil.rmtree(local_dir, ignore_errors=True)
    os.makedirs(os.path.dirname(local_dir), exist_ok=True)
    run_and_check(
        ["docker", "cp", f"{flink}:{FLINK_WAREHOUSE}/default.db/{table_name}", local_dir]
    )
    upload_paimon_table(started_cluster, "s3", table_name)


def test_flink_live_incremental(started_cluster_paimon_flink):
    started_cluster = started_cluster_paimon_flink
    instance = started_cluster.instances["node1"]
    table_name = "flink_live_" + get_uuid_str()

    _flink_sql(
        started_cluster,
        f"CREATE TABLE {table_name} (id INT NOT NULL, val STRING) "
        f"WITH ('file.format' = 'parquet');\n"
        f"INSERT INTO {table_name} VALUES (0, 'warmup');",
    )
    _pull_and_upload(started_cluster, table_name)

    create_paimon_table(
        "s3",
        instance,
        table_name,
        started_cluster,
        additional_settings=incremental_settings(table_name),
    )
    with drop_tables_at_exit(instance, table_name):
        count_query = f"SELECT count() FROM {table_name}"
        ids_query = f"SELECT arraySort(groupArray(id)) FROM {table_name}"
        wait_until_query_result(instance, count_query, "1\n")
        wait_until_query_result(instance, count_query, "0\n")

        # Two live Flink commits, consumed incrementally and exactly once.
        _flink_sql(
            started_cluster,
            f"INSERT INTO {table_name} VALUES "
            + ", ".join(f"({i}, 'x')" for i in range(1, 6))
            + ";",
        )
        _pull_and_upload(started_cluster, table_name)
        wait_until_query_result(instance, ids_query, id_array_str(range(1, 6)))
        wait_until_query_result(instance, count_query, "0\n")

        _flink_sql(
            started_cluster,
            f"INSERT INTO {table_name} VALUES "
            + ", ".join(f"({i}, 'x')" for i in range(6, 11))
            + ";",
        )
        _pull_and_upload(started_cluster, table_name)
        wait_until_query_result(instance, ids_query, id_array_str(range(6, 11)))
        wait_until_query_result(instance, count_query, "0\n")
