# coding: utf-8

import os
import time

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

DOCKER_COMPOSE_PATH = get_docker_compose_path()

INCREMENTAL_WRITER_JAR = "/opt/paimon/paimon-incremental-writer.jar"
CLICKHOUSE_WORKDIR = "/var/lib/clickhouse"
USER_FILES_PATH = f"{CLICKHOUSE_WORKDIR}/user_files"
PAIMON_WAREHOUSE_URI = f"file://{USER_FILES_PATH}/warehouse/"
PAIMON_TABLE_PATH = f"{USER_FILES_PATH}/warehouse/test.db/test_table"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=["configs/config.xml"],
)

cluster.base_cmd.extend(
    ["--file", os.path.join(DOCKER_COMPOSE_PATH, "docker_compose_paimon_incremental_writer.yml")]
)


def wait_for_container(cluster, service_name, timeout=60):
    docker_id = cluster.get_instance_docker_id(service_name)
    container = cluster.get_docker_handle(docker_id)
    start = time.time()
    while time.time() - start < timeout:
        info = container.client.api.inspect_container(container.name)
        if info["State"]["Running"]:
            return
        time.sleep(1)
    raise Exception(f"Container {service_name} did not start in {timeout}s")


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        wait_for_container(cluster, "paimon-incremental-writer")
        yield cluster
    finally:
        cluster.shutdown()


def _run_writer(
    container_id: str,
    *,
    start_id: int,
    rows_per_commit: int,
    commit_times: int,
) -> None:
    writer_cmd = (
        f"java -jar {INCREMENTAL_WRITER_JAR} "
        f'"{PAIMON_WAREHOUSE_URI}" "test" "test_table" "{start_id}" "{rows_per_commit}" "{commit_times}"'
    )
    run_and_check(
        [f"docker exec {container_id} bash -c '{writer_cmd}'"],
        shell=True,
    )


def _clean_warehouse(container_id: str):
    run_and_check(
        [f'docker exec {container_id} bash -c "rm -rf {USER_FILES_PATH}/warehouse"'],
        shell=True,
    )


def _get_system_event_value(event_name: str) -> int:
    return int(
        node.query(
            "SELECT value FROM system.events WHERE event = '{}'".format(event_name)
        ).strip()
        or "0"
    )


def _get_system_metric_value(metric_name: str) -> int:
    return int(
        node.query(
            "SELECT value FROM system.metrics WHERE metric = '{}'".format(metric_name)
        ).strip()
        or "0"
    )


def test_paimon_metadata_files_cache(started_cluster):
    writer_container_id = cluster.get_instance_docker_id("paimon-incremental-writer")

    _clean_warehouse(writer_container_id)

    # Produce one snapshot so that Paimon table has data and metadata files.
    _run_writer(writer_container_id, start_id=0, rows_per_commit=3, commit_times=1)

    table_name = "paimon_metadata_files_cache_test"
    create_query = "CREATE TABLE {table_name} ENGINE = PaimonLocal('{table_path}')".format(
        table_name=table_name, table_path=PAIMON_TABLE_PATH
    )
    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC;")

    # Cache disabled: events should not increase.
    node.query(
        create_query,
        settings={
            "allow_experimental_paimon_storage_engine": 1,
            "use_paimon_metadata_files_cache": 0,
        },
    )
    hits_before = _get_system_event_value("PaimonMetadataFilesCacheHits")
    misses_before = _get_system_event_value("PaimonMetadataFilesCacheMisses")
    node.query(
        f"SELECT count() FROM {table_name} FORMAT Null",
        settings={"use_paimon_metadata_files_cache": 0},
    )
    node.query(
        f"SELECT count() FROM {table_name} FORMAT Null",
        settings={"use_paimon_metadata_files_cache": 0},
    )
    hits_after = _get_system_event_value("PaimonMetadataFilesCacheHits")
    misses_after = _get_system_event_value("PaimonMetadataFilesCacheMisses")
    assert hits_after - hits_before == 0
    assert misses_after - misses_before == 0
    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC;")

    # Cache enabled: first query should miss, second query should hit.
    node.query(
        create_query,
        settings={
            "allow_experimental_paimon_storage_engine": 1,
            "use_paimon_metadata_files_cache": 1,
        },
    )
    hits_before = _get_system_event_value("PaimonMetadataFilesCacheHits")
    misses_before = _get_system_event_value("PaimonMetadataFilesCacheMisses")
    node.query(
        f"SELECT count() FROM {table_name} FORMAT Null",
        settings={"use_paimon_metadata_files_cache": 1},
    )
    node.query(
        f"SELECT count() FROM {table_name} FORMAT Null",
        settings={"use_paimon_metadata_files_cache": 1},
    )
    hits_after = _get_system_event_value("PaimonMetadataFilesCacheHits")
    misses_after = _get_system_event_value("PaimonMetadataFilesCacheMisses")

    assert hits_after - hits_before > 0
    assert misses_after - misses_before > 0
    assert _get_system_metric_value("PaimonMetadataFilesCacheFiles") > 0
    assert _get_system_metric_value("PaimonMetadataFilesCacheBytes") > 0

    node.query(f"DROP TABLE IF EXISTS {table_name} SYNC;")
