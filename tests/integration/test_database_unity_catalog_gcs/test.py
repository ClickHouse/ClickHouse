import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake.writer import write_deltalake

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
MOCK_PORT = 8080
REMOTE_STORAGE_ROOT = "/var/lib/clickhouse/user_files/unity_gcs_storage"
DELETION_VECTOR = {
    "storageType": "i",
    "pathOrInlineDv": "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L",
    "sizeInBytes": 40,
    "cardinality": 1,
}

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/named_collections.xml"],
    user_configs=["configs/users.xml"],
    with_minio=True,
)


def copy_directory_to_container(local_root, container_id, remote_root):
    cluster.exec_in_container(container_id, ["mkdir", "-p", remote_root])
    for root, _, files in os.walk(local_root):
        relative_root = os.path.relpath(root, local_root)
        remote_directory = remote_root if relative_root == "." else os.path.join(remote_root, relative_root)
        cluster.exec_in_container(container_id, ["mkdir", "-p", remote_directory])
        for filename in files:
            cluster.copy_file_to_container(
                container_id,
                os.path.join(root, filename),
                os.path.join(remote_directory, filename),
            )


def write_table(local_root):
    write_deltalake(local_root, pa.table({"value": pa.array([1, 2, 3], type=pa.int64())}))
    with open(os.path.join(local_root, "_delta_log", "00000000000000000000.json")) as log:
        actions = [json.loads(line) for line in log if line.strip()]
    metadata = next(action["metaData"] for action in actions if "metaData" in action)
    add = next(action["add"] for action in actions if "add" in action)
    return metadata, add


def write_deletion_vector_commit(local_root, add_first=False):
    _, add = write_table(local_root)
    file_actions = [
        {"remove": {"path": add["path"], "deletionTimestamp": add["modificationTime"], "dataChange": True}},
        {"add": {**add, "deletionVector": DELETION_VECTOR}},
    ]
    if add_first:
        file_actions.reverse()
    actions = [
        {
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["deletionVectors"],
                "writerFeatures": ["deletionVectors"],
            }
        },
        *file_actions,
    ]
    with open(os.path.join(local_root, "_delta_log", "00000000000000000001.json"), "w") as log:
        log.write("\n".join(json.dumps(action) for action in actions) + "\n")


def write_column_mapping_table(local_root):
    write_table(local_root)
    log_path = os.path.join(local_root, "_delta_log", "00000000000000000000.json")
    with open(log_path) as log:
        actions = [json.loads(line) for line in log if line.strip()]
    for action in actions:
        if "metaData" in action:
            metadata = action["metaData"]
            schema = json.loads(metadata["schemaString"])
            for field in schema["fields"]:
                field["metadata"]["delta.columnMapping.id"] = 1
                field["metadata"]["delta.columnMapping.physicalName"] = "col-value"
            metadata["schemaString"] = json.dumps(schema)
            metadata["configuration"] = {**metadata.get("configuration", {}), "delta.columnMapping.mode": "name"}
    with open(log_path, "w") as log:
        log.write("\n".join(json.dumps(action) for action in actions) + "\n")


def write_deletion_vector_checkpoint(local_root):
    metadata, add = write_table(local_root)
    deletion_vector_type = pa.struct(
        [
            ("storageType", pa.string()),
            ("pathOrInlineDv", pa.string()),
            ("offset", pa.int32()),
            ("sizeInBytes", pa.int32()),
            ("cardinality", pa.int64()),
        ]
    )
    add_type = pa.struct(
        [
            ("path", pa.string()),
            ("partitionValues", pa.map_(pa.string(), pa.string())),
            ("size", pa.int64()),
            ("modificationTime", pa.int64()),
            ("dataChange", pa.bool_()),
            ("stats", pa.string()),
            ("deletionVector", deletion_vector_type),
        ]
    )
    metadata_type = pa.struct(
        [
            ("id", pa.string()),
            ("schemaString", pa.string()),
            ("partitionColumns", pa.list_(pa.string())),
            ("createdTime", pa.int64()),
        ]
    )
    checkpoint = pa.table(
        {
            "add": pa.array(
                [
                    None,
                    {
                        "path": add["path"],
                        "partitionValues": [],
                        "size": add["size"],
                        "modificationTime": add["modificationTime"],
                        "dataChange": False,
                        "stats": add.get("stats"),
                        "deletionVector": {**DELETION_VECTOR, "offset": None},
                    },
                ],
                type=add_type,
            ),
            "metaData": pa.array(
                [
                    {
                        "id": metadata["id"],
                        "schemaString": metadata["schemaString"],
                        "partitionColumns": metadata["partitionColumns"],
                        "createdTime": metadata.get("createdTime"),
                    },
                    None,
                ],
                type=metadata_type,
            ),
        }
    )
    log_directory = os.path.join(local_root, "_delta_log")
    pq.write_table(checkpoint, os.path.join(log_directory, "00000000000000000001.checkpoint.parquet"))
    with open(os.path.join(log_directory, "_last_checkpoint"), "w") as last_checkpoint:
        json.dump({"version": 1, "size": 2}, last_checkpoint)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        resolver_id = cluster.get_container_id("resolver")
        for name, writer in (
            ("table", write_table),
            ("dv_json", write_deletion_vector_commit),
            ("dv_json_add_first", lambda root: write_deletion_vector_commit(root, add_first=True)),
            ("dv_checkpoint", write_deletion_vector_checkpoint),
            ("column_mapping", write_column_mapping_table),
        ):
            local_table = os.path.join(cluster.instances_dir, f"unity_gcs_{name}")
            writer(local_table)
            copy_directory_to_container(local_table, resolver_id, os.path.join(REMOTE_STORAGE_ROOT, name))
        start_mock_servers(
            cluster,
            SCRIPT_DIR,
            [
                ("unity_gcs_mock.py", "resolver", str(MOCK_PORT), [REMOTE_STORAGE_ROOT]),
                ("gcp_oauth_mock.py", "resolver", "80"),
            ],
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_gcs_vended_credentials_fall_back_from_delta_kernel(started_cluster):
    node.query(
        f"""
        CREATE DATABASE unity
        ENGINE = DataLakeCatalog('http://resolver:{MOCK_PORT}/api/2.1/unity-catalog')
        SETTINGS warehouse = 'warehouse', catalog_credential = 'catalog-token', catalog_type = 'unity',
            vended_credentials = true, storage_endpoint = 'http://resolver:{MOCK_PORT}', storage_uri_style = 'path'
        """,
        settings={"allow_experimental_database_unity_catalog": 1},
    )

    has_delta_kernel = (
        node.query(
            "SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_DELTA_KERNEL_RS'"
        ).strip()
        == "1"
    )

    result = node.query(
        "SELECT value FROM unity.`namespace.table` ORDER BY value",
        settings={"allow_delta_kernel_rs": 1},
    )
    assert result == "1\n2\n3\n"
    if has_delta_kernel:
        assert node.contains_in_log(
            "Using the native Delta Lake metadata reader because Delta Kernel does not support bearer authentication"
        )

        error = node.query_and_get_error(
            "SELECT value FROM unity.`namespace.table`",
            settings={
                "allow_delta_kernel_rs": 1,
                "delta_lake_reload_schema_for_consistency": 1,
            },
        )
        assert "delta_lake_reload_schema_for_consistency) is not supported without Delta Kernel" in error


def test_gcp_oauth_falls_back_from_delta_kernel(started_cluster):
    result = node.query(
        "SELECT value FROM deltaLake(gcs_delta) ORDER BY value",
        settings={"allow_delta_kernel_rs": 1},
    )
    assert result == "1\n2\n3\n"


@pytest.mark.parametrize(
    "collection", ["gcs_delta_dv_json", "gcs_delta_dv_json_add_first", "gcs_delta_dv_checkpoint"]
)
def test_native_reader_rejects_deletion_vectors(started_cluster, collection):
    error = node.query_and_get_error(
        f"SELECT value FROM deltaLake({collection})",
        settings={"allow_delta_kernel_rs": 1},
    )
    assert "Reading Delta Lake files with deletion vectors is not supported without Delta Kernel" in error


def test_bearer_authentication_rejects_column_mapping(started_cluster):
    has_delta_kernel = (
        node.query(
            "SELECT value IN ('ON', '1') FROM system.build_options WHERE name = 'USE_DELTA_KERNEL_RS'"
        ).strip()
        == "1"
    )
    if not has_delta_kernel:
        pytest.skip("Requires Delta Kernel")

    error = node.query_and_get_error(
        "SELECT * FROM deltaLake(gcs_delta_column_mapping)",
        settings={"allow_delta_kernel_rs": 1},
    )
    assert "Reading Delta Lake tables with column mapping is not supported with bearer authentication" in error
