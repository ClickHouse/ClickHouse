import random
import string
import uuid
from typing import Any, Dict

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain
from helpers.database_disk import get_database_disk_name, replace_text_in_metadata


cluster = ClickHouseCluster(__file__)


node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml", "configs/database_replicated_settings.xml"],
    user_configs=["configs/users.xml"],
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "1"},
    stay_alive=True,
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": "shard1", "replica": "2"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def get_random_string(string_length=8):
    alphabet = string.ascii_letters + string.digits
    return "".join((random.choice(alphabet) for _ in range(string_length)))

def convert_setting(key : str, value: str):
    match key:
        case "node_with_database_replicated_settings" | "check_consistency":
            if value.lower() == "false":
                value = False
            elif value.lower() == "true":
                value = True
            else:
                raise Exception(f"Invalid settings {key}: {value}")
            return key, value
        case "max_broken_tables_ratio":
            value = float(value)
            return key, value
        case (
            "max_replication_lag_to_enqueue"
            | "wait_entry_commited_timeout_sec"
            | "max_retries_before_automatic_recovery"
            | "logs_to_keep"
        ):
            value = int(value)
            return key, value
        case "collection_name":
            return key, value

    raise Exception(f"Unknown settings {key}: {value}")

def get_settings_from_logs(node, db_name) ->Dict[str, Any]:
    node.query("SYSTEM FLUSH LOGS")
    result = str(node.query(
        f"SELECT message FROM system.text_log WHERE logger_name='DatabaseReplicated ({db_name})' AND position(message, 'DatabaseReplicatedSettings') = 1"
    ).strip())
    assert result.startswith("DatabaseReplicatedSettings")
    settings = result.removeprefix("DatabaseReplicatedSettings").strip()

    if settings == "":
        return {}

    config_dict = {}
    # Split into key-value pairs
    pairs = settings.split(",")
    for pair in pairs:
        key, value = pair.strip().split("=", 1)
        key = key.strip()
        value = value.strip()
        if(value.startswith(r"\'")):
            value = value.strip(r"\'")

        key, value = convert_setting(key, value)
        config_dict[key] = value

    return config_dict


@pytest.mark.parametrize("node_with_database_replicated_settings", [True, False])
@pytest.mark.parametrize("max_broken_tables_ratio", [None, 0.5])
@pytest.mark.parametrize("max_replication_lag_to_enqueue", [None, 10])
@pytest.mark.parametrize("wait_entry_commited_timeout_sec", [None, 900])
@pytest.mark.parametrize("collection_name", [None, "postgres2"])
@pytest.mark.parametrize("check_consistency", [None, True, False])
@pytest.mark.parametrize("max_retries_before_automatic_recovery", [None, 1])
@pytest.mark.parametrize("logs_to_keep", [None, 100])
def test_database_replicated_settings(
    started_cluster,
    node_with_database_replicated_settings: bool,  # default config from database_replicated_settings.xml
    max_broken_tables_ratio,
    max_replication_lag_to_enqueue,
    wait_entry_commited_timeout_sec,
    collection_name,
    check_consistency,
    max_retries_before_automatic_recovery,
    logs_to_keep,
):
    db_name = "test_" + get_random_string()

    node = node1 if node_with_database_replicated_settings else node2

    node.query(f"DROP DATABASE IF EXISTS {db_name}")

    settings_in_query = ""
    setting_dict : Dict[str, Any]= {}

    def add_setting(key, val, default):
        nonlocal settings_in_query
        nonlocal setting_dict
        nonlocal node_with_database_replicated_settings
        if val == None:
            if node_with_database_replicated_settings:
                setting_dict[key] = default
            return

        setting_dict[key] = val

        if isinstance(val, str):
            val = f"'{val}'"
        if settings_in_query != "":
            settings_in_query += f", {key}={val}"
        else :
            settings_in_query += f"{key}={val}"

    add_setting("max_broken_tables_ratio", max_broken_tables_ratio, float(0.75) if node_with_database_replicated_settings else float(1.0))
    add_setting("max_replication_lag_to_enqueue", max_replication_lag_to_enqueue, int(100) if node_with_database_replicated_settings else int(50))
    add_setting("wait_entry_commited_timeout_sec", wait_entry_commited_timeout_sec, int(1800) if node_with_database_replicated_settings else int(3600))
    add_setting("collection_name", collection_name, "postgres1" if node_with_database_replicated_settings else "")
    add_setting("check_consistency", check_consistency, False if node_with_database_replicated_settings else True)
    add_setting(
        "max_retries_before_automatic_recovery", max_retries_before_automatic_recovery, int(5) if node_with_database_replicated_settings else int(10)
    )
    add_setting(
        "logs_to_keep",
        logs_to_keep,
        int(200) if node_with_database_replicated_settings else int(1000),
    )

    if settings_in_query != "":
        settings_in_query = f"SETTINGS {settings_in_query}"
    node.query(
        f"CREATE DATABASE {db_name} ENGINE=Replicated('/test/{db_name}', "
        + r"'{shard}', '{replica}') "
        + settings_in_query
    )

    settings_dict_from_logs = get_settings_from_logs(node, db_name)
    assert setting_dict == settings_dict_from_logs
    node.query(f"DROP DATABASE IF EXISTS {db_name}")


def test_database_replicated_settings_zero_logs_to_keep(started_cluster):
    db_name = "test_" + get_random_string()

    assert "A setting's value has to be greater than 0" in node1.query_and_get_error(
        f"CREATE DATABASE {db_name} ENGINE=Replicated('/test/{db_name}', "
        + r"'{shard}', '{replica}') "
        + "SETTINGS logs_to_keep=0"
    )

def test_create_database_replicated_with_default_args(started_cluster):
    db_name = "test_" + get_random_string()
    db_uuid = uuid.uuid4()
    assert "within an ON CLUSTER query" in node1.query_and_get_error(f"CREATE DATABASE {db_name} Engine=Replicated")

    node1.query(f"CREATE DATABASE {db_name}_1 ON CLUSTER default Engine=Replicated")
    node2.query(f"CREATE DATABASE {db_name}_2 ON CLUSTER default Engine=Replicated")

    node1.query(f"CREATE DATABASE {db_name}_explicit UUID '{db_uuid}' Engine=Replicated")
    node2.query(f"CREATE DATABASE {db_name}_explicit UUID '{db_uuid}' Engine=Replicated")

    for db in [f"{db_name}_1", f"{db_name}_2", f"{db_name}_explicit"]:
        resp1 = node1.query(f"SELECT database, zookeeper_path FROM system.database_replicas WHERE database='{db}' ORDER BY ALL")
        resp2 = node2.query(f"SELECT database, zookeeper_path FROM system.database_replicas WHERE database='{db}' ORDER BY ALL")
        assert resp1 == resp2
