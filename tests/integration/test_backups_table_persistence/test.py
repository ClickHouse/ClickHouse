import pytest
import logging
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, TSV
from itertools import count


cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config.d/backups.xml"],
    stay_alive=True,
)


CONFIG_BODY = """<clickhouse>
    <backups>
        <allowed_path>/backups</allowed_path>
        {system_table}
    </backups>
</clickhouse>"""


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def around_the_test():
    try:
        instance.query("CREATE DATABASE test")
        instance.query("CREATE TABLE test.table(x UInt32) ENGINE=MergeTree ORDER BY x")
        instance.query("INSERT INTO test.table SELECT number FROM numbers(10)")
        yield
    finally:
        instance.query("DROP DATABASE IF EXISTS test")


SELECT_FROM_BACKUPS = "SELECT status, error FROM system.backups{suffix} WHERE id='{id}'"
SELECT_FROM_TABLES = (
    "SELECT database, name, engine, engine_full FROM system.tables WHERE name='{name}'"
)

curr_backup_num = count()


def new_backup_name():
    return f"File('/backups/test_backup_{next(curr_backup_num)}/')"


def backup_table(backup_name):
    return instance.query(f"BACKUP TABLE test.table TO {backup_name}").split("\t")


def restore_table(backup_name):
    return instance.query(f"RESTORE TABLE test.table FROM {backup_name}").split("\t")


def change_config(table_section):
    xml = CONFIG_BODY.format(system_table=table_section)
    logging.info(f"Next config:\n{xml}")
    instance.replace_config("/etc/clickhouse-server/config.d/backups.xml", xml)


def perform_backup_and_restore_operations():
    backup_name = new_backup_name()

    [backup_id, status] = backup_table(backup_name)
    assert status == "CREATING_BACKUP\n" or status == "BACKUP_CREATED\n"
    assert_eq_with_retry(
        instance,
        SELECT_FROM_BACKUPS.format(suffix="", id=backup_id),
        TSV([["BACKUP_CREATED", ""]]),
        200,
    )

    instance.query("DROP TABLE test.table SYNC")

    [restore_id, status] = restore_table(backup_name)
    assert status == "RESTORING\n" or status == "RESTORED\n"
    assert_eq_with_retry(
        instance,
        SELECT_FROM_BACKUPS.format(suffix="", id=restore_id),
        TSV([["RESTORED", ""]]),
        200,
    )

    return backup_id, restore_id


def check_backup_and_restore_records(backup_id, restore_id, exist, suffix=None):
    suffix = suffix or ""
    if exist:
        assert instance.query(
            SELECT_FROM_BACKUPS.format(suffix=suffix, id=backup_id)
        ) == TSV([["BACKUP_CREATED", ""]])
        assert instance.query(
            SELECT_FROM_BACKUPS.format(suffix=suffix, id=restore_id)
        ) == TSV([["RESTORED", ""]])
    else:
        assert (
            instance.query(SELECT_FROM_BACKUPS.format(suffix=suffix, id=backup_id))
            == ""
        )
        assert (
            instance.query(SELECT_FROM_BACKUPS.format(suffix=suffix, id=restore_id))
            == ""
        )


def check_table_definition(engine, engine_full):
    assert instance.query(SELECT_FROM_TABLES.format(name="backups")) == TSV(
        [
            [
                "system",
                "backups",
                engine,
                engine_full,
            ]
        ]
    )


def restart_and_check(engine_short, engine_full, ids_to_check):
    instance.restart_clickhouse()
    check_table_definition(engine_short, engine_full)
    for params in ids_to_check:
        check_backup_and_restore_records(*params)


def test_system_backups():
    BACKUPS_TABLE_SECTION = """<system_table>
                {engine}
            </system_table>"""

    assert instance.query("SELECT * FROM system.backups") == ""

    check_table_definition("SystemBackups", "SystemBackups")
    logging.info(
        "Starting without <system_table> section => 'SystemBackups' transient storage is implied"
    )

    # Write transient records
    (backup_id, restore_id) = perform_backup_and_restore_operations()

    restart_and_check(
        "SystemBackups", "SystemBackups", [[backup_id, restore_id, False]]
    )

    logging.info(
        "The next is empty <system_table> section => 'MergeTree' (default settings)"
    )
    change_config(BACKUPS_TABLE_SECTION.format(engine=""))

    restart_and_check(
        "MergeTree",
        "MergeTree PARTITION BY toYYYYMM(start_time) ORDER BY start_time SETTINGS index_granularity = 8192",
        [[backup_id, restore_id, False]],
    )

    # Write persistent records
    (backup_id, restore_id) = perform_backup_and_restore_operations()

    restart_and_check(
        "MergeTree",
        "MergeTree PARTITION BY toYYYYMM(start_time) ORDER BY start_time SETTINGS index_granularity = 8192",
        [[backup_id, restore_id, True]],
    )

    logging.info(
        "The next is <system_table> section with an explicit 'MergeTree' definition => 'MergeTree' (custom settings)"
    )
    change_config(
        BACKUPS_TABLE_SECTION.format(
            engine="<engine>ENGINE=MergeTree ORDER BY start_time SETTINGS index_granularity = 1024</engine>"
        )
    )

    restart_and_check(
        "MergeTree",
        "MergeTree ORDER BY start_time SETTINGS index_granularity = 1024",
        [[backup_id, restore_id, True, "_0"], [backup_id, restore_id, False]],
    )

    # Write persistent records
    (backup_id, restore_id) = perform_backup_and_restore_operations()

    restart_and_check(
        "MergeTree",
        "MergeTree ORDER BY start_time SETTINGS index_granularity = 1024",
        [[backup_id, restore_id, True]],
    )

    logging.info(
        "The next is <system_table> section with some explicit 'MergeTree' settings => 'MergeTree' (new custom settings)"
    )
    change_config(
        BACKUPS_TABLE_SECTION.format(
            engine="<partition_by>id</partition_by><order_by>id, start_time</order_by>"
        )
    )

    restart_and_check(
        "MergeTree",
        "MergeTree PARTITION BY id ORDER BY (id, start_time) SETTINGS index_granularity = 8192",
        [[backup_id, restore_id, True, "_1"], [backup_id, restore_id, False]],
    )

    # Write persistent records
    (backup_id, restore_id) = perform_backup_and_restore_operations()

    restart_and_check(
        "MergeTree",
        "MergeTree PARTITION BY id ORDER BY (id, start_time) SETTINGS index_granularity = 8192",
        [[backup_id, restore_id, True]],
    )

    logging.info(
        "The next is <system_table> section an explicit 'SystemBackups' special engine name"
    )
    change_config(
        BACKUPS_TABLE_SECTION.format(engine="<engine>ENGINE=SystemBackups</engine>")
    )

    restart_and_check(
        "SystemBackups",
        "SystemBackups",
        [[backup_id, restore_id, True, "_2"], [backup_id, restore_id, False]],
    )

    # Write transient records
    (backup_id, restore_id) = perform_backup_and_restore_operations()

    restart_and_check(
        "SystemBackups", "SystemBackups", [[backup_id, restore_id, False]]
    )

    logging.info(
        "The next is <system_table> section with an explicit 'Memory' definition"
    )
    change_config(BACKUPS_TABLE_SECTION.format(engine="<engine>ENGINE=Memory</engine>"))

    restart_and_check("Memory", "Memory", [[backup_id, restore_id, False]])

    # Write transient records
    (backup_id, restore_id) = perform_backup_and_restore_operations()

    restart_and_check("Memory", "Memory", [[backup_id, restore_id, False]])
