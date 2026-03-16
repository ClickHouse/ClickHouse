from collections import defaultdict
import datetime
import inspect
import json
import random
import time
from multiprocessing.dummy import Pool

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/logs_config.xml",
        "configs/config.d/instant_moves.xml",
        "configs/config.d/storage_configuration.xml",
        "configs/config.d/cluster.xml",
    ],
    with_zookeeper=True,
    tmpfs=["/test_ttl_move_jbod1:size=40M", "/test_ttl_move_jbod2:size=40M", "/test_ttl_move_external:size=40M"],
    macros={"shard": 0, "replica": 1},
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/logs_config.xml",
        "configs/config.d/instant_moves.xml",
        "configs/config.d/storage_configuration.xml",
        "configs/config.d/cluster.xml",
    ],
    with_zookeeper=True,
    tmpfs=["/test_ttl_move_jbod1:size=40M", "/test_ttl_move_jbod2:size=40M", "/test_ttl_move_external:size=40M"],
    macros={"shard": 0, "replica": 2},
)

# ---------------------------------
@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def check_if_should_skip_this_then_skip(node):
    if node.is_built_with_sanitizer() or node.is_debug_build():
        pytest.skip("Disabled for debug and sanitizer builds")

# --------- Helpers to track MOVE operations ---------
def get_disks_state(node, table_name, partition=None):
    and_with_partition = '' if partition is None else "AND partition='{}'".format(partition)

    query_result = node.query(
        f"""
        SELECT part_name,rows,disk_name,modification_time,now()
        FROM system.parts
        WHERE table == '{table_name}' AND active=1 {and_with_partition}
        FORMAT JSONEachRow
        """)

    result = defaultdict(lambda: {
            'rows': 0,
    })

    for line in query_result.strip().split('\n'):
        if line:
            line = json.loads(line)
            disk_stats  = result[line['disk_name']]
            disk_stats['rows'] += line['rows']
    return result

def get_disk_names(node, table_name, partition=None):
    return set(get_disks_state(node, table_name, partition).keys())

def wait_if_moving_then_assert_disks(node, table_name, partition=None, retries=30, sleep_sec=1, target_state=None):

    for _ in range(retries):
        moves = int(node.query(f"SELECT count() FROM system.moves WHERE table = '{table_name}'").strip())

        if moves == 0 and get_disks_state(node, table_name, partition) == target_state:
            # if there's no active move happening, and we're in the target state - this is success
            return

        time.sleep(sleep_sec)

    # timeout is reached, we gonna fail if we're not in target state yet
    assert get_disks_state(node, table_name, partition) == target_state, "Move hasn't finished in at least {} seconds".format(retries * sleep_sec)


# --------- Helpers to populate/manipulate data ---------
F_SMALL_STRING = lambda: 'randomPrintableASCII(1024)'
F_LARGE_STRING = lambda: 'randomPrintableASCII(1024*1024)'

SOON_SECONDS = 3
NEVER_SECONDS = 24*3600
F_ALREADY_EXPIRED_DATETIME = lambda now_ts: f'toDateTime({now_ts - 1})'
F_SOON_TO_EXPIRE_DATETIME = lambda now_ts: f'toDateTime({now_ts + SOON_SECONDS})'
F_NEVER_EXPIRE_DATETIME = lambda now_ts: f'toDateTime({now_ts + NEVER_SECONDS})'

def gen_uni_records(num, f_gen_string_field):
    v_string_field = f_gen_string_field()

    for _ in range(num):
        yield f'({v_string_field})'

def gen_pair_records(num, f_gen_string_field, f_gen_datetime_field, now_ts=None):
    v_string_field = f_gen_string_field()
    v_datetime_field = f_gen_datetime_field(now_ts is not None or time.time())

    for _ in range(num):
        yield f'({v_string_field},{v_datetime_field})'

def gen_triplet_records(num, v_partition, f_gen_string_field, f_gen_datetime_field, now_ts=None):
    v_string_field = f_gen_string_field()
    v_datetime_field = f_gen_datetime_field(now_ts is not None or time.time())

    for _ in range(num):
        yield f'({v_partition},{v_string_field},{v_datetime_field})'


def populate_with_records(node, table_name, generators):
    print(f'INSTER1 t={datetime.datetime.now()}, {time.time()}')
    for g in generators:
        insert_query = "INSERT INTO {} VALUES {}".format(
            table_name,
            ','.join(g)
        )
        print(insert_query)
        node.query(insert_query)

def drop_if_exists(node, table_name):
    try:
        node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")
    except:
        pass

def unique_table_name(pytest_request_fixture):
    if hasattr(pytest_request_fixture.node, 'callspec'):
        name = pytest_request_fixture.node.originalname + '_' + pytest_request_fixture.node.callspec.id
    else:
        name = pytest_request_fixture.node.originalname
    return f"{name.replace('[', '_').replace(']', '_').replace('-', '_')}_{int(time.time())}"

# ---------------------------------
@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "MergeTree()",
            id="mt"
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_create_with_invalid_destination_rmt', '1')",
            id="rmt"
        )
    ]
)
def test_create_with_invalid_destination(started_cluster, engine, request):
    table_name = unique_table_name(request)

    try:
        def get_create_command(rule, policy):
            return f"""
                CREATE TABLE {table_name} (
                    s1 String,
                    d1 DateTime
                ) ENGINE = {engine}
                ORDER BY tuple()
                {rule}
                SETTINGS storage_policy='{policy}'
            """

        # unknown disk
        node1.query_and_get_error(
            get_create_command(
                rule="TTL d1 TO DISK 'unknown'",
                policy="small_jbod_with_external"
            )
        )
        # unknown volume
        node1.query_and_get_error(
            get_create_command(
                rule="TTL d1 TO VOLUME 'unknown'",
                policy="small_jbod_with_external"
            )
        )
        # unsupported disk
        node1.query_and_get_error(
            get_create_command(
                rule="TTL d1 TO DISK 'jbod1'",
                policy="only_jbod2"
            )
        )
        # unsupported volume
        node1.query_and_get_error(
            get_create_command(
                rule="TTL d1 TO VOLUME 'external'",
                policy="only_jbod2"
            )
        )
        # unknown field
        node1.query_and_get_error(
            get_create_command(
                rule="TTL unknownField TO DISK 'jbod2'",
                policy="only_jbod2"
            )
        )

    finally:
        drop_if_exists(node1, table_name)


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "MergeTree()",
            id="mt"
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_alter_with_invalid_destination', '1')",
            id="rmt"
        )
    ]
)
def test_alter_with_invalid_destination(started_cluster, engine, request):
    table_name = unique_table_name(request)

    try:
        def get_create_command(policy):
            return f"""
                CREATE TABLE {table_name} (
                    s1 String,
                    d1 DateTime
                ) ENGINE = {engine}
                ORDER BY tuple()
                SETTINGS storage_policy='{policy}'
            """
        def get_alter_command(rule):
            return f"""
                ALTER TABLE {table_name} MODIFY TTL {rule}
            """

        node1.query(get_create_command('small_jbod_with_external'))
        # unknown disk
        node1.query_and_get_error(
            get_alter_command(
                rule="TTL d1 TO DISK 'unknown'"
            )
        )
        # unknown volume
        node1.query_and_get_error(
            get_alter_command(
                rule="TTL d1 TO VOLUME 'unknown'",
            )
        )

        # recreate table with different policy
        drop_if_exists(node1, table_name)
        node1.query(get_create_command('only_jbod2'))
        # unsupported disk
        node1.query_and_get_error(
            get_alter_command(
                rule="TTL d1 TO DISK 'jbod1'",
            )
        )
        # unsupported volume
        node1.query_and_get_error(
            get_alter_command(
                rule="TTL d1 TO VOLUME 'external'",
            )
        )
        # unknown field
        node1.query_and_get_error(
            get_alter_command(
                rule="TTL unknownField TO DISK 'jbod2'",
            )
        )

    finally:
        drop_if_exists(node1, table_name)


@pytest.mark.parametrize(
    "engine,ttl_rule",
    [
        pytest.param(
            "MergeTree()",
            "TTL d1 TO DISK 'external'",
            id="mt_disk",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_inserts_and_moves_rmt_disk', '1')",
            "TTL d1 TO DISK 'external'",
            id="rmt_disk",
        ),
        pytest.param(
            "MergeTree()",
            "TTL d1 TO VOLUME 'external'",
            id="mt_volume",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_inserts_and_moves_rmt_volume', '1')",
            "TTL d1 TO VOLUME 'external'",
            id="rmt_volume",
        ),
    ],
)
def test_inserts_and_moves(started_cluster, engine, ttl_rule, request):
    table_name = unique_table_name(request)

    try:
        # --------------------- CASE I ---------------------
        # INSERT into a simple table with all defaults, using records:
        #   - are already expired and should be written to 'external' directly
        #   - gonna expire soon and should be moved to 'external' by the end of the test
        # We check that both types of data eventually make it to the target destination
        # NOTE: we are not checking resuls of direct INSERT here to prevent flakinness; it's covered later at CASE II in a controlled fashion
        node1.query(f"""
            CREATE TABLE {table_name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            {ttl_rule}
            SETTINGS storage_policy='small_jbod_with_external'
        """)

        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
            ]
        )

        time.sleep(SOON_SECONDS)    # waiting to let the records expire
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'external': {'rows': 40},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "40"

        drop_if_exists(node1, table_name)
        # --------------------- CASE I~ (NEGATIVE CHECK) ---------------------
        # INSERT into a simple table with all defaults, using records:
        #   - are already expired and should be written to 'external' directly
        #   - never gonna expire and won't move by the end of the test
        # NOTE: Case I and Case I~ could be united together, but mixing all three types of records won't play nicely for RMT
        #       We'll check it more thouroughly later, when manually controlling MOVES and MERGES, but here it's kept simple
        node1.query(f"""
            CREATE TABLE {table_name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            {ttl_rule}
            SETTINGS storage_policy='small_jbod_with_external'
        """)

        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
            ]
        )
        # Check 1. Immediately after INSERT
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 20},
            'external': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "40"

        # Check 2. Upon expiration
        time.sleep(SOON_SECONDS)    # wait for expiration
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 20},
            'external': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "40"


        drop_if_exists(node1, table_name)
        # --------------------- CASE II ---------------------
        # INSERT into a simple table with MERGES and MOVES disabled, using records which:
        #   - are already expired and should be written to 'external' directly
        #   - gonna expire soon
        # and then checking that MOVE doesn't occur until re-enabled
        node1.query(f"""
            CREATE TABLE {table_name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            {ttl_rule}
            SETTINGS storage_policy='small_jbod_with_external'
        """)
        node1.query(f"SYSTEM STOP MERGES {table_name}")
        node1.query(f"SYSTEM STOP MOVES {table_name}")

        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
            ]
        )
        # Check 1. Immediately after INSERT
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 20},
            'external': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "40"


        # Check 2. Upon expiration
        time.sleep(SOON_SECONDS)    # wait for expiration
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 20},
            'external': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "40"

        # Check 3. Upon re-enabling of MOVES
        node1.query(f"SYSTEM START MOVES {table_name}")
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'external': {'rows': 40},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "40"


        drop_if_exists(node1, table_name)
        # --------------------- CASE III ---------------------
        # INSERT into a PARTITIONed table with MERGES and MOVES disabled, using records which:
        #   - are already expired and should be written to 'external' directly
        #   - gonna expire soon
        # and then checking that MOVE doesn't occur until re-enabled
        node1.query(f"""
            CREATE TABLE {table_name} (
                p1 Int64,
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            {ttl_rule}
            SETTINGS storage_policy='small_jbod_with_external'
        """)
        # Disabling MOVES and MERGES to get full control over the course of the scenario
        node1.query(f"SYSTEM STOP MERGES {table_name}")
        node1.query(f"SYSTEM STOP MOVES {table_name}")
        # 1. Let's write some parts:
        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_triplet_records(10, 0, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_triplet_records(10, 1, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_triplet_records(10, 0, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
                gen_triplet_records(10, 1, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
                gen_triplet_records(10, 0, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
                gen_triplet_records(10, 1, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
            ]
        )
        # Check 1. Immediately after INSERT
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 40},
            'external': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "60"

        # Check 2. Upon expiration
        time.sleep(SOON_SECONDS)    # wait for expiration
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 40},
            'external': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "60"

        # Check 3. Upon re-enabling of MOVES
        node1.query(f"SYSTEM START MOVES {table_name}")
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 20},
            'external': {'rows': 40},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "60"


        drop_if_exists(node1, table_name)
        # --------------------- CASE IV ---------------------
        # Very special case with direct INSERTs disabled (see 'jbod_without_instant_ttl_move') - the data is moved only via MOVE operation
        node1.query(f"""
            CREATE TABLE {table_name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            {ttl_rule}
            SETTINGS storage_policy='jbod_without_instant_ttl_move'
        """)
        node1.query(f"SYSTEM STOP MOVES {table_name}")

        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
            ]
        )
        # Check 1. Immediately after INSERT
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "20"


        # Check 2. Upon re-enabling of MOVES
        node1.query(f"SYSTEM START MOVES {table_name}")
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'external': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "20"


    finally:
        drop_if_exists(node1, table_name)


@pytest.mark.parametrize(
    "engine,ttl_rule",
    [
        pytest.param(
            "MergeTree()",
            f"TTL d1 + INTERVAL {SOON_SECONDS} SECOND DELETE",
            id="mt_interval_delete",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_delete_rmt_interval_delete', '1')",
            f"TTL d1 + INTERVAL {SOON_SECONDS} SECOND DELETE",
            id="rmt_interval_delete",
        ),
        pytest.param(
            "MergeTree()",
            "TTL d1 DELETE",
            id="mt_delete",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_delete_rmt_delete', '1')",
            "TTL d1 DELETE",
            id="rmt_delete",
        ),
    ],
)
def test_delete(started_cluster, engine, ttl_rule, request):
    table_name = unique_table_name(request)

    try:
        # This test is to prove 'TTL .. DELETE' works. Also we validate that DELETE is actually performed by MERGE operation,
        # by disabling/enabling at checkpoints
        node1.query(f"""
            CREATE TABLE {table_name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            {ttl_rule}
            SETTINGS storage_policy='only_jbod1'
        """)
        node1.query(f"SYSTEM STOP MERGES {table_name}")
        node1.query(f"SYSTEM STOP MOVES {table_name}")

        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
            ]
        )
        # Check 1. All records are in place
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "60"
        # Check 2. MOVES do nothing
        node1.query(f"SYSTEM START MOVES {table_name}")
        time.sleep(SOON_SECONDS*2)    # wait for expiration and a bit more for MOVE to kick in (it won't)
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "60"
        # Check 3. MERGES actually deletes data
        node1.query(f"SYSTEM START MERGES {table_name}")
        node1.query(f"OPTIMIZE TABLE {table_name} FINAL",
                    settings={'mutations_sync': 1})
        assert get_disks_state(node1, table_name) == {
            'jbod1' : {'rows':20}
        }
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "20"

    finally:
        drop_if_exists(node1, table_name)


@pytest.mark.parametrize(
    "engine,ttl_create_rule,ttl_alter_rule",
    [
        pytest.param(
            "MergeTree()",
            "TTL d1 TO DISK 'external'",
            "MODIFY TTL d1 + INTERVAL 60 MINUTE DELETE",
            id="mt_disk_interval_delete",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_alter_rmt_1', '1')",
            "TTL d1 TO DISK 'external'",
            "MODIFY TTL d1 + INTERVAL 60 MINUTE DELETE",
            id="rmt_disk_internal_delete",
        ),
        pytest.param(
            "MergeTree()",
            "TTL d1 TO VOLUME 'external'",
            "MODIFY TTL d1 + INTERVAL 60 MINUTE TO DISK 'external'",
            id="mt_volume_interval_delete",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_alter_rmt_2', '1')",
            "TTL d1 TO VOLUME 'external'",
            "MODIFY TTL d1 + INTERVAL 60 MINUTE TO DISK 'external'",
            id="rmt_volume_interval_delete",
        ),
        pytest.param(
            "MergeTree()",
            "TTL d1 TO DISK 'external'",
            "REMOVE TTL",
            id="mt_disk_remove_ttl",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_alter_rmt_3', '1')",
            "TTL d1 TO DISK 'external'",
            "REMOVE TTL",
            id="rmt_disk_remove_ttl",
        ),
        pytest.param(
            "MergeTree()",
            "TTL d1 TO VOLUME 'external'",
            "REMOVE TTL",
            id="mt_disk_remove_ttl",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_alter_rmt_3', '1')",
            "TTL d1 TO VOLUME 'external'",
            "REMOVE TTL",
            id="rmt_disk_remove_ttl",
        ),
    ],
)
def test_alter(started_cluster, engine, ttl_create_rule, ttl_alter_rule, request):
    table_name = unique_table_name(request)

    try:
        # This test is to prove that ALTERing TTL rules is possible. We first create a table with immdiate TTL move policy,
        # and then modifying it to disable it.
        node1.query(f"""
            CREATE TABLE {table_name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            {ttl_create_rule}
            SETTINGS storage_policy='small_jbod_with_external'
        """)
        node1.query(f"""
            ALTER TABLE {table_name} {ttl_alter_rule}
        """)

        # 1. Let's write some parts:
        #   - some are already expired - to validate that direct INSERT doesn't work anymore
        #   - some are not yet expired (but will expire later during the test) - to validate that MOVE won't work either
        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
            ]
        )

        # Check 1. Immediately after INSERT
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 40},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "40"


        # Check 2. Upon expiration
        time.sleep(SOON_SECONDS*2)    # wait for expiration and a bit more for MOVE to kick in (it won't in happy scenario)
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 40},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "40"

    finally:
        drop_if_exists(node1, table_name)


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "MergeTree()",
            id="mt",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_ttl_can_be_changed_with_alter_ReplicatedMergeTree', '1')",
            id="rmt",
        ),
    ],
)
def test_layered_tired_storage(started_cluster, engine, request):
    table_name = unique_table_name(request)

    try:
        # Multi-layered tired storage is validated in this test. The entire policy applies via ALTER.
        # From other tests we're confident that it doesn't matter if TTL is configured at CREATE or via ALTER.
        # But using step-by-step altering appoach prevents flakinness on this test:
        #   at each phase the target state is either achieved, either failed - variety of states is eliminated
        #
        # Natural and straight forward approach would be to have multiple layers configured at once, and then to sleep to let MOVEs to happen.
        # This approach is prone to flakinness due to unexpected delays happenning randomly at test and node sides.
        node1.query(f"""
            CREATE TABLE {table_name} (
                p1 Int64,
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION by p1
            SETTINGS storage_policy='jbod1_then_jbod2_then_external'
        """)

        # 1.
        node1.query(f"""
            ALTER TABLE {table_name} MODIFY
            TTL d1 + INTERVAL 0 SECOND TO DISK 'jbod2'
        """)

        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_triplet_records(10, 0, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_triplet_records(10, 1, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_triplet_records(10, 0, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
                gen_triplet_records(10, 1, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
            ]
        )

        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 20},
            'jbod2': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "40"

        # 2.
        node1.query(f"""
            ALTER TABLE {table_name} MODIFY
            TTL d1 + INTERVAL 0 SECOND TO DISK 'jbod2',
                d1 + INTERVAL {SOON_SECONDS} SECOND TO DISK 'external'
        """)
        time.sleep(SOON_SECONDS)
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 20},
            'external': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "40"

    finally:
        drop_if_exists(node1, table_name)


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "MergeTree()",
            id="mt"
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_materialize_ttl_per_partition_with_rmt', '1')",
            id="rmt"
        ),
    ]
)
def test_materialize_ttl_per_partition(started_cluster, engine, request):
    table_name = unique_table_name(request)

    try:
        # This test checks that TTL can be materialized per partition
        node1.query(f"""
            CREATE TABLE {table_name} (
                p1 Int8,
                s1 String,
                d1 DateTime
            ) ENGINE {engine}
            ORDER BY p1
            PARTITION BY p1
            SETTINGS storage_policy='small_jbod_with_external'
        """)
        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_triplet_records(10, 0, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_triplet_records(10, 1, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_triplet_records(10, 2, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_triplet_records(10, 3, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_triplet_records(10, 4, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
            ]
        )
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 50},
        })

        # Now let's apply TTL policy
        node1.query(f"""
            ALTER TABLE {table_name}
                MODIFY TTL
                d1 TO DISK 'external' SETTINGS materialize_ttl_after_modify=0, mutations_sync=1
        """)
        # Nothing has changes becuase of 'materialize_ttl_after_modify=0'
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 50},
        })
        # Now we manually trigger materialization
        node1.query(f"""
            ALTER TABLE {table_name} MATERIALIZE TTL IN PARTITION 2
            SETTINGS mutations_sync=1
        """)
        node1.query(f"""
            ALTER TABLE {table_name} MATERIALIZE TTL IN PARTITION 4
            SETTINGS mutations_sync=1
        """)

        # And checking that the correct partitions were moved
        wait_if_moving_then_assert_disks(node1, table_name, partition=0, target_state={
            'jbod1': {'rows': 10}})
        wait_if_moving_then_assert_disks(node1, table_name, partition=1, target_state={
            'jbod1': {'rows': 10}})
        wait_if_moving_then_assert_disks(node1, table_name, partition=2, target_state={
            'external': {'rows': 10}})
        wait_if_moving_then_assert_disks(node1, table_name, partition=3, target_state={
            'jbod1': {'rows': 10}})
        wait_if_moving_then_assert_disks(node1, table_name, partition=4, target_state={
            'external': {'rows': 10}})
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "50"

    finally:
        drop_if_exists(node1, table_name)


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "MergeTree()",
            id="mt",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_move_works_after_policy_change_rmt', '1')",
            id="rmt",
        ),
    ]
)
def test_move_after_policy_change(started_cluster, engine, request):
    table_name = unique_table_name(request)

    try:
        node1.query(f"""
            CREATE TABLE {table_name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
        """)

        node1.query(f"""
            ALTER TABLE {table_name} MODIFY SETTING storage_policy='default_with_small_jbod_with_external'
        """)

        # 1st rule won't work, 2nd will
        node1.query(f"""
            ALTER TABLE {table_name} MODIFY TTL now()-3600 TO DISK 'jbod1', d1 TO DISK 'external'
            """,
            settings={"allow_suspicious_ttl_expressions": 1}
        )
        # We control MOVES to prevent flakinness
        node1.query(f"SYSTEM STOP MOVES {table_name}")

        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
            ]
        )

        # 1. Records are not yet expired, so we expect them to be at the default disk - jbod1
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "20"

        # Now it's safe to re-enable MOVES
        node1.query(f"SYSTEM START MOVES {table_name}")
        time.sleep(SOON_SECONDS)
        # 2. Now should be expired and moved to the 'external' disk
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'external': {'rows': 20}
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "20"

    finally:
        drop_if_exists(node1, table_name)


def test_replicated_download_ttl_info(started_cluster, request):
    table_name = unique_table_name(request)
    engine = "ReplicatedMergeTree('/clickhouse/test_replicated_download_ttl_info', '{replica}')"

    try:
        for node in (node1, node2):
            node.query(f"""
                CREATE TABLE {table_name} (
                    s1 String,
                    d1 DateTime
                ) ENGINE = {engine}
                ORDER BY tuple()
                TTL d1 TO DISK 'external'
                SETTINGS storage_policy='small_jbod_with_external'
            """)

        # 1. We'll check direct inserts, which should work with MOVES disabled:
        # - expired records will be written directly to 'external'
        # - soon-to-expire records will remain on 'jbod1'
        node1.query(f"SYSTEM STOP MOVES {table_name}")
        node2.query(f"SYSTEM STOP MOVES {table_name}")

        populate_with_records(
            node=node2,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
            ]
        )

        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 10},
            'external': {'rows': 10},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "20"

        wait_if_moving_then_assert_disks(node2, table_name, target_state={
            'jbod1': {'rows': 10},
            'external': {'rows': 10},
        })
        assert node2.query(f"SELECT count() FROM {table_name}").strip() == "20"

        # 2. Now let's resume MOVES, and check that soon-to-expire records move
        node1.query(f"SYSTEM START MOVES {table_name}")
        node2.query(f"SYSTEM START MOVES {table_name}")
        time.sleep(SOON_SECONDS)

        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'external': {'rows': 20}
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "20"

        wait_if_moving_then_assert_disks(node2, table_name, target_state={
            'external': {'rows': 20}
        })
        assert node2.query(f"SELECT count() FROM {table_name}").strip() == "20"

    finally:
        for n in (node1, node2):
            drop_if_exists(n, table_name)


@pytest.mark.parametrize(
    "dest_type",
    [
        pytest.param("DISK", id="rmt_disk"),
        pytest.param("VOLUME", id="rmt_volume"),
    ]
)
def test_ttl_move_if_exists(started_cluster, dest_type, request):
    table_name = unique_table_name(request)

    try:
        # This test validates that IF EXISTS works correctly with TTL rules. Two replicas are created with different
        # storage policies, and the data is eventually moved if the specified destination exists.
        # The data is placed to the available disk if the specified destination doesn't exist.
        create_query_template = """
            CREATE TABLE {table_name} (
                s1 String,
                d1 DateTime
            ) ENGINE = ReplicatedMergeTree('/clickhouse/test_ttl_move_if_exists', '{node_name}')
            ORDER BY tuple()
            TTL d1 TO {dest_type} {if_exists} 'external'
            SETTINGS storage_policy='{policy}'
        """

        node1.query_and_get_error(
            create_query_template.format(
                table_name=table_name,
                node_name=node1.name,
                dest_type=dest_type,
                if_exists="",           # this why this call gonna fail - 'external' is attempted to be used
                policy="only_jbod1",    # with the 'jbod1' only policy
            )
        )

        for node,policy in zip(
            (node1, node2), ("only_jbod1", "small_jbod_with_external")
        ):
            node.query(
                create_query_template.format(
                    table_name=table_name,
                    node_name=node.name,
                    dest_type=dest_type,
                    if_exists="IF EXISTS",  # now this should work despite whether 'external' is configured
                    policy=policy,
                )
            )

        # 1. MOVE'ing expired data, it ends up directly at the target disk
        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
            ]
        )
        node2.query(f"SYSTEM SYNC REPLICA {table_name}") # this sync is blocking, no sleep needed

        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 20},
        })
        wait_if_moving_then_assert_disks(node2, table_name, target_state={
            'external': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "20"
        assert node2.query(f"SELECT count() FROM {table_name}").strip() == "20"

        # 2. MOVE'ing never-expiring data, it will be placed to the original 'jbod1'
        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
            ]
        )
        node2.query(f"SYSTEM SYNC REPLICA {table_name}")

        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1': {'rows': 40},
        })
        wait_if_moving_then_assert_disks(node2, table_name, target_state={
            'jbod1': {'rows': 20},
            'external': {'rows': 20},
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "40"
        assert node2.query(f"SELECT count() FROM {table_name}").strip() == "40"

    finally:
        drop_if_exists(node1, table_name)
        drop_if_exists(node2, table_name)


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "MergeTree()",
            id="mt"
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/replicated_test_merges_to_disk_rmt', '1')",
            id="rmt"
        ),
    ]
)
def test_merges_to_disk(started_cluster, engine, request):
    table_name = unique_table_name(request)

    try:
        node1.query(f"""
            CREATE TABLE {table_name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 TO DISK 'external'
            SETTINGS storage_policy='small_jbod_with_external'
        """)

        node1.query(f"SYSTEM STOP MERGES {table_name}")
        node1.query(f"SYSTEM STOP MOVES {table_name}")

        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_NEVER_EXPIRE_DATETIME),
            ]
        )

        # 1.
        assert get_disks_state(node1, table_name) == {
            'jbod1': {'rows': 40},
            'external': {'rows': 20}
        }
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "60"

        # 2. Wait for expiration and re-enable & trigger MERGES to see that it applied TTL policy and the data has moved
        # NOTE: Merge operation will merge all outstanding parts to the disk with the highest id, which is 'external' in this case
        time.sleep(SOON_SECONDS)
        node1.query(f"SYSTEM START MERGES {table_name}")
        node1.query(f"OPTIMIZE TABLE {table_name}",
                    settings={'mutations_sync': 1})

        assert get_disks_state(node1, table_name) == {
            'external': {'rows': 60}
        }
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "60"

    finally:
        drop_if_exists(node1, table_name)


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "MergeTree()",
            id="mt",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_moves_with_full_disk_work_rmt', '1')",
            id="rmt",
        ),
    ],
)
def test_moves_with_full_disk(started_cluster, engine, request):
    check_if_should_skip_this_then_skip(node1)

    table_name = unique_table_name(request)
    temp_table_name = table_name + '_temp'

    try:
        # This test checks that if the target disk is full, the data will be placed onto the available disk,
        # and later moved to the correct destination once possible.
        # TEMP table to fill up the disk
        node1.query(f"""
            CREATE TABLE {temp_table_name} (
                s1 String
            ) ENGINE = MergeTree()
            ORDER BY tuple()
            SETTINGS storage_policy='only_jbod2'
        """)

        # Disk is 40Mb, we write 35Mb
        populate_with_records(
            node=node1,
            table_name=temp_table_name,
            generators=[
                gen_uni_records(35, F_LARGE_STRING),
            ]
        )
        assert get_disk_names(node1, temp_table_name) == {'jbod2'}

        # Now we create the target table and populate it
        node1.query(f"""
        CREATE TABLE {table_name} (
            s1 String,
            d1 DateTime
        ) ENGINE = {engine}
        ORDER BY tuple()
        TTL d1 TO DISK 'jbod2'
        SETTINGS storage_policy='jbod1_with_jbod2'
        """)
        # 10Mb of expired records to be placed to 'jbod2' as a single part, but it's full up to 35Mb out of 40Mb
        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_LARGE_STRING, F_ALREADY_EXPIRED_DATETIME),
            ]
        )
        # so this part ends up at 'jbod1'
        assert get_disk_names(node1, table_name) == {'jbod1'}

        # Now we drop the TEMP table, free up the space, and eventually the move happens
        node1.query(f"DROP TABLE {temp_table_name} SYNC")
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod2': {'rows': 10}
        })
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "10"

    finally:
        drop_if_exists(node1, temp_table_name)
        drop_if_exists(node1, table_name)


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "MergeTree()",
            id="mt",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_merges_with_full_disk_rmt', '1')",
            id="rmt",
        ),
    ],
)
def test_merges_with_full_disk(started_cluster, engine, request):
    check_if_should_skip_this_then_skip(node1)

    table_name = unique_table_name(request)
    temp_table_name = table_name + '_temp'

    try:
        # This test checks that MERGE falls back to the available disk, if the target disk is full
        # NOTE: MERGE operation is sensitive to not-having enough free space at 'jbod1' - its data sizes are imbalanced (or 'jbod1' is more full than expected), this test might flap by hanging in OPTIMIZE

        # TEMP table to fill up the disk
        node1.query(f"""
            CREATE TABLE {temp_table_name} (
                s1 String
            ) ENGINE = MergeTree()
            ORDER BY tuple()
            SETTINGS storage_policy='only_jbod2'
        """)

        # Disk is 40Mb, we write 35Mb
        populate_with_records(
            node=node1,
            table_name=temp_table_name,
            generators=[
                gen_uni_records(35, F_LARGE_STRING),
            ]
        )
        assert get_disk_names(node1, temp_table_name) == {'jbod2'}

        # Now we create the target table and populate it
        node1.query(f"""
            CREATE TABLE {table_name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 TO DISK 'jbod2'
            SETTINGS storage_policy='jbod1_with_jbod2'
        """)
        # 2 parts of 10Mb of expired records to be move to 'jbod2', but it's full up to 35Mb out of 40Mb
        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(6, F_LARGE_STRING, F_ALREADY_EXPIRED_DATETIME),
                gen_pair_records(6, F_LARGE_STRING, F_ALREADY_EXPIRED_DATETIME),
            ]
        )

        # 1. Check that records are placed at 'jbod1'
        assert get_disk_names(node1, table_name) == {'jbod1'}
        assert node1.query(f"SELECT count() FROM system.parts WHERE table='{table_name}' AND active=1").strip() == "2"

        # 2. Disk is still full, but we force MERGE, data is expected to still be at 'jbod1'
        node1.query(f"OPTIMIZE TABLE {table_name} SETTINGS mutations_sync=1")
        time.sleep(1)
        assert get_disk_names(node1, table_name) == {'jbod1'}
        assert node1.query(f"SELECT count() FROM system.parts WHERE table='{table_name}' AND active=1").strip() == "1"
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "12"

    finally:
        drop_if_exists(node1, temp_table_name)
        drop_if_exists(node1, table_name)


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "MergeTree()",
            id="mt"
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_moves_after_merges_rmt', '1')",
            id="rmt"
        ),
    ]
)
def test_moves_after_merges(started_cluster, engine, request):
    table_name = unique_table_name(request)

    try:
        node1.query(f"""
            CREATE TABLE {table_name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 TO DISK 'external'
            SETTINGS storage_policy='small_jbod_with_external'
        """)
        # we want to control over when to MOVE to eliminate flakinness
        node1.query(f"SYSTEM STOP MOVES {table_name}")
        populate_with_records(
            node=node1,
            table_name=table_name,
            generators=[
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
                gen_pair_records(10, F_SMALL_STRING, F_SOON_TO_EXPIRE_DATETIME),
            ]
        )

        node1.query(f"OPTIMIZE TABLE {table_name}")

        # 1. Check after MERGE
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'jbod1' : {'rows': 20}
        })
        assert node1.query(f"SELECT count() FROM system.parts WHERE table='{table_name}' AND active=1").strip() == "1"

        # 2. Waiting for the data to expire and then expecting it at the target location
        node1.query(f"SYSTEM START MOVES {table_name}")
        time.sleep(SOON_SECONDS)
        wait_if_moving_then_assert_disks(node1, table_name, target_state={
            'external' : {'rows': 20}
        })
        assert node1.query(f"SELECT count() FROM system.parts WHERE table='{table_name}' AND active=1").strip() == "1"
        assert node1.query(f"SELECT count() FROM {table_name}").strip() == "20"

    finally:
        drop_if_exists(node1, table_name)


@pytest.mark.parametrize(
    "engine",
    [
        pytest.param(
            "MergeTree()",
            id="mt",
        ),
        pytest.param(
            "ReplicatedMergeTree('/clickhouse/test_concurrent_alter_with_ttl_move', '1')",
            id="rmt"
        ),
    ]
)
def test_concurrent_alter_with_ttl_move(started_cluster, engine, request):
    check_if_should_skip_this_then_skip(node1)

    table_name = unique_table_name(request)

    try:
        # This is an opportunistic stress test, which runs multiple concurrent threads to trigger:
        # - INSERTs         (insert)
        # - direct MOVEs    (alter_move)
        # - MUTATEs         (alter_update)
        # - MOVEs by TTL    (alter_modify_ttl)
        # - MERGES          (optimize_table)
        # It is quite robust because we don't validate mid-states interactively, the only logical check is to assert for the correct number of rows
        # TODO: in the worst case, the case can stuck for 25*120 sec (num operations X timeout per operation).
        #       This is unlikely, but if that occurs, we should try making individual operations longer (increase data size + lower bandwidth) and lowering their total number.
        # TODO: There's no clear failure criteria for this test - we stress it to check whether something fails unexpectedly.
        #       Perhaps we could add post-validation (assert) for each operation, OR to build a serialized trace of execution and to validate it mid-points

        node1.query(f"""
            CREATE TABLE {table_name} (
                EventDate Date,
                number UInt64
            ) ENGINE = {engine}
            ORDER BY tuple()
            PARTITION BY toYYYYMM(EventDate)
            SETTINGS storage_policy='jbods_with_external'
        """)

        values = list({random.randint(1, 1000000) for _ in range(0, 1000)})

        def insert(num):
            for i in range(num):
                day = random.randint(11, 30)
                value = values.pop()
                month = "0" + str(random.choice([3, 4]))
                node1.query(
                    "INSERT INTO {} VALUES(toDate('2019-{m}-{d}'), {v})".format(
                        table_name, m=month, d=day, v=value
                    )
                )

        def alter_move(num):
            def produce_alter_move(node, name):
                move_type = random.choice(["PART", "PARTITION"])
                if move_type == "PART":
                    for _ in range(10):
                        try:
                            parts = (
                                node1.query(
                                    "SELECT name from system.parts where table = '{}' and active = 1".format(
                                        name
                                    )
                                )
                                .strip()
                                .split("\n")
                            )
                            break
                        except QueryRuntimeException:
                            pass
                    else:
                        raise Exception("Cannot select from system.parts")

                    move_part = random.choice(["'" + part + "'" for part in parts])
                else:
                    move_part = random.choice([201903, 201904])

                move_disk = random.choice(["DISK", "VOLUME"])
                if move_disk == "DISK":
                    move_volume = random.choice(["'external'", "'jbod1'", "'jbod2'"])
                else:
                    move_volume = random.choice(["'main'", "'external'"])
                try:
                    node1.query(
                        "ALTER TABLE {} MOVE {mt} {mp} TO {md} {mv}".format(
                            name,
                            mt=move_type,
                            mp=move_part,
                            md=move_disk,
                            mv=move_volume,
                        )
                    )
                except QueryRuntimeException:
                    pass

            for i in range(num):
                produce_alter_move(node1, table_name)

        def alter_update(num):
            for i in range(num):
                try:
                    node1.query(
                        "ALTER TABLE {} UPDATE number = number + 1 WHERE 1".format(table_name)
                    )
                except:
                    pass

        def alter_modify_ttl(num):
            for i in range(num):
                ttls = []
                for j in range(random.randint(1, 10)):
                    what = random.choice(
                        [
                            "TO VOLUME 'main'",
                            "TO VOLUME 'external'",
                            "TO DISK 'jbod1'",
                            "TO DISK 'jbod2'",
                            "TO DISK 'external'",
                        ]
                    )
                    when = "now()+{}".format(random.randint(-1, 5))
                    ttls.append("{} {}".format(when, what))
                try:
                    node1.query(
                        "ALTER TABLE {} MODIFY TTL {}".format(table_name, ", ".join(ttls))
                    )
                except QueryRuntimeException:
                    pass

        def stop_and_resume_moves(num):
            for i in range(num):
                node1.query(f"SYSTEM STOP MOVES {table_name}")
                time.sleep(1)
                node1.query(f"SYSTEM START MOVES {table_name}")
                time.sleep(1)


        def optimize_table(num):
            for i in range(num):
                try:  # optimize may throw after concurrent alter
                    node1.query(
                        "OPTIMIZE TABLE {} FINAL".format(table_name),
                        settings={"optimize_throw_if_noop": "1"},
                    )
                    break
                except:
                    pass

        p = Pool(15)
        tasks = []
        for i in range(5):
            tasks.append(p.apply_async(insert, (30,)))
            tasks.append(p.apply_async(alter_move, (30,)))
            tasks.append(p.apply_async(alter_update, (30,)))
            tasks.append(p.apply_async(alter_modify_ttl, (30,)))
            tasks.append(p.apply_async(optimize_table, (30,)))
            tasks.append(p.apply_async(stop_and_resume_moves, (10, )))

        for task in tasks:
            task.get(timeout=120)

        assert node1.query("SELECT 1") == "1\n"
        assert node1.query("SELECT COUNT() FROM {}".format(table_name)) == "150\n"
    finally:
        node1.query("DROP TABLE IF EXISTS {name} SYNC".format(name=table_name))


# TODO: there was a test here, called TestCancelBackgroundMoving, which used to test whether STOP MOVES actually cancels the background operation.
#       It was the flakiest amongst them all - the method of slowing down moves via throughput trottling coulnd't guarantee that we'd catch the move when trying to cancel it
#       Removed the test as not having a more stable alternative (should explore using FailPoints for that)
#       To minimally compensate coverage of pausing - added STOP/START to 'test_concurrent_alter_with_ttl_move' (concurrent randomized churner test)
