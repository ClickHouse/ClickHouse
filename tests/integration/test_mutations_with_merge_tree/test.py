import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

instance_test_mutations = cluster.add_instance(
    "test_mutations_with_merge_tree",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance_test_mutations.query(
            """CREATE TABLE test_mutations_with_ast_elements(date Date, a UInt64, b String) ENGINE = MergeTree PARTITION BY toYYYYMM(date) ORDER BY (a, date)"""
        )
        instance_test_mutations.query(
            """INSERT INTO test_mutations_with_ast_elements SELECT '2019-07-29' AS date, 1, toString(number) FROM numbers(1) SETTINGS force_index_by_date = 0, force_primary_key = 0"""
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_mutations_with_merge_background_task(started_cluster):
    instance_test_mutations.query(
        """SYSTEM STOP MERGES test_mutations_with_ast_elements"""
    )

    ## The number of asts per query is 15
    for execution_times_for_mutation in range(100):
        instance_test_mutations.query(
            """ALTER TABLE test_mutations_with_ast_elements DELETE WHERE 1 = 1 AND toUInt32(b) IN (1)"""
        )

    all_done = False
    for wait_times_for_mutation in range(100):  # wait for replication 80 seconds max
        time.sleep(0.8)

        def get_done_mutations(instance):
            instance_test_mutations.query(
                """DETACH TABLE test_mutations_with_ast_elements"""
            )
            instance_test_mutations.query(
                """ATTACH TABLE test_mutations_with_ast_elements"""
            )
            return int(
                instance.query(
                    "SELECT sum(is_done) FROM system.mutations WHERE table = 'test_mutations_with_ast_elements' SETTINGS force_index_by_date = 0, force_primary_key = 0"
                ).rstrip()
            )

        if get_done_mutations(instance_test_mutations) == 100:
            all_done = True
            break

    print(
        instance_test_mutations.query(
            "SELECT mutation_id, command, parts_to_do, is_done FROM system.mutations WHERE table = 'test_mutations_with_ast_elements' SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT TSVWithNames"
        )
    )
    assert all_done


def test_mutations_with_truncate_table(started_cluster):
    instance_test_mutations.query(
        """SYSTEM STOP MERGES test_mutations_with_ast_elements"""
    )

    ## The number of asts per query is 15
    for execute_number in range(100):
        instance_test_mutations.query(
            """ALTER TABLE test_mutations_with_ast_elements DELETE WHERE 1 = 1 AND toUInt32(b) IN (1)"""
        )

    instance_test_mutations.query("TRUNCATE TABLE test_mutations_with_ast_elements")
    assert (
        instance_test_mutations.query(
            "SELECT COUNT() FROM system.mutations WHERE table = 'test_mutations_with_ast_elements SETTINGS force_index_by_date = 0, force_primary_key = 0'"
        ).rstrip()
        == "0"
    )
