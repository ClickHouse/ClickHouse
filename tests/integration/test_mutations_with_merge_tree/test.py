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


def test_mutations_in_partition_background(started_cluster):
    try:
        numbers = 100

        name = "test_mutations_in_partition"
        instance_test_mutations.query(
            f"""CREATE TABLE {name} (date Date, a UInt64, b String) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY a"""
        )
        instance_test_mutations.query(
            f"""INSERT INTO {name} SELECT '2019-07-29' AS date, number, toString(number) FROM numbers({numbers})"""
        )

        for i in range(0, numbers, 3):
            instance_test_mutations.query(
                f"""ALTER TABLE {name} DELETE IN PARTITION {i} WHERE a = {i}"""
            )

        for i in range(1, numbers, 3):
            instance_test_mutations.query(
                f"""ALTER TABLE {name} UPDATE b = 'changed' IN PARTITION {i} WHERE a = {i} """
            )

        def count_and_changed():
            return instance_test_mutations.query(
                f"SELECT count(), countIf(b == 'changed') FROM {name} SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT CSV"
            ).splitlines()

        all_done = False
        for wait_times_for_mutation in range(
            100
        ):  # wait for replication 80 seconds max
            time.sleep(0.8)

            if count_and_changed() == ["66,33"]:
                all_done = True
                break

        print(
            instance_test_mutations.query(
                f"SELECT mutation_id, command, parts_to_do, is_done, latest_failed_part, latest_fail_reason, parts_to_do_names FROM system.mutations WHERE table = '{name}' SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT TSVWithNames"
            )
        )

        assert (count_and_changed(), all_done) == (["66,33"], True)
        assert instance_test_mutations.query(
            f"SELECT count(), sum(is_done) FROM system.mutations WHERE table = '{name}' SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT CSV"
        ).splitlines() == ["67,67"]

    finally:
        instance_test_mutations.query(f"""DROP TABLE {name}""")


@pytest.mark.parametrize("sync", [("last",), ("all",)])
def test_mutations_in_partition_sync(started_cluster, sync):
    try:
        numbers = 10

        name = "test_mutations_in_partition_sync"
        instance_test_mutations.query(
            f"""CREATE TABLE {name} (date Date, a UInt64, b String) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY a"""
        )
        instance_test_mutations.query(
            f"""INSERT INTO {name} SELECT '2019-07-29' AS date, number, toString(number) FROM numbers({numbers})"""
        )

        for i in range(0, numbers, 3):
            instance_test_mutations.query(
                f"""ALTER TABLE {name} DELETE IN PARTITION {i} WHERE a = {i}"""
                + (" SETTINGS mutations_sync = 1" if sync == "all" else "")
            )

        for reverse_index, i in reversed(
            list(enumerate(reversed(range(1, numbers, 3))))
        ):
            instance_test_mutations.query(
                f"""ALTER TABLE {name} UPDATE b = 'changed' IN PARTITION {i} WHERE a = {i}"""
                + (
                    " SETTINGS mutations_sync = 1"
                    if not reverse_index or sync == "all"
                    else ""
                )
            )

        def count_and_changed():
            return instance_test_mutations.query(
                f"SELECT count(), countIf(b == 'changed') FROM {name} SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT CSV"
            ).splitlines()

        print(
            instance_test_mutations.query(
                f"SELECT mutation_id, command, parts_to_do, is_done, latest_failed_part, latest_fail_reason FROM system.mutations WHERE table = '{name}' SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT TSVWithNames"
            )
        )

        assert count_and_changed() == ["6,3"]
        assert instance_test_mutations.query(
            f"SELECT count(), sum(is_done) FROM system.mutations WHERE table = '{name}' SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT CSV"
        ).splitlines() == ["7,7"]

    finally:
        instance_test_mutations.query(f"""DROP TABLE {name}""")


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


def test_mutations_will_not_hang_for_non_existing_parts_sync(started_cluster):
    try:
        numbers = 100

        name = "test_mutations_will_not_hang_for_non_existing_parts_sync"
        instance_test_mutations.query(
            f"""CREATE TABLE {name} (date Date, a UInt64, b String) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY a"""
        )
        instance_test_mutations.query(
            f"""INSERT INTO {name} SELECT '2019-07-29' AS date, number, toString(number) FROM numbers({numbers})"""
        )

        for i in range(0, numbers, 3):
            instance_test_mutations.query(
                f"""ALTER TABLE {name} DELETE IN PARTITION {i+1000} WHERE a = {i} SETTINGS mutations_sync = 1"""
            )

        def count():
            return instance_test_mutations.query(
                f"SELECT count() FROM {name} SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT CSV"
            ).splitlines()

        print(
            instance_test_mutations.query(
                f"SELECT mutation_id, command, parts_to_do, is_done, latest_failed_part, latest_fail_reason, parts_to_do_names FROM system.mutations WHERE table = '{name}' SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT TSVWithNames"
            )
        )

        assert count() == [f"{numbers}"]
        assert instance_test_mutations.query(
            f"SELECT count(), sum(is_done) FROM system.mutations WHERE table = '{name}' SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT CSV"
        ).splitlines() == [f"34,34"]

    finally:
        instance_test_mutations.query(f"""DROP TABLE {name}""")


def test_mutations_will_not_hang_for_non_existing_parts_async(started_cluster):
    try:
        numbers = 100

        name = "test_mutations_will_not_hang_for_non_existing_parts_async"
        instance_test_mutations.query(
            f"""CREATE TABLE {name} (date Date, a UInt64, b String) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY a"""
        )
        instance_test_mutations.query(
            f"""INSERT INTO {name} SELECT '2019-07-29' AS date, number, toString(number) FROM numbers({numbers})"""
        )

        for i in range(0, numbers, 3):
            instance_test_mutations.query(
                f"""ALTER TABLE {name} DELETE IN PARTITION {i+1000} WHERE a = {i}"""
            )

        def count():
            return instance_test_mutations.query(
                f"SELECT count() FROM {name} SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT CSV"
            ).splitlines()

        def count_and_sum_is_done():
            return instance_test_mutations.query(
                f"SELECT count(), sum(is_done) FROM system.mutations WHERE table = '{name}' SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT CSV"
            ).splitlines()

        all_done = False
        for wait_times_for_mutation in range(
            100
        ):  # wait for replication 80 seconds max
            time.sleep(0.8)

            if count_and_sum_is_done() == ["34,34"]:
                all_done = True
                break

        print(
            instance_test_mutations.query(
                f"SELECT mutation_id, command, parts_to_do, is_done, latest_failed_part, latest_fail_reason, parts_to_do_names FROM system.mutations WHERE table = '{name}' SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT TSVWithNames"
            )
        )

        assert count() == [f"{numbers}"]
        assert count_and_sum_is_done() == ["34,34"]

    finally:
        instance_test_mutations.query(f"""DROP TABLE {name}""")
