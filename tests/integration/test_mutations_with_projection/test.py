import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

instance_test_mutations = cluster.add_instance(
    "test_mutations_with_projection",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        instance_test_mutations.query(
            """
        CREATE TABLE video_log
        (
        `datetime` DateTime, -- 20,000 records per second
        `user_id` UInt64, -- Cardinality == 100,000,000
        `device_id` UInt64, -- Cardinality == 200,000,000
        `video_id` UInt64, -- Cardinality == 100,00000
        `domain` LowCardinality(String), -- Cardinality == 100
        `bytes` UInt64, -- Ranging from 128 to 1152
        `duration` UInt64, -- Ranging from 100 to 400
        PROJECTION p_norm (SELECT datetime, device_id, bytes, duration ORDER BY device_id),
        PROJECTION p_agg (SELECT toStartOfHour(datetime) AS hour, domain, sum(bytes), avg(duration) GROUP BY hour, domain)
        )
        ENGINE = MergeTree
        PARTITION BY toDate(datetime) -- Daily partitioning
        ORDER BY (user_id, device_id, video_id) -- Can only favor one column here
        SETTINGS index_granularity = 1000;
        """
        )

        instance_test_mutations.query(
            """CREATE TABLE rng (`user_id_raw` UInt64, `device_id_raw` UInt64, `video_id_raw` UInt64, `domain_raw` UInt64, `bytes_raw` UInt64, `duration_raw` UInt64) ENGINE = GenerateRandom(1024);"""
        )

        instance_test_mutations.query(
            """INSERT INTO video_log SELECT toUnixTimestamp(toDateTime(today())) + (rowNumberInAllBlocks() / 20000), user_id_raw % 100000000 AS user_id, device_id_raw % 200000000 AS device_id, video_id_raw % 100000000 AS video_id, domain_raw % 100, (bytes_raw % 1024) + 128, (duration_raw % 300) + 100 FROM rng LIMIT 500000;"""
        )

        instance_test_mutations.query("""OPTIMIZE TABLE video_log FINAL;""")

        yield cluster
    finally:
        cluster.shutdown()


def test_mutations_with_multi_level_merge_of_projections(started_cluster):
    try:
        instance_test_mutations.query(
            """ALTER TABLE video_log UPDATE bytes = bytes + 10086 WHERE 1;"""
        )

        def count_and_changed():
            return instance_test_mutations.query(
                "SELECT count(), countIf(bytes > 10000) FROM video_log SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT CSV"
            ).splitlines()

        all_done = False
        for wait_times_for_mutation in range(
            100
        ):  # wait for replication 80 seconds max
            time.sleep(0.8)

            if count_and_changed() == ["500000,500000"]:
                all_done = True
                break

        print(
            instance_test_mutations.query(
                "SELECT mutation_id, command, parts_to_do, is_done, latest_failed_part, latest_fail_reason, parts_to_do_names FROM system.mutations WHERE table = 'video_log' SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT TSVWithNames"
            )
        )

        assert (count_and_changed(), all_done) == (["500000,500000"], True)
        assert instance_test_mutations.query(
            f"SELECT DISTINCT arraySort(projections) FROM system.parts WHERE table = 'video_log' SETTINGS force_index_by_date = 0, force_primary_key = 0 FORMAT TSVRaw"
        ).splitlines() == ["['p_agg','p_norm']"]

    finally:
        instance_test_mutations.query(f"""DROP TABLE video_log""")
