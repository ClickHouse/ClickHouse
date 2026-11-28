"""A mutation of a part whose TTL has expired for every row must wait for free space on the
part's own disk of a multi-disk volume, like the mutation of any other part, instead of
writing into that disk while it is full and failing with "No space left on device"."""

import logging
import re
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

CASES = ["recompress", "drop_parts", "replicated"]

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_configuration.xml"],
    tmpfs=[f"/mut_expired_{case}_{i}:size=64M" for case in CASES for i in (1, 2)],
    with_zookeeper=True,
)

# One insert of this many 1 KiB incompressible strings makes a single part of about 18.7 MB.
ROWS = 18000

# Next to that part, this many 1 KiB strings leave less unreserved space on a 64 MiB disk
# than the part holds, so a rewrite of the part cannot fit there.
FILLER_ROWS = 34000

DELETE_TTL = "ts + INTERVAL 1 DAY DELETE"
RECOMPRESS_TTL = "ts + INTERVAL 10 YEAR RECOMPRESS CODEC(ZSTD(3))"

SIZE_UNITS = {"B": 1, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30}


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def query_int(query):
    return int(node.query(query).strip())


def create_table(case, table):
    if case == "replicated":
        zookeeper_path = f"/clickhouse/tables/{table}_{uuid.uuid4().hex}"
        engine = f"ReplicatedMergeTree('{zookeeper_path}', 'r1')"
    else:
        engine = "MergeTree"

    ttl = DELETE_TTL if case == "drop_parts" else f"{DELETE_TTL}, {RECOMPRESS_TTL}"
    extra_settings = ", ttl_only_drop_parts = 1" if case == "drop_parts" else ""

    node.query(
        f"CREATE TABLE {table} (id UInt64, v UInt64, s String, ts DateTime) "
        f"ENGINE = {engine} ORDER BY id TTL {ttl} "
        f"SETTINGS storage_policy = 'jbod_{case}', min_bytes_for_wide_part = 1, "
        f"max_postpone_time_for_failed_mutations_ms = 1000{extra_settings}"
    )


def unreserved_space(disk):
    return query_int(f"SELECT unreserved_space FROM system.disks WHERE name = '{disk}'")


def not_enough_space_errors():
    return query_int(
        "SELECT sum(value) FROM system.errors WHERE name = 'NOT_ENOUGH_SPACE'"
    )


def mutation_part_log(table, table_uuid, since):
    """Rows of part_log about a mutation of `table` that started after `since`."""
    node.query("SYSTEM FLUSH LOGS part_log")
    return node.query(
        "SELECT event_type, error, exception FROM system.part_log "
        f"WHERE database = currentDatabase() AND table = '{table}' "
        f"AND table_uuid = '{table_uuid}' AND event_time_microseconds > '{since}' "
        "AND event_type IN ('MutatePartStart', 'MutatePart') "
        "ORDER BY event_time_microseconds FORMAT Vertical"
    ).strip()


def mutation_state(table):
    return node.query(
        "SELECT is_done, parts_to_do, latest_fail_reason FROM system.mutations "
        f"WHERE database = currentDatabase() AND table = '{table}' FORMAT Vertical"
    ).strip()


def mutation_is_done(table):
    done = node.query(
        "SELECT min(is_done) FROM system.mutations "
        f"WHERE database = currentDatabase() AND table = '{table}'"
    )
    return done.strip() == "1"


def max_reserved_bytes_since(disk, since):
    """The largest space reservation made on `disk` after `since`, read from the server log."""
    node.query("SYSTEM FLUSH LOGS text_log")
    messages = node.query(
        "SELECT message FROM system.text_log "
        f"WHERE event_time_microseconds > '{since}' "
        f"AND message LIKE 'Reserved % on local disk `{disk}`%'"
    )
    sizes = [
        float(value) * SIZE_UNITS[unit]
        for value, unit in re.findall(r"Reserved ([0-9.]+) (B|KiB|MiB|GiB) ", messages)
    ]
    logging.info("reservations on disk %s since %s: %s bytes", disk, since, sizes)
    return max(sizes, default=0)


@pytest.mark.parametrize("case", CASES)
def test_mutation_of_expired_part_waits_for_space(started_cluster, case):
    table = f"t_{case}"
    filler = f"filler_{case}"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(f"DROP TABLE IF EXISTS {filler} SYNC")

    try:
        create_table(case, table)
        node.query(f"SYSTEM STOP TTL MERGES {table}")
        node.query(
            f"INSERT INTO {table} SELECT number, number, randomString(1024), "
            f"now() - INTERVAL 10 DAY FROM numbers({ROWS}) SETTINGS max_insert_threads = 1"
        )

        parts = node.query(
            "SELECT disk_name, bytes_on_disk, delete_ttl_info_max <= now() FROM system.parts "
            f"WHERE database = currentDatabase() AND table = '{table}' AND active FORMAT TSV"
        ).strip()
        assert len(parts.splitlines()) == 1, f"expected exactly one part:\n{parts}"
        part_disk, part_bytes, expired = parts.split("\t")
        part_bytes = int(part_bytes)
        assert expired == "1", f"the part has rows whose TTL has not expired: {parts}"

        disk_index = part_disk[-1]
        other_disk = f"d_{case}_{'2' if disk_index == '1' else '1'}"

        node.query(
            f"CREATE TABLE {filler} (s String) ENGINE = MergeTree ORDER BY tuple() "
            f"SETTINGS storage_policy = 'only_{case}_{disk_index}'"
        )
        node.query(f"SYSTEM STOP MERGES {filler}")
        node.query(
            f"INSERT INTO {filler} SELECT randomString(1024) FROM numbers({FILLER_ROWS})"
        )

        # A rewrite of the part cannot fit on its own disk, while the other disk of the
        # volume has enough space for the mutation to be selected.
        part_disk_unreserved = unreserved_space(part_disk)
        other_disk_unreserved = unreserved_space(other_disk)
        assert part_disk_unreserved < part_bytes, (
            f"disk {part_disk} has {part_disk_unreserved} bytes unreserved, "
            f"enough for the {part_bytes} bytes of the part"
        )
        assert other_disk_unreserved / 1.1 >= part_bytes, (
            f"disk {other_disk} has only {other_disk_unreserved} bytes unreserved, "
            f"so a mutation of the {part_bytes} bytes part would not be selected"
        )

        table_uuid = node.query(
            "SELECT uuid FROM system.tables "
            f"WHERE database = currentDatabase() AND name = '{table}'"
        ).strip()
        since = node.query("SELECT now64(6)").strip()
        refusals_before = not_enough_space_errors()
        node.query(f"ALTER TABLE {table} UPDATE s = concat(s, 'x') WHERE 1")

        deadline = time.monotonic() + 60
        while True:
            refusals = not_enough_space_errors() - refusals_before
            started = mutation_part_log(table, table_uuid, since)
            if refusals > 0 or started:
                break
            if time.monotonic() > deadline:
                raise AssertionError(
                    f"the mutation of {table} was neither refused nor started within 60s; "
                    f"mutation state:\n{mutation_state(table)}"
                )
            time.sleep(1)

        assert not started, (
            f"the mutation of {table} started while disk {part_disk} had "
            f"{part_disk_unreserved} bytes unreserved for a {part_bytes} bytes part:\n{started}"
        )
        assert refusals > 0

        node.query(f"DROP TABLE {filler} SYNC")

        deadline = time.monotonic() + 120
        while not mutation_is_done(table):
            if time.monotonic() > deadline:
                raise AssertionError(
                    f"the mutation of {table} is not done 120s after the disk was freed; "
                    f"mutation state:\n{mutation_state(table)}"
                )
            time.sleep(1)

        reserved = max_reserved_bytes_since(part_disk, since)
        assert reserved >= part_bytes, (
            f"the mutation reserved {reserved} bytes on disk {part_disk} "
            f"for a {part_bytes} bytes part"
        )

        # With ttl_only_drop_parts, the mutation leaves an empty part in place of a part
        # whose rows have all expired.
        expected_rows = "0\t0" if case == "drop_parts" else f"{ROWS}\t{ROWS}"
        assert (
            node.query(
                f"SELECT count(), countIf(endsWith(s, 'x')) FROM {table}"
            ).strip()
            == expected_rows
        )
    finally:
        node.query(f"DROP TABLE IF EXISTS {filler} SYNC")
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
