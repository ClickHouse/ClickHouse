import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import PurePosixPath

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/storage.xml"], with_zookeeper=True, stay_alive=True
)
TABLE = "local_copy_cancellation"
FAILPOINT = "copy_local_file_pause_after_chunk"
TABLE_FILTER = f"database = 'default' AND table = '{TABLE}'"
ACTIVE_PARTS = f"{TABLE_FILTER} AND active"


@pytest.fixture(scope="module")
def started_cluster() -> Iterator[ClickHouseCluster]:
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_for(condition: Callable[[], bool], description: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.1)
    pytest.fail(f"Timed out after {timeout}s waiting for {description}")


def paused_copies() -> int:
    # The wait frame excludes function entry before the pause is registered.
    return int(
        node.query(
            "SELECT count() FROM system.stack_trace "
            "WHERE arrayExists(address -> position(addressToSymbol(address), "
            "'notifyPauseAndWaitForResume') > 0, trace) "
            "AND arrayExists(address -> position(addressToSymbol(address), "
            "'condition_variable4waitER') > 0 OR position(addressToSymbol(address), "
            "'pthread_cond_wait') > 0, trace)",
            settings={"allow_introspection_functions": 1},
            timeout=10,
        )
    )


def file_size(path: PurePosixPath) -> int:
    if not node.path_exists(str(path)):
        return 0
    return int(node.exec_in_container(["stat", "-c", "%s", str(path)]))


def copied_bytes(path: PurePosixPath) -> int:
    if not node.path_exists(str(path)):
        return 0
    sizes = node.exec_in_container(["find", str(path), "-type", "f", "-printf", "%s\n"])
    return sum(int(size) for size in sizes.splitlines())


def assert_unthrottled_local_copy() -> None:
    assert (
        node.query(
            "SELECT count() FROM system.settings "
            "WHERE name IN ('max_local_read_bandwidth', 'max_local_write_bandwidth') "
            "AND value = '0'"
        )
        == "2\n"
    )
    assert (
        node.query(
            "SELECT count() FROM system.server_settings "
            "WHERE name IN ('max_local_read_bandwidth_for_server', "
            "'max_local_write_bandwidth_for_server') AND value = '0'"
        )
        == "2\n"
    )


def test_keeper_expiry_cancels_local_copy(started_cluster: ClickHouseCluster) -> None:
    assert_unthrottled_local_copy()
    node.query(
        f"CREATE TABLE {TABLE} (id UInt64, payload String CODEC(NONE)) "
        f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/{TABLE}', 'node') "
        "ORDER BY id SETTINGS storage_policy = 'local_copy', "
        "min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
    )
    node.query(
        f"INSERT INTO {TABLE} SELECT number, rightPad(toString(number), 4096, 'x') "
        "FROM numbers(8192)"
    )
    part, source_directory = (
        node.query(f"SELECT name, path FROM system.parts WHERE {ACTIVE_PARTS}")
        .strip()
        .split("\t")
    )
    source_path = PurePosixPath(source_directory)
    source_root = PurePosixPath(
        node.query("SELECT path FROM system.disks WHERE name = 'source'").strip()
    )
    destination_root = PurePosixPath(
        node.query("SELECT path FROM system.disks WHERE name = 'destination'").strip()
    )
    clone = (
        destination_root / source_path.relative_to(source_root).parent / "moving" / part
    )
    payload_size = file_size(source_path / "payload.bin")
    assert payload_size > 8 * 1024 * 1024
    row_query = f"SELECT count(), sum(id), sum(cityHash64(payload)) FROM {TABLE}"
    expected_rows = node.query(row_query)
    checksum_query = (
        "SELECT name, hash_of_all_files, hash_of_uncompressed_files "
        f"FROM system.parts WHERE {ACTIVE_PARTS}"
    )
    expected_checksums = node.query(checksum_query)
    session = node.query(
        "SELECT client_id FROM system.zookeeper_connection WHERE name = 'default'"
    ).strip()
    assert session
    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    try:
        node.query(
            f"ALTER TABLE {TABLE} MOVE PART '{part}' TO DISK 'destination' "
            "SETTINGS alter_move_to_space_execute_async = 1"
        )

        previous_copied = None

        def partially_copied() -> bool:
            nonlocal previous_copied
            disk = node.query(
                f"SELECT disk_name FROM system.parts WHERE {ACTIVE_PARTS}"
            ).strip()
            assert (
                disk == "source"
            ), "Local copy completed without pausing between chunks"
            if previous_copied is not None and copied_bytes(clone) <= previous_copied:
                return False
            if paused_copies() != 1:
                return False
            copied = file_size(clone / "payload.bin")
            if 0 < copied < payload_size:
                logging.info(
                    "Local copy paused after %s of %s bytes", copied, payload_size
                )
                return True
            # Metadata files can be copied before the payload.
            previous_copied = copied_bytes(clone)
            node.query(f"SYSTEM NOTIFY FAILPOINT {FAILPOINT}")
            return False

        wait_for(partially_copied, "a paused, partially written local file", 60)
        assert (
            node.query(f"SELECT count() FROM system.moves WHERE {TABLE_FILTER}")
            == "1\n"
        )

        with PartitionManager() as partition:
            partition.drop_instance_zk_connections(node)
            wait_for(
                lambda: node.query(
                    f"SELECT is_readonly AND is_session_expired "
                    f"FROM system.replicas WHERE {TABLE_FILTER}",
                    timeout=10,
                )
                == "1\n",
                "Keeper session expiry and readonly replica",
                180,
            )
            # `is_readonly` is set before the move cancellation blocker is held.
            wait_for(
                lambda: node.query(
                    "SELECT count() FROM system.stack_trace "
                    "WHERE thread_name = 'BgSchPool' "
                    "AND arrayExists(address -> position(addressToSymbol(address), "
                    "'partialShutdown') > 0, trace) "
                    "AND arrayExists(address -> position(addressToSymbol(address), "
                    "'removeTasksCorrespondingToStorage') > 0, trace)",
                    settings={"allow_introspection_functions": 1},
                    timeout=10,
                )
                == "1\n",
                "replica shutdown waiting for the move executor",
                60,
            )

        def keeper_reconnected() -> bool:
            try:
                replacement = node.query(
                    "SELECT client_id FROM system.zookeeper_connection "
                    "WHERE name = 'default' AND NOT is_expired",
                    timeout=10,
                ).strip()
                return bool(replacement) and replacement != session
            except QueryRuntimeException:
                return False

        wait_for(keeper_reconnected, "a replacement Keeper session", 60)
        reconnected_at = time.monotonic()
        assert (
            node.query(f"SELECT is_readonly FROM system.replicas WHERE {TABLE_FILTER}")
            == "1\n"
        )
        assert paused_copies() == 1
        assert 0 < file_size(clone / "payload.bin") < payload_size
        node.query(f"SYSTEM NOTIFY FAILPOINT {FAILPOINT}")
        wait_for(
            lambda: node.query(
                f"SELECT is_readonly FROM system.replicas WHERE {TABLE_FILTER}",
                timeout=10,
            )
            == "0\n",
            "readonly recovery after Keeper reconnected",
            60 - (time.monotonic() - reconnected_at),
        )
        recovery_seconds = time.monotonic() - reconnected_at
        assert recovery_seconds <= 60
        logging.info(
            "Readonly cleared %.3fs after Keeper reconnected", recovery_seconds
        )
        assert (
            node.query(f"SELECT count() FROM system.moves WHERE {TABLE_FILTER}")
            == "0\n"
        )
        assert not node.path_exists(str(clone))
        assert (
            node.query(f"SELECT disk_name FROM system.parts WHERE {ACTIVE_PARTS}")
            == "source\n"
        )
        assert node.query(row_query) == expected_rows
        assert node.query(checksum_query) == expected_checksums
        assert file_size(source_path / "payload.bin") == payload_size
        assert (
            node.query(
                f"CHECK TABLE {TABLE}", settings={"check_query_single_value_result": 1}
            )
            == "1\n"
        )
        node.query("SYSTEM FLUSH LOGS part_log")
        assert (
            node.query(
                "SELECT errorCodeToName(error) FROM system.part_log "
                f"WHERE {TABLE_FILTER} AND event_type = 'MovePart' AND part_name = '{part}' "
                "AND table_uuid = (SELECT uuid FROM system.tables "
                f"WHERE database = 'default' AND name = '{TABLE}')"
            )
            == "ABORTED\n"
        )

        node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        node.query(f"ALTER TABLE {TABLE} MOVE PART '{part}' TO DISK 'destination'")
        assert (
            node.query(f"SELECT disk_name FROM system.parts WHERE {ACTIVE_PARTS}")
            == "destination\n"
        )
        assert not node.path_exists(str(clone))
        assert node.query(row_query) == expected_rows
        assert node.query(checksum_query) == expected_checksums
        assert (
            node.query(
                f"CHECK TABLE {TABLE}", settings={"check_query_single_value_result": 1}
            )
            == "1\n"
        )
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        node.query_with_retry(f"DROP TABLE IF EXISTS {TABLE} SYNC", timeout=10)


@dataclass(frozen=True)
class LocalPart:
    table: str
    name: str
    source_path: PurePosixPath
    clone: PurePosixPath
    payload_size: int
    expected_rows: str
    expected_checksums: str

    @property
    def table_filter(self) -> str:
        return f"database = 'default' AND table = '{self.table}'"

    @property
    def active_parts(self) -> str:
        return f"{self.table_filter} AND active"

    @property
    def row_query(self) -> str:
        return f"SELECT count(), sum(id), sum(cityHash64(payload)) FROM {self.table}"

    @property
    def checksum_query(self) -> str:
        return (
            "SELECT name, hash_of_all_files, hash_of_uncompressed_files "
            f"FROM system.parts WHERE {self.active_parts}"
        )

    def moving(self) -> bool:
        return (
            node.query(f"SELECT count() FROM system.moves WHERE {self.table_filter}")
            == "1\n"
        )

    def assert_intact(self, disk: str) -> None:
        assert (
            node.query(f"SELECT disk_name FROM system.parts WHERE {self.active_parts}")
            == f"{disk}\n"
        )
        assert node.query(self.row_query) == self.expected_rows
        assert node.query(self.checksum_query) == self.expected_checksums
        assert (
            node.query(
                f"CHECK TABLE {self.table}",
                settings={"check_query_single_value_result": 1},
            )
            == "1\n"
        )
        if disk == "source":
            assert file_size(self.source_path / "payload.bin") == self.payload_size


def test_concurrent_local_copy_cancellation_and_restart(
    started_cluster: ClickHouseCluster,
) -> None:
    assert_unthrottled_local_copy()
    tables = ("concurrent_local_copy_target", "concurrent_local_copy_other")
    pid = node.get_process_pid("clickhouse")
    assert pid is not None

    source_root = PurePosixPath(
        node.query("SELECT path FROM system.disks WHERE name = 'source'").strip()
    )
    destination_root = PurePosixPath(
        node.query("SELECT path FROM system.disks WHERE name = 'destination'").strip()
    )

    def create_part(table: str, rows: int) -> LocalPart:
        node.query(
            f"CREATE TABLE {table} (id UInt64, payload String CODEC(NONE)) "
            "ENGINE = MergeTree ORDER BY id SETTINGS "
            "storage_policy = 'local_copy', "
            "min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
        )
        node.query(
            f"INSERT INTO {table} SELECT number, rightPad(toString(number), 4096, 'x') "
            f"FROM numbers({rows})"
        )
        active_parts = f"database = 'default' AND table = '{table}' AND active"
        name, source_directory = (
            node.query(f"SELECT name, path FROM system.parts WHERE {active_parts}")
            .strip()
            .split("\t")
        )
        source_path = PurePosixPath(source_directory)
        clone = (
            destination_root
            / source_path.relative_to(source_root).parent
            / "moving"
            / name
        )
        payload_size = file_size(source_path / "payload.bin")
        assert payload_size > 8 * 1024 * 1024
        return LocalPart(
            table=table,
            name=name,
            source_path=source_path,
            clone=clone,
            payload_size=payload_size,
            expected_rows=node.query(
                f"SELECT count(), sum(id), sum(cityHash64(payload)) FROM {table}"
            ),
            expected_checksums=node.query(
                "SELECT name, hash_of_all_files, hash_of_uncompressed_files "
                f"FROM system.parts WHERE {active_parts}"
            ),
        )

    def start_move(part: LocalPart) -> None:
        node.query(
            f"ALTER TABLE {part.table} MOVE PART '{part.name}' TO DISK 'destination' "
            "SETTINGS alter_move_to_space_execute_async = 1"
        )

    try:
        target = create_part(tables[0], 8192)
        other = create_part(tables[1], 32768)
        node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
        start_move(other)
        previous_copied = None
        move_threads = None

        def both_partially_copied() -> bool:
            nonlocal previous_copied, move_threads
            for part in (target, other):
                assert (
                    node.query(
                        f"SELECT disk_name FROM system.parts WHERE {part.active_parts}"
                    )
                    == "source\n"
                ), f"{part.table} completed its local copy without pausing"
            if previous_copied is not None:
                assert move_threads is not None
                if any(
                    copied_bytes(part.clone) <= previous
                    for part, previous in zip((target, other), previous_copied)
                ):
                    return False
            if paused_copies() != 2:
                return False
            if move_threads is None:
                move_threads = tuple(
                    int(
                        node.query(
                            "SELECT thread_id FROM system.moves "
                            f"WHERE {part.table_filter}"
                        )
                    )
                    for part in (target, other)
                )
                assert len(set(move_threads)) == 2
            copied = [file_size(part.clone / "payload.bin") for part in (target, other)]
            if all(
                0 < size < part.payload_size
                for part, size in zip((target, other), copied)
            ):
                assert target.moving() and other.moving()
                logging.info(
                    "Two local moves paused: target %s / %s bytes, other %s / %s bytes",
                    copied[0],
                    target.payload_size,
                    copied[1],
                    other.payload_size,
                )
                return True
            # Each chunk advances the clone, including metadata before the payload.
            previous_copied = tuple(
                copied_bytes(part.clone) for part in (target, other)
            )
            node.query(f"SYSTEM NOTIFY FAILPOINT {FAILPOINT}")
            return False

        for attempt in range(1, 3):
            previous_copied = None
            move_threads = None
            start_move(target)
            wait_for(
                both_partially_copied,
                "two local moves with paused, partially written files",
                60,
            )
            assert move_threads is not None
            other_before_notify = copied_bytes(other.clone)
            node.query(f"SYSTEM STOP MOVES {target.table}")
            # The cancelled move exits; the unrelated move pauses after its next chunk.
            node.query(f"SYSTEM NOTIFY FAILPOINT {FAILPOINT}")
            wait_for(
                lambda: not target.moving()
                and copied_bytes(other.clone) > other_before_notify
                and paused_copies() == 1,
                "target cancellation while the unrelated move remains paused",
                60,
            )
            assert not node.path_exists(str(target.clone))
            target.assert_intact("source")
            assert other.moving()
            other_copied = file_size(other.clone / "payload.bin")
            other_total_bytes = copied_bytes(other.clone)
            assert 0 < other_copied < other.payload_size
            other.assert_intact("source")
            assert file_size(other.clone / "payload.bin") == other_copied
            assert copied_bytes(other.clone) == other_total_bytes
            assert paused_copies() == 1
            node.query("SYSTEM FLUSH LOGS part_log")
            assert (
                node.query(
                    "SELECT errorCodeToName(error) FROM system.part_log "
                    f"WHERE {target.table_filter} AND event_type = 'MovePart' "
                    f"AND part_name = '{target.name}' AND table_uuid = "
                    "(SELECT uuid FROM system.tables "
                    f"WHERE database = 'default' AND name = '{target.table}')"
                )
                == "ABORTED\n" * attempt
            )
            logging.info(
                "Target cancellation %s completed while unrelated copy stayed at %s bytes",
                attempt,
                other_copied,
            )
            node.query(f"SYSTEM START MOVES {target.table}")

        node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        wait_for(lambda: not other.moving(), "the unrelated local move to finish", 60)
        assert paused_copies() == 0
        assert not node.path_exists(str(other.clone))
        target.assert_intact("source")
        other.assert_intact("destination")

        assert node.get_process_pid("clickhouse") == pid
        assert node.stop_clickhouse(stop_wait_sec=60) is True
        node.start_clickhouse()
        restarted_pid = node.get_process_pid("clickhouse")
        assert restarted_pid is not None and restarted_pid != pid
        logging.info(
            "ClickHouse restarted after cancellation: PID %s -> %s", pid, restarted_pid
        )
        target.assert_intact("source")
        other.assert_intact("destination")

        node.query(
            f"ALTER TABLE {target.table} MOVE PART '{target.name}' TO DISK 'destination'"
        )
        target.assert_intact("destination")
        assert not node.path_exists(str(target.clone))
        node.query(
            f"ALTER TABLE {other.table} MOVE PART '{other.name}' TO DISK 'source'"
        )
        other.assert_intact("source")
        other_return_clone = other.source_path.parent / "moving" / other.name
        assert not node.path_exists(str(other_return_clone))
    finally:
        if node.get_process_pid("clickhouse") is None:
            node.start_clickhouse()
        node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        for table in tables:
            if node.query(f"EXISTS TABLE {table}") == "1\n":
                node.query(f"SYSTEM START MOVES {table}")
                node.query_with_retry(f"DROP TABLE IF EXISTS {table} SYNC", timeout=10)
