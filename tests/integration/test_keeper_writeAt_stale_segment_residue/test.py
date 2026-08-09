"""
Regression for https://github.com/ClickHouse/ClickHouse/issues/112101

Cross-segment writeAt used to acknowledge a rewritten log entry before
asynchronously removing later changelog segments. A crash in that window left a
clean rewrite (duplicated index) plus stale higher-index files; startup then
exited with CORRUPTED_DATA.

This test reconstructs that on-disk residue and asserts Keeper starts and serves.
"""

from __future__ import annotations

import base64
import logging
import struct
from pathlib import PurePosixPath

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
)

LOG_DIR = "/var/lib/clickhouse/coordination/log"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_fake_zk(timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, "node1", timeout=timeout)


def start_clickhouse():
    node1.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, node1)


class _SipHash:
    """ClickHouse SipHash 2-4 (64-bit), little-endian streaming updates."""

    def __init__(self, key0: int = 0, key1: int = 0):
        self.v0 = key0 ^ 0x736F6D6570736575
        self.v1 = key1 ^ 0x646F72616E646F6D
        self.v2 = key0 ^ 0x6C7967656E657261
        self.v3 = key1 ^ 0x7465646279746573
        self.cnt = 0
        self.current_word = 0

    def _sipround(self):
        v0, v1, v2, v3 = self.v0, self.v1, self.v2, self.v3

        def rotl(x, b):
            return ((x << b) & 0xFFFFFFFFFFFFFFFF) | (x >> (64 - b))

        v0 = (v0 + v1) & 0xFFFFFFFFFFFFFFFF
        v1 = rotl(v1, 13)
        v1 ^= v0
        v0 = rotl(v0, 32)
        v2 = (v2 + v3) & 0xFFFFFFFFFFFFFFFF
        v3 = rotl(v3, 16)
        v3 ^= v2
        v0 = (v0 + v3) & 0xFFFFFFFFFFFFFFFF
        v3 = rotl(v3, 21)
        v3 ^= v0
        v2 = (v2 + v1) & 0xFFFFFFFFFFFFFFFF
        v1 = rotl(v1, 17)
        v1 ^= v2
        v2 = rotl(v2, 32)
        self.v0, self.v1, self.v2, self.v3 = v0, v1, v2, v3

    def update(self, data: bytes):
        i = 0
        n = len(data)
        if self.cnt & 7:
            buf = bytearray(struct.pack("<Q", self.current_word))
            while (self.cnt & 7) and i < n:
                buf[self.cnt & 7] = data[i]
                i += 1
                self.cnt += 1
            self.current_word = struct.unpack("<Q", bytes(buf))[0]
            if self.cnt & 7:
                return
            self.v3 ^= self.current_word
            self._sipround()
            self._sipround()
            self.v0 ^= self.current_word

        self.cnt += n - i
        while n - i >= 8:
            word = struct.unpack_from("<Q", data, i)[0]
            self.v3 ^= word
            self._sipround()
            self._sipround()
            self.v0 ^= word
            i += 8

        self.current_word = 0
        rem = data[i:]
        if rem:
            buf = bytearray(8)
            buf[: len(rem)] = rem
            self.current_word = struct.unpack("<Q", bytes(buf))[0]

    def get64(self) -> int:
        buf = bytearray(struct.pack("<Q", self.current_word))
        buf[7] = self.cnt & 0xFF
        word = struct.unpack("<Q", bytes(buf))[0]
        self.v3 ^= word
        self._sipround()
        self._sipround()
        self.v0 ^= word
        self.v2 ^= 0xFF
        self._sipround()
        self._sipround()
        self._sipround()
        self._sipround()
        return self.v0 ^ self.v1 ^ self.v2 ^ self.v3


def _record_checksum(version: int, index: int, term: int, value_type: int, blob: bytes) -> int:
    h = _SipHash()
    h.update(struct.pack("<B", version))
    h.update(struct.pack("<Q", index))
    h.update(struct.pack("<Q", term))
    h.update(struct.pack("<i", value_type))
    h.update(struct.pack("<Q", len(blob)))
    if blob:
        h.update(blob)
    return h.get64()


def _parse_records(data: bytes):
    """Yield (offset, version, index, term, value_type, blob) for uncompressed changelogs."""
    pos = 0
    while pos + 8 + 1 + 8 + 8 + 4 + 8 <= len(data):
        start = pos
        (_checksum,) = struct.unpack_from("<Q", data, pos)
        pos += 8
        version = data[pos]
        pos += 1
        index, term = struct.unpack_from("<QQ", data, pos)
        pos += 16
        (value_type,) = struct.unpack_from("<i", data, pos)
        pos += 4
        (blob_size,) = struct.unpack_from("<Q", data, pos)
        pos += 8
        if pos + blob_size > len(data):
            break
        blob = data[pos : pos + blob_size]
        pos += blob_size
        yield start, version, index, term, value_type, blob


def _build_record(version: int, index: int, term: int, value_type: int, blob: bytes) -> bytes:
    checksum = _record_checksum(version, index, term, value_type, blob)
    return (
        struct.pack("<Q", checksum)
        + struct.pack("<B", version)
        + struct.pack("<QQ", index, term)
        + struct.pack("<i", value_type)
        + struct.pack("<Q", len(blob))
        + blob
    )


def _get_from_to_index(name: str):
    parts = name.replace("changelog_", "").replace(".bin", "").split("_")
    return int(parts[0]), int(parts[1])


def _list_changelogs():
    listing = node1.exec_in_container(["ls", LOG_DIR]).strip()
    if not listing:
        return []
    return [f for f in listing.split("\n") if f.startswith("changelog_") and f.endswith(".bin")]


def _forge_writeAt_residue(rewrite_index: int = 15):
    """
    Emulate crash after writeAt(rewrite_index) appended a newer-term duplicate into
    the earlier segment but before async removal of later segments.
    """
    files = sorted(_list_changelogs(), key=lambda n: _get_from_to_index(n)[0])
    assert files, "expected changelog files"

    target = None
    for name in files:
        frm, to = _get_from_to_index(name)
        if frm <= rewrite_index <= to:
            target = name
            break
    assert target is not None, f"no changelog contains index {rewrite_index}: {files}"

    later = [n for n in files if _get_from_to_index(n)[0] > rewrite_index]
    assert later, f"need stale later segments after {target}, got {files}"

    path = str(PurePosixPath(LOG_DIR) / target)
    b64 = node1.exec_in_container(["base64", "-w0", path]).strip()
    data = base64.b64decode(b64)
    records = list(_parse_records(data))
    assert records, f"failed to parse records from {target}"

    # Clone the last record that is <= rewrite_index as the rewrite payload.
    donor = None
    for rec in records:
        if rec[2] <= rewrite_index:
            donor = rec
    assert donor is not None
    _start, version, _index, term, value_type, blob = donor
    forged = _build_record(version, rewrite_index, term + 1, value_type, blob)

    # Append forge via base64 to avoid shell binary mangling.
    forged_b64 = base64.b64encode(forged).decode("ascii")
    node1.exec_in_container(
        [
            "bash",
            "-c",
            f"echo {forged_b64} | base64 -d >> {path}",
        ]
    )

    logging.info(
        "Forged writeAt residue: appended duplicate index=%s term=%s to %s; leaving stale %s",
        rewrite_index,
        term + 1,
        target,
        later,
    )
    return target, later


def test_keeper_starts_after_writeAt_stale_segment_residue(started_cluster):
    node1_conn = None
    try:
        node1.stop_clickhouse()
        node1.exec_in_container(["rm", "-rf", LOG_DIR])
        node1.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination/snapshots"])
        start_clickhouse()

        node1_conn = get_fake_zk()
        node1_conn.create("/test_writeAt_residue")
        # Enough znodes that with rotate_interval=10 we get multiple changelog segments
        # covering past index 15 and later files that become "stale" after a rewrite.
        for i in range(40):
            node1_conn.create(f"/test_writeAt_residue/n{i}", b"x")

        node1_conn.stop()
        node1_conn.close()
        node1_conn = None

        node1.stop_clickhouse()

        _forge_writeAt_residue(rewrite_index=15)

        # Bug case without the fix: CORRUPTED_DATA / process exit. With the fix, starts cleanly.
        start_clickhouse()
        keeper_utils.wait_until_connected(cluster, node1)

        # Stale later segments should have been detached/removed during changelog init.
        remaining = _list_changelogs()
        active_later = [n for n in remaining if _get_from_to_index(n)[0] > 15]
        assert not active_later, f"stale later changelogs still present: {active_later}"

        node1_conn = get_fake_zk()
        children = node1_conn.get_children("/test_writeAt_residue")
        assert len(children) == 40

        # Must accept new writes (log append still healthy).
        created = node1_conn.create("/test_writeAt_residue_after", b"ok", sequence=True)
        assert node1_conn.get(created)[0] == b"ok"
    finally:
        try:
            if node1_conn:
                node1_conn.stop()
                node1_conn.close()
        except Exception:
            pass
