#!/usr/bin/env python3

import pytest
from kazoo.security import make_acl

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown(ignore_fatal=True)


def get_fake_zk(timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, "node", timeout=timeout)


# A `setACL` request is preprocessed into two deltas, in this order: a
# `SetACLDelta` followed by an `UpdateNodeStatDelta` that bumps `aversion`
# (which, unlike the ACLs, is part of the node digest).
#
# When updating the uncommitted digest, `UncommittedState::applyDelta` is
# supposed to subtract a node's previous digest the first time the node is
# touched in a transaction and add the new digest at the end. Every delta marks
# the node as "seen", but the `SetACLDelta` branch never performs the
# subtraction. Because the `SetACLDelta` is processed first, it consumes the
# "first touch" without subtracting, and the following `UpdateNodeStatDelta`
# then skips its own subtraction. The preprocessed digest therefore ends up too
# large by the old node digest.
#
# With `digest_enabled_on_commit` enabled, the committed digest is recomputed
# from the actual storage on commit (correctly), so it disagrees with the digest
# the leader embedded into the log entry during preprocessing, and `assertDigest`
# terminates the server. This test exercises that scenario: it fails on the
# buggy code (the server terminates while committing the `setACL`) and passes
# once the digest accounting is fixed.
def test_keeper_set_acl_digest_on_commit(started_cluster):
    if (
        node.is_built_with_thread_sanitizer()
        or node.is_built_with_address_sanitizer()
        or node.is_built_with_memory_sanitizer()
    ):
        pytest.skip("doesn't fit in timeouts for stacktrace generation")

    keeper_utils.wait_until_connected(cluster, node)

    zk = None
    try:
        zk = get_fake_zk()

        # `Create` digests are computed correctly, so this commits successfully.
        zk.create("/test_set_acl", b"data")

        # Fire the `setACL` without waiting for the response: if the bug is present
        # the server terminates while committing it and the connection dies.
        acl = make_acl("world", "anyone", all=True)
        zk.set_acls_async("/test_set_acl", acls=[acl])

        # If the digest accounting is broken the server logs this fatal message and
        # terminates; if it is correct the line never appears and the wait times out.
        crashed = True
        try:
            node.wait_for_log_line(
                "Digest for nodes is not matching after committing request of type 'SetACL' at log index",
                timeout=20,
            )
        except Exception:
            crashed = False

        assert not crashed, (
            "Keeper terminated while committing a SetACL request: the digest "
            "computed during preprocessing does not match the digest recomputed "
            "on commit (digest_enabled_on_commit)"
        )

        # The server must still be serving and the SetACL must have been applied
        # (a single SetACL bumps the node's aversion from 0 to 1).
        keeper_utils.wait_until_connected(cluster, node)
        verify_zk = get_fake_zk()
        try:
            stat = verify_zk.exists("/test_set_acl")
            assert stat is not None
            assert stat.aversion == 1
        finally:
            verify_zk.stop()
            verify_zk.close()
    finally:
        try:
            if zk is not None:
                zk.stop()
                zk.close()
        except:
            pass
        # Kill the server (already dead if the bug is present, still running once
        # fixed) so the module teardown is not confused by a fatal log line.
        node.stop_clickhouse(kill=True)
