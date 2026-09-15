import hashlib
import json
import os
import subprocess
import time
import urllib.request

import pytest
import urllib3
from minio import Minio
from urllib3.util import Timeout as _Urllib3Timeout

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

DISK = "disk_cas_renewal"
SERVER_ROOT_ID = "itest-cas-renewal"
STORAGE_POLICY = "cas_mount_renewal"
MOUNT_OBJECT_KEY = "cas_mount_renewal/gc/server-roots/{}/mount".format(SERVER_ROOT_ID)
MOUNT_REQUEST_PATH = "/test/{}".format(MOUNT_OBJECT_KEY)
RENEWAL_EVENTS = (
    "CASMountRenewalAttempts",
    "CASMountRenewalRetries",
    "CASMountRenewalResolved",
    "CASMountRenewalRecovered",
    "CASMountRenewalDeadlineExceeded",
    "CASRemountAttempts",
    "CASRemountSucceeded",
    "CASRemountFailed",
)

# The lease timing `configs/storage_conf.xml` compiles into `disk_cas_renewal`. One fixed budget for
# every build (no sanitizer-conditional scaling): wide enough that a single physical attempt, however
# slow the host, cannot plausibly cross the lease TTL (see the config file's own comment for the
# validateCasRequestBudget arithmetic), while still short enough that the hard-restart tests below
# observe the token-stability wait within a bounded test timeout. Mirrored here (rather than read back
# from the server) so every expectation string/number in this module derives from one place; keep both
# sides in sync with configs/storage_conf.xml.
MOUNT_LEASE_TTL_MS = 10000
MOUNT_RENEW_PERIOD_MS = 2000
ATTEMPT_TIMEOUT_MS = 500
LEASE_SAFETY_MARGIN_MS = 500


def _control(base_url, path, patch=None, timeout=10):
    # `timeout` bounds each individual blocking socket operation (connect, then each read), not the
    # wall-clock time to a fully-read response: a peer that trickles bytes in slowly enough to keep
    # resetting the read timeout, without ever exceeding it, could still keep this call running past
    # the caller's deadline. Accepted: the s3proxy control server this talks to answers in one small,
    # immediate response with nothing in the path that could trickle, and every `_wait_until` probe in
    # this module calls it at most once, so the worst case this leaves open is bounded and small -- not
    # worth a cancellation thread for a well-behaved local test double.
    if patch is None:
        request = urllib.request.Request("{}{}".format(base_url, path))
    else:
        request = urllib.request.Request(
            "{}{}".format(base_url, path),
            data=json.dumps(patch).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode())


class _Deadline:
    """One absolute deadline, shared across a whole `_wait_until` call (every retry) AND across every
    query one of its probes issues. `remaining()` must be re-read before EACH query in a probe that
    issues more than one: reusing a single `remaining()` value across sequential queries would let
    EACH one spend the full remaining budget, so an N-query probe could run up to Nx as long as the
    caller intended before anything ever times out.
    """

    def __init__(self, timeout):
        self._deadline = time.monotonic() + timeout

    def remaining(self):
        return max(0.0, self._deadline - time.monotonic())

    def expired(self):
        return time.monotonic() >= self._deadline


def _wait_until(probe, timeout=40, interval=0.2):
    # `probe` is called with a shared `_Deadline` on every attempt, so it (and every query it issues)
    # can bound its own work at `deadline.remaining()` instead of inheriting a client's much larger
    # default -- otherwise one slow or stuck call could silently burn the whole budget of a caller that
    # is itself waiting on a much tighter deadline. A truthy result that only comes back after this
    # deadline has already passed (the probe ran long enough to blow through its own remaining budget)
    # is rejected here too, rather than accepted as an on-time success.
    deadline = _Deadline(timeout)
    last = None
    while not deadline.expired():
        last = probe(deadline)
        if last and not deadline.expired():
            return last
        time.sleep(min(interval, deadline.remaining()))
    raise AssertionError("condition did not become true within {}s; last={!r}".format(timeout, last))


def _profile_events(node, timeout=None):
    rows = node.query(
        "SELECT event, value FROM system.events WHERE event IN ({}) FORMAT TSV".format(
            ", ".join("'{}'".format(event) for event in RENEWAL_EVENTS)
        ),
        timeout=timeout,
    )
    values = {event: 0 for event in RENEWAL_EVENTS}
    for row in rows.splitlines():
        event, value = row.split("\t")
        values[event] = int(value)
    return values


def _event_delta(before, after):
    return {event: after[event] - before[event] for event in RENEWAL_EVENTS}


def _mount_snapshot(node, timeout=None):
    row = node.query(
        "SELECT renewal_sequence, state, lifecycle, gc_fenced "
        "FROM system.cas_mounts "
        "WHERE disk = '{}' AND server_root_id = '{}' LIMIT 1 FORMAT TSV".format(
            DISK, SERVER_ROOT_ID
        ),
        timeout=timeout,
    ).strip()
    assert row, "the local CAS mount row must be visible"
    sequence, state, lifecycle, gc_fenced = row.split("\t")
    return {
        "sequence": int(sequence),
        "state": state,
        "lifecycle": lifecycle,
        "gc_fenced": int(gc_fenced),
    }


def _rustfs_client(timeout):
    # A fresh, deadline-scoped client rather than the shared `cluster.rustfs_client`: that one's
    # `http_client` (see wait_rustfs_to_start in helpers/cluster.py) has no configured timeout, so a
    # stalled RustFS response through it could block a `_wait_until` probe past its own deadline
    # without ever timing out on its own. Cheap to construct; only used for this module's polling reads.
    #
    # `timeout` bounds each individual blocking socket operation (connect, then each read) through this
    # client, not the wall-clock time to a fully-read response: a peer trickling bytes slowly enough to
    # keep resetting the read timeout, without ever exceeding it, could still run past the caller's
    # deadline. Accepted: RustFS is a local, well-behaved test double (never observed to trickle), and
    # every operation _read_mount_object issues through a client built here recomputes ITS OWN fresh
    # timeout first, so the number of such operations per probe is fixed and small -- not worth a
    # cancellation thread for a local test double.
    return Minio(
        "{}:{}".format(cluster.rustfs_ip, cluster.rustfs_port),
        access_key=cluster.rustfs_access_key,
        secret_key=cluster.rustfs_secret_key,
        secure=False,
        http_client=urllib3.PoolManager(
            cert_reqs="CERT_NONE",
            timeout=_Urllib3Timeout(connect=timeout, read=timeout),
        ),
    )


def _read_mount_object(deadline=None):
    # Three sequential RustFS operations (client construction for the GET, the GET/body-read, then
    # client construction for the HEAD): `deadline.remaining()` is read again before EACH one rather
    # than reused from the first, so a slow GET cannot silently gift the HEAD the same full budget
    # again. Standalone callers (outside any `_wait_until` probe) get a fresh 20s deadline of their own.
    if deadline is None:
        deadline = _Deadline(20)

    remaining = deadline.remaining()
    if remaining <= 0:
        raise AssertionError("_read_mount_object: deadline already expired before the GET")
    response = _rustfs_client(remaining).get_object(cluster.rustfs_bucket, MOUNT_OBJECT_KEY)
    try:
        body = response.read()
    finally:
        response.close()
        response.release_conn()

    remaining = deadline.remaining()
    if remaining <= 0:
        raise AssertionError("_read_mount_object: deadline already expired before the HEAD")
    stat = _rustfs_client(remaining).stat_object(cluster.rustfs_bucket, MOUNT_OBJECT_KEY)
    return body, stat.etag.strip('"')


def _decode_mount(body):
    lines = body.decode().splitlines()
    assert len(lines) == 2, lines
    header = json.loads(lines[0])
    assert header["type"] == "cas_mount_lease" and int(header["v"]) > 0, header
    return json.loads(lines[1])


def _log_count_since_last_restart(node, pattern):
    # The shortened renewal period makes this server log heavily enough to rotate
    # clickhouse-server.log mid-test, so a plain grep on the live file alone can miss matches that
    # already rotated out to clickhouse-server.log.N.gz. Concatenate the rotated files (oldest
    # first, by the numeric suffix) followed by the live file, then count matches only after the
    # LAST "Starting ClickHouse" line -- i.e. since the current server incarnation's own start --
    # so the count is a clean per-restart delta regardless of how many times rotation happened.
    script = (
        "combined=$(mktemp); "
        "for f in $(ls /var/log/clickhouse-server/clickhouse-server.log.[0-9]*.gz 2>/dev/null "
        "| sort -t. -k3,3rn); do zcat \"$f\" >> \"$combined\"; done; "
        "cat /var/log/clickhouse-server/clickhouse-server.log >> \"$combined\"; "
        "start_line=$(grep -n 'Starting ClickHouse' \"$combined\" | tail -1 | cut -d: -f1); "
        "tail -n +\"${start_line:-1}\" \"$combined\" | grep -c -- '%s' || true; "
        "rm -f \"$combined\""
    ) % pattern
    return int(node.exec_in_container(["bash", "-c", script]).strip())


def _renewal_log_rows(node, since, deadline=None):
    # Two sequential queries: `deadline.remaining()` is read again before the second one rather than
    # reused from the first, so this whole call cannot spend twice the caller's remaining budget.
    node.query("SYSTEM FLUSH LOGS", timeout=deadline.remaining() if deadline else None)
    rows = node.query(
        "SELECT outcome, detail['seq'], detail['write_attempt_id'], "
        "detail['attempts_sent'], detail['classification'] "
        "FROM system.cas_log "
        "WHERE event_type = 'watermark_renew' AND disk_name = '{}' "
        "AND detail['server_root_id'] = '{}' "
        "AND event_time_microseconds >= toDateTime64('{}', 6) "
        "ORDER BY event_time_microseconds FORMAT TSV".format(
            DISK, SERVER_ROOT_ID, since
        ),
        timeout=deadline.remaining() if deadline else None,
    )
    return [tuple(row.split("\t")) for row in rows.splitlines() if row]


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.add_instance(
        "node",
        main_configs=["configs/storage_conf.xml"],
        with_rustfs=True,
        stay_alive=True,
    )
    # A separate instance for the two `disk_cas_budget_*` probe disks (see
    # configs/request_budget_disks.xml): the hard-restart tests below count EXACT occurrences of a
    # production log line across every writable CAS disk on `node`, so sharing that node with more
    # writable CAS disks would inflate their counts.
    cluster.add_instance(
        "budget_probe",
        main_configs=["configs/request_budget_disks.xml"],
        with_rustfs=True,
    )
    cluster.base_cmd.extend(
        ["--file", os.path.join(os.path.dirname(__file__), "docker_compose_proxy.yml")]
    )

    control_url = None
    try:
        cluster.start()
        binding = subprocess.check_output(
            cluster.base_cmd + ["port", "s3proxy", "8474"], text=True
        ).strip()
        control_url = "http://{}".format(binding)
        _wait_until(lambda deadline: _control(control_url, "/healthz", timeout=deadline.remaining()), timeout=30)
        _control(control_url, "/config", {"reset": True})

        node = cluster.instances["node"]
        node.query(
            "CREATE TABLE renewal_probe (id UInt64, payload String) "
            "ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = '{}'".format(
                STORAGE_POLICY
            )
        )
        node.query("INSERT INTO renewal_probe VALUES (0, 'before')")
        yield {
            "node": node,
            "budget_probe": cluster.instances["budget_probe"],
            "control_url": control_url,
        }
    finally:
        if control_url is not None:
            try:
                _control(control_url, "/config", {"reset": True})
            except Exception:
                pass
        cluster.shutdown()


def test_openpoolview_handoff_freezes_the_connect_cap_from_the_real_disk(start_cluster):
    # `ContentAddressedMetadataStorage::openPoolView` derives `connect_timeout_cap_ms` from the
    # disk's OWN S3 client (`freezeConnectTimeoutCapMs`) and hands it into the pool's request budget;
    # `Pool::open` logs that budget once, at startup, through logger `CasRequestBudget`. This proves
    # the handoff end to end through the two disks' real startup, not through a test-constructed
    # backend: `disk_cas_budget_capped` (connect_timeout_ms=1000) must show the derived cap 1000 and
    # envelope 7000; `disk_cas_budget_unbounded` (connect_timeout_ms=0, Poco's "unbounded") must show
    # the cap falling back to the attempt timeout itself, 5000, and envelope 15000. Either disk
    # opening with the base client's own timeout instead (1000/1000/... or 2000/...) would mean the
    # freeze was skipped or the frozen value never reached the backend.
    node = start_cluster["budget_probe"]
    assert _log_count_since_last_restart(
        node, "CAS request budget in effect: attempt_timeout_ms=5000 connect_timeout_cap_ms=1000 envelope_ms=7000"
    ) == 1
    assert _log_count_since_last_restart(
        node, "CAS request budget in effect: attempt_timeout_ms=5000 connect_timeout_cap_ms=5000 envelope_ms=15000"
    ) == 1


def test_transient_mount_renewal_retries_without_remount(start_cluster):
    node = start_cluster["node"]
    control_url = start_cluster["control_url"]
    _control(control_url, "/config", {"reset": True})
    mount_before = _mount_snapshot(node)
    _, token_before = _read_mount_object()
    counters_before = _profile_events(node)
    since = node.query("SELECT toString(now64(6))").strip()

    _control(
        control_url,
        "/config",
        {
            "rate": 1.0,
            "modes": ["503"],
            "methods": ["PUT"],
            "path_substring": MOUNT_REQUEST_PATH,
            "remaining_faults": 1,
            "seed": 801,
        },
    )

    def recovered_snapshot(deadline):
        # Read the counter before the mount row: `CASMountRenewalRecovered` is incremented as soon as
        # the renewal decides its outcome, strictly before the mount row's `renewal_sequence` (and the
        # matching cas_log row) is updated to the new sequence. With the shortened renewal period a
        # background (fault-free) renewal can land between the two reads; reading counters first makes
        # the subsequent mount read very unlikely to still observe the pre-recovery sequence.
        # `deadline.remaining()` is read again for the second query rather than reused from the first,
        # so this probe cannot spend twice its caller's remaining budget.
        counters = _profile_events(node, timeout=deadline.remaining())
        mount = _mount_snapshot(node, timeout=deadline.remaining())
        if (
            mount["sequence"] > mount_before["sequence"]
            and counters["CASMountRenewalRecovered"]
            > counters_before["CASMountRenewalRecovered"]
        ):
            return mount, counters
        return None

    mount_after, counters_after = _wait_until(recovered_snapshot)
    _control(control_url, "/config", {"rate": 0.0})
    stats = _control(control_url, "/stats")
    body_after, token_after = _read_mount_object()
    mount_body = _decode_mount(body_after)
    delta = _event_delta(counters_before, counters_after)
    # The engine paces the reissues inside one renewal; what the log records is the renewal's
    # outcome, and the attempt count on that row is what says a retry happened. Look the row up by
    # outcome rather than by a snapshot-derived sequence (see _renewal_log_rows): with the shortened
    # renewal period, background renewals can advance `system.cas_mounts` past the exact sequence this
    # recovery landed on before either of these two reads gets to it.
    rows = _wait_until(
        lambda deadline: (
            found
            if any(row[0] == "recovered" for row in found)
            else None
        )
        if (found := _renewal_log_rows(node, since, deadline=deadline))
        else None,
        timeout=20,
    )
    recovered = next(row for row in rows if row[0] == "recovered")
    sequence = int(recovered[1])

    assert delta["CASMountRenewalAttempts"] > 1, delta
    assert delta["CASMountRenewalRetries"] > 0, delta
    assert delta["CASMountRenewalRecovered"] > 0, delta
    assert delta["CASMountRenewalDeadlineExceeded"] == 0, delta
    assert delta["CASRemountAttempts"] == 0, delta
    assert delta["CASRemountSucceeded"] == 0, delta
    assert delta["CASRemountFailed"] == 0, delta
    assert mount_after["state"] == "live", mount_after
    assert mount_after["lifecycle"] == "live", mount_after
    assert mount_after["gc_fenced"] == 0, mount_after
    # >= rather than == : body_after may reflect a later, unrelated background renewal that landed
    # after the one this test is verifying.
    assert int(mount_body["seq"]) >= sequence
    assert token_after != token_before
    assert stats["faults"] == 1, stats
    assert stats["by_mode"].get("503") == 1, stats
    print("targeted request count (transient renewal): {}".format(stats["faults"]), flush=True)

    assert int(recovered[3]) > 1, rows
    assert recovered[4] == "committed_after_retry", rows

    node.query(
        "ALTER TABLE renewal_probe UPDATE payload = 'after-retry' WHERE id = 0 "
        "SETTINGS mutations_sync = 2"
    )
    assert node.query("SELECT payload FROM renewal_probe WHERE id = 0").strip() == "after-retry"


def test_landed_response_lost_adopts_exact_mount_write(start_cluster):
    node = start_cluster["node"]
    control_url = start_cluster["control_url"]
    _control(control_url, "/config", {"reset": True})
    mount_before = _mount_snapshot(node)
    body_before, token_before = _read_mount_object()
    counters_before = _profile_events(node)
    since = node.query("SELECT toString(now64(6))").strip()

    _control(
        control_url,
        "/config",
        {
            "rate": 1.0,
            "modes": ["drop_after_forward"],
            "methods": ["PUT"],
            "path_substring": MOUNT_REQUEST_PATH,
            "remaining_faults": 1,
            "seed": 802,
        },
    )

    # The proxy records the dropped-after-forward request (and the upstream_etag the real PUT landed
    # under) as soon as it happens -- the physical write itself already reached the object store; only
    # the response back to ClickHouse was dropped. That is well before ClickHouse's own request notices
    # the lost response and resolves it by re-reading. With the renewal period this short, a plain
    # "read the object once resolution is confirmed" can just as easily observe a LATER, unrelated
    # background renewal that started immediately after this one resolved (see the mount_before/after
    # sequence race this replaced). Wait for the proxy's own record first, then poll the object for
    # its exact upstream_etag, so body_after is unambiguously the write this test is about.
    def dropped_record(deadline):
        found_stats = _control(control_url, "/stats", timeout=deadline.remaining())
        found_records = found_stats["drop_after_forward"]
        return (found_stats, found_records[0]) if len(found_records) == 1 else None

    stats, record = _wait_until(dropped_record)
    target_etag = record["upstream_etag"].strip('"')

    def matching_object(deadline):
        body, token = _read_mount_object(deadline)
        return (body, token) if token == target_etag else None

    body_after, token_after = _wait_until(matching_object)
    mount_body = _decode_mount(body_after)

    def resolved_snapshot(deadline):
        # `deadline.remaining()` is read again for the second query rather than reused from the
        # first, so this probe cannot spend twice its caller's remaining budget.
        counters = _profile_events(node, timeout=deadline.remaining())
        mount = _mount_snapshot(node, timeout=deadline.remaining())
        if (
            mount["sequence"] > mount_before["sequence"]
            and counters["CASMountRenewalResolved"]
            > counters_before["CASMountRenewalResolved"]
            and counters["CASMountRenewalRecovered"]
            > counters_before["CASMountRenewalRecovered"]
        ):
            return mount, counters
        return None

    # Polling at the renewal's own period (`MOUNT_RENEW_PERIOD_MS`) can alias with it on a slow host --
    # a resolved renewal can land and be superseded by the next one between two polls. Poll faster than
    # the cadence it observes, with a longer timeout to match.
    mount_after, counters_after = _wait_until(resolved_snapshot, timeout=120, interval=0.05)
    _control(control_url, "/config", {"rate": 0.0})
    delta = _event_delta(counters_before, counters_after)
    # Look the recovered row up by outcome/classification rather than by a snapshot-derived sequence
    # (see _renewal_log_rows): background renewals can advance `system.cas_mounts` past the exact
    # sequence this recovery landed on before either of these reads gets to it.
    rows = _wait_until(
        lambda deadline: (
            found
            if any(row[0] == "recovered" and row[4] == "committed_by_read" for row in found)
            else None
        )
        if (found := _renewal_log_rows(node, since, deadline=deadline))
        else None,
        timeout=20,
    )
    recovered = next(row for row in rows if row[0] == "recovered" and row[4] == "committed_by_read")
    sequence = int(recovered[1])

    assert stats["faults"] == 1, stats
    assert stats["by_mode"].get("drop_after_forward") == 1, stats
    assert record["method"] == "PUT", record
    assert record["path"].split("?", 1)[0] == MOUNT_REQUEST_PATH, record
    assert 200 <= record["upstream_status"] < 300, record
    assert record["request_body_sha256"] == hashlib.sha256(body_after).hexdigest(), record
    assert body_after != body_before
    assert token_after != token_before
    # >= rather than == : a later, unrelated background renewal may have advanced the mount object
    # again between the capture above and this read of the confirmed sequence from the log.
    assert int(mount_body["seq"]) >= sequence

    # >= rather than == : with the shortened renewal period, an unrelated fault-free background
    # renewal can complete (and count its own single attempt) right before or after this one, in the
    # gap between configuring the fault and observing this specific renewal's resolution. Resolved and
    # Recovered stay exact -- only a lost-response renewal like this one increments them.
    assert delta["CASMountRenewalAttempts"] >= 1, delta
    assert delta["CASMountRenewalRetries"] == 0, delta
    assert delta["CASMountRenewalResolved"] == 1, delta
    assert delta["CASMountRenewalRecovered"] == 1, delta
    assert delta["CASMountRenewalDeadlineExceeded"] == 0, delta
    assert delta["CASRemountAttempts"] == 0, delta
    assert delta["CASRemountSucceeded"] == 0, delta
    assert delta["CASRemountFailed"] == 0, delta
    assert mount_after["state"] == "live", mount_after
    assert mount_after["lifecycle"] == "live", mount_after
    assert mount_after["gc_fenced"] == 0, mount_after

    assert recovered[2] and mount_body["write_attempt_id"].startswith(recovered[2]), rows
    assert recovered[3] == "1", rows
    assert recovered[4] == "committed_by_read", rows
    print("targeted request count (landed response lost): {}".format(stats["faults"]), flush=True)


def test_hard_restart_observes_then_the_unsafe_knob_skips_the_observation(start_cluster):
    node = start_cluster["node"]

    def log_count_since_last_restart(pattern):
        return _log_count_since_last_restart(node, pattern)

    # mountObservationThresholdMs(ttl_ms, poll=max(1, period_ms / 2)) = ttl_ms + ttl_ms / 20 + poll:
    # derived from the module's own lease constants (see their definition above) rather than
    # hard-coded, so this stays correct if that fixed budget ever changes.
    poll_ms = max(1, MOUNT_RENEW_PERIOD_MS // 2)
    threshold_ms = MOUNT_LEASE_TTL_MS + MOUNT_LEASE_TTL_MS // 20 + poll_ms
    observation = "waiting ~{} ms (token-stability observation)".format(threshold_ms)
    epoch_before = int(
        node.query(
            "SELECT writer_epoch FROM system.cas_mounts WHERE disk = '{}' LIMIT 1".format(DISK)
        ).strip()
    )

    # A hard kill leaves the previous incarnation's mount slot claimed; the restart must pay the
    # token-stability observation wait once before it can safely reclaim it.
    node.stop_clickhouse(kill=True)
    node.start_clickhouse()
    # `node.start_clickhouse()` only waits for the server to accept queries, not for the CAS disk's
    # `Pool::open` (which performs the observation wait itself) to finish; reading the log or the mount
    # row before that completes races the very thing being measured. Wait for the mount to report
    # "live" first, then the log line and row are both settled.
    _wait_until(lambda deadline: _mount_snapshot(node, timeout=deadline.remaining())["state"] == "live", timeout=120)
    assert log_count_since_last_restart(observation) == 1
    assert _mount_snapshot(node)["state"] == "live"
    # This restart already reclaims the slot and advances the epoch on its own (via the observation
    # wait, not the knob), so the knob-restart's own advance must be measured from THIS value, not
    # from epoch_before -- otherwise a knob-restart that wrongly reused this same epoch would still
    # pass an `epoch_after > epoch_before` check.
    epoch_after_safe_restart = int(
        node.query(
            "SELECT writer_epoch FROM system.cas_mounts WHERE disk = '{}' LIMIT 1".format(DISK)
        ).strip()
    )
    assert epoch_after_safe_restart > epoch_before

    # Enable the unsafe knob while the server is stopped (a test-stand-only config.d overlay), then
    # hard-kill again: this server's own uuid already holds the slot, so the knob may reclaim it at
    # once and skip the observation wait entirely.
    node.stop_clickhouse(kill=True)
    node.copy_file_to_container(
        os.path.join(os.path.dirname(__file__), "configs/unsafe_remount.xml"),
        "/etc/clickhouse-server/config.d/unsafe_remount.xml",
    )
    try:
        node.start_clickhouse()
        # Same race as the safe restart above: wait for the mount to settle before reading the log.
        _wait_until(lambda deadline: _mount_snapshot(node, timeout=deadline.remaining())["state"] == "live", timeout=120)
        assert log_count_since_last_restart(observation) == 0
        assert _mount_snapshot(node)["state"] == "live"
        epoch_after_knob_restart = int(
            node.query(
                "SELECT writer_epoch FROM system.cas_mounts WHERE disk = '{}' LIMIT 1".format(DISK)
            ).strip()
        )
        assert epoch_after_knob_restart > epoch_after_safe_restart
    finally:
        node.exec_in_container(["rm", "-f", "/etc/clickhouse-server/config.d/unsafe_remount.xml"])
