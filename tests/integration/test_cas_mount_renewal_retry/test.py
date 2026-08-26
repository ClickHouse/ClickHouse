import hashlib
import json
import os
import subprocess
import time
import urllib.request

import pytest

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


def _control(base_url, path, patch=None):
    if patch is None:
        request = urllib.request.Request("{}{}".format(base_url, path))
    else:
        request = urllib.request.Request(
            "{}{}".format(base_url, path),
            data=json.dumps(patch).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode())


def _wait_until(probe, timeout=40):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = probe()
        if last:
            return last
        time.sleep(0.2)
    raise AssertionError("condition did not become true within {}s; last={!r}".format(timeout, last))


def _profile_events(node):
    rows = node.query(
        "SELECT event, value FROM system.events WHERE event IN ({}) FORMAT TSV".format(
            ", ".join("'{}'".format(event) for event in RENEWAL_EVENTS)
        )
    )
    values = {event: 0 for event in RENEWAL_EVENTS}
    for row in rows.splitlines():
        event, value = row.split("\t")
        values[event] = int(value)
    return values


def _event_delta(before, after):
    return {event: after[event] - before[event] for event in RENEWAL_EVENTS}


def _mount_snapshot(node):
    row = node.query(
        "SELECT renewal_sequence, state, lifecycle, gc_fenced "
        "FROM system.cas_mounts "
        "WHERE disk = '{}' AND server_root_id = '{}' LIMIT 1 FORMAT TSV".format(
            DISK, SERVER_ROOT_ID
        )
    ).strip()
    assert row, "the local CAS mount row must be visible"
    sequence, state, lifecycle, gc_fenced = row.split("\t")
    return {
        "sequence": int(sequence),
        "state": state,
        "lifecycle": lifecycle,
        "gc_fenced": int(gc_fenced),
    }


def _read_mount_object():
    response = cluster.rustfs_client.get_object(cluster.rustfs_bucket, MOUNT_OBJECT_KEY)
    try:
        body = response.read()
    finally:
        response.close()
        response.release_conn()
    stat = cluster.rustfs_client.stat_object(cluster.rustfs_bucket, MOUNT_OBJECT_KEY)
    return body, stat.etag.strip('"')


def _decode_mount(body):
    lines = body.decode().splitlines()
    assert len(lines) == 2, lines
    header = json.loads(lines[0])
    assert header["type"] == "cas_mount_lease" and int(header["v"]) > 0, header
    return json.loads(lines[1])


def _renewal_log_rows(node, since, sequence):
    node.query("SYSTEM FLUSH LOGS")
    rows = node.query(
        "SELECT outcome, detail['seq'], detail['write_attempt_id'], "
        "detail['attempts_sent'], detail['classification'] "
        "FROM system.cas_log "
        "WHERE event_type = 'watermark_renew' AND disk_name = '{}' "
        "AND detail['server_root_id'] = '{}' "
        "AND event_time_microseconds >= toDateTime64('{}', 6) "
        "AND detail['seq'] = '{}' "
        "ORDER BY event_time_microseconds FORMAT TSV".format(
            DISK, SERVER_ROOT_ID, since, sequence
        )
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
        _wait_until(lambda: _control(control_url, "/healthz"), timeout=30)
        _control(control_url, "/config", {"reset": True})

        node = cluster.instances["node"]
        node.query(
            "CREATE TABLE renewal_probe (id UInt64, payload String) "
            "ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = '{}'".format(
                STORAGE_POLICY
            )
        )
        node.query("INSERT INTO renewal_probe VALUES (0, 'before')")
        yield {"node": node, "control_url": control_url}
    finally:
        if control_url is not None:
            try:
                _control(control_url, "/config", {"reset": True})
            except Exception:
                pass
        cluster.shutdown()


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

    def recovered_snapshot():
        mount = _mount_snapshot(node)
        counters = _profile_events(node)
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
    sequence = mount_after["sequence"]
    rows = _wait_until(
        lambda: (
            found
            if {row[0] for row in found} >= {"retrying", "recovered"}
            else None
        )
        if (found := _renewal_log_rows(node, since, sequence))
        else None,
        timeout=20,
    )

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
    assert int(mount_body["seq"]) == sequence
    assert token_after != token_before
    assert stats["faults"] == 1, stats
    assert stats["by_mode"].get("503") == 1, stats
    print("targeted request count (transient renewal): {}".format(stats["faults"]), flush=True)

    retrying = next(row for row in rows if row[0] == "retrying")
    recovered = next(row for row in rows if row[0] == "recovered")
    assert retrying[1] == recovered[1] == str(sequence), rows
    assert retrying[2] == recovered[2], rows
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

    def resolved_snapshot():
        mount = _mount_snapshot(node)
        counters = _profile_events(node)
        if (
            mount["sequence"] > mount_before["sequence"]
            and counters["CASMountRenewalResolved"]
            > counters_before["CASMountRenewalResolved"]
            and counters["CASMountRenewalRecovered"]
            > counters_before["CASMountRenewalRecovered"]
        ):
            return mount, counters
        return None

    mount_after, counters_after = _wait_until(resolved_snapshot)
    _control(control_url, "/config", {"rate": 0.0})
    stats = _control(control_url, "/stats")
    body_after, token_after = _read_mount_object()
    mount_body = _decode_mount(body_after)
    delta = _event_delta(counters_before, counters_after)
    sequence = mount_after["sequence"]
    rows = _wait_until(
        lambda: (
            found
            if any(row[0] == "recovered" and row[4] == "committed_by_get" for row in found)
            else None
        )
        if (found := _renewal_log_rows(node, since, sequence))
        else None,
        timeout=20,
    )

    records = stats["drop_after_forward"]
    assert stats["faults"] == 1, stats
    assert stats["by_mode"].get("drop_after_forward") == 1, stats
    assert len(records) == 1, records
    record = records[0]
    assert record["method"] == "PUT", record
    assert record["path"].split("?", 1)[0] == MOUNT_REQUEST_PATH, record
    assert 200 <= record["upstream_status"] < 300, record
    assert record["request_body_sha256"] == hashlib.sha256(body_after).hexdigest(), record
    assert record["upstream_etag"].strip('"') == token_after, record
    assert body_after != body_before
    assert token_after != token_before
    assert int(mount_body["seq"]) == sequence

    assert delta["CASMountRenewalAttempts"] == 1, delta
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

    recovered = next(row for row in rows if row[0] == "recovered")
    assert recovered[1] == str(sequence), rows
    assert recovered[2] and mount_body["write_attempt_id"].startswith(recovered[2]), rows
    assert recovered[3] == "1", rows
    assert recovered[4] == "committed_by_get", rows
    print("targeted request count (landed response lost): {}".format(stats["faults"]), flush=True)
