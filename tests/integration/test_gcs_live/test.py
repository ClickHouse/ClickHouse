"""The live-GCS characterization gate for the two native GCS HTTP clients.

## Why this suite exists, and what nothing else can replace

The unit tests prove which headers a request carries, and `test_cas_gcs` proves that a real CAS mount
still works once generation semantics stop being applied to every request on the client. Neither can
prove that *Google* accepts the resulting authenticated requests. A fake models whatever we assumed
when we wrote it, so a green `test_cas_gcs` is not evidence for anything below.

Two things in particular can only be settled here:

  - `deduceProviderType` is pure endpoint-substring matching, and the whole `ApiMode` block in
    `Client::BuildHttpRequest` is nested under `provider_type == ProviderType::GCS`. `test_cas_gcs`
    deliberately uses a hostname containing no `storage.googleapis.com`, so `provider_type` is UNKNOWN
    there and the api-mode transformations never run. Against a real endpoint they DO, and they run
    underneath the request-mode logic.
  - Whether the GOOG4 signed-header allowlist produces a signature Google actually accepts.

## Gating

Every live group is opt-in through environment variables and skips cleanly when they are absent.
The two credential-free helper regressions run by default, but they use only local files and a
synthetic query result. No test that touches a real bucket or issues billable requests runs by default.

  - `GCS_LIVE_BUCKET`             — required for any group. A bucket the caller is willing to have
                                    objects created and deleted in.
  - `GCS_LIVE_PREFIX`             — optional key prefix, default `clickhouse-gcs-live-gate`. A random
                                    per-run suffix is always appended, so two concurrent runs cannot
                                    share a prefix.
  - `GCS_LIVE_HMAC_ACCESS_KEY_ID` — a GOOG4 HMAC key pair. Enables the ordinary and CAS GOOG4
                                    scenarios.
  - `GCS_LIVE_HMAC_SECRET_ACCESS_KEY`
  - `GCS_LIVE_OAUTH_FROM_METADATA=1` — declares that the HOST running this suite can reach the GCE
                                    metadata server and that its service account may write to the
                                    bucket. Enables the ordinary and CAS OAuth scenarios on GCE.
  - `GCS_LIVE_OAUTH_ADC_CLIENT_ID` — Application Default Credentials, the alternative that enables
  - `GCS_LIVE_OAUTH_ADC_CLIENT_SECRET`  OAuth scenarios from anywhere, not just on GCE. A CAS disk accepts
  - `GCS_LIVE_OAUTH_ADC_REFRESH_TOKEN`  these: `non_cas_keys` in `ContentAddressedSettings.cpp` lists
                                    `metadata_service`, `request_token_path`, `service_account` and the
                                    whole ADC triple, and its own comment says the triple "is the only
                                    way to run `gcp_oauth` off a GCE instance". Either source is
                                    enough; the ADC one exists because requiring a GCE host is what
                                    would keep this gate from ever being run.
  - `GCS_LIVE_AMBIGUITY_PROXY_URI` — URI of the operator-controlled TLS fault proxy. Together with
  - `GCS_LIVE_AMBIGUITY_CONTROL_URL`  its control URL and public CA file, enables the fault arms.
  - `GCS_LIVE_AMBIGUITY_CA_FILE`      The terminating proxy's public CA bundle. ClickHouse retains
                                    strict certificate verification and trusts this file in addition
                                    to the image's default CA roots.
                                    The proxy must meet the phase contract documented on
                                    `test_live_cas_ambiguous_staged_copy_absent_retry_uses_retagged_replacement`.
                                    Both URLs must be credential-free endpoints. Authentication
                                    material remains entirely inside the operator's proxy and the
                                    ClickHouse credential variables already listed above. The two
                                    control contracts are documented on the queued-delete and
                                    staged-copy-ambiguity tests.

Only disks whose gates are satisfied are written into the configuration. That is deliberate: a CAS
disk mounts and runs its capability battery at server startup with no fallback, so an unusable CAS
disk in the config would stop the server and take the other groups down with it.

## What this gate asserts, and what it deliberately does not

It asserts what a client can observe: that each operation SUCCEEDS against Google, that ordinary
non-CAS requests retain their ETag-based contract, that CAS records generations rather than ETags,
and that each named body-publication action was actually selected. The Task 10 cases use a statement
query id in `system.cas_log` and `system.query_log`, so unrelated background work cannot satisfy them.
The older ordinary characterization still uses process-wide `system.events`; its limitations remain
spelled out below rather than being silently hidden.

## OPEN QUESTION FOR WHOEVER FIRST RUNS THIS WITH CREDENTIALS

`system.events` counters are PROCESS-WIDE, and this configuration also holds several CAS disks whose
control writers issue object-storage requests of their own. Their GC schedulers are stopped before the
tests, but mount leases and other control work still exist. So every ordinary counter delta asserted here
is only as sound as the assumption that no CAS activity moved that counter inside the measured window.
Where that assumption fails, the assertion still passes — for a reason that has nothing to do with the
statement it names.

This is an open question, not a known defect: which counters CAS can actually move during these
windows is not determinable without a real run. It is written here rather than in a tracked item
because the first run is when it matters and this docstring is what its reader will have in front of
them. **On that run, check each counter individually instead of trusting a pass** — for any counter CAS
can move, a green assertion is not evidence that the statement under test issued the operation.

One test is EXEMPT, and the reason is the template for clearing the others:
`test_default_gcs_client_parquet_metadata_cache_keys_on_the_ordinary_etag` uses
`ParquetMetadataCacheMisses` and `ParquetMetadataCacheHits`, which only a Parquet read moves. No CAS
disk can touch either, so those two deltas mean exactly what they say. Clearing a counter means showing
that same thing about it — not observing it pass.

One instance is already settled and serves as the pattern for the other direction. `S3ListObjects` was
asserted here and has been removed: an ordinary MergeTree lifecycle on a local-metadata disk never lists, so it could not
have been satisfied by this workload at all — but the CAS disks in this same configuration DO list, so
a background collection round inside the window could have satisfied it anyway. That is exactly the
failure mode above, and it is why "make something list somehow" would have produced a test passing for
the wrong reason rather than a working one.

It does NOT assert the outbound header set — that `x-goog-if-generation-match` appears on the wire,
that `x-amz-date` / `x-amz-content-sha256` / `x-amz-security-token` / `x-amz-api-version` are absent,
or which headers the GOOG4 signature covers. That is a scope decision, not an impossibility, and the
alternatives considered were each worse than the gap:

  - `PocoHTTPClient` logs RESPONSE headers under `enable_s3_requests_logging` and never logs the
    request headers, so the server log cannot supply them.
  - A plain forward proxy would have to be named as the endpoint, which makes `deduceProviderType`
    report UNKNOWN and switches off the very `ApiMode` behaviour this suite exists to exercise.
  - Downgrading to plain HTTP so a proxy can read the headers puts live credentials in clear text on
    the wire.
  - A TLS-TERMINATING proxy does work and is the honest option: `endpoint` stays
    `storage.googleapis.com`, so `provider_type` is still GCS and `ApiMode::GCS` stays active, while
    the proxy observes plaintext request headers inside a process the test operator already controls —
    the same trust boundary as the container that already holds the plaintext HMAC secret in its
    config. It is not built here because it needs a proxy container, a generated CA distributed into
    the server's trust store, and per-disk proxy configuration: real infrastructure for a property the
    unit tests already establish by inspecting the request object directly, with no network at all.

So the outbound header set stays with the unit tests. What is left for this gate is acceptance — and
acceptance is the part a unit test structurally cannot reach.
"""

import hashlib
import html
import json
import os
import random
import string
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance

BUCKET = os.environ.get("GCS_LIVE_BUCKET", "")
BASE_PREFIX = os.environ.get("GCS_LIVE_PREFIX", "clickhouse-gcs-live-gate")
HMAC_KEY_ID = os.environ.get("GCS_LIVE_HMAC_ACCESS_KEY_ID", "")
HMAC_SECRET = os.environ.get("GCS_LIVE_HMAC_SECRET_ACCESS_KEY", "")
OAUTH_FROM_METADATA = os.environ.get("GCS_LIVE_OAUTH_FROM_METADATA", "") == "1"
ADC_CLIENT_ID = os.environ.get("GCS_LIVE_OAUTH_ADC_CLIENT_ID", "")
ADC_CLIENT_SECRET = os.environ.get("GCS_LIVE_OAUTH_ADC_CLIENT_SECRET", "")
ADC_REFRESH_TOKEN = os.environ.get("GCS_LIVE_OAUTH_ADC_REFRESH_TOKEN", "")
ADC_AVAILABLE = bool(ADC_CLIENT_ID and ADC_CLIENT_SECRET and ADC_REFRESH_TOKEN)

# The ambiguity arm needs infrastructure that can terminate the TLS connection after Google accepts
# a native copy, exact-delete that landed generation, and then let the writer retry. The proxy URI is
# consumed by the global HTTPS client configuration; the control URL arms the one-shot fault and
# exposes a credential-free phase report.
# None of these values carries credentials: the third is a public trust anchor. Merely having
# ordinary GCS credentials is not enough to make an ambiguous outcome controllable, so this remains a
# separate release gate rather than a best-effort timing race.
AMBIGUITY_PROXY_URI = os.environ.get("GCS_LIVE_AMBIGUITY_PROXY_URI", "")
AMBIGUITY_CONTROL_URL = os.environ.get("GCS_LIVE_AMBIGUITY_CONTROL_URL", "")
AMBIGUITY_CA_FILE = os.environ.get("GCS_LIVE_AMBIGUITY_CA_FILE", "")


def _credential_free_url(value):
    parsed = urllib.parse.urlsplit(value)
    return bool(parsed.scheme in ("http", "https") and parsed.netloc and parsed.username is None and parsed.password is None and not parsed.query and not parsed.fragment)


AMBIGUITY_DRIVER_AVAILABLE = bool(
    AMBIGUITY_PROXY_URI
    and AMBIGUITY_CONTROL_URL
    and AMBIGUITY_CA_FILE
    and os.path.isfile(AMBIGUITY_CA_FILE)
    and _credential_free_url(AMBIGUITY_PROXY_URI)
    and _credential_free_url(AMBIGUITY_CONTROL_URL)
)

HMAC_AVAILABLE = bool(BUCKET and HMAC_KEY_ID and HMAC_SECRET)
# Either token source satisfies OAuth — the GCE metadata server, or Application Default Credentials.
OAUTH_AVAILABLE = bool(BUCKET and (OAUTH_FROM_METADATA or ADC_AVAILABLE))

# The endpoint must be spelled with `storage.googleapis.com`, not a regional or private alias: that
# substring is the whole of `deduceProviderType`, and the api-mode transformations this gate exists to
# exercise are nested under the provider it deduces.
GCS_ENDPOINT = "https://storage.googleapis.com"

RUN_ID = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(12))
PREFIX = "{}/{}".format(BASE_PREFIX.strip("/"), RUN_ID)

HMAC_PLAIN_DISK = "live_hmac_plain"
# A second ordinary disk exists only so a partition can be MOVED between two volumes of one policy.
# That is the one SQL statement that reaches a server-side `CopyObject`: `FREEZE` and
# `REPLACE PARTITION` hardlink the LOCAL metadata files and issue no object-storage copy at all, so a
# test built on them would leave `S3CopyObject` at zero and prove nothing about GCS accepting a copy.
HMAC_PLAIN_DISK_2 = "live_hmac_plain_cold"
HMAC_TWO_VOLUME_POLICY = "live_hmac_two_volume"
OAUTH_PLAIN_DISK = "live_oauth_plain"
OAUTH_PLAIN_DISK_2 = "live_oauth_plain_cold"
OAUTH_TWO_VOLUME_POLICY = "live_oauth_two_volume"
# An ordinary `gcs_hmac` disk pointed at a bucket that does not exist, so a refused request can be
# observed ON THE GOOG4 PATH. A disk rather than `s3(...)` because it reuses the configuration surface
# the rest of this group already exercises — NOT because the table function cannot select the client:
# `StorageS3Configuration::fromNamedCollection` does read `http_client`, which is what
# `PARQUET_NAMED_COLLECTION` below relies on. What has no spelling for it is the POSITIONAL argument
# form (`fromAST` sets `http_client` only through the BigLake ADC path, which forces `gcp_oauth`), so a
# bare `s3('url', 'key', 'secret')` would sign with ordinary AWS SigV4 and say nothing about GOOG4.
HMAC_ABSENT_BUCKET_DISK = "live_hmac_absent_bucket"
# Carries `http_client=gcs_hmac` into the object-storage TABLE ENGINE path, which is the only way to
# reach the Parquet metadata cache: that cache is consumed in `StorageObjectStorageSource` and
# `ParquetV3BlockInputFormat`, never by a MergeTree disk, so no statement on the disks above can touch
# it.
PARQUET_NAMED_COLLECTION = "live_gcs_hmac_parquet"
OAUTH_PARQUET_NAMED_COLLECTION = "live_gcs_oauth_parquet"
CAS_OAUTH_DISK = "live_cas_oauth"
CAS_HMAC_DISK = "live_cas_hmac"
CAS_OAUTH_STAGED_DISK = "live_cas_oauth_staged"
CAS_HMAC_STAGED_DISK = "live_cas_hmac_staged"
CAS_OAUTH_AMBIGUITY_DISK = "live_cas_oauth_ambiguity"
CAS_HMAC_AMBIGUITY_DISK = "live_cas_hmac_ambiguity"
AMBIGUITY_SUBPREFIX = {
    "gcs_hmac": "cas-hmac-ambiguity",
    "gcp_oauth": "cas-oauth-ambiguity",
}

# Lowering the genuine-conditional ceiling makes a modest live payload prove that blob publication
# no longer inherits the former GCS-only size cliff. The blob body now uses Default mode and may be
# either a normal one-shot PUT or ordinary multipart; mutable CAS objects keep this ceiling.
FORMER_CONDITIONAL_PUT_CAP = 5 * 1024 * 1024
LIVE_LARGE_VALUE_ITEMS = 400000
BATCH_DELETE_LOG_PATTERN = r"Objects with paths \["

cluster = ClickHouseCluster(__file__)


def _xml(value):
    """Escape a runtime value before placing it in the generated XML configuration."""
    return html.escape(str(value), quote=True)


def _disk_xml(
    name,
    subprefix,
    cas,
    client,
    bucket=None,
    skip_access_check=False,
    staging_backend="local",
):
    lines = [
        "            <{}>".format(name),
        "                <type>object_storage</type>",
        "                <object_storage_type>s3</object_storage_type>",
    ]
    if skip_access_check:
        # Only for the deliberately-unreachable-bucket disk, and only because that disk is NOT a CAS
        # mount. `IDisk::startup` calls `checkAccess` and rethrows, and `DiskSelector::initialize` is a
        # function-try-block with a single `catch (...)` around the whole construction loop — there is
        # no per-disk isolation, so one unreachable disk aborts the entire selector build and every
        # other disk in this file dies with it. `Server.cpp` hardcodes
        # `registerDisks(global_skip_access_check=false)`, so the per-disk key is the only way out.
        #
        # NEVER put this on a `cas=True` disk. A writable generation-token CAS mount must refuse it, so
        # that `runCapabilityProbe` cannot be bypassed — that battery is the only thing proving a
        # token-exact DELETE really carries its generation precondition.
        lines.append("                <skip_access_check>true</skip_access_check>")
    if cas:
        lines += [
            "                <metadata_type>cas</metadata_type>",
            "                <server_root_id>{}</server_root_id>".format(name),
            "                <gc_interval_sec>3600</gc_interval_sec>",
            "                <gcs_max_conditional_put_bytes>{}</gcs_max_conditional_put_bytes>".format(FORMER_CONDITIONAL_PUT_CAP),
            "                <staging_backend>{}</staging_backend>".format(staging_backend),
        ]
    lines += [
        "                <endpoint>{}/{}/{}/{}/</endpoint>".format(GCS_ENDPOINT, _xml(bucket or BUCKET), _xml(PREFIX), _xml(subprefix)),
        "                <http_client>{}</http_client>".format(client),
    ]
    if client == "gcs_hmac":
        lines += [
            "                <access_key_id>{}</access_key_id>".format(_xml(HMAC_KEY_ID)),
            "                <secret_access_key>{}</secret_access_key>".format(_xml(HMAC_SECRET)),
        ]
    if client == "gcp_oauth" and ADC_AVAILABLE:
        # `requestBearerToken` picks between the GCE metadata server and these; a CAS disk accepts them
        # (they are in `non_cas_keys`). Present only when supplied, so a GCE run keeps using metadata.
        lines += [
            "                <google_adc_client_id>{}</google_adc_client_id>".format(_xml(ADC_CLIENT_ID)),
            "                <google_adc_client_secret>{}</google_adc_client_secret>".format(_xml(ADC_CLIENT_SECRET)),
            "                <google_adc_refresh_token>{}</google_adc_refresh_token>".format(_xml(ADC_REFRESH_TOKEN)),
        ]
    lines.append("            </{}>".format(name))
    return "\n".join(lines)


def _policy_xml(name):
    return "            <{name}>\n                <volumes><main><disk>{name}</disk></main></volumes>\n            </{name}>".format(name=name)


def _two_volume_policy_xml(name, hot, cold):
    return (
        "            <{policy}>\n"
        "                <volumes>\n"
        "                    <hot><disk>{hot}</disk></hot>\n"
        "                    <cold><disk>{cold}</disk></cold>\n"
        "                </volumes>\n"
        "            </{policy}>".format(policy=name, hot=hot, cold=cold)
    )


def _named_collection_xml(name, client):
    lines = [
        "        <{}>".format(name),
        "            <url>{}/{}/{}/parquet/</url>".format(GCS_ENDPOINT, _xml(BUCKET), _xml(PREFIX)),
        "            <http_client>{}</http_client>".format(client),
    ]
    if client == "gcs_hmac":
        lines += [
            "            <access_key_id>{}</access_key_id>".format(_xml(HMAC_KEY_ID)),
            "            <secret_access_key>{}</secret_access_key>".format(_xml(HMAC_SECRET)),
        ]
    elif ADC_AVAILABLE:
        lines += [
            "            <google_adc_client_id>{}</google_adc_client_id>".format(_xml(ADC_CLIENT_ID)),
            "            <google_adc_client_secret>{}</google_adc_client_secret>".format(_xml(ADC_CLIENT_SECRET)),
            "            <google_adc_refresh_token>{}</google_adc_refresh_token>".format(_xml(ADC_REFRESH_TOKEN)),
        ]
    lines.append("        </{}>".format(name))
    return "\n".join(lines)


def _ambiguity_endpoint_settings_xml():
    entries = []
    for name, subprefix in (
        ("task10_hmac_ambiguity", AMBIGUITY_SUBPREFIX["gcs_hmac"]),
        ("task10_oauth_ambiguity", AMBIGUITY_SUBPREFIX["gcp_oauth"]),
    ):
        endpoint = "{}/{}/{}/{}/".format(GCS_ENDPOINT, BUCKET, PREFIX, subprefix)
        entries.append("        <{name}>\n            <endpoint>{endpoint}</endpoint>\n            <retry_attempts>0</retry_attempts>\n        </{name}>".format(name=name, endpoint=_xml(endpoint)))
    return "    <s3>\n{}\n    </s3>\n".format("\n".join(entries))


def _write_config(path):
    """Build a storage configuration holding only the disks whose environment gates are satisfied."""
    disks = []
    policies = []
    if HMAC_AVAILABLE:
        disks += [
            _disk_xml(HMAC_PLAIN_DISK, "plain", cas=False, client="gcs_hmac"),
            _disk_xml(HMAC_PLAIN_DISK_2, "plain-cold", cas=False, client="gcs_hmac"),
            _disk_xml(
                HMAC_ABSENT_BUCKET_DISK,
                "absent",
                cas=False,
                client="gcs_hmac",
                bucket="clickhouse-gcs-live-gate-bucket-that-does-not-exist",
                skip_access_check=True,
            ),
            _disk_xml(CAS_HMAC_DISK, "cas-hmac", cas=True, client="gcs_hmac"),
            _disk_xml(
                CAS_HMAC_STAGED_DISK,
                "cas-hmac-staged",
                cas=True,
                client="gcs_hmac",
                staging_backend="s3",
            ),
        ]
        if AMBIGUITY_DRIVER_AVAILABLE:
            disks.append(
                _disk_xml(
                    CAS_HMAC_AMBIGUITY_DISK,
                    "cas-hmac-ambiguity",
                    cas=True,
                    client="gcs_hmac",
                    staging_backend="s3",
                )
            )
        policies += [
            _policy_xml(HMAC_ABSENT_BUCKET_DISK),
            _policy_xml(CAS_HMAC_DISK),
            _policy_xml(CAS_HMAC_STAGED_DISK),
            _two_volume_policy_xml(HMAC_TWO_VOLUME_POLICY, HMAC_PLAIN_DISK, HMAC_PLAIN_DISK_2),
        ]
        if AMBIGUITY_DRIVER_AVAILABLE:
            policies.append(_policy_xml(CAS_HMAC_AMBIGUITY_DISK))
    if OAUTH_AVAILABLE:
        disks += [
            _disk_xml(OAUTH_PLAIN_DISK, "plain-oauth", cas=False, client="gcp_oauth"),
            _disk_xml(OAUTH_PLAIN_DISK_2, "plain-oauth-cold", cas=False, client="gcp_oauth"),
            _disk_xml(CAS_OAUTH_DISK, "cas-oauth", cas=True, client="gcp_oauth"),
            _disk_xml(
                CAS_OAUTH_STAGED_DISK,
                "cas-oauth-staged",
                cas=True,
                client="gcp_oauth",
                staging_backend="s3",
            ),
        ]
        if AMBIGUITY_DRIVER_AVAILABLE:
            disks.append(
                _disk_xml(
                    CAS_OAUTH_AMBIGUITY_DISK,
                    "cas-oauth-ambiguity",
                    cas=True,
                    client="gcp_oauth",
                    staging_backend="s3",
                )
            )
        policies += [
            _policy_xml(CAS_OAUTH_DISK),
            _policy_xml(CAS_OAUTH_STAGED_DISK),
            _two_volume_policy_xml(OAUTH_TWO_VOLUME_POLICY, OAUTH_PLAIN_DISK, OAUTH_PLAIN_DISK_2),
        ]
        if AMBIGUITY_DRIVER_AVAILABLE:
            policies.append(_policy_xml(CAS_OAUTH_AMBIGUITY_DISK))

    named_collection_entries = []
    if HMAC_AVAILABLE:
        named_collection_entries.append(_named_collection_xml(PARQUET_NAMED_COLLECTION, "gcs_hmac"))
    if OAUTH_AVAILABLE:
        named_collection_entries.append(_named_collection_xml(OAUTH_PARQUET_NAMED_COLLECTION, "gcp_oauth"))
    named_collections = ""
    if named_collection_entries:
        named_collections = "    <named_collections>\n{}\n    </named_collections>\n".format("\n".join(named_collection_entries))

    with open(path, "w", encoding="utf-8") as out:
        out.write("<clickhouse>\n")
        if AMBIGUITY_DRIVER_AVAILABLE:
            # Global HTTPS proxy configuration is required because a nested disk `<proxy>` key would
            # be rejected by `ContentAddressedSettings`. The external driver stays transparent until
            # armed for the dedicated ambiguity prefix.
            out.write("    <proxy><https><uri>{}</uri></https></proxy>\n".format(_xml(AMBIGUITY_PROXY_URI)))
            out.write(
                "    <openSSL><client><caConfig>/etc/clickhouse-server/extra_conf.d/{}</caConfig>"
                "<loadDefaultCAFile>true</loadDefaultCAFile><verificationMode>strict</verificationMode>"
                "</client></openSSL>\n".format(_xml(os.path.basename(AMBIGUITY_CA_FILE)))
            )
            # Disable SDK-internal retries only on the two dedicated prefixes. The response-loss
            # scenario must return control to `PartWriteTxn` after the first ambiguous copy; ordinary
            # disks retain their pre-change retry profile.
            out.write(_ambiguity_endpoint_settings_xml())
        out.write("    <storage_configuration>\n        <disks>\n")
        out.write("\n".join(disks))
        out.write("\n        </disks>\n        <policies>\n")
        out.write("\n".join(policies))
        out.write("\n        </policies>\n    </storage_configuration>\n")
        out.write(named_collections)
        out.write("</clickhouse>\n")


def _configured_cas_disks():
    disks = []
    if HMAC_AVAILABLE:
        disks += [CAS_HMAC_DISK, CAS_HMAC_STAGED_DISK]
        if AMBIGUITY_DRIVER_AVAILABLE:
            disks.append(CAS_HMAC_AMBIGUITY_DISK)
    if OAUTH_AVAILABLE:
        disks += [CAS_OAUTH_DISK, CAS_OAUTH_STAGED_DISK]
        if AMBIGUITY_DRIVER_AVAILABLE:
            disks.append(CAS_OAUTH_AMBIGUITY_DISK)
    return disks


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    if not (HMAC_AVAILABLE or OAUTH_AVAILABLE):
        yield cluster
        return

    configs_dir = os.path.join(os.path.dirname(__file__), "configs")
    os.makedirs(configs_dir, exist_ok=True)
    config_path = os.path.join(configs_dir, "live_gcs_generated.xml")
    _write_config(config_path)

    cluster.add_instance(
        "node",
        main_configs=[config_path],
        extra_configs=[AMBIGUITY_CA_FILE] if AMBIGUITY_DRIVER_AVAILABLE else [],
        stay_alive=True,
    )
    try:
        cluster.start()
        node = cluster.instances["node"]
        # Manual rounds are part of the GC scenarios. Stop each background scheduler first so an
        # uncorrelated round cannot consume a transition between the event assertions that bracket it.
        for disk in _configured_cas_disks():
            node.query("SYSTEM CAS GC STOP '{}'".format(disk))
        yield cluster
    finally:
        # Everything this run wrote lives under `PREFIX`, which carries a per-run random suffix, so no
        # two runs and nothing pre-existing can collide. The DROPs below let each CAS pool retire its
        # own metadata rather than deleting a live pool's keys from underneath it.
        #
        # They do NOT leave the bucket exactly as found: the Parquet test writes a plain object through
        # a table function, and there is no SQL verb that deletes an object. `PREFIX/parquet/` therefore
        # survives the run. Delete the whole `PREFIX` afterwards, or give the bucket a lifecycle rule —
        # this suite runs against a real, billable bucket and cannot clean those keys itself.
        node = cluster.instances.get("node")
        if node is not None:
            try:
                tables = node.query(
                    "SELECT name FROM system.tables WHERE database = currentDatabase() AND (startsWith(name, 'task10_') OR startsWith(name, 't_live_') OR startsWith(name, 'src_live_')) FORMAT TSV"
                ).split()
                for table in tables:
                    node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
                for disk in (HMAC_PLAIN_DISK, CAS_HMAC_DISK, CAS_OAUTH_DISK):
                    node.query("DROP TABLE IF EXISTS t_{} SYNC".format(disk))
                    node.query("DROP TABLE IF EXISTS src_{} SYNC".format(disk))
            except Exception:  # noqa: BLE001 - teardown must not mask a test failure
                pass
        cluster.shutdown()


def _events(node, names):
    """Current values of the named `system.events` counters, zero-filled for absent ones."""
    rows = node.query("SELECT event, value FROM system.events WHERE event IN ({}) FORMAT TSV".format(", ".join("'{}'".format(n) for n in names)))
    seen = {}
    for line in rows.strip().splitlines():
        event, value = line.split("\t")
        seen[event] = int(value)
    return {name: seen.get(name, 0) for name in names}


def _create(node, disk, table=None):
    table = table or "t_" + disk
    node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
    node.query(
        """
        CREATE TABLE {} (id Int64, data String)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy = '{}'
        """.format(table, disk)
    )
    return table


def _opaque_generation_evidence(node, query):
    """Keep raw generations in a hidden frame and return only domain evidence plus one-way digests."""
    __tracebackhide__ = True
    node.query("SYSTEM FLUSH LOGS")
    raw = node.query(query)
    generations = [line.strip().strip('"') for line in raw.strip().splitlines() if line.strip()]
    return (
        len(generations),
        bool(generations) and all(generation.isdigit() for generation in generations),
        tuple(hashlib.sha256(generation.encode("utf-8")).hexdigest() for generation in generations),
    )


def _cas_generation_domain(node, disk):
    """Whether this disk recorded generations and every recorded value belongs to the numeric domain."""
    __tracebackhide__ = True
    count, all_numeric, _digests = _opaque_generation_evidence(
        node,
        "SELECT DISTINCT token FROM system.cas_log WHERE disk_name = '{}' AND token != '' FORMAT TSV".format(disk),
    )
    return count > 0, all_numeric


def _cas_event_generation_evidence(node, disk, object_hash, event_type, outcome=""):
    """Return count, numeric-domain evidence, and an opaque digest for one CAS event generation."""
    __tracebackhide__ = True
    clauses = [
        "disk_name = '{}'".format(disk),
        "object_hash = '{}'".format(object_hash),
        "event_type = '{}'".format(event_type),
        "token != ''",
    ]
    if outcome:
        clauses.append("outcome = '{}'".format(outcome))
    count, all_numeric, digests = _opaque_generation_evidence(
        node,
        "SELECT token FROM system.cas_log WHERE {} ORDER BY event_time_microseconds FORMAT TSV".format(" AND ".join(clauses)),
    )
    return count, all_numeric, digests[0] if count == 1 else ""


def _query_id(scenario, auth_mode):
    return "task10_{}_{}_{}".format(scenario, auth_mode, RUN_ID)


def _cas_events(node, disk, query_id="", event_types=(), object_hash=""):
    """Return attributable CAS events without ever reading configuration or authentication data."""
    __tracebackhide__ = True
    node.query("SYSTEM FLUSH LOGS")
    clauses = ["disk_name = '{}'".format(disk)]
    if query_id:
        clauses.append("query_id = '{}'".format(query_id))
    if event_types:
        clauses.append("event_type IN ({})".format(", ".join("'{}'".format(event_type) for event_type in event_types)))
    if object_hash:
        clauses.append("object_hash = '{}'".format(object_hash))
    raw = node.query("SELECT event_type, object_hash, outcome, detail FROM system.cas_log WHERE {} ORDER BY event_time_microseconds FORMAT JSONEachRow".format(" AND ".join(clauses)))
    events = [json.loads(line) for line in raw.splitlines() if line]
    for event in events:
        event.pop("token", None)
    return events


def _ordinary_etag_domain(node, probe):
    """Keep raw ETags hidden and report only whether observed values remain ordinary and non-empty."""
    __tracebackhide__ = True
    lines = node.grep_in_log("{} |".format(probe))
    values = [line.rsplit("|", 1)[1].strip().strip('"') for line in lines.splitlines() if "|" in line]
    return bool(lines), bool(values), bool(values) and all(value and not value.isdigit() for value in values)


def _query_profile_events(node, query_id, names):
    """Read named ProfileEvents from the successful query-log row for one statement."""
    node.query("SYSTEM FLUSH LOGS")
    expressions = ", ".join("toUInt64(ProfileEvents['{0}']) AS {0}".format(name) for name in names)
    raw = node.query(
        "SELECT {} FROM system.query_log WHERE query_id = '{}' AND type = 'QueryFinish' ORDER BY event_time_microseconds DESC LIMIT 1 FORMAT JSONEachRow".format(expressions, query_id)
    ).strip()
    assert raw, "query log has no successful row for {}".format(query_id)
    return json.loads(raw)


def _assert_one_head_per_blob_task(node, query_id):
    profile = _query_profile_events(
        node,
        query_id,
        ("CASBlobUploadFanoutTasks", "CASBlobHead", "CASBlobHeadMiss"),
    )
    tasks = profile["CASBlobUploadFanoutTasks"]
    assert tasks > 0, profile
    assert profile["CASBlobHead"] + profile["CASBlobHeadMiss"] == tasks, profile
    return profile


def _largest_blob_put(events):
    puts = [event for event in events if event["event_type"] == "blob_put"]
    assert puts, "the statement emitted no attributable `blob_put` event"
    return max(puts, key=lambda event: int(event["detail"].get("size", "0")))


def _create_payload_table(node, table, disk):
    node.query("DROP TABLE IF EXISTS {} SYNC".format(table))
    node.query("CREATE TABLE {} (id UInt64, payload String CODEC(NONE)) ENGINE = MergeTree ORDER BY id SETTINGS storage_policy = '{}'".format(table, disk))


def _small_payload_insert(table, auth_mode, scenario):
    return "INSERT INTO {} SELECT number, concat('{}-{}-', toString(number), repeat('p', 4096)) FROM numbers(64)".format(table, scenario, auth_mode)


def _large_payload_insert(table, salt):
    # `hex(cityHash64(...))` is deterministic but not compressible enough for `CODEC(NONE)` to hide
    # the size threshold. One row keeps the target blob easy to identify by its logged logical size.
    return "INSERT INTO {} SELECT 1, arrayStringConcat(arrayMap(x -> hex(cityHash64(x + {})), range({})))".format(table, salt, LIVE_LARGE_VALUE_ITEMS)


def _gc_until(node, disk, object_hash, event_type, outcome="", max_rounds=20):
    """Run bounded synchronous GC rounds until one target transition becomes durable in the log."""
    for _ in range(max_rounds):
        node.query("SYSTEM CAS GC RUN '{}'".format(disk))
        rows = _cas_events(node, disk, event_types=(event_type,), object_hash=object_hash)
        if outcome:
            rows = [row for row in rows if row["outcome"] == outcome]
        if rows:
            return rows[-1]
    assert False, "{} did not emit {} outcome={!r} in {} manual rounds".format(object_hash, event_type, outcome, max_rounds)


def _ambiguity_driver_request(path, payload):
    """Call the opt-in fault driver; callers assert only phase booleans, never auth material."""
    request = urllib.request.Request(
        AMBIGUITY_CONTROL_URL.rstrip("/") + path,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _wait_for_driver_phase(path, scenario_id, phase, timeout=60):
    deadline = time.monotonic() + timeout
    while True:
        report = _ambiguity_driver_request(path, {"scenario_id": scenario_id})
        if report.get(phase) is True:
            return report
        assert time.monotonic() < deadline, "fault driver did not reach phase {}".format(phase)
        time.sleep(0.1)


# ---------------------------------------------------------------------------------------------------
# Group 1: Default requests on `gcs_hmac` and `gcp_oauth`. Nothing here is content-addressed; the
# question is whether GCS accepts every ordinary ClickHouse object-storage operation while its ETag
# response contract remains independent from CAS generation tokens.
# ---------------------------------------------------------------------------------------------------

requires_hmac = pytest.mark.skipif(
    not HMAC_AVAILABLE,
    reason="set GCS_LIVE_HMAC_ACCESS_KEY_ID and GCS_LIVE_HMAC_SECRET_ACCESS_KEY",
)
requires_oauth = pytest.mark.skipif(
    not OAUTH_AVAILABLE,
    reason="set GCS_LIVE_OAUTH_FROM_METADATA=1 on a GCE host, or the GCS_LIVE_OAUTH_ADC_* triple anywhere else",
)

ORDINARY_DISK_CASES = (
    pytest.param(
        "gcs_hmac",
        HMAC_TWO_VOLUME_POLICY,
        "/plain",
        marks=requires_hmac,
        id="gcs_hmac",
    ),
    pytest.param(
        "gcp_oauth",
        OAUTH_TWO_VOLUME_POLICY,
        "/plain-oauth",
        marks=requires_oauth,
        id="gcp_oauth",
    ),
)

NAMED_COLLECTION_CASES = (
    pytest.param("gcs_hmac", PARQUET_NAMED_COLLECTION, marks=requires_hmac, id="gcs_hmac"),
    pytest.param("gcp_oauth", OAUTH_PARQUET_NAMED_COLLECTION, marks=requires_oauth, id="gcp_oauth"),
)

CAS_STREAM_CASES = (
    pytest.param(
        "gcs_hmac",
        CAS_HMAC_DISK,
        marks=requires_hmac,
        id="gcs_hmac",
    ),
    pytest.param(
        "gcp_oauth",
        CAS_OAUTH_DISK,
        marks=requires_oauth,
        id="gcp_oauth",
    ),
)

CAS_STAGED_CASES = (
    pytest.param("gcs_hmac", CAS_HMAC_STAGED_DISK, marks=requires_hmac, id="gcs_hmac"),
    pytest.param("gcp_oauth", CAS_OAUTH_STAGED_DISK, marks=requires_oauth, id="gcp_oauth"),
)

CAS_AMBIGUITY_CASES = (
    pytest.param("gcs_hmac", CAS_HMAC_AMBIGUITY_DISK, marks=requires_hmac, id="gcs_hmac"),
    pytest.param("gcp_oauth", CAS_OAUTH_AMBIGUITY_DISK, marks=requires_oauth, id="gcp_oauth"),
)

requires_ambiguity_driver = pytest.mark.skipif(
    not AMBIGUITY_DRIVER_AVAILABLE,
    reason="set credential-free GCS_LIVE_AMBIGUITY_PROXY_URI and GCS_LIVE_AMBIGUITY_CONTROL_URL "
    "endpoints plus public GCS_LIVE_AMBIGUITY_CA_FILE; required response-loss and held-delete "
    "scenarios cannot be synthesized by ordinary GCS credentials",
)


@pytest.mark.parametrize("auth_mode,policy,path_fragment", ORDINARY_DISK_CASES)
def test_default_gcs_client_accepts_the_ordinary_object_storage_operation_set(auth_mode, policy, path_fragment):
    """Every S3 operation an ordinary disk issues is accepted under either GCS client.

    The `system.events` deltas are what make this non-vacuous: each named operation must have been
    issued at least once, so a statement that quietly stopped reaching object storage — because a
    default changed, or because a part stayed in memory — cannot leave the assertion true.

    The statement-to-operation mapping is deliberately NOT pinned. Which statement produces a batch
    delete rather than singular ones is a ClickHouse implementation detail that moves between versions;
    whether GCS accepts a batch delete is what this gate is asking. Pinning the mapping would make this
    test fail on refactors that say nothing about GCS.

    Object LISTING is not covered by THIS test — an ordinary MergeTree lifecycle on a local-metadata
    disk never issues one, see the comment on `counters` below.
    `test_default_gcs_client_accepts_an_object_listing` covers it on the same authenticated client
    through the table-engine path, which is a lister.

    Would fail if: bearer-token or GOOG4 authentication produced a request Google rejects for some
    operation, or either configured credential source stopped being accepted by its client selector
    (the disk would not resolve and no statement below would run).
    """
    node = cluster.instances["node"]
    # `S3ListObjects` is deliberately NOT in this set, and must not be re-added. Both of its increment
    # sites live in `S3IteratorAsync::getBatchAndCheckNext` and `S3ObjectStorage::listObjects`, which
    # are reached through `IObjectStorage::iterate`/`listObjects` — called by the object-storage table
    # engines, the data lakes, `ObjectStorageQueue`, the plain/plain_rewritable metadata storages and
    # CAS, none of which is in play here. These disks set no `metadata_type`, so they use local
    # metadata: MergeTree's own `iterate` calls go through `IDisk::iterateDirectory` over the LOCAL
    # metadata directory and issue no S3 listing at all. An ordinary lifecycle on a local-metadata disk
    # never lists.
    #
    # Worse than merely unsatisfiable, it would be unsound: `system.events` is process-wide, and the
    # CAS disks in this same configuration DO list, so a background GC round landing inside the delta
    # window could satisfy it for a reason that has nothing to do with this test's workload.
    counters = [
        "S3PutObject",
        "S3GetObject",
        "S3HeadObject",
        "S3CopyObject",
        "S3DeleteObjects",
        "S3CreateMultipartUpload",
        "S3UploadPart",
        "S3CompleteMultipartUpload",
    ]
    before = _events(node, counters)

    table = _create(node, policy, "task10_plain_{}".format(auth_mode))

    # A single-part PUT with custom metadata, then the HEAD that `s3_check_objects_after_upload`
    # issues to verify it.
    node.query(
        "INSERT INTO {} SELECT number, toString(number) FROM numbers(500)".format(table),
        settings={"s3_check_objects_after_upload": 1},
    )
    # A multipart upload: a tiny single-part ceiling rather than a large body, so the run does not
    # depend on how large a default part happens to be.
    node.query(
        "INSERT INTO {} SELECT number, repeat('x', 4096) FROM numbers(500, 4000)".format(table),
        settings={
            "s3_min_upload_part_size": 5 * 1024 * 1024,
            "s3_max_single_part_upload_size": 1024,
        },
    )
    assert int(node.query("SELECT count() FROM {}".format(table))) == 4500
    assert int(node.query("SELECT sum(id) FROM {}".format(table))) > 0

    # A server-side copy: moving a partition between the two volumes of one policy copies each object
    # and then deletes the source. This is the only statement here that reaches `CopyObject`.
    node.query("ALTER TABLE {} MOVE PARTITION tuple() TO VOLUME 'cold'".format(table))
    assert int(node.query("SELECT count() FROM {}".format(table))) == 4500

    # A merge (more reads and writes), then the deletes.
    node.query("OPTIMIZE TABLE {} FINAL".format(table))
    node.query("ALTER TABLE {} DROP PARTITION tuple()".format(table))
    assert int(node.query("SELECT count() FROM {}".format(table))) == 0

    after = _events(node, counters)
    for name in counters:
        assert after[name] > before[name], "{} was never issued ({} -> {}), so GCS acceptance of it is unproven".format(name, before[name], after[name])

    # `S3DeleteObjects` counts the singular and batch forms together, so the counter alone cannot say
    # the batch form was accepted. The two paths log differently, which separates them:
    # `deleteFileFromS3` logs "Object with path <k> was removed from S3" and `deleteFilesFromS3` logs
    # "Objects with paths [<k>,...] were removed from S3".
    #
    # The load-bearing one is the third line. When GCS refuses a batch `DeleteObjects`,
    # `deleteFilesFromS3` logs "DeleteObjects is not supported", calls
    # `s3_capabilities.setIsBatchDeleteSupported(false)` and silently retries with plain
    # `DeleteObject` — so the batch form failing looks EXACTLY like success at both the counter and the
    # data level. Asserting that line is absent while the plural line is present is the only way to say
    # GCS accepted the batch shape rather than the fallback having covered for it.
    #
    # LOG LEVEL, because this assertion's ability to FAIL depends on it. The two lines sit at different
    # levels: the plural "Objects with paths [...]" is `LOG_DEBUG`, the fallback notice is `LOG_TRACE`.
    # A server that did not admit TRACE would make the absence check pass unconditionally — a test that
    # cannot fail. It is admitted here because `add_instance` copies
    # `helpers/0_common_instance_config.xml` unconditionally and that sets `<level>test</level>`, and
    # `Poco::Message` orders `PRIO_TEST` BELOW `PRIO_TRACE`, so `test` admits trace messages. (The
    # `with_installed_binary` path rewrites it to `trace`, which also admits them.) If this suite ever
    # sets `copy_common_configs=False` or overrides the logger level, re-check this before trusting the
    # absence half.
    # Filtered to this test's own key prefix: the log carries every disk's traffic, and the CAS disks
    # in this configuration delete objects too, so an unfiltered match would be the same
    # someone-else's-traffic confound the module docstring warns about for counters.
    batch_lines = [line for line in node.grep_in_log(BATCH_DELETE_LOG_PATTERN).splitlines() if PREFIX in line and path_fragment in line]
    assert batch_lines, "no batch delete was logged for this run's own keys, so GCS acceptance of the batch DeleteObjects shape is unproven"
    assert not node.grep_in_log("DeleteObjects is not supported"), (
        "GCS refused the batch DeleteObjects shape and ClickHouse fell back to singular deletes; the counter and the row counts cannot see this, which is why it is asserted here"
    )

    # The singular shape needs its own evidence. `S3DeleteObjects` aggregates both, so the counter
    # moving says nothing about which of the two GCS accepted, and the assertions above speak only for
    # the batch one -- a build that never issued a singular DeleteObject at all would satisfy them.
    # Same prefix filter and the same reason for it.
    single_lines = [line for line in node.grep_in_log("Object with path ").splitlines() if PREFIX in line and path_fragment in line]
    assert single_lines, "no singular delete was logged for this run's own keys, so GCS acceptance of the singular DeleteObject shape is unproven -- only the batch shape is"


@requires_hmac
def test_default_gcs_hmac_reports_a_typed_error_for_a_refused_request():
    """A refused request must arrive as a typed S3 error, not an unparsed body.

    GCS answers the XML API with an `<Error><Code>` document, and the whole point of keeping the
    request on the S3 XML path is that the SDK parses it. Would fail if: the GOOG4 path returned a
    response the error parser cannot read, which would surface as a generic transport failure with the
    real cause only in the body.

    A disk rather than a positional `s3('url', 'key', 'secret')`: that argument form has no spelling
    for `http_client` and would sign with ordinary AWS SigV4, passing or failing for a reason unrelated
    to GOOG4. A NAMED COLLECTION would work — see the Parquet test below — but the disk is what the
    rest of this group already exercises.
    """
    node = cluster.instances["node"]
    table = _create(node, HMAC_ABSENT_BUCKET_DISK)
    error = node.query_and_get_error("INSERT INTO {} SELECT number, toString(number) FROM numbers(10)".format(table))
    # A parsed S3 error names the bucket problem. An unparsed one surfaces as a bare transport or
    # timeout failure, which is what must not appear.
    assert ("NoSuchBucket" in error) or ("S3_ERROR" in error) or ("ACCESS_DENIED" in error), error


@pytest.mark.parametrize("auth_mode,named_collection", NAMED_COLLECTION_CASES)
def test_default_gcs_client_accepts_an_object_listing(auth_mode, named_collection):
    """GCS accepts a LIST under either authentication mode through a named collection.

    The disks above cannot produce one: they use local metadata, so MergeTree's directory iteration
    reads the local metadata directory and `IObjectStorage::iterate` is never called. The table-engine
    path IS a lister — `StorageObjectStorageSource` calls `object_storage->iterate` to expand a glob —
    and the named collection puts that on the same `gcs_hmac` client, so the listing is signed the same
    way as everything else in this group.

    The reachability proof is the DATA, not a counter, and that is deliberate: `S3ListObjects` is
    exactly the counter the module's OPEN QUESTION section warns about, since the CAS disks in this
    configuration list too. Reading rows that came from two separate objects through one glob cannot be
    satisfied by anyone else's traffic — the listing must have enumerated both to return their union.

    Would fail if: GCS rejected a GOOG4-signed `ListObjectsV2`, or returned a body the SDK cannot parse
    into keys — the glob would resolve to fewer objects and the union would be short.
    """
    node = cluster.instances["node"]
    probe = "listing-probe-{}".format(auth_mode)
    for part in (1, 2):
        node.query(
            "INSERT INTO FUNCTION s3({}, filename='{}-{}.parquet', format='Parquet') SELECT {} AS part, number AS id FROM numbers(10)".format(named_collection, probe, part, part),
            settings={"s3_truncate_on_insert": 1},
        )

    glob = "s3({}, filename='{}-*.parquet', format='Parquet')".format(named_collection, probe)
    assert int(node.query("SELECT count() FROM {}".format(glob))) == 20
    # Both objects, through one glob: the listing enumerated them rather than a single key being read.
    assert node.query("SELECT DISTINCT part FROM {} ORDER BY part FORMAT TSV".format(glob)).split() == ["1", "2"]


@pytest.mark.parametrize("auth_mode,named_collection", NAMED_COLLECTION_CASES)
def test_default_gcs_client_parquet_metadata_cache_keys_on_the_ordinary_etag(auth_mode, named_collection):
    """The Parquet metadata cache keys off the object's ordinary ETag, never a generation.

    Three cache consumers — the filesystem cache, the page cache and this one — key off ONE value, the
    `etag` on the object metadata; only their formulas differ, and each formula is pinned by a unit
    test. So this is the end-to-end arm for the shared VALUE, and what it has to establish on a live
    endpoint is that the value arriving here is an ordinary ETag and not a numeric generation. A
    generation reaching a cache key is the concrete bug the request-mode isolation exists to prevent:
    the same object would acquire different keys depending on whether its metadata came from LIST or
    from HEAD.

    It needs the object-storage TABLE ENGINE, not a disk — `ParquetV3BlockInputFormat` builds the key
    and only `StorageObjectStorageSource` reaches it, so no MergeTree statement can. Selecting
    `gcs_hmac` there requires a NAMED COLLECTION: `StorageS3Configuration::fromNamedCollection` reads
    `http_client`, while the positional argument form does not.

    Would fail if: a generation reached the ETag field on this path — the digit check breaks; or the
    key stopped being stable across two reads of one unchanged object — the hit count stays zero.

    NOT subject to the counter hazard in the module docstring, and that is deliberate rather than
    lucky. Its reachability preconditions are `ParquetMetadataCacheMisses` and
    `ParquetMetadataCacheHits`, which only a Parquet read moves. The CAS disks in this configuration
    cannot touch either, so unlike the S3 counters in the group above these two mean what they say.
    """
    node = cluster.instances["node"]
    events = ["ParquetMetadataCacheMisses", "ParquetMetadataCacheHits"]
    probe = "cache-key-probe-{}.parquet".format(auth_mode)
    table_function = "s3({}, filename='{}', format='Parquet')".format(named_collection, probe)

    node.query(
        "INSERT INTO FUNCTION {} SELECT number AS id, toString(number) AS data FROM numbers(1000)".format(table_function),
        settings={"s3_truncate_on_insert": 1},
    )

    before = _events(node, events)
    # First read: cold, so the metadata is fetched from the object and the key is minted.
    assert int(node.query("SELECT count() FROM {}".format(table_function))) == 1000
    after_cold = _events(node, events)
    assert after_cold["ParquetMetadataCacheMisses"] > before["ParquetMetadataCacheMisses"], "no Parquet metadata cache miss, so the read never reached the object and nothing below is meaningful"

    # Second read of the same unchanged object: the key must be rebuilt identically and hit.
    assert int(node.query("SELECT count() FROM {}".format(table_function))) == 1000
    after_warm = _events(node, events)
    assert after_warm["ParquetMetadataCacheHits"] > after_cold["ParquetMetadataCacheHits"], "the second read of an unchanged object missed the cache, so the key is not stable"

    # The key's own ETag component comes from `cache miss <path> | <etag>`. Raw provider values stay
    # inside a traceback-hidden helper so a domain failure cannot disclose one through `--showlocals`.
    cache_key_logged, etag_observed, ordinary_etag_domain = _ordinary_etag_domain(node, probe)
    assert cache_key_logged, "the cache logged no key for this object, so the ETag domain cannot be checked"
    assert etag_observed, "the cache key carried no observable ETag"
    assert ordinary_etag_domain, "the cache key ETag was empty or entered the numeric generation domain"


# ---------------------------------------------------------------------------------------------------
# Groups 2 and 3: NativeConditional requests, on `gcp_oauth` and on `gcs_hmac`. Reaching a readable
# table is the strongest single assertion available: a CAS mount runs `runCapabilityProbe`, which
# requires conditional create, conditional overwrite, a REFUSED delete on a wrong token and an
# accepted delete on the right one — all against live GCS, all before the mount is allowed to
# complete. A mounted disk means Google accepted every one of them.
# ---------------------------------------------------------------------------------------------------


def _run_cas_group(disk):
    node = cluster.instances["node"]
    table = _create(node, disk)

    node.query("INSERT INTO {} SELECT number, toString(number) FROM numbers(300)".format(table))
    node.query("INSERT INTO {} SELECT number, toString(number) FROM numbers(300, 300)".format(table))
    assert int(node.query("SELECT count() FROM {}".format(table))) == 600
    assert int(node.query("SELECT uniqExact(data) FROM {}".format(table))) == 600

    # A merge rewrites part metadata through the same conditional-write path, and dropping a partition
    # drives the exact, token-carrying DELETE.
    node.query("OPTIMIZE TABLE {} FINAL".format(table))
    node.query("ALTER TABLE {} DROP PARTITION tuple()".format(table))
    assert int(node.query("SELECT count() FROM {}".format(table))) == 0

    generations_seen, numeric_generation_domain = _cas_generation_domain(node, disk)
    assert generations_seen, "no incarnation generation was recorded, so the domain assertion is vacuous"
    assert numeric_generation_domain, "a CAS incarnation left the numeric GCS generation domain"


@requires_oauth
def test_native_conditional_gcp_oauth_mounts_and_keeps_generation_tokens():
    """Group 2. Conditional PUT, native-token HEAD and exact DELETE under bearer-token auth.

    Would fail if: GCS refused a conditional create carrying `x-goog-if-generation-match`, refused an
    exact DELETE, or answered a token-producing write without a generation — the token recorded would
    then be an ETag and the digit assertion would break. It would also fail if the OAuth cleanup left
    a stale AWS signing artifact on the request that Google rejects, which is one of the two things
    only a live endpoint can settle: against `storage.googleapis.com` the `ApiMode::GCS` block in
    `Client::BuildHttpRequest` becomes active, and `test_cas_gcs` cannot reach it.

    Two shapes the plan asks of this group are NOT here, because no configuration this suite can hold
    produces them. Both enumerations are written out rather than asserted, since "nothing can drive
    this" is a claim about a set:

    A CHECKSUM-BEARING or CHUNKED/FRAMED PUT. The only producer of `x-amz-checksum-*` is
    `RequestChecksumRequired`, which returns `is_s3express_bucket`, and `setChecksumAlgorithm` has
    exactly one caller, `setIsS3ExpressBucket`. `is_s3express_bucket` has one source,
    `S3::isS3ExpressEndpoint(url.endpoint)`, which is `endpoint.contains("s3express")`. This gate
    REQUIRES the endpoint to be `storage.googleapis.com` — that substring is what makes
    `deduceProviderType` report GCS, which is the property the gate exists to exercise. `disable_checksum`
    only suppresses `Content-MD5`; it never turns checksum headers on. So against a real GCS endpoint
    the aws-chunked framing headers cannot appear, and the allowlist's `Consume` rule for
    `x-amz-checksum-` and `Reject` rules for `x-amz-trailer` / `x-amz-decoded-content-length` are
    reachable only from the dialect unit tests. They guard a future SDK change, not a current config.

    An ATTRIBUTE ROUND TRIP. No production path fills object attributes on any object storage: every
    `writeObject` caller outside the object-storage layer passes `/* attributes= */ {}`, no
    `ObjectAttributes{...}` is constructed outside tests, and every CAS `putIfAbsent` /
    `nativeConditionalPut` site forwards a `meta` parameter without ever building a non-empty one. So
    there is no SQL statement that writes custom metadata, and nothing to read back.
    """
    _run_cas_group(CAS_OAUTH_DISK)


@requires_hmac
def test_native_conditional_gcs_hmac_mounts_and_keeps_generation_tokens():
    """Group 3. The same three operations under GOOG4 signing.

    Would fail if: the GOOG4 signed-header allowlist produced a signature Google rejects for a
    conditional request — the conditional headers are exactly the ones an allowlist bug would drop or
    fail to cover, and no unit test can tell a signature Google accepts from one it does not.
    """
    _run_cas_group(CAS_HMAC_DISK)


# ---------------------------------------------------------------------------------------------------
# Group 4: the unconditional blob-publication protocol against real GCS. Each case is run once on a
# bearer-token client and once on a GOOG4 client when that credential source is available. Assertions
# use the statement's own query id, so background work or a sibling authentication mode cannot make a
# scenario green.
# ---------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("auth_mode,stream_disk", CAS_STREAM_CASES)
def test_live_cas_fresh_streaming_then_duplicate_adoption(auth_mode, stream_disk):
    """A fresh body streams after a miss; byte-identical reuse performs no second publication."""
    node = cluster.instances["node"]
    table = "task10_fresh_{}".format(auth_mode)
    _create_payload_table(node, table, stream_disk)
    insert = _small_payload_insert(table, auth_mode, "fresh-duplicate")

    fresh_query_id = _query_id("fresh", auth_mode)
    node.query(insert, query_id=fresh_query_id)
    _assert_one_head_per_blob_task(node, fresh_query_id)
    fresh_events = _cas_events(
        node,
        stream_disk,
        query_id=fresh_query_id,
        event_types=("blob_put", "blob_reuse_adopt"),
    )
    target = _largest_blob_put(fresh_events)
    target_hash = target["object_hash"]
    assert target["detail"].get("publication_reason") == "absent"
    assert target["detail"].get("transport") == "streaming"

    duplicate_query_id = _query_id("duplicate", auth_mode)
    node.query(insert, query_id=duplicate_query_id)
    duplicate_profile = _assert_one_head_per_blob_task(node, duplicate_query_id)
    duplicate_events = _cas_events(
        node,
        stream_disk,
        query_id=duplicate_query_id,
        event_types=("blob_put", "blob_reuse_adopt"),
        object_hash=target_hash,
    )
    assert [event for event in duplicate_events if event["event_type"] == "blob_reuse_adopt"]
    assert not [event for event in duplicate_events if event["event_type"] == "blob_put"]
    assert duplicate_profile["CASBlobHead"] > 0, duplicate_profile
    assert int(node.query("SELECT count() FROM {}".format(table))) == 128
    node.query("DROP TABLE {} SYNC".format(table))


@pytest.mark.parametrize("auth_mode,stream_disk", CAS_STREAM_CASES)
def test_live_cas_concurrent_equivalent_publishers(auth_mode, stream_disk):
    """Two equivalent writers may race unconditionally, but both publish one readable value."""
    node = cluster.instances["node"]
    tables = [
        "task10_concurrent_{}_a".format(auth_mode),
        "task10_concurrent_{}_b".format(auth_mode),
    ]
    for table in tables:
        _create_payload_table(node, table, stream_disk)

    query_ids = [
        _query_id("concurrent_a", auth_mode),
        _query_id("concurrent_b", auth_mode),
    ]
    barrier = threading.Barrier(2)

    def publish(table, query_id):
        barrier.wait()
        node.query(_small_payload_insert(table, auth_mode, "concurrent"), query_id=query_id)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(publish, table, query_id) for table, query_id in zip(tables, query_ids)]
        for future in futures:
            future.result()

    event_sets = []
    for query_id in query_ids:
        _assert_one_head_per_blob_task(node, query_id)
        event_sets.append(
            _cas_events(
                node,
                stream_disk,
                query_id=query_id,
                event_types=("blob_put", "blob_reuse_adopt"),
            )
        )
    common = set(event["object_hash"] for event in event_sets[0]) & set(event["object_hash"] for event in event_sets[1])
    assert common, "the equivalent writers touched no common content hash"
    target_hash = max(
        common,
        key=lambda object_hash: max(int(event["detail"].get("size", "0")) for events in event_sets for event in events if event["object_hash"] == object_hash),
    )
    target_events = [event for events in event_sets for event in events if event["object_hash"] == target_hash]
    publications = [event for event in target_events if event["event_type"] == "blob_put"]
    assert publications, "neither concurrent writer published the shared target"
    for publication in publications:
        assert publication["detail"].get("publication_reason") == "absent"
        assert publication["detail"].get("transport") == "streaming"
    for table in tables:
        assert int(node.query("SELECT count() FROM {}".format(table))) == 64
        assert int(node.query("SELECT uniqExact(payload) FROM {}".format(table))) == 64
        node.query("DROP TABLE {} SYNC".format(table))


@pytest.mark.parametrize("auth_mode,stream_disk", CAS_STREAM_CASES)
def test_live_cas_streaming_blob_above_the_former_conditional_cap(auth_mode, stream_disk):
    """A Default single-part blob PUT succeeds above the genuine-conditional GCS ceiling."""
    node = cluster.instances["node"]
    table = "task10_former_cap_{}".format(auth_mode)
    _create_payload_table(node, table, stream_disk)
    query_id = _query_id("former_cap", auth_mode)
    node.query(
        _large_payload_insert(table, 1100000 if auth_mode == "gcs_hmac" else 1200000),
        query_id=query_id,
        settings={"s3_max_single_part_upload_size": 64 * 1024 * 1024},
    )
    _assert_one_head_per_blob_task(node, query_id)
    target = _largest_blob_put(_cas_events(node, stream_disk, query_id=query_id, event_types=("blob_put",)))
    assert int(target["detail"].get("size", "0")) > FORMER_CONDITIONAL_PUT_CAP
    assert target["detail"].get("publication_reason") == "absent"
    assert target["detail"].get("transport") == "streaming"
    multipart = _query_profile_events(node, query_id, ("S3CreateMultipartUpload",))
    assert multipart["S3CreateMultipartUpload"] == 0, multipart
    assert int(node.query("SELECT length(payload) FROM {}".format(table))) > FORMER_CONDITIONAL_PUT_CAP
    node.query("DROP TABLE {} SYNC".format(table))


@pytest.mark.parametrize("auth_mode,stream_disk", CAS_STREAM_CASES)
def test_live_cas_default_blob_publication_uses_multipart(auth_mode, stream_disk):
    """A large Default blob body reaches Google's multipart create/part/complete protocol."""
    node = cluster.instances["node"]
    table = "task10_multipart_{}".format(auth_mode)
    _create_payload_table(node, table, stream_disk)
    query_id = _query_id("multipart", auth_mode)
    node.query(
        _large_payload_insert(table, 2100000 if auth_mode == "gcs_hmac" else 2200000),
        query_id=query_id,
        settings={
            "s3_max_single_part_upload_size": 0,
            "s3_min_upload_part_size": FORMER_CONDITIONAL_PUT_CAP,
        },
    )
    _assert_one_head_per_blob_task(node, query_id)
    target = _largest_blob_put(_cas_events(node, stream_disk, query_id=query_id, event_types=("blob_put",)))
    assert int(target["detail"].get("size", "0")) > FORMER_CONDITIONAL_PUT_CAP
    assert target["detail"].get("transport") == "streaming"
    multipart = _query_profile_events(
        node,
        query_id,
        ("S3CreateMultipartUpload", "S3UploadPart", "S3CompleteMultipartUpload"),
    )
    for event in multipart.values():
        assert event > 0, multipart
    assert int(node.query("SELECT count() FROM {}".format(table))) == 1
    node.query("DROP TABLE {} SYNC".format(table))


@pytest.mark.parametrize("auth_mode,staged_disk", CAS_STAGED_CASES)
def test_live_cas_native_staged_copy_is_first_absent_publication(auth_mode, staged_disk):
    """An S3-staged source uses Google's native copy exactly on first-plus-absent."""
    node = cluster.instances["node"]
    table = "task10_staged_{}".format(auth_mode)
    _create_payload_table(node, table, staged_disk)
    query_id = _query_id("staged", auth_mode)
    node.query(_small_payload_insert(table, auth_mode, "native-staged"), query_id=query_id)
    _assert_one_head_per_blob_task(node, query_id)
    put_events = _cas_events(node, staged_disk, query_id=query_id, event_types=("blob_put",))
    target = _largest_blob_put(put_events)
    assert target["detail"].get("publication_reason") == "absent"
    assert target["detail"].get("transport") == "server_side_copy"
    copy_profile = _query_profile_events(node, query_id, ("S3CopyObject",))
    copy_publications = [event for event in put_events if event["detail"].get("transport") == "server_side_copy"]
    assert copy_publications, "the staged statement emitted no successful native publication"
    assert copy_profile["S3CopyObject"] == len(copy_publications), (
        copy_profile,
        len(copy_publications),
    )
    assert int(node.query("SELECT count() FROM {}".format(table))) == 64
    node.query("DROP TABLE {} SYNC".format(table))


def test_batch_delete_log_pattern_matches_literal_prefix_via_grep_in_log(tmp_path):
    """The ordinary batch-delete matcher must survive `grep_in_log`'s regex-mode `zgrep`."""
    representative = "Objects with paths [/bucket/prefix/plain/a] were removed from S3"
    absent = "Object with path /bucket/prefix/plain/a was removed from S3"
    (tmp_path / "batch.log").write_text(representative + "\n", encoding="utf-8")
    (tmp_path / "absent.log").write_text(absent + "\n", encoding="utf-8")

    instance = object.__new__(ClickHouseInstance)
    instance.logs_dir = str(tmp_path)

    matched = instance.grep_in_log(
        BATCH_DELETE_LOG_PATTERN,
        from_host=True,
        filename="batch.log",
        only_latest=True,
    )
    missing = instance.grep_in_log(
        BATCH_DELETE_LOG_PATTERN,
        from_host=True,
        filename="absent.log",
        only_latest=True,
    )
    assert matched.strip() == representative
    assert missing == ""


def test_generation_evidence_does_not_cross_the_test_frame_boundary():
    """CAS generations stay inside traceback-hidden helpers; callers receive redacted evidence."""

    class GenerationEvidenceProbeNode:
        def query(self, query):
            if "FORMAT JSONEachRow" in query:
                return '{"event_type":"blob_retire","object_hash":"probe-hash","token":"0","outcome":"pending","detail":{}}\n'
            return '"0"\n'

    node = GenerationEvidenceProbeNode()
    events = _cas_events(
        node,
        "probe-disk",
        event_types=("blob_retire",),
        object_hash="probe-hash",
    )
    assert events == [
        {
            "event_type": "blob_retire",
            "object_hash": "probe-hash",
            "outcome": "pending",
            "detail": {},
        }
    ]

    seen, all_numeric = _cas_generation_domain(node, "probe-disk")
    assert seen is True
    assert all_numeric is True

    count, numeric, generation_digest = _cas_event_generation_evidence(
        node,
        "probe-disk",
        "probe-hash",
        "blob_retire",
        outcome="pending",
    )
    assert count == 1
    assert numeric is True
    assert len(generation_digest) == 64
    assert all(character in string.hexdigits for character in generation_digest)
    assert not any(value == "0" or '"token":"0"' in repr(value) for value in locals().values()), "a raw generation reached the focused test frame"


@pytest.mark.parametrize("auth_mode,staged_disk", CAS_STAGED_CASES)
def test_live_cas_condemned_staged_source_retags_by_streaming(auth_mode, staged_disk):
    """A staged payload observed as `Condemned` gets a new streaming envelope."""
    node = cluster.instances["node"]
    first_table = "task10_condemned_{}_first".format(auth_mode)
    second_table = "task10_condemned_{}_second".format(auth_mode)
    insert_scenario = "condemned-retag"

    _create_payload_table(node, first_table, staged_disk)
    first_query_id = _query_id("condemned_seed", auth_mode)
    node.query(
        _small_payload_insert(first_table, auth_mode, insert_scenario),
        query_id=first_query_id,
    )
    target = _largest_blob_put(_cas_events(node, staged_disk, query_id=first_query_id, event_types=("blob_put",)))
    target_hash = target["object_hash"]
    assert target["detail"].get("transport") == "server_side_copy"
    node.query("DROP TABLE {} SYNC".format(first_table))

    _gc_until(node, staged_disk, target_hash, "blob_retire")
    retired_count, retired_numeric, retired_generation_digest = _cas_event_generation_evidence(
        node,
        staged_disk,
        target_hash,
        "blob_retire",
    )
    assert retired_count == 1, "the target did not record exactly one retirement generation"
    assert retired_numeric, "the retired incarnation left the numeric GCS generation domain"
    assert retired_generation_digest, "the retired incarnation produced no opaque generation evidence"

    _create_payload_table(node, second_table, staged_disk)
    retag_query_id = _query_id("condemned_retag", auth_mode)
    node.query(
        _small_payload_insert(second_table, auth_mode, insert_scenario),
        query_id=retag_query_id,
    )
    target_events = _cas_events(
        node,
        staged_disk,
        query_id=retag_query_id,
        event_types=("blob_put", "blob_reuse_adopt"),
        object_hash=target_hash,
    )
    assert len(target_events) == 1, "the condemned target did not have one publication decision"
    retag = target_events[0]
    assert retag["event_type"] == "blob_put"
    assert retag["detail"].get("publication_reason") == "condemned"
    assert retag["detail"].get("transport") == "streaming"
    assert int(node.query("SELECT count() FROM {}".format(second_table))) == 64
    node.query("DROP TABLE {} SYNC".format(second_table))


@requires_ambiguity_driver
@pytest.mark.parametrize("auth_mode,ambiguity_disk", CAS_AMBIGUITY_CASES)
def test_live_cas_queued_old_token_delete_misses_retagged_replacement(auth_mode, ambiguity_disk):
    """Hold a queued old exact DELETE across retagging, then require a provider mismatch.

    `POST /v1/queued-old-delete/arm` accepts the scenario id, non-secret prefix, and target content
    hash. After the next GC cut it holds the already-authenticated exact DELETE without changing it.
    `POST /v1/queued-old-delete/status` reports only `old_delete_held`; release forwards that same
    request after the writer has replaced the generation. The final result must report a provider
    precondition mismatch and a surviving replacement. The driver never returns or records the
    signed request, authorization material, or generation value.
    """
    node = cluster.instances["node"]
    first_table = "task10_queued_delete_{}_first".format(auth_mode)
    replacement_table = "task10_queued_delete_{}_replacement".format(auth_mode)
    insert_scenario = "queued-old-delete"

    _create_payload_table(node, first_table, ambiguity_disk)
    first_query_id = _query_id("queued_delete_seed", auth_mode)
    node.query(
        _small_payload_insert(first_table, auth_mode, insert_scenario),
        query_id=first_query_id,
    )
    target = _largest_blob_put(
        _cas_events(
            node,
            ambiguity_disk,
            query_id=first_query_id,
            event_types=("blob_put",),
        )
    )
    target_hash = target["object_hash"]
    assert target["detail"].get("transport") == "server_side_copy"
    node.query("DROP TABLE {} SYNC".format(first_table))

    _gc_until(node, ambiguity_disk, target_hash, "blob_retire")
    retired_count, retired_numeric, retired_generation_digest = _cas_event_generation_evidence(
        node,
        ambiguity_disk,
        target_hash,
        "blob_retire",
    )
    assert retired_count == 1, "the queued target did not record exactly one retirement generation"
    assert retired_numeric, "the queued incarnation left the numeric GCS generation domain"
    assert retired_generation_digest, "the queued incarnation produced no opaque generation evidence"
    _gc_until(
        node,
        ambiguity_disk,
        target_hash,
        "gc_recheck_verdict",
        outcome="pending",
    )
    _create_payload_table(node, replacement_table, ambiguity_disk)

    scenario_id = _query_id("queued_delete_driver", auth_mode)
    armed = _ambiguity_driver_request(
        "/v1/queued-old-delete/arm",
        {
            "scenario_id": scenario_id,
            "object_prefix": "{}/{}/".format(PREFIX, AMBIGUITY_SUBPREFIX[auth_mode]),
            "target_object_hash": target_hash,
        },
    )
    assert armed.get("armed") is True, "the queued-delete driver did not arm"

    replacement_query_id = _query_id("queued_delete_retag", auth_mode)
    with ThreadPoolExecutor(max_workers=1) as pool:
        gc_future = pool.submit(node.query, "SYSTEM CAS GC RUN '{}'".format(ambiguity_disk))
        try:
            _wait_for_driver_phase("/v1/queued-old-delete/status", scenario_id, "old_delete_held")
            node.query(
                _small_payload_insert(replacement_table, auth_mode, insert_scenario),
                query_id=replacement_query_id,
            )
        finally:
            _ambiguity_driver_request("/v1/queued-old-delete/release", {"scenario_id": scenario_id})
        gc_future.result()

    retag_events = _cas_events(
        node,
        ambiguity_disk,
        query_id=replacement_query_id,
        event_types=("blob_put", "blob_reuse_adopt"),
        object_hash=target_hash,
    )
    assert len(retag_events) == 1, "the held-delete target had no single writer decision"
    assert retag_events[0]["event_type"] == "blob_put"
    assert retag_events[0]["detail"].get("publication_reason") == "condemned"
    assert retag_events[0]["detail"].get("transport") == "streaming"

    delete_events = _cas_events(
        node,
        ambiguity_disk,
        event_types=("blob_delete",),
        object_hash=target_hash,
    )
    replaced = [event for event in delete_events if event["outcome"] == "replaced"]
    assert len(replaced) == 1, "the released old exact delete did not miss the replacement"
    replaced_count, replaced_numeric, replaced_generation_digest = _cas_event_generation_evidence(
        node,
        ambiguity_disk,
        target_hash,
        "blob_delete",
        outcome="replaced",
    )
    assert replaced_count == 1, "the replaced delete did not record exactly one generation"
    assert replaced_numeric, "the replaced delete left the numeric GCS generation domain"
    assert replaced_generation_digest == retired_generation_digest, "the released delete did not carry the generation captured at retirement"

    result = _ambiguity_driver_request("/v1/queued-old-delete/result", {"scenario_id": scenario_id})
    for phase in (
        "old_delete_forwarded",
        "provider_precondition_mismatch",
        "replacement_present_after_old_delete",
    ):
        assert result.get(phase) is True, "queued-delete phase {} is unproven".format(phase)
    assert int(node.query("SELECT count() FROM {}".format(replacement_table))) == 64
    node.query("DROP TABLE {} SYNC".format(replacement_table))


@requires_ambiguity_driver
@pytest.mark.parametrize("auth_mode,ambiguity_disk", CAS_AMBIGUITY_CASES)
def test_live_cas_ambiguous_staged_copy_absent_retry_uses_retagged_replacement(auth_mode, ambiguity_disk):
    """A landed copy loses its response, is exact-deleted, then retries absent by streaming.

    The driver contract is intentionally narrow. `POST /v1/staged-copy-ambiguity/arm` accepts a
    scenario id, authentication-mode label, and non-secret object prefix. It transparently forwards
    all other traffic. For the first native copy below that prefix it must: let Google accept the
    copy; suppress the response; exact-delete the landed generation before the retry `HEAD`; let the
    retry and retagged PUT complete; retry the OLD exact delete after replacement; and `POST
    /v1/staged-copy-ambiguity/result` returns only phase booleans plus the target content hash. The
    proxy owns whatever provider credentials its exact-delete control plane needs; this test never
    sends, reads, logs, or records them.
    """
    node = cluster.instances["node"]
    table = "task10_ambiguity_{}".format(auth_mode)
    _create_payload_table(node, table, ambiguity_disk)
    scenario_id = _query_id("ambiguity_driver", auth_mode)
    armed = _ambiguity_driver_request(
        "/v1/staged-copy-ambiguity/arm",
        {
            "scenario_id": scenario_id,
            "auth_mode": auth_mode,
            "object_prefix": "{}/{}/".format(PREFIX, AMBIGUITY_SUBPREFIX[auth_mode]),
        },
    )
    assert armed.get("armed") is True, "the ambiguity driver did not arm"

    query_id = _query_id("ambiguity", auth_mode)
    node.query(
        _small_payload_insert(table, auth_mode, "ambiguity-absent-retag"),
        query_id=query_id,
    )
    report = _ambiguity_driver_request("/v1/staged-copy-ambiguity/result", {"scenario_id": scenario_id})
    for phase in (
        "first_native_copy_landed",
        "first_response_lost",
        "old_incarnation_exact_deleted",
        "retry_head_observed_absent",
        "retagged_replacement_landed",
        "queued_old_delete_missed_replacement",
        "replacement_present_after_old_delete",
    ):
        assert report.get(phase) is True, "ambiguity driver phase {} is unproven".format(phase)
    target_hash = report.get("target_object_hash", "")
    assert target_hash, "the ambiguity driver did not identify its target hash"

    events = _cas_events(
        node,
        ambiguity_disk,
        query_id=query_id,
        event_types=("blob_put",),
        object_hash=target_hash,
    )
    assert len(events) == 1, "the retried target did not emit one successful publication"
    assert events[0]["detail"].get("publication_reason") == "absent"
    assert events[0]["detail"].get("transport") == "streaming"
    profile = _query_profile_events(node, query_id, ("S3CopyObject", "S3PutObject"))
    assert profile["S3CopyObject"] > 0, profile
    assert profile["S3PutObject"] > 0, profile
    assert int(node.query("SELECT count() FROM {}".format(table))) == 64
