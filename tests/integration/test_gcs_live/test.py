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

Every group is opt-in through environment variables and skips cleanly when they are absent. This
suite touches a real bucket and issues real, billable requests, so it must never run by default.

  - `GCS_LIVE_BUCKET`             — required for any group. A bucket the caller is willing to have
                                    objects created and deleted in.
  - `GCS_LIVE_PREFIX`             — optional key prefix, default `clickhouse-gcs-live-gate`. A random
                                    per-run suffix is always appended, so two concurrent runs cannot
                                    share a prefix.
  - `GCS_LIVE_HMAC_ACCESS_KEY_ID` — a GOOG4 HMAC key pair. Enables groups 1 and 3.
  - `GCS_LIVE_HMAC_SECRET_ACCESS_KEY`
  - `GCS_LIVE_OAUTH_FROM_METADATA=1` — declares that the HOST running this suite can reach the GCE
                                    metadata server and that its service account may write to the
                                    bucket. Enables group 2 on a GCE instance.
  - `GCS_LIVE_OAUTH_ADC_CLIENT_ID` — Application Default Credentials, the alternative that enables
  - `GCS_LIVE_OAUTH_ADC_CLIENT_SECRET`  group 2 from ANYWHERE, not just on GCE. A CAS disk accepts
  - `GCS_LIVE_OAUTH_ADC_REFRESH_TOKEN`  these: `non_cas_keys` in `ContentAddressedSettings.cpp` lists
                                    `metadata_service`, `request_token_path`, `service_account` and the
                                    whole ADC triple, and its own comment says the triple "is the only
                                    way to run `gcp_oauth` off a GCE instance". Either source is
                                    enough; the ADC one exists because requiring a GCE host is what
                                    would keep this gate from ever being run.

Only disks whose gates are satisfied are written into the configuration. That is deliberate: a CAS
disk mounts and runs its capability battery at server startup with no fallback, so an unusable CAS
disk in the config would stop the server and take the other groups down with it.

## What this gate asserts, and what it deliberately does not

It asserts what a client can observe: that each operation SUCCEEDS against Google, that the operation
the test names was actually issued (read off `system.events`, so a statement that silently stopped
reaching S3 cannot leave an assertion vacuously true), and that the tokens CAS recorded are
generations rather than ETags.

## OPEN QUESTION FOR WHOEVER FIRST RUNS THIS WITH CREDENTIALS

`system.events` counters are PROCESS-WIDE, and this configuration also holds two CAS disks whose
background collection issues object-storage requests of its own. So every counter delta asserted here
is only as sound as the assumption that no CAS activity moved that counter inside the measured window.
Where that assumption fails, the assertion still passes — for a reason that has nothing to do with the
statement it names.

This is an open question, not a known defect: which counters CAS can actually move during these
windows is not determinable without a real run. It is written here rather than in a tracked item
because the first run is when it matters and this docstring is what its reader will have in front of
them. **On that run, check each counter individually instead of trusting a pass** — for any counter CAS
can move, a green assertion is not evidence that the statement under test issued the operation.

One test is EXEMPT, and the reason is the template for clearing the others:
`test_default_gcs_hmac_parquet_metadata_cache_keys_on_the_ordinary_etag` uses
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

import os
import random
import string

import pytest

from helpers.cluster import ClickHouseCluster

BUCKET = os.environ.get("GCS_LIVE_BUCKET", "")
BASE_PREFIX = os.environ.get("GCS_LIVE_PREFIX", "clickhouse-gcs-live-gate")
HMAC_KEY_ID = os.environ.get("GCS_LIVE_HMAC_ACCESS_KEY_ID", "")
HMAC_SECRET = os.environ.get("GCS_LIVE_HMAC_SECRET_ACCESS_KEY", "")
OAUTH_FROM_METADATA = os.environ.get("GCS_LIVE_OAUTH_FROM_METADATA", "") == "1"
ADC_CLIENT_ID = os.environ.get("GCS_LIVE_OAUTH_ADC_CLIENT_ID", "")
ADC_CLIENT_SECRET = os.environ.get("GCS_LIVE_OAUTH_ADC_CLIENT_SECRET", "")
ADC_REFRESH_TOKEN = os.environ.get("GCS_LIVE_OAUTH_ADC_REFRESH_TOKEN", "")
ADC_AVAILABLE = bool(ADC_CLIENT_ID and ADC_CLIENT_SECRET and ADC_REFRESH_TOKEN)

HMAC_AVAILABLE = bool(BUCKET and HMAC_KEY_ID and HMAC_SECRET)
# Either token source satisfies group 2 — the GCE metadata server, or Application Default Credentials.
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
CAS_OAUTH_DISK = "live_cas_oauth"
CAS_HMAC_DISK = "live_cas_hmac"

pytestmark = pytest.mark.skipif(
    not BUCKET,
    reason="live GCS gate: set GCS_LIVE_BUCKET (and the per-group credential variables) to run it",
)

cluster = ClickHouseCluster(__file__)


def _disk_xml(name, subprefix, cas, client, bucket=None, skip_access_check=False):
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
        ]
    lines += [
        "                <endpoint>{}/{}/{}/{}/</endpoint>".format(
            GCS_ENDPOINT, bucket or BUCKET, PREFIX, subprefix
        ),
        "                <http_client>{}</http_client>".format(client),
    ]
    if client == "gcs_hmac":
        lines += [
            "                <access_key_id>{}</access_key_id>".format(HMAC_KEY_ID),
            "                <secret_access_key>{}</secret_access_key>".format(HMAC_SECRET),
        ]
    if client == "gcp_oauth" and ADC_AVAILABLE:
        # `requestBearerToken` picks between the GCE metadata server and these; a CAS disk accepts them
        # (they are in `non_cas_keys`). Present only when supplied, so a GCE run keeps using metadata.
        lines += [
            "                <google_adc_client_id>{}</google_adc_client_id>".format(ADC_CLIENT_ID),
            "                <google_adc_client_secret>{}</google_adc_client_secret>".format(
                ADC_CLIENT_SECRET
            ),
            "                <google_adc_refresh_token>{}</google_adc_refresh_token>".format(
                ADC_REFRESH_TOKEN
            ),
        ]
    lines.append("            </{}>".format(name))
    return "\n".join(lines)


def _policy_xml(name):
    return (
        "            <{name}>\n"
        "                <volumes><main><disk>{name}</disk></main></volumes>\n"
        "            </{name}>".format(name=name)
    )


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
        ]
        policies += [
            _policy_xml(HMAC_ABSENT_BUCKET_DISK),
            _policy_xml(CAS_HMAC_DISK),
            "            <{policy}>\n"
            "                <volumes>\n"
            "                    <hot><disk>{hot}</disk></hot>\n"
            "                    <cold><disk>{cold}</disk></cold>\n"
            "                </volumes>\n"
            "            </{policy}>".format(
                policy=HMAC_TWO_VOLUME_POLICY, hot=HMAC_PLAIN_DISK, cold=HMAC_PLAIN_DISK_2
            ),
        ]
    if OAUTH_AVAILABLE:
        disks.append(_disk_xml(CAS_OAUTH_DISK, "cas-oauth", cas=True, client="gcp_oauth"))
        policies.append(_policy_xml(CAS_OAUTH_DISK))

    named_collections = ""
    if HMAC_AVAILABLE:
        named_collections = (
            "    <named_collections>\n"
            "        <{name}>\n"
            "            <url>{endpoint}/{bucket}/{prefix}/parquet/</url>\n"
            "            <access_key_id>{key}</access_key_id>\n"
            "            <secret_access_key>{secret}</secret_access_key>\n"
            "            <http_client>gcs_hmac</http_client>\n"
            "        </{name}>\n"
            "    </named_collections>\n".format(
                name=PARQUET_NAMED_COLLECTION,
                endpoint=GCS_ENDPOINT,
                bucket=BUCKET,
                prefix=PREFIX,
                key=HMAC_KEY_ID,
                secret=HMAC_SECRET,
            )
        )

    with open(path, "w", encoding="utf-8") as out:
        out.write("<clickhouse>\n    <storage_configuration>\n        <disks>\n")
        out.write("\n".join(disks))
        out.write("\n        </disks>\n        <policies>\n")
        out.write("\n".join(policies))
        out.write("\n        </policies>\n    </storage_configuration>\n")
        out.write(named_collections)
        out.write("</clickhouse>\n")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    configs_dir = os.path.join(os.path.dirname(__file__), "configs")
    os.makedirs(configs_dir, exist_ok=True)
    config_path = os.path.join(configs_dir, "live_gcs_generated.xml")
    _write_config(config_path)

    cluster.add_instance("node", main_configs=[config_path], stay_alive=True)
    try:
        cluster.start()
        yield cluster
    finally:
        # Everything this run wrote lives under `PREFIX`, which carries a per-run random suffix, so no
        # two runs and nothing pre-existing can collide. The DROPs below let each CAS pool retire its
        # own metadata rather than deleting a live pool's keys from underneath it.
        #
        # They do NOT leave the bucket exactly as found: the Parquet test writes a plain object through
        # a table function, and there is no SQL verb that deletes an object. `PREFIX/parquet/` therefore
        # survives the run. Delete the whole `PREFIX` afterwards, or give the bucket a lifecycle rule —
        # this suite runs against a real, billable bucket and cannot clean that one key itself.
        node = cluster.instances.get("node")
        if node is not None:
            for disk in (HMAC_PLAIN_DISK, CAS_HMAC_DISK, CAS_OAUTH_DISK):
                try:
                    node.query("DROP TABLE IF EXISTS t_{} SYNC".format(disk))
                    node.query("DROP TABLE IF EXISTS src_{} SYNC".format(disk))
                except Exception:  # noqa: BLE001 - teardown must not mask a test failure
                    pass
        cluster.shutdown()


def _events(node, names):
    """Current values of the named `system.events` counters, zero-filled for absent ones."""
    rows = node.query(
        "SELECT event, value FROM system.events WHERE event IN ({}) FORMAT TSV".format(
            ", ".join("'{}'".format(n) for n in names)
        )
    )
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
        """.format(
            table, disk
        )
    )
    return table


def _cas_tokens(node, disk):
    """Every non-empty incarnation token `system.cas_log` recorded for this disk."""
    node.query("SYSTEM FLUSH LOGS")
    raw = node.query(
        "SELECT DISTINCT token FROM system.cas_log WHERE disk_name = '{}' AND token != '' "
        "FORMAT TSV".format(disk)
    )
    # The transport quotes a generation, and `tokenForHead` is what strips that syntax; strip it here
    # too so the digit check below is about the token's DOMAIN and not about its quoting.
    return [line.strip().strip('"') for line in raw.strip().splitlines() if line.strip()]


# ---------------------------------------------------------------------------------------------------
# Group 1: Default requests on a `gcs_hmac` client, i.e. the ordinary object-storage path under GOOG4
# signing. Nothing here is content-addressed; the question is only whether GCS accepts every operation
# an ordinary ClickHouse disk issues once the signing path stopped being AWS SigV4.
# ---------------------------------------------------------------------------------------------------

requires_hmac = pytest.mark.skipif(
    not HMAC_AVAILABLE,
    reason="set GCS_LIVE_HMAC_ACCESS_KEY_ID and GCS_LIVE_HMAC_SECRET_ACCESS_KEY",
)
requires_oauth = pytest.mark.skipif(
    not OAUTH_AVAILABLE,
    reason="set GCS_LIVE_OAUTH_FROM_METADATA=1 on a GCE host, or the GCS_LIVE_OAUTH_ADC_* triple "
    "anywhere else",
)


@requires_hmac
def test_default_gcs_hmac_accepts_the_ordinary_object_storage_operation_set():
    """Every S3 operation an ordinary disk issues, accepted by GCS under GOOG4 signing.

    The `system.events` deltas are what make this non-vacuous: each named operation must have been
    issued at least once, so a statement that quietly stopped reaching object storage — because a
    default changed, or because a part stayed in memory — cannot leave the assertion true.

    The statement-to-operation mapping is deliberately NOT pinned. Which statement produces a batch
    delete rather than singular ones is a ClickHouse implementation detail that moves between versions;
    whether GCS accepts a batch delete is what this gate is asking. Pinning the mapping would make this
    test fail on refactors that say nothing about GCS.

    Object LISTING is not covered by THIS test — an ordinary MergeTree lifecycle on a local-metadata
    disk never issues one, see the comment on `counters` below.
    `test_default_gcs_hmac_accepts_an_object_listing` covers it on the same `gcs_hmac` client through
    the table-engine path, which is a lister.

    Would fail if: GOOG4 signing produced a signature Google rejects for some operation, or the
    existing `access_key_id`/`secret_access_key` spelling stopped being accepted by the `gcs_hmac`
    selector (the disk would not resolve and no statement below would run).
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

    table = _create(node, HMAC_TWO_VOLUME_POLICY, "t_" + HMAC_PLAIN_DISK)

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
        assert after[name] > before[name], (
            "{} was never issued ({} -> {}), so GCS acceptance of it is unproven".format(
                name, before[name], after[name]
            )
        )

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
    batch_lines = [
        line
        for line in node.grep_in_log("Objects with paths [").splitlines()
        if PREFIX in line
    ]
    assert batch_lines, (
        "no batch delete was logged for this run's own keys, so GCS acceptance of the batch "
        "DeleteObjects shape is unproven"
    )
    assert not node.grep_in_log("DeleteObjects is not supported"), (
        "GCS refused the batch DeleteObjects shape and ClickHouse fell back to singular deletes; the "
        "counter and the row counts cannot see this, which is why it is asserted here"
    )

    # The singular shape needs its own evidence. `S3DeleteObjects` aggregates both, so the counter
    # moving says nothing about which of the two GCS accepted, and the assertions above speak only for
    # the batch one -- a build that never issued a singular DeleteObject at all would satisfy them.
    # Same prefix filter and the same reason for it.
    single_lines = [
        line
        for line in node.grep_in_log("Object with path ").splitlines()
        if PREFIX in line
    ]
    assert single_lines, (
        "no singular delete was logged for this run's own keys, so GCS acceptance of the singular "
        "DeleteObject shape is unproven -- only the batch shape is"
    )


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
    error = node.query_and_get_error(
        "INSERT INTO {} SELECT number, toString(number) FROM numbers(10)".format(table)
    )
    # A parsed S3 error names the bucket problem. An unparsed one surfaces as a bare transport or
    # timeout failure, which is what must not appear.
    assert ("NoSuchBucket" in error) or ("S3_ERROR" in error) or ("ACCESS_DENIED" in error), error


@requires_hmac
def test_default_gcs_hmac_accepts_an_object_listing():
    """GCS accepts a LIST under GOOG4 signing, driven by a glob over the named collection.

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
    for part in (1, 2):
        node.query(
            "INSERT INTO FUNCTION s3({}, filename='listing-probe-{}.parquet', format='Parquet') "
            "SELECT {} AS part, number AS id FROM numbers(10)".format(
                PARQUET_NAMED_COLLECTION, part, part
            ),
            settings={"s3_truncate_on_insert": 1},
        )

    glob = "s3({}, filename='listing-probe-*.parquet', format='Parquet')".format(
        PARQUET_NAMED_COLLECTION
    )
    assert int(node.query("SELECT count() FROM {}".format(glob))) == 20
    # Both objects, through one glob: the listing enumerated them rather than a single key being read.
    assert (
        node.query("SELECT DISTINCT part FROM {} ORDER BY part FORMAT TSV".format(glob)).split()
        == ["1", "2"]
    )


@requires_hmac
def test_default_gcs_hmac_parquet_metadata_cache_keys_on_the_ordinary_etag():
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
    table_function = "s3({}, filename='cache-key-probe.parquet', format='Parquet')".format(
        PARQUET_NAMED_COLLECTION
    )

    node.query(
        "INSERT INTO FUNCTION {} SELECT number AS id, toString(number) AS data "
        "FROM numbers(1000)".format(table_function),
        settings={"s3_truncate_on_insert": 1},
    )

    before = _events(node, events)
    # First read: cold, so the metadata is fetched from the object and the key is minted.
    assert int(node.query("SELECT count() FROM {}".format(table_function))) == 1000
    after_cold = _events(node, events)
    assert after_cold["ParquetMetadataCacheMisses"] > before["ParquetMetadataCacheMisses"], (
        "no Parquet metadata cache miss, so the read never reached the object and nothing below is "
        "meaningful"
    )

    # Second read of the same unchanged object: the key must be rebuilt identically and hit.
    assert int(node.query("SELECT count() FROM {}".format(table_function))) == 1000
    after_warm = _events(node, events)
    assert after_warm["ParquetMetadataCacheHits"] > after_cold["ParquetMetadataCacheHits"], (
        "the second read of an unchanged object missed the cache, so the key is not stable"
    )

    # The key's own ETag component, read off the cache's log line: `cache miss <path> | <etag>`.
    lines = node.grep_in_log("cache-key-probe.parquet |")
    assert lines, "the cache logged no key for this object, so the value below cannot be checked"
    etags = {line.rsplit("|", 1)[1].strip() for line in lines.splitlines() if "|" in line}
    assert etags, lines
    for etag in etags:
        value = etag.strip('"')
        assert value, "the Parquet cache key carried an EMPTY etag: {!r}".format(etag)
        assert not value.isdigit(), (
            "a numeric generation reached the Parquet metadata cache key: {!r}. On this path the "
            "value must be the ordinary ETag".format(etag)
        )


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

    tokens = _cas_tokens(node, disk)
    assert tokens, "no incarnation token was recorded, so the token assertion below is vacuous"
    for token in tokens:
        assert token.isdigit(), (
            "CAS recorded a non-numeric incarnation token {!r} on {}: the response carried an ETag "
            "where the generation adapter should have supplied a generation".format(token, disk)
        )


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
