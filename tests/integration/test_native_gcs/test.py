"""Integration tests for the native Google Cloud Storage backend (google-cloud-cpp).

These exercise the native JSON-API client (selected by `use_native_gcs=1` for the table
function / `GCS` engine, and by `object_storage_type: gcs` for the storage disk) against a
`fake-gcs-server` emulator. minio cannot be used here because it only speaks the S3 API,
which is the *default* (S3-compatibility) path, not the native one.

The native backend requires ClickHouse to be built with the google-cloud-cpp SDK
(`USE_GOOGLE_CLOUD=1`, the default on Linux amd64/aarch64). The whole module is skipped on
builds without it. The disk is defined inline in the CREATE query rather than in a static
config so that the server starts even on such builds (a static `gcs` disk would otherwise
fail startup with UNKNOWN_ELEMENT_IN_CONFIG).
"""

import json
import os
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Two stub token endpoints for the explicit `google_adc_*` tests, differing only in the lifetime
# they report. google-cloud-cpp's caching decorator refreshes a token once it is within
# `GoogleOAuthAccessTokenExpirationSlack()` (4 minutes) of expiry, so a 30-second lifetime makes
# every use a refresh while a 2-hour one is minted once and then served from the cache.
SHORT_LIVED_TOKEN_PORT = 8963
LONG_LIVED_TOKEN_PORT = 8964

# A third stub, standing in for the endpoint an Application Default Credentials file names. The one
# test that must let the client really fall back to ADC points `GOOGLE_APPLICATION_CREDENTIALS` at a
# stub `authorized_user` file whose `token_uri` is this port: without it the ADC chain ends at the
# GCE metadata server, which is unreachable in the test network and hangs the first write.
ADC_TOKEN_PORT = 8965
ADC_CREDENTIALS_PATH = "/var/lib/clickhouse/google_application_credentials.json"

cluster = ClickHouseCluster(__file__)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/forbid_headers.xml",
                "configs/dynamic_gcs_disk_include.xml",
                "configs/dynamic_gcs_disk_include_source.xml",
                "configs/filesystem_caches.xml",
                "configs/custom_local_disks.xml",
                "configs/allowed_disks_for_table_engines.xml",
            ],
            with_gcs=True,
            env_variables={
                "NATIVE_GCS_DYNAMIC_DISK_TYPE": "gcs",
                "GOOGLE_APPLICATION_CREDENTIALS": ADC_CREDENTIALS_PATH,
            },
        )
        cluster.start()

        node = cluster.instances["node"]
        built_with_sdk = (
            node.query(
                "SELECT value FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'"
            ).strip()
            == "1"
        )
        if not built_with_sdk:
            pytest.skip(
                "ClickHouse was built without the google-cloud-cpp SDK (USE_GOOGLE_CLOUD=0)"
            )

        # The stubs run inside the server container, so the server reaches them on localhost.
        start_mock_servers(
            cluster,
            SCRIPT_DIR,
            [
                ("oauth_token_mock.py", "node", SHORT_LIVED_TOKEN_PORT, ["30"]),
                ("oauth_token_mock.py", "node", LONG_LIVED_TOKEN_PORT, ["7200"]),
                ("oauth_token_mock.py", "node", ADC_TOKEN_PORT, ["7200"]),
            ],
        )

        # The Application Default Credentials file the `GOOGLE_APPLICATION_CREDENTIALS` environment
        # variable above names. It is written after the server has started because nothing reads it
        # before the first request that resolves ADC.
        node.exec_in_container(
            [
                "bash",
                "-c",
                "cat > {} <<'CREDENTIALS_EOF'\n{}\nCREDENTIALS_EOF".format(
                    ADC_CREDENTIALS_PATH,
                    json.dumps(
                        {
                            "type": "authorized_user",
                            "client_id": "adc-client-id",
                            "client_secret": "adc-client-secret",
                            "refresh_token": "adc-refresh-token",
                            "token_uri": f"http://localhost:{ADC_TOKEN_PORT}/token",
                        }
                    ),
                ),
            ]
        )

        yield cluster
    finally:
        cluster.shutdown()


def token_exchanges(port):
    """How many `refresh_token` grants the stub endpoint on `port` served, and the last request body."""
    node = cluster.instances["node"]
    response = node.exec_in_container(
        ["curl", "-s", f"http://localhost:{port}/count"], nothrow=True
    )
    parsed = json.loads(response)
    return parsed["exchanges"], parsed["last_body"]


def gcs_url(path):
    # The ClickHouse server reaches the emulator via the docker-network hostname.
    return f"http://{cluster.gcs_host}:{cluster.gcs_port}/{cluster.gcs_bucket}/{path}"


def test_table_function_insert_select(started_cluster):
    node = started_cluster.instances["node"]
    url = gcs_url("tf/data.tsv")

    node.query(
        f"INSERT INTO FUNCTION gcs('{url}', NOSIGN, 'TSV', 'a UInt64, b String') "
        f"SELECT number, toString(number) FROM numbers(100) "
        f"SETTINGS use_native_gcs = 1"
    )

    res = node.query(
        f"SELECT count(), sum(a) FROM gcs('{url}', NOSIGN, 'TSV', 'a UInt64, b String') "
        f"SETTINGS use_native_gcs = 1"
    )
    assert res.strip() == "100\t4950"


def test_table_function_glob(started_cluster):
    node = started_cluster.instances["node"]

    for i in range(3):
        node.query(
            f"INSERT INTO FUNCTION gcs('{gcs_url(f'glob/part{i}.tsv')}', NOSIGN, 'TSV', 'a UInt64') "
            f"SELECT number FROM numbers(10) "
            f"SETTINGS use_native_gcs = 1"
        )

    res = node.query(
        f"SELECT count() FROM gcs('{gcs_url('glob/*.tsv')}', NOSIGN, 'TSV', 'a UInt64') "
        f"SETTINGS use_native_gcs = 1"
    )
    assert res.strip() == "30"


def test_mergetree_on_gcs_disk(started_cluster):
    node = started_cluster.instances["node"]
    disk_endpoint = (
        f"http://{cluster.gcs_host}:{cluster.gcs_port}/{cluster.gcs_bucket}/mergetree/"
    )

    node.query("DROP TABLE IF EXISTS gcs_mt SYNC")
    node.query(
        "CREATE TABLE gcs_mt (a UInt64, b String) ENGINE = MergeTree ORDER BY a "
        "SETTINGS disk = disk("
        "  name = 'gcs_disk_test',"
        "  type = object_storage,"
        "  object_storage_type = gcs,"
        "  metadata_type = local,"
        f"  endpoint = '{disk_endpoint}',"
        "  no_sign_request = true"
        ")",
        settings={"use_native_gcs": 1},
    )

    node.query("INSERT INTO gcs_mt SELECT number, toString(number) FROM numbers(1000)")
    node.query(
        "INSERT INTO gcs_mt SELECT number, toString(number) FROM numbers(1000, 1000)"
    )
    assert node.query("SELECT count() FROM gcs_mt").strip() == "2000"

    # Merge parts (writes new part blobs, deletes old ones) and re-read.
    node.query("OPTIMIZE TABLE gcs_mt FINAL")
    assert node.query("SELECT count() FROM gcs_mt").strip() == "2000"
    assert node.query("SELECT sum(a) FROM gcs_mt").strip() == str(sum(range(2000)))

    node.query("DROP TABLE gcs_mt SYNC")


def test_dynamic_gcs_disk_allows_indirect_backend_type_with_no_sign_request(
    started_cluster,
):
    """An indirect backend that resolves to GCS must not take the S3 credentials-only path."""
    node = started_cluster.instances["node"]
    node.query("DROP TABLE IF EXISTS gcs_indirect_type SYNC")
    node.query(
        "CREATE TABLE gcs_indirect_type (x UInt64) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = disk("
        "  name = 'gcs_indirect_type_disk',"
        "  type = object_storage,"
        "  object_storage_type = 'from_env NATIVE_GCS_DYNAMIC_DISK_TYPE',"
        "  metadata_type = local,"
        f"  endpoint = '{gcs_url('indirect-type/')}',"
        "  no_sign_request = true"
        ")",
        settings={"dynamic_disk_allow_from_env": 1, "use_native_gcs": 1},
    )
    node.query("DROP TABLE gcs_indirect_type SYNC")


def test_dynamic_gcs_disk_rejects_header_from_include_even_with_credential_opt_in(
    started_cluster,
):
    """An included request header has no durable opt-in marker in table metadata.

    Reject it during creation even when the session permits server credentials; otherwise the table can be
    created but becomes unloadable after restart under the default restricted profile.
    """
    node = started_cluster.instances["node"]
    node.query("DROP TABLE IF EXISTS gcs_include_header SYNC")
    error = node.query_and_get_error(
        "CREATE TABLE gcs_include_header (x UInt64) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = disk("
        "  name = 'gcs_include_header_disk',"
        "  type = object_storage,"
        "  object_storage_type = gcs,"
        "  metadata_type = local,"
        f"  endpoint = '{gcs_url('include-header/')}',"
        "  access_token = 'user-token',"
        "  include = 'gcs_included_header'"
        ")",
        settings={
            "dynamic_disk_allow_include": 1,
            "use_native_gcs": 1,
            "s3_allow_server_credentials_in_user_queries": 1,
        },
    )
    assert "ACCESS_DENIED" in error and "header" in error, error


def test_dynamic_gcs_disk_rejects_service_account_key_shadowed_by_include(
    started_cluster,
):
    """An `include` written before a literal credential shadows it.

    `ConfigProcessor` inserts the included children before the `<include>` node and duplicate siblings
    resolve to the first one, so the disk would authenticate with the *included* (server-managed)
    `service_account_key` while the SQL definition only ever vouched for its own literal value.
    """
    node = started_cluster.instances["node"]
    node.query("DROP TABLE IF EXISTS gcs_include_shadowed_key SYNC")
    error = node.query_and_get_error(
        "CREATE TABLE gcs_include_shadowed_key (x UInt64) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = disk("
        "  name = 'gcs_include_shadowed_key_disk',"
        "  type = object_storage,"
        "  object_storage_type = gcs,"
        "  metadata_type = local,"
        f"  endpoint = '{gcs_url('include-shadowed-key/')}',"
        "  include = 'gcs_included_service_account_key',"
        "  service_account_key = 'user-supplied-key'"
        ")",
        settings={"dynamic_disk_allow_include": 1, "use_native_gcs": 1},
    )
    assert "ACCESS_DENIED" in error and "service_account_key" in error, error


def test_dynamic_gcs_disk_allows_literal_header_with_unrelated_include(started_cluster):
    """A literal SQL header remains valid when `include` contributes unrelated disk settings."""
    node = started_cluster.instances["node"]
    node.query("DROP TABLE IF EXISTS gcs_literal_header SYNC")
    node.query(
        "CREATE TABLE gcs_literal_header (x UInt64) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = disk("
        "  name = 'gcs_literal_header_disk',"
        "  type = object_storage,"
        "  object_storage_type = gcs,"
        "  metadata_type = local,"
        f"  endpoint = '{gcs_url('literal-header/')}',"
        "  no_sign_request = true,"
        "  header = 'X-ClickHouse-Native-GCS-Literal: 1',"
        "  include = 'gcs_included_without_headers'"
        ")",
        settings={"dynamic_disk_allow_include": 1, "use_native_gcs": 1},
    )
    node.query("DROP TABLE gcs_literal_header SYNC")


def test_url_only_named_collection_reads_anonymously(started_cluster):
    """A named collection that only specifies a `url` must read anonymously.

    `StorageS3Configuration::fromNamedCollection` gives such a collection
    `use_environment_credentials = 0` by default, precisely so that it does not borrow the server's
    identity. The native projection used to drop that flag, so the same collection silently switched to
    Application Default Credentials -- or, under the default
    `s3_allow_server_credentials_in_user_queries = 0`, was refused outright. Here the read has to
    succeed, which it can only do unsigned: the emulator is reached with no credential at all.
    """
    node = started_cluster.instances["node"]
    url = gcs_url("url_only_collection/data.tsv")

    node.query(
        f"INSERT INTO FUNCTION gcs('{url}', NOSIGN, 'TSV', 'a UInt64') "
        "SELECT number FROM numbers(42) SETTINGS use_native_gcs = 1"
    )

    node.query("DROP NAMED COLLECTION IF EXISTS gcs_url_only")
    node.query(
        f"CREATE NAMED COLLECTION gcs_url_only AS url = '{url}', format = 'TSV', structure = 'a UInt64'"
    )
    try:
        assert (
            node.query(
                "SELECT count() FROM gcs(gcs_url_only)",
                settings={"use_native_gcs": 1},
            ).strip()
            == "42"
        )
    finally:
        node.query("DROP NAMED COLLECTION IF EXISTS gcs_url_only")


def test_dynamic_gcs_disk_renews_a_refresh_token_credential(started_cluster):
    """A `google_adc_*` disk must keep working past the first access token's expiry.

    The triple is handed to the transport, which exchanges it for an access token and renews that
    token as it nears expiry. Minting one token when the disk is created -- which is what the earlier
    implementation did -- makes the disk stop working once that token expires, so the test checks both
    that the SQL-supplied credential is the one presented to the token endpoint and that the exchange
    happens again rather than once for good.
    """
    node = started_cluster.instances["node"]
    disk_endpoint = f"http://{cluster.gcs_host}:{cluster.gcs_port}/{cluster.gcs_bucket}/adc_refresh/"

    before, _ = token_exchanges(SHORT_LIVED_TOKEN_PORT)

    node.query("DROP TABLE IF EXISTS gcs_adc_refresh SYNC")
    node.query(
        "CREATE TABLE gcs_adc_refresh (a UInt64) ENGINE = MergeTree ORDER BY a "
        "SETTINGS disk = disk("
        "  name = 'gcs_adc_refresh_disk',"
        "  type = object_storage,"
        "  object_storage_type = gcs,"
        "  metadata_type = local,"
        f"  endpoint = '{disk_endpoint}',"
        "  google_adc_client_id = 'ch-client-id',"
        "  google_adc_client_secret = 'ch-client-secret',"
        "  google_adc_refresh_token = 'ch-refresh-token',"
        f"  google_adc_token_uri = 'http://localhost:{SHORT_LIVED_TOKEN_PORT}/token'"
        ")",
        settings={"use_native_gcs": 1},
    )

    node.query("INSERT INTO gcs_adc_refresh SELECT number FROM numbers(100)")
    after_write, last_body = token_exchanges(SHORT_LIVED_TOKEN_PORT)
    assert after_write > before, "the disk did not exchange the refresh token at all"
    # The credential presented to the token endpoint is the one the SQL definition supplied, not an
    # ambient one resolved on the server.
    assert "grant_type=refresh_token" in last_body, last_body
    assert "client_id=ch-client-id" in last_body, last_body
    assert "refresh_token=ch-refresh-token" in last_body, last_body

    assert node.query("SELECT sum(a) FROM gcs_adc_refresh").strip() == str(
        sum(range(100))
    )
    after_read, _ = token_exchanges(SHORT_LIVED_TOKEN_PORT)
    assert (
        after_read > after_write
    ), "the access token was minted once and never renewed for a short-lived token"

    node.query("DROP TABLE gcs_adc_refresh SYNC")


def test_dynamic_gcs_disk_caches_a_long_lived_access_token(started_cluster):
    """The renewal above must not turn into a token exchange per request.

    google-cloud-cpp's caching decorator keeps the access token until it is close to expiring, so a
    disk whose token endpoint reports a long lifetime exchanges the refresh token exactly once no
    matter how many GCS requests the queries make.
    """
    node = started_cluster.instances["node"]
    disk_endpoint = (
        f"http://{cluster.gcs_host}:{cluster.gcs_port}/{cluster.gcs_bucket}/adc_cached/"
    )

    before, _ = token_exchanges(LONG_LIVED_TOKEN_PORT)

    node.query("DROP TABLE IF EXISTS gcs_adc_cached SYNC")
    node.query(
        "CREATE TABLE gcs_adc_cached (a UInt64) ENGINE = MergeTree ORDER BY a "
        "SETTINGS disk = disk("
        "  name = 'gcs_adc_cached_disk',"
        "  type = object_storage,"
        "  object_storage_type = gcs,"
        "  metadata_type = local,"
        f"  endpoint = '{disk_endpoint}',"
        "  google_adc_client_id = 'ch-client-id',"
        "  google_adc_client_secret = 'ch-client-secret',"
        "  google_adc_refresh_token = 'ch-refresh-token',"
        f"  google_adc_token_uri = 'http://localhost:{LONG_LIVED_TOKEN_PORT}/token'"
        ")",
        settings={"use_native_gcs": 1},
    )

    node.query("INSERT INTO gcs_adc_cached SELECT number FROM numbers(1000)")
    node.query("INSERT INTO gcs_adc_cached SELECT number FROM numbers(1000, 1000)")
    assert node.query("SELECT count() FROM gcs_adc_cached").strip() == "2000"
    assert node.query("SELECT sum(a) FROM gcs_adc_cached").strip() == str(
        sum(range(2000))
    )

    after, _ = token_exchanges(LONG_LIVED_TOKEN_PORT)
    assert after - before == 1, f"expected one token exchange, got {after - before}"

    node.query("DROP TABLE gcs_adc_cached SYNC")


def test_dynamic_disk_credential_grant_is_scoped_to_the_resolved_backend(
    started_cluster,
):
    """The persisted `_server_credentials_allowed` marker must follow the *resolved* backend.

    A disk created while the session had `s3_allow_server_credentials_in_user_queries = 1` records
    the grant in its stored definition, so it still loads after a restart under the default
    restricted profile. That decision cannot be made from the AST alone: an `include` is
    conservatively treated as potentially S3/GCS, which is right for refusing a create but would hand
    the grant to a disk whose backend is neither -- and retargeting that server-side include at `s3`
    or `gcs` later would inherit an authorization the new backend never earned.
    """
    node = started_cluster.instances["node"]
    opt_in = {
        "use_native_gcs": 1,
        "dynamic_disk_allow_include": 1,
        "s3_allow_server_credentials_in_user_queries": 1,
    }

    # A GCS backend that really falls back to Application Default Credentials earns the grant. The
    # fallback is a real one: no credential field is given, so the client resolves the stub
    # `authorized_user` file that `GOOGLE_APPLICATION_CREDENTIALS` names and exchanges its refresh
    # token at the stub endpoint -- which is exactly what makes the disk depend on the server's own
    # ambient identity, and therefore on the grant.
    before, _ = token_exchanges(ADC_TOKEN_PORT)
    node.query("DROP TABLE IF EXISTS gcs_marker_adc SYNC")
    node.query(
        "CREATE TABLE gcs_marker_adc (x UInt64) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = disk("
        "  name = 'gcs_marker_adc_disk',"
        "  type = object_storage,"
        "  object_storage_type = gcs,"
        "  metadata_type = local,"
        f"  endpoint = '{gcs_url('marker-adc/')}',"
        "  skip_access_check = 1"
        ")",
        settings=opt_in,
    )
    assert "_server_credentials_allowed" in node.query(
        "SHOW CREATE TABLE gcs_marker_adc"
    )
    after, _ = token_exchanges(ADC_TOKEN_PORT)
    assert after > before, "the disk did not resolve Application Default Credentials"
    node.query("DROP TABLE gcs_marker_adc SYNC")

    # An `include` that resolves to a local backend does not: it never relied on server credentials.
    node.query("DROP TABLE IF EXISTS gcs_marker_local_include SYNC")
    node.query(
        "CREATE TABLE gcs_marker_local_include (x UInt64) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = disk("
        "  name = 'gcs_marker_local_include_disk',"
        "  include = 'gcs_included_local_backend'"
        ")",
        settings=opt_in,
    )
    assert "_server_credentials_allowed" not in node.query(
        "SHOW CREATE TABLE gcs_marker_local_include"
    )
    node.query("DROP TABLE gcs_marker_local_include SYNC")


def test_iceberg_on_a_native_gcs_disk(started_cluster):
    """An Iceberg table can live on a native GCS disk.

    The `Iceberg` engine and the `iceberg` table function pick their backend from the `disk` setting,
    so wiring `ObjectStorageType::GCS` into that dispatch is what makes the native backend reachable.
    This exercises the whole write path -- metadata JSON, manifests, Parquet data files and the
    `version-hint.text` compare-and-swap that `readSmallObjectAndGetObjectMetadata` supplies the etag
    for -- and then reads the table back through both surfaces.

    The disk is registered by name through a throwaway MergeTree table rather than in the server
    configuration: a static `gcs` disk would stop the server from starting on a build without the
    SDK, which is exactly the case this module means to skip rather than break.
    """
    node = started_cluster.instances["node"]
    disk_endpoint = (
        f"http://{cluster.gcs_host}:{cluster.gcs_port}/{cluster.gcs_bucket}/iceberg/"
    )

    node.query("DROP TABLE IF EXISTS gcs_iceberg_disk_holder SYNC")
    node.query(
        "CREATE TABLE gcs_iceberg_disk_holder (x UInt64) ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS disk = disk("
        "  name = 'gcs_iceberg_disk',"
        "  type = object_storage,"
        "  object_storage_type = gcs,"
        "  metadata_type = local,"
        f"  endpoint = '{disk_endpoint}',"
        "  no_sign_request = true"
        ")",
        settings={"use_native_gcs": 1},
    )

    node.query("DROP TABLE IF EXISTS gcs_iceberg SYNC")
    node.query(
        "CREATE TABLE gcs_iceberg (a UInt64, b String) "
        "ENGINE = Iceberg(path = 'table1/', format = Parquet) "
        "SETTINGS disk = 'gcs_iceberg_disk', iceberg_use_version_hint = 1"
    )

    node.query(
        "INSERT INTO gcs_iceberg SELECT number, toString(number) FROM numbers(100)"
    )
    assert node.query("SELECT count(), sum(a) FROM gcs_iceberg").strip() == "100\t4950"

    # A second commit advances the snapshot and rewrites `version-hint.text` against the etag of the
    # version it read, which is the conditional write the metadata override exists for.
    node.query(
        "INSERT INTO gcs_iceberg SELECT number, toString(number) FROM numbers(100, 100)"
    )
    assert node.query("SELECT count() FROM gcs_iceberg").strip() == "200"

    # The same table through the table function, which dispatches on the disk type separately.
    assert (
        node.query(
            "SELECT count() FROM iceberg(path = 'table1/', SETTINGS disk = 'gcs_iceberg_disk')"
        ).strip()
        == "200"
    )

    node.query("DROP TABLE gcs_iceberg SYNC")
    node.query("DROP TABLE gcs_iceberg_disk_holder SYNC")


def test_schema_inference_cache(started_cluster):
    """The native `gcs` schema cache must be visible in `system.schema_inference_cache` and
    clearable with `SYSTEM DROP SCHEMA CACHE FOR GCS` (otherwise a stale inferred schema
    could only be purged by restarting the server)."""
    node = started_cluster.instances["node"]
    url = gcs_url("schema_cache/data.tsv")

    node.query("SYSTEM DROP SCHEMA CACHE FOR GCS")
    node.query(
        f"INSERT INTO FUNCTION gcs('{url}', NOSIGN, 'TSV', 'a UInt64, b String') "
        f"SELECT number, toString(number) FROM numbers(10) "
        f"SETTINGS use_native_gcs = 1"
    )

    # Reading without an explicit structure infers the schema and caches it.
    node.query(
        f"SELECT count() FROM gcs('{url}', NOSIGN, 'TSV') SETTINGS use_native_gcs = 1"
    )

    assert (
        node.query(
            "SELECT count() FROM system.schema_inference_cache "
            "WHERE storage = 'GCS' AND source LIKE '%schema_cache/data.tsv'"
        ).strip()
        == "1"
    )

    node.query("SYSTEM DROP SCHEMA CACHE FOR GCS")
    assert (
        node.query(
            "SELECT count() FROM system.schema_inference_cache WHERE storage = 'GCS'"
        ).strip()
        == "0"
    )


def test_forbidden_header_rejected(started_cluster):
    """Headers destined for the native client must pass the server-wide `<http_forbid_headers>`
    filter (`configs/forbid_headers.xml` forbids `X-ClickHouse-Native-GCS-Forbidden`), whatever
    surface supplied them: `headers(...)` in the query or a disk `<header>` entry. A header not
    on the forbidden list keeps working."""
    node = started_cluster.instances["node"]
    url = gcs_url("forbid_headers/data.tsv")

    # The query surface: headers(...) of the table function.
    err = node.query_and_get_error(
        f"SELECT * FROM gcs('{url}', NOSIGN, 'TSV', 'a UInt64', "
        f"headers('X-ClickHouse-Native-GCS-Forbidden' = '1')) "
        f"SETTINGS use_native_gcs = 1"
    )
    assert "forbidden in configuration file" in err

    # The disk surface: a <header> entry of the disk configuration, checked when the client
    # is built during disk (and therefore table) creation.
    node.query("DROP TABLE IF EXISTS gcs_forbidden_header SYNC")
    err = node.query_and_get_error(
        "CREATE TABLE gcs_forbidden_header (a UInt64) ENGINE = MergeTree ORDER BY a "
        "SETTINGS disk = disk("
        "  name = 'gcs_disk_forbidden_header',"
        "  type = object_storage,"
        "  object_storage_type = gcs,"
        "  metadata_type = local,"
        f"  endpoint = '{gcs_url('forbid_headers_disk/')}',"
        "  no_sign_request = true,"
        "  header = 'X-ClickHouse-Native-GCS-Forbidden: 1'"
        ")",
        settings={"use_native_gcs": 1},
    )
    assert "forbidden in configuration file" in err

    # A header that is not forbidden passes the filter and the request succeeds.
    node.query(
        f"INSERT INTO FUNCTION gcs('{url}', NOSIGN, 'TSV', 'a UInt64', "
        f"headers('X-ClickHouse-Native-GCS-Allowed' = '1')) "
        f"SELECT number FROM numbers(5) "
        f"SETTINGS use_native_gcs = 1"
    )
    res = node.query(
        f"SELECT count() FROM gcs('{url}', NOSIGN, 'TSV', 'a UInt64', "
        f"headers('X-ClickHouse-Native-GCS-Allowed' = '1')) "
        f"SETTINGS use_native_gcs = 1"
    )
    assert res.strip() == "5"


def test_table_function_filesystem_cache(started_cluster):
    """A native GCS read can be served from the filesystem cache. Its cache key is
    `hash(path, etag)`, and the etag of this backend is the object generation, which changes on
    every overwrite - so it is a strong content identifier and safe to key a cache with.
    """
    node = started_cluster.instances["node"]
    url = gcs_url("fs_cache/data.tsv")

    node.query(
        f"INSERT INTO FUNCTION gcs('{url}', NOSIGN, 'TSV', 'a UInt64') "
        "SELECT number FROM numbers(100) SETTINGS use_native_gcs = 1"
    )

    read_settings = "SETTINGS use_native_gcs = 1, filesystem_cache_name = 'cache1', enable_filesystem_cache = 1"
    select = (
        f"SELECT count() FROM gcs('{url}', NOSIGN, 'TSV', 'a UInt64') {read_settings}"
    )

    # Cold read: the data is fetched from GCS and written into the cache.
    cold_query_id = f"gcs_fs_cache_cold_{uuid.uuid4()}"
    assert node.query(select, query_id=cold_query_id).strip() == "100"
    node.query("SYSTEM FLUSH LOGS")

    written = int(
        node.query(
            "SELECT ProfileEvents['CachedReadBufferCacheWriteBytes'] FROM system.query_log "
            f"WHERE query_id = '{cold_query_id}' AND type = 'QueryFinish'"
        )
    )
    assert written > 0

    # Warm read: served from the cache, with nothing new written to it.
    node.query("SYSTEM CLEAR SCHEMA CACHE")
    warm_query_id = f"gcs_fs_cache_warm_{uuid.uuid4()}"
    assert node.query(select, query_id=warm_query_id).strip() == "100"
    node.query("SYSTEM FLUSH LOGS")

    assert 0 < int(
        node.query(
            "SELECT ProfileEvents['CachedReadBufferReadFromCacheBytes'] FROM system.query_log "
            f"WHERE query_id = '{warm_query_id}' AND type = 'QueryFinish'"
        )
    )
    assert 0 == int(
        node.query(
            "SELECT ProfileEvents['CachedReadBufferCacheWriteBytes'] FROM system.query_log "
            f"WHERE query_id = '{warm_query_id}' AND type = 'QueryFinish'"
        )
    )


def test_profile_events(started_cluster):
    """The native backend must account its requests, the way `S3GetObject` and friends do for the
    S3-compatibility path. Without these the backend is unobservable: there is no way to see request
    counts, bytes or latency for a GCS disk in production."""
    node = started_cluster.instances["node"]
    disk_endpoint = f"http://{cluster.gcs_host}:{cluster.gcs_port}/{cluster.gcs_bucket}/profile_events/"

    node.query("DROP TABLE IF EXISTS gcs_events SYNC")
    node.query(
        "CREATE TABLE gcs_events (a UInt64, b String) ENGINE = MergeTree ORDER BY a "
        "SETTINGS disk = disk("
        "  name = 'gcs_disk_events',"
        "  type = object_storage,"
        "  object_storage_type = gcs,"
        "  metadata_type = local,"
        f"  endpoint = '{disk_endpoint}',"
        "  no_sign_request = true"
        ")",
        settings={"use_native_gcs": 1},
    )

    write_id = f"gcs_ev_write_{uuid.uuid4()}"
    node.query(
        "INSERT INTO gcs_events SELECT number, toString(number) FROM numbers(10000)",
        query_id=write_id,
    )

    read_id = f"gcs_ev_read_{uuid.uuid4()}"
    # `sum(a)` rather than `count()`: with `optimize_trivial_count_query` a count is answered from
    # the part metadata and never reaches object storage.
    assert sum(range(10000)) == int(
        node.query("SELECT sum(a) FROM gcs_events", query_id=read_id)
    )

    node.query("SYSTEM FLUSH LOGS")

    def event(query_id, name):
        return int(
            node.query(
                f"SELECT ProfileEvents['{name}'] FROM system.query_log "
                f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
            )
        )

    # A disk write goes through the resumable upload and is counted for the disk as well.
    assert event(write_id, "GCSWriteObject") > 0
    assert event(write_id, "DiskGCSWriteObject") > 0
    assert event(write_id, "WriteBufferFromGCSBytes") > 0

    # A disk read issues ReadObject and reports the bytes it got back.
    assert event(read_id, "GCSGetObject") > 0
    assert event(read_id, "DiskGCSGetObject") > 0
    assert event(read_id, "ReadBufferFromGCSBytes") > 0

    node.query("DROP TABLE gcs_events SYNC")


def test_profile_events_list_objects_is_counted_by_the_transport(started_cluster):
    """`GCSListObjects` counts `objects.list` REST calls, and the increment lives in the transport
    rather than at the call site: the library fetches the later pages of a `ListObjectsReader` on its
    own as the iteration advances, so a call-site counter would see one call for a whole paged
    listing and be useless for rate-limit and cost analysis (the S3 and Azure backends count per
    request too).

    Only the "the request is counted at all" half of that can be asserted here: `fake-gcs-server`
    1.52.2 honours `maxResults` but never returns a `nextPageToken`, so a listing against the
    emulator is always a single page (and lowering `s3_list_object_keys_size` silently truncates it
    rather than paging), leaving no way to observe a multi-page listing.
    """
    node = started_cluster.instances["node"]

    objects = 6
    for i in range(objects):
        node.query(
            f"INSERT INTO FUNCTION gcs('{gcs_url(f'list_pages/part{i}.tsv')}', NOSIGN, 'TSV', 'a UInt64') "
            "SELECT number FROM numbers(10) SETTINGS use_native_gcs = 1"
        )

    query_id = f"gcs_ev_list_{uuid.uuid4()}"
    assert objects * 10 == int(
        node.query(
            f"SELECT count() FROM gcs('{gcs_url('list_pages/*.tsv')}', NOSIGN, 'TSV', 'a UInt64')",
            settings={"use_native_gcs": 1},
            query_id=query_id,
        )
    )

    node.query("SYSTEM FLUSH LOGS")

    list_calls = int(
        node.query(
            "SELECT ProfileEvents['GCSListObjects'] FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
        )
    )
    assert list_calls >= 1


def test_profile_events_table_function_is_not_counted_as_disk(started_cluster):
    """`DiskGCS*` must count only server-configured disks, so that disk traffic can be told apart
    from `gcs()` traffic -- the same split the S3 backend makes with `DiskS3*`."""
    node = started_cluster.instances["node"]
    url = gcs_url("profile_events_tf/data.tsv")

    node.query(
        f"INSERT INTO FUNCTION gcs('{url}', NOSIGN, 'TSV', 'a UInt64') "
        "SELECT number FROM numbers(1000) SETTINGS use_native_gcs = 1"
    )

    query_id = f"gcs_ev_tf_{uuid.uuid4()}"
    assert 1000 == int(
        node.query(
            f"SELECT count() FROM gcs('{url}', NOSIGN, 'TSV', 'a UInt64')",
            settings={"use_native_gcs": 1},
            query_id=query_id,
        )
    )

    node.query("SYSTEM FLUSH LOGS")

    def event(name):
        return int(
            node.query(
                f"SELECT ProfileEvents['{name}'] FROM system.query_log "
                f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
            )
        )

    assert event("GCSGetObject") > 0
    assert event("DiskGCSGetObject") == 0


def test_parallel_download_of_one_object(started_cluster):
    """A whole-object read must be split across `max_download_threads` ranged requests.

    `ParallelReadBuffer` only engages when the underlying buffer reports `supportsReadAt`, so without
    it a single large file is fetched by one stream no matter what `max_download_threads` says -- the
    S3-compatibility path issues `max_download_threads` requests for the same file and finishes
    sooner. `FormatFactory::wrapReadBufferIfNeeded` additionally requires the file to be at least
    twice `max_download_buffer_size`, which is why that setting is lowered here instead of writing a
    20 MiB fixture.
    """
    node = started_cluster.instances["node"]
    url = gcs_url("parallel/data.tsv")
    num_rows = 50000

    node.query(
        f"INSERT INTO FUNCTION gcs('{url}', NOSIGN, 'TSV', 'a UInt64') "
        f"SELECT number FROM numbers({num_rows}) SETTINGS use_native_gcs = 1"
    )

    def read(query_id, threads):
        # `sum(a)` rather than `count()`: a count over a file is answered from the row-count cache
        # (`use_cache_for_count_from_files`, on by default) as soon as one query has read the object,
        # so the second of the two reads below would not touch object storage at all.
        return int(
            node.query(
                f"SELECT sum(a) FROM gcs('{url}', NOSIGN, 'TSV', 'a UInt64')",
                query_id=query_id,
                settings={
                    "use_native_gcs": 1,
                    "max_download_threads": threads,
                    "max_download_buffer_size": 16384,
                    "input_format_parallel_parsing": 0,
                },
            )
        )

    def gets(query_id):
        return int(
            node.query(
                "SELECT ProfileEvents['GCSGetObject'] FROM system.query_log "
                f"WHERE query_id = '{query_id}' AND type = 'QueryFinish'"
            )
        )

    serial_id = f"gcs_serial_{uuid.uuid4()}"
    parallel_id = f"gcs_parallel_{uuid.uuid4()}"

    expected_sum = num_rows * (num_rows - 1) // 2
    assert expected_sum == read(serial_id, 1)
    assert expected_sum == read(parallel_id, 4)

    node.query("SYSTEM FLUSH LOGS")

    # One stream when parallelism is off, several ranged requests when it is on.
    assert gets(serial_id) == 1
    assert gets(parallel_id) > 1
