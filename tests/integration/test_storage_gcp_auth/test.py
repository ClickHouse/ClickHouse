import json
import logging
import os
import re
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/named_collections.xml",
                "configs/filesystem_caches.xml",
                "configs/page_cache.xml",
            ],
            user_configs=["configs/users.xml"],
            with_minio=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        run_gcs_mocks(cluster)

        yield cluster
    finally:
        cluster.shutdown()


TOKEN_PATH = "computeMetadata/v1/instance/service-accounts"
SERVICE_ACCOUNT = "my-account"


def run_gcs_mocks(cluster):
    logging.info("Starting gcs mocks")
    mocks = (
        ("echo.py", "resolver", ["22234"]),
        ("auth.py", "resolver", ["80", TOKEN_PATH, SERVICE_ACCOUNT]),
    )

    for mock_filename, container, args in mocks:
        container_id = cluster.get_container_id(container)
        current_dir = os.path.dirname(__file__)
        cluster.copy_file_to_container(
            container_id,
            os.path.join(current_dir, "gcs_mocks", mock_filename),
            mock_filename,
        )

        cluster.exec_in_container(
            container_id, ["python", mock_filename] + args, detach=True
        )

    # Wait for S3 mocks to start
    for mock_filename, container, args in mocks:
        port = args[0]
        num_attempts = 100
        for attempt in range(num_attempts):
            ping_response = cluster.exec_in_container(
                cluster.get_container_id(container),
                ["curl", "-s", f"http://localhost:{port}/ping"],
                nothrow=True,
            )
            if ping_response != "OK":
                if attempt == num_attempts - 1:
                    assert ping_response == "OK", 'Expected "OK", but got "{}"'.format(
                        ping_response
                    )
                else:
                    time.sleep(1)
            else:
                logging.debug(
                    f"mock {mock_filename} ({port}) answered {ping_response} on attempt {attempt}"
                )
                break

    logging.info("S3 mocks started")


def test_gcp_auth(started_cluster):
    """`gcs_conn`'s URL (`http://resolver:22234/test/`) has no `storage.googleapis.com` in it, so
    `Client` never deduces `ProviderType::GCS` for this connection and `api_mode` stays `AWS` for the
    whole test -- this characterizes `gcp_oauth` against a proxied/private GCS endpoint (a real,
    spec-supported shape), NOT the common case of a user pointing `gcp_oauth` directly at
    `storage.googleapis.com`, where `api_mode` can become `GCS` and the `ApiMode::GCS`-gated header
    mappings in `Requests.cpp` (see `CopyObjectRequestGetRequestSpecificHeadersRenamesOnlyUnderGcsApiMode`
    in `gtest_aws_s3_client.cpp`) would actually fire.
    """
    node = started_cluster.instances["node"]

    # Reset mock counters so the test is repeatable
    resolver_id = started_cluster.get_container_id("resolver")
    for port in [80, 22234]:
        started_cluster.exec_in_container(
            resolver_id,
            ["curl", "-s", f"http://localhost:{port}/reset"],
            nothrow=True,
        )

    def get_num_requests():
        count_response = started_cluster.exec_in_container(
            resolver_id,
            ["curl", "-s", "http://localhost/counter"],
            nothrow=True,
        )

        return int(count_response)

    node.query("DROP TABLE IF EXISTS s3_table")
    node.query(
        "CREATE TABLE s3_table (line String) ENGINE = S3(gcs_conn, filename='test.txt', format='LineAsString')"
    )

    assert get_num_requests() == 0
    assert node.query("SELECT * FROM s3_table") == "OK\n"

    # Wait to refresh token
    time.sleep(4)

    assert get_num_requests() == 2
    assert node.query("SELECT * FROM s3_table") == "OK\n"

    assert get_num_requests() == 4
    assert (
        node.query(
            "SELECT * FROM s3(gcs_conn, filename='test.txt', format='LineAsString')"
        )
        == "OK\n"
    )

    with pytest.raises(QueryRuntimeException) as ei:
        node.query(
            "SELECT * FROM s3(gcs_conn_bad, filename='test.txt', format='LineAsString')"
        )

    assert "AUTHENTICATION_FAILED" in ei.value.stderr


def test_gcp_auth_ordinary_contract(started_cluster):
    """Pins Default-mode `gcp_oauth` behaviour against the same claims Task 4/5 make in the unit
    tests, but for the requests ClickHouse actually issues end-to-end: Bearer authentication still
    works, the response ETag is the mock's ordinary one (never the independent x-goog-generation
    also present on every response), and no request ever carries `x-goog-if-generation-match` --
    this is the entire point of `Default` mode staying free of CAS's GCS generation dialect.

    `PUT` with `x-amz-meta-*` is unreachable from ordinary SQL on purpose, not by oversight: nothing
    in the plain `S3(gcs_conn, ...)` write path fills the object-attributes parameter that would put
    `x-amz-meta-*` on the wire -- that plumbing only exists for the CAS envelope. `CopyObject` is
    likewise unreachable here: it only happens for a same-object-storage `MergeTree` part move on a
    `Disk`, which this named collection does not configure. Both are covered instead, and more
    precisely, by direct SDK request construction in `gtest_aws_s3_client.cpp` / `gtest_goog4_signer.cpp`.

    This test also inherits `test_gcp_auth`'s fidelity gap: `gcs_conn`'s endpoint has no
    `storage.googleapis.com` substring, so it runs with `api_mode` staying `AWS`, never `GCS` -- the
    `ApiMode::GCS`-gated header mappings in `Requests.cpp` do not fire on this path either. See the
    `test_gcp_auth` docstring and `CopyObjectRequestGetRequestSpecificHeadersRenamesOnlyUnderGcsApiMode`
    in `gtest_aws_s3_client.cpp` for where that mechanism actually gets exercised.
    """
    node = started_cluster.instances["node"]
    resolver_id = started_cluster.get_container_id("resolver")

    def reset():
        for port in [80, 22234]:
            started_cluster.exec_in_container(
                resolver_id, ["curl", "-s", f"http://localhost:{port}/reset"], nothrow=True
            )
        started_cluster.exec_in_container(
            resolver_id,
            ["curl", "-s", "http://localhost:22234/reset_captured"],
            nothrow=True,
        )

    def get_num_requests():
        count_response = started_cluster.exec_in_container(
            resolver_id, ["curl", "-s", "http://localhost/counter"], nothrow=True
        )
        return int(count_response)

    def get_captured():
        raw = started_cluster.exec_in_container(
            resolver_id, ["curl", "-s", "http://localhost:22234/captured"], nothrow=True
        )
        return json.loads(raw)

    def assert_no_generation_precondition(requests):
        for request in requests:
            assert "x-goog-if-generation-match" not in request["headers"], request

    reset()

    node.query("DROP TABLE IF EXISTS s3_ordinary_write")
    node.query(
        "CREATE TABLE s3_ordinary_write (line String) ENGINE = S3(gcs_conn, filename='ordinary.txt', format='LineAsString')"
    )

    # PUT: bearer authentication drives a real write; the token-refresh count moving at all proves
    # the request went through the same OAuth path as the pre-existing test_gcp_auth PUT/GET traffic.
    before_write = get_num_requests()
    node.query("INSERT INTO s3_ordinary_write VALUES ('hello')")
    assert get_num_requests() > before_write

    put_requests = [r for r in get_captured() if r["method"] == "PUT"]
    assert put_requests, "expected the INSERT to issue a PUT"
    assert_no_generation_precondition(put_requests)

    # GET/HEAD: the response carries both a stable ETag and an independent x-goog-generation (set by
    # the PUT above); a Default read must come back as the ordinary content, not fail or reinterpret
    # the generation as the object's identity.
    reset()
    assert node.query("SELECT * FROM s3_ordinary_write") == "hello\n"
    read_requests = get_captured()
    assert any(r["method"] == "GET" for r in read_requests)
    assert_no_generation_precondition(read_requests)

    # LIST: a glob forces a real ListObjectsV2 call (`list-type=2`), independent of the single-key
    # GET/HEAD path above.
    reset()
    assert (
        node.query(
            "SELECT * FROM s3(gcs_conn, filename='ordinary*.txt', format='LineAsString')"
        )
        == "hello\n"
    )
    list_requests = [
        r for r in get_captured() if r["method"] == "GET" and "list-type=2" in r["path"]
    ]
    assert list_requests, "expected the glob read to issue a ListObjectsV2 request"
    assert_no_generation_precondition(list_requests)

    # DELETE: TRUNCATE on the S3 engine removes the underlying object.
    reset()
    node.query("TRUNCATE TABLE s3_ordinary_write")
    delete_requests = [r for r in get_captured() if r["method"] == "DELETE"]
    assert delete_requests, "expected TRUNCATE to issue a DELETE"
    assert_no_generation_precondition(delete_requests)

    # Multipart-sized write: `gcs_conn_multipart` lowers the part-size thresholds so even a small
    # INSERT forces CreateMultipartUpload / UploadPart / CompleteMultipartUpload.
    reset()
    node.query("DROP TABLE IF EXISTS s3_multipart_write")
    node.query(
        "CREATE TABLE s3_multipart_write (line String) ENGINE = S3(gcs_conn_multipart, filename='multipart.txt', format='LineAsString')"
    )
    payload = "x" * (2 * 1024 * 1024)
    node.query(f"INSERT INTO s3_multipart_write VALUES ('{payload}')")

    multipart_requests = get_captured()
    assert any(
        r["method"] == "POST" and "uploads" in r["path"] for r in multipart_requests
    ), "expected CreateMultipartUpload"
    assert any(
        r["method"] == "PUT" and "partNumber" in r["path"] for r in multipart_requests
    ), "expected UploadPart"
    assert any(
        r["method"] == "POST" and "uploadId=" in r["path"] and "uploads" not in r["path"]
        for r in multipart_requests
    ), "expected CompleteMultipartUpload"
    assert_no_generation_precondition(multipart_requests)

    node.query("DROP TABLE s3_ordinary_write")
    node.query("DROP TABLE s3_multipart_write")


def test_gcp_auth_etag_and_cache_isolation(started_cluster):
    """Regression fence for the user-visible half of the isolation plan: a `Default`-mode `gcp_oauth`
    response must expose the mock's stable ETag as `_etag`, never `x-goog-generation` (which the mock
    also sets, as an unrelated large counter, on every HEAD/GET/PUT/CompleteMultipartUpload response),
    regardless of whether the metadata reached ClickHouse through a LIST (the XML body's `<ETag>`) or a
    HEAD/GET (the `ETag` header). Because `_etag` feeds the filesystem-cache key and the page-cache key
    (`StorageObjectStorageSource.cpp`), a blanket generation substitution on only some response kinds
    would split those caches by read path for the identical object. This is exercised end to end
    rather than at the unit level, because the failure mode is in which responses the substitution
    reaches, not in the cache-key hashing itself (deterministic regardless of its input, so it cannot
    by itself catch a wrong-but-consistent input).

    A third `_etag` consumer named by the plan, the Parquet metadata cache
    (`ParquetMetadataCache::createKey`), is deliberately NOT covered here. Proving it end to end
    requires a genuinely cold metadata-cache read with both body caches off, which forces a real
    ranged HTTP GET for the row-group `OffsetIndex` -- `gcs_mocks/echo.py` ignores the `Range` header
    entirely and always returns the full object, so that read gets the wrong bytes and Parquet's
    thrift parser rejects them (`TProtocolException: Invalid data`) regardless of ETag isolation.
    Every other read in this file tolerates that gap because it goes through a body cache that, once
    warm, serves sub-ranges from its own local copy rather than issuing a new ranged request to the
    mock. Extending the mock to serve real `Range` responses is separate work; until then, the
    Parquet-metadata-cache consumer is covered only by the same-shaped unit-level proof for the other
    two consumers (Tasks 4-5) plus the shared reasoning that all three key off the identical
    `object_info.metadata->etag` value validated by the assertions above.
    """
    node = started_cluster.instances["node"]
    resolver_id = started_cluster.get_container_id("resolver")

    def reset():
        for port in [80, 22234]:
            started_cluster.exec_in_container(
                resolver_id, ["curl", "-s", f"http://localhost:{port}/reset"], nothrow=True
            )

    reset()

    object_name = "cache_isolation.parquet"
    node.query(
        f"INSERT INTO FUNCTION s3(gcs_conn, filename='{object_name}', format='Parquet') "
        f"SELECT number FROM numbers(2000) SETTINGS s3_truncate_on_insert=1"
    )

    # HEAD/GET path: a direct key, no glob.
    etag_head = node.query(
        f"SELECT _etag FROM s3(gcs_conn, filename='{object_name}', format='Parquet') LIMIT 1"
    ).strip()

    # LIST path: a glob forces ListObjectsV2, whose XML body carries its own <ETag>, independent of
    # the HEAD/GET header path above.
    etag_list = node.query(
        f"SELECT _etag FROM s3(gcs_conn, filename='cache_isolation*.parquet', format='Parquet') LIMIT 1"
    ).strip()

    assert etag_head == etag_list, (etag_head, etag_list)
    # The mock's generation counter is a large, purely-numeric string (see `_next_generation` /
    # `bump_generation` in `gcs_mocks/echo.py`); its ETag never is (`stable_etag` prefixes with
    # "etag-"). A blanket generation-for-ETag substitution -- the bug this plan fixes -- would make
    # `_etag` numeric here; this assertion is fireable because the two formats cannot collide.
    assert not re.fullmatch(r"\d+", etag_head), etag_head

    # --- Filesystem cache: the first read is LIST-sourced (glob), the second is HEAD/GET-sourced (a
    # direct key). The cache key is `SipHash(path, etag)`
    # (`StorageObjectStorageSource.cpp`), so the second read can only be served from cache -- with no
    # further `GetObject` call -- if the two read paths agree on `etag` for the identical object. A
    # generation leaking into only one of the two response kinds would make this a cache miss.
    fs_settings = "filesystem_cache_name='gcp_oauth_cache1', enable_filesystem_cache=1, use_page_cache_for_object_storage=0"
    fs_query_id = f"fs-{object_name}-1"
    node.query(
        f"SELECT sum(ignore(*)) FROM s3(gcs_conn, filename='cache_isolation*.parquet', format='Parquet') SETTINGS {fs_settings}",
        query_id=fs_query_id,
    )
    node.query("SYSTEM FLUSH LOGS")
    write_bytes = int(
        node.query(
            f"SELECT ProfileEvents['CachedReadBufferCacheWriteBytes'] FROM system.query_log "
            f"WHERE query_id='{fs_query_id}' AND type='QueryFinish'"
        )
    )
    assert write_bytes > 0

    node.query("SYSTEM CLEAR SCHEMA CACHE")
    fs_query_id_2 = f"fs-{object_name}-2"
    node.query(
        f"SELECT sum(ignore(*)) FROM s3(gcs_conn, filename='{object_name}', format='Parquet') SETTINGS {fs_settings}",
        query_id=fs_query_id_2,
    )
    node.query("SYSTEM FLUSH LOGS")
    read_bytes, misses, gets = node.query(
        f"SELECT ProfileEvents['CachedReadBufferReadFromCacheBytes'], "
        f"ProfileEvents['CachedReadBufferReadFromCacheMisses'], ProfileEvents['S3GetObject'] "
        f"FROM system.query_log WHERE query_id='{fs_query_id_2}' AND type='QueryFinish'"
    ).split("\t")
    # Not `read_bytes == write_bytes`: `CachedReadBufferCacheWriteBytes` counts one physical
    # population of the cache, while `CachedReadBufferReadFromCacheBytes` sums every buffer instance
    # that reads through the cache in that query (schema resolution, prefetch, and the execution read
    # each open their own `CachedOnDiskReadBufferFromFile` and each re-reads the small cached object in
    # full) -- for this object that was observed to be exactly 3x on a clean second read, so the two
    # counters are not comparable quantities even when nothing is wrong. What isolation actually
    # requires is that every one of those reads is a hit: zero cache misses, and no `GetObject` at all.
    assert int(read_bytes) > 0
    assert int(misses) == 0
    assert int(gets) == 0

    # --- Page cache: same cross-path shape as the filesystem cache above (LIST-sourced warm read,
    # then a HEAD/GET-sourced read that must hit), over the independent page-cache key
    # `"etag:" + etag` (`StorageObjectStorageSource.cpp`). Mutually exclusive with the filesystem
    # cache in the read pipeline (`use_page_cache` in `StorageObjectStorageSource::createReadBuffer`
    # requires `!use_filesystem_cache`), so this is its own query with the filesystem cache off. ---
    node.query("SYSTEM CLEAR SCHEMA CACHE")
    pc_settings = "enable_filesystem_cache=0, use_page_cache_for_object_storage=1"
    pc_query_id = f"pc-{object_name}-1"
    node.query(
        f"SELECT sum(ignore(*)) FROM s3(gcs_conn, filename='cache_isolation*.parquet', format='Parquet') SETTINGS {pc_settings}",
        query_id=pc_query_id,
    )
    node.query("SYSTEM FLUSH LOGS")
    misses = int(
        node.query(
            f"SELECT ProfileEvents['PageCacheMisses'] FROM system.query_log "
            f"WHERE query_id='{pc_query_id}' AND type='QueryFinish'"
        )
    )
    assert misses > 0

    node.query("SYSTEM CLEAR SCHEMA CACHE")
    pc_query_id_2 = f"pc-{object_name}-2"
    node.query(
        f"SELECT sum(ignore(*)) FROM s3(gcs_conn, filename='{object_name}', format='Parquet') "
        f"SETTINGS {pc_settings}, read_from_page_cache_if_exists_otherwise_bypass_cache=1",
        query_id=pc_query_id_2,
    )
    node.query("SYSTEM FLUSH LOGS")
    hits, misses_2, gets = node.query(
        f"SELECT ProfileEvents['PageCacheHits'], ProfileEvents['PageCacheMisses'], "
        f"ProfileEvents['S3GetObject'] FROM system.query_log "
        f"WHERE query_id='{pc_query_id_2}' AND type='QueryFinish'"
    ).split("\t")
    assert int(hits) > 0
    assert int(misses_2) == 0
    assert int(gets) == 0

    # Parquet metadata cache is not exercised here -- see the function docstring: proving it cold
    # requires a real ranged GET that this mock cannot serve correctly.
