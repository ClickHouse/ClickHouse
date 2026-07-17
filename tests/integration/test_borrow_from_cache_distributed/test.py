import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/storage.xml"],
    with_minio=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_distributed_on_borrow_from_cache_disk_is_rejected(started_cluster):
    # `borrow_from_cache` uses the in-memory metadata storage, whose `getPath()` is just a
    # placeholder root ("/") -- there is no real directory behind it. `Distributed` manages its
    # local async-insert queue directories with raw `std::filesystem` calls on
    # `disk->getPath() + relative_data_path`, bypassing the `IDisk` API entirely. Without a guard,
    # creating a `Distributed` table on a storage policy backed by such a disk would attempt
    # `fs::create_directories` under the container's real filesystem root instead of failing
    # closed, so the table must be rejected outright.
    error = node.query_and_get_error(
        """
        CREATE TABLE dist (key UInt64) ENGINE = Distributed(test_cluster, currentDatabase(), 'underlying', rand(), 'borrowed_policy')
        """
    )
    assert "NOT_IMPLEMENTED" in error
    assert "does not have a real filesystem path" in error

    # No table should have been left behind, and no directories were created under `/`.
    assert node.query("EXISTS TABLE dist").strip() == "0"


def test_distributed_on_read_only_wrapped_borrow_from_cache_disk_is_rejected(started_cluster):
    # The same disk wrapped in a `ReadOnlyDiskWrapper` (via `read_only = 1`) must also be rejected.
    # The wrapper forwards `getPath()` to the delegate (still the placeholder root "/"), so it must
    # forward `isPathOnLocalFilesystem()` as well; otherwise it would inherit the default `true` and
    # let the `Distributed` table slip past the guard and touch the container's real filesystem root.
    error = node.query_and_get_error(
        """
        CREATE TABLE dist_ro (key UInt64) ENGINE = Distributed(test_cluster, currentDatabase(), 'underlying', rand(), 'borrowed_policy_ro')
        """
    )
    assert "NOT_IMPLEMENTED" in error
    assert "does not have a real filesystem path" in error

    assert node.query("EXISTS TABLE dist_ro").strip() == "0"


def test_distributed_on_web_disk_is_rejected(started_cluster):
    # The same guard must cover the sibling `web` metadata backend: its `getPath()` is an empty
    # placeholder root with no real directory behind it, so `MetadataStorageFromStaticFilesWebServer`
    # reports `isPathOnLocalFilesystem() == false` too. Without that, a `Distributed` table on a web
    # object-storage disk would `fs::create_directories` under a relative path from the working
    # directory instead of failing closed. The web endpoint is never contacted -- the guard throws
    # before any web access.
    error = node.query_and_get_error(
        """
        CREATE TABLE dist_web (key UInt64) ENGINE = Distributed(test_cluster, currentDatabase(), 'underlying', rand(), 'web_policy')
        """
    )
    assert "NOT_IMPLEMENTED" in error
    assert "does not have a real filesystem path" in error

    assert node.query("EXISTS TABLE dist_web").strip() == "0"


def test_distributed_on_cached_web_disk_is_rejected(started_cluster):
    # A cache layer over the web disk forwards `getPath()` to the underlying metadata storage, so
    # it must forward `isPathOnLocalFilesystem()` as well; otherwise the cache wrapper would
    # inherit the default `true` and let the `Distributed` table slip past the guard even though
    # the path is still the web backend's empty placeholder.
    error = node.query_and_get_error(
        """
        CREATE TABLE dist_cached_web (key UInt64) ENGINE = Distributed(test_cluster, currentDatabase(), 'underlying', rand(), 'cached_web_policy')
        """
    )
    assert "NOT_IMPLEMENTED" in error
    assert "does not have a real filesystem path" in error

    assert node.query("EXISTS TABLE dist_cached_web").strip() == "0"


def test_distributed_on_plain_s3_disk_is_rejected(started_cluster):
    # For `plain` (and `plain_rewritable`) metadata over a remote object storage, `getPath()` is
    # only an object-key prefix inside the bucket, not a local filesystem path, so the guard must
    # reject the disk instead of running `fs::create_directories` on a path relative to the
    # working directory. The `CREATE` itself never reaches S3 -- the guard throws first.
    error = node.query_and_get_error(
        """
        CREATE TABLE dist_plain_s3 (key UInt64) ENGINE = Distributed(test_cluster, currentDatabase(), 'underlying', rand(), 'plain_s3_policy')
        """
    )
    assert "NOT_IMPLEMENTED" in error
    assert "does not have a real filesystem path" in error

    assert node.query("EXISTS TABLE dist_plain_s3").strip() == "0"
