import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.export_partition_helpers import SOURCE_ENGINE_IDS, SOURCE_ENGINES

# `EXPORT PARTITION` to a plain object-storage destination, split across several modules so the
# harness can spread them over xdist workers (`--dist=loadfile` assigns a whole module to one
# worker).
#
# Each module declares the instances it needs in `CLUSTER_INSTANCES` and gets a cluster with only
# those. That matters because several of these modules can be in flight on different workers at
# once, and every instance is a full server - starting the whole set six times over saturates the
# host under a sanitizer build.

NAMED_COLLECTIONS = "configs/named_collections.xml"
EXPORT_ENABLED = "configs/allow_experimental_export_partition.xml"
PROFILE = "configs/users.d/profile.xml"

_REPLICA = dict(
    main_configs=[NAMED_COLLECTIONS, EXPORT_ENABLED],
    user_configs=[PROFILE],
    with_minio=True,
    stay_alive=True,
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read"],
)

INSTANCES = {
    "replica1": _REPLICA,
    "replica2": _REPLICA,
    # Takes no part in the export, but has visibility over the destination table.
    "watcher_node": dict(
        main_configs=[NAMED_COLLECTIONS],
        user_configs=[],
        with_minio=True,
    ),
    "replica_with_export_disabled": dict(
        _REPLICA,
        main_configs=[NAMED_COLLECTIONS, "configs/disable_experimental_export_partition.xml"],
    ),
    # Sharded instances for the filename pattern tests.
    "shard1_replica1": dict(
        _REPLICA,
        main_configs=[NAMED_COLLECTIONS, EXPORT_ENABLED, "configs/macros_shard1_replica1.xml"],
    ),
    "shard2_replica1": dict(
        _REPLICA,
        main_configs=[NAMED_COLLECTIONS, EXPORT_ENABLED, "configs/macros_shard2_replica1.xml"],
    ),
}


@pytest.fixture(scope="module")
def cluster(request):
    instance_names = getattr(request.module, "CLUSTER_INSTANCES", list(INSTANCES))
    try:
        cluster = ClickHouseCluster(__file__)
        for name in instance_names:
            cluster.add_instance(name, **INSTANCES[name])
        logging.info("Starting cluster with instances %s...", instance_names)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_tables_after_test(cluster):
    """Drop all tables in the default database after every test.

    Without this, ReplicatedMergeTree tables from completed tests remain alive and keep
    running ZooKeeper background threads (merge selector, queue log, cleanup, export manifest
    updater).  With many tables alive simultaneously the ZooKeeper session becomes overwhelmed
    and subsequent tests start seeing operation-timeout / session-expired errors.
    """
    yield
    for instance_name, instance in cluster.instances.items():
        try:
            tables_str = instance.query(
                "SELECT name FROM system.tables WHERE database = 'default' FORMAT TabSeparated"
            ).strip()
            if not tables_str:
                continue
            # One client invocation for the whole batch. Every query spawns a fresh client
            # process, which costs seconds on a sanitizer build, so dropping tables one at a
            # time made teardown a large share of this suite's runtime.
            tables = [table.strip() for table in tables_str.split('\n') if table.strip()]
            if tables:
                instance.query(
                    "".join(f"DROP TABLE IF EXISTS default.`{table}` SYNC;" for table in tables)
                )
        except Exception as e:
            logging.warning(f"drop_tables_after_test: cleanup failed on {instance_name}: {e}")


@pytest.fixture(params=SOURCE_ENGINES, ids=SOURCE_ENGINE_IDS)
def source_engine(request):
    """The MergeTree flavour of the export source table.

    A test that requests this fixture runs once per engine; the scenarios that only make sense
    with cross-replica coordination do not request it and stay on `ReplicatedMergeTree`.
    """
    return request.param
