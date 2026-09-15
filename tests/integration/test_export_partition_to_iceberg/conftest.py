import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.export_partition_helpers import SOURCE_ENGINE_IDS, SOURCE_ENGINES

# `EXPORT PARTITION` to an Iceberg destination, split across several modules so the harness can
# spread them over xdist workers (`--dist=loadfile` assigns a whole module to one worker).
#
# Each module declares the instances it needs in `CLUSTER_INSTANCES` and gets a cluster with only
# those, so modules that never touch a second replica do not start one.

REPLICA = dict(
    main_configs=[
        "configs/allow_experimental_export_partition.xml",
        "configs/config.d/metadata_log.xml",
    ],
    user_configs=["configs/users.d/profile.xml"],
    with_minio=True,
    stay_alive=True,
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read"],
)

INSTANCES = {"replica1": REPLICA, "replica2": REPLICA}


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
    running ZooKeeper background threads.  With many tables alive simultaneously the
    ZooKeeper session becomes overwhelmed and subsequent tests start seeing
    operation-timeout / session-expired errors.
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
            tables = [table.strip() for table in tables_str.split("\n") if table.strip()]
            if tables:
                instance.query(
                    "".join(f"DROP TABLE IF EXISTS default.`{table}` SYNC;" for table in tables)
                )
        except Exception as e:
            logging.warning(
                f"drop_tables_after_test: cleanup failed on {instance_name}: {e}"
            )


@pytest.fixture(params=SOURCE_ENGINES, ids=SOURCE_ENGINE_IDS)
def source_engine(request):
    """The MergeTree flavour of the export source table.

    A test that requests this fixture runs once per engine; the scenarios that only make sense
    with cross-replica coordination do not request it and stay on `ReplicatedMergeTree`.
    """
    return request.param
