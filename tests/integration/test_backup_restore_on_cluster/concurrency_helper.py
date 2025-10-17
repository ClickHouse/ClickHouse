from pathlib import Path
from typing import Callable, List

from helpers.cluster import ClickHouseCluster, ClickHouseInstance


def generate_cluster_def(file: str, num_nodes: int) -> str:
    path = (
        Path(__file__).parent / f"_gen/cluster_{Path(file).stem}_{num_nodes}_nodes.xml"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    replicas = "\n".join(
        f"""                <replica>
                    <host>node{i}</host>
                    <port>9000</port>
                </replica>"""
        for i in range(num_nodes)
    )
    path.write_text(
        encoding="utf-8",
        data=f"""<clickhouse>
    <remote_servers>
        <cluster>
            <shard>
{replicas}
            </shard>
        </cluster>
    </remote_servers>
</clickhouse>""",
    )
    return str(path.absolute())


def add_nodes_to_cluster(
    cluster: ClickHouseCluster,
    num_nodes: int,
    main_configs: List[str],
    user_configs: List[str],
) -> List[ClickHouseInstance]:
    nodes = [
        cluster.add_instance(
            f"node{i}",
            main_configs=main_configs,
            user_configs=user_configs,
            external_dirs=["/backups/"],
            macros={"replica": f"node{i}", "shard": "shard1"},
            with_zookeeper=True,
        )
        for i in range(num_nodes)
    ]
    return nodes


def create_test_table(node: ClickHouseInstance) -> None:
    node.query(
        """CREATE TABLE tbl ON CLUSTER 'cluster' ( x UInt64 )
ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')
ORDER BY tuple()"""
    )
