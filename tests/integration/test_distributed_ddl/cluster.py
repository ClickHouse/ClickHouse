import os
import os.path as p
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV


class ClickHouseClusterWithDDLHelpers(ClickHouseCluster):
    def __init__(self, base_path, config_dir, testcase_name):
        ClickHouseCluster.__init__(self, base_path, name=testcase_name)

        self.test_config_dir = config_dir

    def prepare(self, replace_hostnames_with_ips=True):
        try:
            main_configs_files = [
                "clusters.xml",
                "zookeeper_session_timeout.xml",
                "macro.xml",
                "query_log.xml",
                "ddl.xml",
            ]
            main_configs = [
                os.path.join(self.test_config_dir, "config.d", f)
                for f in main_configs_files
            ]
            user_configs = [
                os.path.join(self.test_config_dir, "users.d", f)
                for f in ["restricted_user.xml", "query_log.xml"]
            ]
            if self.test_config_dir == "configs_secure":
                main_configs += [
                    os.path.join(self.test_config_dir, f)
                    for f in [
                        "server.crt",
                        "server.key",
                        "dhparam.pem",
                        "config.d/ssl_conf.xml",
                    ]
                ]

            for i in range(4):
                self.add_instance(
                    "ch{}".format(i + 1),
                    main_configs=main_configs,
                    user_configs=user_configs,
                    macros={"layer": 0, "shard": i // 2 + 1, "replica": i % 2 + 1},
                    with_zookeeper=True,
                )

            self.start()

            # Replace config files for testing ability to set host in DNS and IP formats
            if replace_hostnames_with_ips:
                self.replace_domains_to_ip_addresses_in_cluster_config(["ch1", "ch3"])

            # Select sacrifice instance to test CONNECTION_LOSS and server fail on it
            sacrifice = self.instances["ch4"]
            self.pm_random_drops = PartitionManager()
            self.pm_random_drops._add_rule(
                {
                    "probability": 0.01,
                    "destination": sacrifice.ip_address,
                    "source_port": 2181,
                    "action": "REJECT --reject-with tcp-reset",
                }
            )
            self.pm_random_drops._add_rule(
                {
                    "probability": 0.01,
                    "source": sacrifice.ip_address,
                    "destination_port": 2181,
                    "action": "REJECT --reject-with tcp-reset",
                }
            )

            # Initialize databases and service tables
            instance = self.instances["ch1"]

            self.ddl_check_query(
                instance,
                """
        CREATE TABLE IF NOT EXISTS all_tables ON CLUSTER 'cluster_no_replicas'
            (database String, name String, engine String, metadata_modification_time DateTime)
            ENGINE = Distributed('cluster_no_replicas', 'system', 'tables')
                """,
            )

            self.ddl_check_query(
                instance, "CREATE DATABASE IF NOT EXISTS test ON CLUSTER 'cluster'"
            )

        except Exception as e:
            print(e)
            raise

    def sync_replicas(self, table, timeout=5):
        for instance in list(self.instances.values()):
            instance.query("SYSTEM SYNC REPLICA {}".format(table), timeout=timeout)

    def check_all_hosts_successfully_executed(self, tsv_content, num_hosts=None):
        if num_hosts is None:
            num_hosts = len(self.instances)

        M = TSV.toMat(tsv_content)
        hosts = [(l[0], l[1]) for l in M]  # (host, port)
        codes = [l[2] for l in M]
        messages = [l[3] for l in M]

        assert len(hosts) == num_hosts and len(set(hosts)) == num_hosts, (
            "\n" + tsv_content
        )
        assert len(set(codes)) == 1, "\n" + tsv_content
        assert codes[0] == "0", "\n" + tsv_content

    def ddl_check_query(self, instance, query, num_hosts=None, settings=None):
        contents = instance.query_with_retry(query, settings=settings)
        self.check_all_hosts_successfully_executed(contents, num_hosts)
        return contents

    def replace_domains_to_ip_addresses_in_cluster_config(self, instances_to_replace):
        clusters_config = open(
            p.join(
                self.base_dir, "{}/config.d/clusters.xml".format(self.test_config_dir)
            )
        ).read()

        for inst_name, inst in list(self.instances.items()):
            clusters_config = clusters_config.replace(inst_name, str(inst.ip_address))

        for inst_name in instances_to_replace:
            inst = self.instances[inst_name]
            self.instances[inst_name].exec_in_container(
                [
                    "bash",
                    "-c",
                    'echo "$NEW_CONFIG" > /etc/clickhouse-server/config.d/clusters.xml',
                ],
                environment={"NEW_CONFIG": clusters_config},
                privileged=True,
            )
            # print cluster.instances[inst_name].exec_in_container(['cat', "/etc/clickhouse-server/config.d/clusters.xml"])

    @staticmethod
    def ddl_check_there_are_no_dublicates(instance):
        query = "SELECT max(c), argMax(q, c) FROM (SELECT lower(query) AS q, count() AS c FROM system.query_log WHERE type=2 AND q LIKE '/* ddl_entry=query-%' GROUP BY query)"
        rows = instance.query(query)
        assert len(rows) > 0 and rows[0][0] == "1", "dublicates on {} {}: {}".format(
            instance.name, instance.ip_address, rows
        )

    @staticmethod
    def insert_reliable(instance, query_insert):
        """
        Make retries in case of UNKNOWN_STATUS_OF_INSERT or zkutil::KeeperException errors
        """

        for i in range(100):
            try:
                instance.query(query_insert)
                return
            except Exception as e:
                last_exception = e
                s = str(e)
                if not (
                    s.find("Unknown status, client must retry") >= 0
                    or s.find("zkutil::KeeperException")
                ):
                    raise e

        raise last_exception
