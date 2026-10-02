from abc import abstractmethod
import json
import os
import pathlib
import random
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

from environment import set_environment_variables
from integration.helpers.client import CommandRequest
from integration.helpers.cluster import ClickHouseInstance
from integration.helpers.config_cluster import (
    pg_pass,
    mysql_pass,
    mongo_pass,
)


class Generator:
    def __init__(
        self,
        binary: pathlib.Path,
        config: pathlib.Path,
        tmpdir: pathlib.Path,
        _suffix: Optional[str],
    ):
        self.binary: pathlib.Path = binary
        self.config: pathlib.Path = config
        if _suffix is not None:
            self.temp = tempfile.NamedTemporaryFile(dir=tmpdir, suffix=_suffix)

    @abstractmethod
    def get_run_cmd(self, server: ClickHouseInstance) -> list[str]:
        pass

    @abstractmethod
    def validate_exit_code(self, exit_code: int) -> bool:
        pass

    def run_generator(self, server: ClickHouseInstance, logger, args) -> CommandRequest:
        return CommandRequest(
            self.get_run_cmd(server),
            stdin="",
            timeout=None,
            ignore_error=True,
            parse=False,
            stdout_file_path=sys.stdout,
            stderr_file_path=sys.stderr,
            env=set_environment_variables(logger, args, "generator"),
        )


class BuzzHouseGenerator(Generator):
    def __init__(self, args, cluster, catalog_server, server_settings):
        super().__init__(
            args.client_binary, args.client_config, args.tmp_files_dir, ".json"
        )

        if server_settings is None:
            root = ET.Element("clickhouse")
        else:
            tree = ET.parse(server_settings)
            root = tree.getroot()
            if root.tag != "clickhouse":
                raise Exception("<clickhouse> element not found")

        # Load configuration
        buzz_config = {}
        if args.client_config is not None:
            with open(args.client_config, "r") as file1:
                buzz_config = json.load(file1)

        buzz_config["seed"] = random.randint(1, 18446744073709551615)

        # Set paths
        buzz_config["client_file_path"] = (
            f"{Path(cluster.instances_dir) / 'node0' / 'database' / 'user_files'}"
        )
        buzz_config["server_file_path"] = "/var/lib/clickhouse/user_files"
        # Set available servers
        for entry in [
            ("remote_servers", "9000"),
            ("remote_secure_servers", "9440"),
            ("http_servers", "8123"),
            ("https_servers", "8443"),
        ]:
            buzz_config[entry[0]] = [
                f"{val.hostname}:{entry[1]}" for val in cluster.instances.values()
            ]
        if args.with_arrowflight:
            buzz_config["arrow_flight_servers"] = ["arrowflight1:5005"]
        # Add external integrations credentials
        if args.with_minio:
            buzz_config["minio"] = {
                "database": cluster.minio_bucket,
                "server_hostname": "minio",
                "client_hostname": cluster.minio_ip,
                "port": 9000,
                "user": "minio",
                "password": cluster.minio_access_key,
                "secret": cluster.minio_secret_key,
                "named_collection": "s3",
            }
        if args.with_postgresql:
            buzz_config["postgresql"] = {
                "query_log_file": os.path.join(args.tmp_files_dir, "postgresql.sql"),
                "database": "test",
                "server_hostname": cluster.postgres_ip,
                "client_hostname": cluster.postgres_ip,
                "port": cluster.postgres_port,
                "user": "postgres",
                "password": pg_pass,
            }
        if args.with_mysql:
            buzz_config["mysql"] = {
                "query_log_file": os.path.join(args.tmp_files_dir, "mysql.sql"),
                "database": "test",
                "server_hostname": cluster.mysql8_ip,
                "client_hostname": cluster.mysql8_ip,
                "port": cluster.mysql8_port,
                "user": "root",
                "password": mysql_pass,
            }
        if args.with_sqlite:
            buzz_config["sqlite"] = {
                "query_log_file": os.path.join(args.tmp_files_dir, "sqlite.sql")
            }
        if args.with_mongodb:
            import urllib

            buzz_config["mongodb"] = {
                "query_log_file": os.path.join(args.tmp_files_dir, "mongodb.doc"),
                "database": "test",
                "server_hostname": cluster.mongo_host,
                "port": cluster.mongo_port,
                "user": "root",
                "password": urllib.parse.quote_plus(mongo_pass),
            }
        if args.with_redis:
            buzz_config["redis"] = {
                "server_hostname": cluster.redis_host,
                "port": cluster.redis_port,
                "user": "",
                "password": "clickhouse",
            }
        if args.with_nginx:
            buzz_config["http"] = {
                "server_hostname": cluster.nginx_host,
                "port": cluster.nginx_port,
            }
        if args.with_azurite:
            buzz_config["azurite"] = {
                "server_hostname": cluster.env_variables["AZURITE_STORAGE_ACCOUNT_URL"],
                "database": cluster.env_variables[
                    "AZURITE_CONNECTION_STRING"
                ],  # it's hacking a little
                "container": cluster.azure_container_name,
                "user": cluster.azurite_account,
                "password": cluster.azurite_key,
                "named_collection": "azure",
            }
        if args.add_keeper_map_prefix:
            buzz_config["keeper_map_path_prefix"] = "/keeper_map_tables"
        # Set SMT disk only when property.py doesn't do it
        buzz_config["set_smt_disk"] = root.find("shared_merge_tree") is None
        if (
            args.with_spark
            or args.with_glue
            or args.with_hms
            or args.with_rest
            or args.with_unity
            or args.with_kafka
        ):
            buzz_config["dolor"] = {
                "server_hostname": catalog_server.host,
                "client_hostname": catalog_server.host,
                "port": catalog_server.port,
            }
            if args.with_glue:
                buzz_config["dolor"]["glue"] = {
                    "server_hostname": "glue",
                    "region": "us-east-1",
                    "port": 3000,
                    "warehouse": "warehouse-glue",
                }
            if args.with_hms:
                buzz_config["dolor"]["hive"] = {
                    "server_hostname": "hive",
                    "region": "us-east-1",
                    "port": 9083,
                    "warehouse": "warehouse-hms",
                }
            if args.with_rest:
                buzz_config["dolor"]["rest"] = {
                    "server_hostname": "rest",
                    "region": "us-east-1",
                    "port": 8181,
                    "path": "/v1",
                    "warehouse": "warehouse-rest",
                }
            if args.with_unity:
                buzz_config["dolor"]["unity"] = {
                    "server_hostname": "host.docker.internal",
                    "port": 8085,
                    "path": "/api/2.1/unity-catalog",
                    "warehouse": "unity",
                }
            if args.with_kafka:
                buzz_config["kafka"] = {
                    "server_hostname": cluster.kafka_host,
                    "port": cluster.kafka_port,
                    "user": "",
                    "password": "",
                }

        with open(self.temp.name, "w+") as file2:
            file2.write(json.dumps(buzz_config))

    def get_run_cmd(self, server: ClickHouseInstance) -> list[str]:
        return [
            str(self.binary),
            "--client",
            "--host",
            f"{server.ip_address}",
            "--port",
            "9000",
            "--max_memory_usage_in_client=1000000000",
            f"--buzz-house-config={self.temp.name}",
        ]

    def validate_exit_code(self, exit_code: int) -> bool:
        return exit_code in (0, 137, 143)
