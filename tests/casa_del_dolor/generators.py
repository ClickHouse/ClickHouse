from abc import abstractmethod
import json
import pathlib
import random
import sys
import tempfile

from integration.helpers.client import CommandRequest
from integration.helpers.cluster import ClickHouseInstance
from integration.helpers.config_cluster import (
    minio_secret_key,
    pg_pass,
    mysql_pass,
    mongo_pass,
)


class Generator:
    def __init__(self, binary: pathlib.Path, config: pathlib.Path, _suffix: str):
        self.binary: pathlib.Path = binary
        self.config: pathlib.Path = config
        self.temp = tempfile.NamedTemporaryFile(suffix=_suffix)

    @abstractmethod
    def run_generator(self) -> CommandRequest:
        pass


class BuzzHouseGenerator(Generator):
    def __init__(self, args, cluster):
        super().__init__(args.client_binary, args.client_config, ".json")

        # Load configuration
        buzz_config = {}
        if args.client_config is not None:
            with open(args.client_config, "r") as file1:
                buzz_config = json.load(file1)

        buzz_config["seed"] = random.randint(1, 18446744073709551615)

        # Connect back to peer ClickHouse server running in the host machine
        if "clickhouse" in buzz_config:
            buzz_config["clickhouse"]["server_hostname"] = "host.docker.internal"

        # Add external integrations credentials
        if args.with_minio:
            buzz_config["minio"] = {
                "database": "/" + cluster.minio_bucket,
                "server_hostname": cluster.minio_host,
                "port": cluster.minio_port,
                "user": "minio",
                "password": minio_secret_key,
            }
        if args.with_postgresql:
            buzz_config["postgresql"] = {
                "query_log_file": "/tmp/postgresql.sql",
                "database": "test",
                "server_hostname": cluster.postgres_ip,
                "port": cluster.postgres_port,
                "user": "postgres",
                "password": pg_pass,
            }
        if args.with_mysql:
            buzz_config["mysql"] = {
                "query_log_file": "/tmp/mysql.sql",
                "database": "test",
                "server_hostname": cluster.mysql8_ip,
                "port": cluster.mysql8_port,
                "user": "root",
                "password": mysql_pass,
            }
        if args.with_sqlite:
            buzz_config["sqlite"] = {"query_log_file": "/tmp/sqlite.sql"}
        if args.with_mongodb:
            import urllib

            buzz_config["mongodb"] = {
                "query_log_file": "/tmp/mongodb.doc",
                "database": "test",
                "server_hostname": "localhost",
                "port": 27017,
                "user": "root",
                "password": urllib.parse.quote_plus(mongo_pass),
            }
        if args.with_redis:
            buzz_config["redis"] = {
                "server_hostname": cluster.redis_host,
                "port": 6379,
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
                "container": "cont",
                "user": "devstoreaccount1",
                "password": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
            }
        if args.add_keeper_map_prefix:
            buzz_config["keeper_map_path_prefix"] = "/keeper_map_tables"

        with open(self.temp.name, "w") as file2:
            file2.write(json.dumps(buzz_config))

    def run_generator(self, server: ClickHouseInstance) -> CommandRequest:
        return CommandRequest(
            [
                self.binary,
                "--client",
                "--host",
                f"{server.ip_address}",
                "--port",
                "9000",
                f"--buzz-house-config={self.temp.name}",
            ],
            stdin="",
            timeout=None,
            ignore_error=True,
            parse=False,
            stdout_file_path=sys.stdout,
            stderr_file_path=sys.stderr,
        )
