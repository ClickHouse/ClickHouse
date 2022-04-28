import base64
import errno
import http.client
import logging
import os
import stat
import os.path as p
import pprint
import pwd
import re
import shutil
import socket
import subprocess
import time
import traceback
import urllib.parse
import shlex
import urllib3
import requests

try:
    # Please, add modules that required for specific tests only here.
    # So contributors will be able to run most tests locally
    # without installing tons of unneeded packages that may be not so easy to install.
    from cassandra.policies import RoundRobinPolicy
    import cassandra.cluster
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    import pymongo
    import pymysql
    from confluent_kafka.avro.cached_schema_registry_client import (
        CachedSchemaRegistryClient,
    )
    import meilisearch
except Exception as e:
    logging.warning(f"Cannot import some modules, some tests may not work: {e}")

from dict2xml import dict2xml
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
from minio import Minio

from helpers.test_tools import assert_eq_with_retry, exec_query_with_retry
from helpers import pytest_xdist_logging_to_separate_files
from helpers.client import QueryRuntimeException

import docker

from .client import Client
from .hdfs_api import HDFSApi

HELPERS_DIR = p.dirname(__file__)
CLICKHOUSE_ROOT_DIR = p.join(p.dirname(__file__), "../../..")
LOCAL_DOCKER_COMPOSE_DIR = p.join(
    CLICKHOUSE_ROOT_DIR, "docker/test/integration/runner/compose/"
)
DEFAULT_ENV_NAME = ".env"

SANITIZER_SIGN = "=================="

# to create docker-compose env file
def _create_env_file(path, variables):
    logging.debug(f"Env {variables} stored in {path}")
    with open(path, "w") as f:
        for var, value in list(variables.items()):
            f.write("=".join([var, value]) + "\n")
    return path


def run_and_check(
    args,
    env=None,
    shell=False,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    timeout=300,
    nothrow=False,
    detach=False,
):
    if detach:
        subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
            shell=shell,
        )
        return

    logging.debug(f"Command:{args}")
    res = subprocess.run(
        args, stdout=stdout, stderr=stderr, env=env, shell=shell, timeout=timeout
    )
    out = res.stdout.decode("utf-8")
    err = res.stderr.decode("utf-8")
    # check_call(...) from subprocess does not print stderr, so we do it manually
    for outline in out.splitlines():
        logging.debug(f"Stdout:{outline}")
    for errline in err.splitlines():
        logging.debug(f"Stderr:{errline}")
    if res.returncode != 0:
        logging.debug(f"Exitcode:{res.returncode}")
        if env:
            logging.debug(f"Env:{env}")
        if not nothrow:
            raise Exception(
                f"Command {args} return non-zero code {res.returncode}: {res.stderr.decode('utf-8')}"
            )
    return out


# Based on https://stackoverflow.com/questions/2838244/get-open-tcp-port-in-python/2838309#2838309
def get_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


def retry_exception(num, delay, func, exception=Exception, *args, **kwargs):
    """
    Retry if `func()` throws, `num` times.

    :param func: func to run
    :param num: number of retries

    :throws StopIteration
    """
    i = 0
    while i <= num:
        try:
            func(*args, **kwargs)
            time.sleep(delay)
        except exception:  # pylint: disable=broad-except
            i += 1
            continue
        return
    raise StopIteration("Function did not finished successfully")


def subprocess_check_call(args, detach=False, nothrow=False):
    # Uncomment for debugging
    # logging.info('run:' + ' '.join(args))
    return run_and_check(args, detach=detach, nothrow=nothrow)


def get_odbc_bridge_path():
    path = os.environ.get("CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH")
    if path is None:
        server_path = os.environ.get("CLICKHOUSE_TESTS_SERVER_BIN_PATH")
        if server_path is not None:
            return os.path.join(os.path.dirname(server_path), "clickhouse-odbc-bridge")
        else:
            return "/usr/bin/clickhouse-odbc-bridge"
    return path


def get_library_bridge_path():
    path = os.environ.get("CLICKHOUSE_TESTS_LIBRARY_BRIDGE_BIN_PATH")
    if path is None:
        server_path = os.environ.get("CLICKHOUSE_TESTS_SERVER_BIN_PATH")
        if server_path is not None:
            return os.path.join(
                os.path.dirname(server_path), "clickhouse-library-bridge"
            )
        else:
            return "/usr/bin/clickhouse-library-bridge"
    return path


def get_docker_compose_path():
    compose_path = os.environ.get("DOCKER_COMPOSE_DIR")
    if compose_path is not None:
        return os.path.dirname(compose_path)
    else:
        if os.path.exists(os.path.dirname("/compose/")):
            return os.path.dirname("/compose/")  # default in docker runner container
        else:
            logging.debug(
                f"Fallback docker_compose_path to LOCAL_DOCKER_COMPOSE_DIR: {LOCAL_DOCKER_COMPOSE_DIR}"
            )
            return LOCAL_DOCKER_COMPOSE_DIR


def check_kafka_is_available(kafka_id, kafka_port):
    p = subprocess.Popen(
        (
            "docker",
            "exec",
            "-i",
            kafka_id,
            "/usr/bin/kafka-broker-api-versions",
            "--bootstrap-server",
            f"INSIDE://localhost:{kafka_port}",
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    p.communicate()
    return p.returncode == 0


def check_rabbitmq_is_available(rabbitmq_id):
    p = subprocess.Popen(
        ("docker", "exec", "-i", rabbitmq_id, "rabbitmqctl", "await_startup"),
        stdout=subprocess.PIPE,
    )
    p.communicate()
    return p.returncode == 0


def enable_consistent_hash_plugin(rabbitmq_id):
    p = subprocess.Popen(
        (
            "docker",
            "exec",
            "-i",
            rabbitmq_id,
            "rabbitmq-plugins",
            "enable",
            "rabbitmq_consistent_hash_exchange",
        ),
        stdout=subprocess.PIPE,
    )
    p.communicate()
    return p.returncode == 0


def get_instances_dir():
    if (
        "INTEGRATION_TESTS_RUN_ID" in os.environ
        and os.environ["INTEGRATION_TESTS_RUN_ID"]
    ):
        return "_instances_" + shlex.quote(os.environ["INTEGRATION_TESTS_RUN_ID"])
    else:
        return "_instances"


class ClickHouseCluster:
    """ClickHouse cluster with several instances and (possibly) ZooKeeper.

    Add instances with several calls to add_instance(), then start them with the start() call.

    Directories for instances are created in the directory of base_path. After cluster is started,
    these directories will contain logs, database files, docker-compose config, ClickHouse configs etc.
    """

    def __init__(
        self,
        base_path,
        name=None,
        base_config_dir=None,
        server_bin_path=None,
        client_bin_path=None,
        odbc_bridge_bin_path=None,
        library_bridge_bin_path=None,
        zookeeper_config_path=None,
        custom_dockerd_host=None,
        zookeeper_keyfile=None,
        zookeeper_certfile=None,
    ):
        for param in list(os.environ.keys()):
            logging.debug("ENV %40s %s" % (param, os.environ[param]))
        self.base_path = base_path
        self.base_dir = p.dirname(base_path)
        self.name = name if name is not None else ""

        self.base_config_dir = base_config_dir or os.environ.get(
            "CLICKHOUSE_TESTS_BASE_CONFIG_DIR", "/etc/clickhouse-server/"
        )
        self.server_bin_path = p.realpath(
            server_bin_path
            or os.environ.get("CLICKHOUSE_TESTS_SERVER_BIN_PATH", "/usr/bin/clickhouse")
        )
        self.odbc_bridge_bin_path = p.realpath(
            odbc_bridge_bin_path or get_odbc_bridge_path()
        )
        self.library_bridge_bin_path = p.realpath(
            library_bridge_bin_path or get_library_bridge_path()
        )
        self.client_bin_path = p.realpath(
            client_bin_path
            or os.environ.get(
                "CLICKHOUSE_TESTS_CLIENT_BIN_PATH", "/usr/bin/clickhouse-client"
            )
        )
        self.zookeeper_config_path = (
            p.join(self.base_dir, zookeeper_config_path)
            if zookeeper_config_path
            else p.join(HELPERS_DIR, "zookeeper_config.xml")
        )

        project_name = (
            pwd.getpwuid(os.getuid()).pw_name + p.basename(self.base_dir) + self.name
        )
        # docker-compose removes everything non-alphanumeric from project names so we do it too.
        self.project_name = re.sub(r"[^a-z0-9]", "", project_name.lower())
        instances_dir_name = "_instances"
        if self.name:
            instances_dir_name += "_" + self.name

        if (
            "INTEGRATION_TESTS_RUN_ID" in os.environ
            and os.environ["INTEGRATION_TESTS_RUN_ID"]
        ):
            instances_dir_name += "_" + shlex.quote(
                os.environ["INTEGRATION_TESTS_RUN_ID"]
            )

        self.instances_dir = p.join(self.base_dir, instances_dir_name)
        self.docker_logs_path = p.join(self.instances_dir, "docker.log")
        self.env_file = p.join(self.instances_dir, DEFAULT_ENV_NAME)
        self.env_variables = {}
        self.env_variables["TSAN_OPTIONS"] = "second_deadlock_stack=1"
        self.env_variables["CLICKHOUSE_WATCHDOG_ENABLE"] = "0"
        self.up_called = False

        custom_dockerd_host = custom_dockerd_host or os.environ.get(
            "CLICKHOUSE_TESTS_DOCKERD_HOST"
        )
        self.docker_api_version = os.environ.get("DOCKER_API_VERSION")
        self.docker_base_tag = os.environ.get("DOCKER_BASE_TAG", "latest")

        self.base_cmd = ["docker-compose"]
        if custom_dockerd_host:
            self.base_cmd += ["--host", custom_dockerd_host]
        self.base_cmd += ["--env-file", self.env_file]
        self.base_cmd += ["--project-name", self.project_name]

        self.base_zookeeper_cmd = None
        self.base_mysql_cmd = []
        self.base_kafka_cmd = []
        self.base_kerberized_kafka_cmd = []
        self.base_rabbitmq_cmd = []
        self.base_cassandra_cmd = []
        self.base_jdbc_bridge_cmd = []
        self.base_redis_cmd = []
        self.pre_zookeeper_commands = []
        self.instances = {}
        self.with_zookeeper = False
        self.with_zookeeper_secure = False
        self.with_mysql_client = False
        self.with_mysql = False
        self.with_mysql8 = False
        self.with_mysql_cluster = False
        self.with_postgres = False
        self.with_postgres_cluster = False
        self.with_kafka = False
        self.with_kerberized_kafka = False
        self.with_rabbitmq = False
        self.with_odbc_drivers = False
        self.with_hdfs = False
        self.with_kerberized_hdfs = False
        self.with_mongo = False
        self.with_mongo_secure = False
        self.with_net_trics = False
        self.with_redis = False
        self.with_cassandra = False
        self.with_jdbc_bridge = False
        self.with_nginx = False
        self.with_hive = False

        self.with_minio = False
        self.minio_dir = os.path.join(self.instances_dir, "minio")
        self.minio_certs_dir = None  # source for certificates
        self.minio_host = "minio1"
        self.minio_ip = None
        self.minio_bucket = "root"
        self.minio_bucket_2 = "root2"
        self.minio_port = 9001
        self.minio_client = None  # type: Minio
        self.minio_redirect_host = "proxy1"
        self.minio_redirect_ip = None
        self.minio_redirect_port = 8080

        self.with_azurite = False

        # available when with_hdfs == True
        self.hdfs_host = "hdfs1"
        self.hdfs_ip = None
        self.hdfs_name_port = 50070
        self.hdfs_data_port = 50075
        self.hdfs_dir = p.abspath(p.join(self.instances_dir, "hdfs"))
        self.hdfs_logs_dir = os.path.join(self.hdfs_dir, "logs")
        self.hdfs_api = None  # also for kerberized hdfs

        # available when with_kerberized_hdfs == True
        self.hdfs_kerberized_host = "kerberizedhdfs1"
        self.hdfs_kerberized_ip = None
        self.hdfs_kerberized_name_port = 50070
        self.hdfs_kerberized_data_port = 1006
        self.hdfs_kerberized_dir = p.abspath(
            p.join(self.instances_dir, "kerberized_hdfs")
        )
        self.hdfs_kerberized_logs_dir = os.path.join(self.hdfs_kerberized_dir, "logs")

        # available when with_kafka == True
        self.kafka_host = "kafka1"
        self.kafka_port = get_free_port()
        self.kafka_docker_id = None
        self.schema_registry_host = "schema-registry"
        self.schema_registry_port = get_free_port()
        self.kafka_docker_id = self.get_instance_docker_id(self.kafka_host)

        # available when with_kerberozed_kafka == True
        self.kerberized_kafka_host = "kerberized_kafka1"
        self.kerberized_kafka_port = get_free_port()
        self.kerberized_kafka_docker_id = self.get_instance_docker_id(
            self.kerberized_kafka_host
        )

        # available when with_mongo == True
        self.mongo_host = "mongo1"
        self.mongo_port = get_free_port()
        self.mongo_no_cred_host = "mongo2"
        self.mongo_no_cred_port = get_free_port()

        # available when with_cassandra == True
        self.cassandra_host = "cassandra1"
        self.cassandra_port = 9042
        self.cassandra_ip = None
        self.cassandra_id = self.get_instance_docker_id(self.cassandra_host)

        # available when with_rabbitmq == True
        self.rabbitmq_host = "rabbitmq1"
        self.rabbitmq_ip = None
        self.rabbitmq_port = 5672
        self.rabbitmq_dir = p.abspath(p.join(self.instances_dir, "rabbitmq"))
        self.rabbitmq_logs_dir = os.path.join(self.rabbitmq_dir, "logs")

        # available when with_nginx == True
        self.nginx_host = "nginx"
        self.nginx_ip = None
        self.nginx_port = 80
        self.nginx_id = self.get_instance_docker_id(self.nginx_host)

        # available when with_redis == True
        self.redis_host = "redis1"
        self.redis_port = get_free_port()

        # available when with_postgres == True
        self.postgres_host = "postgres1"
        self.postgres_ip = None
        self.postgres_conn = None
        self.postgres2_host = "postgres2"
        self.postgres2_ip = None
        self.postgres2_conn = None
        self.postgres3_host = "postgres3"
        self.postgres3_ip = None
        self.postgres3_conn = None
        self.postgres4_host = "postgres4"
        self.postgres4_ip = None
        self.postgres4_conn = None
        self.postgres_port = 5432
        self.postgres_dir = p.abspath(p.join(self.instances_dir, "postgres"))
        self.postgres_logs_dir = os.path.join(self.postgres_dir, "postgres1")
        self.postgres2_logs_dir = os.path.join(self.postgres_dir, "postgres2")
        self.postgres3_logs_dir = os.path.join(self.postgres_dir, "postgres3")
        self.postgres4_logs_dir = os.path.join(self.postgres_dir, "postgres4")

        # available when with_mysql_client == True
        self.mysql_client_host = "mysql_client"
        self.mysql_client_container = None

        # available when with_mysql == True
        self.mysql_host = "mysql57"
        self.mysql_port = 3306
        self.mysql_ip = None
        self.mysql_dir = p.abspath(p.join(self.instances_dir, "mysql"))
        self.mysql_logs_dir = os.path.join(self.mysql_dir, "logs")

        # available when with_mysql_cluster == True
        self.mysql2_host = "mysql2"
        self.mysql3_host = "mysql3"
        self.mysql4_host = "mysql4"
        self.mysql2_ip = None
        self.mysql3_ip = None
        self.mysql4_ip = None
        self.mysql_cluster_dir = p.abspath(p.join(self.instances_dir, "mysql"))
        self.mysql_cluster_logs_dir = os.path.join(self.mysql_dir, "logs")

        # available when with_mysql8 == True
        self.mysql8_host = "mysql80"
        self.mysql8_port = 3306
        self.mysql8_ip = None
        self.mysql8_dir = p.abspath(p.join(self.instances_dir, "mysql8"))
        self.mysql8_logs_dir = os.path.join(self.mysql8_dir, "logs")

        # available when with_zookeper_secure == True
        self.zookeeper_secure_port = 2281
        self.zookeeper_keyfile = zookeeper_keyfile
        self.zookeeper_certfile = zookeeper_certfile

        # available when with_zookeper == True
        self.use_keeper = True
        self.zookeeper_port = 2181
        self.keeper_instance_dir_prefix = p.join(
            p.abspath(self.instances_dir), "keeper"
        )  # if use_keeper = True
        self.zookeeper_instance_dir_prefix = p.join(self.instances_dir, "zk")
        self.zookeeper_dirs_to_create = []

        # available when with_jdbc_bridge == True
        self.jdbc_bridge_host = "bridge1"
        self.jdbc_bridge_ip = None
        self.jdbc_bridge_port = 9019
        self.jdbc_driver_dir = p.abspath(p.join(self.instances_dir, "jdbc_driver"))
        self.jdbc_driver_logs_dir = os.path.join(self.jdbc_driver_dir, "logs")

        self.docker_client = None
        self.is_up = False
        self.env = os.environ.copy()
        logging.debug(f"CLUSTER INIT base_config_dir:{self.base_config_dir}")

    def cleanup(self):
        if (
            os.environ
            and "DISABLE_CLEANUP" in os.environ
            and os.environ["DISABLE_CLEANUP"] == "1"
        ):
            logging.warning("Cleanup is disabled")
            return

        # Just in case kill unstopped containers from previous launch
        try:
            unstopped_containers = self.get_running_containers()
            if unstopped_containers:
                logging.debug(
                    f"Trying to kill unstopped containers: {unstopped_containers}"
                )
                for id in unstopped_containers:
                    run_and_check(f"docker kill {id}", shell=True, nothrow=True)
                    run_and_check(f"docker rm {id}", shell=True, nothrow=True)
                unstopped_containers = self.get_running_containers()
                if unstopped_containers:
                    logging.debug(f"Left unstopped containers: {unstopped_containers}")
                else:
                    logging.debug(f"Unstopped containers killed.")
            else:
                logging.debug(f"No running containers for project: {self.project_name}")
        except:
            pass

        # # Just in case remove unused networks
        # try:
        #     logging.debug("Trying to prune unused networks...")

        #     run_and_check(['docker', 'network', 'prune', '-f'])
        #     logging.debug("Networks pruned")
        # except:
        #     pass

        # Remove unused images
        # try:
        #     logging.debug("Trying to prune unused images...")

        #     run_and_check(['docker', 'image', 'prune', '-f'])
        #     logging.debug("Images pruned")
        # except:
        #     pass

        # Remove unused volumes
        try:
            logging.debug("Trying to prune unused volumes...")

            result = run_and_check(["docker volume ls | wc -l"], shell=True)
            if int(result > 0):
                run_and_check(["docker", "volume", "prune", "-f"])
            logging.debug(f"Volumes pruned: {result}")
        except:
            pass

    def get_docker_handle(self, docker_id):
        exception = None
        for i in range(5):
            try:
                return self.docker_client.containers.get(docker_id)
            except Exception as ex:
                print("Got exception getting docker handle", str(ex))
                time.sleep(i * 2)
                exception = ex
        raise exception

    def get_client_cmd(self):
        cmd = self.client_bin_path
        if p.basename(cmd) == "clickhouse":
            cmd += " client"
        return cmd

    # Returns the list of currently running docker containers corresponding to this ClickHouseCluster.
    def get_running_containers(self):
        # docker-compose names containers using the following formula:
        # container_name = project_name + '_' + instance_name + '_1'
        # We need to have "^/" and "$" in the "--filter name" option below to filter by exact name of the container, see
        # https://stackoverflow.com/questions/48767760/how-to-make-docker-container-ls-f-name-filter-by-exact-name
        filter_name = f"^/{self.project_name}_.*_1$"
        # We want the command "docker container list" to show only containers' ID and their names, separated by colon.
        format = "{{.ID}}:{{.Names}}"
        containers = run_and_check(
            f"docker container list --all --filter name='{filter_name}' --format '{format}'",
            shell=True,
        )
        containers = dict(
            line.split(":", 1) for line in containers.decode("utf8").splitlines()
        )
        return containers

    def copy_file_from_container_to_container(
        self, src_node, src_path, dst_node, dst_path
    ):
        fname = os.path.basename(src_path)
        run_and_check(
            [f"docker cp {src_node.docker_id}:{src_path} {self.instances_dir}"],
            shell=True,
        )
        run_and_check(
            [f"docker cp {self.instances_dir}/{fname} {dst_node.docker_id}:{dst_path}"],
            shell=True,
        )

    def setup_zookeeper_secure_cmd(
        self, instance, env_variables, docker_compose_yml_dir
    ):
        logging.debug("Setup ZooKeeper Secure")
        zookeeper_docker_compose_path = p.join(
            docker_compose_yml_dir, "docker_compose_zookeeper_secure.yml"
        )
        env_variables["ZOO_SECURE_CLIENT_PORT"] = str(self.zookeeper_secure_port)
        env_variables["ZK_FS"] = "bind"
        for i in range(1, 4):
            zk_data_path = os.path.join(
                self.zookeeper_instance_dir_prefix + str(i), "data"
            )
            zk_log_path = os.path.join(
                self.zookeeper_instance_dir_prefix + str(i), "log"
            )
            env_variables["ZK_DATA" + str(i)] = zk_data_path
            env_variables["ZK_DATA_LOG" + str(i)] = zk_log_path
            self.zookeeper_dirs_to_create += [zk_data_path, zk_log_path]
            logging.debug(f"DEBUG ZK: {self.zookeeper_dirs_to_create}")

        self.with_zookeeper_secure = True
        self.base_cmd.extend(["--file", zookeeper_docker_compose_path])
        self.base_zookeeper_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            zookeeper_docker_compose_path,
        ]
        return self.base_zookeeper_cmd

    def setup_zookeeper_cmd(self, instance, env_variables, docker_compose_yml_dir):
        logging.debug("Setup ZooKeeper")
        zookeeper_docker_compose_path = p.join(
            docker_compose_yml_dir, "docker_compose_zookeeper.yml"
        )

        env_variables["ZK_FS"] = "bind"
        for i in range(1, 4):
            zk_data_path = os.path.join(
                self.zookeeper_instance_dir_prefix + str(i), "data"
            )
            zk_log_path = os.path.join(
                self.zookeeper_instance_dir_prefix + str(i), "log"
            )
            env_variables["ZK_DATA" + str(i)] = zk_data_path
            env_variables["ZK_DATA_LOG" + str(i)] = zk_log_path
            self.zookeeper_dirs_to_create += [zk_data_path, zk_log_path]
            logging.debug(f"DEBUG ZK: {self.zookeeper_dirs_to_create}")

        self.with_zookeeper = True
        self.base_cmd.extend(["--file", zookeeper_docker_compose_path])
        self.base_zookeeper_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            zookeeper_docker_compose_path,
        ]
        return self.base_zookeeper_cmd

    def setup_keeper_cmd(self, instance, env_variables, docker_compose_yml_dir):
        logging.debug("Setup Keeper")
        keeper_docker_compose_path = p.join(
            docker_compose_yml_dir, "docker_compose_keeper.yml"
        )

        binary_path = self.server_bin_path
        if binary_path.endswith("-server"):
            binary_path = binary_path[: -len("-server")]

        env_variables["keeper_binary"] = binary_path
        env_variables["image"] = "altinityinfra/integration-test:" + self.docker_base_tag
        env_variables["user"] = str(os.getuid())
        env_variables["keeper_fs"] = "bind"
        for i in range(1, 4):
            keeper_instance_dir = self.keeper_instance_dir_prefix + f"{i}"
            logs_dir = os.path.join(keeper_instance_dir, "log")
            configs_dir = os.path.join(keeper_instance_dir, "config")
            coordination_dir = os.path.join(keeper_instance_dir, "coordination")
            env_variables[f"keeper_logs_dir{i}"] = logs_dir
            env_variables[f"keeper_config_dir{i}"] = configs_dir
            env_variables[f"keeper_db_dir{i}"] = coordination_dir
            self.zookeeper_dirs_to_create += [logs_dir, configs_dir, coordination_dir]
        logging.debug(f"DEBUG KEEPER: {self.zookeeper_dirs_to_create}")

        self.with_zookeeper = True
        self.base_cmd.extend(["--file", keeper_docker_compose_path])
        self.base_zookeeper_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            keeper_docker_compose_path,
        ]
        return self.base_zookeeper_cmd

    def setup_mysql_client_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_mysql_client = True
        self.base_cmd.extend(
            [
                "--file",
                p.join(docker_compose_yml_dir, "docker_compose_mysql_client.yml"),
            ]
        )
        self.base_mysql_client_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_mysql_client.yml"),
        ]

        return self.base_mysql_client_cmd

    def setup_mysql_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_mysql = True
        env_variables["MYSQL_HOST"] = self.mysql_host
        env_variables["MYSQL_PORT"] = str(self.mysql_port)
        env_variables["MYSQL_ROOT_HOST"] = "%"
        env_variables["MYSQL_LOGS"] = self.mysql_logs_dir
        env_variables["MYSQL_LOGS_FS"] = "bind"
        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_mysql.yml")]
        )
        self.base_mysql_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_mysql.yml"),
        ]

        return self.base_mysql_cmd

    def setup_mysql8_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_mysql8 = True
        env_variables["MYSQL8_HOST"] = self.mysql8_host
        env_variables["MYSQL8_PORT"] = str(self.mysql8_port)
        env_variables["MYSQL8_ROOT_HOST"] = "%"
        env_variables["MYSQL8_LOGS"] = self.mysql8_logs_dir
        env_variables["MYSQL8_LOGS_FS"] = "bind"
        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_mysql_8_0.yml")]
        )
        self.base_mysql8_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_mysql_8_0.yml"),
        ]

        return self.base_mysql8_cmd

    def setup_mysql_cluster_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_mysql_cluster = True
        env_variables["MYSQL_CLUSTER_PORT"] = str(self.mysql_port)
        env_variables["MYSQL_CLUSTER_ROOT_HOST"] = "%"
        env_variables["MYSQL_CLUSTER_LOGS"] = self.mysql_cluster_logs_dir
        env_variables["MYSQL_CLUSTER_LOGS_FS"] = "bind"

        self.base_cmd.extend(
            [
                "--file",
                p.join(docker_compose_yml_dir, "docker_compose_mysql_cluster.yml"),
            ]
        )
        self.base_mysql_cluster_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_mysql_cluster.yml"),
        ]

        return self.base_mysql_cluster_cmd

    def setup_postgres_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_postgres.yml")]
        )
        env_variables["POSTGRES_PORT"] = str(self.postgres_port)
        env_variables["POSTGRES_DIR"] = self.postgres_logs_dir
        env_variables["POSTGRES_LOGS_FS"] = "bind"

        self.with_postgres = True
        self.base_postgres_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_postgres.yml"),
        ]
        return self.base_postgres_cmd

    def setup_postgres_cluster_cmd(
        self, instance, env_variables, docker_compose_yml_dir
    ):
        self.with_postgres_cluster = True
        env_variables["POSTGRES_PORT"] = str(self.postgres_port)
        env_variables["POSTGRES2_DIR"] = self.postgres2_logs_dir
        env_variables["POSTGRES3_DIR"] = self.postgres3_logs_dir
        env_variables["POSTGRES4_DIR"] = self.postgres4_logs_dir
        env_variables["POSTGRES_LOGS_FS"] = "bind"
        self.base_cmd.extend(
            [
                "--file",
                p.join(docker_compose_yml_dir, "docker_compose_postgres_cluster.yml"),
            ]
        )
        self.base_postgres_cluster_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_postgres_cluster.yml"),
        ]

    def setup_hdfs_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_hdfs = True
        env_variables["HDFS_HOST"] = self.hdfs_host
        env_variables["HDFS_NAME_PORT"] = str(self.hdfs_name_port)
        env_variables["HDFS_DATA_PORT"] = str(self.hdfs_data_port)
        env_variables["HDFS_LOGS"] = self.hdfs_logs_dir
        env_variables["HDFS_FS"] = "bind"
        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_hdfs.yml")]
        )
        self.base_hdfs_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_hdfs.yml"),
        ]
        logging.debug("HDFS BASE CMD:{self.base_hdfs_cmd)}")
        return self.base_hdfs_cmd

    def setup_kerberized_hdfs_cmd(
        self, instance, env_variables, docker_compose_yml_dir
    ):
        self.with_kerberized_hdfs = True
        env_variables["KERBERIZED_HDFS_HOST"] = self.hdfs_kerberized_host
        env_variables["KERBERIZED_HDFS_NAME_PORT"] = str(self.hdfs_kerberized_name_port)
        env_variables["KERBERIZED_HDFS_DATA_PORT"] = str(self.hdfs_kerberized_data_port)
        env_variables["KERBERIZED_HDFS_LOGS"] = self.hdfs_kerberized_logs_dir
        env_variables["KERBERIZED_HDFS_FS"] = "bind"
        env_variables["KERBERIZED_HDFS_DIR"] = instance.path + "/"
        self.base_cmd.extend(
            [
                "--file",
                p.join(docker_compose_yml_dir, "docker_compose_kerberized_hdfs.yml"),
            ]
        )
        self.base_kerberized_hdfs_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_kerberized_hdfs.yml"),
        ]
        return self.base_kerberized_hdfs_cmd

    def setup_kafka_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_kafka = True
        env_variables["KAFKA_HOST"] = self.kafka_host
        env_variables["KAFKA_EXTERNAL_PORT"] = str(self.kafka_port)
        env_variables["SCHEMA_REGISTRY_EXTERNAL_PORT"] = str(self.schema_registry_port)
        env_variables["SCHEMA_REGISTRY_INTERNAL_PORT"] = "8081"
        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_kafka.yml")]
        )
        self.base_kafka_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_kafka.yml"),
        ]
        return self.base_kafka_cmd

    def setup_kerberized_kafka_cmd(
        self, instance, env_variables, docker_compose_yml_dir
    ):
        self.with_kerberized_kafka = True
        env_variables["KERBERIZED_KAFKA_DIR"] = instance.path + "/"
        env_variables["KERBERIZED_KAFKA_HOST"] = self.kerberized_kafka_host
        env_variables["KERBERIZED_KAFKA_EXTERNAL_PORT"] = str(
            self.kerberized_kafka_port
        )
        self.base_cmd.extend(
            [
                "--file",
                p.join(docker_compose_yml_dir, "docker_compose_kerberized_kafka.yml"),
            ]
        )
        self.base_kerberized_kafka_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_kerberized_kafka.yml"),
        ]
        return self.base_kerberized_kafka_cmd

    def setup_redis_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_redis = True
        env_variables["REDIS_HOST"] = self.redis_host
        env_variables["REDIS_EXTERNAL_PORT"] = str(self.redis_port)
        env_variables["REDIS_INTERNAL_PORT"] = "6379"

        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_redis.yml")]
        )
        self.base_redis_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_redis.yml"),
        ]
        return self.base_redis_cmd

    def setup_rabbitmq_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_rabbitmq = True
        env_variables["RABBITMQ_HOST"] = self.rabbitmq_host
        env_variables["RABBITMQ_PORT"] = str(self.rabbitmq_port)
        env_variables["RABBITMQ_LOGS"] = self.rabbitmq_logs_dir
        env_variables["RABBITMQ_LOGS_FS"] = "bind"

        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_rabbitmq.yml")]
        )
        self.base_rabbitmq_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_rabbitmq.yml"),
        ]
        return self.base_rabbitmq_cmd

    def setup_mongo_secure_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_mongo = self.with_mongo_secure = True
        env_variables["MONGO_HOST"] = self.mongo_host
        env_variables["MONGO_EXTERNAL_PORT"] = str(self.mongo_port)
        env_variables["MONGO_INTERNAL_PORT"] = "27017"
        env_variables["MONGO_CONFIG_PATH"] = HELPERS_DIR
        self.base_cmd.extend(
            [
                "--file",
                p.join(docker_compose_yml_dir, "docker_compose_mongo_secure.yml"),
            ]
        )
        self.base_mongo_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_mongo_secure.yml"),
        ]
        return self.base_mongo_cmd

    def setup_mongo_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_mongo = True
        env_variables["MONGO_HOST"] = self.mongo_host
        env_variables["MONGO_EXTERNAL_PORT"] = str(self.mongo_port)
        env_variables["MONGO_INTERNAL_PORT"] = "27017"
        env_variables["MONGO_NO_CRED_EXTERNAL_PORT"] = str(self.mongo_no_cred_port)
        env_variables["MONGO_NO_CRED_INTERNAL_PORT"] = "27017"
        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_mongo.yml")]
        )
        self.base_mongo_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_mongo.yml"),
        ]
        return self.base_mongo_cmd

    def setup_minio_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_minio = True
        cert_d = p.join(self.minio_dir, "certs")
        env_variables["MINIO_CERTS_DIR"] = cert_d
        env_variables["MINIO_PORT"] = str(self.minio_port)
        env_variables["SSL_CERT_FILE"] = p.join(self.base_dir, cert_d, "public.crt")

        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_minio.yml")]
        )
        self.base_minio_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_minio.yml"),
        ]
        return self.base_minio_cmd

    def setup_azurite_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_azurite = True
        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_azurite.yml")]
        )
        self.base_azurite_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_azurite.yml"),
        ]
        return self.base_azurite_cmd

    def setup_cassandra_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_cassandra = True
        env_variables["CASSANDRA_PORT"] = str(self.cassandra_port)
        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_cassandra.yml")]
        )
        self.base_cassandra_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_cassandra.yml"),
        ]
        return self.base_cassandra_cmd

    def setup_jdbc_bridge_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_jdbc_bridge = True
        env_variables["JDBC_DRIVER_LOGS"] = self.jdbc_driver_logs_dir
        env_variables["JDBC_DRIVER_FS"] = "bind"
        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_jdbc_bridge.yml")]
        )
        self.base_jdbc_bridge_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_jdbc_bridge.yml"),
        ]
        return self.base_jdbc_bridge_cmd

    def setup_nginx_cmd(self, instance, env_variables, docker_compose_yml_dir):
        self.with_nginx = True

        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_nginx.yml")]
        )
        self.base_nginx_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_nginx.yml"),
        ]
        return self.base_nginx_cmd

    def setup_hive(self, instance, env_variables, docker_compose_yml_dir):
        self.with_hive = True
        self.base_cmd.extend(
            ["--file", p.join(docker_compose_yml_dir, "docker_compose_hive.yml")]
        )
        self.base_hive_cmd = [
            "docker-compose",
            "--env-file",
            instance.env_file,
            "--project-name",
            self.project_name,
            "--file",
            p.join(docker_compose_yml_dir, "docker_compose_hive.yml"),
        ]
        return self.base_hive_cmd

    def add_instance(
        self,
        name,
        base_config_dir=None,
        main_configs=None,
        user_configs=None,
        dictionaries=None,
        macros=None,
        with_zookeeper=False,
        with_zookeeper_secure=False,
        with_mysql_client=False,
        with_mysql=False,
        with_mysql8=False,
        with_mysql_cluster=False,
        with_kafka=False,
        with_kerberized_kafka=False,
        with_rabbitmq=False,
        clickhouse_path_dir=None,
        with_odbc_drivers=False,
        with_postgres=False,
        with_postgres_cluster=False,
        with_hdfs=False,
        with_kerberized_hdfs=False,
        with_mongo=False,
        with_mongo_secure=False,
        with_nginx=False,
        with_redis=False,
        with_minio=False,
        with_azurite=False,
        with_cassandra=False,
        with_jdbc_bridge=False,
        with_hive=False,
        hostname=None,
        env_variables=None,
        image="altinityinfra/integration-test",
        tag=None,
        stay_alive=False,
        ipv4_address=None,
        ipv6_address=None,
        with_installed_binary=False,
        external_dirs=None,
        tmpfs=None,
        zookeeper_docker_compose_path=None,
        minio_certs_dir=None,
        use_keeper=True,
        main_config_name="config.xml",
        users_config_name="users.xml",
        copy_common_configs=True,
        config_root_name="clickhouse",
        extra_configs=[],
    ) -> "ClickHouseInstance":

        """Add an instance to the cluster.

        name - the name of the instance directory and the value of the 'instance' macro in ClickHouse.
        base_config_dir - a directory with config.xml and users.xml files which will be copied to /etc/clickhouse-server/ directory
        main_configs - a list of config files that will be added to config.d/ directory
        user_configs - a list of config files that will be added to users.d/ directory
        with_zookeeper - if True, add ZooKeeper configuration to configs and ZooKeeper instances to the cluster.
        with_zookeeper_secure - if True, add ZooKeeper Secure configuration to configs and ZooKeeper instances to the cluster.
        extra_configs - config files cannot put into config.d and users.d
        """

        if self.is_up:
            raise Exception("Can't add instance %s: cluster is already up!" % name)

        if name in self.instances:
            raise Exception(
                "Can't add instance `%s': there is already an instance with the same name!"
                % name
            )

        if tag is None:
            tag = self.docker_base_tag
        if not env_variables:
            env_variables = {}

        self.use_keeper = use_keeper

        # Code coverage files will be placed in database directory
        # (affect only WITH_COVERAGE=1 build)
        env_variables[
            "LLVM_PROFILE_FILE"
        ] = "/var/lib/clickhouse/server_%h_%p_%m.profraw"

        instance = ClickHouseInstance(
            cluster=self,
            base_path=self.base_dir,
            name=name,
            base_config_dir=base_config_dir
            if base_config_dir
            else self.base_config_dir,
            custom_main_configs=main_configs or [],
            custom_user_configs=user_configs or [],
            custom_dictionaries=dictionaries or [],
            macros=macros or {},
            with_zookeeper=with_zookeeper,
            zookeeper_config_path=self.zookeeper_config_path,
            with_mysql_client=with_mysql_client,
            with_mysql=with_mysql,
            with_mysql8=with_mysql8,
            with_mysql_cluster=with_mysql_cluster,
            with_kafka=with_kafka,
            with_kerberized_kafka=with_kerberized_kafka,
            with_rabbitmq=with_rabbitmq,
            with_nginx=with_nginx,
            with_kerberized_hdfs=with_kerberized_hdfs,
            with_mongo=with_mongo or with_mongo_secure,
            with_redis=with_redis,
            with_minio=with_minio,
            with_azurite=with_azurite,
            with_cassandra=with_cassandra,
            with_jdbc_bridge=with_jdbc_bridge,
            with_hive=with_hive,
            server_bin_path=self.server_bin_path,
            odbc_bridge_bin_path=self.odbc_bridge_bin_path,
            library_bridge_bin_path=self.library_bridge_bin_path,
            clickhouse_path_dir=clickhouse_path_dir,
            with_odbc_drivers=with_odbc_drivers,
            with_postgres=with_postgres,
            with_postgres_cluster=with_postgres_cluster,
            hostname=hostname,
            env_variables=env_variables,
            image=image,
            tag=tag,
            stay_alive=stay_alive,
            ipv4_address=ipv4_address,
            ipv6_address=ipv6_address,
            with_installed_binary=with_installed_binary,
            main_config_name=main_config_name,
            users_config_name=users_config_name,
            copy_common_configs=copy_common_configs,
            external_dirs=external_dirs,
            tmpfs=tmpfs or [],
            config_root_name=config_root_name,
            extra_configs=extra_configs,
        )

        docker_compose_yml_dir = get_docker_compose_path()

        self.instances[name] = instance
        if ipv4_address is not None or ipv6_address is not None:
            self.with_net_trics = True
            self.base_cmd.extend(
                ["--file", p.join(docker_compose_yml_dir, "docker_compose_net.yml")]
            )

        self.base_cmd.extend(["--file", instance.docker_compose_path])

        cmds = []
        if with_zookeeper_secure and not self.with_zookeeper_secure:
            cmds.append(
                self.setup_zookeeper_secure_cmd(
                    instance, env_variables, docker_compose_yml_dir
                )
            )

        if with_zookeeper and not self.with_zookeeper:
            if self.use_keeper:
                cmds.append(
                    self.setup_keeper_cmd(
                        instance, env_variables, docker_compose_yml_dir
                    )
                )
            else:
                cmds.append(
                    self.setup_zookeeper_cmd(
                        instance, env_variables, docker_compose_yml_dir
                    )
                )

        if with_mysql_client and not self.with_mysql_client:
            cmds.append(
                self.setup_mysql_client_cmd(
                    instance, env_variables, docker_compose_yml_dir
                )
            )

        if with_mysql and not self.with_mysql:
            cmds.append(
                self.setup_mysql_cmd(instance, env_variables, docker_compose_yml_dir)
            )

        if with_mysql8 and not self.with_mysql8:
            cmds.append(
                self.setup_mysql8_cmd(instance, env_variables, docker_compose_yml_dir)
            )

        if with_mysql_cluster and not self.with_mysql_cluster:
            cmds.append(
                self.setup_mysql_cluster_cmd(
                    instance, env_variables, docker_compose_yml_dir
                )
            )

        if with_postgres and not self.with_postgres:
            cmds.append(
                self.setup_postgres_cmd(instance, env_variables, docker_compose_yml_dir)
            )

        if with_postgres_cluster and not self.with_postgres_cluster:
            cmds.append(
                self.setup_postgres_cluster_cmd(
                    instance, env_variables, docker_compose_yml_dir
                )
            )

        if with_odbc_drivers and not self.with_odbc_drivers:
            self.with_odbc_drivers = True
            if not self.with_mysql:
                cmds.append(
                    self.setup_mysql_cmd(
                        instance, env_variables, docker_compose_yml_dir
                    )
                )

            if not self.with_postgres:
                cmds.append(
                    self.setup_postgres_cmd(
                        instance, env_variables, docker_compose_yml_dir
                    )
                )

        if with_kafka and not self.with_kafka:
            cmds.append(
                self.setup_kafka_cmd(instance, env_variables, docker_compose_yml_dir)
            )

        if with_kerberized_kafka and not self.with_kerberized_kafka:
            cmds.append(
                self.setup_kerberized_kafka_cmd(
                    instance, env_variables, docker_compose_yml_dir
                )
            )

        if with_rabbitmq and not self.with_rabbitmq:
            cmds.append(
                self.setup_rabbitmq_cmd(instance, env_variables, docker_compose_yml_dir)
            )

        if with_nginx and not self.with_nginx:
            cmds.append(
                self.setup_nginx_cmd(instance, env_variables, docker_compose_yml_dir)
            )

        if with_hdfs and not self.with_hdfs:
            cmds.append(
                self.setup_hdfs_cmd(instance, env_variables, docker_compose_yml_dir)
            )

        if with_kerberized_hdfs and not self.with_kerberized_hdfs:
            cmds.append(
                self.setup_kerberized_hdfs_cmd(
                    instance, env_variables, docker_compose_yml_dir
                )
            )

        if (with_mongo or with_mongo_secure) and not (
            self.with_mongo or self.with_mongo_secure
        ):
            if with_mongo_secure:
                cmds.append(
                    self.setup_mongo_secure_cmd(
                        instance, env_variables, docker_compose_yml_dir
                    )
                )
            else:
                cmds.append(
                    self.setup_mongo_cmd(
                        instance, env_variables, docker_compose_yml_dir
                    )
                )

        if self.with_net_trics:
            for cmd in cmds:
                cmd.extend(
                    ["--file", p.join(docker_compose_yml_dir, "docker_compose_net.yml")]
                )

        if with_redis and not self.with_redis:
            cmds.append(
                self.setup_redis_cmd(instance, env_variables, docker_compose_yml_dir)
            )

        if with_minio and not self.with_minio:
            cmds.append(
                self.setup_minio_cmd(instance, env_variables, docker_compose_yml_dir)
            )

        if with_azurite and not self.with_azurite:
            cmds.append(
                self.setup_azurite_cmd(instance, env_variables, docker_compose_yml_dir)
            )

        if minio_certs_dir is not None:
            if self.minio_certs_dir is None:
                self.minio_certs_dir = minio_certs_dir
            else:
                raise Exception("Overwriting minio certs dir")

        if with_cassandra and not self.with_cassandra:
            cmds.append(
                self.setup_cassandra_cmd(
                    instance, env_variables, docker_compose_yml_dir
                )
            )

        if with_jdbc_bridge and not self.with_jdbc_bridge:
            cmds.append(
                self.setup_jdbc_bridge_cmd(
                    instance, env_variables, docker_compose_yml_dir
                )
            )

        if with_hive:
            cmds.append(
                self.setup_hive(instance, env_variables, docker_compose_yml_dir)
            )

        logging.debug(
            "Cluster name:{} project_name:{}. Added instance name:{} tag:{} base_cmd:{} docker_compose_yml_dir:{}".format(
                self.name,
                self.project_name,
                name,
                tag,
                self.base_cmd,
                docker_compose_yml_dir,
            )
        )
        return instance

    def get_instance_docker_id(self, instance_name):
        # According to how docker-compose names containers.
        return self.project_name + "_" + instance_name + "_1"

    def _replace(self, path, what, to):
        with open(path, "r") as p:
            data = p.read()
        data = data.replace(what, to)
        with open(path, "w") as p:
            p.write(data)

    def restart_instance_with_ip_change(self, node, new_ip):
        if "::" in new_ip:
            if node.ipv6_address is None:
                raise Exception("You should specity ipv6_address in add_node method")
            self._replace(node.docker_compose_path, node.ipv6_address, new_ip)
            node.ipv6_address = new_ip
        else:
            if node.ipv4_address is None:
                raise Exception("You should specity ipv4_address in add_node method")
            self._replace(node.docker_compose_path, node.ipv4_address, new_ip)
            node.ipv4_address = new_ip
        run_and_check(self.base_cmd + ["stop", node.name])
        run_and_check(self.base_cmd + ["rm", "--force", "--stop", node.name])
        run_and_check(
            self.base_cmd + ["up", "--force-recreate", "--no-deps", "-d", node.name]
        )
        node.ip_address = self.get_instance_ip(node.name)
        node.client = Client(node.ip_address, command=self.client_bin_path)

        logging.info("Restart node with ip change")
        # In builds with sanitizer the server can take a long time to start
        node.wait_for_start(start_timeout=180.0, connection_timeout=600.0)  # seconds
        res = node.client.query("SELECT 30")
        logging.debug(f"Read '{res}'")
        assert "30\n" == res
        logging.info("Restarted")

        return node

    def restart_service(self, service_name):
        run_and_check(self.base_cmd + ["restart", service_name])

    def get_instance_ip(self, instance_name):
        logging.debug("get_instance_ip instance_name={}".format(instance_name))
        docker_id = self.get_instance_docker_id(instance_name)
        # for cont in self.docker_client.containers.list():
        # logging.debug("CONTAINERS LIST: ID={} NAME={} STATUS={}".format(cont.id, cont.name, cont.status))
        handle = self.docker_client.containers.get(docker_id)
        return list(handle.attrs["NetworkSettings"]["Networks"].values())[0][
            "IPAddress"
        ]

    def get_container_id(self, instance_name):
        return self.get_instance_docker_id(instance_name)
        # docker_id = self.get_instance_docker_id(instance_name)
        # handle = self.docker_client.containers.get(docker_id)
        # return handle.attrs['Id']

    def get_container_logs(self, instance_name):
        container_id = self.get_container_id(instance_name)
        return self.docker_client.api.logs(container_id).decode()

    def exec_in_container(
        self, container_id, cmd, detach=False, nothrow=False, use_cli=True, **kwargs
    ):
        if use_cli:
            logging.debug(
                f"run container_id:{container_id} detach:{detach} nothrow:{nothrow} cmd: {cmd}"
            )
            exec_cmd = ["docker", "exec"]
            if "user" in kwargs:
                exec_cmd += ["-u", kwargs["user"]]
            result = subprocess_check_call(
                exec_cmd + [container_id] + cmd, detach=detach, nothrow=nothrow
            )
            return result
        else:
            exec_id = self.docker_client.api.exec_create(container_id, cmd, **kwargs)
            output = self.docker_client.api.exec_start(exec_id, detach=detach)

            exit_code = self.docker_client.api.exec_inspect(exec_id)["ExitCode"]
            if exit_code:
                container_info = self.docker_client.api.inspect_container(container_id)
                image_id = container_info.get("Image")
                image_info = self.docker_client.api.inspect_image(image_id)
                logging.debug(("Command failed in container {}: ".format(container_id)))
                pprint.pprint(container_info)
                logging.debug("")
                logging.debug(
                    ("Container {} uses image {}: ".format(container_id, image_id))
                )
                pprint.pprint(image_info)
                logging.debug("")
                message = 'Cmd "{}" failed in container {}. Return code {}. Output: {}'.format(
                    " ".join(cmd), container_id, exit_code, output
                )
                if nothrow:
                    logging.debug(message)
                else:
                    raise Exception(message)
            if not detach:
                return output.decode()
            return output

    def copy_file_to_container(self, container_id, local_path, dest_path):
        with open(local_path, "r") as fdata:
            data = fdata.read()
            encodedBytes = base64.b64encode(data.encode("utf-8"))
            encodedStr = str(encodedBytes, "utf-8")
            self.exec_in_container(
                container_id,
                [
                    "bash",
                    "-c",
                    "echo {} | base64 --decode > {}".format(encodedStr, dest_path),
                ],
                user="root",
            )

    def wait_for_url(
        self, url="http://localhost:8123/ping", conn_timeout=2, interval=2, timeout=60
    ):
        if not url.startswith("http"):
            url = "http://" + url
        if interval <= 0:
            interval = 2
        if timeout <= 0:
            timeout = 60

        attempts = 1
        errors = []
        start = time.time()
        while time.time() - start < timeout:
            try:
                requests.get(
                    url, allow_redirects=True, timeout=conn_timeout, verify=False
                ).raise_for_status()
                logging.debug(
                    "{} is available after {} seconds".format(url, time.time() - start)
                )
                return
            except Exception as ex:
                logging.debug(
                    "{} Attempt {} failed, retrying in {} seconds".format(
                        ex, attempts, interval
                    )
                )
                attempts += 1
                errors += [str(ex)]
                time.sleep(interval)

        run_and_check(["docker", "ps", "--all"])
        logging.error("Can't connect to URL:{}".format(errors))
        raise Exception(
            "Cannot wait URL {}(interval={}, timeout={}, attempts={})".format(
                url, interval, timeout, attempts
            )
        )

    def wait_mysql_client_to_start(self, timeout=180):
        start = time.time()
        errors = []
        self.mysql_client_container = self.get_docker_handle(
            self.get_instance_docker_id(self.mysql_client_host)
        )

        while time.time() - start < timeout:
            try:
                info = self.mysql_client_container.client.api.inspect_container(
                    self.mysql_client_container.name
                )
                if info["State"]["Health"]["Status"] == "healthy":
                    logging.debug("Mysql Client Container Started")
                    return
                time.sleep(1)
            except Exception as ex:
                errors += [str(ex)]
                time.sleep(1)

        run_and_check(["docker", "ps", "--all"])
        logging.error("Can't connect to MySQL Client:{}".format(errors))
        raise Exception("Cannot wait MySQL Client container")

    def wait_mysql_to_start(self, timeout=180):
        self.mysql_ip = self.get_instance_ip("mysql57")
        start = time.time()
        errors = []
        while time.time() - start < timeout:
            try:
                conn = pymysql.connect(
                    user="root",
                    password="clickhouse",
                    host=self.mysql_ip,
                    port=self.mysql_port,
                )
                conn.close()
                logging.debug("Mysql Started")
                return
            except Exception as ex:
                errors += [str(ex)]
                time.sleep(0.5)

        run_and_check(["docker-compose", "ps", "--services", "--all"])
        logging.error("Can't connect to MySQL:{}".format(errors))
        raise Exception("Cannot wait MySQL container")

    def wait_mysql8_to_start(self, timeout=180):
        self.mysql8_ip = self.get_instance_ip("mysql80")
        start = time.time()
        while time.time() - start < timeout:
            try:
                conn = pymysql.connect(
                    user="root",
                    password="clickhouse",
                    host=self.mysql8_ip,
                    port=self.mysql8_port,
                )
                conn.close()
                logging.debug("Mysql 8 Started")
                return
            except Exception as ex:
                logging.debug("Can't connect to MySQL 8 " + str(ex))
                time.sleep(0.5)

        run_and_check(["docker-compose", "ps", "--services", "--all"])
        raise Exception("Cannot wait MySQL 8 container")

    def wait_mysql_cluster_to_start(self, timeout=180):
        self.mysql2_ip = self.get_instance_ip(self.mysql2_host)
        self.mysql3_ip = self.get_instance_ip(self.mysql3_host)
        self.mysql4_ip = self.get_instance_ip(self.mysql4_host)
        start = time.time()
        errors = []
        while time.time() - start < timeout:
            try:
                for ip in [self.mysql2_ip, self.mysql3_ip, self.mysql4_ip]:
                    conn = pymysql.connect(
                        user="root",
                        password="clickhouse",
                        host=ip,
                        port=self.mysql_port,
                    )
                    conn.close()
                    logging.debug(f"Mysql Started {ip}")
                return
            except Exception as ex:
                errors += [str(ex)]
                time.sleep(0.5)

        run_and_check(["docker-compose", "ps", "--services", "--all"])
        logging.error("Can't connect to MySQL:{}".format(errors))
        raise Exception("Cannot wait MySQL container")

    def wait_postgres_to_start(self, timeout=260):
        self.postgres_ip = self.get_instance_ip(self.postgres_host)
        start = time.time()
        while time.time() - start < timeout:
            try:
                self.postgres_conn = psycopg2.connect(
                    host=self.postgres_ip,
                    port=self.postgres_port,
                    database="postgres",
                    user="postgres",
                    password="mysecretpassword",
                )
                self.postgres_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                self.postgres_conn.autocommit = True
                logging.debug("Postgres Started")
                return
            except Exception as ex:
                logging.debug("Can't connect to Postgres " + str(ex))
                time.sleep(0.5)

        raise Exception("Cannot wait Postgres container")

    def wait_postgres_cluster_to_start(self, timeout=180):
        self.postgres2_ip = self.get_instance_ip(self.postgres2_host)
        self.postgres3_ip = self.get_instance_ip(self.postgres3_host)
        self.postgres4_ip = self.get_instance_ip(self.postgres4_host)
        start = time.time()
        while time.time() - start < timeout:
            try:
                self.postgres2_conn = psycopg2.connect(
                    host=self.postgres2_ip,
                    port=self.postgres_port,
                    database="postgres",
                    user="postgres",
                    password="mysecretpassword",
                )
                self.postgres2_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                self.postgres2_conn.autocommit = True
                logging.debug("Postgres Cluster host 2 started")
                break
            except Exception as ex:
                logging.debug("Can't connect to Postgres host 2" + str(ex))
                time.sleep(0.5)
        while time.time() - start < timeout:
            try:
                self.postgres3_conn = psycopg2.connect(
                    host=self.postgres3_ip,
                    port=self.postgres_port,
                    database="postgres",
                    user="postgres",
                    password="mysecretpassword",
                )
                self.postgres3_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                self.postgres3_conn.autocommit = True
                logging.debug("Postgres Cluster host 3 started")
                break
            except Exception as ex:
                logging.debug("Can't connect to Postgres host 3" + str(ex))
                time.sleep(0.5)
        while time.time() - start < timeout:
            try:
                self.postgres4_conn = psycopg2.connect(
                    host=self.postgres4_ip,
                    port=self.postgres_port,
                    database="postgres",
                    user="postgres",
                    password="mysecretpassword",
                )
                self.postgres4_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                self.postgres4_conn.autocommit = True
                logging.debug("Postgres Cluster host 4 started")
                return
            except Exception as ex:
                logging.debug("Can't connect to Postgres host 4" + str(ex))
                time.sleep(0.5)

        raise Exception("Cannot wait Postgres container")

    def wait_rabbitmq_to_start(self, timeout=180, throw=True):
        self.rabbitmq_ip = self.get_instance_ip(self.rabbitmq_host)

        start = time.time()
        while time.time() - start < timeout:
            try:
                if check_rabbitmq_is_available(self.rabbitmq_docker_id):
                    logging.debug("RabbitMQ is available")
                    if enable_consistent_hash_plugin(self.rabbitmq_docker_id):
                        logging.debug("RabbitMQ consistent hash plugin is available")
                        return True
                time.sleep(0.5)
            except Exception as ex:
                logging.debug("Can't connect to RabbitMQ " + str(ex))
                time.sleep(0.5)

        if throw:
            raise Exception("Cannot wait RabbitMQ container")
        return False

    def wait_nginx_to_start(self, timeout=60):
        self.nginx_ip = self.get_instance_ip(self.nginx_host)
        start = time.time()
        while time.time() - start < timeout:
            try:
                self.exec_in_container(
                    self.nginx_id,
                    ["curl", "-X", "PUT", "-d", "Test", "http://test.com/test.txt"],
                )
                res = self.exec_in_container(
                    self.nginx_id, ["curl", "-X", "GET", "http://test.com/test.txt"]
                )
                assert res == "Test"
                print("nginx static files server is available")
                return
            except Exception as ex:
                print("Can't connect to nginx: " + str(ex))
                time.sleep(0.5)

    def wait_zookeeper_secure_to_start(self, timeout=20):
        logging.debug("Wait ZooKeeper Secure to start")
        start = time.time()
        while time.time() - start < timeout:
            try:
                for instance in ["zoo1", "zoo2", "zoo3"]:
                    conn = self.get_kazoo_client(instance)
                    conn.get_children("/")
                    conn.stop()
                logging.debug("All instances of ZooKeeper Secure started")
                return
            except Exception as ex:
                logging.debug("Can't connect to ZooKeeper secure " + str(ex))
                time.sleep(0.5)

        raise Exception("Cannot wait ZooKeeper secure container")

    def wait_zookeeper_to_start(self, timeout=180):
        logging.debug("Wait ZooKeeper to start")
        start = time.time()
        while time.time() - start < timeout:
            try:
                for instance in ["zoo1", "zoo2", "zoo3"]:
                    conn = self.get_kazoo_client(instance)
                    conn.get_children("/")
                    conn.stop()
                logging.debug("All instances of ZooKeeper started")
                return
            except Exception as ex:
                logging.debug("Can't connect to ZooKeeper " + str(ex))
                time.sleep(0.5)

        raise Exception("Cannot wait ZooKeeper container")

    def make_hdfs_api(self, timeout=180, kerberized=False):
        if kerberized:
            keytab = p.abspath(
                p.join(self.instances["node1"].path, "secrets/clickhouse.keytab")
            )
            krb_conf = p.abspath(
                p.join(self.instances["node1"].path, "secrets/krb_long.conf")
            )
            self.hdfs_kerberized_ip = self.get_instance_ip(self.hdfs_kerberized_host)
            kdc_ip = self.get_instance_ip("hdfskerberos")

            self.hdfs_api = HDFSApi(
                user="root",
                timeout=timeout,
                kerberized=True,
                principal="root@TEST.CLICKHOUSE.TECH",
                keytab=keytab,
                krb_conf=krb_conf,
                host=self.hdfs_kerberized_host,
                protocol="http",
                proxy_port=self.hdfs_kerberized_name_port,
                data_port=self.hdfs_kerberized_data_port,
                hdfs_ip=self.hdfs_kerberized_ip,
                kdc_ip=kdc_ip,
            )
        else:
            self.hdfs_ip = self.get_instance_ip(self.hdfs_host)
            self.hdfs_api = HDFSApi(
                user="root",
                host=self.hdfs_host,
                data_port=self.hdfs_data_port,
                proxy_port=self.hdfs_name_port,
                hdfs_ip=self.hdfs_ip,
            )

    def wait_kafka_is_available(self, kafka_docker_id, kafka_port, max_retries=50):
        retries = 0
        while True:
            if check_kafka_is_available(kafka_docker_id, kafka_port):
                break
            else:
                retries += 1
                if retries > max_retries:
                    raise Exception("Kafka is not available")
                logging.debug("Waiting for Kafka to start up")
                time.sleep(1)

    def wait_hdfs_to_start(self, timeout=300, check_marker=False):
        start = time.time()
        while time.time() - start < timeout:
            try:
                self.hdfs_api.write_data("/somefilewithrandomname222", "1")
                logging.debug("Connected to HDFS and SafeMode disabled! ")
                if check_marker:
                    self.hdfs_api.read_data("/preparations_done_marker")

                return
            except Exception as ex:
                logging.exception(
                    "Can't connect to HDFS or preparations are not done yet " + str(ex)
                )
                time.sleep(1)

        raise Exception("Can't wait HDFS to start")

    def wait_mongo_to_start(self, timeout=30, secure=False):
        connection_str = "mongodb://{user}:{password}@{host}:{port}".format(
            host="localhost", port=self.mongo_port, user="root", password="clickhouse"
        )
        if secure:
            connection_str += "/?tls=true&tlsAllowInvalidCertificates=true"
        connection = pymongo.MongoClient(connection_str)
        start = time.time()
        while time.time() - start < timeout:
            try:
                connection.list_database_names()
                logging.debug(f"Connected to Mongo dbs: {connection.database_names()}")
                return
            except Exception as ex:
                logging.debug("Can't connect to Mongo " + str(ex))
                time.sleep(1)

    def wait_minio_to_start(self, timeout=180, secure=False):
        self.minio_ip = self.get_instance_ip(self.minio_host)
        self.minio_redirect_ip = self.get_instance_ip(self.minio_redirect_host)

        os.environ["SSL_CERT_FILE"] = p.join(
            self.base_dir, self.minio_dir, "certs", "public.crt"
        )
        minio_client = Minio(
            f"{self.minio_ip}:{self.minio_port}",
            access_key="minio",
            secret_key="minio123",
            secure=secure,
            http_client=urllib3.PoolManager(cert_reqs="CERT_NONE"),
        )  # disable SSL check as we test ClickHouse and not Python library
        start = time.time()
        while time.time() - start < timeout:
            try:
                minio_client.list_buckets()

                logging.debug("Connected to Minio.")

                buckets = [self.minio_bucket, self.minio_bucket_2]

                for bucket in buckets:
                    if minio_client.bucket_exists(bucket):
                        delete_object_list = map(
                            lambda x: x.object_name,
                            minio_client.list_objects_v2(bucket, recursive=True),
                        )
                        errors = minio_client.remove_objects(bucket, delete_object_list)
                        for error in errors:
                            logging.error(f"Error occured when deleting object {error}")
                        minio_client.remove_bucket(bucket)
                    minio_client.make_bucket(bucket)
                    logging.debug("S3 bucket '%s' created", bucket)

                self.minio_client = minio_client
                return
            except Exception as ex:
                logging.debug("Can't connect to Minio: %s", str(ex))
                time.sleep(1)

        raise Exception("Can't wait Minio to start")

    def wait_azurite_to_start(self, timeout=180):
        from azure.storage.blob import BlobServiceClient

        connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
        time.sleep(1)
        start = time.time()
        while time.time() - start < timeout:
            try:
                blob_service_client = BlobServiceClient.from_connection_string(
                    connection_string
                )
                logging.debug(blob_service_client.get_account_information())
                self.blob_service_client = blob_service_client
                return
            except Exception as ex:
                logging.debug("Can't connect to Azurite: %s", str(ex))
                time.sleep(1)

        raise Exception("Can't wait Azurite to start")

    def wait_schema_registry_to_start(self, timeout=180):
        sr_client = CachedSchemaRegistryClient(
            {"url": "http://localhost:{}".format(self.schema_registry_port)}
        )
        start = time.time()
        while time.time() - start < timeout:
            try:
                sr_client._send_request(sr_client.url)
                logging.debug("Connected to SchemaRegistry")
                return sr_client
            except Exception as ex:
                logging.debug(("Can't connect to SchemaRegistry: %s", str(ex)))
                time.sleep(1)

        raise Exception("Can't wait Schema Registry to start")

    def wait_cassandra_to_start(self, timeout=180):
        self.cassandra_ip = self.get_instance_ip(self.cassandra_host)
        cass_client = cassandra.cluster.Cluster(
            [self.cassandra_ip],
            port=self.cassandra_port,
            load_balancing_policy=RoundRobinPolicy(),
        )
        start = time.time()
        while time.time() - start < timeout:
            try:
                logging.info(
                    f"Check Cassandra Online {self.cassandra_id} {self.cassandra_ip} {self.cassandra_port}"
                )
                check = self.exec_in_container(
                    self.cassandra_id,
                    [
                        "bash",
                        "-c",
                        f"/opt/cassandra/bin/cqlsh -u cassandra -p cassandra -e 'describe keyspaces' {self.cassandra_ip} {self.cassandra_port}",
                    ],
                    user="root",
                )
                logging.info("Cassandra Online")
                cass_client.connect()
                logging.info("Connected Clients to Cassandra")
                return
            except Exception as ex:
                logging.warning("Can't connect to Cassandra: %s", str(ex))
                time.sleep(1)

        raise Exception("Can't wait Cassandra to start")

    def start(self, destroy_dirs=True):
        pytest_xdist_logging_to_separate_files.setup()
        logging.info("Running tests in {}".format(self.base_path))

        logging.debug(
            "Cluster start called. is_up={}, destroy_dirs={}".format(
                self.is_up, destroy_dirs
            )
        )
        if self.is_up:
            return

        try:
            self.cleanup()
        except Exception as e:
            logging.warning("Cleanup failed:{e}")

        try:
            # clickhouse_pull_cmd = self.base_cmd + ['pull']
            # print(f"Pulling images for {self.base_cmd}")
            # retry_exception(10, 5, subprocess_check_call, Exception, clickhouse_pull_cmd)

            if destroy_dirs and p.exists(self.instances_dir):
                logging.debug(f"Removing instances dir {self.instances_dir}")
                shutil.rmtree(self.instances_dir)

            for instance in list(self.instances.values()):
                logging.debug(
                    (
                        "Setup directory for instance: {} destroy_dirs: {}".format(
                            instance.name, destroy_dirs
                        )
                    )
                )
                instance.create_dir(destroy_dir=destroy_dirs)

            _create_env_file(os.path.join(self.env_file), self.env_variables)
            self.docker_client = docker.DockerClient(
                base_url="unix:///var/run/docker.sock",
                version=self.docker_api_version,
                timeout=600,
            )

            common_opts = ["--verbose", "up", "-d"]

            images_pull_cmd = self.base_cmd + ["pull"]
            # sometimes dockerhub/proxy can be flaky
            for i in range(5):
                try:
                    run_and_check(images_pull_cmd)
                    break
                except Exception as ex:
                    if i == 4:
                        raise ex
                    logging.info("Got exception pulling images: %s", ex)
                    time.sleep(i * 3)

            if self.with_zookeeper_secure and self.base_zookeeper_cmd:
                logging.debug("Setup ZooKeeper Secure")
                logging.debug(
                    f"Creating internal ZooKeeper dirs: {self.zookeeper_dirs_to_create}"
                )
                for i in range(1, 3):
                    if os.path.exists(self.zookeeper_instance_dir_prefix + f"{i}"):
                        shutil.rmtree(self.zookeeper_instance_dir_prefix + f"{i}")
                for dir in self.zookeeper_dirs_to_create:
                    os.makedirs(dir)
                run_and_check(self.base_zookeeper_cmd + common_opts, env=self.env)
                self.up_called = True

                self.wait_zookeeper_secure_to_start()
                for command in self.pre_zookeeper_commands:
                    self.run_kazoo_commands_with_retries(command, repeats=5)

            if self.with_zookeeper and self.base_zookeeper_cmd:
                logging.debug("Setup ZooKeeper")
                logging.debug(
                    f"Creating internal ZooKeeper dirs: {self.zookeeper_dirs_to_create}"
                )
                if self.use_keeper:
                    for i in range(1, 4):
                        if os.path.exists(self.keeper_instance_dir_prefix + f"{i}"):
                            shutil.rmtree(self.keeper_instance_dir_prefix + f"{i}")
                else:
                    for i in range(1, 3):
                        if os.path.exists(self.zookeeper_instance_dir_prefix + f"{i}"):
                            shutil.rmtree(self.zookeeper_instance_dir_prefix + f"{i}")

                for dir in self.zookeeper_dirs_to_create:
                    os.makedirs(dir)

                if self.use_keeper:  # TODO: remove hardcoded paths from here
                    for i in range(1, 4):
                        shutil.copy(
                            os.path.join(HELPERS_DIR, f"keeper_config{i}.xml"),
                            os.path.join(
                                self.keeper_instance_dir_prefix + f"{i}", "config"
                            ),
                        )

                run_and_check(self.base_zookeeper_cmd + common_opts, env=self.env)
                self.up_called = True

                self.wait_zookeeper_to_start()
                for command in self.pre_zookeeper_commands:
                    self.run_kazoo_commands_with_retries(command, repeats=5)

            if self.with_mysql_client and self.base_mysql_client_cmd:
                logging.debug("Setup MySQL Client")
                subprocess_check_call(self.base_mysql_client_cmd + common_opts)
                self.wait_mysql_client_to_start()

            if self.with_mysql and self.base_mysql_cmd:
                logging.debug("Setup MySQL")
                if os.path.exists(self.mysql_dir):
                    shutil.rmtree(self.mysql_dir)
                os.makedirs(self.mysql_logs_dir)
                os.chmod(self.mysql_logs_dir, stat.S_IRWXU | stat.S_IRWXO)
                subprocess_check_call(self.base_mysql_cmd + common_opts)
                self.up_called = True
                self.wait_mysql_to_start()

            if self.with_mysql8 and self.base_mysql8_cmd:
                logging.debug("Setup MySQL 8")
                if os.path.exists(self.mysql8_dir):
                    shutil.rmtree(self.mysql8_dir)
                os.makedirs(self.mysql8_logs_dir)
                os.chmod(self.mysql8_logs_dir, stat.S_IRWXU | stat.S_IRWXO)
                subprocess_check_call(self.base_mysql8_cmd + common_opts)
                self.wait_mysql8_to_start()

            if self.with_mysql_cluster and self.base_mysql_cluster_cmd:
                print("Setup MySQL")
                if os.path.exists(self.mysql_cluster_dir):
                    shutil.rmtree(self.mysql_cluster_dir)
                os.makedirs(self.mysql_cluster_logs_dir)
                os.chmod(self.mysql_cluster_logs_dir, stat.S_IRWXU | stat.S_IRWXO)

                subprocess_check_call(self.base_mysql_cluster_cmd + common_opts)
                self.up_called = True
                self.wait_mysql_cluster_to_start()

            if self.with_postgres and self.base_postgres_cmd:
                logging.debug("Setup Postgres")
                if os.path.exists(self.postgres_dir):
                    shutil.rmtree(self.postgres_dir)
                os.makedirs(self.postgres_logs_dir)
                os.chmod(self.postgres_logs_dir, stat.S_IRWXU | stat.S_IRWXO)

                subprocess_check_call(self.base_postgres_cmd + common_opts)
                self.up_called = True
                self.wait_postgres_to_start()

            if self.with_postgres_cluster and self.base_postgres_cluster_cmd:
                print("Setup Postgres")
                os.makedirs(self.postgres2_logs_dir)
                os.chmod(self.postgres2_logs_dir, stat.S_IRWXU | stat.S_IRWXO)
                os.makedirs(self.postgres3_logs_dir)
                os.chmod(self.postgres3_logs_dir, stat.S_IRWXU | stat.S_IRWXO)
                os.makedirs(self.postgres4_logs_dir)
                os.chmod(self.postgres4_logs_dir, stat.S_IRWXU | stat.S_IRWXO)
                subprocess_check_call(self.base_postgres_cluster_cmd + common_opts)
                self.up_called = True
                self.wait_postgres_cluster_to_start()

            if self.with_kafka and self.base_kafka_cmd:
                logging.debug("Setup Kafka")
                subprocess_check_call(
                    self.base_kafka_cmd + common_opts + ["--renew-anon-volumes"]
                )
                self.up_called = True
                self.wait_kafka_is_available(self.kafka_docker_id, self.kafka_port)
                self.wait_schema_registry_to_start()

            if self.with_kerberized_kafka and self.base_kerberized_kafka_cmd:
                logging.debug("Setup kerberized kafka")
                run_and_check(
                    self.base_kerberized_kafka_cmd
                    + common_opts
                    + ["--renew-anon-volumes"]
                )
                self.up_called = True
                self.wait_kafka_is_available(
                    self.kerberized_kafka_docker_id, self.kerberized_kafka_port, 100
                )

            if self.with_rabbitmq and self.base_rabbitmq_cmd:
                logging.debug("Setup RabbitMQ")
                os.makedirs(self.rabbitmq_logs_dir)
                os.chmod(self.rabbitmq_logs_dir, stat.S_IRWXU | stat.S_IRWXO)

                for i in range(5):
                    subprocess_check_call(
                        self.base_rabbitmq_cmd + common_opts + ["--renew-anon-volumes"]
                    )
                    self.up_called = True
                    self.rabbitmq_docker_id = self.get_instance_docker_id("rabbitmq1")
                    logging.debug(f"RabbitMQ checking container try: {i}")
                    if self.wait_rabbitmq_to_start(throw=(i == 4)):
                        break

            if self.with_hdfs and self.base_hdfs_cmd:
                logging.debug("Setup HDFS")
                os.makedirs(self.hdfs_logs_dir)
                os.chmod(self.hdfs_logs_dir, stat.S_IRWXU | stat.S_IRWXO)
                subprocess_check_call(self.base_hdfs_cmd + common_opts)
                self.up_called = True
                self.make_hdfs_api()
                self.wait_hdfs_to_start()

            if self.with_kerberized_hdfs and self.base_kerberized_hdfs_cmd:
                logging.debug("Setup kerberized HDFS")
                os.makedirs(self.hdfs_kerberized_logs_dir)
                os.chmod(self.hdfs_kerberized_logs_dir, stat.S_IRWXU | stat.S_IRWXO)
                run_and_check(self.base_kerberized_hdfs_cmd + common_opts)
                self.up_called = True
                self.make_hdfs_api(kerberized=True)
                self.wait_hdfs_to_start(check_marker=True)

            if self.with_nginx and self.base_nginx_cmd:
                logging.debug("Setup nginx")
                subprocess_check_call(
                    self.base_nginx_cmd + common_opts + ["--renew-anon-volumes"]
                )
                self.up_called = True
                self.nginx_docker_id = self.get_instance_docker_id("nginx")
                self.wait_nginx_to_start()

            if self.with_mongo and self.base_mongo_cmd:
                logging.debug("Setup Mongo")
                run_and_check(self.base_mongo_cmd + common_opts)
                self.up_called = True
                self.wait_mongo_to_start(30, secure=self.with_mongo_secure)

            if self.with_redis and self.base_redis_cmd:
                logging.debug("Setup Redis")
                subprocess_check_call(self.base_redis_cmd + common_opts)
                self.up_called = True
                time.sleep(10)

            if self.with_hive and self.base_hive_cmd:
                logging.debug("Setup hive")
                subprocess_check_call(self.base_hive_cmd + common_opts)
                self.up_called = True
                time.sleep(300)

            if self.with_minio and self.base_minio_cmd:
                # Copy minio certificates to minio/certs
                os.mkdir(self.minio_dir)
                if self.minio_certs_dir is None:
                    os.mkdir(os.path.join(self.minio_dir, "certs"))
                else:
                    shutil.copytree(
                        os.path.join(self.base_dir, self.minio_certs_dir),
                        os.path.join(self.minio_dir, "certs"),
                    )

                minio_start_cmd = self.base_minio_cmd + common_opts

                logging.info(
                    "Trying to create Minio instance by command %s",
                    " ".join(map(str, minio_start_cmd)),
                )
                run_and_check(minio_start_cmd)
                self.up_called = True
                logging.info("Trying to connect to Minio...")
                self.wait_minio_to_start(secure=self.minio_certs_dir is not None)

            if self.with_azurite and self.base_azurite_cmd:
                azurite_start_cmd = self.base_azurite_cmd + common_opts
                logging.info(
                    "Trying to create Azurite instance by command %s",
                    " ".join(map(str, azurite_start_cmd)),
                )
                run_and_check(azurite_start_cmd)
                self.up_called = True
                logging.info("Trying to connect to Azurite")
                self.wait_azurite_to_start()

            if self.with_cassandra and self.base_cassandra_cmd:
                subprocess_check_call(self.base_cassandra_cmd + ["up", "-d"])
                self.up_called = True
                self.wait_cassandra_to_start()

            if self.with_jdbc_bridge and self.base_jdbc_bridge_cmd:
                os.makedirs(self.jdbc_driver_logs_dir)
                os.chmod(self.jdbc_driver_logs_dir, stat.S_IRWXU | stat.S_IRWXO)

                subprocess_check_call(self.base_jdbc_bridge_cmd + ["up", "-d"])
                self.up_called = True
                self.jdbc_bridge_ip = self.get_instance_ip(self.jdbc_bridge_host)
                self.wait_for_url(
                    f"http://{self.jdbc_bridge_ip}:{self.jdbc_bridge_port}/ping"
                )

            clickhouse_start_cmd = self.base_cmd + ["up", "-d", "--no-recreate"]
            logging.debug(
                (
                    "Trying to create ClickHouse instance by command %s",
                    " ".join(map(str, clickhouse_start_cmd)),
                )
            )
            self.up_called = True
            run_and_check(clickhouse_start_cmd)
            logging.debug("ClickHouse instance created")

            start_timeout = 300.0  # seconds
            for instance in self.instances.values():
                instance.docker_client = self.docker_client
                instance.ip_address = self.get_instance_ip(instance.name)

                logging.debug(
                    f"Waiting for ClickHouse start in {instance.name}, ip: {instance.ip_address}..."
                )
                instance.wait_for_start(start_timeout)
                logging.debug(f"ClickHouse {instance.name} started")

                instance.client = Client(
                    instance.ip_address, command=self.client_bin_path
                )

            self.is_up = True

        except BaseException as e:
            logging.debug("Failed to start cluster: ")
            logging.debug(str(e))
            logging.debug(traceback.print_exc())
            self.shutdown()
            raise

    def shutdown(self, kill=True, ignore_fatal=True):
        sanitizer_assert_instance = None
        fatal_log = None

        if self.up_called:
            with open(self.docker_logs_path, "w+") as f:
                try:
                    subprocess.check_call(  # STYLE_CHECK_ALLOW_SUBPROCESS_CHECK_CALL
                        self.base_cmd + ["logs"], stdout=f
                    )
                except Exception as e:
                    logging.debug("Unable to get logs from docker.")
                f.seek(0)
                for line in f:
                    if SANITIZER_SIGN in line:
                        sanitizer_assert_instance = line.split("|")[0].strip()
                        break

            if kill:
                try:
                    run_and_check(self.base_cmd + ["stop", "--timeout", "20"])
                except Exception as e:
                    logging.debug(
                        "Kill command failed during shutdown. {}".format(repr(e))
                    )
                    logging.debug("Trying to kill forcefully")
                    run_and_check(self.base_cmd + ["kill"])

            # Check server logs for Fatal messages and sanitizer failures.
            # NOTE: we cannot do this via docker since in case of Fatal message container may already die.
            for name, instance in self.instances.items():
                if instance.contains_in_log(SANITIZER_SIGN, from_host=True):
                    sanitizer_assert_instance = instance.grep_in_log(
                        SANITIZER_SIGN, from_host=True, filename="stderr.log"
                    )
                    logging.error(
                        "Sanitizer in instance %s log %s",
                        name,
                        sanitizer_assert_instance,
                    )

                if not ignore_fatal and instance.contains_in_log(
                    "Fatal", from_host=True
                ):
                    fatal_log = instance.grep_in_log("Fatal", from_host=True)
                    if "Child process was terminated by signal 9 (KILL)" in fatal_log:
                        fatal_log = None
                        continue
                    logging.error("Crash in instance %s fatal log %s", name, fatal_log)

            try:
                subprocess_check_call(self.base_cmd + ["down", "--volumes"])
            except Exception as e:
                logging.debug(
                    "Down + remove orphans failed during shutdown. {}".format(repr(e))
                )
        else:
            logging.warning(
                "docker-compose up was not called. Trying to export docker.log for running containers"
            )

        self.cleanup()

        self.is_up = False

        self.docker_client = None

        for instance in list(self.instances.values()):
            instance.docker_client = None
            instance.ip_address = None
            instance.client = None

        if sanitizer_assert_instance is not None:
            raise Exception(
                "Sanitizer assert found in {} for instance {}".format(
                    self.docker_logs_path, sanitizer_assert_instance
                )
            )
        if fatal_log is not None:
            raise Exception("Fatal messages found: {}".format(fatal_log))

    def pause_container(self, instance_name):
        subprocess_check_call(self.base_cmd + ["pause", instance_name])

    #    subprocess_check_call(self.base_cmd + ['kill', '-s SIGSTOP', instance_name])

    def unpause_container(self, instance_name):
        subprocess_check_call(self.base_cmd + ["unpause", instance_name])

    #    subprocess_check_call(self.base_cmd + ['kill', '-s SIGCONT', instance_name])

    def open_bash_shell(self, instance_name):
        os.system(" ".join(self.base_cmd + ["exec", instance_name, "/bin/bash"]))

    def get_kazoo_client(self, zoo_instance_name):
        use_ssl = False
        if self.with_zookeeper_secure:
            port = self.zookeeper_secure_port
            use_ssl = True
        elif self.with_zookeeper:
            port = self.zookeeper_port
        else:
            raise Exception("Cluster has no ZooKeeper")

        ip = self.get_instance_ip(zoo_instance_name)
        logging.debug(
            f"get_kazoo_client: {zoo_instance_name}, ip:{ip}, port:{port}, use_ssl:{use_ssl}"
        )
        zk = KazooClient(
            hosts=f"{ip}:{port}",
            use_ssl=use_ssl,
            verify_certs=False,
            certfile=self.zookeeper_certfile,
            keyfile=self.zookeeper_keyfile,
        )
        zk.start()
        return zk

    def run_kazoo_commands_with_retries(
        self, kazoo_callback, zoo_instance_name="zoo1", repeats=1, sleep_for=1
    ):
        zk = self.get_kazoo_client(zoo_instance_name)
        logging.debug(
            f"run_kazoo_commands_with_retries: {zoo_instance_name}, {kazoo_callback}"
        )
        for i in range(repeats - 1):
            try:
                kazoo_callback(zk)
                return
            except KazooException as e:
                logging.debug(repr(e))
                time.sleep(sleep_for)
        kazoo_callback(zk)
        zk.stop()

    def add_zookeeper_startup_command(self, command):
        self.pre_zookeeper_commands.append(command)

    def stop_zookeeper_nodes(self, zk_nodes):
        for n in zk_nodes:
            logging.info("Stopping zookeeper node: %s", n)
            subprocess_check_call(self.base_zookeeper_cmd + ["stop", n])

    def start_zookeeper_nodes(self, zk_nodes):
        for n in zk_nodes:
            logging.info("Starting zookeeper node: %s", n)
            subprocess_check_call(self.base_zookeeper_cmd + ["start", n])


CLICKHOUSE_START_COMMAND = (
    "clickhouse server --config-file=/etc/clickhouse-server/{main_config_file}"
    " --log-file=/var/log/clickhouse-server/clickhouse-server.log "
    " --errorlog-file=/var/log/clickhouse-server/clickhouse-server.err.log"
)

CLICKHOUSE_STAY_ALIVE_COMMAND = "bash -c \"trap 'pkill tail' INT TERM; {} --daemon; coproc tail -f /dev/null; wait $$!\"".format(
    CLICKHOUSE_START_COMMAND
)

# /run/xtables.lock passed inside for correct iptables --wait
DOCKER_COMPOSE_TEMPLATE = """
version: '2.3'
services:
    {name}:
        image: {image}:{tag}
        hostname: {hostname}
        volumes:
            - {instance_config_dir}:/etc/clickhouse-server/
            - {db_dir}:/var/lib/clickhouse/
            - {logs_dir}:/var/log/clickhouse-server/
            - /etc/passwd:/etc/passwd:ro
            - /run/xtables.lock:/run/xtables.lock:ro
            {binary_volume}
            {odbc_bridge_volume}
            {library_bridge_volume}
            {external_dirs_volumes}
            {odbc_ini_path}
            {keytab_path}
            {krb5_conf}
        entrypoint: {entrypoint_cmd}
        tmpfs: {tmpfs}
        cap_add:
            - SYS_PTRACE
            - NET_ADMIN
            - IPC_LOCK
            - SYS_NICE
        depends_on: {depends_on}
        user: '{user}'
        env_file:
            - {env_file}
        security_opt:
            - label:disable
        dns_opt:
            - attempts:2
            - timeout:1
            - inet6
            - rotate
        {networks}
            {app_net}
                {ipv4_address}
                {ipv6_address}
                {net_aliases}
                    {net_alias1}
"""


class ClickHouseInstance:
    def __init__(
        self,
        cluster,
        base_path,
        name,
        base_config_dir,
        custom_main_configs,
        custom_user_configs,
        custom_dictionaries,
        macros,
        with_zookeeper,
        zookeeper_config_path,
        with_mysql_client,
        with_mysql,
        with_mysql8,
        with_mysql_cluster,
        with_kafka,
        with_kerberized_kafka,
        with_rabbitmq,
        with_nginx,
        with_kerberized_hdfs,
        with_mongo,
        with_redis,
        with_minio,
        with_azurite,
        with_jdbc_bridge,
        with_hive,
        with_cassandra,
        server_bin_path,
        odbc_bridge_bin_path,
        library_bridge_bin_path,
        clickhouse_path_dir,
        with_odbc_drivers,
        with_postgres,
        with_postgres_cluster,
        clickhouse_start_command=CLICKHOUSE_START_COMMAND,
        main_config_name="config.xml",
        users_config_name="users.xml",
        copy_common_configs=True,
        hostname=None,
        env_variables=None,
        image="altinityinfra/integration-test",
        tag="latest",
        stay_alive=False,
        ipv4_address=None,
        ipv6_address=None,
        with_installed_binary=False,
        external_dirs=None,
        tmpfs=None,
        config_root_name="clickhouse",
        extra_configs=[],
    ):

        self.name = name
        self.base_cmd = cluster.base_cmd
        self.docker_id = cluster.get_instance_docker_id(self.name)
        self.cluster = cluster
        self.hostname = hostname if hostname is not None else self.name

        self.external_dirs = external_dirs
        self.tmpfs = tmpfs or []
        self.base_config_dir = (
            p.abspath(p.join(base_path, base_config_dir)) if base_config_dir else None
        )
        self.custom_main_config_paths = [
            p.abspath(p.join(base_path, c)) for c in custom_main_configs
        ]
        self.custom_user_config_paths = [
            p.abspath(p.join(base_path, c)) for c in custom_user_configs
        ]
        self.custom_dictionaries_paths = [
            p.abspath(p.join(base_path, c)) for c in custom_dictionaries
        ]
        self.custom_extra_config_paths = [
            p.abspath(p.join(base_path, c)) for c in extra_configs
        ]
        self.clickhouse_path_dir = (
            p.abspath(p.join(base_path, clickhouse_path_dir))
            if clickhouse_path_dir
            else None
        )
        self.kerberos_secrets_dir = p.abspath(p.join(base_path, "secrets"))
        self.macros = macros if macros is not None else {}
        self.with_zookeeper = with_zookeeper
        self.zookeeper_config_path = zookeeper_config_path

        self.server_bin_path = server_bin_path
        self.odbc_bridge_bin_path = odbc_bridge_bin_path
        self.library_bridge_bin_path = library_bridge_bin_path

        self.with_mysql_client = with_mysql_client
        self.with_mysql = with_mysql
        self.with_mysql8 = with_mysql8
        self.with_mysql_cluster = with_mysql_cluster
        self.with_postgres = with_postgres
        self.with_postgres_cluster = with_postgres_cluster
        self.with_kafka = with_kafka
        self.with_kerberized_kafka = with_kerberized_kafka
        self.with_rabbitmq = with_rabbitmq
        self.with_nginx = with_nginx
        self.with_kerberized_hdfs = with_kerberized_hdfs
        self.with_mongo = with_mongo
        self.with_redis = with_redis
        self.with_minio = with_minio
        self.with_azurite = with_azurite
        self.with_cassandra = with_cassandra
        self.with_jdbc_bridge = with_jdbc_bridge
        self.with_hive = with_hive

        self.main_config_name = main_config_name
        self.users_config_name = users_config_name
        self.copy_common_configs = copy_common_configs

        self.clickhouse_start_command = clickhouse_start_command.replace(
            "{main_config_file}", self.main_config_name
        )

        self.path = p.join(self.cluster.instances_dir, name)
        self.docker_compose_path = p.join(self.path, "docker-compose.yml")
        self.env_variables = env_variables or {}
        self.env_file = self.cluster.env_file
        if with_odbc_drivers:
            self.odbc_ini_path = self.path + "/odbc.ini:/etc/odbc.ini"
            self.with_mysql = True
        else:
            self.odbc_ini_path = ""

        if with_kerberized_kafka or with_kerberized_hdfs:
            self.keytab_path = (
                "- "
                + os.path.dirname(self.docker_compose_path)
                + "/secrets:/tmp/keytab"
            )
            self.krb5_conf = (
                "- "
                + os.path.dirname(self.docker_compose_path)
                + "/secrets/krb.conf:/etc/krb5.conf:ro"
            )
        else:
            self.keytab_path = ""
            self.krb5_conf = ""

        self.docker_client = None
        self.ip_address = None
        self.client = None
        self.image = image
        self.tag = tag
        self.stay_alive = stay_alive
        self.ipv4_address = ipv4_address
        self.ipv6_address = ipv6_address
        self.with_installed_binary = with_installed_binary
        self.is_up = False
        self.config_root_name = config_root_name

    def is_built_with_sanitizer(self, sanitizer_name=""):
        build_opts = self.query(
            "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS'"
        )
        return "-fsanitize={}".format(sanitizer_name) in build_opts

    def is_debug_build(self):
        build_opts = self.query(
            "SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS'"
        )
        return "NDEBUG" not in build_opts

    def is_built_with_thread_sanitizer(self):
        return self.is_built_with_sanitizer("thread")

    def is_built_with_address_sanitizer(self):
        return self.is_built_with_sanitizer("address")

    def is_built_with_memory_sanitizer(self):
        return self.is_built_with_sanitizer("memory")

    # Connects to the instance via clickhouse-client, sends a query (1st argument) and returns the answer
    def query(
        self,
        sql,
        stdin=None,
        timeout=None,
        settings=None,
        user=None,
        password=None,
        database=None,
        ignore_error=False,
        query_id=None,
    ):
        logging.debug("Executing query %s on %s", sql, self.name)
        return self.client.query(
            sql,
            stdin=stdin,
            timeout=timeout,
            settings=settings,
            user=user,
            password=password,
            database=database,
            ignore_error=ignore_error,
            query_id=query_id,
        )

    def query_with_retry(
        self,
        sql,
        stdin=None,
        timeout=None,
        settings=None,
        user=None,
        password=None,
        database=None,
        ignore_error=False,
        retry_count=20,
        sleep_time=0.5,
        check_callback=lambda x: True,
    ):
        logging.debug(f"Executing query {sql} on {self.name}")
        result = None
        for i in range(retry_count):
            try:
                result = self.query(
                    sql,
                    stdin=stdin,
                    timeout=timeout,
                    settings=settings,
                    user=user,
                    password=password,
                    database=database,
                    ignore_error=ignore_error,
                )
                if check_callback(result):
                    return result
                time.sleep(sleep_time)
            except Exception as ex:
                logging.debug("Retry {} got exception {}".format(i + 1, ex))
                time.sleep(sleep_time)

        if result is not None:
            return result
        raise Exception("Can't execute query {}".format(sql))

    # As query() but doesn't wait response and returns response handler
    def get_query_request(self, sql, *args, **kwargs):
        logging.debug(f"Executing query {sql} on {self.name}")
        return self.client.get_query_request(sql, *args, **kwargs)

    # Connects to the instance via clickhouse-client, sends a query (1st argument), expects an error and return its code
    def query_and_get_error(
        self,
        sql,
        stdin=None,
        timeout=None,
        settings=None,
        user=None,
        password=None,
        database=None,
    ):
        logging.debug(f"Executing query {sql} on {self.name}")
        return self.client.query_and_get_error(
            sql,
            stdin=stdin,
            timeout=timeout,
            settings=settings,
            user=user,
            password=password,
            database=database,
        )

    # The same as query_and_get_error but ignores successful query.
    def query_and_get_answer_with_error(
        self,
        sql,
        stdin=None,
        timeout=None,
        settings=None,
        user=None,
        password=None,
        database=None,
    ):
        logging.debug(f"Executing query {sql} on {self.name}")
        return self.client.query_and_get_answer_with_error(
            sql,
            stdin=stdin,
            timeout=timeout,
            settings=settings,
            user=user,
            password=password,
            database=database,
        )

    # Connects to the instance via HTTP interface, sends a query and returns the answer
    def http_query(
        self,
        sql,
        data=None,
        params=None,
        user=None,
        password=None,
        expect_fail_and_get_error=False,
        port=8123,
        timeout=None,
        retry_strategy=None,
    ):
        logging.debug(f"Executing query {sql} on {self.name} via HTTP interface")
        if params is None:
            params = {}
        else:
            params = params.copy()

        params["query"] = sql

        auth = None
        if user and password:
            auth = requests.auth.HTTPBasicAuth(user, password)
        elif user:
            auth = requests.auth.HTTPBasicAuth(user, "")
        url = f"http://{self.ip_address}:{port}/?" + urllib.parse.urlencode(params)

        if retry_strategy is None:
            requester = requests
        else:
            adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
            requester = requests.Session()
            requester.mount("https://", adapter)
            requester.mount("http://", adapter)
        if data:
            r = requester.post(url, data, auth=auth, timeout=timeout)
        else:
            r = requester.get(url, auth=auth, timeout=timeout)

        def http_code_and_message():
            code = r.status_code
            return str(code) + " " + http.client.responses[code] + ": " + r.text

        if expect_fail_and_get_error:
            if r.ok:
                raise Exception(
                    "ClickHouse HTTP server is expected to fail, but succeeded: "
                    + r.text
                )
            return http_code_and_message()
        else:
            if not r.ok:
                raise Exception(
                    "ClickHouse HTTP server returned " + http_code_and_message()
                )
            return r.text

    # Connects to the instance via HTTP interface, sends a query and returns the answer
    def http_request(self, url, method="GET", params=None, data=None, headers=None):
        logging.debug(f"Sending HTTP request {url} to {self.name}")
        url = "http://" + self.ip_address + ":8123/" + url
        return requests.request(
            method=method, url=url, params=params, data=data, headers=headers
        )

    # Connects to the instance via HTTP interface, sends a query, expects an error and return the error message
    def http_query_and_get_error(
        self, sql, data=None, params=None, user=None, password=None
    ):
        logging.debug(f"Executing query {sql} on {self.name} via HTTP interface")
        return self.http_query(
            sql=sql,
            data=data,
            params=params,
            user=user,
            password=password,
            expect_fail_and_get_error=True,
        )

    def stop_clickhouse(self, stop_wait_sec=30, kill=False):
        if not self.stay_alive:
            raise Exception(
                "clickhouse can be stopped only with stay_alive=True instance"
            )
        try:
            ps_clickhouse = self.exec_in_container(
                ["bash", "-c", "ps -C clickhouse"], nothrow=True, user="root"
            )
            if ps_clickhouse == "  PID TTY      STAT   TIME COMMAND":
                logging.warning("ClickHouse process already stopped")
                return

            self.exec_in_container(
                ["bash", "-c", "pkill {} clickhouse".format("-9" if kill else "")],
                user="root",
            )

            start_time = time.time()
            stopped = False
            while time.time() <= start_time + stop_wait_sec:
                pid = self.get_process_pid("clickhouse")
                if pid is None:
                    stopped = True
                    break
                else:
                    time.sleep(1)

            if not stopped:
                pid = self.get_process_pid("clickhouse")
                if pid is not None:
                    logging.warning(
                        f"Force kill clickhouse in stop_clickhouse. ps:{pid}"
                    )
                    self.exec_in_container(
                        [
                            "bash",
                            "-c",
                            f"gdb -batch -ex 'thread apply all bt full' -p {pid} > {os.path.join(self.path, 'logs/stdout.log')}",
                        ],
                        user="root",
                    )
                    self.stop_clickhouse(kill=True)
                else:
                    ps_all = self.exec_in_container(
                        ["bash", "-c", "ps aux"], nothrow=True, user="root"
                    )
                    logging.warning(
                        f"We want force stop clickhouse, but no clickhouse-server is running\n{ps_all}"
                    )
                    return
        except Exception as e:
            logging.warning(f"Stop ClickHouse raised an error {e}")

    def start_clickhouse(self, start_wait_sec=60):
        if not self.stay_alive:
            raise Exception(
                "ClickHouse can be started again only with stay_alive=True instance"
            )
        start_time = time.time()
        time_to_sleep = 0.5

        while start_time + start_wait_sec >= time.time():
            # sometimes after SIGKILL (hard reset) server may refuse to start for some time
            # for different reasons.
            pid = self.get_process_pid("clickhouse")
            if pid is None:
                logging.debug("No clickhouse process running. Start new one.")
                self.exec_in_container(
                    ["bash", "-c", "{} --daemon".format(self.clickhouse_start_command)],
                    user=str(os.getuid()),
                )
                time.sleep(1)
                continue
            else:
                logging.debug("Clickhouse process running.")
                try:
                    self.wait_start(start_wait_sec + start_time - time.time())
                    return
                except Exception as e:
                    logging.warning(
                        f"Current start attempt failed. Will kill {pid} just in case."
                    )
                    self.exec_in_container(
                        ["bash", "-c", f"kill -9 {pid}"], user="root", nothrow=True
                    )
                    time.sleep(time_to_sleep)

        raise Exception("Cannot start ClickHouse, see additional info in logs")

    def wait_start(self, start_wait_sec):
        start_time = time.time()
        last_err = None
        while True:
            try:
                pid = self.get_process_pid("clickhouse")
                if pid is None:
                    raise Exception("ClickHouse server is not running. Check logs.")
                exec_query_with_retry(self, "select 20", retry_count=10, silent=True)
                return
            except QueryRuntimeException as err:
                last_err = err
                pid = self.get_process_pid("clickhouse")
                if pid is not None:
                    logging.warning(f"ERROR {err}")
                else:
                    raise Exception("ClickHouse server is not running. Check logs.")
            if time.time() > start_time + start_wait_sec:
                break
        logging.error(
            f"No time left to start. But process is still running. Will dump threads."
        )
        ps_clickhouse = self.exec_in_container(
            ["bash", "-c", "ps -C clickhouse"], nothrow=True, user="root"
        )
        logging.info(f"PS RESULT:\n{ps_clickhouse}")
        pid = self.get_process_pid("clickhouse")
        if pid is not None:
            self.exec_in_container(
                ["bash", "-c", f"gdb -batch -ex 'thread apply all bt full' -p {pid}"],
                user="root",
            )
        if last_err is not None:
            raise last_err

    def restart_clickhouse(self, stop_start_wait_sec=60, kill=False):
        self.stop_clickhouse(stop_start_wait_sec, kill)
        self.start_clickhouse(stop_start_wait_sec)

    def exec_in_container(self, cmd, detach=False, nothrow=False, **kwargs):
        return self.cluster.exec_in_container(
            self.docker_id, cmd, detach, nothrow, **kwargs
        )

    def rotate_logs(self):
        self.exec_in_container(
            ["bash", "-c", f"kill -HUP {self.get_process_pid('clickhouse server')}"],
            user="root",
        )

    def contains_in_log(
        self, substring, from_host=False, filename="clickhouse-server.log"
    ):
        if from_host:
            # We check fist file exists but want to look for all rotated logs as well
            result = subprocess_check_call(
                [
                    "bash",
                    "-c",
                    f'[ -f {self.logs_dir}/{filename} ] && zgrep -aH "{substring}" {self.logs_dir}/{filename}* || true',
                ]
            )
        else:
            result = self.exec_in_container(
                [
                    "bash",
                    "-c",
                    f'[ -f /var/log/clickhouse-server/{filename} ] && zgrep -aH "{substring}" /var/log/clickhouse-server/{filename} || true',
                ]
            )
        return len(result) > 0

    def grep_in_log(self, substring, from_host=False, filename="clickhouse-server.log"):
        logging.debug(f"grep in log called %s", substring)
        if from_host:
            # We check fist file exists but want to look for all rotated logs as well
            result = subprocess_check_call(
                [
                    "bash",
                    "-c",
                    f'[ -f {self.logs_dir}/{filename} ] && zgrep -a "{substring}" {self.logs_dir}/{filename}* || true',
                ]
            )
        else:
            result = self.exec_in_container(
                [
                    "bash",
                    "-c",
                    f'[ -f /var/log/clickhouse-server/{filename} ] && zgrep -a "{substring}" /var/log/clickhouse-server/{filename}* || true',
                ]
            )
        logging.debug("grep result %s", result)
        return result

    def count_in_log(self, substring):
        result = self.exec_in_container(
            [
                "bash",
                "-c",
                'grep -a "{}" /var/log/clickhouse-server/clickhouse-server.log | wc -l'.format(
                    substring
                ),
            ]
        )
        return result

    def wait_for_log_line(
        self,
        regexp,
        filename="/var/log/clickhouse-server/clickhouse-server.log",
        timeout=30,
        repetitions=1,
        look_behind_lines=100,
    ):
        start_time = time.time()
        result = self.exec_in_container(
            [
                "bash",
                "-c",
                'timeout {} tail -Fn{} "{}" | grep -Em {} {}'.format(
                    timeout,
                    look_behind_lines,
                    filename,
                    repetitions,
                    shlex.quote(regexp),
                ),
            ]
        )

        # if repetitions>1 grep will return success even if not enough lines were collected,
        if repetitions > 1 and len(result.splitlines()) < repetitions:
            logging.debug(
                "wait_for_log_line: those lines were found during {} seconds:".format(
                    timeout
                )
            )
            logging.debug(result)
            raise Exception(
                "wait_for_log_line: Not enough repetitions: {} found, while {} expected".format(
                    len(result.splitlines()), repetitions
                )
            )

        wait_duration = time.time() - start_time

        logging.debug(
            '{} log line(s) matching "{}" appeared in a {:.3f} seconds'.format(
                repetitions, regexp, wait_duration
            )
        )
        return wait_duration

    def path_exists(self, path):
        return (
            self.exec_in_container(
                [
                    "bash",
                    "-c",
                    "echo $(if [ -e '{}' ]; then echo 'yes'; else echo 'no'; fi)".format(
                        path
                    ),
                ]
            )
            == "yes\n"
        )

    def copy_file_to_container(self, local_path, dest_path):
        return self.cluster.copy_file_to_container(
            self.docker_id, local_path, dest_path
        )

    def get_process_pid(self, process_name):
        output = self.exec_in_container(
            [
                "bash",
                "-c",
                "ps ax | grep '{}' | grep -v 'grep' | grep -v 'coproc' | grep -v 'bash -c' | awk '{{print $1}}'".format(
                    process_name
                ),
            ]
        )
        if output:
            try:
                pid = int(output.split("\n")[0].strip())
                return pid
            except:
                return None
        return None

    def restart_with_original_version(
        self, stop_start_wait_sec=300, callback_onstop=None, signal=15
    ):
        begin_time = time.time()
        if not self.stay_alive:
            raise Exception("Cannot restart not stay alive container")
        self.exec_in_container(
            ["bash", "-c", "pkill -{} clickhouse".format(signal)], user="root"
        )
        retries = int(stop_start_wait_sec / 0.5)
        local_counter = 0
        # wait stop
        while local_counter < retries:
            if not self.get_process_pid("clickhouse server"):
                break
            time.sleep(0.5)
            local_counter += 1

        # force kill if server hangs
        if self.get_process_pid("clickhouse server"):
            # server can die before kill, so don't throw exception, it's expected
            self.exec_in_container(
                ["bash", "-c", "pkill -{} clickhouse".format(9)],
                nothrow=True,
                user="root",
            )

        if callback_onstop:
            callback_onstop(self)
        self.exec_in_container(
            [
                "bash",
                "-c",
                "echo 'restart_with_original_version: From version' && /usr/bin/clickhouse server --version && echo 'To version' && /usr/share/clickhouse_original server --version",
            ]
        )
        self.exec_in_container(
            [
                "bash",
                "-c",
                "cp /usr/share/clickhouse_original /usr/bin/clickhouse && chmod 777 /usr/bin/clickhouse",
            ],
            user="root",
        )
        self.exec_in_container(
            [
                "bash",
                "-c",
                "cp /usr/share/clickhouse-odbc-bridge_fresh /usr/bin/clickhouse-odbc-bridge && chmod 777 /usr/bin/clickhouse",
            ],
            user="root",
        )
        self.exec_in_container(
            ["bash", "-c", "{} --daemon".format(self.clickhouse_start_command)],
            user=str(os.getuid()),
        )

        # wait start
        time_left = begin_time + stop_start_wait_sec - time.time()
        if time_left <= 0:
            raise Exception(f"No time left during restart")
        else:
            self.wait_start(time_left)

    def restart_with_latest_version(
        self, stop_start_wait_sec=300, callback_onstop=None, signal=15
    ):
        begin_time = time.time()
        if not self.stay_alive:
            raise Exception("Cannot restart not stay alive container")
        self.exec_in_container(
            ["bash", "-c", "pkill -{} clickhouse".format(signal)], user="root"
        )
        retries = int(stop_start_wait_sec / 0.5)
        local_counter = 0
        # wait stop
        while local_counter < retries:
            if not self.get_process_pid("clickhouse server"):
                break
            time.sleep(0.5)
            local_counter += 1

        # force kill if server hangs
        if self.get_process_pid("clickhouse server"):
            # server can die before kill, so don't throw exception, it's expected
            self.exec_in_container(
                ["bash", "-c", "pkill -{} clickhouse".format(9)],
                nothrow=True,
                user="root",
            )

        if callback_onstop:
            callback_onstop(self)
        self.exec_in_container(
            ["bash", "-c", "cp /usr/bin/clickhouse /usr/share/clickhouse_original"],
            user="root",
        )
        self.exec_in_container(
            [
                "bash",
                "-c",
                "cp /usr/share/clickhouse_fresh /usr/bin/clickhouse && chmod 777 /usr/bin/clickhouse",
            ],
            user="root",
        )
        self.exec_in_container(
            [
                "bash",
                "-c",
                "echo 'restart_with_latest_version: From version' && /usr/share/clickhouse_original server --version && echo 'To version' /usr/share/clickhouse_fresh server --version",
            ]
        )
        self.exec_in_container(
            [
                "bash",
                "-c",
                "cp /usr/share/clickhouse-odbc-bridge_fresh /usr/bin/clickhouse-odbc-bridge && chmod 777 /usr/bin/clickhouse",
            ],
            user="root",
        )
        self.exec_in_container(
            ["bash", "-c", "{} --daemon".format(self.clickhouse_start_command)],
            user=str(os.getuid()),
        )

        # wait start
        time_left = begin_time + stop_start_wait_sec - time.time()
        if time_left <= 0:
            raise Exception(f"No time left during restart")
        else:
            self.wait_start(time_left)

    def get_docker_handle(self):
        return self.cluster.get_docker_handle(self.docker_id)

    def stop(self):
        self.get_docker_handle().stop()

    def start(self):
        self.get_docker_handle().start()

    def wait_for_start(self, start_timeout=None, connection_timeout=None):
        handle = self.get_docker_handle()

        if start_timeout is None or start_timeout <= 0:
            raise Exception("Invalid timeout: {}".format(start_timeout))

        if connection_timeout is not None and connection_timeout < start_timeout:
            raise Exception(
                "Connection timeout {} should be grater then start timeout {}".format(
                    connection_timeout, start_timeout
                )
            )

        start_time = time.time()
        prev_rows_in_log = 0

        def has_new_rows_in_log():
            nonlocal prev_rows_in_log
            try:
                rows_in_log = int(self.count_in_log(".*").strip())
                res = rows_in_log > prev_rows_in_log
                prev_rows_in_log = rows_in_log
                return res
            except ValueError:
                return False

        while True:
            handle.reload()
            status = handle.status
            if status == "exited":
                raise Exception(
                    f"Instance `{self.name}' failed to start. Container status: {status}, logs: {handle.logs().decode('utf-8')}"
                )

            deadline = start_time + start_timeout
            # It is possible that server starts slowly.
            # If container is running, and there is some progress in log, check connection_timeout.
            if connection_timeout and status == "running" and has_new_rows_in_log():
                deadline = start_time + connection_timeout

            current_time = time.time()
            if current_time >= deadline:
                raise Exception(
                    f"Timed out while waiting for instance `{self.name}' with ip address {self.ip_address} to start. "
                    f"Container status: {status}, logs: {handle.logs().decode('utf-8')}"
                )

            socket_timeout = min(start_timeout, deadline - current_time)

            # Repeatedly poll the instance address until there is something that listens there.
            # Usually it means that ClickHouse is ready to accept queries.
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(socket_timeout)
                sock.connect((self.ip_address, 9000))
                self.is_up = True
                return
            except socket.timeout:
                continue
            except socket.error as e:
                if (
                    e.errno == errno.ECONNREFUSED
                    or e.errno == errno.EHOSTUNREACH
                    or e.errno == errno.ENETUNREACH
                ):
                    time.sleep(0.1)
                else:
                    raise
            finally:
                sock.close()

    def dict_to_xml(self, dictionary):
        xml_str = dict2xml(
            dictionary, wrap=self.config_root_name, indent="  ", newlines=True
        )
        return xml_str

    @property
    def odbc_drivers(self):
        if self.odbc_ini_path:
            return {
                "SQLite3": {
                    "DSN": "sqlite3_odbc",
                    "Database": "/tmp/sqliteodbc",
                    "Driver": "/usr/lib/x86_64-linux-gnu/odbc/libsqlite3odbc.so",
                    "Setup": "/usr/lib/x86_64-linux-gnu/odbc/libsqlite3odbc.so",
                },
                "MySQL": {
                    "DSN": "mysql_odbc",
                    "Driver": "/usr/lib/x86_64-linux-gnu/odbc/libmyodbc.so",
                    "Database": "clickhouse",
                    "Uid": "root",
                    "Pwd": "clickhouse",
                    "Server": self.cluster.mysql_host,
                },
                "PostgreSQL": {
                    "DSN": "postgresql_odbc",
                    "Database": "postgres",
                    "UserName": "postgres",
                    "Password": "mysecretpassword",
                    "Port": str(self.cluster.postgres_port),
                    "Servername": self.cluster.postgres_host,
                    "Protocol": "9.3",
                    "ReadOnly": "No",
                    "RowVersioning": "No",
                    "ShowSystemTables": "No",
                    "Driver": "/usr/lib/x86_64-linux-gnu/odbc/psqlodbca.so",
                    "Setup": "/usr/lib/x86_64-linux-gnu/odbc/libodbcpsqlS.so",
                    "ConnSettings": "",
                },
            }
        else:
            return {}

    def _create_odbc_config_file(self):
        with open(self.odbc_ini_path.split(":")[0], "w") as f:
            for driver_setup in list(self.odbc_drivers.values()):
                f.write("[{}]\n".format(driver_setup["DSN"]))
                for key, value in list(driver_setup.items()):
                    if key != "DSN":
                        f.write(key + "=" + value + "\n")

    def replace_config(self, path_to_config, replacement):
        self.exec_in_container(
            ["bash", "-c", "echo '{}' > {}".format(replacement, path_to_config)]
        )

    def replace_in_config(self, path_to_config, replace, replacement):
        self.exec_in_container(
            ["bash", "-c", f"sed -i 's/{replace}/{replacement}/g' {path_to_config}"]
        )

    def create_dir(self, destroy_dir=True):
        """Create the instance directory and all the needed files there."""

        if destroy_dir:
            self.destroy_dir()
        elif p.exists(self.path):
            return

        os.makedirs(self.path)

        instance_config_dir = p.abspath(p.join(self.path, "configs"))
        os.makedirs(instance_config_dir)

        print(
            f"Copy common default production configuration from {self.base_config_dir}. Files: {self.main_config_name}, {self.users_config_name}"
        )

        shutil.copyfile(
            p.join(self.base_config_dir, self.main_config_name),
            p.join(instance_config_dir, self.main_config_name),
        )
        shutil.copyfile(
            p.join(self.base_config_dir, self.users_config_name),
            p.join(instance_config_dir, self.users_config_name),
        )

        logging.debug("Create directory for configuration generated in this helper")
        # used by all utils with any config
        conf_d_dir = p.abspath(p.join(instance_config_dir, "conf.d"))
        os.mkdir(conf_d_dir)

        logging.debug("Create directory for common tests configuration")
        # used by server with main config.xml
        self.config_d_dir = p.abspath(p.join(instance_config_dir, "config.d"))
        os.mkdir(self.config_d_dir)
        users_d_dir = p.abspath(p.join(instance_config_dir, "users.d"))
        os.mkdir(users_d_dir)
        dictionaries_dir = p.abspath(p.join(instance_config_dir, "dictionaries"))
        os.mkdir(dictionaries_dir)
        extra_conf_dir = p.abspath(p.join(instance_config_dir, "extra_conf.d"))
        os.mkdir(extra_conf_dir)

        def write_embedded_config(name, dest_dir, fix_log_level=False):
            with open(p.join(HELPERS_DIR, name), "r") as f:
                data = f.read()
                data = data.replace("clickhouse", self.config_root_name)
                if fix_log_level:
                    data = data.replace("<level>test</level>", "<level>trace</level>")
                with open(p.join(dest_dir, name), "w") as r:
                    r.write(data)

        logging.debug("Copy common configuration from helpers")
        # The file is named with 0_ prefix to be processed before other configuration overloads.
        if self.copy_common_configs:
            need_fix_log_level = self.tag != "latest"
            write_embedded_config(
                "0_common_instance_config.xml", self.config_d_dir, need_fix_log_level
            )

        write_embedded_config("0_common_instance_users.xml", users_d_dir)

        if len(self.custom_dictionaries_paths):
            write_embedded_config("0_common_enable_dictionaries.xml", self.config_d_dir)

        logging.debug("Generate and write macros file")
        macros = self.macros.copy()
        macros["instance"] = self.name
        with open(p.join(conf_d_dir, "macros.xml"), "w") as macros_config:
            macros_config.write(self.dict_to_xml({"macros": macros}))

        # Put ZooKeeper config
        if self.with_zookeeper:
            shutil.copy(self.zookeeper_config_path, conf_d_dir)

        if self.with_kerberized_kafka or self.with_kerberized_hdfs:
            shutil.copytree(
                self.kerberos_secrets_dir, p.abspath(p.join(self.path, "secrets"))
            )

        # Copy config.d configs
        logging.debug(
            f"Copy custom test config files {self.custom_main_config_paths} to {self.config_d_dir}"
        )
        for path in self.custom_main_config_paths:
            shutil.copy(path, self.config_d_dir)

        # Copy users.d configs
        for path in self.custom_user_config_paths:
            shutil.copy(path, users_d_dir)

        # Copy dictionaries configs to configs/dictionaries
        for path in self.custom_dictionaries_paths:
            shutil.copy(path, dictionaries_dir)
        for path in self.custom_extra_config_paths:
            shutil.copy(path, extra_conf_dir)

        db_dir = p.abspath(p.join(self.path, "database"))
        logging.debug(f"Setup database dir {db_dir}")
        if self.clickhouse_path_dir is not None:
            logging.debug(f"Database files taken from {self.clickhouse_path_dir}")
            shutil.copytree(self.clickhouse_path_dir, db_dir)
            logging.debug(
                f"Database copied from {self.clickhouse_path_dir} to {db_dir}"
            )
        else:
            os.mkdir(db_dir)

        logs_dir = p.abspath(p.join(self.path, "logs"))
        logging.debug(f"Setup logs dir {logs_dir}")
        os.mkdir(logs_dir)
        self.logs_dir = logs_dir

        depends_on = []

        if self.with_mysql_client:
            depends_on.append(self.cluster.mysql_client_host)

        if self.with_mysql:
            depends_on.append("mysql57")

        if self.with_mysql8:
            depends_on.append("mysql80")

        if self.with_mysql_cluster:
            depends_on.append("mysql57")
            depends_on.append("mysql2")
            depends_on.append("mysql3")
            depends_on.append("mysql4")

        if self.with_postgres_cluster:
            depends_on.append("postgres2")
            depends_on.append("postgres3")
            depends_on.append("postgres4")

        if self.with_kafka:
            depends_on.append("kafka1")
            depends_on.append("schema-registry")

        if self.with_kerberized_kafka:
            depends_on.append("kerberized_kafka1")

        if self.with_kerberized_hdfs:
            depends_on.append("kerberizedhdfs1")

        if self.with_rabbitmq:
            depends_on.append("rabbitmq1")

        if self.with_zookeeper:
            depends_on.append("zoo1")
            depends_on.append("zoo2")
            depends_on.append("zoo3")

        if self.with_minio:
            depends_on.append("minio1")

        if self.with_azurite:
            depends_on.append("azurite1")

        self.cluster.env_variables.update(self.env_variables)

        odbc_ini_path = ""
        if self.odbc_ini_path:
            self._create_odbc_config_file()
            odbc_ini_path = "- " + self.odbc_ini_path

        entrypoint_cmd = self.clickhouse_start_command

        if self.stay_alive:
            entrypoint_cmd = CLICKHOUSE_STAY_ALIVE_COMMAND.replace(
                "{main_config_file}", self.main_config_name
            )
        else:
            entrypoint_cmd = (
                "["
                + ", ".join(map(lambda x: '"' + x + '"', entrypoint_cmd.split()))
                + "]"
            )

        logging.debug("Entrypoint cmd: {}".format(entrypoint_cmd))

        networks = app_net = ipv4_address = ipv6_address = net_aliases = net_alias1 = ""
        if (
            self.ipv4_address is not None
            or self.ipv6_address is not None
            or self.hostname != self.name
        ):
            networks = "networks:"
            app_net = "default:"
            if self.ipv4_address is not None:
                ipv4_address = "ipv4_address: " + self.ipv4_address
            if self.ipv6_address is not None:
                ipv6_address = "ipv6_address: " + self.ipv6_address
            if self.hostname != self.name:
                net_aliases = "aliases:"
                net_alias1 = "- " + self.hostname

        if not self.with_installed_binary:
            binary_volume = "- " + self.server_bin_path + ":/usr/bin/clickhouse"
            odbc_bridge_volume = (
                "- " + self.odbc_bridge_bin_path + ":/usr/bin/clickhouse-odbc-bridge"
            )
            library_bridge_volume = (
                "- "
                + self.library_bridge_bin_path
                + ":/usr/bin/clickhouse-library-bridge"
            )
        else:
            binary_volume = "- " + self.server_bin_path + ":/usr/share/clickhouse_fresh"
            odbc_bridge_volume = (
                "- "
                + self.odbc_bridge_bin_path
                + ":/usr/share/clickhouse-odbc-bridge_fresh"
            )
            library_bridge_volume = (
                "- "
                + self.library_bridge_bin_path
                + ":/usr/share/clickhouse-library-bridge_fresh"
            )

        external_dirs_volumes = ""
        if self.external_dirs:
            for external_dir in self.external_dirs:
                external_dir_abs_path = p.abspath(
                    p.join(self.path, external_dir.lstrip("/"))
                )
                logging.info(f"external_dir_abs_path={external_dir_abs_path}")
                os.mkdir(external_dir_abs_path)
                external_dirs_volumes += (
                    "- " + external_dir_abs_path + ":" + external_dir + "\n"
                )

        with open(self.docker_compose_path, "w") as docker_compose:
            docker_compose.write(
                DOCKER_COMPOSE_TEMPLATE.format(
                    image=self.image,
                    tag=self.tag,
                    name=self.name,
                    hostname=self.hostname,
                    binary_volume=binary_volume,
                    odbc_bridge_volume=odbc_bridge_volume,
                    library_bridge_volume=library_bridge_volume,
                    instance_config_dir=instance_config_dir,
                    config_d_dir=self.config_d_dir,
                    db_dir=db_dir,
                    external_dirs_volumes=external_dirs_volumes,
                    tmpfs=str(self.tmpfs),
                    logs_dir=logs_dir,
                    depends_on=str(depends_on),
                    user=os.getuid(),
                    env_file=self.env_file,
                    odbc_ini_path=odbc_ini_path,
                    keytab_path=self.keytab_path,
                    krb5_conf=self.krb5_conf,
                    entrypoint_cmd=entrypoint_cmd,
                    networks=networks,
                    app_net=app_net,
                    ipv4_address=ipv4_address,
                    ipv6_address=ipv6_address,
                    net_aliases=net_aliases,
                    net_alias1=net_alias1,
                )
            )

    def destroy_dir(self):
        if p.exists(self.path):
            shutil.rmtree(self.path)

    def wait_for_path_exists(self, path, seconds):
        while seconds > 0:
            seconds -= 1
            if self.path_exists(path):
                return
            time.sleep(1)

    def get_backuped_s3_objects(self, disk, backup_name):
        path = f"/var/lib/clickhouse/disks/{disk}/shadow/{backup_name}/store"
        self.wait_for_path_exists(path, 10)
        command = [
            "find",
            path,
            "-type",
            "f",
            "-exec",
            "grep",
            "-o",
            "r[01]\\{64\\}-file-[[:lower:]]\\{32\\}",
            "{}",
            ";",
        ]
        return self.exec_in_container(command).split("\n")


class ClickHouseKiller(object):
    def __init__(self, clickhouse_node):
        self.clickhouse_node = clickhouse_node

    def __enter__(self):
        self.clickhouse_node.stop_clickhouse(kill=True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clickhouse_node.start_clickhouse()
