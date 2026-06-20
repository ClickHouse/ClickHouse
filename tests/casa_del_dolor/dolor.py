import argparse
import atexit
import logging
import mmap
import os
import pathlib
import random
import re
import subprocess
import tempfile
import time
import signal
import sys

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
from integration.helpers.cluster import ZOOKEEPER_CONTAINERS
from sparkserver import (
    get_unique_free_ports,
    create_spark_http_server,
    close_spark_http_server,
)

# Needs to get free ports before importing ClickHouseCluster
os.environ["WORKER_FREE_PORTS"] = " ".join([str(p) for p in get_unique_free_ports(50)])

from environment import set_environment_variables
from integration.helpers.cluster import ClickHouseCluster, ClickHouseInstance
from integration.helpers.postgres_utility import get_postgres_conn
from integration.helpers.s3_tools import (
    AzureUploader,
    LocalUploader,
    S3Uploader,
    LocalDownloader,
    prepare_s3_bucket,
)
from integration.helpers.config_cluster import minio_access_key, minio_secret_key
from generators import Generator, BuzzHouseGenerator
from leaks import ElOracloDeLeaks
from oracles import ElOraculoDeTablas
from properties import (
    modify_server_settings,
    modify_user_settings,
    modify_keeper_settings,
)


def ordered_pair(value):
    try:
        x, y = map(int, value.split(","))
        if x > y:
            raise argparse.ArgumentTypeError(
                f"First value must be less than second (got {x} > {y})"
            )
        return (x, y)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "Must be two comma-separated integers (e.g., '1,10')"
        )


def list_of_values(arg):
    return arg.split(",")


parser = argparse.ArgumentParser()
parser.add_argument(
    "--server-settings-prob",
    type=int,
    default=80,
    choices=range(0, 101),
    help="Probability to set server properties",
)
parser.add_argument(
    "--add-disk-settings-prob",
    type=int,
    default=80,
    choices=range(0, 101),
    help="Probability to set random disks",
)
parser.add_argument(
    "--number-disks",
    type=ordered_pair,
    default=(1, 5),
    help="Number of disks to generate. Two ordered integers separated by comma (e.g., 1,3)",
)
parser.add_argument(
    "--add-policy-settings-prob",
    type=int,
    default=70,
    choices=range(0, 101),
    help="Probability to set random storage policies",
)
parser.add_argument(
    "--add-remote-server-settings-prob",
    type=int,
    default=80,
    choices=range(0, 101),
    help="Probability to set random servers",
)
parser.add_argument(
    "--number-servers",
    type=ordered_pair,
    default=(1, 3),
    help="Number of servers to generate. Two ordered integers separated by comma (e.g., 1,3)",
)
parser.add_argument(
    "--add-filesystem-caches-prob",
    type=int,
    default=80,
    choices=range(0, 101),
    help="Probability to add filesystem caches",
)
parser.add_argument(
    "--number-caches",
    type=ordered_pair,
    default=(1, 3),
    help="Number of filesystem caches to generate. Two ordered integers separated by comma (e.g., 1,3)",
)
parser.add_argument(
    "--change-server-version-prob",
    type=int,
    default=80,
    choices=range(0, 101),
    help="Probability to change server version after restart",
)
parser.add_argument(
    "--client-binary", type=pathlib.Path, required=True, help="Path to client binary"
)
parser.add_argument(
    "--server-binaries",
    type=list_of_values,
    required=True,
    help="Path of server binaries to test",
)
parser.add_argument(
    "-c", "--client-config", type=pathlib.Path, help="Path to client configuration file"
)
parser.add_argument(
    "-g",
    "--generator",
    choices=["buzzhouse"],
    type=str.lower,
    required=True,
    help="What generator to use",
)
parser.add_argument(
    "-l",
    "--log-path",
    type=pathlib.Path,
    default=tempfile.NamedTemporaryFile(suffix=".log"),
    help="Log path",
)
parser.add_argument(
    "--replica-values",
    type=list_of_values,
    default="1",
    help="Comma separated list for replica values",
)
parser.add_argument(
    "--shard-values",
    type=list_of_values,
    default="1",
    help="Comma separated list for shard values",
)
parser.add_argument(
    "--server-config", type=pathlib.Path, help="Path to config.xml file"
)
parser.add_argument("-s", "--seed", type=int, default=0, help="Server fuzzer seed")
parser.add_argument(
    "-u", "--user-config", type=pathlib.Path, help="Path to users.xml file"
)
parser.add_argument(
    "--kill-server-prob",
    type=int,
    default=50,
    choices=range(0, 101),
    help="Probability to kill the server instead of shutting it down",
)
parser.add_argument(
    "--restart-clickhouse-prob",
    type=int,
    default=50,
    choices=range(0, 101),
    help="Probability to restart ClickHouse instead of integration servers",
)
parser.add_argument(
    "--time-between-shutdowns",
    type=ordered_pair,
    default=(20, 30),
    help="In seconds. Two ordered integers separated by comma (e.g., 30,60)",
)
parser.add_argument(
    "--time-between-integration-shutdowns",
    type=ordered_pair,
    default=(3, 5),
    help="In seconds. Two ordered integers separated by comma (e.g., 3,5)",
)
parser.add_argument(
    "--without-minio",
    action="store_false",
    dest="with_minio",
    help="Without MinIO integration",
)
parser.add_argument(
    "--with-azurite", action="store_true", help="With Azure integration"
)
parser.add_argument(
    "--without-zookeeper",
    action="store_false",
    dest="with_zookeeper",
    help="Without Zookeeper server",
)
parser.add_argument(
    "--with-postgresql", action="store_true", help="With PostgreSQL integration"
)
parser.add_argument("--with-mysql", action="store_true", help="With MySQL integration")
parser.add_argument("--with-nginx", action="store_true", help="With Nginx integration")
parser.add_argument(
    "--with-sqlite", action="store_true", help="With SQLite integration"
)
parser.add_argument(
    "--with-mongodb", action="store_true", help="With MongoDB integration"
)
parser.add_argument("--with-redis", action="store_true", help="With Redis integration")
parser.add_argument(
    "--with-arrowflight", action="store_true", help="With Arrow flight support"
)
parser.add_argument("--with-kafka", action="store_true", help="With Kafka integration")
parser.add_argument(
    "--mem-limit", type=str, default="", help="Set a memory limit, e.g. '1g'"
)
parser.add_argument(
    "--without-keeper-map-prefix",
    action="store_false",
    dest="add_keeper_map_prefix",
    help="Add 'keeper_map_path_prefix' server setting",
)
parser.add_argument(
    "--without-transactions",
    action="store_false",
    dest="add_transactions",
    help="Add 'allow_experimental_transactions' server setting",
)
parser.add_argument(
    "--without-log-tables",
    action="store_false",
    dest="add_log_tables",
    help="Add log tables server settings",
)
parser.add_argument(
    "--without-distributed-ddl",
    action="store_false",
    dest="add_distributed_ddl",
    help="Add 'distributed_ddl' settings",
)
parser.add_argument(
    "--without-shared-catalog",
    action="store_false",
    dest="add_shared_catalog",
    help="Add 'shared_database_catalog' settings",
)
parser.add_argument(
    "--without-database-replicated",
    action="store_false",
    dest="add_database_replicated",
    help="Add 'database_replicated' settings",
)
parser.add_argument(
    "--compare-table-dump-prob",
    type=int,
    default=50,
    choices=range(0, 101),
    help="Probability to compare contents of a table after a server restart",
)
parser.add_argument(
    "--set-locales-prob",
    type=int,
    default=50,
    choices=range(0, 101),
    help="Probability to send a random locale to all instances in a cluster",
)
parser.add_argument(
    "--set-timezones-prob",
    type=int,
    default=50,
    choices=range(0, 101),
    help="Probability to send a random timezone to all instances in a cluster",
)
parser.add_argument(
    "--keeper-settings-prob",
    type=int,
    default=80,
    choices=range(0, 101),
    help="Probability to set keeper server properties",
)
parser.add_argument(
    "--with-spark", action="store_true", help="With Spark support in Dolor HTTP server"
)
parser.add_argument(
    "--with-glue", action="store_true", help="With AWS Glue catalog for Spark"
)
parser.add_argument(
    "--with-rest", action="store_true", help="With Iceberg REST catalog for Spark"
)
parser.add_argument(
    "--with-hms", action="store_true", help="With Hive catalog for Spark"
)
parser.add_argument(
    "--with-unity",
    type=pathlib.Path,
    help="With Unity catalog for Spark, path to Unity dir",
)
parser.add_argument(
    "--with-leak-detection", action="store_true", help="Check for memory leaks"
)
parser.add_argument(
    "--time-between-leak-detections",
    type=ordered_pair,
    default=(20, 30),
    help="In seconds. Two ordered integers separated by comma (e.g., 30,60)",
)
parser.add_argument(
    "--set-shared-mergetree-disk",
    action="store_true",
    help="Set shared merge tree disk or policy",
)
parser.add_argument(
    "--without-monitoring",
    action="store_false",
    dest="with_monitoring",
    help="Remove periodic monitoring of the cluster",
)
parser.add_argument(
    "--time-between-monitoring-runs",
    type=ordered_pair,
    default=(5, 10),
    help="In seconds. Two ordered integers separated by comma (e.g., 30,60)",
)
UNSET = object()
parser.add_argument(
    "--timeout",
    type=int,
    default=UNSET,
    help="Total time to run the test in minutes (the test will stop after this time)",
)
parser.add_argument(
    "--tmp-files-dir",
    type=pathlib.Path,
    default=pathlib.Path("/tmp"),
    help="Path to temporary files dir",
)

args = parser.parse_args()

if len(args.replica_values) != len(args.shard_values):
    raise Exception(
        f"The length of replica values {len(args.replica_values)} is not the same as shard values {len(args.shard_values)}"
    )

logging.basicConfig(
    filename=args.log_path,
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

# Set seed first
seed = args.seed
if seed == 0:
    import secrets

    seed = secrets.randbits(64)  # 64 - bit random integer
random.seed(seed)
logger.info(f"Using seed: {seed}")

sorted_binaries = []
# Start the cluster, by using one of the server binaries
if len(args.server_binaries) > 1 and random.randint(1, 100) <= 90:
    # Pick the lowest version most of the times
    def get_clickhouse_version(binary_path):
        result = subprocess.run(
            [binary_path, "--version"], capture_output=True, text=True
        )
        # Output like: "ClickHouse client version 24.3.1.2 (official build)."
        match = re.search(r"version (\d+\.\d+\.\d+\.?\d*)", result.stdout)
        if match:
            return tuple(int(x) for x in match.group(1).split("."))
        raise ValueError(f"Could not parse version from {binary_path}")

    first_server = args.server_binaries[0]
    lowest_version = get_clickhouse_version(first_server)
    for val in args.server_binaries:
        next_version = get_clickhouse_version(val)
        if next_version < lowest_version:
            first_server = val
            lowest_version = next_version
else:
    first_server = random.choice(args.server_binaries)
# Make sure the first server version is always first
sorted_binaries.append(first_server)
for val in args.server_binaries:
    if val != first_server:
        sorted_binaries.append(val)

# Find if private binary is being used
is_private_binary = False
with open(first_server, "rb") as f:
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    is_private_binary = mm.find(b"isCoordinatedMergesTasksActivated") > -1
    mm.close()

logger.info(f"Private binary {'' if is_private_binary else 'not '}detected")
keeper_configs: list[str] = modify_keeper_settings(args, is_private_binary)

if args.with_minio:
    # Set environment variables before cluster starts
    credentials_file = tempfile.NamedTemporaryFile(dir=args.tmp_files_dir)
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:3000"
    os.environ["MINIO_ACCESS_KEY"] = minio_access_key
    os.environ["MINIO_SECRET_KEY"] = minio_secret_key
    with open(credentials_file.name, "w+") as file:
        file.write(
            f"[default]\naws_access_key_id = testing\naws_secret_access_key = testing\naws_session_token = testing\naws_region = us-east-1\naws_endpoint_url = http://localhost:3000\n"
        )
    os.environ["AWS_CONFIG_FILE"] = credentials_file.name
    os.environ["AWS_SHARED_CREDENTIALS_FILE"] = credentials_file.name

cluster = ClickHouseCluster(
    __file__,
    name="dolor",
    custom_keeper_configs=keeper_configs,
    azurite_default_port=10000,
    server_bin_path=first_server,
    client_bin_path=args.client_binary,
    server_binaries=sorted_binaries,
    with_dolor=True,
)

# Set environment variables such as locales and timezones
test_env_variables = set_environment_variables(logger, args, "cluster")

# Use random server settings sometimes
server_settings = args.server_config
user_settings = args.user_config
modified_server_settings = modified_user_settings = False
generated_clusters = 0
if server_settings is not None:
    modified_server_settings, server_settings, generated_clusters = (
        modify_server_settings(args, cluster, is_private_binary, server_settings)
    )
    if generated_clusters > 0:
        modified_user_settings, user_settings = modify_user_settings(
            args, user_settings, generated_clusters
        )

dolor_main_configs = [
    "../config/server.crt",
    "../config/server.key",
    "../config/server-cert.pem",
    "../config/server-key.pem",
    "../config/ca-cert.pem",
    "../config/dhparam.pem",
]
if server_settings is not None:
    dolor_main_configs.append(server_settings)


servers: list[ClickHouseInstance] = []
for i in range(0, len(args.replica_values)):
    servers.append(
        cluster.add_instance(
            f"node{i}",
            stay_alive=True,
            copy_common_configs=False,
            with_zookeeper=args.with_zookeeper,
            with_minio=args.with_minio,
            with_nginx=args.with_nginx,
            with_azurite=args.with_azurite,
            with_postgres=args.with_postgresql,
            with_mysql8=args.with_mysql,
            with_mongo=args.with_mongodb,
            with_redis=args.with_redis,
            with_iceberg_catalog=args.with_rest,
            with_glue_catalog=args.with_glue,
            with_hms_catalog=args.with_hms,
            with_arrowflight=args.with_arrowflight,
            with_kafka=args.with_kafka,
            mem_limit=None if args.mem_limit == "" else args.mem_limit,
            main_configs=dolor_main_configs,
            user_configs=[user_settings] if user_settings is not None else [],
            env_variables=test_env_variables,
            macros={"replica": args.replica_values[i], "shard": args.shard_values[i]},
        )
    )
# Copy the binaries into the containers
server_versions = {}
for server in servers:
    server_versions[server.name] = first_server
cluster.start()
logger.info(
    f"Starting cluster with {len(servers)} server(s) and server binary {first_server}"
)
for i in range(0, len(args.replica_values)):
    logger.info(
        f"Server node{i} running on host {servers[i].hostname}, with IPv4 {servers[i].ip_address}, port 9000"
    )
servers[len(servers) - 1].wait_start(8)
servers[0].give_user_files_permissions()

# Uploaders for object storage
if args.with_minio:
    prepare_s3_bucket(cluster)
    cluster.default_s3_uploader = S3Uploader(cluster.minio_client, cluster.minio_bucket)
if args.with_azurite:
    cluster.blob_service_client = cluster.blob_service_client
    cluster.container_client = cluster.blob_service_client.create_container(
        cluster.azure_container_name
    )
    cluster.default_azure_uploader = AzureUploader(
        cluster.blob_service_client, cluster.azure_container_name
    )
cluster.default_local_uploader = LocalUploader(cluster.instances["node0"])
cluster.default_local_downloader = LocalDownloader(cluster.instances["node0"])

if args.with_postgresql:
    postgres_conn = get_postgres_conn(
        ip=cluster.postgres_ip, port=cluster.postgres_port
    )
    cursor = postgres_conn.cursor()
    cursor.execute(f"CREATE DATABASE test")
    cursor.close()
    postgres_conn.close()

# Handler for HTTP server
catalog_server = create_spark_http_server(cluster, args.with_unity, test_env_variables)

# Start the load generator, at the moment only BuzzHouse is available
generator: Generator = Generator(pathlib.Path(), pathlib.Path(), pathlib.Path(), None)
if args.generator == "buzzhouse":
    generator = BuzzHouseGenerator(args, cluster, catalog_server, server_settings)
logger.info("Starting load generator")
client = generator.run_generator(servers[0], logger, args)
logger.info("Load generator started with PID %s", client.process.pid)
everything_cleaned = False


def dolor_cleanup():
    global everything_cleaned
    if not everything_cleaned:
        if client.process.poll() is None:
            client.process.kill()
            client.process.wait()
        logger.info(f"Load generator exited with code: {client.process.returncode}")

        try:
            cluster.shutdown(kill=True, ignore_fatal=False)
        except:
            pass
        close_spark_http_server(catalog_server)
        if modified_server_settings:
            try:
                os.unlink(server_settings)
            except FileNotFoundError:
                pass
        if modified_user_settings:
            try:
                os.unlink(user_settings)
            except FileNotFoundError:
                pass
        try:
            os.unlink(generator.temp.name)
        except FileNotFoundError:
            pass
        if args.with_minio:
            try:
                os.unlink(credentials_file.name)
            except FileNotFoundError:
                pass
        for entry in keeper_configs:
            try:
                os.unlink(entry)
            except FileNotFoundError:
                pass
        everything_cleaned = True


def my_signal_handler(sig, frame):
    dolor_cleanup()
    sys.exit(0)


signal.signal(signal.SIGINT, my_signal_handler)
atexit.register(dolor_cleanup)
time.sleep(3)

integrations = []
if args.with_zookeeper:
    integrations.extend(["zookeeper", "zookeeper"])  # Increased probability
if args.with_minio:
    integrations.append("minio")
if args.with_nginx:
    integrations.append("nginx")
if args.with_azurite:
    integrations.append("azurite")
if args.with_postgresql:
    integrations.append("postgres")
if args.with_mysql:
    integrations.append("mysql8")
if args.with_mongodb:
    integrations.append("mongo")
if args.with_redis:
    integrations.append("redis")
if args.with_kafka:
    integrations.append("kafka")

# This is the main loop, run while client and server are running
all_running = True
good_exit = True
tables_oracle: ElOraculoDeTablas = ElOraculoDeTablas()
# Shutdown info
lower_bound, upper_bound = args.time_between_shutdowns
integration_lower_bound, integration_upper_bound = (
    args.time_between_integration_shutdowns
)
monitoring_lower_bound, monitoring_upper_bound = args.time_between_monitoring_runs
# Leak detection
leak_detector: ElOracloDeLeaks = ElOracloDeLeaks()
leak_lower_bound, leak_upper_bound = args.time_between_leak_detections
if args.with_leak_detection:
    leak_detector.reset_and_capture_baseline(cluster)
# Test timeout if set
test_limit = None if args.timeout is UNSET else time.time() + args.timeout * 60
reached_limit = False


def explain_returncode(rc: int) -> str:
    if rc == 0:
        return "successfully (0)."
    if rc < 0:
        sig = -rc
        try:
            name = signal.Signals(sig).name
        except ValueError:
            name = f"SIG{sig}"
        try:
            reason = signal.strsignal(sig)  # Py3.8+ (wording varies by platform)
        except Exception:
            reason = ""
        extra = f" - {reason}" if reason else ""
        return f"terminated by signal {sig} ({name}){extra}."
    return f"with status {rc}."


while all_running and (not reached_limit):
    start = time.time()
    finish = start + random.randint(lower_bound, upper_bound)
    next_leak_detection = start + random.randint(leak_lower_bound, leak_upper_bound)
    next_monitoring = start + random.randint(
        monitoring_lower_bound, monitoring_upper_bound
    )

    while all_running and (not reached_limit) and start < finish:
        if client.process.poll() is not None:
            logger.info(
                f"Load generator finished {explain_returncode(client.process.returncode)}"
            )
            all_running = False
            good_exit = good_exit and generator.validate_exit_code(
                client.process.returncode
            )
        for server in servers:
            pid = server.get_process_pid("clickhouse")
            if pid is None:
                logger.info(f"The server {server.name} is not running")
                all_running = good_exit = False
        reached_limit = test_limit is not None and time.time() >= test_limit
        if reached_limit:
            logger.info("Test timeout reached, stopping the load generator and exiting")
        if all_running and (not reached_limit):
            if args.with_leak_detection and next_leak_detection < time.time():
                leak_detector.run_next_leak_detection(cluster, client)
                next_leak_detection += random.randint(
                    leak_lower_bound, leak_upper_bound
                )
            if args.with_monitoring and next_monitoring < time.time():
                tables_oracle.run_health_check(cluster, servers, logger)
                next_monitoring += random.randint(
                    monitoring_lower_bound, monitoring_upper_bound
                )
        interval = 1
        time.sleep(interval)
        start += interval

    if (not all_running) or reached_limit:
        break

    dump_table = (
        tables_oracle.collect_table_hash_before_shutdown(cluster, logger)
        if random.randint(1, 100) <= args.compare_table_dump_prob
        else None
    )
    kill_server = random.randint(1, 100) <= args.kill_server_prob
    # Pick one of the servers to restart
    # Restart ClickHouse
    if random.randint(1, 100) <= args.restart_clickhouse_prob:
        next_pick = random.choice(servers)
        logger.info(
            f"Restarting the server {next_pick.name} with {'kill' if kill_server else 'manual shutdown'}"
        )

        try:
            next_pick.stop_clickhouse(stop_wait_sec=10, kill=kill_server)
        except Exception as ex:
            logger.error(f"Failed to stop ClickHouse: {ex}")
            logger.info(f"The server {next_pick.name} is not running")
            all_running = good_exit = False
        time.sleep(1)
        # Replace server binary, using a new temporary symlink
        if (
            all_running
            and len(sorted_binaries) > 1
            and random.randint(1, 100) <= args.change_server_version_prob
        ):
            if len(servers) == 1 and len(sorted_binaries) == 2:
                # Pick the other server version
                next_server = sorted_binaries[
                    0 if server_versions[next_pick.name] == sorted_binaries[1] else 1
                ]
            else:
                next_server = random.choice(sorted_binaries)
            logger.info(f"Picked the server binary {next_server} for restart")
            # Update symlink in the container
            next_pick.exec_in_container(
                [
                    "ln",
                    "-sf",
                    f"/usr/bin/clickhouse{sorted_binaries.index(next_server)}",
                    "/usr/bin/clickhouse",
                ],
                user="root",
            )
            server_versions[next_pick.name] = next_server
        if all_running:
            time.sleep(3)  # Let the keeper session expire
            try:
                next_pick.start_clickhouse(start_wait_sec=10, retry_start=False)
            except Exception as ex:
                logger.error(f"Failed to start ClickHouse: {ex}")
                logger.info(f"The server {next_pick.name} is not running")
                all_running = good_exit = False
            if all_running and args.with_leak_detection and next_pick.name == "node0":
                # Has to reset leak detector
                leak_detector.reset_and_capture_baseline(cluster)
    elif len(integrations) > 0:
        # Restart any other integration
        next_pick = random.choice(integrations)
        choosen_instances = []
        available_options = {
            "zookeeper": list(ZOOKEEPER_CONTAINERS),
            "minio": ["minio1"],
            "nginx": ["nginx"],
            "azurite": ["azurite1"],
            "postgres": ["postgres1"],
            "mysql8": ["mysql80"],
            "mongo": ["mongo1", "mongo_no_cred", "mongo_secure"],
            "redis": ["redis1"],
            "kafka": ["kafka1"],
        }

        restart_choices = list(available_options[next_pick])
        random.shuffle(restart_choices)
        for i in range(0, random.randint(1, len(restart_choices))):
            choosen_instances.append(restart_choices[i])
        logger.info(
            f"Restarting {next_pick} instances {', '.join(choosen_instances)} with {'kill' if kill_server else 'manual shutdown'}"
        )

        cluster.process_integration_nodes(
            next_pick, choosen_instances, "kill" if kill_server else "stop"
        )
        time.sleep(random.randint(integration_lower_bound, integration_upper_bound))
        cluster.process_integration_nodes(next_pick, choosen_instances, "start")
    if all_running:
        tables_oracle.collect_table_hash_after_shutdown(cluster, logger, dump_table)

sys.exit(0 if good_exit else 1)
