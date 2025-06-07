import argparse
import atexit
import logging
import mmap
import os
import pathlib
import random
import tempfile
import time
import sys

sys.path.append("..")
from integration.helpers.cluster import is_port_free


# Needs to get free ports before importing ClickHouseCluster
def get_unique_free_ports(total):
    ports = []
    for port in range(30000, 55000):
        if is_port_free(port) and port not in ports:
            ports.append(port)

        if len(ports) == total:
            return ports

    raise Exception(f"Can't collect {total} ports. Collected: {len(ports)}")


os.environ["WORKER_FREE_PORTS"] = " ".join([str(p) for p in get_unique_free_ports(50)])

from integration.helpers.cluster import ClickHouseCluster
from integration.helpers.postgres_utility import get_postgres_conn
from generators import BuzzHouseGenerator
from properties import modify_server_settings


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
    "--min-disks",
    type=int,
    default=1,
    help="Minimum number of disks to generate",
)
parser.add_argument(
    "--max-disks",
    type=int,
    default=5,
    help="Maximum number of disks to generate",
)
parser.add_argument(
    "--add-policy-settings-prob",
    type=int,
    default=70,
    choices=range(0, 101),
    help="Probability to set random storage policies",
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
    default=tempfile.NamedTemporaryFile(),
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
    "--time-between-shutdowns",
    type=ordered_pair,
    default=(20, 30),
    help="In seconds. Two ordered integers separated by comma (e.g., 30,60)",
)
parser.add_argument(
    "--with-postgresql", type=bool, default=False, help="With PostgreSQL integration"
)
parser.add_argument(
    "--with-mysql", type=bool, default=False, help="With MySQL integration"
)
parser.add_argument(
    "--with-minio", type=bool, default=True, help="With MinIO integration"
)
parser.add_argument(
    "--with-nginx", type=bool, default=False, help="With Nginx integration"
)
parser.add_argument(
    "--with-azurite", type=bool, default=False, help="With Azure integration"
)
parser.add_argument(
    "--with-sqlite", type=bool, default=False, help="With SQLite integration"
)
parser.add_argument(
    "--with-mongodb", type=bool, default=False, help="With MongoDB integration"
)
parser.add_argument(
    "--with-redis", type=bool, default=False, help="With Redis integration"
)
parser.add_argument(
    "--mem-limit", type=str, default="", help="Set a memory limit, e.g. '1g'"
)
parser.add_argument(
    "--storage-limit", type=str, default="", help="Set a storage limit, e.g. '1g'"
)
parser.add_argument(
    "--add-keeper-map-prefix",
    type=bool,
    default=True,
    help="Add 'keeper_map_path_prefix' server setting",
)
args = parser.parse_args()

if len(args.replica_values) != len(args.shard_values):
    raise f"The length of replica values {len(args.replica_values)} is not the same as shard values {len(args.shard_values)}"
if args.min_disks > args.max_disks:
    raise f"The min disk value {args.min_disks} is greater max disk value {args.max_disks}"

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

# Start the cluster, by using one the server binaries
server_path = os.path.join(tempfile.gettempdir(), "clickhouse")
try:
    os.unlink(server_path)
except FileNotFoundError:
    pass
current_server = random.choice(args.server_binaries)
os.symlink(current_server, server_path)
os.environ["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = server_path

# Find if private binary is being used
is_private_binary = False
with open(current_server, "r+") as f:
    mm = mmap.mmap(f.fileno(), 0)
    is_private_binary = mm.find(b"s3_with_keeper") > -1
    mm.close()

logger.info(f"Private binary {"" if is_private_binary else "not "}detected")
cluster = ClickHouseCluster(__file__)

# Use random server settings sometimes
server_settings = args.server_config
modified_server_settings = False
if server_settings is not None:
    modified_server_settings, server_settings = modify_server_settings(
        args, cluster, is_private_binary, server_settings
    )


servers = []
for i in range(0, len(args.replica_values)):
    servers.append(
        cluster.add_instance(
            f"node{i}",
            with_dolor=True,
            with_zookeeper=True,
            stay_alive=True,
            keeper_required_feature_flags=["multi_read"],
            with_minio=args.with_minio,
            with_nginx=args.with_nginx,
            with_azurite=args.with_azurite,
            with_postgres=args.with_postgresql,
            with_mysql8=args.with_mysql,
            with_mongo=args.with_mongodb,
            with_redis=args.with_redis,
            mem_limit=None if args.mem_limit == "" else args.mem_limit,
            storage_opt=None if args.storage_limit == "" else args.storage_limit,
            main_configs=[server_settings] if server_settings is not None else [],
            user_configs=[args.user_config] if args.user_config is not None else [],
            macros={"replica": args.replica_values[i], "shard": args.shard_values[i]},
        )
    )
cluster.start()
logger.info(
    f"Starting cluster with {len(servers)} server(s) and server binary {current_server} "
)
for i in range(0, len(args.replica_values)):
    logger.info(f"Server node{i} running on host {servers[i].ip_address}, port 9000")
servers[len(servers) - 1].wait_start(8)

if args.with_postgresql:
    postgres_conn = get_postgres_conn(
        ip=cluster.postgres_ip, port=cluster.postgres_port
    )
    cursor = postgres_conn.cursor()
    cursor.execute(f"CREATE DATABASE test")
    cursor.close()
    postgres_conn.close()

# Start the load generator, at the moment only BuzzHouse is available
generator = None
if args.generator == "buzzhouse":
    generator = BuzzHouseGenerator(args, cluster)
logger.info("Start load generator")
client = generator.run_generator(servers[0])


def dolor_cleanup():
    if client.process.poll() is None:
        client.process.kill()
    if modified_server_settings:
        try:
            os.unlink(server_settings)
        except FileNotFoundError:
            pass
    try:
        os.unlink(server_path)
    except FileNotFoundError:
        pass
    try:
        os.unlink(generator.temp.name)
    except FileNotFoundError:
        pass


atexit.register(dolor_cleanup)
time.sleep(3)

# This is the main loop, run while client and server are running
while True:
    if client.process.poll() is not None:
        logger.info("Load generator finished")
        break
    for server in servers:
        try:
            server.query("SELECT 1;")
        except:
            logger.info(f"The server {server.name} is not running")
            break

    lower_bound, upper_bound = args.time_between_shutdowns
    time.sleep(random.randint(lower_bound, upper_bound))

    # Pick one of the servers to restart
    next_pick = random.choice(servers)
    kill_server = random.randint(1, 100) <= args.kill_server_prob
    logger.info(
        f"Restart the server {next_pick.name} with {"kill" if kill_server else "manual shutdown"}"
    )

    next_pick.stop_clickhouse(stop_wait_sec=10, kill=kill_server)
    # Replace server binary, using a new temporary symlink, then replace the old one
    if (
        len(args.server_binaries) > 1
        and random.randint(1, 100) <= args.change_server_version_prob
    ):
        if len(servers) == 1 and len(args.server_binaries) == 2:
            current_server = (
                args.server_binaries[0]
                if current_server == args.server_binaries[1]
                else args.server_binaries[1]
            )
        else:
            current_server = random.choice(args.server_binaries)
        logger.info(f"Using the server binary {current_server} after restart")
        new_temp_server_path = os.path.join(tempfile.gettempdir(), "clickhousetemp")
        try:
            os.unlink(new_temp_server_path)
        except FileNotFoundError:
            pass
        os.symlink(current_server, new_temp_server_path)
        os.rename(new_temp_server_path, server_path)
    time.sleep(15)  # Let the zookeeper session expire
    next_pick.start_clickhouse(start_wait_sec=10, retry_start=False)

cluster.shutdown()
