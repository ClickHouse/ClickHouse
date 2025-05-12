import argparse
import atexit
import logging
import os
import pathlib
import random
import tempfile
import time
import sys

sys.path.append('..')
from integration.helpers.cluster import ClickHouseCluster
from generators import BuzzHouseGenerator
from properties import modify_server_settings_with_random_properties

def ordered_pair(value):
    try:
        x, y = map(int, value.split(','))
        if x >= y:
            raise argparse.ArgumentTypeError(f"First value must be less than second (got {x} >= {y})")
        return (x, y)
    except ValueError:
        raise argparse.ArgumentTypeError("Must be two comma-separated integers (e.g., '1,10')")

def list_of_paths(arg):
    return arg.split(',')

parser = argparse.ArgumentParser()
parser.add_argument("--server-settings-prob", type = int, default = 80, choices=range(0, 100), help = 'Probability to set server properties')
parser.add_argument("--change-server-version-prob", type = int, default = 80, choices=range(0, 100), help = 'Probability to change server version after restart')
parser.add_argument("--client-binary", type = pathlib.Path, required = True, help = 'Path to client binary')
parser.add_argument("--server-binaries", type = list_of_paths, required = True, help = 'Path of server binaries to test')
parser.add_argument("-c", "--client-config", type = pathlib.Path, help = 'Path to client configuration file')
parser.add_argument("-g", "--generator", choices =['buzzhouse'], type = str.lower, required = True, help = 'What generator to use')
#parser.add_argument("--number-instances", type = int, default = 2, help = 'Number of default instances')
parser.add_argument("-l", "--log-path", type = pathlib.Path, default=tempfile.NamedTemporaryFile(), help = 'Log path')
parser.add_argument("--server-config", type = pathlib.Path, help = 'Path to config.xml file')
parser.add_argument("-s", "--seed", type = int, default = 0, help = 'Server fuzzer seed')
parser.add_argument("-u", "--user-config", type = pathlib.Path, help = 'Path to users.xml file')
parser.add_argument("--kill-server-prob", type = int, default = 50, choices=range(0, 100), help = 'Probability to kill the server instead of shutting it down')
parser.add_argument('--time-between-shutdowns', type=ordered_pair, default=(20, 30), help="Two ordered integers separated by comma (e.g., 30,60)")
args = parser.parse_args()

logging.basicConfig(filename=args.log_path, filemode='w',
                    level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger(__name__)

# Set seed first
seed = args.seed
if seed == 0:
    import secrets
    seed = secrets.randbits(64) #64 - bit random integer
random.seed(seed)
logger.info(f"Using seed: {seed}")

# Set generator, at the moment only BuzzHouse is available
generator = None
if args.generator == 'buzzhouse':
    generator = BuzzHouseGenerator(args.client_binary, args.client_config)

# Use random server settings sometimes
server_settings = args.server_config
modified_server_settings = False
if server_settings is not None and random.randint(1, 100) <= args.server_settings_prob:
    modified_server_settings = True
    server_settings = modify_server_settings_with_random_properties(server_settings)

# Start the cluster, by using one the server binaries
server_path = os.path.join(tempfile.gettempdir(), "clickhouse")
try:
    os.unlink(server_path)
except FileNotFoundError:
    pass
current_server = args.server_binaries[0]
os.symlink(current_server, server_path)
os.environ["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = server_path

cluster = ClickHouseCluster(__file__)
server = cluster.add_instance("server",
                              with_zookeeper = True,
                              with_minio = True,
                              stay_alive = True,
                              with_dolor = True,
                              main_configs = [server_settings] if server_settings is not None else [],
                              user_configs = [args.user_config] if args.user_config is not None else[],
                              macros = {"shard" : 1, "replica" : 1 })
cluster.start()
logger.info("Starting cluster")
server.wait_start(8)

# Start the load generator
logger.info("Start load generator")
client = generator.run_generator(server)
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
atexit.register(dolor_cleanup)
time.sleep(3)

# This is the main loop, run while client and server are running
while True:
    if client.process.poll() is not None:
        logger.info("Load generator finished")
        break
    try:
        server.query("SELECT 1;")
    except:
        logger.info("The server is not running")
        break

    lower_bound, upper_bound = args.time_between_shutdowns
    time.sleep(int(random.uniform(lower_bound, upper_bound)))
    kill_server = random.randint(1, 100) <= args.kill_server_prob
    logger.info(f"Restart the server with {"kill" if kill_server else "manual shutdown"}")

    server.stop_clickhouse(stop_wait_sec = 10, kill = kill_server)
    # Replace server binary, using a new temporary symlink, then replace the old one
    if len(args.server_binaries) > 1 and random.randint(1, 100) <= args.change_server_version_prob:
        if len(args.server_binaries) == 2:
            current_server = args.server_binaries[0] if current_server == args.server_binaries[1] else args.server_binaries[1]
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
    server.start_clickhouse(start_wait_sec = 10, retry_start = False)

cluster.shutdown()
