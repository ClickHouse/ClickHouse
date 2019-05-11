import os
import os.path as p
import pwd
import re
import subprocess
import shutil
import distutils.dir_util
import socket
import time
import errno
from dicttoxml import dicttoxml
import pymysql
import xml.dom.minidom
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
import psycopg2
import requests
import base64
import pymongo
import urllib

import docker
from docker.errors import ContainerError

from .client import Client, CommandRequest
from .hdfs_api import HDFSApi


HELPERS_DIR = p.dirname(__file__)
DEFAULT_ENV_NAME = 'env_file'

def _create_env_file(path, variables, fname=DEFAULT_ENV_NAME):
    full_path = os.path.join(path, fname)
    with open(full_path, 'w') as f:
        for var, value in variables.items():
            f.write("=".join([var, value]) + "\n")
    return full_path

def subprocess_check_call(args):
    # Uncomment for debugging
    # print('run:', ' ' . join(args))
    subprocess.check_call(args)

def subprocess_call(args):
    # Uncomment for debugging
    # print('run:', ' ' . join(args))
    subprocess.call(args)

def get_odbc_bridge_path():
    path = os.environ.get('CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH')
    if path is None:
        server_path = os.environ.get('CLICKHOUSE_TESTS_SERVER_BIN_PATH')
        if server_path is not None:
            return os.path.join(os.path.dirname(server_path), 'clickhouse-odbc-bridge')
        else:
            return '/usr/bin/clickhouse-odbc-bridge'
    return path


class ClickHouseCluster:
    """ClickHouse cluster with several instances and (possibly) ZooKeeper.

    Add instances with several calls to add_instance(), then start them with the start() call.

    Directories for instances are created in the directory of base_path. After cluster is started,
    these directories will contain logs, database files, docker-compose config, ClickHouse configs etc.
    """

    def __init__(self, base_path, name=None, base_configs_dir=None, server_bin_path=None, client_bin_path=None,
                 odbc_bridge_bin_path=None, zookeeper_config_path=None, custom_dockerd_host=None):
        self.base_dir = p.dirname(base_path)
        self.name = name if name is not None else ''

        self.base_configs_dir = base_configs_dir or os.environ.get('CLICKHOUSE_TESTS_BASE_CONFIG_DIR', '/etc/clickhouse-server/')
        self.server_bin_path = p.realpath(server_bin_path or os.environ.get('CLICKHOUSE_TESTS_SERVER_BIN_PATH', '/usr/bin/clickhouse'))
        self.odbc_bridge_bin_path = p.realpath(odbc_bridge_bin_path or get_odbc_bridge_path())
        self.client_bin_path = p.realpath(client_bin_path or os.environ.get('CLICKHOUSE_TESTS_CLIENT_BIN_PATH', '/usr/bin/clickhouse-client'))
        self.zookeeper_config_path = p.join(self.base_dir, zookeeper_config_path) if zookeeper_config_path else p.join(HELPERS_DIR, 'zookeeper_config.xml')

        self.project_name = pwd.getpwuid(os.getuid()).pw_name + p.basename(self.base_dir) + self.name
        # docker-compose removes everything non-alphanumeric from project names so we do it too.
        self.project_name = re.sub(r'[^a-z0-9]', '', self.project_name.lower())
        self.instances_dir = p.join(self.base_dir, '_instances' + ('' if not self.name else '_' + self.name))

        custom_dockerd_host = custom_dockerd_host or os.environ.get('CLICKHOUSE_TESTS_DOCKERD_HOST')
        self.docker_api_version = os.environ.get("DOCKER_API_VERSION")

        self.base_cmd = ['docker-compose']
        if custom_dockerd_host:
            self.base_cmd += ['--host', custom_dockerd_host]

        self.base_cmd += ['--project-directory', self.base_dir, '--project-name', self.project_name]
        self.base_zookeeper_cmd = None
        self.base_mysql_cmd = []
        self.base_kafka_cmd = []
        self.pre_zookeeper_commands = []
        self.instances = {}
        self.with_zookeeper = False
        self.with_mysql = False
        self.with_postgres = False
        self.with_kafka = False
        self.with_odbc_drivers = False
        self.with_hdfs = False
        self.with_mongo = False

        self.docker_client = None
        self.is_up = False


    def get_client_cmd(self):
        cmd = self.client_bin_path
        if p.basename(cmd) == 'clickhouse':
            cmd += " client"
        return cmd

    def add_instance(self, name, config_dir=None, main_configs=[], user_configs=[], macros={}, with_zookeeper=False, with_mysql=False, with_kafka=False, clickhouse_path_dir=None, with_odbc_drivers=False, with_postgres=False, with_hdfs=False, with_mongo=False, hostname=None, env_variables={}, image="yandex/clickhouse-integration-test", stay_alive=False, ipv4_address=None, ipv6_address=None):
        """Add an instance to the cluster.

        name - the name of the instance directory and the value of the 'instance' macro in ClickHouse.
        config_dir - a directory with config files which content will be copied to /etc/clickhouse-server/ directory
        main_configs - a list of config files that will be added to config.d/ directory
        user_configs - a list of config files that will be added to users.d/ directory
        with_zookeeper - if True, add ZooKeeper configuration to configs and ZooKeeper instances to the cluster.
        """

        if self.is_up:
            raise Exception("Can\'t add instance %s: cluster is already up!" % name)

        if name in self.instances:
            raise Exception("Can\'t add instance `%s': there is already an instance with the same name!" % name)

        instance = ClickHouseInstance(
            self, self.base_dir, name, config_dir, main_configs, user_configs, macros, with_zookeeper,
            self.zookeeper_config_path, with_mysql, with_kafka, with_mongo, self.base_configs_dir, self.server_bin_path,
            self.odbc_bridge_bin_path, clickhouse_path_dir, with_odbc_drivers, hostname=hostname,
            env_variables=env_variables, image=image, stay_alive=stay_alive, ipv4_address=ipv4_address, ipv6_address=ipv6_address)

        self.instances[name] = instance
        self.base_cmd.extend(['--file', instance.docker_compose_path])
        if with_zookeeper and not self.with_zookeeper:
            self.with_zookeeper = True
            self.base_cmd.extend(['--file', p.join(HELPERS_DIR, 'docker_compose_zookeeper.yml')])
            self.base_zookeeper_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file', p.join(HELPERS_DIR, 'docker_compose_zookeeper.yml')]

        if with_mysql and not self.with_mysql:
            self.with_mysql = True
            self.base_cmd.extend(['--file', p.join(HELPERS_DIR, 'docker_compose_mysql.yml')])
            self.base_mysql_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file', p.join(HELPERS_DIR, 'docker_compose_mysql.yml')]

        if with_postgres and not self.with_postgres:
            self.with_postgres = True
            self.base_cmd.extend(['--file', p.join(HELPERS_DIR, 'docker_compose_postgres.yml')])
            self.base_postgres_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file', p.join(HELPERS_DIR, 'docker_compose_postgres.yml')]

        if with_odbc_drivers and not self.with_odbc_drivers:
            self.with_odbc_drivers = True
            if not self.with_mysql:
                self.with_mysql = True
                self.base_cmd.extend(['--file', p.join(HELPERS_DIR, 'docker_compose_mysql.yml')])
                self.base_mysql_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file', p.join(HELPERS_DIR, 'docker_compose_mysql.yml')]
            if not self.with_postgres:
                self.with_postgres = True
                self.base_cmd.extend(['--file', p.join(HELPERS_DIR, 'docker_compose_postgres.yml')])
                self.base_postgres_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file', p.join(HELPERS_DIR, 'docker_compose_postgres.yml')]

        if with_kafka and not self.with_kafka:
            self.with_kafka = True
            self.base_cmd.extend(['--file', p.join(HELPERS_DIR, 'docker_compose_kafka.yml')])
            self.base_kafka_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file', p.join(HELPERS_DIR, 'docker_compose_kafka.yml')]

        if with_hdfs and not self.with_hdfs:
            self.with_hdfs = True
            self.base_cmd.extend(['--file', p.join(HELPERS_DIR, 'docker_compose_hdfs.yml')])
            self.base_hdfs_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file', p.join(HELPERS_DIR, 'docker_compose_hdfs.yml')]

        if with_mongo and not self.with_mongo:
            self.with_mongo = True
            self.base_cmd.extend(['--file', p.join(HELPERS_DIR, 'docker_compose_mongo.yml')])
            self.base_mongo_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file', p.join(HELPERS_DIR, 'docker_compose_mongo.yml')]

        return instance


    def get_instance_docker_id(self, instance_name):
        # According to how docker-compose names containers.
        return self.project_name + '_' + instance_name + '_1'


    def get_instance_ip(self, instance_name):
        docker_id = self.get_instance_docker_id(instance_name)
        handle = self.docker_client.containers.get(docker_id)
        return handle.attrs['NetworkSettings']['Networks'].values()[0]['IPAddress']

    def wait_mysql_to_start(self, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                conn = pymysql.connect(user='root', password='clickhouse', host='127.0.0.1', port=3308)
                conn.close()
                print "Mysql Started"
                return
            except Exception as ex:
                print "Can't connect to MySQL " + str(ex)
                time.sleep(0.5)

        raise Exception("Cannot wait MySQL container")

    def wait_postgres_to_start(self, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                conn_string = "host='localhost' user='postgres' password='mysecretpassword'"
                conn = psycopg2.connect(conn_string)
                conn.close()
                print "Postgres Started"
                return
            except Exception as ex:
                print "Can't connect to Postgres " + str(ex)
                time.sleep(0.5)

        raise Exception("Cannot wait Postgres container")

    def wait_zookeeper_to_start(self, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                for instance in ['zoo1', 'zoo2', 'zoo3']:
                    conn = self.get_kazoo_client(instance)
                    conn.get_children('/')
                print "All instances of ZooKeeper started"
                return
            except Exception as ex:
                print "Can't connect to ZooKeeper " + str(ex)
                time.sleep(0.5)

        raise Exception("Cannot wait ZooKeeper container")

    def wait_hdfs_to_start(self, timeout=60):
        hdfs_api = HDFSApi("root")
        start = time.time()
        while time.time() - start < timeout:
            try:
                hdfs_api.write_data("/somefilewithrandomname222", "1")
                print "Connected to HDFS and SafeMode disabled! "
                return
            except Exception as ex:
                print "Can't connect to HDFS " + str(ex)
                time.sleep(1)

        raise Exception("Can't wait HDFS to start")

    def wait_mongo_to_start(self, timeout=30):
        connection_str = 'mongodb://{user}:{password}@{host}:{port}'.format(
            host='localhost', port='27018', user='root', password='clickhouse')
        connection = pymongo.MongoClient(connection_str)
        start = time.time()
        while time.time() - start < timeout:
            try:
                connection.database_names()
                print "Connected to Mongo dbs:", connection.database_names()
                return
            except Exception as ex:
                print "Can't connect to Mongo " + str(ex)
                time.sleep(1)

    def start(self, destroy_dirs=True):
        if self.is_up:
            return

        # Just in case kill unstopped containers from previous launch
        try:
            if not subprocess_call(['docker-compose', 'kill']):
                subprocess_call(['docker-compose', 'down', '--volumes'])
        except:
            pass

        if destroy_dirs and p.exists(self.instances_dir):
            print "Removing instances dir", self.instances_dir
            shutil.rmtree(self.instances_dir)

        for instance in self.instances.values():
            instance.create_dir(destroy_dir=destroy_dirs)

        self.docker_client = docker.from_env(version=self.docker_api_version)

        if self.with_zookeeper and self.base_zookeeper_cmd:
            subprocess_check_call(self.base_zookeeper_cmd + ['up', '-d', '--force-recreate'])
            for command in self.pre_zookeeper_commands:
                self.run_kazoo_commands_with_retries(command, repeats=5)
            self.wait_zookeeper_to_start(120)

        if self.with_mysql and self.base_mysql_cmd:
            subprocess_check_call(self.base_mysql_cmd + ['up', '-d', '--force-recreate'])
            self.wait_mysql_to_start(120)

        if self.with_postgres and self.base_postgres_cmd:
            subprocess_check_call(self.base_postgres_cmd + ['up', '-d', '--force-recreate'])
            self.wait_postgres_to_start(120)

        if self.with_kafka and self.base_kafka_cmd:
            subprocess_check_call(self.base_kafka_cmd + ['up', '-d', '--force-recreate'])
            self.kafka_docker_id = self.get_instance_docker_id('kafka1')

        if self.with_hdfs and self.base_hdfs_cmd:
            subprocess_check_call(self.base_hdfs_cmd + ['up', '-d', '--force-recreate'])
            self.wait_hdfs_to_start(120)

        if self.with_mongo and self.base_mongo_cmd:
            subprocess_check_call(self.base_mongo_cmd + ['up', '-d', '--force-recreate'])
            self.wait_mongo_to_start(30)

        subprocess_check_call(self.base_cmd + ['up', '-d', '--no-recreate'])

        start_deadline = time.time() + 20.0 # seconds
        for instance in self.instances.itervalues():
            instance.docker_client = self.docker_client
            instance.ip_address = self.get_instance_ip(instance.name)

            instance.wait_for_start(start_deadline)

            instance.client = Client(instance.ip_address, command=self.client_bin_path)

        self.is_up = True


    def shutdown(self, kill=True):
        if kill:
            subprocess_check_call(self.base_cmd + ['kill'])
        subprocess_check_call(self.base_cmd + ['down', '--volumes', '--remove-orphans'])
        self.is_up = False

        self.docker_client = None

        for instance in self.instances.values():
            instance.docker_client = None
            instance.ip_address = None
            instance.client = None


    def get_kazoo_client(self, zoo_instance_name):
        zk = KazooClient(hosts=self.get_instance_ip(zoo_instance_name))
        zk.start()
        return zk


    def run_kazoo_commands_with_retries(self, kazoo_callback, zoo_instance_name = 'zoo1', repeats=1, sleep_for=1):
        for i in range(repeats - 1):
            try:
                kazoo_callback(self.get_kazoo_client(zoo_instance_name))
                return
            except KazooException as e:
                print repr(e)
                time.sleep(sleep_for)

        kazoo_callback(self.get_kazoo_client(zoo_instance_name))


    def add_zookeeper_startup_command(self, command):
        self.pre_zookeeper_commands.append(command)


CLICKHOUSE_START_COMMAND = "clickhouse server --config-file=/etc/clickhouse-server/config.xml --log-file=/var/log/clickhouse-server/clickhouse-server.log --errorlog-file=/var/log/clickhouse-server/clickhouse-server.err.log"

CLICKHOUSE_STAY_ALIVE_COMMAND = 'bash -c "{} --daemon; tail -f /dev/null"'.format(CLICKHOUSE_START_COMMAND)

DOCKER_COMPOSE_TEMPLATE = '''
version: '2.2'
services:
    {name}:
        image: {image}
        hostname: {hostname}
        volumes:
            - {binary_path}:/usr/bin/clickhouse:ro
            - {odbc_bridge_bin_path}:/usr/bin/clickhouse-odbc-bridge:ro
            - {configs_dir}:/etc/clickhouse-server/
            - {db_dir}:/var/lib/clickhouse/
            - {logs_dir}:/var/log/clickhouse-server/
            {odbc_ini_path}
        entrypoint: {entrypoint_cmd}
        cap_add:
            - SYS_PTRACE
        depends_on: {depends_on}
        user: '{user}'
        env_file:
            - {env_file}
        security_opt:
            - label:disable
        {networks}
            {app_net}
                {ipv4_address}
                {ipv6_address}

networks:
  app_net:
    driver: bridge
    enable_ipv6: true
    ipam:
      driver: default
      config:
      - subnet: 10.5.0.0/12
        gateway: 10.5.1.1
      - subnet: 2001:3984:3989::/64
        gateway: 2001:3984:3989::1
'''

class ClickHouseInstance:

    def __init__(
            self, cluster, base_path, name, custom_config_dir, custom_main_configs, custom_user_configs, macros,
            with_zookeeper, zookeeper_config_path, with_mysql, with_kafka, with_mongo, base_configs_dir, server_bin_path, odbc_bridge_bin_path,
            clickhouse_path_dir, with_odbc_drivers, hostname=None, env_variables={}, image="yandex/clickhouse-integration-test",
            stay_alive=False, ipv4_address=None, ipv6_address=None):

        self.name = name
        self.base_cmd = cluster.base_cmd[:]
        self.docker_id = cluster.get_instance_docker_id(self.name)
        self.cluster = cluster
        self.hostname = hostname if hostname is not None else self.name

        self.custom_config_dir = p.abspath(p.join(base_path, custom_config_dir)) if custom_config_dir else None
        self.custom_main_config_paths = [p.abspath(p.join(base_path, c)) for c in custom_main_configs]
        self.custom_user_config_paths = [p.abspath(p.join(base_path, c)) for c in custom_user_configs]
        self.clickhouse_path_dir = p.abspath(p.join(base_path, clickhouse_path_dir)) if clickhouse_path_dir else None
        self.macros = macros if macros is not None else {}
        self.with_zookeeper = with_zookeeper
        self.zookeeper_config_path = zookeeper_config_path

        self.base_configs_dir = base_configs_dir
        self.server_bin_path = server_bin_path
        self.odbc_bridge_bin_path = odbc_bridge_bin_path

        self.with_mysql = with_mysql
        self.with_kafka = with_kafka
        self.with_mongo = with_mongo

        self.path = p.join(self.cluster.instances_dir, name)
        self.docker_compose_path = p.join(self.path, 'docker_compose.yml')
        self.env_variables = env_variables
        if with_odbc_drivers:
            self.odbc_ini_path = os.path.dirname(self.docker_compose_path) + "/odbc.ini:/etc/odbc.ini"
            self.with_mysql = True
        else:
            self.odbc_ini_path = ""

        self.docker_client = None
        self.ip_address = None
        self.client = None
        self.default_timeout = 20.0 # 20 sec
        self.image = image
        self.stay_alive = stay_alive
        self.ipv4_address = ipv4_address
        self.ipv6_address = ipv6_address

    # Connects to the instance via clickhouse-client, sends a query (1st argument) and returns the answer
    def query(self, sql, stdin=None, timeout=None, settings=None, user=None, ignore_error=False):
        return self.client.query(sql, stdin, timeout, settings, user, ignore_error)

    def query_with_retry(self, sql, stdin=None, timeout=None, settings=None, user=None, ignore_error=False, retry_count=20, sleep_time=0.5, check_callback=lambda x: True):
        result = None
        for i in range(retry_count):
            try:
                result = self.query(sql, stdin, timeout, settings, user, ignore_error)
                if check_callback(result):
                    return result
                time.sleep(sleep_time)
            except Exception as ex:
                print "Retry {} got exception {}".format(i + 1, ex)
                time.sleep(sleep_time)

        if result is not None:
            return result
        raise Exception("Can't execute query {}".format(sql))

    # As query() but doesn't wait response and returns response handler
    def get_query_request(self, *args, **kwargs):
        return self.client.get_query_request(*args, **kwargs)

    # Connects to the instance via clickhouse-client, sends a query (1st argument), expects an error and return its code
    def query_and_get_error(self, sql, stdin=None, timeout=None, settings=None, user=None):
        return self.client.query_and_get_error(sql, stdin, timeout, settings, user)

    # Connects to the instance via HTTP interface, sends a query and returns the answer
    def http_query(self, sql, data=None):
        return urllib.urlopen("http://"+self.ip_address+":8123/?query="+urllib.quote(sql,safe=''), data).read()

    def restart_clickhouse(self, stop_start_wait_sec=5):
        if not self.stay_alive:
            raise Exception("clickhouse can be restarted only with stay_alive=True instance")

        self.exec_in_container(["bash", "-c", "pkill clickhouse"], user='root')
        time.sleep(stop_start_wait_sec)
        self.exec_in_container(["bash", "-c", "{} --daemon".format(CLICKHOUSE_START_COMMAND)], user='root')

    def exec_in_container(self, cmd, detach=False, **kwargs):
        container = self.get_docker_handle()
        exec_id = self.docker_client.api.exec_create(container.id, cmd, **kwargs)
        output = self.docker_client.api.exec_start(exec_id, detach=detach)

        output = output.decode('utf8')
        exit_code = self.docker_client.api.exec_inspect(exec_id)['ExitCode']
        if exit_code:
            raise Exception('Cmd "{}" failed! Return code {}. Output: {}'.format(' '.join(cmd), exit_code, output))
        return output

    def contains_in_log(self, substring):
        result = self.exec_in_container(["bash", "-c", "grep '{}' /var/log/clickhouse-server/clickhouse-server.log || true".format(substring)])
        return len(result) > 0

    def copy_file_to_container(self, local_path, dest_path):
        with open(local_path, 'r') as fdata:
            data = fdata.read()
            encoded_data = base64.b64encode(data)
            self.exec_in_container(["bash", "-c", "echo {} | base64 --decode > {}".format(encoded_data, dest_path)])



    def get_docker_handle(self):
        return self.docker_client.containers.get(self.docker_id)


    def stop(self):
        self.get_docker_handle().stop(self.default_timeout)


    def start(self):
        self.get_docker_handle().start()


    def wait_for_start(self, deadline=None, timeout=None):
        start_time = time.time()

        if timeout is not None:
            deadline = start_time + timeout

        while True:
            handle = self.get_docker_handle()
            status = handle.status;
            if status == 'exited':
                raise Exception("Instance `{}' failed to start. Container status: {}, logs: {}".format(self.name, status, handle.logs()))

            current_time = time.time()
            time_left = deadline - current_time
            if deadline is not None and current_time >= deadline:
                raise Exception("Timed out while waiting for instance `{}' with ip address {} to start. "
                                "Container status: {}".format(self.name, self.ip_address, status))

            # Repeatedly poll the instance address until there is something that listens there.
            # Usually it means that ClickHouse is ready to accept queries.
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(time_left)
                sock.connect((self.ip_address, 9000))
                return
            except socket.timeout:
                continue
            except socket.error as e:
                if e.errno == errno.ECONNREFUSED:
                    time.sleep(0.1)
                else:
                    raise
            finally:
                sock.close()


    @staticmethod
    def dict_to_xml(dictionary):
        xml_str = dicttoxml(dictionary, custom_root="yandex", attr_type=False)
        return xml.dom.minidom.parseString(xml_str).toprettyxml()

    @property
    def odbc_drivers(self):
        if self.odbc_ini_path:
            return {
                "SQLite3": {
                    "DSN": "sqlite3_odbc",
                    "Database" : "/tmp/sqliteodbc",
                    "Driver": "/usr/lib/x86_64-linux-gnu/odbc/libsqlite3odbc.so",
                    "Setup": "/usr/lib/x86_64-linux-gnu/odbc/libsqlite3odbc.so",
                },
                "MySQL": {
                    "DSN": "mysql_odbc",
                    "Driver": "/usr/lib/x86_64-linux-gnu/odbc/libmyodbc.so",
                    "Database": "clickhouse",
                    "Uid": "root",
                    "Pwd": "clickhouse",
                    "Server": "mysql1",
                },
                "PostgreSQL": {
                    "DSN": "postgresql_odbc",
                    "Database": "postgres",
                    "UserName": "postgres",
                    "Password": "mysecretpassword",
                    "Port": "5432",
                    "Servername": "postgres1",
                    "Protocol": "9.3",
                    "ReadOnly": "No",
                    "RowVersioning": "No",
                    "ShowSystemTables": "No",
                    "Driver": "/usr/lib/x86_64-linux-gnu/odbc/psqlodbca.so",
                    "Setup": "/usr/lib/x86_64-linux-gnu/odbc/libodbcpsqlS.so",
                    "ConnSettings": "",
                }
            }
        else:
            return {}

    def _create_odbc_config_file(self):
        with open(self.odbc_ini_path.split(':')[0], 'w') as f:
            for driver_setup in self.odbc_drivers.values():
                f.write("[{}]\n".format(driver_setup["DSN"]))
                for key, value in driver_setup.items():
                    if key != "DSN":
                        f.write(key + "=" + value + "\n")

    def create_dir(self, destroy_dir=True):
        """Create the instance directory and all the needed files there."""

        if destroy_dir:
            self.destroy_dir()
        elif p.exists(self.path):
            return

        os.makedirs(self.path)

        configs_dir = p.abspath(p.join(self.path, 'configs'))
        os.mkdir(configs_dir)

        shutil.copy(p.join(self.base_configs_dir, 'config.xml'), configs_dir)
        shutil.copy(p.join(self.base_configs_dir, 'users.xml'), configs_dir)

        # used by all utils with any config
        conf_d_dir = p.abspath(p.join(configs_dir, 'conf.d'))
        # used by server with main config.xml
        config_d_dir = p.abspath(p.join(configs_dir, 'config.d'))
        users_d_dir = p.abspath(p.join(configs_dir, 'users.d'))
        os.mkdir(conf_d_dir)
        os.mkdir(config_d_dir)
        os.mkdir(users_d_dir)

        shutil.copy(p.join(HELPERS_DIR, 'common_instance_config.xml'), config_d_dir)

        # Generate and write macros file
        macros = self.macros.copy()
        macros['instance'] = self.name
        with open(p.join(config_d_dir, 'macros.xml'), 'w') as macros_config:
            macros_config.write(self.dict_to_xml({"macros" : macros}))

        # Put ZooKeeper config
        if self.with_zookeeper:
            shutil.copy(self.zookeeper_config_path, conf_d_dir)

        # Copy config dir
        if self.custom_config_dir:
            distutils.dir_util.copy_tree(self.custom_config_dir, configs_dir)

        # Copy config.d configs
        for path in self.custom_main_config_paths:
            shutil.copy(path, config_d_dir)

        # Copy users.d configs
        for path in self.custom_user_config_paths:
            shutil.copy(path, users_d_dir)

        db_dir = p.abspath(p.join(self.path, 'database'))
        os.mkdir(db_dir)
        if self.clickhouse_path_dir is not None:
            distutils.dir_util.copy_tree(self.clickhouse_path_dir, db_dir)

        logs_dir = p.abspath(p.join(self.path, 'logs'))
        os.mkdir(logs_dir)

        depends_on = []

        if self.with_mysql:
            depends_on.append("mysql1")

        if self.with_kafka:
            depends_on.append("kafka1")

        if self.with_zookeeper:
            depends_on.append("zoo1")
            depends_on.append("zoo2")
            depends_on.append("zoo3")

        env_file = _create_env_file(os.path.dirname(self.docker_compose_path), self.env_variables)

        odbc_ini_path = ""
        if self.odbc_ini_path:
            self._create_odbc_config_file()
            odbc_ini_path = '- ' + self.odbc_ini_path

        entrypoint_cmd = CLICKHOUSE_START_COMMAND

        if self.stay_alive:
            entrypoint_cmd = CLICKHOUSE_STAY_ALIVE_COMMAND

        ipv4_address = ipv6_address = ""
        if self.ipv4_address is None and self.ipv6_address is None:
            networks = ""
            app_net = ""
        else:
            networks = "networks:"
            app_net = "app_net:"
            if self.ipv4_address is not None:
                ipv4_address = "ipv4_address: " + self.ipv4_address
            if self.ipv6_address is not None:
                ipv6_address = "ipv6_address: " + self.ipv6_address

        with open(self.docker_compose_path, 'w') as docker_compose:
            docker_compose.write(DOCKER_COMPOSE_TEMPLATE.format(
                image=self.image,
                name=self.name,
                hostname=self.hostname,
                binary_path=self.server_bin_path,
                odbc_bridge_bin_path=self.odbc_bridge_bin_path,
                configs_dir=configs_dir,
                config_d_dir=config_d_dir,
                db_dir=db_dir,
                logs_dir=logs_dir,
                depends_on=str(depends_on),
                user=os.getuid(),
                env_file=env_file,
                odbc_ini_path=odbc_ini_path,
                entrypoint_cmd=entrypoint_cmd,
                networks=networks,
                app_net=app_net,
                ipv4_address=ipv4_address,
                ipv6_address=ipv6_address,
            ))


    def destroy_dir(self):
        if p.exists(self.path):
            shutil.rmtree(self.path)
