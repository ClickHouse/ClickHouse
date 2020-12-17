import base64
import errno
import http.client
import logging
import os
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

import cassandra.cluster
import docker
import psycopg2
import pymongo
import pymysql
import requests
import xml.dom.minidom
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from dicttoxml import dicttoxml
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
from minio import Minio

from .client import Client
from .hdfs_api import HDFSApi

HELPERS_DIR = p.dirname(__file__)
CLICKHOUSE_ROOT_DIR = p.join(p.dirname(__file__), "../../..")
LOCAL_DOCKER_COMPOSE_DIR = p.join(CLICKHOUSE_ROOT_DIR, "docker/test/integration/runner/compose/")
DEFAULT_ENV_NAME = 'env_file'

SANITIZER_SIGN = "=================="


def _create_env_file(path, variables, fname=DEFAULT_ENV_NAME):
    full_path = os.path.join(path, fname)
    with open(full_path, 'w') as f:
        for var, value in list(variables.items()):
            f.write("=".join([var, value]) + "\n")
    return full_path

def subprocess_check_call(args):
    # Uncomment for debugging
    # print('run:', ' ' . join(args))
    subprocess.check_call(args)


def subprocess_call(args):
    # Uncomment for debugging..;
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


def get_docker_compose_path():
    compose_path = os.environ.get('DOCKER_COMPOSE_DIR')
    if compose_path is not None:
        return os.path.dirname(compose_path)
    else:
        if os.path.exists(os.path.dirname('/compose/')):
            return os.path.dirname('/compose/')  # default in docker runner container
        else:
            print(("Fallback docker_compose_path to LOCAL_DOCKER_COMPOSE_DIR: {}".format(LOCAL_DOCKER_COMPOSE_DIR)))
            return LOCAL_DOCKER_COMPOSE_DIR


class ClickHouseCluster:
    """ClickHouse cluster with several instances and (possibly) ZooKeeper.

    Add instances with several calls to add_instance(), then start them with the start() call.

    Directories for instances are created in the directory of base_path. After cluster is started,
    these directories will contain logs, database files, docker-compose config, ClickHouse configs etc.
    """

    def __init__(self, base_path, name=None, base_config_dir=None, server_bin_path=None, client_bin_path=None,
                 odbc_bridge_bin_path=None, zookeeper_config_path=None, custom_dockerd_host=None):
        for param in list(os.environ.keys()):
            print("ENV %40s %s" % (param, os.environ[param]))
        self.base_dir = p.dirname(base_path)
        self.name = name if name is not None else ''

        self.base_config_dir = base_config_dir or os.environ.get('CLICKHOUSE_TESTS_BASE_CONFIG_DIR',
                                                                 '/etc/clickhouse-server/')
        self.server_bin_path = p.realpath(
            server_bin_path or os.environ.get('CLICKHOUSE_TESTS_SERVER_BIN_PATH', '/usr/bin/clickhouse'))
        self.odbc_bridge_bin_path = p.realpath(odbc_bridge_bin_path or get_odbc_bridge_path())
        self.client_bin_path = p.realpath(
            client_bin_path or os.environ.get('CLICKHOUSE_TESTS_CLIENT_BIN_PATH', '/usr/bin/clickhouse-client'))
        self.zookeeper_config_path = p.join(self.base_dir, zookeeper_config_path) if zookeeper_config_path else p.join(
            HELPERS_DIR, 'zookeeper_config.xml')

        self.project_name = pwd.getpwuid(os.getuid()).pw_name + p.basename(self.base_dir) + self.name
        # docker-compose removes everything non-alphanumeric from project names so we do it too.
        self.project_name = re.sub(r'[^a-z0-9]', '', self.project_name.lower())
        self.instances_dir = p.join(self.base_dir, '_instances' + ('' if not self.name else '_' + self.name))
        self.docker_logs_path = p.join(self.instances_dir, 'docker.log')

        custom_dockerd_host = custom_dockerd_host or os.environ.get('CLICKHOUSE_TESTS_DOCKERD_HOST')
        self.docker_api_version = os.environ.get("DOCKER_API_VERSION")
        self.docker_base_tag = os.environ.get("DOCKER_BASE_TAG", "latest")

        self.base_cmd = ['docker-compose']
        if custom_dockerd_host:
            self.base_cmd += ['--host', custom_dockerd_host]

        self.base_cmd += ['--project-directory', self.base_dir, '--project-name', self.project_name]
        self.base_zookeeper_cmd = None
        self.base_mysql_cmd = []
        self.base_kafka_cmd = []
        self.base_kerberized_kafka_cmd = []
        self.base_rabbitmq_cmd = []
        self.base_cassandra_cmd = []
        self.pre_zookeeper_commands = []
        self.instances = {}
        self.with_zookeeper = False
        self.with_mysql = False
        self.with_postgres = False
        self.with_kafka = False
        self.with_kerberized_kafka = False
        self.with_rabbitmq = False
        self.with_odbc_drivers = False
        self.with_hdfs = False
        self.with_mongo = False
        self.with_net_trics = False
        self.with_redis = False
        self.with_cassandra = False

        self.with_minio = False
        self.minio_certs_dir = None
        self.minio_host = "minio1"
        self.minio_bucket = "root"
        self.minio_port = 9001
        self.minio_client = None  # type: Minio
        self.minio_redirect_host = "proxy1"
        self.minio_redirect_port = 8080

        # available when with_kafka == True
        self.schema_registry_client = None
        self.schema_registry_host = "schema-registry"
        self.schema_registry_port = 8081

        self.zookeeper_use_tmpfs = True

        self.docker_client = None
        self.is_up = False
        print("CLUSTER INIT base_config_dir:{}".format(self.base_config_dir))

    def get_client_cmd(self):
        cmd = self.client_bin_path
        if p.basename(cmd) == 'clickhouse':
            cmd += " client"
        return cmd

    def add_instance(self, name, base_config_dir=None, main_configs=None, user_configs=None, dictionaries=None,
                     macros=None,
                     with_zookeeper=False, with_mysql=False, with_kafka=False, with_kerberized_kafka=False, with_rabbitmq=False,
                     clickhouse_path_dir=None,
                     with_odbc_drivers=False, with_postgres=False, with_hdfs=False, with_mongo=False,
                     with_redis=False, with_minio=False, with_cassandra=False,
                     hostname=None, env_variables=None, image="yandex/clickhouse-integration-test", tag=None,
                     stay_alive=False, ipv4_address=None, ipv6_address=None, with_installed_binary=False, tmpfs=None,
                     zookeeper_docker_compose_path=None, zookeeper_use_tmpfs=True, minio_certs_dir=None):
        """Add an instance to the cluster.

        name - the name of the instance directory and the value of the 'instance' macro in ClickHouse.
        base_config_dir - a directory with config.xml and users.xml files which will be copied to /etc/clickhouse-server/ directory
        main_configs - a list of config files that will be added to config.d/ directory
        user_configs - a list of config files that will be added to users.d/ directory
        with_zookeeper - if True, add ZooKeeper configuration to configs and ZooKeeper instances to the cluster.
        """

        if self.is_up:
            raise Exception("Can\'t add instance %s: cluster is already up!" % name)

        if name in self.instances:
            raise Exception("Can\'t add instance `%s': there is already an instance with the same name!" % name)

        if tag is None:
            tag = self.docker_base_tag

        instance = ClickHouseInstance(
            cluster=self,
            base_path=self.base_dir,
            name=name,
            base_config_dir=base_config_dir if base_config_dir else self.base_config_dir,
            custom_main_configs=main_configs or [],
            custom_user_configs=user_configs or [],
            custom_dictionaries=dictionaries or [],
            macros=macros or {},
            with_zookeeper=with_zookeeper,
            zookeeper_config_path=self.zookeeper_config_path,
            with_mysql=with_mysql,
            with_kafka=with_kafka,
            with_kerberized_kafka=with_kerberized_kafka,
            with_rabbitmq=with_rabbitmq,
            with_mongo=with_mongo,
            with_redis=with_redis,
            with_minio=with_minio,
            with_cassandra=with_cassandra,
            server_bin_path=self.server_bin_path,
            odbc_bridge_bin_path=self.odbc_bridge_bin_path,
            clickhouse_path_dir=clickhouse_path_dir,
            with_odbc_drivers=with_odbc_drivers,
            hostname=hostname,
            env_variables=env_variables or {},
            image=image,
            tag=tag,
            stay_alive=stay_alive,
            ipv4_address=ipv4_address,
            ipv6_address=ipv6_address,
            with_installed_binary=with_installed_binary,
            tmpfs=tmpfs or [])

        docker_compose_yml_dir = get_docker_compose_path()

        self.instances[name] = instance
        if ipv4_address is not None or ipv6_address is not None:
            self.with_net_trics = True
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_net.yml')])

        self.base_cmd.extend(['--file', instance.docker_compose_path])

        cmds = []
        if with_zookeeper and not self.with_zookeeper:
            if not zookeeper_docker_compose_path:
                zookeeper_docker_compose_path = p.join(docker_compose_yml_dir, 'docker_compose_zookeeper.yml')

            self.with_zookeeper = True
            self.zookeeper_use_tmpfs = zookeeper_use_tmpfs
            self.base_cmd.extend(['--file', zookeeper_docker_compose_path])
            self.base_zookeeper_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file', zookeeper_docker_compose_path]
            cmds.append(self.base_zookeeper_cmd)

        if with_mysql and not self.with_mysql:
            self.with_mysql = True
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_mysql.yml')])
            self.base_mysql_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                   self.project_name, '--file',
                                   p.join(docker_compose_yml_dir, 'docker_compose_mysql.yml')]

            cmds.append(self.base_mysql_cmd)

        if with_postgres and not self.with_postgres:
            self.with_postgres = True
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_postgres.yml')])
            self.base_postgres_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                      self.project_name, '--file',
                                      p.join(docker_compose_yml_dir, 'docker_compose_postgres.yml')]
            cmds.append(self.base_postgres_cmd)

        if with_odbc_drivers and not self.with_odbc_drivers:
            self.with_odbc_drivers = True
            if not self.with_mysql:
                self.with_mysql = True
                self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_mysql.yml')])
                self.base_mysql_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file',
                                       p.join(docker_compose_yml_dir, 'docker_compose_mysql.yml')]
                cmds.append(self.base_mysql_cmd)

            if not self.with_postgres:
                self.with_postgres = True
                self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_postgres.yml')])
                self.base_postgres_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                          self.project_name, '--file',
                                          p.join(docker_compose_yml_dir, 'docker_compose_postgres.yml')]
                cmds.append(self.base_postgres_cmd)

        if with_kafka and not self.with_kafka:
            self.with_kafka = True
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_kafka.yml')])
            self.base_kafka_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                   self.project_name, '--file',
                                   p.join(docker_compose_yml_dir, 'docker_compose_kafka.yml')]
            cmds.append(self.base_kafka_cmd)

        if with_kerberized_kafka and not self.with_kerberized_kafka:
            self.with_kerberized_kafka = True
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_kerberized_kafka.yml')])
            self.base_kerberized_kafka_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                   self.project_name, '--file', p.join(docker_compose_yml_dir, 'docker_compose_kerberized_kafka.yml')]
            cmds.append(self.base_kerberized_kafka_cmd)

        if with_rabbitmq and not self.with_rabbitmq:
            self.with_rabbitmq = True
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_rabbitmq.yml')])
            self.base_rabbitmq_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                      self.project_name, '--file',
                                      p.join(docker_compose_yml_dir, 'docker_compose_rabbitmq.yml')]
            cmds.append(self.base_rabbitmq_cmd)

        if with_hdfs and not self.with_hdfs:
            self.with_hdfs = True
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_hdfs.yml')])
            self.base_hdfs_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                  self.project_name, '--file',
                                  p.join(docker_compose_yml_dir, 'docker_compose_hdfs.yml')]
            cmds.append(self.base_hdfs_cmd)

        if with_mongo and not self.with_mongo:
            self.with_mongo = True
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_mongo.yml')])
            self.base_mongo_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                   self.project_name, '--file',
                                   p.join(docker_compose_yml_dir, 'docker_compose_mongo.yml')]
            cmds.append(self.base_mongo_cmd)

        if self.with_net_trics:
            for cmd in cmds:
                cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_net.yml')])

        if with_redis and not self.with_redis:
            self.with_redis = True
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_redis.yml')])
            self.base_redis_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                   self.project_name, '--file',
                                   p.join(docker_compose_yml_dir, 'docker_compose_redis.yml')]

        if with_minio and not self.with_minio:
            self.with_minio = True
            self.minio_certs_dir = minio_certs_dir
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_minio.yml')])
            self.base_minio_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                   self.project_name, '--file',
                                   p.join(docker_compose_yml_dir, 'docker_compose_minio.yml')]
            cmds.append(self.base_minio_cmd)

        if with_cassandra and not self.with_cassandra:
            self.with_cassandra = True
            self.base_cmd.extend(['--file', p.join(docker_compose_yml_dir, 'docker_compose_cassandra.yml')])
            self.base_cassandra_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file',
                                       p.join(docker_compose_yml_dir, 'docker_compose_cassandra.yml')]

        return instance

    def get_instance_docker_id(self, instance_name):
        # According to how docker-compose names containers.
        return self.project_name + '_' + instance_name + '_1'

    def _replace(self, path, what, to):
        with open(path, 'r') as p:
            data = p.read()
        data = data.replace(what, to)
        with open(path, 'w') as p:
            p.write(data)

    def restart_instance_with_ip_change(self, node, new_ip):
        if '::' in new_ip:
            if node.ipv6_address is None:
                raise Exception("You should specity ipv6_address in add_node method")
            self._replace(node.docker_compose_path, node.ipv6_address, new_ip)
            node.ipv6_address = new_ip
        else:
            if node.ipv4_address is None:
                raise Exception("You should specity ipv4_address in add_node method")
            self._replace(node.docker_compose_path, node.ipv4_address, new_ip)
            node.ipv4_address = new_ip
        subprocess.check_call(self.base_cmd + ["stop", node.name])
        subprocess.check_call(self.base_cmd + ["rm", "--force", "--stop", node.name])
        subprocess.check_call(self.base_cmd + ["up", "--force-recreate", "--no-deps", "-d", node.name])
        node.ip_address = self.get_instance_ip(node.name)
        node.client = Client(node.ip_address, command=self.client_bin_path)
        start_deadline = time.time() + 20.0  # seconds
        node.wait_for_start(start_deadline)
        return node

    def get_instance_ip(self, instance_name):
        docker_id = self.get_instance_docker_id(instance_name)
        handle = self.docker_client.containers.get(docker_id)
        return list(handle.attrs['NetworkSettings']['Networks'].values())[0]['IPAddress']

    def get_container_id(self, instance_name):
        docker_id = self.get_instance_docker_id(instance_name)
        handle = self.docker_client.containers.get(docker_id)
        return handle.attrs['Id']

    def get_container_logs(self, instance_name):
        container_id = self.get_container_id(instance_name)
        return self.docker_client.api.logs(container_id).decode()

    def exec_in_container(self, container_id, cmd, detach=False, nothrow=False, **kwargs):
        exec_id = self.docker_client.api.exec_create(container_id, cmd, **kwargs)
        output = self.docker_client.api.exec_start(exec_id, detach=detach)

        exit_code = self.docker_client.api.exec_inspect(exec_id)['ExitCode']
        if exit_code:
            container_info = self.docker_client.api.inspect_container(container_id)
            image_id = container_info.get('Image')
            image_info = self.docker_client.api.inspect_image(image_id)
            print(("Command failed in container {}: ".format(container_id)))
            pprint.pprint(container_info)
            print("")
            print(("Container {} uses image {}: ".format(container_id, image_id)))
            pprint.pprint(image_info)
            print("")
            message = 'Cmd "{}" failed in container {}. Return code {}. Output: {}'.format(' '.join(cmd), container_id,
                                                                                           exit_code, output)
            if nothrow:
                print(message)
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
            self.exec_in_container(container_id,
                                   ["bash", "-c", "echo {} | base64 --decode > {}".format(encodedStr, dest_path)],
                                   user='root')

    def wait_mysql_to_start(self, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                conn = pymysql.connect(user='root', password='clickhouse', host='127.0.0.1', port=3308)
                conn.close()
                print("Mysql Started")
                return
            except Exception as ex:
                print("Can't connect to MySQL " + str(ex))
                time.sleep(0.5)

        subprocess_call(['docker-compose', 'ps', '--services', '--all'])
        raise Exception("Cannot wait MySQL container")

    def wait_postgres_to_start(self, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                conn_string = "host='localhost' user='postgres' password='mysecretpassword'"
                conn = psycopg2.connect(conn_string)
                conn.close()
                print("Postgres Started")
                return
            except Exception as ex:
                print("Can't connect to Postgres " + str(ex))
                time.sleep(0.5)

        raise Exception("Cannot wait Postgres container")

    def wait_zookeeper_to_start(self, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                for instance in ['zoo1', 'zoo2', 'zoo3']:
                    conn = self.get_kazoo_client(instance)
                    conn.get_children('/')
                print("All instances of ZooKeeper started")
                return
            except Exception as ex:
                print("Can't connect to ZooKeeper " + str(ex))
                time.sleep(0.5)

        raise Exception("Cannot wait ZooKeeper container")

    def wait_hdfs_to_start(self, timeout=60):
        hdfs_api = HDFSApi("root")
        start = time.time()
        while time.time() - start < timeout:
            try:
                hdfs_api.write_data("/somefilewithrandomname222", "1")
                print("Connected to HDFS and SafeMode disabled! ")
                return
            except Exception as ex:
                print("Can't connect to HDFS " + str(ex))
                time.sleep(1)

        raise Exception("Can't wait HDFS to start")

    def wait_mongo_to_start(self, timeout=30):
        connection_str = 'mongodb://{user}:{password}@{host}:{port}'.format(
            host='localhost', port='27018', user='root', password='clickhouse')
        connection = pymongo.MongoClient(connection_str)
        start = time.time()
        while time.time() - start < timeout:
            try:
                connection.list_database_names()
                print("Connected to Mongo dbs:", connection.database_names())
                return
            except Exception as ex:
                print("Can't connect to Mongo " + str(ex))
                time.sleep(1)

    def wait_minio_to_start(self, timeout=30, secure=False):
        minio_client = Minio('localhost:9001',
                             access_key='minio',
                             secret_key='minio123',
                             secure=secure)
        start = time.time()
        while time.time() - start < timeout:
            try:
                minio_client.list_buckets()

                print("Connected to Minio.")

                if minio_client.bucket_exists(self.minio_bucket):
                    minio_client.remove_bucket(self.minio_bucket)

                minio_client.make_bucket(self.minio_bucket)

                print(("S3 bucket '%s' created", self.minio_bucket))

                self.minio_client = minio_client
                return
            except Exception as ex:
                print(("Can't connect to Minio: %s", str(ex)))
                time.sleep(1)

        raise Exception("Can't wait Minio to start")

    def wait_schema_registry_to_start(self, timeout=10):
        sr_client = CachedSchemaRegistryClient('http://localhost:8081')
        start = time.time()
        while time.time() - start < timeout:
            try:
                sr_client._send_request(sr_client.url)
                self.schema_registry_client = sr_client
                print("Connected to SchemaRegistry")
                return
            except Exception as ex:
                print(("Can't connect to SchemaRegistry: %s", str(ex)))
                time.sleep(1)

    def wait_cassandra_to_start(self, timeout=30):
        cass_client = cassandra.cluster.Cluster(["localhost"], port="9043")
        start = time.time()
        while time.time() - start < timeout:
            try:
                cass_client.connect()
                logging.info("Connected to Cassandra")
                return
            except Exception as ex:
                logging.warning("Can't connect to Cassandra: %s", str(ex))
                time.sleep(1)

    def start(self, destroy_dirs=True):
        print("Cluster start called. is_up={}, destroy_dirs={}".format(self.is_up, destroy_dirs))
        if self.is_up:
            return

        # Just in case kill unstopped containers from previous launch
        try:
            print("Trying to kill unstopped containers...")

            if not subprocess_call(['docker-compose', 'kill']):
                subprocess_call(['docker-compose', 'down', '--volumes'])
            print("Unstopped containers killed")
        except:
            pass

        try:
            if destroy_dirs and p.exists(self.instances_dir):
                print(("Removing instances dir %s", self.instances_dir))
                shutil.rmtree(self.instances_dir)

            for instance in list(self.instances.values()):
                print(('Setup directory for instance: {} destroy_dirs: {}'.format(instance.name, destroy_dirs)))
                instance.create_dir(destroy_dir=destroy_dirs)

            self.docker_client = docker.from_env(version=self.docker_api_version)

            common_opts = ['up', '-d', '--force-recreate']

            if self.with_zookeeper and self.base_zookeeper_cmd:
                print('Setup ZooKeeper')
                env = os.environ.copy()
                if not self.zookeeper_use_tmpfs:
                    env['ZK_FS'] = 'bind'
                    for i in range(1, 4):
                        zk_data_path = self.instances_dir + '/zkdata' + str(i)
                        zk_log_data_path = self.instances_dir + '/zklog' + str(i)
                        if not os.path.exists(zk_data_path):
                            os.mkdir(zk_data_path)
                        if not os.path.exists(zk_log_data_path):
                            os.mkdir(zk_log_data_path)
                        env['ZK_DATA' + str(i)] = zk_data_path
                        env['ZK_DATA_LOG' + str(i)] = zk_log_data_path
                subprocess.check_call(self.base_zookeeper_cmd + common_opts, env=env)
                for command in self.pre_zookeeper_commands:
                    self.run_kazoo_commands_with_retries(command, repeats=5)
                self.wait_zookeeper_to_start(120)

            if self.with_mysql and self.base_mysql_cmd:
                print('Setup MySQL')
                subprocess_check_call(self.base_mysql_cmd + common_opts)
                self.wait_mysql_to_start(120)

            if self.with_postgres and self.base_postgres_cmd:
                print('Setup Postgres')
                subprocess_check_call(self.base_postgres_cmd + common_opts)
                self.wait_postgres_to_start(120)

            if self.with_kafka and self.base_kafka_cmd:
                print('Setup Kafka')
                subprocess_check_call(self.base_kafka_cmd + common_opts + ['--renew-anon-volumes'])
                self.kafka_docker_id = self.get_instance_docker_id('kafka1')
                self.wait_schema_registry_to_start(120)

            if self.with_kerberized_kafka and self.base_kerberized_kafka_cmd:
                env = os.environ.copy()
                env['KERBERIZED_KAFKA_DIR'] = instance.path + '/'
                subprocess.check_call(self.base_kerberized_kafka_cmd + common_opts + ['--renew-anon-volumes'], env=env)
                self.kerberized_kafka_docker_id = self.get_instance_docker_id('kerberized_kafka1')
            if self.with_rabbitmq and self.base_rabbitmq_cmd:
                subprocess_check_call(self.base_rabbitmq_cmd + common_opts + ['--renew-anon-volumes'])
                self.rabbitmq_docker_id = self.get_instance_docker_id('rabbitmq1')

            if self.with_hdfs and self.base_hdfs_cmd:
                print('Setup HDFS')
                subprocess_check_call(self.base_hdfs_cmd + common_opts)
                self.wait_hdfs_to_start(120)

            if self.with_mongo and self.base_mongo_cmd:
                print('Setup Mongo')
                subprocess_check_call(self.base_mongo_cmd + common_opts)
                self.wait_mongo_to_start(30)

            if self.with_redis and self.base_redis_cmd:
                print('Setup Redis')
                subprocess_check_call(self.base_redis_cmd + ['up', '-d', '--force-recreate'])
                time.sleep(10)

            if self.with_minio and self.base_minio_cmd:
                env = os.environ.copy()
                prev_ca_certs = os.environ.get('SSL_CERT_FILE')
                if self.minio_certs_dir:
                    minio_certs_dir = p.join(self.base_dir, self.minio_certs_dir)
                    env['MINIO_CERTS_DIR'] = minio_certs_dir
                    # Minio client (urllib3) uses SSL_CERT_FILE for certificate validation.
                    os.environ['SSL_CERT_FILE'] = p.join(minio_certs_dir, 'public.crt')
                else:
                    # Attach empty certificates directory to ensure non-secure mode.
                    minio_certs_dir = p.join(self.instances_dir, 'empty_minio_certs_dir')
                    os.mkdir(minio_certs_dir)
                    env['MINIO_CERTS_DIR'] = minio_certs_dir

                minio_start_cmd = self.base_minio_cmd + common_opts

                logging.info("Trying to create Minio instance by command %s", ' '.join(map(str, minio_start_cmd)))
                subprocess.check_call(minio_start_cmd, env=env)

                try:
                    logging.info("Trying to connect to Minio...")
                    self.wait_minio_to_start(secure=self.minio_certs_dir is not None)
                finally:
                    # Safely return previous value of SSL_CERT_FILE environment variable.
                    if self.minio_certs_dir:
                        if prev_ca_certs:
                            os.environ['SSL_CERT_FILE'] = prev_ca_certs
                        else:
                            os.environ.pop('SSL_CERT_FILE')

            if self.with_cassandra and self.base_cassandra_cmd:
                subprocess_check_call(self.base_cassandra_cmd + ['up', '-d', '--force-recreate'])
                self.wait_cassandra_to_start()

            clickhouse_start_cmd = self.base_cmd + ['up', '-d', '--no-recreate']
            print(("Trying to create ClickHouse instance by command %s", ' '.join(map(str, clickhouse_start_cmd))))
            subprocess.check_output(clickhouse_start_cmd)
            print("ClickHouse instance created")

            start_deadline = time.time() + 20.0  # seconds
            for instance in self.instances.values():
                instance.docker_client = self.docker_client
                instance.ip_address = self.get_instance_ip(instance.name)

                print("Waiting for ClickHouse start...")
                instance.wait_for_start(start_deadline)
                print("ClickHouse started")

                instance.client = Client(instance.ip_address, command=self.client_bin_path)

            self.is_up = True

        except BaseException as e:
            print("Failed to start cluster: ")
            print(str(e))
            print(traceback.print_exc())
            raise

    def shutdown(self, kill=True):
        sanitizer_assert_instance = None
        with open(self.docker_logs_path, "w+") as f:
            try:
                subprocess.check_call(self.base_cmd + ['logs'], stdout=f)
            except Exception as e:
                print("Unable to get logs from docker.")
            f.seek(0)
            for line in f:
                if SANITIZER_SIGN in line:
                    sanitizer_assert_instance = line.split('|')[0].strip()
                    break

        if kill:
            try:
                subprocess_check_call(self.base_cmd + ['kill'])
            except Exception as e:
                print("Kill command failed durung shutdown. {}".format(repr(e)))

        try:
            subprocess_check_call(self.base_cmd + ['down', '--volumes', '--remove-orphans'])
        except Exception as e:
            print("Down + remove orphans failed durung shutdown. {}".format(repr(e)))

        self.is_up = False

        self.docker_client = None

        for instance in list(self.instances.values()):
            instance.docker_client = None
            instance.ip_address = None
            instance.client = None

        if not self.zookeeper_use_tmpfs:
            for i in range(1, 4):
                zk_data_path = self.instances_dir + '/zkdata' + str(i)
                zk_log_data_path = self.instances_dir + '/zklog' + str(i)
                if os.path.exists(zk_data_path):
                    shutil.rmtree(zk_data_path)
                if os.path.exists(zk_log_data_path):
                    shutil.rmtree(zk_log_data_path)

        if sanitizer_assert_instance is not None:
            raise Exception(
                "Sanitizer assert found in {} for instance {}".format(self.docker_logs_path, sanitizer_assert_instance))

    def pause_container(self, instance_name):
        subprocess_check_call(self.base_cmd + ['pause', instance_name])

    #    subprocess_check_call(self.base_cmd + ['kill', '-s SIGSTOP', instance_name])

    def unpause_container(self, instance_name):
        subprocess_check_call(self.base_cmd + ['unpause', instance_name])

    #    subprocess_check_call(self.base_cmd + ['kill', '-s SIGCONT', instance_name])

    def open_bash_shell(self, instance_name):
        os.system(' '.join(self.base_cmd + ['exec', instance_name, '/bin/bash']))

    def get_kazoo_client(self, zoo_instance_name):
        zk = KazooClient(hosts=self.get_instance_ip(zoo_instance_name))
        zk.start()
        return zk

    def run_kazoo_commands_with_retries(self, kazoo_callback, zoo_instance_name='zoo1', repeats=1, sleep_for=1):
        for i in range(repeats - 1):
            try:
                kazoo_callback(self.get_kazoo_client(zoo_instance_name))
                return
            except KazooException as e:
                print(repr(e))
                time.sleep(sleep_for)

        kazoo_callback(self.get_kazoo_client(zoo_instance_name))

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


CLICKHOUSE_START_COMMAND = "clickhouse server --config-file=/etc/clickhouse-server/config.xml --log-file=/var/log/clickhouse-server/clickhouse-server.log --errorlog-file=/var/log/clickhouse-server/clickhouse-server.err.log"

CLICKHOUSE_STAY_ALIVE_COMMAND = 'bash -c "{} --daemon; tail -f /dev/null"'.format(CLICKHOUSE_START_COMMAND)

DOCKER_COMPOSE_TEMPLATE = '''
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
            {binary_volume}
            {odbc_bridge_volume}
            {odbc_ini_path}
            {keytab_path}
            {krb5_conf}
        entrypoint: {entrypoint_cmd}
        tmpfs: {tmpfs}
        cap_add:
            - SYS_PTRACE
            - NET_ADMIN
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
'''


class ClickHouseInstance:

    def __init__(
            self, cluster, base_path, name, base_config_dir, custom_main_configs, custom_user_configs,
            custom_dictionaries,
            macros, with_zookeeper, zookeeper_config_path, with_mysql, with_kafka, with_kerberized_kafka, with_rabbitmq, with_mongo,
            with_redis, with_minio,
            with_cassandra, server_bin_path, odbc_bridge_bin_path, clickhouse_path_dir, with_odbc_drivers,
            hostname=None, env_variables=None,
            image="yandex/clickhouse-integration-test", tag="latest",
            stay_alive=False, ipv4_address=None, ipv6_address=None, with_installed_binary=False, tmpfs=None):

        self.name = name
        self.base_cmd = cluster.base_cmd
        self.docker_id = cluster.get_instance_docker_id(self.name)
        self.cluster = cluster
        self.hostname = hostname if hostname is not None else self.name

        self.tmpfs = tmpfs or []
        self.base_config_dir = p.abspath(p.join(base_path, base_config_dir)) if base_config_dir else None
        self.custom_main_config_paths = [p.abspath(p.join(base_path, c)) for c in custom_main_configs]
        self.custom_user_config_paths = [p.abspath(p.join(base_path, c)) for c in custom_user_configs]
        self.custom_dictionaries_paths = [p.abspath(p.join(base_path, c)) for c in custom_dictionaries]
        self.clickhouse_path_dir = p.abspath(p.join(base_path, clickhouse_path_dir)) if clickhouse_path_dir else None
        self.kerberos_secrets_dir = p.abspath(p.join(base_path, 'secrets'))
        self.macros = macros if macros is not None else {}
        self.with_zookeeper = with_zookeeper
        self.zookeeper_config_path = zookeeper_config_path

        self.server_bin_path = server_bin_path
        self.odbc_bridge_bin_path = odbc_bridge_bin_path

        self.with_mysql = with_mysql
        self.with_kafka = with_kafka
        self.with_kerberized_kafka = with_kerberized_kafka
        self.with_rabbitmq = with_rabbitmq
        self.with_mongo = with_mongo
        self.with_redis = with_redis
        self.with_minio = with_minio
        self.with_cassandra = with_cassandra

        self.path = p.join(self.cluster.instances_dir, name)
        self.docker_compose_path = p.join(self.path, 'docker_compose.yml')
        self.env_variables = env_variables or {}
        if with_odbc_drivers:
            self.odbc_ini_path = self.path + "/odbc.ini:/etc/odbc.ini"
            self.with_mysql = True
        else:
            self.odbc_ini_path = ""

        if with_kerberized_kafka:
            self.keytab_path = '- ' + os.path.dirname(self.docker_compose_path) + "/secrets:/tmp/keytab"
            self.krb5_conf = '- ' + os.path.dirname(self.docker_compose_path) + "/secrets/krb.conf:/etc/krb5.conf:ro"
        else:
            self.keytab_path = ""
            self.krb5_conf = ""

        self.docker_client = None
        self.ip_address = None
        self.client = None
        self.default_timeout = 20.0  # 20 sec
        self.image = image
        self.tag = tag
        self.stay_alive = stay_alive
        self.ipv4_address = ipv4_address
        self.ipv6_address = ipv6_address
        self.with_installed_binary = with_installed_binary

    def is_built_with_thread_sanitizer(self):
        build_opts = self.query("SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS'")
        return "-fsanitize=thread" in build_opts

    def is_built_with_address_sanitizer(self):
        build_opts = self.query("SELECT value FROM system.build_options WHERE name = 'CXX_FLAGS'")
        return "-fsanitize=address" in build_opts

    # Connects to the instance via clickhouse-client, sends a query (1st argument) and returns the answer
    def query(self, sql, stdin=None, timeout=None, settings=None, user=None, password=None, database=None,
              ignore_error=False):
        return self.client.query(sql, stdin=stdin, timeout=timeout, settings=settings, user=user, password=password,
                                 database=database, ignore_error=ignore_error)

    def query_with_retry(self, sql, stdin=None, timeout=None, settings=None, user=None, password=None, database=None,
                         ignore_error=False,
                         retry_count=20, sleep_time=0.5, check_callback=lambda x: True):
        result = None
        for i in range(retry_count):
            try:
                result = self.query(sql, stdin=stdin, timeout=timeout, settings=settings, user=user, password=password,
                                    database=database, ignore_error=ignore_error)
                if check_callback(result):
                    return result
                time.sleep(sleep_time)
            except Exception as ex:
                print("Retry {} got exception {}".format(i + 1, ex))
                time.sleep(sleep_time)

        if result is not None:
            return result
        raise Exception("Can't execute query {}".format(sql))

    # As query() but doesn't wait response and returns response handler
    def get_query_request(self, *args, **kwargs):
        return self.client.get_query_request(*args, **kwargs)

    # Connects to the instance via clickhouse-client, sends a query (1st argument), expects an error and return its code
    def query_and_get_error(self, sql, stdin=None, timeout=None, settings=None, user=None, password=None,
                            database=None):
        return self.client.query_and_get_error(sql, stdin=stdin, timeout=timeout, settings=settings, user=user,
                                               password=password, database=database)

    # The same as query_and_get_error but ignores successful query.
    def query_and_get_answer_with_error(self, sql, stdin=None, timeout=None, settings=None, user=None, password=None,
                                        database=None):
        return self.client.query_and_get_answer_with_error(sql, stdin=stdin, timeout=timeout, settings=settings,
                                                           user=user, password=password, database=database)

    # Connects to the instance via HTTP interface, sends a query and returns the answer
    def http_query(self, sql, data=None, params=None, user=None, password=None, expect_fail_and_get_error=False):
        if params is None:
            params = {}
        else:
            params = params.copy()

        params["query"] = sql

        auth = None
        if user and password:
            auth = requests.auth.HTTPBasicAuth(user, password)
        elif user:
            auth = requests.auth.HTTPBasicAuth(user, '')
        url = "http://" + self.ip_address + ":8123/?" + urllib.parse.urlencode(params)

        if data:
            r = requests.post(url, data, auth=auth)
        else:
            r = requests.get(url, auth=auth)

        def http_code_and_message():
            code = r.status_code
            return str(code) + " " + http.client.responses[code] + ": " + r.text

        if expect_fail_and_get_error:
            if r.ok:
                raise Exception("ClickHouse HTTP server is expected to fail, but succeeded: " + r.text)
            return http_code_and_message()
        else:
            if not r.ok:
                raise Exception("ClickHouse HTTP server returned " + http_code_and_message())
            return r.text

    # Connects to the instance via HTTP interface, sends a query and returns the answer
    def http_request(self, url, method='GET', params=None, data=None, headers=None):
        url = "http://" + self.ip_address + ":8123/" + url
        return requests.request(method=method, url=url, params=params, data=data, headers=headers)

    # Connects to the instance via HTTP interface, sends a query, expects an error and return the error message
    def http_query_and_get_error(self, sql, data=None, params=None, user=None, password=None):
        return self.http_query(sql=sql, data=data, params=params, user=user, password=password,
                               expect_fail_and_get_error=True)

    def kill_clickhouse(self, stop_start_wait_sec=5):
        pid = self.get_process_pid("clickhouse")
        if not pid:
            raise Exception("No clickhouse found")
        self.exec_in_container(["bash", "-c", "kill -9 {}".format(pid)], user='root')
        time.sleep(stop_start_wait_sec)

    def restore_clickhouse(self, retries=100):
        pid = self.get_process_pid("clickhouse")
        if pid:
            raise Exception("ClickHouse has already started")
        self.exec_in_container(["bash", "-c", "{} --daemon".format(CLICKHOUSE_START_COMMAND)], user=str(os.getuid()))
        from helpers.test_tools import assert_eq_with_retry
        # wait start
        assert_eq_with_retry(self, "select 1", "1", retry_count=retries)

    def restart_clickhouse(self, stop_start_wait_sec=5, kill=False):
        if not self.stay_alive:
            raise Exception("clickhouse can be restarted only with stay_alive=True instance")

        self.exec_in_container(["bash", "-c", "pkill {} clickhouse".format("-9" if kill else "")], user='root')
        time.sleep(stop_start_wait_sec)
        self.exec_in_container(["bash", "-c", "{} --daemon".format(CLICKHOUSE_START_COMMAND)], user=str(os.getuid()))
        # wait start
        from helpers.test_tools import assert_eq_with_retry
        assert_eq_with_retry(self, "select 1", "1", retry_count=int(stop_start_wait_sec / 0.5), sleep_time=0.5)

    def exec_in_container(self, cmd, detach=False, nothrow=False, **kwargs):
        container_id = self.get_docker_handle().id
        return self.cluster.exec_in_container(container_id, cmd, detach, nothrow, **kwargs)

    def contains_in_log(self, substring):
        result = self.exec_in_container(
            ["bash", "-c", 'grep "{}" /var/log/clickhouse-server/clickhouse-server.log || true'.format(substring)])
        return len(result) > 0

    def file_exists(self, path):
        return self.exec_in_container(
            ["bash", "-c", "echo $(if [ -e '{}' ]; then echo 'yes'; else echo 'no'; fi)".format(path)]) == 'yes\n'

    def copy_file_to_container(self, local_path, dest_path):
        container_id = self.get_docker_handle().id
        return self.cluster.copy_file_to_container(container_id, local_path, dest_path)

    def get_process_pid(self, process_name):
        output = self.exec_in_container(["bash", "-c",
                                         "ps ax | grep '{}' | grep -v 'grep' | grep -v 'bash -c' | awk '{{print $1}}'".format(
                                             process_name)])
        if output:
            try:
                pid = int(output.split('\n')[0].strip())
                return pid
            except:
                return None
        return None

    def restart_with_latest_version(self, stop_start_wait_sec=10, callback_onstop=None, signal=15):
        if not self.stay_alive:
            raise Exception("Cannot restart not stay alive container")
        self.exec_in_container(["bash", "-c", "pkill -{} clickhouse".format(signal)], user='root')
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
            self.exec_in_container(["bash", "-c", "pkill -{} clickhouse".format(9)], nothrow=True, user='root')

        if callback_onstop:
            callback_onstop(self)
        self.exec_in_container(
            ["bash", "-c", "cp /usr/share/clickhouse_fresh /usr/bin/clickhouse && chmod 777 /usr/bin/clickhouse"],
            user='root')
        self.exec_in_container(["bash", "-c",
                                "cp /usr/share/clickhouse-odbc-bridge_fresh /usr/bin/clickhouse-odbc-bridge && chmod 777 /usr/bin/clickhouse"],
                               user='root')
        self.exec_in_container(["bash", "-c", "{} --daemon".format(CLICKHOUSE_START_COMMAND)], user=str(os.getuid()))
        from helpers.test_tools import assert_eq_with_retry
        # wait start
        assert_eq_with_retry(self, "select 1", "1", retry_count=retries)

    def get_docker_handle(self):
        return self.docker_client.containers.get(self.docker_id)

    def stop(self):
        self.get_docker_handle().stop()

    def start(self):
        self.get_docker_handle().start()

    def wait_for_start(self, deadline=None, timeout=None):
        start_time = time.time()

        if timeout is not None:
            deadline = start_time + timeout

        while True:
            handle = self.get_docker_handle()
            status = handle.status
            if status == 'exited':
                raise Exception(
                    "Instance `{}' failed to start. Container status: {}, logs: {}".format(self.name, status,
                                                                                           handle.logs()))

            current_time = time.time()
            time_left = deadline - current_time
            if deadline is not None and current_time >= deadline:
                raise Exception("Timed out while waiting for instance `{}' with ip address {} to start. "
                                "Container status: {}, logs: {}".format(self.name, self.ip_address, status,
                                                                        handle.logs()))

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
                if e.errno == errno.ECONNREFUSED or e.errno == errno.EHOSTUNREACH or e.errno == errno.ENETUNREACH:
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
            for driver_setup in list(self.odbc_drivers.values()):
                f.write("[{}]\n".format(driver_setup["DSN"]))
                for key, value in list(driver_setup.items()):
                    if key != "DSN":
                        f.write(key + "=" + value + "\n")

    def replace_config(self, path_to_config, replacement):
        self.exec_in_container(["bash", "-c", "echo '{}' > {}".format(replacement, path_to_config)])

    def create_dir(self, destroy_dir=True):
        """Create the instance directory and all the needed files there."""

        if destroy_dir:
            self.destroy_dir()
        elif p.exists(self.path):
            return

        os.makedirs(self.path)

        instance_config_dir = p.abspath(p.join(self.path, 'configs'))
        os.makedirs(instance_config_dir)

        print("Copy common default production configuration from {}".format(self.base_config_dir))
        shutil.copyfile(p.join(self.base_config_dir, 'config.xml'), p.join(instance_config_dir, 'config.xml'))
        shutil.copyfile(p.join(self.base_config_dir, 'users.xml'), p.join(instance_config_dir, 'users.xml'))

        print("Create directory for configuration generated in this helper")
        # used by all utils with any config
        conf_d_dir = p.abspath(p.join(instance_config_dir, 'conf.d'))
        os.mkdir(conf_d_dir)

        print("Create directory for common tests configuration")
        # used by server with main config.xml
        self.config_d_dir = p.abspath(p.join(instance_config_dir, 'config.d'))
        os.mkdir(self.config_d_dir)
        users_d_dir = p.abspath(p.join(instance_config_dir, 'users.d'))
        os.mkdir(users_d_dir)
        dictionaries_dir = p.abspath(p.join(instance_config_dir, 'dictionaries'))
        os.mkdir(dictionaries_dir)

        print("Copy common configuration from helpers")
        # The file is named with 0_ prefix to be processed before other configuration overloads.
        shutil.copy(p.join(HELPERS_DIR, '0_common_instance_config.xml'), self.config_d_dir)
        shutil.copy(p.join(HELPERS_DIR, '0_common_instance_users.xml'), users_d_dir)
        if len(self.custom_dictionaries_paths):
            shutil.copy(p.join(HELPERS_DIR, '0_common_enable_dictionaries.xml'), self.config_d_dir)

        print("Generate and write macros file")
        macros = self.macros.copy()
        macros['instance'] = self.name
        with open(p.join(conf_d_dir, 'macros.xml'), 'w') as macros_config:
            macros_config.write(self.dict_to_xml({"macros": macros}))

        # Put ZooKeeper config
        if self.with_zookeeper:
            shutil.copy(self.zookeeper_config_path, conf_d_dir)

        if self.with_kerberized_kafka:
            shutil.copytree(self.kerberos_secrets_dir, p.abspath(p.join(self.path, 'secrets')))

        # Copy config.d configs
        print("Copy custom test config files {} to {}".format(self.custom_main_config_paths, self.config_d_dir))
        for path in self.custom_main_config_paths:
            shutil.copy(path, self.config_d_dir)

        # Copy users.d configs
        for path in self.custom_user_config_paths:
            shutil.copy(path, users_d_dir)

        # Copy dictionaries configs to configs/dictionaries
        for path in self.custom_dictionaries_paths:
            shutil.copy(path, dictionaries_dir)

        db_dir = p.abspath(p.join(self.path, 'database'))
        print("Setup database dir {}".format(db_dir))
        if self.clickhouse_path_dir is not None:
            print("Database files taken from {}".format(self.clickhouse_path_dir))
            shutil.copytree(self.clickhouse_path_dir, db_dir)
            print("Database copied from {} to {}".format(self.clickhouse_path_dir, db_dir))
        else:
            os.mkdir(db_dir)

        logs_dir = p.abspath(p.join(self.path, 'logs'))
        print("Setup logs dir {}".format(logs_dir))
        os.mkdir(logs_dir)

        depends_on = []

        if self.with_mysql:
            depends_on.append("mysql1")

        if self.with_kafka:
            depends_on.append("kafka1")
            depends_on.append("schema-registry")

        if self.with_kerberized_kafka:
            depends_on.append("kerberized_kafka1")

        if self.with_rabbitmq:
            depends_on.append("rabbitmq1")

        if self.with_zookeeper:
            depends_on.append("zoo1")
            depends_on.append("zoo2")
            depends_on.append("zoo3")

        if self.with_minio:
            depends_on.append("minio1")

        env_file = _create_env_file(os.path.dirname(self.docker_compose_path), self.env_variables)

        print("Env {} stored in {}".format(self.env_variables, env_file))

        odbc_ini_path = ""
        if self.odbc_ini_path:
            self._create_odbc_config_file()
            odbc_ini_path = '- ' + self.odbc_ini_path

        entrypoint_cmd = CLICKHOUSE_START_COMMAND

        if self.stay_alive:
            entrypoint_cmd = CLICKHOUSE_STAY_ALIVE_COMMAND

        print("Entrypoint cmd: {}".format(entrypoint_cmd))

        networks = app_net = ipv4_address = ipv6_address = net_aliases = net_alias1 = ""
        if self.ipv4_address is not None or self.ipv6_address is not None or self.hostname != self.name:
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
            odbc_bridge_volume = "- " + self.odbc_bridge_bin_path + ":/usr/bin/clickhouse-odbc-bridge"
        else:
            binary_volume = "- " + self.server_bin_path + ":/usr/share/clickhouse_fresh"
            odbc_bridge_volume = "- " + self.odbc_bridge_bin_path + ":/usr/share/clickhouse-odbc-bridge_fresh"

        with open(self.docker_compose_path, 'w') as docker_compose:
            docker_compose.write(DOCKER_COMPOSE_TEMPLATE.format(
                image=self.image,
                tag=self.tag,
                name=self.name,
                hostname=self.hostname,
                binary_volume=binary_volume,
                odbc_bridge_volume=odbc_bridge_volume,
                instance_config_dir=instance_config_dir,
                config_d_dir=self.config_d_dir,
                db_dir=db_dir,
                tmpfs=str(self.tmpfs),
                logs_dir=logs_dir,
                depends_on=str(depends_on),
                user=os.getuid(),
                env_file=env_file,
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
            ))

    def destroy_dir(self):
        if p.exists(self.path):
            shutil.rmtree(self.path)


class ClickHouseKiller(object):
    def __init__(self, clickhouse_node):
        self.clickhouse_node = clickhouse_node

    def __enter__(self):
        self.clickhouse_node.kill_clickhouse()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clickhouse_node.restore_clickhouse()
