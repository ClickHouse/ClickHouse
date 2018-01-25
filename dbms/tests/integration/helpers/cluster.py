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
import xml.dom.minidom
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException

import docker
from docker.errors import ContainerError

from .client import Client, CommandRequest


HELPERS_DIR = p.dirname(__file__)


class ClickHouseCluster:
    """ClickHouse cluster with several instances and (possibly) ZooKeeper.

    Add instances with several calls to add_instance(), then start them with the start() call.

    Directories for instances are created in the directory of base_path. After cluster is started,
    these directories will contain logs, database files, docker-compose config, ClickHouse configs etc.
    """

    def __init__(self, base_path, name=None, base_configs_dir=None, server_bin_path=None, client_bin_path=None,
                 zookeeper_config_path=None):
        self.base_dir = p.dirname(base_path)
        self.name = name if name is not None else ''

        self.base_configs_dir = base_configs_dir or os.environ.get('CLICKHOUSE_TESTS_BASE_CONFIG_DIR', '/etc/clickhouse-server/')
        self.server_bin_path = server_bin_path or os.environ.get('CLICKHOUSE_TESTS_SERVER_BIN_PATH', '/usr/bin/clickhouse')
        self.client_bin_path = client_bin_path or os.environ.get('CLICKHOUSE_TESTS_CLIENT_BIN_PATH', '/usr/bin/clickhouse-client')
        self.zookeeper_config_path = p.join(self.base_dir, zookeeper_config_path) if zookeeper_config_path else p.join(HELPERS_DIR, 'zookeeper_config.xml')

        self.project_name = pwd.getpwuid(os.getuid()).pw_name + p.basename(self.base_dir) + self.name
        # docker-compose removes everything non-alphanumeric from project names so we do it too.
        self.project_name = re.sub(r'[^a-z0-9]', '', self.project_name.lower())
        self.instances_dir = p.join(self.base_dir, '_instances' + ('' if not self.name else '_' + self.name))

        self.base_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name', self.project_name]
        self.base_zookeeper_cmd = None
        self.pre_zookeeper_commands = []
        self.instances = {}
        self.with_zookeeper = False

        self.docker_client = None
        self.is_up = False


    def add_instance(self, name, config_dir=None, main_configs=[], user_configs=[], macroses={}, with_zookeeper=False,
        clickhouse_path_dir=None, hostname=None):
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
            self, self.base_dir, name, config_dir, main_configs, user_configs, macroses, with_zookeeper,
            self.zookeeper_config_path, self.base_configs_dir, self.server_bin_path, clickhouse_path_dir, hostname=hostname)

        self.instances[name] = instance
        self.base_cmd.extend(['--file', instance.docker_compose_path])
        if with_zookeeper and not self.with_zookeeper:
            self.with_zookeeper = True
            self.base_cmd.extend(['--file', p.join(HELPERS_DIR, 'docker_compose_zookeeper.yml')])
            self.base_zookeeper_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name',
                                       self.project_name, '--file', p.join(HELPERS_DIR, 'docker_compose_zookeeper.yml')]

        return instance


    def get_instance_docker_id(self, instance_name):
        # According to how docker-compose names containers.
        return self.project_name + '_' + instance_name + '_1'


    def get_instance_ip(self, instance_name):
        docker_id = self.get_instance_docker_id(instance_name)
        handle = self.docker_client.containers.get(docker_id)
        return handle.attrs['NetworkSettings']['Networks'].values()[0]['IPAddress']


    def start(self, destroy_dirs=True):
        if self.is_up:
            return

        # Just in case kill unstopped containers from previous launch
        try:
            if not subprocess.call(['docker-compose', 'kill']):
                subprocess.call(['docker-compose', 'down', '--volumes'])
        except:
            pass

        if destroy_dirs and p.exists(self.instances_dir):
            print "Removing instances dir", self.instances_dir
            shutil.rmtree(self.instances_dir)

        for instance in self.instances.values():
            instance.create_dir(destroy_dir=destroy_dirs)

        self.docker_client = docker.from_env()

        if self.with_zookeeper and self.base_zookeeper_cmd:
            subprocess.check_call(self.base_zookeeper_cmd + ['up', '-d', '--no-recreate'])
            for command in self.pre_zookeeper_commands:
                self.run_kazoo_commands_with_retries(command, repeats=5)

        # Uncomment for debugging
        # print ' '.join(self.base_cmd + ['up', '--no-recreate'])

        subprocess.check_call(self.base_cmd + ['up', '-d', '--no-recreate'])

        start_deadline = time.time() + 20.0 # seconds
        for instance in self.instances.itervalues():
            instance.docker_client = self.docker_client
            instance.ip_address = self.get_instance_ip(instance.name)

            instance.wait_for_start(start_deadline)

            instance.client = Client(instance.ip_address, command=self.client_bin_path)

        self.is_up = True


    def shutdown(self, kill=True):
        if kill:
            subprocess.check_call(self.base_cmd + ['kill'])
        subprocess.check_call(self.base_cmd + ['down', '--volumes', '--remove-orphans'])
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


DOCKER_COMPOSE_TEMPLATE = '''
version: '2'
services:
    {name}:
        image: ubuntu:14.04
        hostname: {hostname}
        user: '{uid}'
        volumes:
            - {binary_path}:/usr/bin/clickhouse:ro
            - {configs_dir}:/etc/clickhouse-server/
            - {db_dir}:/var/lib/clickhouse/
            - {logs_dir}:/var/log/clickhouse-server/
        entrypoint:
            -  /usr/bin/clickhouse
            -  server
            -  --config-file=/etc/clickhouse-server/config.xml
            -  --log-file=/var/log/clickhouse-server/clickhouse-server.log
        depends_on: {depends_on}
'''


class ClickHouseInstance:
    def __init__(
            self, cluster, base_path, name, custom_config_dir, custom_main_configs, custom_user_configs, macroses,
            with_zookeeper, zookeeper_config_path, base_configs_dir, server_bin_path, clickhouse_path_dir, hostname=None):

        self.name = name
        self.base_cmd = cluster.base_cmd[:]
        self.docker_id = cluster.get_instance_docker_id(self.name)
        self.cluster = cluster
        self.hostname = hostname if hostname is not None else self.name

        self.custom_config_dir = p.abspath(p.join(base_path, custom_config_dir)) if custom_config_dir else None
        self.custom_main_config_paths = [p.abspath(p.join(base_path, c)) for c in custom_main_configs]
        self.custom_user_config_paths = [p.abspath(p.join(base_path, c)) for c in custom_user_configs]
        self.clickhouse_path_dir = p.abspath(p.join(base_path, clickhouse_path_dir)) if clickhouse_path_dir else None
        self.macroses = macroses if macroses is not None else {}
        self.with_zookeeper = with_zookeeper
        self.zookeeper_config_path = zookeeper_config_path

        self.base_configs_dir = base_configs_dir
        self.server_bin_path = server_bin_path

        self.path = p.join(self.cluster.instances_dir, name)
        self.docker_compose_path = p.join(self.path, 'docker_compose.yml')

        self.docker_client = None
        self.ip_address = None
        self.client = None
        self.default_timeout = 20.0 # 20 sec

    # Connects to the instance via clickhouse-client, sends a query (1st argument) and returns the answer
    def query(self, *args, **kwargs):
        return self.client.query(*args, **kwargs)

    # As query() but doesn't wait response and returns response handler
    def get_query_request(self, *args, **kwargs):
        return self.client.get_query_request(*args, **kwargs)


    def exec_in_container(self, cmd, **kwargs):
        container = self.get_docker_handle()
        exec_id = self.docker_client.api.exec_create(container.id, cmd, **kwargs)
        output = self.docker_client.api.exec_start(exec_id, detach=False)

        output = output.decode('utf8')
        exit_code = self.docker_client.api.exec_inspect(exec_id)['ExitCode']
        if exit_code:
            raise Exception('Cmd "{}" failed! Return code {}. Output: {}'.format(' '.join(cmd), exit_code, output))
        return output


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
            status = self.get_docker_handle().status

            if status == 'exited':
                raise Exception("Instance `{}' failed to start. Container status: {}".format(self.name, status))

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

        config_d_dir = p.abspath(p.join(configs_dir, 'config.d'))
        users_d_dir = p.abspath(p.join(configs_dir, 'users.d'))
        os.mkdir(config_d_dir)
        os.mkdir(users_d_dir)

        shutil.copy(p.join(HELPERS_DIR, 'common_instance_config.xml'), config_d_dir)

        # Generate and write macroses file
        macroses = self.macroses.copy()
        macroses['instance'] = self.name
        with open(p.join(config_d_dir, 'macros.xml'), 'w') as macros_config:
            macros_config.write(self.dict_to_xml({"macros" : macroses}))

        # Put ZooKeeper config
        if self.with_zookeeper:
            shutil.copy(self.zookeeper_config_path, config_d_dir)

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

        depends_on = '[]'
        if self.with_zookeeper:
            depends_on = '["zoo1", "zoo2", "zoo3"]'

        with open(self.docker_compose_path, 'w') as docker_compose:
            docker_compose.write(DOCKER_COMPOSE_TEMPLATE.format(
                name=self.name,
                hostname=self.hostname,
                uid=os.getuid(),
                binary_path=self.server_bin_path,
                configs_dir=configs_dir,
                config_d_dir=config_d_dir,
                db_dir=db_dir,
                logs_dir=logs_dir,
                depends_on=depends_on))


    def destroy_dir(self):
        if p.exists(self.path):
            shutil.rmtree(self.path)
