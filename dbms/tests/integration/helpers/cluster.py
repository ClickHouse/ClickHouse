import os
import os.path as p
import pwd
import re
import subprocess
import shutil
import socket
import time
import errno

import docker

from .client import Client


HELPERS_DIR = p.dirname(__file__)


class ClickHouseCluster:
    """ClickHouse cluster with several instances and (possibly) ZooKeeper.

    Add instances with several calls to add_instance(), then start them with the start() call.

    Directories for instances are created in the directory of base_path. After cluster is started,
    these directories will contain logs, database files, docker-compose config, ClickHouse configs etc.
    """

    def __init__(self, base_path, base_configs_dir=None, server_bin_path=None, client_bin_path=None):
        self.base_dir = p.dirname(base_path)

        self.base_configs_dir = base_configs_dir or os.environ.get('CLICKHOUSE_TESTS_BASE_CONFIG_DIR', '/etc/clickhouse-server/')
        self.server_bin_path = server_bin_path or os.environ.get('CLICKHOUSE_TESTS_SERVER_BIN_PATH', '/usr/bin/clickhouse')
        self.client_bin_path = client_bin_path or os.environ.get('CLICKHOUSE_TESTS_CLIENT_BIN_PATH', '/usr/bin/clickhouse-client')

        self.project_name = pwd.getpwuid(os.getuid()).pw_name + p.basename(self.base_dir)
        # docker-compose removes everything non-alphanumeric from project names so we do it too.
        self.project_name = re.sub(r'[^a-z0-9]', '', self.project_name.lower())

        self.base_cmd = ['docker-compose', '--project-directory', self.base_dir, '--project-name', self.project_name]
        self.instances = {}
        self.with_zookeeper = False
        self.is_up = False


    def add_instance(self, name, custom_configs, with_zookeeper=False):
        """Add an instance to the cluster.

        name - the name of the instance directory and the value of the 'instance' macro in ClickHouse.
        custom_configs - a list of config files that will be added to config.d/ directory
        with_zookeeper - if True, add ZooKeeper configuration to configs and ZooKeeper instances to the cluster.
        """

        if self.is_up:
            raise Exception('Can\'t add instance %s: cluster is already up!' % name)

        if name in self.instances:
            raise Exception('Can\'t add instance %s: there is already an instance with the same name!' % name)

        instance = ClickHouseInstance(self.base_dir, name, custom_configs, with_zookeeper, self.base_configs_dir, self.server_bin_path)
        self.instances[name] = instance
        self.base_cmd.extend(['--file', instance.docker_compose_path])
        if with_zookeeper and not self.with_zookeeper:
            self.with_zookeeper = True
            self.base_cmd.extend(['--file', p.join(HELPERS_DIR, 'docker_compose_zookeeper.yml')])

        return instance


    def start(self, destroy_dirs=True):
        if self.is_up:
            return

        for instance in self.instances.values():
            instance.create_dir(destroy_dir=destroy_dirs)

        subprocess.check_call(self.base_cmd + ['up', '-d'])

        docker_client = docker.from_env()
        for instance in self.instances.values():
            # According to how docker-compose names containers.
            instance.docker_id = self.project_name + '_' + instance.name + '_1'

            container = docker_client.containers.get(instance.docker_id)
            instance.ip_address = container.attrs['NetworkSettings']['Networks'].values()[0]['IPAddress']

            instance.wait_for_start()

            instance.client = Client(instance.ip_address, command=self.client_bin_path)

        self.is_up = True


    def shutdown(self, kill=True):
        if kill:
            subprocess.check_call(self.base_cmd + ['kill'])
        subprocess.check_call(self.base_cmd + ['down', '--volumes'])
        self.is_up = False

        for instance in self.instances.values():
            instance.docker_id = None
            instance.ip_address = None
            instance.client = None


DOCKER_COMPOSE_TEMPLATE = '''
version: '2'
services:
    {name}:
        image: ubuntu:14.04
        user: '{uid}'
        volumes:
            - {binary_path}:/usr/bin/clickhouse:ro
            - {configs_dir}:/etc/clickhouse-server/
            - {db_dir}:/var/lib/clickhouse/
            - {logs_dir}:/var/log/clickhouse-server/
        entrypoint:
            -  /usr/bin/clickhouse
            -  --config-file=/etc/clickhouse-server/config.xml
            -  --log-file=/var/log/clickhouse-server/clickhouse-server.log
        depends_on: {depends_on}
'''

MACROS_CONFIG_TEMPLATE = '''
<yandex>
    <macros>
        <instance>{name}</instance>
    </macros>
</yandex>
'''

class ClickHouseInstance:
    def __init__(
            self, base_path, name, custom_configs, with_zookeeper,
            base_configs_dir, server_bin_path):

        self.name = name
        self.custom_config_paths = [p.abspath(p.join(base_path, c)) for c in custom_configs]
        self.with_zookeeper = with_zookeeper

        self.base_configs_dir = base_configs_dir
        self.server_bin_path = server_bin_path

        self.path = p.abspath(p.join(base_path, name))
        self.docker_compose_path = p.join(self.path, 'docker_compose.yml')

        self.docker_id = None
        self.ip_address = None
        self.client = None


    def query(self, sql, stdin=None):
        return self.client.query(sql, stdin)


    def wait_for_start(self, timeout=10.0):
        deadline = time.time() + timeout
        while True:
            if time.time() >= deadline:
                raise Exception("Timed out while waiting for instance {} with ip address {} to start".format(self.name, self.ip_address))

            # Repeatedly poll the instance address until there is something that listens there.
            # Usually it means that ClickHouse is ready to accept queries.
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.ip_address, 9000))
                return
            except socket.error as e:
                if e.errno == errno.ECONNREFUSED:
                    time.sleep(0.1)
                else:
                    raise
            finally:
                sock.close()


    def create_dir(self, destroy_dir=True):
        """Create the instance directory and all the needed files there."""

        if destroy_dir:
            self.destroy_dir()
        elif p.exists(self.path):
            return

        os.mkdir(self.path)

        configs_dir = p.join(self.path, 'configs')
        os.mkdir(configs_dir)

        shutil.copy(p.join(self.base_configs_dir, 'config.xml'), configs_dir)
        shutil.copy(p.join(self.base_configs_dir, 'users.xml'), configs_dir)

        config_d_dir = p.join(configs_dir, 'config.d')
        os.mkdir(config_d_dir)

        shutil.copy(p.join(HELPERS_DIR, 'common_instance_config.xml'), config_d_dir)

        with open(p.join(config_d_dir, 'macros.xml'), 'w') as macros_config:
            macros_config.write(MACROS_CONFIG_TEMPLATE.format(name=self.name))

        if self.with_zookeeper:
            shutil.copy(p.join(HELPERS_DIR, 'zookeeper_config.xml'), config_d_dir)

        for path in self.custom_config_paths:
            shutil.copy(path, config_d_dir)

        db_dir = p.join(self.path, 'database')
        os.mkdir(db_dir)

        logs_dir = p.join(self.path, 'logs')
        os.mkdir(logs_dir)

        depends_on = '[]'
        if self.with_zookeeper:
            depends_on = '["zoo1", "zoo2", "zoo3"]'

        with open(self.docker_compose_path, 'w') as docker_compose:
            docker_compose.write(DOCKER_COMPOSE_TEMPLATE.format(
                name=self.name,
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
