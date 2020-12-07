import os
import subprocess
import time

import docker


class PartitionManager:
    """Allows introducing failures in the network between docker containers.

    Can act as a context manager:

    with PartitionManager() as pm:
        pm.partition_instances(instance1, instance2)
        ...
        # At exit all partitions are removed automatically.

    """

    def __init__(self):
        self._iptables_rules = []
        self._netem_delayed_instances = []
        _NetworkManager.get()

    def drop_instance_zk_connections(self, instance, action='DROP'):
        self._check_instance(instance)

        self._add_rule({'source': instance.ip_address, 'destination_port': 2181, 'action': action})
        self._add_rule({'destination': instance.ip_address, 'source_port': 2181, 'action': action})

    def restore_instance_zk_connections(self, instance, action='DROP'):
        self._check_instance(instance)

        self._delete_rule({'source': instance.ip_address, 'destination_port': 2181, 'action': action})
        self._delete_rule({'destination': instance.ip_address, 'source_port': 2181, 'action': action})

    def partition_instances(self, left, right, port=None, action='DROP'):
        self._check_instance(left)
        self._check_instance(right)

        def create_rule(src, dst):
            rule = {'source': src.ip_address, 'destination': dst.ip_address, 'action': action}
            if port is not None:
                rule['destination_port'] = port
            return rule

        self._add_rule(create_rule(left, right))
        self._add_rule(create_rule(right, left))

    def add_network_delay(self, instance, delay_ms):
        self._add_tc_netem_delay(instance, delay_ms)

    def heal_all(self):
        while self._iptables_rules:
            rule = self._iptables_rules.pop()
            _NetworkManager.get().delete_iptables_rule(**rule)

        while self._netem_delayed_instances:
            instance = self._netem_delayed_instances.pop()
            instance.exec_in_container(["bash", "-c", "tc qdisc del dev eth0 root netem"], user="root")

    def pop_rules(self):
        res = self._iptables_rules[:]
        self.heal_all()
        return res

    def push_rules(self, rules):
        for rule in rules:
            self._add_rule(rule)

    @staticmethod
    def _check_instance(instance):
        if instance.ip_address is None:
            raise Exception('Instance + ' + instance.name + ' is not launched!')

    def _add_rule(self, rule):
        _NetworkManager.get().add_iptables_rule(**rule)
        self._iptables_rules.append(rule)

    def _delete_rule(self, rule):
        _NetworkManager.get().delete_iptables_rule(**rule)
        self._iptables_rules.remove(rule)

    def _add_tc_netem_delay(self, instance, delay_ms):
        instance.exec_in_container(["bash", "-c", "tc qdisc add dev eth0 root netem delay {}ms".format(delay_ms)], user="root")
        self._netem_delayed_instances.append(instance)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.heal_all()

    def __del__(self):
        self.heal_all()


class PartitionManagerDisabler:
    def __init__(self, manager):
        self.manager = manager
        self.rules = self.manager.pop_rules()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.manager.push_rules(self.rules)


class _NetworkManager:
    """Execute commands inside a container with access to network settings.

    We need to call iptables to create partitions, but we want to avoid sudo.
    The way to circumvent this restriction is to run iptables in a container with network=host.
    The container is long-running and periodically renewed - this is an optimization to avoid the overhead
    of container creation on each call.
    Source of the idea: https://github.com/worstcase/blockade/blob/master/blockade/host.py
    """

    # Singleton instance.
    _instance = None

    @classmethod
    def get(cls, **kwargs):
        if cls._instance is None:
            cls._instance = cls(**kwargs)
        return cls._instance

    def add_iptables_rule(self, **kwargs):
        cmd = ['iptables', '-I', 'DOCKER-USER', '1']
        cmd.extend(self._iptables_cmd_suffix(**kwargs))
        self._exec_run(cmd, privileged=True)

    def delete_iptables_rule(self, **kwargs):
        cmd = ['iptables', '-D', 'DOCKER-USER']
        cmd.extend(self._iptables_cmd_suffix(**kwargs))
        self._exec_run(cmd, privileged=True)

    @staticmethod
    def _iptables_cmd_suffix(
            source=None, destination=None,
            source_port=None, destination_port=None,
            action=None, probability=None):
        ret = []
        if probability is not None:
            ret.extend(['-m', 'statistic', '--mode', 'random', '--probability', str(probability)])
        ret.extend(['-p', 'tcp'])
        if source is not None:
            ret.extend(['-s', source])
        if destination is not None:
            ret.extend(['-d', destination])
        if source_port is not None:
            ret.extend(['--sport', str(source_port)])
        if destination_port is not None:
            ret.extend(['--dport', str(destination_port)])
        if action is not None:
            ret.extend(['-j'] + action.split())
        return ret

    def __init__(
            self,
            container_expire_timeout=50, container_exit_timeout=60):

        self.container_expire_timeout = container_expire_timeout
        self.container_exit_timeout = container_exit_timeout

        self._docker_client = docker.from_env(version=os.environ.get("DOCKER_API_VERSION"))

        self._container = None

        self._ensure_container()

    def _ensure_container(self):
        if self._container is None or self._container_expire_time <= time.time():

            for i in range(5):
                if self._container is not None:
                    try:
                        self._container.remove(force=True)
                        break
                    except docker.errors.NotFound:
                        break
                    except Exception as ex:
                        print("Error removing network blocade container, will try again", str(ex))
                        time.sleep(i)

            image = subprocess.check_output("docker images -q yandex/clickhouse-integration-helper 2>/dev/null", shell=True)
            if not image.strip():
                print("No network image helper, will try download")
                # for some reason docker api may hang if image doesn't exist, so we download it
                # before running
                for i in range(5):
                    try:
                        subprocess.check_call("docker pull yandex/clickhouse-integration-helper", shell=True)
                        break
                    except:
                        time.sleep(i)
                else:
                    raise Exception("Cannot pull yandex/clickhouse-integration-helper image")

            self._container = self._docker_client.containers.run('yandex/clickhouse-integration-helper',
                                                                 auto_remove=True,
                                                                 command=('sleep %s' % self.container_exit_timeout),
                                                                 detach=True, network_mode='host')
            container_id = self._container.id
            self._container_expire_time = time.time() + self.container_expire_timeout

        return self._container

    def _exec_run(self, cmd, **kwargs):
        container = self._ensure_container()

        handle = self._docker_client.api.exec_create(container.id, cmd, **kwargs)
        output = self._docker_client.api.exec_start(handle).decode('utf8')
        exit_code = self._docker_client.api.exec_inspect(handle)['ExitCode']

        if exit_code != 0:
            print(output)
            raise subprocess.CalledProcessError(exit_code, cmd)

        return output
