import os
import subprocess
import time
import logging
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

    def drop_instance_zk_connections(self, instance, action="DROP"):
        self._check_instance(instance)

        self._add_rule(
            {"source": instance.ip_address, "destination_port": 2181, "action": action}
        )
        self._add_rule(
            {"destination": instance.ip_address, "source_port": 2181, "action": action}
        )

    def restore_instance_zk_connections(self, instance, action="DROP"):
        self._check_instance(instance)

        self._delete_rule(
            {"source": instance.ip_address, "destination_port": 2181, "action": action}
        )
        self._delete_rule(
            {"destination": instance.ip_address, "source_port": 2181, "action": action}
        )

    def partition_instances(self, left, right, port=None, action="DROP"):
        self._check_instance(left)
        self._check_instance(right)

        def create_rule(src, dst):
            rule = {
                "source": src.ip_address,
                "destination": dst.ip_address,
                "action": action,
            }
            if port is not None:
                rule["destination_port"] = port
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
            instance.exec_in_container(
                ["bash", "-c", "tc qdisc del dev eth0 root netem"], user="root"
            )

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
            raise Exception("Instance + " + instance.name + " is not launched!")

    def _add_rule(self, rule):
        _NetworkManager.get().add_iptables_rule(**rule)
        self._iptables_rules.append(rule)

    def _delete_rule(self, rule):
        _NetworkManager.get().delete_iptables_rule(**rule)
        self._iptables_rules.remove(rule)

    def _add_tc_netem_delay(self, instance, delay_ms):
        instance.exec_in_container(
            [
                "bash",
                "-c",
                "tc qdisc add dev eth0 root netem delay {}ms".format(delay_ms),
            ],
            user="root",
        )
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
        cmd = ["iptables", "--wait", "-I", "DOCKER-USER", "1"]
        cmd.extend(self._iptables_cmd_suffix(**kwargs))
        self._exec_run(cmd, privileged=True)

    def delete_iptables_rule(self, **kwargs):
        cmd = ["iptables", "--wait", "-D", "DOCKER-USER"]
        cmd.extend(self._iptables_cmd_suffix(**kwargs))
        self._exec_run(cmd, privileged=True)

    @staticmethod
    def clean_all_user_iptables_rules():
        for i in range(1000):
            iptables_iter = i
            # when rules will be empty, it will return error
            res = subprocess.run("iptables --wait -D DOCKER-USER 1", shell=True)

            if res.returncode != 0:
                logging.info(
                    "All iptables rules cleared, "
                    + str(iptables_iter)
                    + " iterations, last error: "
                    + str(res.stderr)
                )
                return

    @staticmethod
    def _iptables_cmd_suffix(
        source=None,
        destination=None,
        source_port=None,
        destination_port=None,
        action=None,
        probability=None,
        custom_args=None,
    ):
        ret = []
        if probability is not None:
            ret.extend(
                [
                    "-m",
                    "statistic",
                    "--mode",
                    "random",
                    "--probability",
                    str(probability),
                ]
            )
        ret.extend(["-p", "tcp"])
        if source is not None:
            ret.extend(["-s", source])
        if destination is not None:
            ret.extend(["-d", destination])
        if source_port is not None:
            ret.extend(["--sport", str(source_port)])
        if destination_port is not None:
            ret.extend(["--dport", str(destination_port)])
        if action is not None:
            ret.extend(["-j"] + action.split())
        if custom_args is not None:
            ret.extend(custom_args)
        return ret

    def __init__(
        self,
        container_expire_timeout=50,
        container_exit_timeout=60,
        docker_api_version=os.environ.get("DOCKER_API_VERSION"),
    ):

        self.container_expire_timeout = container_expire_timeout
        self.container_exit_timeout = container_exit_timeout

        self._docker_client = docker.DockerClient(
            base_url="unix:///var/run/docker.sock",
            version=docker_api_version,
            timeout=600,
        )

        self._container = None

        self._ensure_container()

    def _ensure_container(self):
        if self._container is None or self._container_expire_time <= time.time():

            for i in range(5):
                if self._container is not None:
                    try:
                        logging.debug("[network] Removing %s", self._container.id)
                        self._container.remove(force=True)
                        break
                    except docker.errors.NotFound:
                        break
                    except Exception as ex:
                        print(
                            "Error removing network blocade container, will try again",
                            str(ex),
                        )
                        time.sleep(i)

            image = subprocess.check_output(
                "docker images -q clickhouse/integration-helper 2>/dev/null", shell=True
            )
            if not image.strip():
                print("No network image helper, will try download")
                # for some reason docker api may hang if image doesn't exist, so we download it
                # before running
                for i in range(5):
                    try:
                        subprocess.check_call(  # STYLE_CHECK_ALLOW_SUBPROCESS_CHECK_CALL
                            "docker pull clickhouse/integration-helper", shell=True
                        )
                        break
                    except:
                        time.sleep(i)
                else:
                    raise Exception("Cannot pull clickhouse/integration-helper image")

            self._container = self._docker_client.containers.run(
                "clickhouse/integration-helper",
                auto_remove=True,
                command=("sleep %s" % self.container_exit_timeout),
                # /run/xtables.lock passed inside for correct iptables --wait
                volumes={
                    "/run/xtables.lock": {"bind": "/run/xtables.lock", "mode": "ro"}
                },
                detach=True,
                network_mode="host",
            )
            logging.debug("[network] Created new container %s", self._container.id)
            self._container_expire_time = time.time() + self.container_expire_timeout

        return self._container

    def _exec_run_with_retry(self, cmd, retry_count, **kwargs):
        for i in range(retry_count):
            try:
                self._exec_run(cmd, **kwargs)
            except subprocess.CalledProcessError as e:
                logging.error(f"_exec_run failed for {cmd}, {e}")

    def _exec_run(self, cmd, **kwargs):
        container = self._ensure_container()

        handle = self._docker_client.api.exec_create(container.id, cmd, **kwargs)
        output = self._docker_client.api.exec_start(handle).decode("utf8")
        exit_code = self._docker_client.api.exec_inspect(handle)["ExitCode"]

        logging.debug(
            "[network] %s: %s (%s): %s", container.id, cmd, exit_code, output.strip()
        )

        if exit_code != 0:
            print(output)
            raise subprocess.CalledProcessError(exit_code, cmd)

        return output


# Approximately mesure network I/O speed for interface
class NetThroughput(object):
    def __init__(self, node):
        self.node = node
        # trying to get default interface and check it in /proc/net/dev
        self.interface = self.node.exec_in_container(
            [
                "bash",
                "-c",
                "awk '{print $1 \" \" $2}' /proc/net/route | grep 00000000 | awk '{print $1}'",
            ]
        ).strip()
        check = self.node.exec_in_container(
            ["bash", "-c", f'grep "^ *{self.interface}:" /proc/net/dev']
        ).strip()
        if not check:  # if check is not successful just try eth{1-10}
            for i in range(10):
                try:
                    self.interface = self.node.exec_in_container(
                        [
                            "bash",
                            "-c",
                            f"awk '{{print $1}}' /proc/net/route | grep 'eth{i}'",
                        ]
                    ).strip()
                    break
                except Exception as ex:
                    print(f"No interface eth{i}")
            else:
                raise Exception(
                    "No interface eth{1-10} and default interface not specified in /proc/net/route, maybe some special network configuration"
                )

        try:
            check = self.node.exec_in_container(
                ["bash", "-c", f'grep "^ *{self.interface}:" /proc/net/dev']
            ).strip()
            if not check:
                raise Exception(
                    f"No such interface {self.interface} found in /proc/net/dev"
                )
        except:
            logging.error(
                "All available interfaces %s",
                self.node.exec_in_container(["bash", "-c", "cat /proc/net/dev"]),
            )
            raise Exception(
                f"No such interface {self.interface} found in /proc/net/dev"
            )

        self.current_in = self._get_in_bytes()
        self.current_out = self._get_out_bytes()
        self.measure_time = time.time()

    def _get_in_bytes(self):
        try:
            result = self.node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f'awk "/^ *{self.interface}:/"\' {{ if ($1 ~ /.*:[0-9][0-9]*/) {{ sub(/^.*:/, "") ; print $1 }} else {{ print $2 }} }}\' /proc/net/dev',
                ]
            )
        except:
            raise Exception(
                f"Cannot receive in bytes from /proc/net/dev for interface {self.interface}"
            )

        try:
            return int(result)
        except:
            raise Exception(
                f"Got non-numeric in bytes '{result}' from /proc/net/dev for interface {self.interface}"
            )

    def _get_out_bytes(self):
        try:
            result = self.node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"awk \"/^ *{self.interface}:/\"' {{ if ($1 ~ /.*:[0-9][0-9]*/) {{ print $9 }} else {{ print $10 }} }}' /proc/net/dev",
                ]
            )
        except:
            raise Exception(
                f"Cannot receive out bytes from /proc/net/dev for interface {self.interface}"
            )

        try:
            return int(result)
        except:
            raise Exception(
                f"Got non-numeric out bytes '{result}' from /proc/net/dev for interface {self.interface}"
            )

    def measure_speed(self, measure="bytes"):
        new_in = self._get_in_bytes()
        new_out = self._get_out_bytes()
        current_time = time.time()
        in_speed = (new_in - self.current_in) / (current_time - self.measure_time)
        out_speed = (new_out - self.current_out) / (current_time - self.measure_time)

        self.current_out = new_out
        self.current_in = new_in
        self.measure_time = current_time

        if measure == "bytes":
            return in_speed, out_speed
        elif measure == "kilobytes":
            return in_speed / 1024.0, out_speed / 1024.0
        elif measure == "megabytes":
            return in_speed / (1024 * 1024), out_speed / (1024 * 1024)
        else:
            raise Exception(f"Unknown measure {measure}")
