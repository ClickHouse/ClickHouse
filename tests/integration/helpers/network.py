import ipaddress
import logging
import time


class PartitionManager:
    """
    Allows introducing network failures between Docker containers by
    executing iptables commands directly inside the target containers.

    Can act as a context manager:

    with PartitionManager() as pm:
        pm.partition_instances(instance1, instance2)
        ...
        # At exit all partitions are removed automatically.

    """

    def __init__(self):
        self._rules = []
        self._netem_delayed_instances = []
        self._prepared_instances = set()

    def _prepare_instance(self, instance):
        if instance.name in self._prepared_instances:
            return

        # Add a missing iptables dependency to some images
        if str(type(instance)) == 'ClickHouseInstance':
            return

        # def check_command_exists(command):
        #     # We use 'sh -c' to ensure the command is run within a shell,
        #     # allowing us to use shell built-ins like 'command -v'.
        #     instance.exec_in_container(['sh', '-c', f'command -v {command}'], user='root')
        #
        # # TODO: move it to docker images and remove
        # # Check if iptables exists
        # try:
        #     check_command_exists('iptables')
        #     logging.info(f"iptables is already available in {instance.name}.")
        #     self._prepared_instances.add(instance.name)
        #     return
        # except Exception as e:
        #     logging.info(e)
        #     logging.info(f"iptables not found in {instance.name}. Attempting to install...")
        #
        # # Check for apt-get and install if found
        # try:
        #     check_command_exists('apt-get')
        #     logging.info(f"Found apt-get in {instance.name}. Installing iptables...")
        #     instance.exec_in_container(['apt-get', 'update'], user='root')
        #     instance.exec_in_container(['apt-get', 'install', '--yes', 'iptables'], user='root')
        #     self._prepared_instances.add(instance.name)
        #     logging.info(f"iptables installed in {instance.name} using apt-get.")
        #     return
        # except Exception as e:
        #     logging.info(e)
        #     logging.info(f"apt-get not found or failed in {instance.name}. Trying yum...")
        #
        # # Check for yum and install if found
        # try:
        #     check_command_exists('yum')
        #     logging.info(f"Found yum in {instance.name}. Installing iptables...")
        #     instance.exec_in_container(['yum', 'install', '--assumeyes', 'iptables'], user='root')
        #     self._prepared_instances.add(instance.name)
        #     logging.info(f"iptables installed in {instance.name} using yum.")
        #     return
        # except Exception as e:
        #     logging.error(f"Yum command failed in {instance.name}: {e}")
        #
        # # If all attempts fail, raise an error
        # raise Exception(
        #     f"Could not find a known package manager (apt-get or yum) in {instance.name} to install iptables.")

    def _iptables_cmd(self, instance, args):
        self._prepare_instance(instance)
        instance.exec_in_container(['iptables'] + args, user='root')

    def _ip6tables_cmd(self, instance, args):
        self._prepare_instance(instance)
        instance.exec_in_container(['ip6tables'] + args, user='root')

    def _build_rule_args(self, action, rule_spec):
        chain = rule_spec.get("chain", "OUTPUT")
        args = ['--wait', action, chain]

        if rule_spec.get('probability'):
            args.extend(['-m', 'statistic', '--mode', 'random', '--probability', str(rule_spec['probability'])])
        if rule_spec.get('protocol'):
            args.extend(['-p', rule_spec['protocol']])
        if rule_spec.get('source'):
            args.extend(['-s', rule_spec['source']])
        if rule_spec.get('destination'):
            args.extend(['-d', rule_spec['destination']])
        if rule_spec.get('source_port'):
            args.extend(['--sport', str(rule_spec['source_port'])])
        if rule_spec.get('destination_port'):
            args.extend(['--dport', str(rule_spec['destination_port'])])

        # Action can be complex, e.g., "REJECT --reject-with tcp-reset"
        args.extend(['-j'] + rule_spec['action'].split())
        return args

    def add_rule(self, rule_spec):
        instance = rule_spec.get('instance')
        if not instance:
            raise ValueError("Rule specification must include an 'instance' key")

        args = self._build_rule_args('-A', rule_spec)

        if self._is_ipv6_rule(rule_spec):
            self._ip6tables_cmd(instance, args)
        else:
            self._iptables_cmd(instance, args)

        if rule_spec not in self._rules:
            self._rules.append(rule_spec)

    def delete_rule(self, rule_spec):
        instance = rule_spec.get('instance')
        if not instance:
            raise ValueError("Rule specification must include an 'instance' key")

        args = self._build_rule_args('-D', rule_spec)
        try:
            if self._is_ipv6_rule(rule_spec):
                self._ip6tables_cmd(instance, args)
            else:
                self._iptables_cmd(instance, args)

            if rule_spec in self._rules:
                self._rules.remove(rule_spec)
        except Exception as e:
            logging.warning(f"Could not delete network rule on {instance.name}: {e}")

    def drop_instance_zk_connections(self, instance, action="DROP"):
        self._check_instance(instance)
        self.add_rule({
            "instance": instance, "chain": "OUTPUT", "destination_port": 2181,
            "protocol": "tcp", "action": action,
        })
        if instance.ipv6_address:
            self.add_rule({
                "instance": instance, "chain": "OUTPUT", "destination_port": 2181,
                "protocol": "tcp", "action": action, "__ipv6": True,
            })

    def restore_instance_zk_connections(self, instance, action="DROP"):
        self._check_instance(instance)
        self.delete_rule({
            "instance": instance, "chain": "OUTPUT", "destination_port": 2181,
            "protocol": "tcp", "action": action,
        })
        if instance.ipv6_address:
            self.delete_rule({
                "instance": instance, "chain": "OUTPUT", "destination_port": 2181,
                "protocol": "tcp", "action": action, "__ipv6": True,
            })

    def partition_instances(self, left, right, port=None, action="DROP"):
        self._check_instance(left)
        self._check_instance(right)
        self.add_rule({
            "instance": left, "destination": right.ip_address, "destination_port": port,
            "action": action, "protocol": "tcp",
        })
        self.add_rule({
            "instance": right, "destination": left.ip_address, "destination_port": port,
            "action": action, "protocol": "tcp",
        })
        if left.ipv6_address and right.ipv6_address:
            self.add_rule({
                "instance": left, "destination": right.ipv6_address, "destination_port": port,
                "action": action, "protocol": "tcp", "__ipv6": True,
            })
            self.add_rule({
                "instance": right, "destination": left.ipv6_address, "destination_port": port,
                "action": action, "protocol": "tcp", "__ipv6": True,
            })

    def pop_rules(self):
        rules_to_pop = list(self._rules)
        for rule in rules_to_pop:
            self.delete_rule(rule)
        return rules_to_pop

    def push_rules(self, rules):
        for rule in rules:
            self.add_rule(rule)

    def heal_all(self):
        while self._rules:
            # Pop first to ensure we don't get stuck in an infinite loop
            # if delete_rule fails (e.g. container died)
            if self._rules:
                rule_spec = self._rules.pop(0)
                self.delete_rule(rule_spec)

        while self._netem_delayed_instances:
            instance = self._netem_delayed_instances.pop()
            try:
                instance.exec_in_container(
                    ["bash", "-c", "tc qdisc del dev eth0 root netem"], user="root"
                )
            except Exception as e:
                logging.warning(f"Could not heal tc rule on {instance.name}: {e}")

    def add_network_delay(self, instance, delay_ms):
        self._add_tc_netem_delay(instance, delay_ms)

    @staticmethod
    def _is_ipv6_rule(rule):
        if rule.get("__ipv6"):
            return True
        if rule.get("destination"):
            return ipaddress.ip_address(rule["destination"]).version == 6
        return False

    def _add_tc_netem_delay(self, instance, delay_ms):
        if instance.name not in self._prepared_instances:
            # Add missing packages (old mysql images)
            try:
                instance.exec_in_container(['which', 'tc'], user='root')
            except Exception:
                logging.info(f"iproute2 not found in {instance.name}, installing...")
                instance.exec_in_container(['apt-get', 'update'], user='root')
                instance.exec_in_container(['apt-get', 'install', '--yes', 'iproute2'], user='root')
                logging.info(f"iproute2 installed in {instance.name}.")

        instance.exec_in_container(
            ["bash", "-c", f"tc qdisc add dev eth0 root netem delay {delay_ms}ms"],
            user="root",
        )
        self._netem_delayed_instances.append(instance)

    @staticmethod
    def _check_instance(instance):
        if instance.ip_address is None:
            raise Exception(f"Instance {instance.name} is not launched!")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.heal_all()

    def __del__(self):
        self.heal_all()


# Approximately measure network I/O speed for interface
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
