#!/usr/bin/env python3

import shutil
import os
import subprocess
import tempfile
import logging
import signal


class SSHAgent:
    def __init__(self):
        self._env = {}
        self._env_backup = {}
        self._keys = {}
        self.start()

    @property
    def pid(self):
        return int(self._env["SSH_AGENT_PID"])

    def start(self):
        if shutil.which("ssh-agent") is None:
            raise Exception("ssh-agent binary is not available")

        self._env_backup["SSH_AUTH_SOCK"] = os.environ.get("SSH_AUTH_SOCK")
        self._env_backup["SSH_OPTIONS"] = os.environ.get("SSH_OPTIONS")

        # set ENV from stdout of ssh-agent
        for line in self._run(["ssh-agent"]).splitlines():
            name, _, value = line.partition(b"=")
            if _ == b"=":
                value = value.split(b";", 1)[0]
                self._env[name.decode()] = value.decode()
                os.environ[name.decode()] = value.decode()

        ssh_options = (
            "," + os.environ["SSH_OPTIONS"] if os.environ.get("SSH_OPTIONS") else ""
        )
        os.environ[
            "SSH_OPTIONS"
        ] = f"{ssh_options}UserKnownHostsFile=/dev/null,StrictHostKeyChecking=no"

    def add(self, key):
        key_pub = self._key_pub(key)

        if key_pub in self._keys:
            self._keys[key_pub] += 1
        else:
            self._run(["ssh-add", "-"], stdin=key.encode())
            self._keys[key_pub] = 1

        return key_pub

    def remove(self, key_pub):
        if key_pub not in self._keys:
            raise Exception(f"Private key not found, public part: {key_pub}")

        if self._keys[key_pub] > 1:
            self._keys[key_pub] -= 1
        else:
            with tempfile.NamedTemporaryFile() as f:
                f.write(key_pub)
                f.flush()
                self._run(["ssh-add", "-d", f.name])
            self._keys.pop(key_pub)

    def print_keys(self):
        keys = self._run(["ssh-add", "-l"]).splitlines()
        if keys:
            logging.info("ssh-agent keys:")
            for key in keys:
                logging.info("%s", key)
        else:
            logging.info("ssh-agent (pid %d) is empty", self.pid)

    def kill(self):
        for k, v in self._env.items():
            os.environ.pop(k, None)

        for k, v in self._env_backup.items():
            if v is not None:
                os.environ[k] = v

        os.kill(self.pid, signal.SIGTERM)

    def _key_pub(self, key):
        with tempfile.NamedTemporaryFile() as f:
            f.write(key.encode())
            f.flush()
            return self._run(["ssh-keygen", "-y", "-f", f.name])

    @staticmethod
    def _run(cmd, stdin=None):
        shell = isinstance(cmd, str)
        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE if stdin else None,
            shell=shell,
        ) as p:
            stdout, stderr = p.communicate(stdin)

            if stdout.strip().decode() == "The agent has no identities.":
                return ""

            if p.returncode:
                message = stderr.strip() + b"\n" + stdout.strip()
                raise Exception(message.strip().decode())

            return stdout


class SSHKey:
    def __init__(self, key_name=None, key_value=None):
        if key_name is None and key_value is None:
            raise Exception("Either key_name or key_value must be specified")
        if key_name is not None and key_value is not None:
            raise Exception("key_name or key_value must be specified")
        if key_name is not None:
            self.key = os.getenv(key_name)
        else:
            self.key = key_value
        self._key_pub = None
        self._ssh_agent = SSHAgent()

    def __enter__(self):
        self._key_pub = self._ssh_agent.add(self.key)
        self._ssh_agent.print_keys()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._ssh_agent.remove(self._key_pub)
        self._ssh_agent.print_keys()
