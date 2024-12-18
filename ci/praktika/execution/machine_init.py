import os
import platform
import signal
import time
import traceback

import requests
from praktika.execution.execution_settings import ExecutionSettings, ScalingType
from praktika.utils import ContextManager, Shell


class StateMachine:
    class StateNames:
        INIT = "init"
        WAIT = "wait"
        RUN = "run"

    def __init__(self):
        self.state = self.StateNames.INIT
        self.scale_type = ExecutionSettings.RUNNER_SCALING_TYPE
        self.machine = Machine(scaling_type=self.scale_type).update_instance_info()
        self.state_updated_at = int(time.time())
        self.forked = False

    def kick(self):
        if self.state == self.StateNames.INIT:
            self.machine.config_actions().run_actions_async()
            print("State Machine: INIT -> WAIT")
            self.state = self.StateNames.WAIT
            self.state_updated_at = int(time.time())
            # TODO: add monitoring
            if not self.machine.is_actions_process_healthy():
                print(f"ERROR: GH runner process unexpectedly died")
                self.machine.self_terminate(decrease_capacity=False)
        elif self.state == self.StateNames.WAIT:
            res = self.machine.check_job_assigned()
            if res:
                print("State Machine: WAIT -> RUN")
                self.state = self.StateNames.RUN
                self.state_updated_at = int(time.time())
                self.check_scale_up()
            else:
                self.check_scale_down()
        elif self.state == self.StateNames.RUN:
            res = self.machine.check_job_running()
            if res:
                pass
            else:
                print("State Machine: RUN -> INIT")
                self.state = self.StateNames.INIT
                self.state_updated_at = int(time.time())

    def check_scale_down(self):
        if self.scale_type not in (
            ScalingType.AUTOMATIC_SCALE_DOWN,
            ScalingType.AUTOMATIC_SCALE_UP_DOWN,
        ):
            return
        if ScalingType.AUTOMATIC_SCALE_UP_DOWN and not self.forked:
            print(
                f"Scaling type is AUTOMATIC_SCALE_UP_DOWN and machine has not run a job - do not scale down"
            )
            return
        if (
            int(time.time()) - self.state_updated_at
            > ExecutionSettings.MAX_WAIT_TIME_BEFORE_SCALE_DOWN_SEC
        ):
            print(
                f"No job assigned for more than MAX_WAIT_TIME_BEFORE_SCALE_DOWN_SEC [{ExecutionSettings.MAX_WAIT_TIME_BEFORE_SCALE_DOWN_SEC}] - scale down the instance"
            )
            if not ExecutionSettings.LOCAL_EXECUTION:
                self.machine.self_terminate(decrease_capacity=True)
            else:
                print("Local execution - skip scaling operation")

    def check_scale_up(self):
        if self.scale_type not in (ScalingType.AUTOMATIC_SCALE_UP_DOWN,):
            return
        if self.forked:
            print("This instance already forked once - do not scale up")
            return
        self.machine.self_fork()
        self.forked = True

    def run(self):
        self.machine.unconfig_actions()
        while True:
            self.kick()
            time.sleep(5)

    def terminate(self):
        try:
            self.machine.unconfig_actions()
        except:
            print("WARNING: failed to unconfig runner")
        if not ExecutionSettings.LOCAL_EXECUTION:
            if self.machine is not None:
                self.machine.self_terminate(decrease_capacity=False)
                time.sleep(10)
                # wait termination
            print("ERROR: failed to terminate instance via aws cli - try os call")
            os.system("sudo shutdown now")
        else:
            print("NOTE: Local execution - machine won't be terminated")


class Machine:
    @staticmethod
    def get_latest_gh_actions_release():
        url = f"https://api.github.com/repos/actions/runner/releases/latest"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            latest_release = response.json()
            return latest_release["tag_name"].removeprefix("v")
        else:
            print(f"Failed to get the latest release: {response.status_code}")
            return None

    def __init__(self, scaling_type):
        self.os_name = platform.system().lower()
        assert self.os_name == "linux", f"Unsupported OS [{self.os_name}]"
        if platform.machine() == "x86_64":
            self.arch = "x64"
        elif "aarch64" in platform.machine().lower():
            self.arch = "arm64"
        else:
            assert False, f"Unsupported arch [{platform.machine()}]"
        self.instance_id = None
        self.asg_name = None
        self.runner_api_endpoint = None
        self.runner_type = None
        self.labels = []
        self.proc = None
        assert scaling_type in ScalingType
        self.scaling_type = scaling_type

    def install_gh_actions_runner(self):
        gh_actions_version = self.get_latest_gh_actions_release()
        assert self.os_name and gh_actions_version and self.arch
        Shell.check(
            f"rm -rf {ExecutionSettings.GH_ACTIONS_DIRECTORY}",
            strict=True,
            verbose=True,
        )
        Shell.check(
            f"mkdir {ExecutionSettings.GH_ACTIONS_DIRECTORY}", strict=True, verbose=True
        )
        with ContextManager.cd(ExecutionSettings.GH_ACTIONS_DIRECTORY):
            Shell.check(
                f"curl -O -L https://github.com/actions/runner/releases/download/v{gh_actions_version}/actions-runner-{self.os_name}-{self.arch}-{gh_actions_version}.tar.gz",
                strict=True,
                verbose=True,
            )
            Shell.check(f"tar xzf *tar.gz", strict=True, verbose=True)
            Shell.check(f"rm -f *tar.gz", strict=True, verbose=True)
            Shell.check(f"sudo ./bin/installdependencies.sh", strict=True, verbose=True)
            Shell.check(
                f"chown -R ubuntu:ubuntu {ExecutionSettings.GH_ACTIONS_DIRECTORY}",
                strict=True,
                verbose=True,
            )

    def _get_gh_token_from_ssm(self):
        gh_token = Shell.get_output_or_raise(
            "/usr/local/bin/aws ssm  get-parameter --name github_runner_registration_token --with-decryption --output text --query Parameter.Value"
        )
        return gh_token

    def update_instance_info(self):
        self.instance_id = Shell.get_output_or_raise("ec2metadata --instance-id")
        assert self.instance_id
        self.asg_name = Shell.get_output(
            f"aws ec2 describe-instances --instance-id {self.instance_id} --query \"Reservations[].Instances[].Tags[?Key=='aws:autoscaling:groupName'].Value\" --output text"
        )
        # self.runner_type = Shell.get_output_or_raise(
        #     f'/usr/local/bin/aws ec2 describe-tags --filters "Name=resource-id,Values={self.instance_id}" --query "Tags[?Key==\'github:runner-type\'].Value" --output text'
        # )
        self.runner_type = self.asg_name
        if (
            self.scaling_type != ScalingType.DISABLED
            and not ExecutionSettings.LOCAL_EXECUTION
        ):
            assert (
                self.asg_name and self.runner_type
            ), f"Failed to retrieve ASG name, which is required for scaling_type [{self.scaling_type}]"
        org = os.getenv("MY_ORG", "")
        assert (
            org
        ), "MY_ORG env variable myst be set to use init script for runner machine"
        self.runner_api_endpoint = f"https://github.com/{org}"

        self.labels = ["self-hosted", self.runner_type]
        return self

    @classmethod
    def check_job_assigned(cls):
        runner_pid = Shell.get_output_or_raise("pgrep Runner.Listener")
        if not runner_pid:
            print("check_job_assigned: No runner pid")
            return False
        log_file = Shell.get_output_or_raise(
            f"lsof -p {runner_pid} | grep -o {ExecutionSettings.GH_ACTIONS_DIRECTORY}/_diag/Runner.*log"
        )
        if not log_file:
            print("check_job_assigned: No log file")
            return False
        return Shell.check(f"grep -q 'Terminal] .* Running job:' {log_file}")

    def check_job_running(self):
        if self.proc is None:
            print(f"WARNING: No job started")
            return False
        exit_code = self.proc.poll()
        if exit_code is None:
            return True
        else:
            print(f"Job runner finished with exit code [{exit_code}]")
            self.proc = None
            return False

    def config_actions(self):
        if not self.instance_id:
            self.update_instance_info()
        token = self._get_gh_token_from_ssm()
        assert token and self.instance_id and self.runner_api_endpoint and self.labels
        command = f"sudo -u ubuntu {ExecutionSettings.GH_ACTIONS_DIRECTORY}/config.sh --token {token} \
            --url {self.runner_api_endpoint} --ephemeral --unattended --replace \
            --runnergroup Default --labels {','.join(self.labels)} --work wd --name {self.instance_id}"
        res = 1
        i = 0
        while i < 10 and res != 0:
            res = Shell.run(command)
            i += 1
            if res != 0:
                print(
                    f"ERROR: failed to configure GH actions runner after [{i}] attempts, exit code [{res}], retry after 10s"
                )
                time.sleep(10)
                self._get_gh_token_from_ssm()
        if res == 0:
            print("GH action runner has been configured")
        else:
            assert False, "GH actions runner configuration failed"
        return self

    def unconfig_actions(self):
        token = self._get_gh_token_from_ssm()
        command = f"sudo -u ubuntu {ExecutionSettings.GH_ACTIONS_DIRECTORY}/config.sh remove --token {token}"
        Shell.check(command, strict=True)
        return self

    def run_actions_async(self):
        command = f"sudo -u ubuntu {ExecutionSettings.GH_ACTIONS_DIRECTORY}/run.sh"
        self.proc = Shell.run_async(command)
        assert self.proc is not None
        return self

    def is_actions_process_healthy(self):
        try:
            if self.proc.poll() is None:
                return True

            stdout, stderr = self.proc.communicate()

            if self.proc.returncode != 0:
                # Handle failure
                print(
                    f"GH Action process failed with return code {self.proc.returncode}"
                )
                print(f"Error output: {stderr}")
                return False
            else:
                print(f"GH Action process is not running")
                return False
        except Exception as e:
            print(f"GH Action process exception: {e}")
            return False

    def self_terminate(self, decrease_capacity):
        print(
            f"WARNING: Self terminate is called, decrease_capacity [{decrease_capacity}]"
        )
        traceback.print_stack()
        if not self.instance_id:
            self.update_instance_info()
        assert self.instance_id
        command = f"aws autoscaling terminate-instance-in-auto-scaling-group --instance-id {self.instance_id}"
        if decrease_capacity:
            command += " --should-decrement-desired-capacity"
        else:
            command += " --no-should-decrement-desired-capacity"
        Shell.check(
            command=command,
            verbose=True,
        )

    def self_fork(self):
        current_capacity = Shell.get_output(
            f'aws autoscaling describe-auto-scaling-groups --auto-scaling-group-name {self.asg_name} \
                --query "AutoScalingGroups[0].DesiredCapacity" --output text'
        )
        current_capacity = int(current_capacity)
        if not current_capacity:
            print("ERROR: failed to get current capacity - cannot scale up")
            return
        desired_capacity = current_capacity + 1
        command = f"aws autoscaling set-desired-capacity --auto-scaling-group-name {self.asg_name} --desired-capacity {desired_capacity}"
        print(f"Increase capacity [{current_capacity} -> {desired_capacity}]")
        res = Shell.check(
            command=command,
            verbose=True,
        )
        if not res:
            print("ERROR: failed to increase capacity - cannot scale up")


def handle_signal(signum, _frame):
    print(f"FATAL: Received signal {signum}")
    raise RuntimeError(f"killed by signal {signum}")


def run():
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    m = None
    try:
        m = StateMachine()
        m.run()
    except Exception as e:
        print(f"FATAL: Exception [{e}] - terminate instance")
        time.sleep(10)
        if m:
            m.terminate()
        raise e


if __name__ == "__main__":
    run()
