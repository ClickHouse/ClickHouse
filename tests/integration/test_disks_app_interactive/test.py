import io
import os
import pathlib
import select
import shutil
import subprocess
from typing import Dict, List, Optional, Tuple, Union

import pytest

from helpers.cluster import ClickHouseCluster


class ClickHouseDisksException(Exception):
    pass


def _worker_suffix() -> str:
    # Under flaky check pytest-xdist runs the same module in many workers
    # concurrently (--dist=each). They all share the runner host filesystem,
    # so paths must be worker-scoped to avoid collisions.
    return os.environ.get("PYTEST_XDIST_WORKER", "main")


def _unique_name(stem: str) -> str:
    return f"{stem}_{_worker_suffix()}"


@pytest.fixture(scope="module")
def started_cluster():
    global cluster
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "disks_app_test",
            main_configs=["server_configs/config.xml"],
            with_minio=True,
        )

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


class DisksClient(object):
    SEPARATOR = b"\a\a\a\a\n"
    local_client: Optional["DisksClient"] = None  # static variable
    # Per-xdist-worker default-disk root so concurrent workers do not stomp
    # each other on the runner host filesystem.
    default_disk_root_directory: str = f"/var/lib/clickhouse-{_worker_suffix()}"

    def __init__(self, bin_path: str, config_path: str, working_path: str):
        self.bin_path = bin_path
        self.working_path = working_path

        self.proc = subprocess.Popen(
            [
                bin_path,
                "disks",
                "--test-mode",
                "--config",
                config_path,
                "--save-logs",
                "--log-level",
                "WARNING",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.poller = select.epoll()
        self.poller.register(self.proc.stdout)
        self.poller.register(self.proc.stderr)

        self.stopped = False

        self._fd_nums = {
            self.proc.stdout.fileno(): self.proc.stdout,
            self.proc.stderr.fileno(): self.proc.stderr,
        }

    def execute_query(self, query: str, timeout: float = 60.0) -> str:
        output = io.BytesIO()

        self.proc.stdin.write(query.encode() + b"\n")
        self.proc.stdin.flush()

        # The disks app marks the end of a command's output with SEPARATOR on
        # stdout. Keep polling until we actually observe it: a command may emit
        # log lines on stderr before (or interleaved with) its stdout result,
        # and reading only a single stderr line and returning would leave the
        # stdout separator unconsumed, desyncing every subsequent command. The
        # timeout is generous so that the slowest sanitizer builds (msan) do not
        # spuriously time out.
        while True:
            events = self.poller.poll(timeout)
            if not events:
                raise TimeoutError("Disks client returned no output")

            separator_seen = False
            for fd_num, event in events:
                if not (event & (select.EPOLLIN | select.EPOLLPRI)):
                    raise ValueError(f"Failed to read from pipe. Flag {event}")

                file = self._fd_nums[fd_num]

                if file == self.proc.stdout:
                    while True:
                        chunk = file.readline()
                        if not chunk:
                            raise ClickHouseDisksException(
                                "Disks client closed its output unexpectedly"
                            )
                        if chunk.endswith(self.SEPARATOR):
                            separator_seen = True
                            break

                        output.write(chunk)

                elif file == self.proc.stderr:
                    error_line = self.proc.stderr.readline()
                    print(error_line)
                    # raise ClickHouseDisksException(error_line.strip().decode())

            if separator_seen:
                break

        data = output.getvalue().strip().decode()
        return data

    def list_disks(self) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
        output = self.execute_query("list-disks")
        lines: List[str] = map(lambda x: x.strip(), output.split("\n"))

        initialized_disks = []
        unitialized_disks = []
        disk_ref = []

        for line in lines:
            if line.strip() == "Initialized disks:":
                disk_ref = initialized_disks
            elif line.strip() == "Uninitialized disks:":
                disk_ref = unitialized_disks
            else:
                if line == "":
                    continue
                disk_ref.append((line.split(":")[0], ":".join(line.split(":")[1:])))

        return list(sorted(initialized_disks)), list(sorted(unitialized_disks))

    def current_disk_with_path(self) -> Tuple[str, str]:
        output = self.execute_query("current_disk_with_path")
        disk_line = output.split("\n")[0]
        path_line = output.split("\n")[1]
        assert disk_line.startswith("Disk: ")
        assert path_line.startswith("Path: ")
        return disk_line[6:], path_line[6:]

    def ls(
        self, path: str, recursive: bool = False, show_hidden: bool = False
    ) -> Union[List[str], Dict[str, List[str]]]:
        recursive_adding = "--recursive " if recursive else ""
        show_hidden_adding = "--all " if show_hidden else ""
        output = self.execute_query(
            f"list {path} {recursive_adding} {show_hidden_adding}"
        )
        # Sort on the client side so assertions do not depend on the disks app
        # sort order.
        if recursive:
            answer: Dict[str, List[str]] = dict()
            blocks = output.split("\n\n")
            for block in blocks:
                directory = block.split("\n")[0][:-1]
                files = sorted(block.split("\n")[1:])
                answer[directory] = files
            return answer
        return sorted(output.split("\n")) if output else []

    def switch_disk(self, disk: str, directory: Optional[str] = None):
        directory_addition = f"--path {directory} " if directory is not None else ""
        self.execute_query(f"switch-disk {disk} {directory_addition}")

    def cd(self, directory: str, disk: Optional[str] = None):
        disk_addition = f"--disk {disk} " if disk is not None else ""
        self.execute_query(f"cd {directory} {disk_addition}")

    def copy(
        self,
        path_from,
        path_to,
        disk_from: Optional[str] = None,
        disk_to: Optional[str] = None,
        recursive: bool = False,
    ):
        disk_from_option = f"--disk-from {disk_from} " if disk_from is not None else ""
        disk_to_option = f"--disk-to {disk_to} " if disk_to is not None else ""
        recursive_tag = "--recursive" if recursive else ""

        self.execute_query(
            f"copy {recursive_tag} {path_from} {path_to} {disk_from_option} {disk_to_option}"
        )

    def move(self, path_from: str, path_to: str):
        self.execute_query(f"move {path_from} {path_to}")

    def rm(self, path: str, recursive: bool = False):
        recursive_tag = "--recursive" if recursive else ""
        self.execute_query(f"rm {recursive_tag} {path}")

    def mkdir(self, path: str, recursive: bool = False):
        recursive_adding = "--recursive " if recursive else ""
        self.execute_query(f"mkdir {path} {recursive_adding}")

    def ln(self, path_from: str, path_to: str):
        self.execute_query(f"link {path_from} {path_to}")

    def read(self, path_from: str, path_to: Optional[str] = None):
        path_to_adding = f"--path-to {path_to} " if path_to is not None else ""
        output = self.execute_query(f"read {path_from} {path_to_adding}")
        return output

    def write(
        self, path_from: str, path_to: str
    ):  # Writing from stdin is difficult to test (do not know how to do this in python)
        path_from_adding = f"--path-from {path_from}"
        self.execute_query(f"write {path_from_adding} {path_to}")

    @staticmethod
    def getLocalDisksClient(refresh: bool):
        if (DisksClient.local_client is None) or refresh:
            # Tear down the previous subprocess if we are refreshing so a stale
            # `clickhouse disks` process does not race with the new one on the
            # shared filesystem.
            if DisksClient.local_client is not None:
                try:
                    DisksClient.local_client.proc.stdin.close()
                except Exception:
                    pass
                try:
                    DisksClient.local_client.proc.terminate()
                    DisksClient.local_client.proc.wait(timeout=5)
                except Exception:
                    try:
                        DisksClient.local_client.proc.kill()
                    except Exception:
                        pass
                DisksClient.local_client = None

            binary_file = os.environ.get("CLICKHOUSE_TESTS_SERVER_BIN_PATH")
            current_working_directory = str(pathlib.Path().resolve())

            # Wipe any leftover state under the per-worker default disk root
            # from a previous iteration of this worker.
            os.makedirs(DisksClient.default_disk_root_directory, exist_ok=True)
            for entry in os.listdir(DisksClient.default_disk_root_directory):
                entry_path = os.path.join(
                    DisksClient.default_disk_root_directory, entry
                )
                if os.path.isdir(entry_path) and not os.path.islink(entry_path):
                    shutil.rmtree(entry_path, ignore_errors=True)
                else:
                    try:
                        os.remove(entry_path)
                    except OSError:
                        pass

            # Generate a per-worker config so concurrent flaky-check workers
            # use different `<path>` roots and do not stomp each other.
            suffix = _worker_suffix()
            config_dir = os.path.join(
                current_working_directory,
                "test_disks_app_interactive",
                "configs",
            )
            os.makedirs(config_dir, exist_ok=True)
            config_file = os.path.join(config_dir, f"config_{suffix}.xml")
            with open(config_file, "w") as cfg:
                cfg.write(
                    "<clickhouse>\n"
                    f"    <path>{DisksClient.default_disk_root_directory}/</path>\n"
                    "    <logger>\n"
                    "        <clickhouse-disks>/clickhouse-disks-interactive.log</clickhouse-disks>\n"
                    "    </logger>\n"
                    "</clickhouse>\n"
                )

            DisksClient.local_client = DisksClient(
                binary_file, config_file, current_working_directory
            )
            return DisksClient.local_client
        else:
            return DisksClient.local_client


def test_disks_app_interactive_list_disks():
    client = DisksClient.getLocalDisksClient(True)
    expected_disks_with_path = [
        ("default", "/"),
    ]
    assert expected_disks_with_path == client.list_disks()[0]
    assert client.current_disk_with_path() == ("default", "/")
    client.switch_disk("local")
    assert client.current_disk_with_path() == (
        "local",
        client.working_path,
    )
    expected_disks_with_path = [
        ("default", "/"),
        ("local", client.working_path),
    ]
    assert expected_disks_with_path == client.list_disks()[0]


def test_disks_app_interactive_list_files_local():
    client = DisksClient.getLocalDisksClient(True)
    client.switch_disk("local")
    excepted_listed_files = sorted(os.listdir("test_disks_app_interactive/"))
    listed_files = sorted(client.ls("test_disks_app_interactive/"))
    assert excepted_listed_files == listed_files


def test_disks_app_interactive_list_directories_default():
    client = DisksClient.getLocalDisksClient(True)
    test_dir = _unique_name("test_dir_default")
    client.mkdir(test_dir)
    client.cd(test_dir)
    client.mkdir("dir1")
    client.mkdir("dir2")
    client.mkdir(".dir3")
    client.cd("dir1")
    client.mkdir("dir11")
    client.mkdir(".dir12")
    client.mkdir("dir13")
    client.cd("../dir2")
    client.mkdir("dir21")
    client.mkdir("dir22")
    client.mkdir(".dir23")
    client.cd("../.dir3")
    client.mkdir("dir31")
    client.mkdir(".dir32")
    client.cd("..")
    traversed_dir = client.ls(".", recursive=True)
    assert traversed_dir == {
        ".": ["dir1", "dir2"],
        "./dir1": ["dir11", "dir13"],
        "./dir2": ["dir21", "dir22"],
        "./dir1/dir11": [],
        "./dir1/dir13": [],
        "./dir2/dir21": [],
        "./dir2/dir22": [],
    }
    traversed_dir = client.ls(".", recursive=True, show_hidden=True)
    assert traversed_dir == {
        ".": [".dir3", "dir1", "dir2"],
        "./dir1": [".dir12", "dir11", "dir13"],
        "./dir2": [".dir23", "dir21", "dir22"],
        "./.dir3": [".dir32", "dir31"],
        "./dir1/dir11": [],
        "./dir1/.dir12": [],
        "./dir1/dir13": [],
        "./dir2/dir21": [],
        "./dir2/dir22": [],
        "./dir2/.dir23": [],
        "./.dir3/dir31": [],
        "./.dir3/.dir32": [],
    }
    client.rm("dir2", recursive=True)
    traversed_dir = client.ls(".", recursive=True, show_hidden=True)
    assert traversed_dir == {
        ".": [".dir3", "dir1"],
        "./dir1": [".dir12", "dir11", "dir13"],
        "./.dir3": [".dir32", "dir31"],
        "./dir1/dir11": [],
        "./dir1/.dir12": [],
        "./dir1/dir13": [],
        "./.dir3/dir31": [],
        "./.dir3/.dir32": [],
    }
    traversed_dir = client.ls(".", recursive=True, show_hidden=False)
    assert traversed_dir == {
        ".": ["dir1"],
        "./dir1": ["dir11", "dir13"],
        "./dir1/dir11": [],
        "./dir1/dir13": [],
    }
    client.rm("dir1", recursive=True)
    client.rm(".dir3", recursive=True)
    assert client.ls(".", recursive=True, show_hidden=False) == {'.': []}
    client.cd('..')
    client.rm(test_dir)


def test_disks_app_interactive_cp_and_read():
    initial_text = "File content"
    src_name = _unique_name("a_cpread") + ".txt"
    dst_name = _unique_name("a_cpread") + ".txt"
    dir_name = _unique_name("dir1_cpread")
    b_name = _unique_name("b_cpread") + ".txt"
    with open(src_name, "w") as file:
        file.write(initial_text)
    client = DisksClient.getLocalDisksClient(True)
    client.switch_disk("default")
    client.copy(src_name, f"/{dst_name}", disk_from="local", disk_to="default")
    read_text = client.read(dst_name)
    assert initial_text == read_text
    client.mkdir(dir_name)
    client.copy(
        src_name, f"/{dir_name}/{b_name}", disk_from="local", disk_to="default"
    )
    read_text = client.read(dst_name, path_to=f"{dir_name}/{b_name}")
    assert "" == read_text
    read_text = client.read(f"/{dir_name}/{b_name}")
    assert read_text == initial_text
    with open(
        f"{DisksClient.default_disk_root_directory}/{dir_name}/{b_name}", "r"
    ) as file:
        read_text = file.read()
        assert read_text == initial_text
    os.remove(src_name)
    client.rm(dst_name)
    client.rm(f"/{dir_name}", recursive=True)


def test_disks_app_interactive_test_move_and_write():
    initial_text = "File content"
    src_name = _unique_name("a_movewrite") + ".txt"
    test_dir = _unique_name("test_movewrite")
    with open(src_name, "w") as file:
        file.write(initial_text)
    client = DisksClient.getLocalDisksClient(True)
    client.switch_disk("default")
    client.mkdir(test_dir)
    client.cd(test_dir)
    client.copy(
        src_name, f"/{test_dir}/{src_name}", disk_from="local", disk_to="default"
    )
    files = client.ls(".")
    assert files == [src_name]
    client.move(src_name, "b.txt")
    files = client.ls(".")
    assert files == ["b.txt"]
    read_text = client.read(f"/{test_dir}/b.txt")
    assert read_text == initial_text
    client.write("b.txt", "c.txt")
    read_text = client.read("c.txt")
    assert read_text == initial_text
    client.rm("b.txt")
    client.rm("c.txt")
    os.remove(src_name)
    client.cd('..')
    client.rm(test_dir)
