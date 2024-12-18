import io
import os
import pathlib
import select
import subprocess
from typing import Dict, List, Optional, Tuple, Union

import pytest

from helpers.cluster import ClickHouseCluster


class ClickHouseDisksException(Exception):
    pass


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
    default_disk_root_directory: str = "/var/lib/clickhouse"

    def __init__(self, bin_path: str, config_path: str, working_path: str):
        self.bin_path = bin_path
        self.working_path = working_path

        self.proc = subprocess.Popen(
            [bin_path, "disks", "--test-mode", "--config", config_path],
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

    def execute_query(self, query: str, timeout: float = 5.0) -> str:
        output = io.BytesIO()

        self.proc.stdin.write(query.encode() + b"\n")
        self.proc.stdin.flush()

        events = self.poller.poll(timeout)
        if not events:
            raise TimeoutError(f"Disks client returned no output")

        for fd_num, event in events:
            if event & (select.EPOLLIN | select.EPOLLPRI):
                file = self._fd_nums[fd_num]

                if file == self.proc.stdout:
                    while True:
                        chunk = file.readline()
                        if chunk.endswith(self.SEPARATOR):
                            break

                        output.write(chunk)

                elif file == self.proc.stderr:
                    error_line = self.proc.stderr.readline()
                    print(error_line)
                    raise ClickHouseDisksException(error_line.strip().decode())

            else:
                raise ValueError(f"Failed to read from pipe. Flag {event}")

        data = output.getvalue().strip().decode()
        return data

    def list_disks(self) -> List[Tuple[str, str]]:
        output = self.execute_query("list-disks")
        return list(
            sorted(
                map(
                    lambda x: (x.split(":")[0], ":".join(x.split(":")[1:])),
                    output.split("\n"),
                )
            )
        )

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
        if recursive:
            answer: Dict[str, List[str]] = dict()
            blocks = output.split("\n\n")
            for block in blocks:
                directory = block.split("\n")[0][:-1]
                files = block.split("\n")[1:]
                answer[directory] = files
            return answer
        else:
            return output.split("\n")

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
            binary_file = os.environ.get("CLICKHOUSE_TESTS_SERVER_BIN_PATH")
            current_working_directory = str(pathlib.Path().resolve())
            config_file = f"{current_working_directory}/test_disks_app_interactive/configs/config.xml"
            if not os.path.exists(DisksClient.default_disk_root_directory):
                os.mkdir(DisksClient.default_disk_root_directory)

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
        ("local", client.working_path),
    ]
    assert expected_disks_with_path == client.list_disks()
    assert client.current_disk_with_path() == ("default", "/")
    client.switch_disk("local")
    assert client.current_disk_with_path() == (
        "local",
        client.working_path,
    )


def test_disks_app_interactive_list_files_local():
    client = DisksClient.getLocalDisksClient(True)
    client.switch_disk("local")
    excepted_listed_files = sorted(os.listdir("test_disks_app_interactive/"))
    listed_files = sorted(client.ls("test_disks_app_interactive/"))
    assert excepted_listed_files == listed_files


def test_disks_app_interactive_list_directories_default():
    client = DisksClient.getLocalDisksClient(True)
    traversed_dir = client.ls(".", recursive=True)
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
    assert client.ls(".", recursive=True, show_hidden=False) == {".": []}


def test_disks_app_interactive_cp_and_read():
    initial_text = "File content"
    with open("a.txt", "w") as file:
        file.write(initial_text)
    client = DisksClient.getLocalDisksClient(True)
    client.switch_disk("default")
    client.copy("a.txt", "/a.txt", disk_from="local", disk_to="default")
    read_text = client.read("a.txt")
    assert initial_text == read_text
    client.mkdir("dir1")
    client.copy("a.txt", "/dir1/b.txt", disk_from="local", disk_to="default")
    read_text = client.read("a.txt", path_to="dir1/b.txt")
    assert "" == read_text
    read_text = client.read("/dir1/b.txt")
    assert read_text == initial_text
    with open(f"{DisksClient.default_disk_root_directory}/dir1/b.txt", "r") as file:
        read_text = file.read()
        assert read_text == initial_text
    os.remove("a.txt")
    client.rm("a.txt")
    client.rm("/dir1", recursive=True)


def test_disks_app_interactive_test_move_and_write():
    initial_text = "File content"
    with open("a.txt", "w") as file:
        file.write(initial_text)
    client = DisksClient.getLocalDisksClient(True)
    client.switch_disk("default")
    client.copy("a.txt", "/a.txt", disk_from="local", disk_to="default")
    files = client.ls(".")
    assert files == ["a.txt"]
    client.move("a.txt", "b.txt")
    files = client.ls(".")
    assert files == ["b.txt"]
    read_text = client.read("/b.txt")
    assert read_text == initial_text
    client.write("b.txt", "c.txt")
    read_text = client.read("c.txt")
    assert read_text == initial_text
    os.remove("a.txt")
