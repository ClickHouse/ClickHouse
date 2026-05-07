import os
import shlex
import shutil
import signal
import subprocess
import time
import urllib.request
from pathlib import Path

from ci.praktika.utils import Shell, Utils

repo_dir = Utils.cwd()
temp_dir = f"{repo_dir}/ci/tmp"


class ClickHouseService:
    def __init__(
        self,
        ch_config_dir: str = f"{temp_dir}/etc/clickhouse-server",
        ch_var_lib_dir: str = f"{temp_dir}/var/lib/clickhouse",
        run_path: str = f"{temp_dir}/run",
    ):
        self.ch_config_dir = ch_config_dir
        self.ch_var_lib_dir = ch_var_lib_dir
        self.run_path = run_path
        self.config_file = f"{ch_config_dir}/config.xml"
        self.pid_file = f"{ch_config_dir}/clickhouse-server.pid"
        self.log_dir = f"{temp_dir}/var/log/clickhouse-server"
        self.user_files_path = f"{run_path}/user_files"
        self._proc = None
        self._log_fd = None

    def __enter__(self):
        Utils.add_to_PATH(temp_dir)

        # Download binary if absent
        clickhouse_bin = Path(temp_dir) / "clickhouse"
        if not clickhouse_bin.exists():
            self._download_binary()

        # Create symlinks if absent
        for link_name in ("clickhouse-server", "clickhouse-client", "clickhouse-local"):
            link_path = Path(temp_dir) / link_name
            if not link_path.exists():
                Utils.link(clickhouse_bin, link_path)

        # Copy server config files if absent
        config_dir = Path(self.ch_config_dir)
        if not (config_dir / "config.xml").exists():
            config_dir.mkdir(parents=True, exist_ok=True)
            src_dir = Path("./programs/server")
            for name in ("config.xml", "users.xml"):
                shutil.copy(src_dir / name, config_dir / name)
            shutil.copytree(
                src_dir / "config.d",
                config_dir / "config.d",
                symlinks=False,
                dirs_exist_ok=True,
            )

        # Recreate data directory so it is owned by the current process user.
        # If the directory was created on the host by a different UID (e.g. 501
        # on macOS) and the server runs as root inside Docker, ClickHouse raises
        # MISMATCHING_USERS_FOR_PROCESS_AND_DATA and refuses to start.
        if Path(self.run_path).exists():
            shutil.rmtree(self.run_path)
        Path(self.run_path).mkdir(parents=True, exist_ok=True)
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)
        Path(self.pid_file).unlink(missing_ok=True)

        argv = [
            str(Path(temp_dir) / "clickhouse-server"),
            "--config-file", self.config_file,
            "--pid-file", self.pid_file,
            "--",
            "--path", self.run_path,
            "--user_files_path", self.user_files_path,
            "--top_level_domains_path", f"{self.ch_config_dir}/top_level_domains",
            "--logger.stderr", f"{self.log_dir}/stderr.log",
        ]
        print(f"Starting ClickHouse server: {shlex.join(argv)}")
        self._log_fd = open(f"{self.log_dir}/clickhouse-server.log", "w")
        self._proc = subprocess.Popen(
            argv,
            stderr=subprocess.STDOUT,
            stdout=self._log_fd,
            start_new_session=True,
            cwd=self.run_path,
        )

        try:
            self._wait_ready()
        except Exception:
            self.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *_):
        if self._proc is None:
            return
        try:
            pgid = os.getpgid(self._proc.pid)
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            self._proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(self._proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            self._proc.wait()
        if self._log_fd is not None:
            self._log_fd.close()
            self._log_fd = None

    @staticmethod
    def _download_binary() -> None:
        dest = Path(temp_dir) / "clickhouse"
        if dest.exists():
            print(f"ClickHouse binary already present at [{dest}], skipping download")
            dest.chmod(0o755)
            return
        arch = "aarch64" if Utils.is_arm() else "amd64"
        url = f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/{arch}/clickhouse"
        print(f"Downloading ClickHouse binary from [{url}] to [{dest}]")
        try:
            urllib.request.urlretrieve(url, dest)
            dest.chmod(0o755)
        except Exception as e:
            raise RuntimeError(f"Failed to download ClickHouse binary: {e}") from e

    def _print_server_log(self) -> None:
        log_path = Path(self.log_dir) / "clickhouse-server.log"
        if self._log_fd is not None:
            self._log_fd.flush()
        if log_path.exists():
            print(f"--- {log_path} ---")
            print(log_path.read_text(errors="replace")[-4096:])
            print("--- end ---")

    def _wait_ready(self, port: int = 9000, attempts: int = 30, delay: int = 2) -> None:
        # Wait for the pid file to appear, bail out early if the process exits
        pid = None
        for _ in range(attempts):
            if self._proc and self._proc.poll() is not None:
                self._print_server_log()
                raise RuntimeError(
                    f"Server process exited with code {self._proc.returncode}"
                )
            try:
                pid = int(Path(self.pid_file).read_text().strip())
                break
            except Exception:
                time.sleep(1)
        if pid is None:
            self._print_server_log()
            raise RuntimeError(f"Failed to get PID from [{self.pid_file}]")

        for attempt in range(attempts):
            _res, out, err = Shell.get_res_stdout_stderr(
                f'clickhouse-client --port {port} --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print("ClickHouse server ready")
                return
            print(f"Server not ready (attempt {attempt + 1}/{attempts}), err: {err}")
            time.sleep(delay)
            if self._proc and self._proc.poll() is not None:
                self._print_server_log()
                raise RuntimeError(
                    f"Server process exited with code {self._proc.returncode}"
                )
        self._print_server_log()
        raise RuntimeError(f"Server not ready after {attempts * delay}s")

    @staticmethod
    def collect_cores(directory) -> list:
        key_path = f"{repo_dir}/ci/defs/public.pem"
        assert Path(key_path).exists(), f"RSA public key not found: {key_path}"
        aes_key_path = str(Path(directory) / "aes.key")
        encrypted = []
        for core in sorted(Path(directory).glob("core.*"))[:3]:
            if not core.name.endswith(".zst") and not core.name.endswith(".enc"):
                zst_path = Utils.compress_zst(core)
                encrypted.append(Utils.encrypt(str(zst_path), key_path, aes_key_path))
        if encrypted and Path(f"{aes_key_path}.rsa").exists():
            encrypted.append(f"{aes_key_path}.rsa")
        return encrypted
