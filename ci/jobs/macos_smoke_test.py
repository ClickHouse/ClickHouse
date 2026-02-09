#!/usr/bin/env python3
"""
macOS Smoke Test

This script runs a basic smoke test for ClickHouse on native macOS.
It downloads the pre-built binary via public HTTP (no AWS credentials needed),
starts the server and executes a simple query to verify the binary works.
"""

import os
import signal
import time
from pathlib import Path

from ci.praktika._environment import _Environment
from ci.praktika.result import Result
from ci.praktika.runtime import RunConfig
from ci.praktika.utils import Shell, Utils

TEMP_DIR = Path(f"{Utils.cwd()}/ci/tmp")
BINARY_PATH = TEMP_DIR / "clickhouse"
DATA_DIR = TEMP_DIR / "data"
LOG_DIR = TEMP_DIR / "log"

S3_BUCKET_HTTP_ENDPOINT = "clickhouse-builds.s3.amazonaws.com"


def download_binary():
    """Download the ClickHouse binary from the public S3 HTTP endpoint."""
    env = _Environment.get()
    job_name = env.JOB_NAME

    if "arm_darwin" in job_name:
        artifact_name = "CH_ARM_DARWIN_BIN"
        build_job_name = "Build (arm_darwin)"
    else:
        raise RuntimeError(f"Cannot determine build type from job name: {job_name}")

    # Resolve S3 prefix, taking cache into account
    try:
        run_config = RunConfig.from_workflow_data()
        if artifact_name in run_config.cache_artifacts:
            record = run_config.cache_artifacts[artifact_name]
            prefix = _Environment.get_s3_prefix_static(
                record.pr_number, record.branch, record.sha
            )
            print(f"Using cached artifact from {record}")
        else:
            prefix = env.get_s3_prefix()
    except Exception as e:
        print(f"WARNING: Failed to read cache config ({e}), using current prefix")
        prefix = env.get_s3_prefix()

    normalized_build_job = Utils.normalize_string(build_job_name)
    url = f"https://{S3_BUCKET_HTTP_ENDPOINT}/{prefix}/{normalized_build_job}/clickhouse"
    print(f"Downloading binary from {url}")
    if not Shell.check(
        f"curl --fail -L -o {BINARY_PATH} '{url}'", verbose=True
    ):
        raise RuntimeError(f"Failed to download binary from {url}")


def prepare_directories():
    """Create necessary directories for ClickHouse."""
    for dir_path in [DATA_DIR, LOG_DIR]:
        dir_path.mkdir(parents=True, exist_ok=True)


def start_server():
    """Start ClickHouse server using embedded config with overrides."""
    cmd = (
        f"{BINARY_PATH} server --daemon"
        f" -- --path {DATA_DIR}/"
        f" --logger.log {LOG_DIR}/clickhouse-server.log"
        f" --logger.errorlog {LOG_DIR}/clickhouse-server.err.log"
        f" --logger.level information"
        f" --logger.console 0"
        f" --tcp_port 9000"
        f" --http_port 8123"
        f" --listen_host 127.0.0.1"
        f" --mlock_executable false"
    )
    print(f"Starting server: {cmd}")
    Shell.check(cmd, verbose=True)
    # Give server time to start
    time.sleep(5)


def wait_for_server(timeout=60):
    """Wait for server to become ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = Shell.check(
            f"{BINARY_PATH} client --query 'SELECT 1'",
            verbose=False,
        )
        if result:
            return True
        time.sleep(1)
    return False


def stop_server():
    """Stop ClickHouse server."""
    pid_file = DATA_DIR / "clickhouse-server.pid"
    if pid_file.exists():
        pid = int(pid_file.read_text().strip())
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
        except ProcessLookupError:
            pass


def collect_logs():
    """Collect server logs for the report."""
    log_files = []
    for log_file in LOG_DIR.glob("*.log"):
        log_files.append(str(log_file))
    return log_files


def main():
    stopwatch = Utils.Stopwatch()
    test_results = []

    # Download binary from public S3 HTTP endpoint
    download_result = Result.from_commands_run(
        name="Download binary",
        command=download_binary,
        with_info=True,
    )
    test_results.append(download_result)
    if not download_result.is_ok():
        Result.create_from(
            results=test_results,
            stopwatch=stopwatch,
        ).complete_job()
        return

    # Make binary executable
    test_results.append(
        Result.from_commands_run(
            name="Make binary executable",
            command=f"chmod +x {BINARY_PATH}",
        )
    )

    # Prepare directories
    prepare_directories()

    # Test: Get version
    test_results.append(
        Result.from_commands_run(
            name="Get version",
            command=f"{BINARY_PATH} --version",
            with_info=True,
        )
    )

    # Test: Start server
    server_started = False
    try:
        start_server()
        if wait_for_server():
            test_results.append(
                Result.create_from(
                    name="Start server",
                    status=Result.Status.SUCCESS,
                    info="Server started successfully",
                )
            )
            server_started = True
        else:
            # Collect error log for diagnostics
            err_log = LOG_DIR / "clickhouse-server.err.log"
            err_content = ""
            if err_log.exists():
                err_content = err_log.read_text()[-2000:]  # Last 2000 chars
            test_results.append(
                Result.create_from(
                    name="Start server",
                    status=Result.Status.FAILED,
                    info=f"Server failed to start within timeout\n{err_content}",
                )
            )
    except Exception as e:
        test_results.append(
            Result.create_from(
                name="Start server",
                status=Result.Status.FAILED,
                info=f"Exception starting server: {e}",
            )
        )

    # Test: Execute SELECT 1
    if server_started:
        test_results.append(
            Result.from_commands_run(
                name="Execute SELECT 1",
                command=f"{BINARY_PATH} client --query 'SELECT 1'",
                with_info=True,
            )
        )

        # Test: Check server info
        test_results.append(
            Result.from_commands_run(
                name="Check server info",
                command=f"{BINARY_PATH} client --query 'SELECT version(), hostName(), uptime()'",
                with_info=True,
            )
        )

    # Stop server
    stop_server()

    # Collect logs
    log_files = collect_logs()

    # Create final result
    result = Result.create_from(results=test_results, stopwatch=stopwatch)
    result.set_files(log_files)
    result.complete_job()


if __name__ == "__main__":
    main()
