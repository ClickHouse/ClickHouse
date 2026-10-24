"""Start and probe the job-local SeaweedFS used by CI test jobs."""

import os
import subprocess
from pathlib import Path


SETUP_SCRIPT = "./ci/jobs/scripts/functional_tests/setup_seaweedfs.sh"
SETUP_TIMEOUT_SEC = 240
S3_PORT = 11111
BUCKET = "test"


def is_healthy(access_key=None, secret_key=None):
    """Check the expected bucket with the credentials used by the setup script."""
    env = {
        **os.environ,
        "AWS_ACCESS_KEY_ID": access_key or os.environ.get("SEAWEEDFS_ACCESS_KEY", "clickhouse"),
        "AWS_SECRET_ACCESS_KEY": secret_key or os.environ.get("SEAWEEDFS_SECRET_KEY", "clickhouse"),
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    try:
        return subprocess.run(
            ["aws", "--endpoint-url", f"http://localhost:{S3_PORT}", "s3", "ls", f"s3://{BUCKET}"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
            check=False,
        ).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def start(test_type, log_path, temp_dir, *, pid_file=None, disable_cache=False, access_key=None, secret_key=None):
    """Provision the service and wait for authenticated bucket readiness."""
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    Path(temp_dir).mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "TEMP_DIR": str(temp_dir)}
    env.pop("SEAWEEDFS_PID_FILE", None)
    env.pop("SEAWEEDFS_DISABLE_CACHE", None)
    if access_key is not None:
        env["SEAWEEDFS_ACCESS_KEY"] = access_key
    if secret_key is not None:
        env["SEAWEEDFS_SECRET_KEY"] = secret_key
    if pid_file is not None:
        env["SEAWEEDFS_PID_FILE"] = str(pid_file)
    if disable_cache:
        env["SEAWEEDFS_DISABLE_CACHE"] = "1"

    with open(log_path, "w", encoding="utf-8") as log:
        proc = subprocess.Popen(
            [SETUP_SCRIPT, test_type, "./tests"],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
        )
    try:
        returncode = proc.wait(timeout=SETUP_TIMEOUT_SEC)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        print(f"SeaweedFS setup timed out after {SETUP_TIMEOUT_SEC}s; see {log_path}")
        return False
    if returncode != 0:
        print(f"SeaweedFS setup exited with {returncode}; see {log_path}")
        return False
    if not is_healthy(access_key, secret_key):
        print(f"SeaweedFS bucket {BUCKET} is not reachable; see {log_path}")
        return False
    return True
