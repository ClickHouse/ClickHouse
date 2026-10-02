"""Job-local S3 endpoint: isolated write namespaces and one shared read-only dataset."""

import os
import signal
import socket
import subprocess
import time
from pathlib import Path

from ci.jobs.scripts import seaweedfs_service
from ci.jobs.scripts.dataset_download import ICEBERG_DATASETS
from ci.praktika.utils import Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"

COLLECTION = "perf_s3"
READ_DATASET = "tpch_ice10"
READ_DATASET_DATABASE = f"{READ_DATASET}_s3"
READ_DATASET_DIRECTORY = ICEBERG_DATASETS[READ_DATASET][0]

# Fixed conventions of setup_seaweedfs.sh, shared with the stateless suite.
# TODO: parameterize port/bucket/credentials when the script reuse is cleaned up.
S3_PORT = seaweedfs_service.S3_PORT
S3_BUCKET = seaweedfs_service.BUCKET
S3_ACCESS_KEY = "clickhouse"
S3_SECRET_KEY = "clickhouse"


def endpoint_url(namespace):
    """One namespace of the shared store: left/right for writes, data for immutable reads."""
    return f"http://localhost:{S3_PORT}/{S3_BUCKET}/perf/{namespace}/"


def test_requires_s3(test_path):
    """Whether a test declares that it needs the job-local S3 endpoint."""
    from xml.etree import ElementTree

    return ElementTree.parse(test_path).getroot().get("requires_s3") == "1"


def test_requires_read_dataset(test_path):
    """Whether a test needs the shared TPC-H dataset attached before startup."""
    from xml.etree import ElementTree

    return ElementTree.parse(test_path).getroot().get("requires_s3_read_dataset") == "1"


def iceberg_s3_database_ddl_commands(server_path):
    """Attach the performance job's shared S3 fixture on one server."""
    metadata = f"{server_path}/db/metadata"
    directory, tables = ICEBERG_DATASETS[READ_DATASET]
    commands = [
        f"mkdir -p {metadata}/{READ_DATASET_DATABASE}",
        f'echo "ATTACH DATABASE {READ_DATASET_DATABASE} ENGINE=Ordinary" > {metadata}/{READ_DATASET_DATABASE}.sql',
    ]
    for table in tables:
        commands.append(
            f'echo "ATTACH TABLE {table} ENGINE = IcebergS3(perf_s3_data, filename = '
            f"'{directory}/{table}/')\" > {metadata}/{READ_DATASET_DATABASE}/{table}.sql"
        )
    return commands


def _client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=f"http://localhost:{S3_PORT}",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}),
    )


def seed_read_dataset(dataset_dir):
    """Upload the downloaded Iceberg dataset once; both servers read this S3 prefix."""
    from botocore.exceptions import ClientError

    source = Path(dataset_dir)
    if not source.is_dir():
        raise RuntimeError(f"S3 read dataset is missing: {source}")
    files = sorted(path for path in source.rglob("*") if path.is_file())
    if not files:
        raise RuntimeError(f"S3 read dataset is empty: {source}")

    s3 = _client()
    uploaded = 0
    for path in files:
        key = f"perf/data/{READ_DATASET_DIRECTORY}/{path.relative_to(source).as_posix()}"
        size = path.stat().st_size
        try:
            existing_size = s3.head_object(Bucket=S3_BUCKET, Key=key)["ContentLength"]
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                raise
            existing_size = None
        if existing_size != size:
            s3.upload_file(str(path), S3_BUCKET, key)
            uploaded += 1
            if s3.head_object(Bucket=S3_BUCKET, Key=key)["ContentLength"] != size:
                raise RuntimeError(f"S3 read dataset upload has wrong size: {key}")
    print(f"s3_service: shared read dataset verified ({len(files)} objects, {uploaded} uploaded)")
    return True


def _port_occupied():
    try:
        with socket.create_connection(("localhost", S3_PORT), timeout=1):
            return True
    except OSError:
        return False


def _owned_pid():
    try:
        pid = int((Path(temp_dir) / "seaweedfs.pid").read_text().strip())
        command = subprocess.check_output(
            ["ps", "-ww", "-p", str(pid), "-o", "args="], text=True
        )
        if "weed server " in command and f"-dir={temp_dir}/seaweedfs_data" in command:
            return pid
    except (OSError, ValueError, subprocess.CalledProcessError):
        pass
    return None


def ensure(log_path):
    """Bring up the job-local S3 endpoint; idempotent and fail-close."""
    if seaweedfs_service.is_healthy(S3_ACCESS_KEY, S3_SECRET_KEY):
        if _owned_pid() is None:
            print(f"s3_service: port {S3_PORT} has a healthy but unowned S3 endpoint")
            return False
        # Reuse keeps stage re-entry (praktika --param) working without re-provisioning.
        # The shared read dataset is seeded again by the caller; write suites use unique paths.
        print(f"s3_service: reusing the healthy S3 endpoint on localhost:{S3_PORT}")
        return True
    if _owned_pid() is not None:
        stop()
    elif _port_occupied():
        print(f"s3_service: port {S3_PORT} is occupied by an unowned endpoint")
        return False
    for _ in range(20):
        if not _port_occupied():
            break
        time.sleep(0.25)
    if _port_occupied():
        print(f"s3_service: port {S3_PORT} is occupied by an unusable endpoint")
        return False

    print(f"s3_service: starting the S3 endpoint via {seaweedfs_service.SETUP_SCRIPT}")
    # Stateful setup provides the bucket but no anonymous identity or uploaded test data.
    if not seaweedfs_service.start(
        "stateful",
        log_path,
        temp_dir,
        pid_file=f"{temp_dir}/seaweedfs.pid",
        disable_cache=True,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
    ):
        _print_log_tail(log_path)
        return False
    if _owned_pid() is None:
        print("s3_service: endpoint is not healthy after a successful setup")
        _print_log_tail(log_path)
        return False
    return True


def write_side_override(config_dir, side):
    """Point one server's `perf_s3` collection at its own namespace - the only per-server config delta."""
    # A config *file*, so compare.sh::restart (confirm_changes) starts the servers with it too.
    path = f"{config_dir}/config.d/zzz-perf-s3-side-override.xml"
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            f"""<!-- Generated by ci/jobs/scripts/perf/s3_service.py: this server's own namespace of the shared object store. -->
<clickhouse>
    <named_collections>
        <{COLLECTION}>
            <url replace="replace">{endpoint_url(side)}</url>
        </{COLLECTION}>
    </named_collections>
</clickhouse>
"""
        )
    print(f"{path}: {COLLECTION} url set to {endpoint_url(side)}")
    return True


def stop():
    """Stop the job-local S3 daemon (best effort)."""
    pid = _owned_pid()
    if pid is None:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    except OSError as error:
        print(f"s3_service: could not stop owned daemon {pid}: {error}")
        return
    # weed server drains its embedded volume server on SIGTERM (the volume
    # pre-stop grace period alone defaults to 10s). Five seconds reports a
    # spurious failure even though the daemon exits normally soon afterwards.
    for _ in range(120):
        if _owned_pid() is None and not _port_occupied():
            (Path(temp_dir) / "seaweedfs.pid").unlink(missing_ok=True)
            return
        time.sleep(0.25)
    print(f"s3_service: port {S3_PORT} remains occupied after stopping daemon {pid}")


def _print_log_tail(log_path):
    try:
        subprocess.run(["tail", "-n", "50", log_path], check=False)
    except OSError as error:
        print(f"s3_service: could not read {log_path}: {error}")
