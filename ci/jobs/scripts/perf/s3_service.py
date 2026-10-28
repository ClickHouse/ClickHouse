"""Job-local S3 endpoint: isolated write namespaces and one shared read-only dataset."""

import os
import signal
import socket
import subprocess
import time
from pathlib import Path

from ci.jobs.scripts import seaweedfs_service
from ci.jobs.scripts.dataset_download import ICEBERG_DATASETS
from ci.praktika.utils import Shell, Utils

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
AWS_CLI = (
    f"AWS_ACCESS_KEY_ID={S3_ACCESS_KEY} AWS_SECRET_ACCESS_KEY={S3_SECRET_KEY} AWS_DEFAULT_REGION=us-east-1"
    f" aws --endpoint-url http://localhost:{S3_PORT}"
)
PUBLIC_BUCKET = "clickhouse-public-datasets"
PUBLIC_PREFIXES = ("lakes/uk_price_paid_iceberg/", "lakes/uk_price_paid_delta/", "lakes/uk_price_paid_hudi/")
CATALOG_PORT = 8181  # `weed server` default
CATALOG_BUCKET = "perf-catalog"
CATALOG_TABLES = 200


def endpoint_url(namespace):
    """One namespace of the shared store: left/right for writes, data for immutable reads."""
    return f"http://localhost:{S3_PORT}/{S3_BUCKET}/perf/{namespace}/"


def iceberg_s3_database_queries():
    """Queries that create the performance job's shared S3 fixture as a database on one running server. Idempotent."""
    directory, tables = ICEBERG_DATASETS[READ_DATASET]
    queries = [f"CREATE DATABASE IF NOT EXISTS {READ_DATASET_DATABASE}"]
    for table in tables:
        queries.append(
            f"CREATE TABLE IF NOT EXISTS {READ_DATASET_DATABASE}.{table} "
            f"ENGINE = IcebergS3(perf_s3_data, filename = '{directory}/{table}/')"
        )
    return queries


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


def seed_public_data(local_dir):
    """Copy the public prefixes under their keys into `perf/data/` for `requires_s3_public_data="1"`; `sync` skips what is there."""
    copies = (
        f"aws s3 sync --no-sign-request --region us-east-1 --only-show-errors --exclude '*.crc' s3://{PUBLIC_BUCKET}/{p} {local_dir}/{p}"
        f" && {AWS_CLI} s3 sync --only-show-errors {local_dir}/{p} s3://{S3_BUCKET}/perf/data/{p}"
        for p in PUBLIC_PREFIXES
    )
    return Shell.check(" && ".join(copies) + f" && rm -rf {local_dir}", verbose=True)


def seed_catalog(clickhouse):
    """For `requires_s3_catalog="1"`: table bucket `perf-catalog`, namespace `many`, tables t1..t200 with one row (id = n); idempotent."""
    catalog = f"http://localhost:{CATALOG_PORT}/v1"
    queries = [
        "SET allow_database_iceberg = 1, allow_insert_into_iceberg = 1, write_full_path_in_iceberg_metadata = 1;",
        f"CREATE DATABASE c ENGINE = DataLakeCatalog('{catalog}', '{S3_ACCESS_KEY}', '{S3_SECRET_KEY}') SETTINGS catalog_type = 'rest',"
        f" warehouse = 's3://{CATALOG_BUCKET}', storage_endpoint = 'http://localhost:{S3_PORT}/{CATALOG_BUCKET}',"
        f" catalog_credential = '{S3_ACCESS_KEY}:{S3_SECRET_KEY}', oauth_server_uri = '{catalog}/oauth/tokens';",
    ]
    for n in range(1, CATALOG_TABLES + 1):
        queries.append(
            f"CREATE TABLE IF NOT EXISTS c.`many.t{n}` (id Int64, s String) ENGINE = IcebergS3("
            f"'http://localhost:{S3_PORT}/{CATALOG_BUCKET}/many/t{n}/', '{S3_ACCESS_KEY}', '{S3_SECRET_KEY}');"
        )
        queries.append(f"INSERT INTO c.`many.t{n}` SELECT {n}, 'x' WHERE (SELECT count() FROM c.`many.t{n}`) = 0;")
    queries.append(
        f"SELECT throwIf(count() != {CATALOG_TABLES} OR sum(id) != {CATALOG_TABLES * (CATALOG_TABLES + 1) // 2})"
        f" FROM merge('c', '^many\\.t');"
    )
    Path(f"{temp_dir}/perf_catalog_seed.sql").write_text("\n".join(queries))
    shell = f"PATH=/:{temp_dir}:$PATH weed shell -master localhost:{S3_PORT + 1}"  # the image unpacks weed to /
    return Shell.check(
        f"timeout 60 sh -c 'until curl -sf -o /dev/null {catalog}/config; do sleep 1; done'"
        f" && (echo 's3tables.bucket -list -account {S3_ACCESS_KEY}' | {shell} | grep -q '{CATALOG_BUCKET}'"
        f" || echo 's3tables.bucket -create -name {CATALOG_BUCKET} -account {S3_ACCESS_KEY}' | {shell})"
        f" && {clickhouse} local --queries-file {temp_dir}/perf_catalog_seed.sql",
        verbose=True,
    )


def clear_write_namespaces():
    """Delete what a test wrote, so that every test starts with empty `perf/left/` and `perf/right/`; log the store size."""
    ok = Shell.check(
        " && ".join(f"{AWS_CLI} s3 rm --recursive --only-show-errors s3://{S3_BUCKET}/perf/{side}/" for side in ("left", "right")),
        verbose=True,
    )
    Shell.check(f"du -sh {temp_dir}/seaweedfs_data", verbose=True)
    return ok


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
        # The caller seeds the shared read dataset again if tests follow; write suites use unique paths.
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
