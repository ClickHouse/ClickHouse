"""Ephemeral MinIO S3 endpoints for the performance-comparison job.

One private MinIO daemon per measured server, mirroring the harness's per-server
isolation (own db dir, own keeper, own ports): the write tests mutate the object
store, and sharing one store would let LEFT's and RIGHT's writes interfere with
the comparison. Both instances are seeded byte-identically from the single
downloaded Iceberg tree, so the read datasets stay identical on both sides.

Everything is deliberately self-contained so the job "just works" on every path
it can be launched from - CI, `praktika run` in docker mode, `--no-docker`, and
ch-build-tool:

* The pinned `minio`/`mc` binaries are downloaded at job runtime into the
  praktika temp dir (the same pattern the job uses for the reference ClickHouse
  binary and the dataset tarballs), so NO docker image change is needed - which
  also keeps the image digest stable for local docker-mode runs.
* No state is kept under HOME: the job container runs as a non-root user with
  an unwritable HOME, so `mc` gets an explicit MC_CONFIG_DIR and the server an
  explicit --certs-dir.
"""

import os
import subprocess
import time

from ci.praktika.utils import Shell

LEFT_PORT = 11110
RIGHT_PORT = 11120
BUCKET = "perf"
# Also hardcoded in the `iceberg_s3_perf` named collection
# (tests/performance/scripts/config/config.d/iceberg_s3_perf.xml).
USER = "clickhouse"
PASSWORD = "clickhouse"
START_TIMEOUT_SEC = 120

# Community releases matching the pins used by the integration tests
# (tests/integration/compose/docker_compose_minio.yml and the mc image tag).
SERVER_RELEASE = "RELEASE.2024-09-13T20-26-02Z"
CLIENT_RELEASE = "RELEASE.2025-04-16T18-13-26Z"

_CURL = "curl -fL --retry 5 --retry-all-errors --retry-delay 5 --connect-timeout 60 --max-time 600"


def _platform():
    system = os.uname().sysname.lower()  # linux / darwin
    machine = os.uname().machine
    arch = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}[machine]
    return f"{system}-{arch}"


def _service_dir(cache_dir):
    return f"{cache_dir}/minio-service"


def _minio_bin(cache_dir):
    return f"{_service_dir(cache_dir)}/{_platform()}/minio.{SERVER_RELEASE}"


def _mc(cache_dir):
    """The mc invocation prefix: explicit config dir instead of ~/.mc, because HOME
    is not writable for the non-root user the job container runs as."""
    return (
        f"MC_CONFIG_DIR={_service_dir(cache_dir)}/mc-config"
        f" {_service_dir(cache_dir)}/{_platform()}/mc.{CLIENT_RELEASE}"
    )


def _alias(port):
    return f"perf_minio_{port}"


def ensure_binaries(cache_dir):
    """Fetch the pinned minio + mc binaries into the cache dir (idempotent).

    The version is part of the file name, so bumping a pin re-downloads and a
    reused local ci/tmp never serves a stale version. Downloads go to a temp
    name first, so an interrupted fetch cannot leave a truncated "ready" binary."""
    platform_dir = f"{_service_dir(cache_dir)}/{_platform()}"
    Shell.check(f"mkdir -p {platform_dir}", strict=True)
    downloads = (
        (
            f"https://dl.min.io/server/minio/release/{_platform()}/archive/minio.{SERVER_RELEASE}",
            f"{platform_dir}/minio.{SERVER_RELEASE}",
        ),
        (
            f"https://dl.min.io/client/mc/release/{_platform()}/archive/mc.{CLIENT_RELEASE}",
            f"{platform_dir}/mc.{CLIENT_RELEASE}",
        ),
    )
    for url, target in downloads:
        if os.path.isfile(target):
            print(f"minio_service: reuse cached [{target}]")
            continue
        if not Shell.check(
            f"{_CURL} {url} -o {target}.tmp && chmod +x {target}.tmp && mv {target}.tmp {target}",
            verbose=True,
        ):
            print(f"minio_service: failed to download [{url}]")
            return False
    return True


def start(cache_dir, server_path, port):
    """Start one server's private MinIO daemon on 127.0.0.1:{port}.

    The state is made deterministic on every call: a daemon left over from a
    previous stage run is replaced and the data dir is recreated empty, then
    re-seeded by the caller - so a stage re-run cannot serve stale objects."""
    data_dir = f"{server_path}/minio"
    certs_dir = f"{server_path}/minio-certs"
    log_file = f"{server_path}/minio.log"
    Shell.check(f"pkill -f 'minio[^ ]* server {data_dir}' ||:", verbose=True)
    Shell.check(
        f"rm -rf {data_dir} {certs_dir} && mkdir -p {data_dir} {certs_dir}",
        verbose=True,
        strict=True,
    )
    with open(log_file, "w", encoding="utf-8") as log:
        subprocess.Popen(
            [
                _minio_bin(cache_dir),
                "server",
                data_dir,
                "--address",
                f"127.0.0.1:{port}",
                # Explicit certs dir: the default ~/.minio/certs needs a writable HOME.
                "--certs-dir",
                certs_dir,
            ],
            env={
                **os.environ,
                "MINIO_ROOT_USER": USER,
                "MINIO_ROOT_PASSWORD": PASSWORD,
                # Console off: one bound port per instance, nothing to collide.
                "MINIO_BROWSER": "off",
            },
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    # `mc alias set` issues a real authenticated request, so it doubles as the
    # readiness probe. Fail-close on timeout: no fallback, the stage fails.
    for _ in range(START_TIMEOUT_SEC):
        if Shell.check(
            f"{_mc(cache_dir)} alias set {_alias(port)} http://127.0.0.1:{port} {USER} {PASSWORD}"
        ):
            return Shell.check(f"{_mc(cache_dir)} mb {_alias(port)}/{BUCKET}", verbose=True)
        time.sleep(1)
    print(f"minio_service: 127.0.0.1:{port} did not become ready, log tail:")
    Shell.check(f"tail -n 50 {log_file} ||:", verbose=True)
    return False


def seed_tree(cache_dir, src_dir, dst_prefix, ports=(LEFT_PORT, RIGHT_PORT)):
    """Mirror one local dataset tree into every instance's bucket.

    All instances receive the same local tree, so LEFT and RIGHT serve
    byte-identical objects under s3://{BUCKET}/{dst_prefix}/ - the same
    invariant the local IcebergLocal attach gets from hardlinking db0 into both
    servers."""
    ok = True
    for port in ports:
        ok = (
            Shell.check(
                f"{_mc(cache_dir)} mirror --overwrite --remove {src_dir} {_alias(port)}/{BUCKET}/{dst_prefix}",
                verbose=True,
            )
            and ok
        )
    return ok


def stop_matching(path_fragment):
    """Stop every daemon whose data dir lives under the given path (job teardown)."""
    Shell.check(f"pkill -f 'minio[^ ]* server {path_fragment}' ||:", verbose=True)
