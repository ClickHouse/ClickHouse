"""Nightly run of the data lake foreign-writer fuzzer (`tests/datalake_fuzzer`).

The fuzzer is a pytest module on top of the integration test harness: one ClickHouse node, MinIO,
the Iceberg REST catalog and an in-process Spark session, so it runs in the integration test runner
image with docker-in-docker. It is not part of the integration test suite: here it runs with fresh
random seeds and a large time budget instead.

Every `--param KEY=VALUE` pair is exported to the fuzzer, overriding the nightly budget below (see
`tests/datalake_fuzzer/foreign_writer_fuzz/budget.py` for the knobs). Pairs are separated by commas,
and a comma that is not followed by `KEY=` belongs to the value, so
`--param FOREIGN_WRITER_FUZZ_WRITERS=spark_iceberg,spark_delta,FOREIGN_WRITER_FUZZ_OPS=10` works.
`--test` is passed to pytest as test node IDs relative to `tests/datalake_fuzzer`, e.g.
`--test test_fuzz.py::test_replay`.
"""

import argparse
import os
import re
import shlex
from pathlib import Path

from ci.jobs.integration_test_job import (
    _COMPOSE_DIR,
    DOCKER_IN_DOCKER_LOG,
    IMAGES_ENV,
    get_images_from_compose_files,
    prefetch_failure_result,
    prefetch_images,
    start_docker_in_docker,
)
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

repo_dir = Utils.cwd()
temp_path = f"{repo_dir}/ci/tmp"
FUZZER_DIR = f"{repo_dir}/tests/datalake_fuzzer"

# The nightly budget. Writers run one after another, each for `FOREIGN_WRITER_FUZZ_TIME_BUDGET`
# seconds (checked between operations), plus up to `FOREIGN_WRITER_FUZZ_SHRINK_SECONDS` per distinct
# failure: 8 writers x 20 min is about 2 h 40 min, which leaves room for the cluster start, shrinking
# and the job timeout in `ci/defs/job_configs.py`.
NIGHTLY_BUDGET = {
    "FOREIGN_WRITER_FUZZ_BASE_SEED": "random",
    # An upper bound only: the time budget ends the run long before.
    "FOREIGN_WRITER_FUZZ_SEEDS": "100000",
    "FOREIGN_WRITER_FUZZ_OPS": "20",
    "FOREIGN_WRITER_FUZZ_TIME_BUDGET": "1200",
    "FOREIGN_WRITER_FUZZ_SHRINK_SECONDS": "300",
}

# The compose files of the cluster that `tests/datalake_fuzzer/conftest.py` starts.
# `with_iceberg_catalog=True` starts only the REST catalog.
COMPOSE_FILES = [
    _COMPOSE_DIR / "docker_compose_minio.yml",
    _COMPOSE_DIR / "docker_compose_iceberg_rest_catalog.yml",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Data lake foreign-writer fuzzer")
    parser.add_argument(
        "--param",
        help="Optional. Comma-separated KEY=VALUE pairs exported to the fuzzer",
        type=str,
        default="",
    )
    parser.add_argument(
        "--test",
        help="Optional. pytest node IDs relative to tests/datalake_fuzzer",
        default=[],
        nargs="+",
        action="extend",
    )
    parser.add_argument(
        "--path",
        help="Optional. Path to the clickhouse binary for a local run",
        type=str,
        default="",
    )
    # Accepted for `praktika run` compatibility, not used.
    parser.add_argument("--count", default=None)
    parser.add_argument("--debug", default=False, action="store_true")
    parser.add_argument("--path_1", default="")
    parser.add_argument("--workers", default=None)
    return parser.parse_args()


def parse_params(param: str) -> dict:
    result = {}
    for item in re.split(r",(?=\s*[A-Za-z_][A-Za-z0-9_]*=)", param):
        key, sep, value = item.partition("=")
        key = key.strip()
        if key:
            assert sep, f"Malformed --param item [{item}], expected KEY=VALUE"
            result[key] = value.strip()
    return result


def find_clickhouse_binary(info: Info, path: str) -> str:
    if path:
        return path
    candidates = [f"{temp_path}/clickhouse"]
    if info.is_local_run:
        candidates += [
            f"{repo_dir}/build/programs/clickhouse",
            f"{repo_dir}/clickhouse",
        ]
    for candidate in candidates:
        if Path(candidate).is_file():
            return candidate
    raise FileNotFoundError(
        f"ClickHouse binary not found in any of {candidates}, pass --path"
    )


def main():
    args = parse_args()
    info = Info()
    sw = Utils.Stopwatch()

    clickhouse_path = find_clickhouse_binary(info, args.path)
    Shell.check(f"chmod +x {clickhouse_path}", verbose=True, strict=True)
    Shell.check(f"{clickhouse_path} --version", verbose=True, strict=True)

    if not Shell.check("docker info > /dev/null 2>&1", verbose=True):
        start_docker_in_docker()
    Shell.check("docker info > /dev/null", verbose=True, strict=True)

    for image_name, env_name in IMAGES_ENV.items():
        tag = info.docker_tag(image_name)
        assert tag, f"No tag found for image [{image_name}]"
        os.environ[env_name] = tag

    prefetched = set()
    if not prefetch_images(
        get_images_from_compose_files(COMPOSE_FILES), fetched_out=prefetched
    ):
        prefetch_failure_result().complete_job()
    server_image = f"clickhouse/integration-test:{os.environ['DOCKER_BASE_TAG']}"
    if server_image not in prefetched:
        prefetch_images([server_image], fetched_out=prefetched)

    java_path = Shell.get_output(
        r"update-alternatives --config java | sed -n 's/.*(providing \/usr\/bin\/java): //p'",
        verbose=True,
    )
    fuzz_env = {**NIGHTLY_BUDGET, **parse_params(args.param)}
    # `Result.from_pytest_run` exports these into the environment and prints them.
    env = {
        **fuzz_env,
        "CLICKHOUSE_TESTS_BASE_CONFIG_DIR": f"{repo_dir}/programs/server",
        "CLICKHOUSE_TESTS_SERVER_BIN_PATH": clickhouse_path,
        "CLICKHOUSE_BINARY": clickhouse_path,
        "CLICKHOUSE_TESTS_CLIENT_BIN_PATH": clickhouse_path,
        "CLICKHOUSE_TESTS_PREFETCHED_IMAGES": " ".join(sorted(prefetched)),
        "JAVA_PATH": java_path,
    }

    tests = " ".join(shlex.quote(t) for t in args.test) or "test_fuzz.py"
    test_result = Result.from_pytest_run(
        command=f"{tests} --tb=short",
        cwd=FUZZER_DIR,
        name="Data lake fuzzer",
        env=env,
        pytest_report_file=f"{temp_path}/pytest_datalake_fuzzer.jsonl",
        pytest_logfile=f"{temp_path}/pytest_datalake_fuzzer.log",
        logfile=f"{temp_path}/datalake_fuzzer.log",
    )

    # Server logs, Spark logs and the minimal failing histories (`foreign_writer_fuzz_*.json`) are
    # under the cluster's instances directory.
    logs_archive = f"{temp_path}/datalake_fuzzer_logs.tar.zst"
    Shell.check(
        f"sudo chown -R $(id -u):$(id -g) {FUZZER_DIR} 2>/dev/null;"
        f" cd {FUZZER_DIR} && tar --zstd -cf {logs_archive} _instances*/ || :",
        verbose=True,
    )
    files = [
        f
        for f in (
            logs_archive,
            f"{temp_path}/pytest_datalake_fuzzer.log",
            f"{temp_path}/datalake_fuzzer.log",
            DOCKER_IN_DOCKER_LOG,
        )
        if Path(f).is_file()
    ]

    Result.create_from(results=[test_result], stopwatch=sw, files=files).complete_job()


if __name__ == "__main__":
    main()
