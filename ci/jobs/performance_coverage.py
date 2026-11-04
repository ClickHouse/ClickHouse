"""Collect performance-query coverage, independently of performance measurements."""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path

from clickhouse_driver import Client

from ci.jobs import performance_tests as perf
from ci.jobs.scripts.cidb_cluster import CIDBCluster
from ci.jobs.scripts.dataset_download import (
    PERFORMANCE_DATASETS,
    download_and_extract_datasets,
)
from ci.jobs.scripts.integration_coverage_export import IntegrationCoverageExporter
from ci.jobs.scripts.perf import s3_service, test_discovery
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Utils

JOB_TIMEOUT = 6 * 3600
EXPORT_RESERVE = 20 * 60
TEST_TIMEOUT = 3600


def configure(binary):
    """The normal perf configuration and datasets, with one instrumented server."""
    config = Path(perf.perf_right_config)
    config.mkdir(parents=True)
    for name in ("config.xml", "users.xml"):
        shutil.copyfile(Path("programs/server") / name, config / name)
    shutil.copytree("programs/server/config.d", config / "config.d")
    shutil.copytree(
        "tests/performance/scripts/config/config.d",
        config / "config.d",
        dirs_exist_ok=True,
    )
    shutil.copytree("tests/performance/scripts/config/users.d", config / "users.d")
    shutil.copytree(
        "tests/config/top_level_domains", Path(perf.perf_wd) / "top_level_domains"
    )
    shutil.copyfile(
        "tests/config/config.d/top_level_domains_lists.xml",
        config / "config.d/top_level_domains_lists.xml",
    )
    for name in (
        "text_log.xml",
        "memory_profiler.yaml",
        "serverwide_trace_collector.xml",
        "jemalloc_flush_profile.yaml",
        "keeper_max_request_size.xml",
        "backups.xml",
        "ssh.xml",
        "storage_conf_local.xml",
    ):
        (config / "config.d" / name).unlink(missing_ok=True)
    # Instrumented code must not be remapped (the PGO collector does the same).
    (config / "config.d/zzzz-coverage.xml").write_text(
        '<clickhouse><remap_executable replace="replace">false</remap_executable></clickhouse>\n'
    )
    binary.chmod(binary.stat().st_mode | 0o111)
    for name in (
        "clickhouse",
        "clickhouse-server",
        "clickhouse-client",
        "clickhouse-local",
        "clickhouse-keeper",
    ):
        (Path(perf.perf_right) / name).symlink_to(binary)
    Utils.add_to_PATH(perf.perf_right)
    db = Path(perf.perf_right) / "db"
    db.mkdir()
    errors = download_and_extract_datasets(PERFORMANCE_DATASETS.values(), str(db))
    if errors:
        raise RuntimeError("\n".join(errors))
    perf.stage_datasets(str(db))
    (db / "user_files").mkdir(exist_ok=True)
    for path in Path("tests/performance/user_files").iterdir():
        (db / "user_files" / path.name).symlink_to(path.resolve())
    (Path(perf.perf_right) / "coordination").mkdir()


def run_tests(paths, output, deadline, warm_runs, manifest, save_manifest, cleanup):
    results = []
    for path in paths:
        state = manifest["tests"][path.name]
        if test_discovery.test_is_long(path) or test_discovery.test_has_shell_query(
            path
        ):
            state.update(
                status="skipped",
                reason="long test or shell subprocess coverage unsupported",
            )
            results.append(
                Result(
                    name=path.stem, status=Result.Status.SKIPPED, info=state["reason"]
                )
            )
            save_manifest()
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                "Collection budget exhausted; remaining tests are marked not_run"
            )
        log = output / f"{path.stem}.log"
        command = [
            sys.executable,
            "tests/performance/scripts/perf.py",
            str(path),
            "--host",
            "localhost",
            "--port",
            str(perf.CHServer.RIGHT_SERVER_PORT),
            "--coverage-output",
            str(output),
            "--coverage-warm-runs",
            str(warm_runs),
            "--stop-merges",
            "--jemalloc-purge",
            "disabled",
            "--prewarm-max-query-seconds",
            "600",
        ]
        sw = Utils.Stopwatch()
        state["status"] = "running"
        save_manifest()
        with log.open("w") as out:
            try:
                process = subprocess.run(
                    command,
                    stdout=out,
                    stderr=subprocess.STDOUT,
                    timeout=min(TEST_TIMEOUT, remaining),
                )
            except subprocess.TimeoutExpired:
                state.update(status="failed", reason="test timeout")
                save_manifest()
                raise
        lines = log.read_text().splitlines()
        skipped = next(
            (
                line.removeprefix("skipped\t")
                for line in lines
                if line.startswith("skipped\t")
            ),
            None,
        )
        complete = (output / f"{path.stem}.json").is_file()
        clean = "teardown-complete" in lines
        if process.returncode or (not skipped and not (complete and clean)):
            state.update(
                status="failed",
                reason=f"runner exit {process.returncode}, manifest={complete}, teardown={clean}",
            )
            save_manifest()
            # A failed/timed-out query can still be running on the server. Stop
            # the batch instead of attributing it to the next test.
            raise RuntimeError(f"{path.name}: {state['reason']}; see {log}")
        if skipped:
            state.update(status="skipped", reason=skipped)
        else:
            state["status"] = "collected"
        results.append(
            Result.create_from(
                name=path.stem,
                status=Result.Status.SKIPPED if skipped else Result.Status.OK,
                stopwatch=sw,
                info=skipped or "",
            )
        )
        save_manifest()
        if (
            test_discovery.test_requires_s3(path)
            and not s3_service.clear_write_namespaces()
        ):
            raise RuntimeError("S3 cleanup failed")
        cleanup()
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ch-path", default=perf.temp_dir)
    parser.add_argument("--test-options", default="1/1")
    parser.add_argument("--test", default="")
    parser.add_argument("--warm-runs", type=int, default=3)
    args = parser.parse_args()
    if args.warm_runs < 1:
        parser.error("--warm-runs must be positive")
    batch, total = map(int, args.test_options.split(",")[-1].strip().split("/"))
    if not 1 <= batch <= total:
        parser.error("invalid batch")
    info = Info()
    sw = Utils.Stopwatch()
    check_start_time = Utils.timestamp_to_str(sw.start_time)
    deadline = time.monotonic() + JOB_TIMEOUT - EXPORT_RESERVE
    output = Path(perf.temp_dir) / "performance_coverage"
    output.mkdir(parents=True, exist_ok=False)
    # Continuous `%c` profiling relocates counters away from the section read
    # by the per-test collector. Never inherit that setting from a coverage image.
    os.environ["LLVM_PROFILE_FILE"] = str(output / "%2m.profraw")
    os.environ.pop("CLICKHOUSE_COVERAGE_TEST_NAME", None)
    binary = (Path(args.ch_path) / "clickhouse").resolve(strict=True)
    paths = [
        Path("tests/performance") / name
        for name in test_discovery.list_test_files("tests/performance")
    ]
    paths = (
        [path for path in paths if args.test in path.name]
        if args.test
        else paths[batch - 1 :: total]
    )
    if not paths:
        parser.error("no matching tests")
    manifest = {
        "version": 1,
        "sha": info.sha,
        "check_start_time": check_start_time,
        "check_name": info.job_name,
        "warm_runs": args.warm_runs,
        "complete": False,
        "tests": {path.name: {"status": "not_run"} for path in paths},
    }

    def save_manifest():
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    save_manifest()
    results = []
    server = None
    needs_s3 = any(
        test_discovery.test_requires_s3(path)
        for path in paths
        if not test_discovery.test_is_long(path)
        and not test_discovery.test_has_shell_query(path)
    )
    needs_s3_read = any(
        test_discovery.test_requires_read_dataset(path) for path in paths
    )
    try:
        dest = None
        if not info.is_local_run:
            dest = CIDBCluster()
            if not dest.is_ready():
                raise RuntimeError("Coverage destination unavailable")
        configure(binary)
        if needs_s3:
            if not s3_service.ensure(f"{perf.perf_wd}/s3_server.log"):
                raise RuntimeError("S3 startup failed")
            if needs_s3_read and not s3_service.seed_read_dataset(
                f"{perf.perf_right}/db/user_files/{s3_service.READ_DATASET_DIRECTORY}"
            ):
                raise RuntimeError("S3 dataset seeding failed")
            if not s3_service.write_side_override(perf.perf_right_config, "right"):
                raise RuntimeError("S3 configuration failed")
        server = perf.CHServer(readiness_timeout=600)
        server.start_cmd += f" --logger.log {perf.perf_right}/clickhouse-server.log --logger.errorlog {perf.perf_right}/clickhouse-server.err.log"
        if not server.start():
            raise RuntimeError("Coverage server failed to start")
        client = Client(host="localhost", port=server.port, send_receive_timeout=1800)
        build_options = dict(
            client.execute(
                "SELECT name, value FROM system.build_options "
                "WHERE name IN ('WITH_COVERAGE', 'WITH_COVERAGE_DEPTH', 'GIT_HASH')"
            )
        )
        if any(
            build_options.get(name) not in ("ON", "1")
            for name in ("WITH_COVERAGE", "WITH_COVERAGE_DEPTH")
        ):
            raise RuntimeError(
                "Server does not have per-test LLVM coverage instrumentation"
            )
        # The build can be cached from an earlier commit; keep its actual source
        # revision alongside the workflow revision that supplies the XML tests.
        manifest["binary_sha"] = build_options["GIT_HASH"]
        save_manifest()
        perf.attach_datasets(server.port, f"{perf.perf_right}/db")
        perf.create_iceberg_databases(
            server.port, perf.perf_right, needs_s3 and needs_s3_read
        )
        perf.populate_data(server.port)
        user_files = Path(perf.perf_right) / "db/user_files"
        preserved = set(user_files.iterdir())

        def cleanup():
            # File-writing tests have no table whose drop would remove these.
            # Preserve only the fixtures installed before the test batch.
            for path in user_files.iterdir():
                if path in preserved:
                    continue
                if path.is_dir() and not path.is_symlink():
                    shutil.rmtree(path)
                else:
                    path.unlink()

        results += run_tests(
            paths, output, deadline, args.warm_runs, manifest, save_manifest, cleanup
        )
        if not any(
            state["status"] == "collected" for state in manifest["tests"].values()
        ):
            raise RuntimeError("No tests produced coverage")
        # The TSV layout is the integration collector's file-sink format, so
        # use its existing aggregation and eight-column CIDB export unchanged.
        if dest:
            IntegrationCoverageExporter(
                str(binary), str(output), dest, info.job_name, check_start_time
            ).do()
        manifest["complete"] = True
    except Exception as error:
        results.append(
            Result(name="Collection", status=Result.Status.ERROR, info=str(error))
        )
    finally:
        save_manifest()
        if server and hasattr(server, "proc"):
            server.terminate()
        if needs_s3:
            s3_service.stop()
    archive = output.parent / "performance_coverage.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for path in sorted(output.iterdir()):
            if path.suffix != ".profraw":
                tar.add(path, arcname=path.name)
    files = [str(archive), str(output / "manifest.json")]
    if server and Path(server.log_file).is_file():
        files.append(server.log_file)
    Result.create_from(results=results, stopwatch=sw, files=files).complete_job(
        do_not_cache=True
    )


if __name__ == "__main__":
    main()
