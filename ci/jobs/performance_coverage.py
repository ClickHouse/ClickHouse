"""Collect performance-query coverage, independently of performance measurements."""

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tarfile
import time
from pathlib import Path

from clickhouse_driver import Client

from ci.jobs import performance_tests as perf
from ci.jobs.performance_coverage_resume import (
    RESUME_PROTOCOL,
    download_archive,
    pin_binary,
    restore_archive,
    source_identity,
    validate_options,
)
from ci.jobs.performance_coverage_sharding import (
    TIMINGS_PATH,
    assign_tests,
    load_timings,
)
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

EXPORT_RESERVE = 20 * 60


def stop_server(server, grace_seconds=10):
    """Wait for the dedicated server group, not just its launching shell."""
    process = server.proc
    # CHServer starts a new session. Keep this ID even after its shell exits.
    group = process.pid
    try:
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(group, sig)
            except ProcessLookupError:
                return
            deadline = time.monotonic() + grace_seconds
            while time.monotonic() < deadline:
                process.poll()  # Reap the shell if it has exited.
                states = subprocess.check_output(
                    ["ps", "-eo", "pgid=,stat="], text=True, timeout=5
                )
                # Container PID 1 may not reap orphaned zombies promptly.
                # They cannot hold the data-directory lock or execute queries.
                if not any(
                    int(pgid) == group and not state.startswith("Z")
                    for pgid, state in (line.split() for line in states.splitlines())
                ):
                    return
                time.sleep(0.1)
        raise RuntimeError(f"Coverage server process group {group} did not stop")
    finally:
        process.poll()
        if server.log_fd:
            server.log_fd.close()


def record_phase_manifests(output, manifest):
    """Let selectors verify the complete publication without downloading TSVs."""
    for filename, state in manifest["tests"].items():
        if state["status"] != "collected":
            continue
        data = json.loads((output / f"{Path(filename).stem}.json").read_text())
        names = sorted(phase["test_name"] for phase in data["phases"])
        if len(names) != len(set(names)):
            raise ValueError(f"Duplicate coverage phase in {filename}")
        state["phase_names_sha256"] = hashlib.sha256(
            "\n".join(names).encode()
        ).hexdigest()
        state["query_count"] = sum(
            not query.get("disabled") for query in data["queries"]
        )
    manifest["phase_manifest_version"] = 1


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


def coverage_command(path, output, warm_runs):
    return [
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
    ]


def run_tests(
    paths,
    output,
    deadline,
    warm_runs,
    manifest,
    save_manifest,
    cleanup,
    results=None,
    recover=None,
):
    if results is None:
        results = []
    for path in paths:
        state = manifest["tests"][path.name]
        if state["status"] == "collected":
            results.append(
                Result(
                    name=path.stem,
                    status=Result.Status.OK,
                    info="Restored complete coverage; see manifest provenance",
                )
            )
            continue
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
        command = coverage_command(path, output, warm_runs)
        sw = Utils.Stopwatch()
        state["status"] = "running"
        save_manifest()
        with log.open("w") as out:
            try:
                process = subprocess.run(
                    command,
                    stdout=out,
                    stderr=subprocess.STDOUT,
                    # Large XML files can legitimately need more than an hour.
                    # Bound setup, queries and teardown together by the shard's
                    # remaining collection budget, not release-query limits.
                    timeout=remaining,
                )
            except subprocess.TimeoutExpired:
                state.update(
                    status="failed",
                    reason="collection budget exhausted during test",
                    wall_seconds=sw.duration,
                )
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
                wall_seconds=sw.duration,
            )
            save_manifest()
            results.append(
                Result.create_from(
                    name=path.stem,
                    status=Result.Status.FAIL,
                    stopwatch=sw,
                    info=f"{state['reason']}; see {log}",
                )
            )
            # Restart and remove leftover test objects before attributing any
            # more coverage. If recovery fails, publish earlier completed tests.
            if recover is None:
                raise RuntimeError(f"Cannot recover after {path.name}")
            recover(path)
            if (
                test_discovery.test_requires_s3(path)
                and not s3_service.clear_write_namespaces()
            ):
                raise RuntimeError("S3 cleanup failed")
            cleanup()
            continue
        if skipped:
            state.update(status="skipped", reason=skipped)
        else:
            state["status"] = "collected"
        state["wall_seconds"] = sw.duration
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


def publish_coverage(binary, output, dest, job_name, check_start_time, manifest):
    """Publish only completed XMLs, retaining failed artifacts for diagnosis."""
    record_phase_manifests(output, manifest)
    collected = [
        name
        for name, state in manifest["tests"].items()
        if state["status"] == "collected"
    ]
    export = output / "export"
    export.mkdir()
    for name in collected:
        source = output / f"{Path(name).stem}.lines.tsv"
        if not source.is_file() or not source.stat().st_size:
            raise ValueError(f"Missing coverage rows for {name}")
        (export / source.name).symlink_to(source.resolve())
    if dest:
        if collected:
            IntegrationCoverageExporter(
                str(binary), str(export), dest, job_name, check_start_time
            ).do()
        # Separate usable coverage from the job verdict. Failed/not-run XMLs
        # remain absent from the selector's covered set and are always selected.
        manifest["coverage_published"] = True
    manifest["complete"] = all(
        state["status"] in ("collected", "skipped")
        for state in manifest["tests"].values()
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ch-path", default=perf.temp_dir)
    parser.add_argument("--test-options", default="1/1")
    parser.add_argument("--test", default="")
    parser.add_argument("--warm-runs", type=int, default=1)
    parser.add_argument("--resume-from", default="")
    parser.add_argument("--binary-url", default="")
    parser.add_argument("--binary-sha256", default="")
    parser.add_argument("--max-hours", choices=("6", "12"), default="12")
    args = parser.parse_args()
    if args.warm_runs < 1:
        parser.error("--warm-runs must be positive")
    batch, total = map(int, args.test_options.split(",")[-1].strip().split("/"))
    if not 1 <= batch <= total:
        parser.error("invalid batch")
    info = Info()
    if info.is_dispatch_event:
        args.resume_from = (
            info.get_workflow_input_value("performance_resume_from") or args.resume_from
        )
        args.max_hours = (
            info.get_workflow_input_value("performance_max_hours") or args.max_hours
        )
        args.binary_url = (
            info.get_workflow_input_value("performance_binary_url") or args.binary_url
        )
        args.binary_sha256 = (
            info.get_workflow_input_value("performance_binary_sha256")
            or args.binary_sha256
        )
    validate_options(
        args.resume_from,
        args.max_hours,
        args.binary_url,
        args.binary_sha256,
    )
    sw = Utils.Stopwatch()
    check_start_time = Utils.timestamp_to_str(sw.start_time)
    deadline = time.monotonic() + int(args.max_hours) * 3600 - EXPORT_RESERVE
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
    if args.test:
        paths = [path for path in paths if args.test in path.name]
    assignments, sharding = assign_tests(paths, total, load_timings())
    paths = assignments[batch - 1]
    sharding["timings_sha256"] = hashlib.sha256(TIMINGS_PATH.read_bytes()).hexdigest()
    sharding["shard"] = batch
    sharding["timings"] = {
        path.name: sharding["timings"][path.name]
        for path in paths
        if path.name in sharding["timings"]
    }
    print(f"Runtime-balanced shard {batch}/{total}: {len(paths)} XML files")
    print(f"New tests with estimated runtimes: {sharding['new_tests']}")
    print(f"Changed tests with estimated runtimes: {sharding['changed_tests']}")
    if not paths:
        parser.error("no matching tests")
    manifest = {
        "version": 1,
        "sha": info.sha,
        "check_start_time": check_start_time,
        "check_name": info.job_name,
        "warm_runs": args.warm_runs,
        "complete": False,
        "publication_version": 1,
        "coverage_published": False,
        "resume_protocol": RESUME_PROTOCOL,
        "source_identity": source_identity("HEAD"),
        "max_hours": int(args.max_hours),
        "tests": {path.name: {"status": "not_run"} for path in paths},
        "sharding": sharding,
    }

    def save_manifest():
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    save_manifest()
    results = []
    server = None
    collection_started = False
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
        if args.binary_url:
            manifest["binary_url"] = args.binary_url
            manifest["binary_sha256"] = pin_binary(
                binary, args.binary_url, args.binary_sha256
            )
            save_manifest()
            print(f"Pinned coverage binary verified: {manifest['binary_sha256']}")
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
        with binary.open("rb") as binary_file:
            manifest["binary_sha256"] = hashlib.file_digest(
                binary_file, "sha256"
            ).hexdigest()
        save_manifest()
        perf.attach_datasets(server.port, f"{perf.perf_right}/db")
        perf.create_iceberg_databases(
            server.port, perf.perf_right, needs_s3 and needs_s3_read
        )
        # Coverage uses the prebuilt parts, as the PGO collector does. The
        # timing job rewrites them to exercise write-time defaults; doing that
        # with LLVM counters made the 100M-row insert exceed its 30-minute
        # budget in every shard, before any test could collect coverage.
        # Keep the original rows and expose the fixture under the test name.
        client.execute("CREATE DATABASE IF NOT EXISTS test")
        client.execute("RENAME TABLE datasets.hits_v1 TO test.hits")
        manifest["dataset_layout"] = "prebuilt"
        save_manifest()
        if args.resume_from:
            available = subprocess.run(
                ["git", "cat-file", "-e", f"{args.resume_from}^{{commit}}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if available.returncode:
                subprocess.run(
                    ["git", "fetch", "--depth=1", "origin", args.resume_from],
                    check=True,
                )
            if source_identity(args.resume_from) != manifest["source_identity"]:
                raise ValueError("Resume source inputs differ from this collection")
            resume_archive = output.parent / "performance_coverage_resume.tar.gz"
            manifest["resume_url"] = download_archive(
                info,
                args.resume_from,
                resume_archive,
            )
            restored = restore_archive(
                resume_archive,
                output,
                manifest,
                args.resume_from,
                manifest["source_identity"],
            )
            print(
                f"Restored {len(restored)} complete tests; interrupted tests will run from setup"
            )
            save_manifest()
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

        def recover(path):
            # A failed query, mutation or failpoint must not survive into the
            # next test. Preserve the stopped process's log before restarting.
            client.disconnect()
            stop_server(server)
            shutil.copyfile(server.log_file, output / f"{path.stem}.server.log")
            if not server.start():
                raise RuntimeError(
                    f"Coverage server failed to restart after {path.name}"
                )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Collection budget exhausted during recovery")
            log = output / f"{path.stem}.cleanup.log"
            with log.open("w") as out:
                process = subprocess.run(
                    coverage_command(path, output, args.warm_runs)
                    + ["--coverage-cleanup-only"],
                    stdout=out,
                    stderr=subprocess.STDOUT,
                    timeout=remaining,
                )
            if (
                process.returncode
                or "teardown-complete" not in log.read_text().splitlines()
            ):
                raise RuntimeError(
                    f"Teardown failed after restarting {path.name}; see {log}"
                )

        collection_started = True
        run_tests(
            paths,
            output,
            deadline,
            args.warm_runs,
            manifest,
            save_manifest,
            cleanup,
            results,
            recover,
        )
    except Exception as error:
        results.append(
            Result(name="Collection", status=Result.Status.ERROR, info=str(error))
        )
    finally:
        for state in manifest["tests"].values():
            if state["status"] == "running":
                state.update(
                    status="failed", reason="collector interrupted during test"
                )
        save_manifest()
        if server and hasattr(server, "proc"):
            try:
                stop_server(server)
            except Exception as error:
                results.append(
                    Result(
                        name="Coverage server shutdown",
                        status=Result.Status.ERROR,
                        info=str(error),
                    )
                )
        if needs_s3:
            s3_service.stop()
    # Even a failed test or exhausted job budget must not discard completed
    # exports. Publish after stopping the server so interrupted work is gone.
    if collection_started:
        try:
            publish_coverage(
                binary, output, dest, info.job_name, check_start_time, manifest
            )
        except Exception as error:
            results.append(
                Result(
                    name="Coverage export", status=Result.Status.ERROR, info=str(error)
                )
            )
    save_manifest()
    archive = output.parent / "performance_coverage.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        for path in sorted(output.iterdir()):
            if path.is_file() and path.suffix != ".profraw":
                tar.add(path, arcname=path.name)
    files = [str(archive), str(output / "manifest.json")]
    if server and Path(server.log_file).is_file():
        files.append(server.log_file)
    Result.create_from(results=results, stopwatch=sw, files=files).complete_job(
        do_not_cache=True
    )


if __name__ == "__main__":
    main()
