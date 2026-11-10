"""Smoke test of a cross-compiled ClickHouse build that nothing else in CI executes, run under QEMU user-mode emulation.

The build job only compiles these binaries, so a bug that shows at run time goes unnoticed. Example,
https://github.com/ClickHouse/ClickHouse/issues/127366: libunwind's `unw_getcontext` left callee-saved vector registers
byte-swapped on ppc64le, and `clickhouse-server` then failed to start in about 10-19% of runs under QEMU ("Both table name
and UUID are empty", from `SystemLogs::SystemLogs`). One start passes most of the time even with that bug, so set
QEMU_SMOKE_STARTS to start the server more times to reproduce a failure that happens only some of the time.

The default server config is used (with paths and ports overridden in `config.d`), not a minimal one: `createSystemLog`
returns early for a system log whose section is missing from the config, so a minimal config never runs the code that failed.

Usage: python3 -m ci.jobs.scripts.qemu_smoke <build type> <build dir> <repo dir> <work dir>
"""

import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from ci.defs.defs import BuildTypes
from ci.praktika.result import Result

NAME = "QEMU smoke test"
STARTS = int(os.environ.get("QEMU_SMOKE_STARTS", "1"))

HTTP_PORT = 18123
TCP_PORT = 19000
READY_TIMEOUT_S = 300
QUERY_TIMEOUT_S = 120
EMPTY_NAME_ERROR = "Both table name and UUID are empty"

ROUND_TRIP = [
    "CREATE TABLE smoke (id UInt64, s String) ENGINE = MergeTree ORDER BY id",
    "INSERT INTO smoke SELECT number, toString(number) FROM numbers(1000)",
]
ROUND_TRIP_QUERY = "SELECT concat(toString(count()), '-', toString(sum(id)), '-', max(s)) FROM smoke"
EXPECTED = "1000-499500-999"


@dataclass(frozen=True)
class Target:
    qemu: str  # the static user-mode emulator
    sysroot: str  # glibc of the cross sysroot, relative to the repository; the binary is dynamically linked against it
    # Guest path -> path in the sysroot, for dynamic loader links that the sysroot does not ship.
    links: tuple = ()
    # The guest loader (a guest path) is run explicitly with --library-path, because its built-in search path does not
    # reach the sysroot libraries and LD_LIBRARY_PATH makes ClickHouse re-execute itself (checkHarmfulEnvironmentVariables),
    # which does not work without a binfmt_misc handler.
    loader: str = ""
    library_path: str = ""
    # Guest files to create empty. Without /etc/ld.so.cache in the sysroot, QEMU hands the guest loader the host's,
    # which a big-endian guest misreads and crashes on when it looks for a library by the cache (s390x).
    empty_files: tuple = ()


# Build types that are cross-compiled and then run here. Adding one is a line, given a toolchain in cmake/linux/.
TARGETS = {
    BuildTypes.PPC64LE: Target(
        qemu="qemu-ppc64le-static",
        sysroot="contrib/sysroot/linux-powerpc64le/powerpc64le-linux-gnu/libc",
        # The binary asks for /lib64/ld64.so.2, but the sysroot only has lib/ld64.so.2.
        links=(("lib64/ld64.so.2", "lib/ld64.so.2"),),
        loader="lib64/ld64.so.2",
        library_path="/lib64",
    ),
    BuildTypes.RISCV64: Target(
        qemu="qemu-riscv64-static",
        sysroot="contrib/sysroot/linux-riscv64",
    ),
    BuildTypes.S390X: Target(
        qemu="qemu-s390x-static",
        sysroot="contrib/sysroot/linux-s390x/s390x-linux-gnu/libc",
        loader="lib/ld64.so.1",
        library_path="/lib64",
        empty_files=("etc/ld.so.cache",),
    ),
    BuildTypes.LOONGARCH64: Target(
        qemu="qemu-loongarch64-static",
        sysroot="contrib/sysroot/linux-loongarch64",
    ),
}


def overlay_sysroot(sysroot, links, empty_files, work):
    """A sysroot made of symlinks to `sysroot`, plus `links` and `empty_files`, for QEMU's -L; the real one is left untouched."""
    if not links and not empty_files:
        return sysroot
    root = work / "sysroot"
    root.mkdir()
    for entry in Path(sysroot).iterdir():
        (root / entry.name).symlink_to(entry)

    def make_room(guest):
        parent = Path(guest).parent
        top = parent.parts[0]
        if (root / top).is_symlink():  # a directory of the sysroot that gets a new entry: copy it as links
            real_dir = Path(sysroot) / top
            (root / top).unlink()
            (root / top).mkdir()
            for entry in real_dir.iterdir():
                (root / top / entry.name).symlink_to(entry)
        (root / parent).mkdir(parents=True, exist_ok=True)

    for guest, real in links:
        make_room(guest)
        (root / guest).symlink_to(Path(sysroot) / real)
    for guest in empty_files:
        make_room(guest)
        (root / guest).write_bytes(b"")
    return str(root)


def qemu_command(target, sysroot, binary, *args):
    command = [target.qemu, "-L", sysroot]
    if target.loader:
        command += [f"{sysroot}/{target.loader}", "--library-path", target.library_path]
    return [*command, binary, *args]


def http(method, query, timeout):
    request = urllib.request.Request(
        f"http://127.0.0.1:{HTTP_PORT}/" + (f"?query={urllib.parse.quote(query)}" if method == "GET" else ""),
        data=None if method == "GET" else query.encode(),
        method=method,
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode().strip()


def write_config(config_source, work):
    conf = work / "conf"
    # Only the two default files: the directory also holds config snippets for tests (zookeeper, clusters, ...).
    conf.mkdir()
    for name in ("config.xml", "users.xml"):
        shutil.copy(config_source / name, conf / name)
    (conf / "config.d").mkdir()
    (conf / "config.d" / "zz_smoke.xml").write_text(
        f"""<clickhouse>
    <logger>
        <level>information</level>
        <log>{work}/log/clickhouse-server.log</log>
        <errorlog>{work}/log/clickhouse-server.err.log</errorlog>
        <console>0</console>
    </logger>
    <path>{work}/data/</path>
    <tmp_path>{work}/data/tmp/</tmp_path>
    <user_files_path>{work}/data/user_files/</user_files_path>
    <format_schema_path>{work}/data/format_schemas/</format_schema_path>
    <google_protos_path>{work}/data/protos/</google_protos_path>
    <custom_cached_disks_base_directory>{work}/data/caches/</custom_cached_disks_base_directory>
    <user_directories>
        <local_directory>
            <path>{work}/data/access/</path>
        </local_directory>
    </user_directories>
    <http_port>{HTTP_PORT}</http_port>
    <tcp_port>{TCP_PORT}</tcp_port>
    <listen_host>127.0.0.1</listen_host>
</clickhouse>
"""
    )
    return conf / "config.xml"


def start_once(target, sysroot, binary, config_file, work):
    """Start the server once and run a small round trip. Returns (ok, why it failed, path of its stderr)."""
    shutil.rmtree(work / "data", ignore_errors=True)
    shutil.rmtree(work / "log", ignore_errors=True)
    (work / "data").mkdir()
    (work / "data" / "protos").mkdir()  # the server resolves this path at start-up and fails if it is missing
    (work / "log").mkdir()
    stderr = work / "server.err"
    with open(work / "server.out", "wb") as out, open(stderr, "wb") as err:
        # The watchdog re-executes /proc/self/exe, which under QEMU is the emulator and not the ClickHouse binary.
        proc = subprocess.Popen(
            qemu_command(target, sysroot, binary, "server", f"--config-file={config_file}"),
            cwd=work,
            stdout=out,
            stderr=err,
            # An empty TZ means UTC for ClickHouse. The guest cannot see the zoneinfo of the machine, and any other value
            # is looked up there and fails.
            env={**os.environ, "CLICKHOUSE_WATCHDOG_ENABLE": "0", "TZ": ""},
            start_new_session=True,
        )
    why = f"no answer within {READY_TIMEOUT_S}s"
    try:
        deadline = time.monotonic() + READY_TIMEOUT_S
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                why = f"the server exited with code {proc.returncode} before answering"
                break
            try:
                if http("GET", "SELECT 1", 10) == "1":
                    for statement in ROUND_TRIP:
                        http("POST", statement, QUERY_TIMEOUT_S)
                    got = http("GET", ROUND_TRIP_QUERY, QUERY_TIMEOUT_S)
                    if got != EXPECTED:
                        why = f"the round trip returned {got!r}, expected {EXPECTED!r}"
                    elif proc.poll() is not None:
                        why = f"the server exited with code {proc.returncode} after answering"
                    else:
                        return True, "", stderr
                    break
            except (urllib.error.URLError, OSError) as e:
                why = f"no answer within {READY_TIMEOUT_S}s, last error: {e}"
            time.sleep(1)
        return False, why, stderr
    finally:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()


def server_errors(stderr, work):
    text = ""
    for path in (stderr, work / "log" / "clickhouse-server.err.log"):
        if path.exists():
            text += path.read_text(errors="replace")
    return text


def check(build_dir, repo_dir, work_dir, build_type, starts=STARTS):
    target = TARGETS[build_type]
    repo, work = Path(repo_dir), Path(work_dir) / "qemu_smoke"
    binary = str(Path(build_dir) / "programs" / "clickhouse")
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    sysroot = overlay_sysroot(str(repo / target.sysroot), target.links, target.empty_files, work)
    if not shutil.which(target.qemu):
        return Result(name=NAME, status=Result.Status.ERROR, info=f"{target.qemu} is not installed in the builder image")

    results = []
    config_file = write_config(repo / "programs" / "server", work)
    failed, empty_name, files = 0, 0, []
    for i in range(1, starts + 1):
        ok, why, stderr = start_once(target, sysroot, binary, config_file, work)
        info = ""
        if not ok:
            failed += 1
            errors = server_errors(stderr, work)
            if EMPTY_NAME_ERROR in errors:
                empty_name += 1
                why += f"; '{EMPTY_NAME_ERROR}' (UNKNOWN_TABLE), see #127366"
            saved = work / f"failed_start_{i}.err"
            shutil.copy(stderr, saved)
            files.append(str(saved))
            info = f"{why}\n{errors[-3000:]}"
        print(f"start {i}/{starts}: {'ok' if ok else 'FAILED: ' + why}")
        if info:
            print(info)
        results.append(
            Result(name=f"server start {i}/{starts}", status=Result.Status.OK if ok else Result.Status.FAIL, info=info)
        )

    summary = f"failed starts: {failed} / {starts} ({empty_name} with '{EMPTY_NAME_ERROR}')"
    print(summary)
    return Result.create_from(name=NAME, results=results, files=files, info=summary)


if __name__ == "__main__":
    build_type, build_dir, repo_dir, work_dir = sys.argv[1:5]
    result = check(build_dir, repo_dir, work_dir, build_type)
    sys.exit(0 if result.is_ok() else 1)
