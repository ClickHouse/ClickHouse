"""Restore completed whole tests from a compatible performance coverage attempt."""

import ast
import hashlib
import json
import re
import subprocess
import tarfile
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path

# Bump when the collector changes fixture preparation, server settings, or the
# execution policy. Time limits may increase without invalidating completed tests.
RESUME_PROTOCOL = 1
INPUT_PATHS = (
    "tests/performance",
    "tests/benchmarks",
    "tests/config",
    "programs/server",
    "ci/jobs/performance_tests.py",
    "ci/jobs/scripts",
    "ci/defs/defs.py",
    "ci/docker/performance-comparison",
    "ci/docker/test-base",
)


def validate_options(resume_from, max_hours, binary_url="", binary_sha256=""):
    if resume_from and not re.fullmatch("[0-9a-f]{40}", resume_from):
        raise ValueError(
            "performance_resume_from must be an empty string or a full commit SHA"
        )
    if max_hours not in ("6", "12"):
        raise ValueError("performance_max_hours must be 6 or 12")
    if resume_from and not (binary_url and binary_sha256):
        raise ValueError("Performance recovery requires a pinned binary URL and SHA256")
    if binary_url or binary_sha256:
        url = urllib.parse.urlsplit(binary_url)
        if (
            url.scheme != "https"
            or url.netloc != "clickhouse-builds.s3.amazonaws.com"
            or url.query
            or url.fragment
            or ".." in urllib.parse.unquote(url.path).split("/")
            or not re.fullmatch(
                r"/(?:REFs/.+|PRs/[0-9]+)/[0-9a-f]{40}/[a-z0-9_]+/"
                r"build_amd_llvm_coverage_per_test/clickhouse",
                url.path,
            )
        ):
            raise ValueError(
                "performance_binary_url must identify an immutable coverage build"
            )
        if not re.fullmatch("[0-9a-f]{64}", binary_sha256):
            raise ValueError(
                "performance_binary_sha256 must be the unpacked binary SHA256"
            )


def pin_binary(binary, url, expected_sha256):
    """Replace the cached build only after verifying the exact unpacked artifact."""
    pinned = binary.with_name("clickhouse.pinned")
    with urllib.request.urlopen(url, timeout=120) as response, pinned.open("xb") as out:
        while chunk := response.read(1024 * 1024):
            out.write(chunk)
    pinned.chmod(pinned.stat().st_mode | 0o111)
    # CI publishes a self-extracting executable. Starting it replaces the
    # wrapper in place; manifests identify the unpacked executable, not the
    # compressed transport or its S3 ETag.
    subprocess.run([str(pinned), "--version"], check=True, timeout=120)
    with pinned.open("rb") as source:
        actual = hashlib.file_digest(source, "sha256").hexdigest()
    if actual != expected_sha256:
        raise ValueError(
            f"Pinned binary SHA256 mismatch: expected {expected_sha256}, got {actual}"
        )
    pinned.replace(binary)
    return actual


def source_identity(revision):
    """Hash inputs, not the workflow revision: wrapper-only fixes can resume."""
    entries = subprocess.check_output(
        ["git", "ls-tree", "-rz", revision, "--", *INPUT_PATHS]
    ).split(b"\0")
    entries = [
        entry
        for entry in entries
        if entry
        and not entry.endswith(b"\tREADME.md")
        and not entry.endswith(b"/README.md")
    ]
    collector = subprocess.check_output(
        ["git", "show", f"{revision}:ci/jobs/performance_coverage.py"], text=True
    )
    configure = next(
        node
        for node in ast.parse(collector).body
        if isinstance(node, ast.FunctionDef) and node.name == "configure"
    )
    digest = hashlib.sha256()
    digest.update(str(RESUME_PROTOCOL).encode())
    digest.update(b"\0".join(entries))
    digest.update(ast.dump(configure, include_attributes=False).encode())
    return digest.hexdigest()


def download_archive(info, revision, destination):
    # An immutable revision in the same branch and job namespace; never consume
    # a mutable 'latest' snapshot.
    branch = urllib.parse.quote(info.git_branch, safe="/")
    job = re.sub(r"[^a-z0-9]+", "_", info.job_name.lower()).strip("_")
    url = (
        "https://s3.amazonaws.com/clickhouse-test-reports/REFs/"
        f"{branch}/{revision}/nightlycoverage/{job}/performance_coverage.tar.gz"
    )
    with urllib.request.urlopen(url, timeout=120) as response, destination.open(
        "xb"
    ) as out:
        while chunk := response.read(1024 * 1024):
            out.write(chunk)
    return url


def restore_archive(archive, output, manifest, revision, expected_source):
    """Fail closed before importing any files; a killed XML is always rerun."""
    with tempfile.TemporaryDirectory(
        dir=output.parent, prefix="coverage_resume_"
    ) as temporary:
        staging = Path(temporary)
        # A single streaming pass avoids repeatedly decompressing the beginning
        # of a multi-gigabyte archive for every manifest/log/region file.
        with tarfile.open(archive, "r|gz") as tar:
            for member in tar:
                if not member.isfile() or Path(member.name).name != member.name:
                    raise ValueError(f"Unsafe coverage archive member: {member.name}")
                with (staging / member.name).open("xb") as out:
                    source = tar.extractfile(member)
                    while chunk := source.read(1024 * 1024):
                        out.write(chunk)

        def read(name):
            return (staging / name).read_bytes()

        previous = json.loads(read("manifest.json"))
        if previous["check_name"] != manifest["check_name"]:
            raise ValueError("Incompatible resume check_name")
        for key in ("binary_sha", "warm_runs", "dataset_layout"):
            if previous[key] != manifest[key]:
                raise ValueError(f"Incompatible resume {key}")
        if previous["version"] != 1 or previous["sha"] != revision:
            raise ValueError("Unexpected resume manifest revision/version")
        if list(previous["tests"]) != list(manifest["tests"]):
            raise ValueError("Resume would change the shard assignment")
        if previous.get("sharding", {}).get("assignment_sha256") != manifest.get(
            "sharding", {}
        ).get("assignment_sha256"):
            raise ValueError("Resume would change the full test assignment")
        if previous.get("resume_protocol", 1) != RESUME_PROTOCOL:
            raise ValueError("Incompatible coverage resume protocol")
        if previous.get("source_identity", expected_source) != expected_source:
            raise ValueError("Incompatible coverage source identity")
        if (
            previous.get("binary_sha256")
            and previous["binary_sha256"] != manifest["binary_sha256"]
        ):
            raise ValueError("Incompatible instrumented binary contents")

        restored = {}
        for filename, state in previous["tests"].items():
            if filename not in manifest["tests"] or state["status"] != "collected":
                continue
            test = Path(filename).stem
            data = json.loads(read(f"{test}.json"))
            if (
                data["version"] != 1
                or data["test"] != test
                or data["warm_runs"] != manifest["warm_runs"]
                or not isinstance(data["settings"], dict)
                or not isinstance(data["queries"], list)
            ):
                raise ValueError(f"Incompatible test manifest: {filename}")
            expected = {f"{test}.setup", f"{test}.teardown"}
            for index, query in enumerate(data["queries"]):
                content = {
                    key: value
                    for key, value in query.items()
                    if key not in ("index", "sha256")
                }
                digest = hashlib.sha256(
                    json.dumps(content, sort_keys=True, ensure_ascii=False).encode()
                ).hexdigest()
                if query["index"] != index or query["sha256"] != digest:
                    raise ValueError(f"Invalid query identity: {filename}:{index}")
                if not query.get("disabled"):
                    expected.update(
                        f"{test}.query{index}.{digest[:16]}.{phase}"
                        for phase in ("first", "warm")
                    )
            phases = [phase["test_name"] for phase in data["phases"]]
            if len(phases) != len(expected) or set(phases) != expected:
                raise ValueError(f"Incomplete phase manifest: {filename}")
            log = read(f"{test}.log").decode().splitlines()
            completed = [
                line.split("\t")[1:]
                for line in log
                if line.startswith("coverage-query\t")
            ]
            if (
                completed
                != [
                    [str(i), str(1 + manifest["warm_runs"])]
                    for i, query in enumerate(data["queries"])
                    if not query.get("disabled")
                ]
                or "teardown-complete" not in log
            ):
                raise ValueError(f"Incomplete execution or teardown: {filename}")
            seen = set()
            rows = 0
            with (staging / f"{test}.lines.tsv").open() as source:
                for raw in source:
                    name, file, start, end, depth, flag = raw.rstrip("\n").split("\t")
                    if (
                        not file
                        or not 0 < int(start) <= int(end)
                        or not 0 <= int(depth) <= 255
                        or int(flag) not in (0, 1, 2)
                    ):
                        raise ValueError(f"Invalid coverage region: {filename}")
                    seen.add(name)
                    rows += 1
            if not rows or rows != data["rows"] or seen != expected:
                raise ValueError(f"Incomplete source coverage: {filename}")
            origin = state.get(
                "origin",
                {
                    "sha": revision,
                    "check_start_time": previous["check_start_time"],
                    "check_name": previous["check_name"],
                },
            )
            restored[filename] = dict(state, origin=origin, resumed_from=revision)

        # Validation above is read-only. Import only complete test triplets;
        # never copy an interrupted test's dump, log or process profile.
        for filename in restored:
            for suffix in ("json", "log", "lines.tsv"):
                name = f"{Path(filename).stem}.{suffix}"
                with (output / name).open("xb") as out, (staging / name).open(
                    "rb"
                ) as source:
                    while chunk := source.read(1024 * 1024):
                        out.write(chunk)
        manifest["tests"].update(restored)
        return restored
