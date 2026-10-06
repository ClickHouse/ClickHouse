import argparse
import filecmp
import re
import shlex
import shutil
from dataclasses import dataclass
from functools import cache, partial
from itertools import zip_longest
from pathlib import Path
from typing import Dict, Set, Tuple

from ci.defs.defs import ArtifactConfigs, ArtifactNames, BuildTypes
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils


REPO_PATH = Path(Utils.cwd())
TEMP_PATH = REPO_PATH / "ci/tmp"
PACKAGED_PATH = TEMP_PATH / "clickhouse.compact"
SYMBOL_BLOB_PATH = TEMP_PATH / "clickhouse.symbols"
BASELINE_RANGES_PATH = TEMP_PATH / "compact_symbols_baseline_ranges.tsv"
PACKAGED_RANGES_PATH = TEMP_PATH / "compact_symbols_packaged_ranges.tsv"
STACKTRACE_LOG_PATH = TEMP_PATH / "compact_symbols_stacktrace.log"

MAX_RELATIVE_COUNT_DIFFERENCE = 0.001
MIN_DB_STACK_FRAMES = 3

state: Dict[str, int] = {}

ARTIFACT_NAMES_BY_BUILD_TYPE = {
    BuildTypes.AMD_RELEASE: (
        ArtifactNames.CH_AMD_RELEASE,
        ArtifactNames.COMPACT_SYMBOLS_AMD_RELEASE,
    ),
    BuildTypes.ARM_RELEASE: (
        ArtifactNames.CH_ARM_RELEASE,
        ArtifactNames.COMPACT_SYMBOLS_ARM_RELEASE,
    ),
}


@dataclass(frozen=True)
class ArtifactPaths:
    clickhouse: Path
    compact_symbols: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compact symbols check")
    parser.add_argument("--build-type", required=True, help="see BuildTypes.*")
    return parser.parse_args()


def resolve_artifact_paths(build_type: str) -> ArtifactPaths:
    if not isinstance(build_type, str) or build_type not in ARTIFACT_NAMES_BY_BUILD_TYPE:
        raise RuntimeError(f"Unsupported compact symbols build type: {build_type!r}")

    clickhouse_name, compact_symbols_name = ARTIFACT_NAMES_BY_BUILD_TYPE[build_type]
    artifact_configs = {
        artifact.name: artifact
        for artifact in (
            *ArtifactConfigs.clickhouse_binaries,
            *ArtifactConfigs.compact_symbols,
        )
    }

    def downloaded_path(artifact_name: str) -> Path:
        artifact_path = artifact_configs[artifact_name].path
        if not isinstance(artifact_path, str):
            raise RuntimeError(
                f"Artifact {artifact_name} must have a single path, got {artifact_path!r}"
            )
        return TEMP_PATH / Path(artifact_path).name

    paths = ArtifactPaths(
        clickhouse=downloaded_path(clickhouse_name),
        compact_symbols=downloaded_path(compact_symbols_name),
    )
    print(
        f"Using {build_type} artifacts: "
        f"{clickhouse_name} at {paths.clickhouse}, "
        f"{compact_symbols_name} at {paths.compact_symbols}"
    )
    return paths


def print_command_output(stdout: str, stderr: str) -> None:
    if stdout:
        print("stdout:")
        print(stdout)
    if stderr:
        print("stderr:")
        print(stderr)


def run_checked(command: str, show_output: bool = False) -> Tuple[str, str]:
    return_code, stdout, stderr = Shell.get_res_stdout_stderr(command, verbose=True)
    if return_code != 0:
        print_command_output(stdout, stderr)
        raise RuntimeError(f"Command failed with exit code {return_code}: {command}")
    if show_output:
        print_command_output(stdout, stderr)
    return stdout, stderr


@cache
def resolve_elf_tool(name: str) -> str:
    candidates = [
        *(f"llvm-{name}-{version}" for version in (22, 21, 20)),
        f"llvm-{name}",
        name,
    ]
    for candidate in candidates:
        executable = shutil.which(candidate)
        if executable:
            print(f"Using {name}: {executable}")
            return executable
    raise RuntimeError(
        f"Could not find a {name} executable; tried: {', '.join(candidates)}"
    )


def clickhouse_local_command(binary: Path, query: str) -> str:
    return (
        f"{shlex.quote(str(binary))} local "
        f"--allow_introspection_functions=1 --query {shlex.quote(query)}"
    )


def section_names(binary: Path) -> Set[str]:
    readelf = shlex.quote(resolve_elf_tool("readelf"))
    stdout, _ = run_checked(
        f"{readelf} --wide --sections {shlex.quote(str(binary))}"
    )
    names = set()
    for line in stdout.splitlines():
        match = re.match(r"\s*\[\s*\d+\]\s+(\S+)", line)
        if match:
            names.add(match.group(1))
    if not names:
        print(stdout)
        raise RuntimeError(f"Could not parse section names from {binary}")
    return names


def assert_section_layout(
    binary: Path, required: Set[str], forbidden: Set[str]
) -> None:
    names = section_names(binary)
    missing = required - names
    unexpected = forbidden & names
    if missing or unexpected:
        print(f"Sections in {binary}: {sorted(names)}")
        raise RuntimeError(
            f"Unexpected section layout for {binary}: "
            f"missing={sorted(missing)}, present_but_forbidden={sorted(unexpected)}"
        )
    print(
        f"Verified sections in {binary}: required={sorted(required)}, "
        f"forbidden={sorted(forbidden)}"
    )


def capture_symbol_counts(binary: Path) -> Tuple[int, int]:
    query = (
        "SELECT count(), "
        "countIf(startsWith(symbol, '_ZN2DB')) "
        "FROM system.symbols FORMAT TSV"
    )
    stdout, _ = run_checked(clickhouse_local_command(binary, query))
    fields = stdout.strip().split("\t")
    if len(fields) != 2:
        print(f"Unexpected symbol count output: {stdout!r}")
        raise RuntimeError(f"Could not parse symbol counts from {binary}")
    total_count, db_count = (int(field) for field in fields)
    if total_count <= 0 or db_count <= 0:
        raise RuntimeError(
            f"Symbol sanity check failed for {binary}: "
            f"total_count={total_count}, db_count={db_count}"
        )
    print(f"Symbols in {binary}: total={total_count}, DB={db_count}")
    return total_count, db_count


def capture_symbol_ranges(binary: Path, output: Path) -> None:
    query = (
        "SELECT DISTINCT address_begin, address_end "
        "FROM system.symbols ORDER BY address_begin, address_end FORMAT TSV"
    )
    command = (
        f"{clickhouse_local_command(binary, query)} "
        f"> {shlex.quote(str(output))}"
    )
    run_checked(command)
    if not output.is_file() or output.stat().st_size == 0:
        raise RuntimeError(f"Symbol range query produced no output for {binary}")
    print(f"Wrote {output.stat().st_size} bytes of sorted symbol ranges to {output}")


def prepare_release_binary(artifacts: ArtifactPaths) -> bool:
    for file_path in (artifacts.clickhouse, artifacts.compact_symbols):
        if not file_path.is_file():
            raise RuntimeError(f"Required artifact is missing: {file_path}")
        Shell.check(
            f"chmod +x {shlex.quote(str(file_path))}", verbose=True, strict=True
        )

    run_checked(f"{shlex.quote(str(artifacts.clickhouse))} --version", show_output=True)
    with artifacts.clickhouse.open("rb") as binary:
        if binary.read(4) != b"\x7fELF":
            raise RuntimeError(
                "The self-extracting artifact did not become an ELF file: "
                f"{artifacts.clickhouse}"
            )
    print(
        f"Decompressed release ELF size: {artifacts.clickhouse.stat().st_size} bytes"
    )
    return True


def collect_baseline(artifacts: ArtifactPaths) -> bool:
    assert_section_layout(
        artifacts.clickhouse,
        required={".symtab", ".strtab"},
        forbidden={".clickhouse.symbols"},
    )
    total_count, db_count = capture_symbol_counts(artifacts.clickhouse)
    state["baseline_total_count"] = total_count
    state["baseline_db_count"] = db_count
    capture_symbol_ranges(artifacts.clickhouse, BASELINE_RANGES_PATH)
    return True


def package_compact_symbols(artifacts: ArtifactPaths) -> bool:
    Shell.check(
        f"cp --reflink=auto --preserve=mode {shlex.quote(str(artifacts.clickhouse))} "
        f"{shlex.quote(str(PACKAGED_PATH))}",
        verbose=True,
        strict=True,
    )
    run_checked(
        f"{shlex.quote(str(artifacts.compact_symbols))} "
        f"{shlex.quote(str(PACKAGED_PATH))} {shlex.quote(str(SYMBOL_BLOB_PATH))}",
        show_output=True,
    )
    if not SYMBOL_BLOB_PATH.is_file() or SYMBOL_BLOB_PATH.stat().st_size == 0:
        raise RuntimeError(f"Compact symbols writer produced no blob: {SYMBOL_BLOB_PATH}")
    print(f"Compact symbols blob size: {SYMBOL_BLOB_PATH.stat().st_size} bytes")

    objcopy = shlex.quote(resolve_elf_tool("objcopy"))
    run_checked(
        f"{objcopy} --strip-all "
        f"--add-section=.clickhouse.symbols={shlex.quote(str(SYMBOL_BLOB_PATH))} "
        "--remove-section=.symtab --remove-section=.strtab "
        f"{shlex.quote(str(PACKAGED_PATH))}"
    )
    SYMBOL_BLOB_PATH.unlink()
    print(f"Packaged ELF size: {PACKAGED_PATH.stat().st_size} bytes")
    return True


def verify_packaged_sections() -> bool:
    assert_section_layout(
        PACKAGED_PATH,
        required={".clickhouse.symbols"},
        forbidden={".symtab", ".strtab"},
    )
    return True


def relative_difference(actual: int, expected: int) -> float:
    if expected <= 0:
        raise RuntimeError(f"Expected count must be positive, got {expected}")
    return abs(actual - expected) / expected


def assert_count_close(name: str, actual: int, expected: int) -> None:
    difference = relative_difference(actual, expected)
    print(
        f"{name}: baseline={expected}, packaged={actual}, "
        f"relative_difference={difference:.6%}"
    )
    if difference >= MAX_RELATIVE_COUNT_DIFFERENCE:
        raise RuntimeError(
            f"{name} differs by {difference:.6%}; required less than "
            f"{MAX_RELATIVE_COUNT_DIFFERENCE:.3%}"
        )


def print_first_range_differences() -> None:
    with BASELINE_RANGES_PATH.open(encoding="utf-8") as baseline, PACKAGED_RANGES_PATH.open(
        encoding="utf-8"
    ) as packaged:
        differences = 0
        for line_number, (baseline_line, packaged_line) in enumerate(
            zip_longest(baseline, packaged, fillvalue="<end of file>\n"), start=1
        ):
            if baseline_line == packaged_line:
                continue
            print(
                f"Range mismatch at line {line_number}: "
                f"baseline={baseline_line.rstrip()!r}, "
                f"packaged={packaged_line.rstrip()!r}"
            )
            differences += 1
            if differences == 10:
                break


def compare_symbol_tables() -> bool:
    total_count, db_count = capture_symbol_counts(PACKAGED_PATH)
    assert_count_close("Total symbol count", total_count, state["baseline_total_count"])
    assert_count_close("DB symbol count", db_count, state["baseline_db_count"])

    capture_symbol_ranges(PACKAGED_PATH, PACKAGED_RANGES_PATH)
    if not filecmp.cmp(
        BASELINE_RANGES_PATH, PACKAGED_RANGES_PATH, shallow=False
    ):
        print_first_range_differences()
        raise RuntimeError("Sorted sets of symbol address ranges differ")
    print("Sorted sets of (address_begin, address_end) ranges are identical")
    return True


def verify_symbolization() -> bool:
    stacktrace_query = "SELECT throwIf(1)"
    stacktrace_command = (
        f"{shlex.quote(str(PACKAGED_PATH))} local --stacktrace "
        f"--query {shlex.quote(stacktrace_query)}"
    )
    return_code, stdout, stderr = Shell.get_res_stdout_stderr(
        stacktrace_command, verbose=True
    )
    STACKTRACE_LOG_PATH.write_text(
        f"stdout:\n{stdout}\n\nstderr:\n{stderr}\n", encoding="utf-8"
    )
    if return_code == 0:
        print_command_output(stdout, stderr)
        raise RuntimeError("throwIf stack trace command unexpectedly succeeded")

    db_frames = re.findall(r"^\d+\. DB::", stderr, flags=re.MULTILINE)
    if "DB::Exception::Exception" not in stderr or len(db_frames) < MIN_DB_STACK_FRAMES:
        print_command_output(stdout, stderr)
        raise RuntimeError(
            "Stack trace did not contain DB::Exception::Exception and at least "
            f"{MIN_DB_STACK_FRAMES} DB frames; found {len(db_frames)} DB frames"
        )
    print(
        f"Stack trace contains DB::Exception::Exception and {len(db_frames)} DB frames"
    )

    smoke_query = (
        "SELECT address, demangle(addressToSymbol(address)) "
        "FROM "
        "(SELECT arrayJoin(trace) AS address FROM system.stack_trace) "
        "WHERE startsWith(demangle(addressToSymbol(address)), 'DB::') "
        "LIMIT 1 FORMAT TSV"
    )
    smoke_stdout, _ = run_checked(
        clickhouse_local_command(PACKAGED_PATH, smoke_query)
    )
    fields = smoke_stdout.strip().split("\t", 1)
    try:
        address = int(fields[0]) if len(fields) == 2 else 0
    except ValueError:
        address = 0
    if len(fields) != 2 or address <= 0 or not fields[1].startswith("DB::"):
        print(f"Unexpected addressToSymbol/demangle output: {smoke_stdout!r}")
        raise RuntimeError("addressToSymbol/demangle smoke test failed")
    print(f"Resolved stack address {fields[0]} to {fields[1]}")
    return True


def main() -> None:
    args = parse_args()
    stopwatch = Utils.Stopwatch()
    artifacts = resolve_artifact_paths(args.build_type)
    results = []
    stages = [
        ("Prepare release binary", partial(prepare_release_binary, artifacts)),
        ("Collect symtab baseline", partial(collect_baseline, artifacts)),
        ("Package compact symbols", partial(package_compact_symbols, artifacts)),
        ("Verify packaged sections", verify_packaged_sections),
        ("Compare symbol tables", compare_symbol_tables),
        ("Verify symbolization", verify_symbolization),
    ]

    for name, stage in stages:
        if results and not results[-1].is_ok():
            break
        results.append(Result.from_commands_run(name=name, command=stage))

    files = [STACKTRACE_LOG_PATH] if STACKTRACE_LOG_PATH.is_file() else []
    Result.create_from(
        results=results, stopwatch=stopwatch, files=files
    ).complete_job()


if __name__ == "__main__":
    main()
