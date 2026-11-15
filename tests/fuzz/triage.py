#!/usr/bin/env python3
"""
Triage libFuzzer crashes: group by stack hash, classify severity, generate report.

Usage:
    python3 triage.py <crashes_dir> [--fuzzer-binary <path>] [--output <report.md>]

The script looks for files matching crash-*, oom-*, timeout-*, leak-* in the
crashes_dir and optional *.log or *.txt files alongside them.
"""

import argparse
import hashlib
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEVERITY_ORDER = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]

SEVERITY_BADGES = {
    "CRITICAL": "🔴 CRITICAL",
    "HIGH": "🟠 HIGH",
    "MEDIUM": "🟡 MEDIUM",
    "LOW": "🟢 LOW",
}

# Map sanitizer error tokens → (severity, canonical name)
# Patterns are matched against the sanitizer header line (case-insensitive).
SANITIZER_RULES: List[Tuple[str, str, str]] = [
    # pattern                           severity    canonical
    (r"heap-buffer-overflow.*WRITE",    "CRITICAL",  "heap-buffer-overflow (write)"),
    (r"heap-use-after-free.*WRITE",     "CRITICAL",  "heap-use-after-free (write)"),
    (r"use-after-free.*WRITE",          "CRITICAL",  "use-after-free (write)"),
    (r"heap-use-after-free.*READ",      "HIGH",      "heap-use-after-free (read)"),
    # The access direction is unknown here, so stay conservative: CRITICAL.
    (r"heap-use-after-free",            "CRITICAL",  "heap-use-after-free"),
    (r"heap-buffer-overflow.*READ",     "HIGH",      "heap-buffer-overflow (read)"),
    (r"heap-buffer-overflow",           "HIGH",      "heap-buffer-overflow"),
    (r"use-after-free.*READ",           "HIGH",      "use-after-free (read)"),
    (r"use-after-free",                 "HIGH",      "use-after-free"),
    (r"stack-buffer-overflow",          "HIGH",      "stack-buffer-overflow"),
    (r"global-buffer-overflow",         "HIGH",      "global-buffer-overflow"),
    (r"stack-overflow",                 "HIGH",      "stack-overflow"),
    # Allocator-contract violations are as exploitable as a use-after-free write.
    (r"double-free",                    "CRITICAL",  "double-free"),
    (r"alloc-dealloc-mismatch",         "CRITICAL",  "alloc-dealloc-mismatch"),
    (r"bad-free",                       "CRITICAL",  "bad-free"),
    (r"attempting free",                "CRITICAL",  "bad-free"),
    # Out-of-bounds and lifetime classes that are not covered by the heap rules above.
    (r"container-overflow",             "HIGH",      "container-overflow"),
    (r"stack-use-after-return",         "HIGH",      "stack-use-after-return"),
    (r"stack-use-after-scope",          "HIGH",      "stack-use-after-scope"),
    (r"negative-size-param",            "HIGH",      "negative-size-param"),
    (r"memcpy-param-overlap",           "HIGH",      "memcpy-param-overlap"),
    (r"use-of-uninitialized-value",     "HIGH",      "use-of-uninitialized-value"),
    (r"data race",                      "HIGH",      "data-race"),
    (r"lock-order-inversion",           "MEDIUM",    "lock-order-inversion"),
    (r"initialization-order-fiasco",    "MEDIUM",    "initialization-order-fiasco"),
    # Leaks are reported by LeakSanitizer; keep them aligned with the `leak` filename prefix.
    (r"detected memory leaks",          "LOW",       "memory-leak"),
    (r"undefined.behavio",              "MEDIUM",    "undefined-behavior"),
    (r"assertion.*failed",              "MEDIUM",    "assertion-failed"),
    (r"SEGV",                           "MEDIUM",    "SEGV"),
]

# Crash type inferred from the filename prefix when no sanitizer log is present.
PREFIX_SEVERITY: Dict[str, Tuple[str, str]] = {
    "crash":   ("MEDIUM",  "crash (unknown)"),
    "oom":     ("LOW",     "out-of-memory"),
    "timeout": ("LOW",     "timeout"),
    "leak":    ("LOW",     "memory-leak"),
}

# Number of top frames used to compute the stack hash.
HASH_FRAMES = 5

# Regex that matches a single sanitizer stack frame line, e.g.:
#     #0 0x55a3f1234abc in DB::SomeFunc /path/to/file.cpp:42:5
FRAME_RE = re.compile(
    r"^\s*#\d+\s+0x[0-9a-fA-F]+\s+in\s+(\S+)"
)

# Regex that matches the sanitizer error header, e.g.:
#   ==12345==ERROR: AddressSanitizer: heap-buffer-overflow on address …
#   ==12345==ERROR: AddressSanitizer: heap-buffer-overflow
SANITIZER_HEADER_RE = re.compile(
    r"==\d+==ERROR:\s+\S+:\s+(.+?)(?:\s+on\s+address|\s*$)",
    re.IGNORECASE,
)

# The access direction (READ / WRITE) is reported on the line that follows the
# sanitizer header, e.g. "WRITE of size 8 at 0x… thread T0". When classifying we
# therefore also look at a few lines after the header so that rules such as
# `heap-buffer-overflow.*WRITE` can match.
SANITIZER_CONTEXT_LINES = 3

# Basename used for per-invocation sanitizer log files inside a temporary directory.
ASAN_LOG_BASENAME = "asan_triage.log"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class CrashInfo:
    """Parsed information about a single crash file."""
    path: Path
    severity: str
    crash_type: str
    frames: List[str]
    stack_hash: str
    raw_error: str = ""


@dataclass
class CrashGroup:
    """A deduplicated group of crashes sharing the same stack hash."""
    stack_hash: str
    severity: str
    crash_type: str
    frames: List[str]
    members: List[Path] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Log parsing helpers
# ---------------------------------------------------------------------------

def _classify_from_sanitizer_text(text: str) -> Tuple[str, str]:
    """Return (severity, crash_type) by scanning sanitizer output lines.

    The error kind (e.g. ``heap-buffer-overflow``) is on the header line, but the
    access direction (``READ``/``WRITE``) is on the following line. We therefore
    match the rules against the header plus the next few lines so that write
    overflows are classified as CRITICAL rather than falling back to the generic
    read/unspecified severity.
    """
    lines = text.splitlines()
    for index, line in enumerate(lines):
        header_match = SANITIZER_HEADER_RE.search(line)
        if header_match:
            description = header_match.group(1).strip()
            # Join the header with the immediately following lines so that access
            # direction keywords (READ/WRITE) are visible to the rules.
            context = " ".join(
                [description] + lines[index + 1 : index + 1 + SANITIZER_CONTEXT_LINES]
            )
            for pattern, severity, canonical in SANITIZER_RULES:
                if re.search(pattern, context, re.IGNORECASE):
                    return severity, canonical
            # Matched the header but no specific rule. The listed rules cover the
            # memory-corruption classes explicitly, so anything left here is genuinely
            # unrecognised and MEDIUM is the right conservative default.
            return "MEDIUM", description
    return "", ""


def _classify_from_prefix(filename: str) -> Tuple[str, str]:
    """Return (severity, crash_type) from the crash filename prefix."""
    for prefix, (severity, crash_type) in PREFIX_SEVERITY.items():
        if filename.startswith(prefix + "-") or filename == prefix:
            return severity, crash_type
    return "MEDIUM", "crash (unknown)"


def _extract_frames(text: str) -> List[str]:
    """Extract function names from the top `HASH_FRAMES` stack frames."""
    frames: List[str] = []
    for line in text.splitlines():
        m = FRAME_RE.match(line)
        if m:
            frames.append(m.group(1))
            if len(frames) >= HASH_FRAMES:
                break
    return frames


def _compute_stack_hash(frames: List[str]) -> str:
    """Return an MD5 hex digest of the joined frame function names."""
    combined = "|".join(frames).encode("utf-8")
    return hashlib.md5(combined).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Log file discovery and fuzzer execution
# ---------------------------------------------------------------------------

def _find_log_for_crash(crash_path: Path) -> Optional[str]:
    """
    Look for a companion log file next to the crash file.
    Accepted extensions: .log, .txt — with the same stem as the crash file,
    or with the crash filename as a prefix.
    """
    parent = crash_path.parent
    stem = crash_path.name
    candidates = [
        parent / f"{stem}.log",
        parent / f"{stem}.txt",
    ]
    for candidate in candidates:
        if candidate.exists():
            try:
                return candidate.read_text(errors="replace")
            except OSError:
                pass
    return None


def _run_fuzzer_on_crash(binary: Path, crash_path: Path) -> Optional[str]:
    """
    Execute `binary crash_path` with sanitizer log-path options and return
    the captured stderr/log content, or None on failure.

    A per-invocation temporary directory is used for the sanitizer log so that
    concurrent triage runs do not interfere with each other.
    """
    env = os.environ.copy()
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_prefix = os.path.join(tmp_dir, ASAN_LOG_BASENAME)
            sanitizer_opts = f"log_path={log_prefix}:halt_on_error=0:exitcode=0"
            env["ASAN_OPTIONS"] = sanitizer_opts
            env["MSAN_OPTIONS"] = sanitizer_opts
            env["UBSAN_OPTIONS"] = f"log_path={log_prefix}:halt_on_error=0:exitcode=0"

            result = subprocess.run(
                [str(binary), str(crash_path)],
                capture_output=True,
                text=True,
                timeout=30,
                env=env,
            )
            output = result.stderr + result.stdout

            # Also read any files written by log_path (sanitizer appends PID suffix).
            for candidate in Path(tmp_dir).glob(f"{ASAN_LOG_BASENAME}*"):
                try:
                    output += candidate.read_text(errors="replace")
                except OSError:
                    pass

            return output if output.strip() else None
    except (subprocess.TimeoutExpired, OSError):
        return None


# ---------------------------------------------------------------------------
# Core triage logic
# ---------------------------------------------------------------------------

def parse_crash(crash_path: Path, fuzzer_binary: Optional[Path]) -> CrashInfo:
    """Parse a single crash file into a `CrashInfo` object."""
    sanitizer_text: Optional[str] = None

    # 1. Try companion log file.
    sanitizer_text = _find_log_for_crash(crash_path)

    # 2. Run the fuzzer binary if provided and no log found.
    if sanitizer_text is None and fuzzer_binary is not None:
        sanitizer_text = _run_fuzzer_on_crash(fuzzer_binary, crash_path)

    severity = ""
    crash_type = ""
    frames: List[str] = []
    raw_error = ""

    if sanitizer_text:
        severity, crash_type = _classify_from_sanitizer_text(sanitizer_text)
        frames = _extract_frames(sanitizer_text)
        # Grab first non-empty sanitizer header line for display.
        for line in sanitizer_text.splitlines():
            if SANITIZER_HEADER_RE.search(line):
                raw_error = line.strip()
                break

    # Fall back to filename-based classification if sanitizer parse failed.
    if not severity:
        severity, crash_type = _classify_from_prefix(crash_path.name)

    # If no frames were found, use the crash filename as a pseudo-frame so
    # each crash still gets a deterministic (though unique) hash.
    if not frames:
        frames = [crash_path.name]

    stack_hash = _compute_stack_hash(frames)

    return CrashInfo(
        path=crash_path,
        severity=severity,
        crash_type=crash_type,
        frames=frames,
        stack_hash=stack_hash,
        raw_error=raw_error,
    )


def _severity_rank(severity: str) -> int:
    """Return the sort rank of a severity (0 = worst). Unknown severities sort last."""
    try:
        return SEVERITY_ORDER.index(severity)
    except ValueError:
        return len(SEVERITY_ORDER)


def group_crashes(crash_infos: List[CrashInfo]) -> List[CrashGroup]:
    """Group `CrashInfo` objects by stack hash and return sorted groups.

    Every member of a group shares the same stack hash (and therefore the same
    frames), but members can still differ in severity and crash type - for
    example a `READ` and a `WRITE` overflow with identical top frames. The group
    must advertise its *worst* member, otherwise a `CRITICAL` write overflow
    seen after a `HIGH` read overflow would be reported as `HIGH`, defeating the
    tool's purpose of surfacing the worst crash per stack.
    """
    groups: Dict[str, CrashGroup] = {}

    for info in crash_infos:
        group = groups.get(info.stack_hash)
        if group is None:
            groups[info.stack_hash] = group = CrashGroup(
                stack_hash=info.stack_hash,
                severity=info.severity,
                crash_type=info.crash_type,
                frames=info.frames,
            )
        elif _severity_rank(info.severity) < _severity_rank(group.severity):
            # A worse crash reached the same stack: promote the group to it.
            group.severity = info.severity
            group.crash_type = info.crash_type
        group.members.append(info.path)

    sorted_groups = sorted(
        groups.values(),
        key=lambda g: (_severity_rank(g.severity), g.stack_hash),
    )
    return sorted_groups


def collect_crash_files(crashes_dir: Path) -> List[Path]:
    """Return all crash input files in `crashes_dir` matching known prefixes.

    Companion log/text files (e.g. ``crash-abc.log``) share the same prefix but
    are excluded so they are not mistakenly treated as fuzzer inputs.
    """
    prefixes = tuple(PREFIX_SEVERITY.keys())
    excluded_suffixes = {".log", ".txt", ".md"}
    crash_files: List[Path] = []
    for entry in sorted(crashes_dir.iterdir()):
        if (
            entry.is_file()
            and entry.name.startswith(prefixes)
            and entry.suffix not in excluded_suffixes
        ):
            crash_files.append(entry)
    return crash_files


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def _severity_counts(groups: List[CrashGroup]) -> Dict[str, int]:
    counts: Dict[str, int] = {s: 0 for s in SEVERITY_ORDER}
    for group in groups:
        counts[group.severity] += len(group.members)
    return counts


def generate_report(groups: List[CrashGroup], crashes_dir: Path) -> str:
    lines: List[str] = []

    lines.append("# libFuzzer Crash Triage Report")
    lines.append("")
    lines.append(f"**Crashes directory:** `{crashes_dir}`")
    lines.append("")

    counts = _severity_counts(groups)
    total = sum(counts.values())

    lines.append("## Summary")
    lines.append("")
    lines.append("| Severity | Unique Groups | Total Crashes |")
    lines.append("|----------|--------------|---------------|")
    for severity in SEVERITY_ORDER:
        group_count = sum(1 for g in groups if g.severity == severity)
        crash_count = counts[severity]
        badge = SEVERITY_BADGES[severity]
        lines.append(f"| {badge} | {group_count} | {crash_count} |")
    lines.append(f"| **Total** | **{len(groups)}** | **{total}** |")
    lines.append("")

    if not groups:
        lines.append("_No crashes found._")
        return "\n".join(lines)

    lines.append("## Crash Groups")
    lines.append("")

    for i, group in enumerate(groups, start=1):
        badge = SEVERITY_BADGES[group.severity]
        lines.append(f"### Group {i} — {badge}")
        lines.append("")
        lines.append(f"- **Stack hash:** `{group.stack_hash}`")
        lines.append(f"- **Crash type:** `{group.crash_type}`")
        lines.append(f"- **Occurrences:** {len(group.members)}")
        lines.append(f"- **Example file:** `{group.members[0].name}`")
        if len(group.members) > 1:
            lines.append(
                "- **Other files:** "
                + ", ".join(f"`{p.name}`" for p in group.members[1:])
            )
        lines.append("")
        lines.append("**Deduplicated stack frames (top 5):**")
        lines.append("")
        lines.append("```")
        for j, frame in enumerate(group.frames):
            lines.append(f"  #{j}  {frame}")
        lines.append("```")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _run_self_tests() -> int:
    """Run built-in regression tests. Returns 0 on success, 1 on failure."""

    def make(stack_hash: str, severity: str, crash_type: str) -> CrashInfo:
        return CrashInfo(
            path=Path(f"crash-{stack_hash}-{severity}"),
            severity=severity,
            crash_type=crash_type,
            frames=["frame_a", "frame_b"],
            stack_hash=stack_hash,
        )

    # A deduplicated group must advertise the worst severity/type regardless of
    # the order in which its members are seen (the read/write overflow case).
    read = make("h1", "HIGH", "heap-buffer-overflow (read)")
    write = make("h1", "CRITICAL", "heap-buffer-overflow (write)")
    for order in ([read, write], [write, read]):
        groups = group_crashes(order)
        assert len(groups) == 1, f"expected 1 group, got {len(groups)}"
        assert groups[0].severity == "CRITICAL", f"severity not promoted: {groups[0].severity}"
        assert groups[0].crash_type == "heap-buffer-overflow (write)", (
            f"crash_type not promoted: {groups[0].crash_type}"
        )
        assert len(groups[0].members) == 2, "both members must be kept"

    # Groups are sorted worst-first.
    order = group_crashes([make("h2", "LOW", "timeout"), make("h1", "CRITICAL", "x")])
    assert [g.severity for g in order] == ["CRITICAL", "LOW"], "groups not sorted worst-first"

    # A WRITE overflow must be CRITICAL even though the direction is on the line
    # following the sanitizer header.
    sample = (
        "==123==ERROR: AddressSanitizer: heap-buffer-overflow on address 0x1\n"
        "WRITE of size 4 at 0x1 thread T0\n"
        "    #0 0xdead in foo\n"
    )
    severity, crash_type = _classify_from_sanitizer_text(sample)
    assert severity == "CRITICAL", f"write overflow misclassified as {severity}"
    assert crash_type == "heap-buffer-overflow (write)", crash_type

    # A READ use-after-free is HIGH, not CRITICAL: the read-specific rule must win
    # over the generic `heap-use-after-free` one.
    sample = (
        "==123==ERROR: AddressSanitizer: heap-use-after-free on address 0x1\n"
        "READ of size 4 at 0x1 thread T0\n"
        "    #0 0xdead in foo\n"
    )
    severity, crash_type = _classify_from_sanitizer_text(sample)
    assert severity == "HIGH", f"read use-after-free misclassified as {severity}"
    assert crash_type == "heap-use-after-free (read)", crash_type

    # A WRITE use-after-free stays CRITICAL.
    sample = (
        "==123==ERROR: AddressSanitizer: heap-use-after-free on address 0x1\n"
        "WRITE of size 4 at 0x1 thread T0\n"
        "    #0 0xdead in foo\n"
    )
    severity, crash_type = _classify_from_sanitizer_text(sample)
    assert severity == "CRITICAL", f"write use-after-free misclassified as {severity}"
    assert crash_type == "heap-use-after-free (write)", crash_type

    # Without a direction line the severity stays conservative.
    sample = "==123==ERROR: AddressSanitizer: heap-use-after-free\n    #0 0xdead in foo\n"
    severity, crash_type = _classify_from_sanitizer_text(sample)
    assert severity == "CRITICAL", f"directionless use-after-free: {severity}"
    assert crash_type == "heap-use-after-free", crash_type

    # Memory-corruption classes that are not heap overflows or use-after-free must
    # still be classified explicitly instead of falling back to MEDIUM.
    explicit_cases = [
        ("AddressSanitizer: attempting double-free", "CRITICAL", "double-free"),
        ("AddressSanitizer: alloc-dealloc-mismatch", "CRITICAL", "alloc-dealloc-mismatch"),
        ("AddressSanitizer: attempting free on address which was not malloc()-ed", "CRITICAL", "bad-free"),
        ("AddressSanitizer: bad-free", "CRITICAL", "bad-free"),
        ("AddressSanitizer: container-overflow", "HIGH", "container-overflow"),
        ("AddressSanitizer: stack-use-after-return", "HIGH", "stack-use-after-return"),
        ("AddressSanitizer: stack-use-after-scope", "HIGH", "stack-use-after-scope"),
        ("AddressSanitizer: negative-size-param", "HIGH", "negative-size-param"),
        ("AddressSanitizer: memcpy-param-overlap", "HIGH", "memcpy-param-overlap"),
        ("MemorySanitizer: use-of-uninitialized-value", "HIGH", "use-of-uninitialized-value"),
        ("ThreadSanitizer: data race", "HIGH", "data-race"),
        ("ThreadSanitizer: lock-order-inversion", "MEDIUM", "lock-order-inversion"),
        ("AddressSanitizer: initialization-order-fiasco", "MEDIUM", "initialization-order-fiasco"),
        ("LeakSanitizer: detected memory leaks", "LOW", "memory-leak"),
    ]
    for header, expected_severity, expected_type in explicit_cases:
        sample = f"==123==ERROR: {header}\n    #0 0xdead in foo\n"
        severity, crash_type = _classify_from_sanitizer_text(sample)
        assert severity == expected_severity, f"{header!r}: {severity} != {expected_severity}"
        assert crash_type == expected_type, f"{header!r}: {crash_type!r} != {expected_type!r}"

    # A double-free must outrank a stack-buffer-overflow in the report ordering.
    order = group_crashes(
        [make("h3", "HIGH", "stack-buffer-overflow"), make("h4", "CRITICAL", "double-free")]
    )
    assert [g.crash_type for g in order] == ["double-free", "stack-buffer-overflow"], (
        f"double-free not sorted first: {[g.crash_type for g in order]}"
    )

    # A genuinely unrecognised sanitizer class still falls back to MEDIUM.
    sample = "==123==ERROR: AddressSanitizer: some-brand-new-check\n    #0 0xdead in foo\n"
    severity, crash_type = _classify_from_sanitizer_text(sample)
    assert severity == "MEDIUM", f"unknown class: {severity}"
    assert crash_type == "some-brand-new-check", crash_type

    print("All self-tests passed.", file=sys.stderr)
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Triage libFuzzer crashes: group by stack hash, classify severity, generate report.",
    )
    parser.add_argument(
        "crashes_dir",
        metavar="CRASHES_DIR",
        type=Path,
        nargs="?",
        default=None,
        help="Directory containing crash-*, oom-*, timeout-*, leak-* files.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run built-in regression tests and exit.",
    )
    parser.add_argument(
        "--fuzzer-binary",
        metavar="PATH",
        type=Path,
        default=None,
        help="Path to the fuzzer binary. When provided, each crash is replayed to obtain sanitizer output.",
    )
    parser.add_argument(
        "--output",
        metavar="REPORT",
        type=Path,
        default=Path("triage_report.md"),
        help="Output markdown report path (default: triage_report.md).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if args.self_test:
        return _run_self_tests()

    crashes_dir: Optional[Path] = args.crashes_dir
    if crashes_dir is None:
        print("error: CRASHES_DIR is required (or pass --self-test)", file=sys.stderr)
        return 1
    if not crashes_dir.is_dir():
        print(f"error: crashes directory not found: {crashes_dir}", file=sys.stderr)
        return 1

    fuzzer_binary: Optional[Path] = args.fuzzer_binary
    if fuzzer_binary is not None and not fuzzer_binary.is_file():
        print(f"error: fuzzer binary not found: {fuzzer_binary}", file=sys.stderr)
        return 1

    crash_files = collect_crash_files(crashes_dir)
    if not crash_files:
        print(f"No crash files found in {crashes_dir}", file=sys.stderr)

    print(f"Found {len(crash_files)} crash file(s). Parsing...", file=sys.stderr)

    crash_infos: List[CrashInfo] = []
    for crash_path in crash_files:
        info = parse_crash(crash_path, fuzzer_binary)
        crash_infos.append(info)
        print(
            f"  [{info.severity:8s}] {crash_path.name} — {info.crash_type}",
            file=sys.stderr,
        )

    groups = group_crashes(crash_infos)
    print(
        f"Grouped into {len(groups)} unique stack(s).",
        file=sys.stderr,
    )

    report = generate_report(groups, crashes_dir)

    output_path: Path = args.output
    output_path.write_text(report, encoding="utf-8")
    print(f"Report written to: {output_path}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
