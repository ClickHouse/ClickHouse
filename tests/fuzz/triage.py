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
    (r"heap-use-after-free",            "CRITICAL",  "heap-use-after-free"),
    (r"heap-buffer-overflow.*READ",     "HIGH",      "heap-buffer-overflow (read)"),
    (r"heap-buffer-overflow",           "HIGH",      "heap-buffer-overflow"),
    (r"use-after-free.*READ",           "HIGH",      "use-after-free (read)"),
    (r"use-after-free",                 "HIGH",      "use-after-free"),
    (r"stack-buffer-overflow",          "HIGH",      "stack-buffer-overflow"),
    (r"global-buffer-overflow",         "HIGH",      "global-buffer-overflow"),
    (r"stack-overflow",                 "HIGH",      "stack-overflow"),
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
# Also matches lines like "READ of size 8" that follow immediately.
SANITIZER_HEADER_RE = re.compile(
    r"==\d+==ERROR:\s+\S+:\s+(.+?)(?:\s+on\s+address|\s*$)",
    re.IGNORECASE,
)

# When `--fuzzer-binary` is provided, the sanitizer is told to write its log
# here so we can read it back.
ASAN_LOG_PATH = "/tmp/asan_triage.log"


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
    """Return (severity, crash_type) by scanning sanitizer output lines."""
    for line in text.splitlines():
        header_match = SANITIZER_HEADER_RE.search(line)
        if header_match:
            description = header_match.group(1).strip()
            for pattern, severity, canonical in SANITIZER_RULES:
                if re.search(pattern, description, re.IGNORECASE):
                    return severity, canonical
            # Matched the header but no specific rule — default to MEDIUM.
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
    """
    env = os.environ.copy()
    log_prefix = ASAN_LOG_PATH
    sanitizer_opts = (
        f"log_path={log_prefix}:halt_on_error=0:exitcode=0"
    )
    env["ASAN_OPTIONS"] = sanitizer_opts
    env["MSAN_OPTIONS"] = sanitizer_opts
    env["UBSAN_OPTIONS"] = f"log_path={log_prefix}:halt_on_error=0:exitcode=0"

    try:
        result = subprocess.run(
            [str(binary), str(crash_path)],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
        output = result.stderr + result.stdout

        # Also try to read the file written by log_path.
        for candidate in Path("/tmp").glob("asan_triage.log*"):
            try:
                output += candidate.read_text(errors="replace")
                candidate.unlink(missing_ok=True)
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


def group_crashes(crash_infos: List[CrashInfo]) -> List[CrashGroup]:
    """Group `CrashInfo` objects by stack hash and return sorted groups."""
    groups: Dict[str, CrashGroup] = {}

    for info in crash_infos:
        if info.stack_hash not in groups:
            groups[info.stack_hash] = CrashGroup(
                stack_hash=info.stack_hash,
                severity=info.severity,
                crash_type=info.crash_type,
                frames=info.frames,
            )
        groups[info.stack_hash].members.append(info.path)

    sorted_groups = sorted(
        groups.values(),
        key=lambda g: (SEVERITY_ORDER.index(g.severity), g.stack_hash),
    )
    return sorted_groups


def collect_crash_files(crashes_dir: Path) -> List[Path]:
    """Return all crash files in `crashes_dir` matching known prefixes."""
    prefixes = tuple(PREFIX_SEVERITY.keys())
    crash_files: List[Path] = []
    for entry in sorted(crashes_dir.iterdir()):
        if entry.is_file() and entry.name.startswith(prefixes):
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
                f"- **Other files:** "
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
        help="Directory containing crash-*, oom-*, timeout-*, leak-* files.",
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

    crashes_dir: Path = args.crashes_dir
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
