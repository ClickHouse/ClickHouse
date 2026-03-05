#!/usr/bin/env python3
"""Assembly analysis tool for compiled binaries.

Accepts a compiled binary plus a code identifier, finds the corresponding
machine code, and reports optimization opportunities with confidence levels.

Usage:
    analyze-assembly <binary> <identifier> [options]
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# ── Version ──────────────────────────────────────────────────────────────────

VERSION = "0.1.0"

# ── Exit Codes ───────────────────────────────────────────────────────────────

EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_USAGE = 2
EXIT_LOOKUP = 3
EXIT_FORMAT = 4
EXIT_DEBUG = 5
EXIT_TOOL = 6

# ── Error Prefixes ───────────────────────────────────────────────────────────

E_USAGE_BAD_ARGS = "E_USAGE_BAD_ARGS"
E_USAGE_CONFLICT = "E_USAGE_CONFLICT"
E_LOOKUP_NOT_FOUND = "E_LOOKUP_NOT_FOUND"
E_LOOKUP_AMBIGUOUS = "E_LOOKUP_AMBIGUOUS"
E_FORMAT_UNSUPPORTED = "E_FORMAT_UNSUPPORTED"
E_DEBUG_MISSING = "E_DEBUG_MISSING"
E_TOOL_MISSING = "E_TOOL_MISSING"
E_TOOL_OLD = "E_TOOL_OLD"
E_TOOL_FAILED = "E_TOOL_FAILED"
E_INTERNAL_PARSE = "E_INTERNAL_PARSE"
E_INTERNAL_UNEXPECTED = "E_INTERNAL_UNEXPECTED"

# ── Default Thresholds ───────────────────────────────────────────────────────

DEFAULT_THRESHOLDS = {
    "spill_density": 0.10,
    "branch_density": 0.25,
    "call_density": 0.12,
}

# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_TIMEOUT_SEC = 60
DEFAULT_MAX_INSTRUCTIONS = 500
DEFAULT_MAX_DISASSEMBLY_BYTES = 4 * 1024 * 1024  # 4 MiB
DEFAULT_MAX_FINDINGS = 20
MIN_LLVM_VERSION = 13  # for --symbolize-operands

# ── Data Classes ─────────────────────────────────────────────────────────────


@dataclass
class BinaryInfo:
    path: str
    format: str = ""          # ELF, Mach-O, COFF
    arch: str = ""            # x86_64, aarch64, ...
    endianness: str = ""      # little, big
    build_id: str = ""
    file_size: int = 0
    mtime: float = 0.0


@dataclass
class ToolInfo:
    name: str
    path: str = ""
    version: str = ""
    available: bool = False


@dataclass
class SymbolCandidate:
    mangled: str
    demangled: str
    address: int
    size: int
    sym_type: str = ""        # T, t, W, w, etc.
    section: str = ""


@dataclass
class Instruction:
    address: int
    raw_bytes: str
    mnemonic: str
    operands: str
    raw_line: str
    # classification
    is_branch: bool = False
    is_call: bool = False
    is_spill_store: bool = False
    is_spill_load: bool = False
    is_ret: bool = False
    is_nop: bool = False
    label: str = ""           # if this address has a label (e.g. <L0>)
    source_line: str = ""     # interleaved source, if any


@dataclass
class BasicBlock:
    label: str
    start_idx: int
    end_idx: int              # exclusive
    instructions: list[Instruction] = field(default_factory=list)
    successors: list[str] = field(default_factory=list)
    is_loop_header: bool = False
    is_landing_pad: bool = False


@dataclass
class Metrics:
    instruction_count: int = 0
    byte_size: int = 0
    branch_count: int = 0
    call_count: int = 0
    spill_store_count: int = 0
    spill_load_count: int = 0
    ret_count: int = 0
    nop_count: int = 0
    branch_density: float = 0.0
    call_density: float = 0.0
    spill_density: float = 0.0
    # per-block aggregates (weighted by block size)
    block_count: int = 0
    loop_count: int = 0
    max_block_branch_density: float = 0.0
    max_block_spill_density: float = 0.0


@dataclass
class Finding:
    id: str
    severity: str             # high, medium, low
    confidence: str           # high, medium, low
    summary: str
    evidence: list[str] = field(default_factory=list)
    why: str = ""
    impact_score: float = 0.0


@dataclass
class McaResult:
    """Results from llvm-mca analysis of a loop body."""
    loop_label: str
    iterations: int = 0
    total_cycles: float = 0.0
    throughput_ipc: float = 0.0          # instructions per cycle
    block_rthroughput: float = 0.0       # reciprocal throughput
    bottleneck: str = ""                 # resource name or empty
    pressure_summary: list[str] = field(default_factory=list)
    raw_output: str = ""


@dataclass
class PerfSample:
    """A single aggregated sample from --perf-map."""
    ip: int
    samples: int
    symbol: str = ""
    dso: str = ""


@dataclass
class PerfMapStats:
    """Accounting for --perf-map parsing."""
    total_rows: int = 0
    valid_rows: int = 0
    total_samples: int = 0
    rejected: dict[str, int] = field(default_factory=dict)  # reason -> count


@dataclass
class DiffResult:
    """Result of a --before/--after comparison."""
    match_method: str = ""               # mangled_symbol, debug_info, unresolved
    caveats: list[str] = field(default_factory=list)
    deltas: dict[str, float] = field(default_factory=dict)
    before_metrics: Metrics | None = None
    after_metrics: Metrics | None = None
    before_binary: BinaryInfo | None = None
    after_binary: BinaryInfo | None = None
    before_target: SymbolCandidate | None = None
    after_target: SymbolCandidate | None = None
    before_findings: list[Finding] = field(default_factory=list)
    after_findings: list[Finding] = field(default_factory=list)


@dataclass
class RunStats:
    timing_ms: dict[str, float] = field(default_factory=dict)
    cache: dict[str, Any] = field(default_factory=dict)
    input_stats: dict[str, int] = field(default_factory=dict)


@dataclass
class SourceLocation:
    file: str
    line: int


# ── Globals ──────────────────────────────────────────────────────────────────

_verbose = False
_redact_paths = True
_home_dir = str(Path.home())
_cwd_dir = os.getcwd()


def verbose(msg: str) -> None:
    if _verbose:
        print(f"  [trace] {msg}", file=sys.stderr)


def warn(msg: str) -> None:
    print(f"  [warn] {msg}", file=sys.stderr)


def error(code: str, msg: str, exit_code: int) -> None:
    print(f"{code}: {msg}", file=sys.stderr)
    sys.exit(exit_code)


def redact(path: str) -> str:
    if not _redact_paths:
        return path
    if os.path.isabs(path):
        try:
            rel = os.path.relpath(path, _cwd_dir)
            if not rel.startswith(".."):
                return rel
        except ValueError:
            pass
    # Replace home directory with ~
    if path.startswith(_home_dir):
        return "~" + path[len(_home_dir):]
    # Replace other absolute paths with basename
    if os.path.isabs(path):
        return os.path.basename(path)
    return path


def redact_arg(arg: str) -> str:
    """Redact path-like CLI argument values for trace output."""
    if not _redact_paths:
        return arg
    if os.path.isabs(arg):
        return redact(arg)
    if "=" in arg:
        key, value = arg.split("=", 1)
        if os.path.isabs(value):
            return f"{key}={redact(value)}"
    return arg


def parse_source_location(identifier: str) -> Optional[SourceLocation]:
    """Parse <file:line> identifier, or return None for non-source identifiers."""
    if ":" not in identifier:
        return None
    file_part, line_part = identifier.rsplit(":", 1)
    if not file_part or not line_part.isdigit():
        return None
    return SourceLocation(file=file_part, line=int(line_part))


# ── Tool Execution ───────────────────────────────────────────────────────────

def run_tool(
    args: list[str],
    timeout: int = DEFAULT_TIMEOUT_SEC,
    capture_stderr: bool = True,
    check: bool = True,
) -> subprocess.CompletedProcess:
    """Run an external tool with timeout and verbose logging."""
    cmd_str = " ".join(redact_arg(a) for a in args)
    verbose(f"exec: {cmd_str}")
    t0 = time.monotonic()
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError:
        error(E_TOOL_MISSING, f"tool not found: {args[0]}", EXIT_TOOL)
    except subprocess.TimeoutExpired:
        error(E_TOOL_FAILED, f"tool timed out after {timeout}s: {args[0]}", EXIT_TOOL)
    dt = time.monotonic() - t0
    verbose(f"  -> {result.returncode} in {dt:.2f}s ({len(result.stdout)} bytes stdout)")
    if check and result.returncode != 0:
        stderr_preview = result.stderr[:500] if result.stderr else "(no stderr)"
        error(E_TOOL_FAILED, f"{args[0]} exited {result.returncode}: {stderr_preview}", EXIT_TOOL)
    return result


# ── Phase: Inspect Binary ────────────────────────────────────────────────────

def inspect_binary(path: str, timeout: int) -> BinaryInfo:
    """Extract binary metadata using llvm-readobj."""
    if not os.path.isfile(path):
        error(E_USAGE_BAD_ARGS, f"binary not found: {redact(path)}", EXIT_USAGE)

    info = BinaryInfo(
        path=path,
        file_size=os.path.getsize(path),
        mtime=os.path.getmtime(path),
    )

    result = run_tool(["llvm-readobj", "--file-headers", path], timeout=timeout)
    output = result.stdout

    # Format
    if "Format: ELF" in output or "Format: elf" in output:
        info.format = "ELF"
    elif "Format: Mach-O" in output or "Format: MachO" in output:
        info.format = "Mach-O"
    elif "Format: COFF" in output or "Format: PE" in output:
        info.format = "COFF"
    else:
        fmt_m = re.search(r"Format:\s*(\S+)", output)
        if fmt_m:
            raw = fmt_m.group(1)
            if "elf" in raw.lower():
                info.format = "ELF"
            elif "mach" in raw.lower():
                info.format = "Mach-O"
            elif "coff" in raw.lower() or "pe" in raw.lower():
                info.format = "COFF"
            else:
                info.format = raw

    # Architecture
    arch_m = re.search(r"Arch:\s*(\S+)", output) or re.search(r"Machine:\s*(\S+)", output)
    if arch_m:
        raw_arch = arch_m.group(1).lower()
        if "x86_64" in raw_arch or "x86-64" in raw_arch or raw_arch == "em_x86_64":
            info.arch = "x86_64"
        elif "aarch64" in raw_arch or "arm64" in raw_arch:
            info.arch = "aarch64"
        elif "x86" in raw_arch or "i386" in raw_arch:
            info.arch = "x86"
        elif "arm" in raw_arch:
            info.arch = "arm"
        else:
            info.arch = raw_arch

    # Also try Machine field for ELF
    if not info.arch:
        machine_m = re.search(r"Machine:\s*(.+)", output)
        if machine_m:
            raw = machine_m.group(1).strip()
            if "X86-64" in raw or "Advanced Micro" in raw:
                info.arch = "x86_64"
            elif "AArch64" in raw:
                info.arch = "aarch64"

    # Endianness
    if "LittleEndian" in output or "little endian" in output.lower() or "IsLittleEndian: Yes" in output:
        info.endianness = "little"
    elif "BigEndian" in output or "big endian" in output.lower():
        info.endianness = "big"

    # Build ID — try readelf for ELF (faster), fall back to llvm-readobj
    if info.format == "ELF":
        try:
            r2 = run_tool(
                ["readelf", "-n", path],
                timeout=timeout,
                check=False,
            )
            bid_m = re.search(r"Build ID:\s*([0-9a-fA-F]+)", r2.stdout)
            if bid_m:
                info.build_id = bid_m.group(1)
        except SystemExit:
            pass  # readelf not available, try llvm-readobj

    if not info.build_id:
        bid_m = re.search(r"BuildID[^:]*:\s*\(?([0-9a-fA-F]+)", output)
        if bid_m:
            info.build_id = bid_m.group(1)

    if not info.format:
        error(E_FORMAT_UNSUPPORTED, f"could not determine binary format for: {path}", EXIT_FORMAT)

    verbose(f"binary: format={info.format} arch={info.arch} endian={info.endianness} "
            f"build_id={info.build_id[:16]}... size={info.file_size}")
    return info


# ── Phase: Probe Tools ───────────────────────────────────────────────────────

def probe_tools(needs_mca: bool = False, needs_dwarfdump: bool = False) -> dict[str, ToolInfo]:
    """Check availability and versions of required tools."""
    tools: dict[str, ToolInfo] = {}

    required = ["llvm-objdump", "c++filt"]
    optional = ["llvm-nm", "readelf", "llvm-dwarfdump"]
    if needs_mca:
        required.append("llvm-mca")
    if needs_dwarfdump:
        required.append("llvm-dwarfdump")

    for name in set(required + optional):
        ti = ToolInfo(name=name)
        ti.path = shutil.which(name) or ""
        ti.available = bool(ti.path)
        if ti.available:
            try:
                r = subprocess.run(
                    [name, "--version"],
                    capture_output=True, text=True, timeout=5,
                )
                ver_m = re.search(r"version\s+([\d.]+)", r.stdout + r.stderr)
                if ver_m:
                    ti.version = ver_m.group(1)
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass
        tools[name] = ti
        verbose(
            f"tool {name}: available={ti.available} version={ti.version} "
            f"path={redact(ti.path) if ti.path else ''}"
        )

    # Check required tools
    for name in required:
        if not tools[name].available:
            error(E_TOOL_MISSING, f"required tool not found: {name}", EXIT_TOOL)

    # Check minimum LLVM version for symbolize-operands
    objdump_ver = tools["llvm-objdump"].version
    if objdump_ver:
        major = int(objdump_ver.split(".")[0])
        if major < MIN_LLVM_VERSION:
            warn(f"llvm-objdump {objdump_ver} < {MIN_LLVM_VERSION}; "
                 f"--symbolize-operands not available, CFG metrics disabled")

    return tools


def get_llvm_major(tools: dict[str, ToolInfo]) -> int:
    ver = tools.get("llvm-objdump", ToolInfo(name="")).version
    if ver:
        return int(ver.split(".")[0])
    return 0


# ── Phase: Symbol Resolution ────────────────────────────────────────────────

def _cache_dir() -> Path:
    xdg = os.environ.get("XDG_CACHE_HOME", "")
    if xdg:
        base = Path(xdg)
    else:
        base = Path.home() / ".cache"
    d = base / "analyze-assembly"
    try:
        d.mkdir(parents=True, exist_ok=True)
    except OSError:
        fallback = Path("tmp") / "analyze-assembly-cache"
        warn(f"cache dir not writable: {redact(str(d))}; using {redact(str(fallback))}")
        fallback.mkdir(parents=True, exist_ok=True)
        d = fallback
    return d


def _cache_key(binary: BinaryInfo) -> str:
    """Build a cache key from build-id + mtime + size."""
    if binary.build_id:
        return binary.build_id
    # Fallback: hash of path + mtime + size
    h = hashlib.sha256()
    h.update(binary.path.encode())
    h.update(str(binary.mtime).encode())
    h.update(str(binary.file_size).encode())
    return h.hexdigest()[:32]


def _build_symbol_cache(
    binary: BinaryInfo,
    tools: dict[str, ToolInfo],
    timeout: int,
    rebuild_cache: bool,
    stats: RunStats,
) -> tuple[Path, Path]:
    """Build or reuse symbol cache files. Returns (raw_path, demangled_path)."""
    key = _cache_key(binary)
    cdir = _cache_dir()
    raw_path = cdir / f"{key}.symbols.raw"
    dem_path = cdir / f"{key}.symbols.demangled"
    lock_path = cdir / f"{key}.lock"

    if not rebuild_cache and raw_path.exists() and dem_path.exists():
        # Validate: check mtime/size fingerprint stored in a .meta file
        meta_path = cdir / f"{key}.meta"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                if (meta.get("mtime") == binary.mtime
                        and meta.get("file_size") == binary.file_size):
                    verbose(f"cache hit: {raw_path}")
                    stats.cache["status"] = "hit"
                    return raw_path, dem_path
            except (json.JSONDecodeError, KeyError):
                pass
        else:
            # Old cache without meta, rebuild
            pass
        verbose("cache key match but meta mismatch, rebuilding")

    stats.cache["status"] = "miss" if not rebuild_cache else "rebuild"

    # Build under lock
    verbose("building symbol cache...")
    t0 = time.monotonic()

    lock_fd = open(lock_path, "w")
    try:
        lock_wait_start = time.monotonic()
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        stats.cache["lock_wait_ms"] = round((time.monotonic() - lock_wait_start) * 1000, 1)

        # Double-check after acquiring lock
        if not rebuild_cache and raw_path.exists() and dem_path.exists():
            meta_path = cdir / f"{key}.meta"
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text())
                    if (meta.get("mtime") == binary.mtime
                            and meta.get("file_size") == binary.file_size):
                        verbose("cache built by another process while waiting")
                        stats.cache["status"] = "hit_after_wait"
                        return raw_path, dem_path
                except (json.JSONDecodeError, KeyError):
                    pass

        raw_lines = _extract_raw_symbol_lines(binary, tools, timeout)

        # Write raw cache atomically
        tmp_raw = raw_path.with_suffix(".tmp")
        tmp_raw.write_text("\n".join(raw_lines) + "\n")
        tmp_raw.rename(raw_path)

        # Demangle via c++filt
        dem_lines = _demangle_symbol_lines(raw_lines, timeout)

        tmp_dem = dem_path.with_suffix(".tmp")
        tmp_dem.write_text("\n".join(dem_lines) + "\n")
        tmp_dem.rename(dem_path)

        # Write meta
        meta_path = cdir / f"{key}.meta"
        tmp_meta = meta_path.with_suffix(".tmp")
        tmp_meta.write_text(json.dumps({
            "mtime": binary.mtime,
            "file_size": binary.file_size,
            "build_id": binary.build_id,
            "binary_path": binary.path,
        }))
        tmp_meta.rename(meta_path)

        dt = time.monotonic() - t0
        n_syms = len(raw_lines)
        verbose(f"cache built: {n_syms} symbols in {dt:.2f}s")
        stats.cache["symbols_cached"] = n_syms
        stats.cache["build_time_ms"] = round(dt * 1000, 1)

    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()
        try:
            lock_path.unlink()
        except OSError:
            pass

    return raw_path, dem_path


def _extract_raw_symbol_lines(
    binary: BinaryInfo,
    tools: dict[str, ToolInfo],
    timeout: int,
) -> list[str]:
    """Extract symbol lines in nm-like format: address size type symbol."""
    # Prefer readelf for ELF (much faster), fall back to llvm-nm.
    if binary.format == "ELF" and tools.get("readelf", ToolInfo(name="")).available:
        verbose("using readelf -sW for symbol extraction (faster for ELF)")
        result = run_tool(
            ["readelf", "-sW", binary.path],
            timeout=timeout,
            check=False,
        )
        return _parse_readelf_symbols(result.stdout)
    verbose("using llvm-nm for symbol extraction")
    result = run_tool(
        ["llvm-nm", "--defined-only", "--print-size", binary.path],
        timeout=timeout,
        check=False,
    )
    return result.stdout.splitlines()


def _demangle_symbol_lines(raw_lines: list[str], timeout: int) -> list[str]:
    """Demangle symbol lines with c++filt while preserving line order."""
    verbose("demangling symbols via c++filt...")
    demangle_input = "\n".join(raw_lines) + "\n"
    try:
        dem_result = subprocess.run(
            ["c++filt"],
            input=demangle_input,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if dem_result.returncode == 0 and dem_result.stdout:
            return dem_result.stdout.splitlines()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    warn("c++filt failed, using raw symbols")
    return raw_lines[:]


def _load_symbol_lines(
    binary: BinaryInfo,
    tools: dict[str, ToolInfo],
    timeout: int,
    no_cache: bool,
    rebuild_cache: bool,
    stats: RunStats,
) -> tuple[list[str], list[str]]:
    """Load raw/demangled symbol lines from cache or directly from tools."""
    if no_cache:
        raw_lines = _extract_raw_symbol_lines(binary, tools, timeout)
        dem_lines = _demangle_symbol_lines(raw_lines, timeout)
        stats.cache["status"] = "disabled"
        stats.cache["symbols_cached"] = len(raw_lines)
        return raw_lines, dem_lines

    raw_path, dem_path = _build_symbol_cache(
        binary, tools, timeout, rebuild_cache, stats,
    )
    return raw_path.read_text().splitlines(), dem_path.read_text().splitlines()


def _parse_symbol_candidates(raw_lines: list[str], dem_lines: list[str]) -> list[SymbolCandidate]:
    if len(raw_lines) != len(dem_lines):
        warn(f"cache line count mismatch: raw={len(raw_lines)} dem={len(dem_lines)}")
        n = min(len(raw_lines), len(dem_lines))
        raw_lines = raw_lines[:n]
        dem_lines = dem_lines[:n]

    candidates: list[SymbolCandidate] = []
    for raw, dem in zip(raw_lines, dem_lines):
        raw_parts = raw.split(None, 3)
        dem_parts = dem.split(None, 3)
        if len(raw_parts) < 4:
            continue
        try:
            addr = int(raw_parts[0], 16)
            size = int(raw_parts[1], 16)
        except ValueError:
            continue

        mangled = raw_parts[3]
        demangled = dem_parts[3] if len(dem_parts) >= 4 else mangled
        candidates.append(SymbolCandidate(
            mangled=mangled,
            demangled=demangled,
            address=addr,
            size=size,
            sym_type=raw_parts[2],
        ))
    candidates.sort(key=lambda c: c.address)
    return candidates


def _parse_readelf_symbols(output: str) -> list[str]:
    """Parse readelf -sW output into llvm-nm-like format: addr size type name."""
    lines = []
    for line in output.splitlines():
        # readelf -sW format:
        #   Num: Value          Size Type    Bind   Vis      Ndx Name
        #     1: 0000000000000000     0 NOTYPE  LOCAL  DEFAULT  UND
        parts = line.split()
        if len(parts) < 8:
            continue
        try:
            addr = int(parts[1].rstrip(":"), 16)
            size = int(parts[2])
        except (ValueError, IndexError):
            continue
        if addr == 0 or size == 0:
            continue
        stype = parts[3]  # FUNC, OBJECT, etc.
        bind = parts[4]   # LOCAL, GLOBAL, WEAK
        ndx = parts[6]
        name = parts[7] if len(parts) >= 8 else ""
        if ndx == "UND" or not name:
            continue
        # Convert to nm-like type letter
        if stype == "FUNC":
            t = "T" if bind == "GLOBAL" else "t"
        elif stype == "OBJECT":
            t = "D" if bind == "GLOBAL" else "d"
        elif bind == "WEAK":
            t = "W"
        else:
            t = "t"
        # Format: address size type name (same as llvm-nm --print-size)
        lines.append(f"{addr:016x} {size:016x} {t} {name}")
    return lines


def resolve_symbols(
    binary: BinaryInfo,
    identifier: str,
    tools: dict[str, ToolInfo],
    timeout: int,
    search_mode: bool,
    fuzzy_mode: bool,
    no_cache: bool,
    rebuild_cache: bool,
    stats: RunStats,
) -> list[SymbolCandidate]:
    """Resolve identifier to symbol candidates."""
    raw_lines, dem_lines = _load_symbol_lines(
        binary, tools, timeout, no_cache, rebuild_cache, stats,
    )
    all_candidates = _parse_symbol_candidates(raw_lines, dem_lines)

    candidates: list[SymbolCandidate] = []
    for candidate in all_candidates:
        mangled = candidate.mangled
        demangled = candidate.demangled
        # Match based on mode
        match = False
        if search_mode:
            try:
                if re.search(identifier, demangled):
                    match = True
            except re.error:
                error(E_USAGE_BAD_ARGS, f"invalid regex: {identifier}", EXIT_USAGE)
        elif fuzzy_mode:
            if identifier.lower() in demangled.lower():
                match = True
        else:
            # Exact match: full demangled name, or suffix match, or mangled match
            if (demangled == identifier
                    or mangled == identifier
                    or demangled.endswith("::" + identifier)
                    or demangled.split("(")[0] == identifier
                    or demangled.split("(")[0].endswith("::" + identifier)):
                match = True

        if match:
            candidates.append(candidate)

    # Sort by address for deterministic ordering
    candidates.sort(key=lambda c: c.address)
    stats.input_stats["symbol_candidates"] = len(candidates)
    verbose(f"found {len(candidates)} candidate(s) for '{identifier}'")
    return candidates


def _source_file_matches(query_file: str, candidate_file: str) -> bool:
    q = os.path.normpath(query_file).replace("\\", "/")
    c = os.path.normpath(candidate_file).replace("\\", "/")
    if q.startswith("./"):
        q = q[2:]
    if c.startswith("./"):
        c = c[2:]
    if q == c:
        return True
    if os.path.basename(q) == os.path.basename(c):
        return True
    return c.endswith("/" + q) or q.endswith("/" + c)


def _parse_debug_line_addresses(output: str, source: SourceLocation) -> tuple[bool, int, list[int]]:
    """Parse `llvm-dwarfdump --debug-line` output and return matching addresses."""
    has_debug_line = "debug_line contents:" in output
    include_dirs: dict[int, str] = {0: ""}
    files: dict[int, str] = {}
    current_file_idx: int | None = None
    current_file_name = ""
    addresses: set[int] = set()
    total_rows = 0

    for line in output.splitlines():
        s = line.strip()
        if s.startswith("debug_line["):
            include_dirs = {0: ""}
            files = {}
            current_file_idx = None
            current_file_name = ""
            continue

        m = re.match(r'^include_directories\[\s*(\d+)\]\s*=\s*"([^"]*)"$', s)
        if m:
            include_dirs[int(m.group(1))] = m.group(2)
            continue

        m = re.match(r"^file_names\[\s*(\d+)\]:$", s)
        if m:
            current_file_idx = int(m.group(1))
            current_file_name = ""
            continue

        if current_file_idx is not None:
            m_name = re.match(r'^name:\s*"([^"]+)"$', s)
            if m_name:
                current_file_name = m_name.group(1)
                continue
            m_dir = re.match(r"^dir_index:\s*(\d+)$", s)
            if m_dir:
                dir_index = int(m_dir.group(1))
                directory = include_dirs.get(dir_index, "")
                full_path = os.path.normpath(os.path.join(directory, current_file_name))
                files[current_file_idx] = full_path
                current_file_idx = None
                current_file_name = ""
                continue

        row_m = re.match(r"^\s*0x([0-9a-fA-F]+)\s+(\d+)\s+\d+\s+(\d+)\b", line)
        if not row_m:
            continue

        address = int(row_m.group(1), 16)
        line_no = int(row_m.group(2))
        file_idx = int(row_m.group(3))
        total_rows += 1
        file_path = files.get(file_idx, "")
        if line_no == source.line and _source_file_matches(source.file, file_path):
            addresses.add(address)

    return has_debug_line, total_rows, sorted(addresses)


def resolve_source_location(
    binary: BinaryInfo,
    source: SourceLocation,
    tools: dict[str, ToolInfo],
    timeout: int,
    no_cache: bool,
    rebuild_cache: bool,
    stats: RunStats,
) -> list[SymbolCandidate]:
    """Resolve `<file:line>` to symbols that cover matching machine-code addresses."""
    dd = run_tool(
        ["llvm-dwarfdump", "--debug-line", binary.path],
        timeout=timeout,
        check=False,
    )

    has_debug_line, debug_rows, addresses = _parse_debug_line_addresses(dd.stdout, source)
    source_ref = f"{source.file}:{source.line}"
    if not has_debug_line or debug_rows == 0:
        error(
            E_DEBUG_MISSING,
            f"debug line info missing for `{source_ref}`; try symbol lookup instead",
            EXIT_DEBUG,
        )
    if not addresses:
        error(
            E_LOOKUP_NOT_FOUND,
            f"no debug line entries match `{source_ref}`",
            EXIT_LOOKUP,
        )

    raw_lines, dem_lines = _load_symbol_lines(
        binary, tools, timeout, no_cache, rebuild_cache, stats,
    )
    all_candidates = _parse_symbol_candidates(raw_lines, dem_lines)
    matches: list[SymbolCandidate] = []
    seen: set[tuple[str, int]] = set()

    for candidate in all_candidates:
        start = candidate.address
        end = start + candidate.size
        if candidate.size <= 0:
            continue
        for address in addresses:
            if start <= address < end:
                key = (candidate.mangled, candidate.address)
                if key not in seen:
                    matches.append(candidate)
                    seen.add(key)
                break

    matches.sort(key=lambda c: c.address)
    stats.input_stats["source_addresses"] = len(addresses)
    stats.input_stats["symbol_candidates"] = len(matches)
    verbose(f"source lookup `{source_ref}` produced {len(matches)} candidate(s)")
    return matches


def resolve_identifier_candidates(
    binary: BinaryInfo,
    identifier: str,
    source_location: SourceLocation | None,
    tools: dict[str, ToolInfo],
    timeout: int,
    search_mode: bool,
    fuzzy_mode: bool,
    no_cache: bool,
    rebuild_cache: bool,
    stats: RunStats,
) -> list[SymbolCandidate]:
    if source_location and not search_mode and not fuzzy_mode:
        return resolve_source_location(
            binary,
            source_location,
            tools,
            timeout,
            no_cache,
            rebuild_cache,
            stats,
        )
    return resolve_symbols(
        binary,
        identifier,
        tools,
        timeout,
        search_mode,
        fuzzy_mode,
        no_cache,
        rebuild_cache,
        stats,
    )


# ── Phase: Disassemble ───────────────────────────────────────────────────────

def disassemble(
    binary: BinaryInfo,
    symbol: SymbolCandidate,
    tools: dict[str, ToolInfo],
    timeout: int,
    source_mode: bool,
    max_instructions: int,
    symbolize_operands: bool,
) -> tuple[str, list[Instruction]]:
    """Disassemble a symbol and return raw text + parsed instructions."""
    llvm_major = get_llvm_major(tools)
    use_range_mode = not source_mode and symbol.size > 0

    args = ["llvm-objdump", "-d", "--no-leading-addr" if False else ""]
    args = ["llvm-objdump", "-d"]

    if binary.arch == "x86_64" or binary.arch == "x86":
        args.append("--x86-asm-syntax=intel")

    args.append("--no-show-raw-insn")

    if symbolize_operands and llvm_major >= MIN_LLVM_VERSION:
        args.append("--symbolize-operands")
        verbose("using --symbolize-operands for CFG construction")

    if use_range_mode:
        start = symbol.address
        stop = symbol.address + symbol.size
        args.extend([
            f"--start-address=0x{start:x}",
            f"--stop-address=0x{stop:x}",
        ])
        verbose(f"range mode: 0x{start:x}..0x{stop:x} ({symbol.size} bytes)")
    else:
        args.extend(["-C", f"--disassemble-symbols={symbol.mangled}"])
        if source_mode:
            args.append("-S")
        verbose(f"symbol mode: {symbol.mangled}")

    args.append(binary.path)

    result = run_tool(args, timeout=timeout, check=False)
    raw_text = result.stdout

    # Parse instructions
    instructions = parse_instructions(raw_text, binary.arch, max_instructions)

    # Fallback: if symbol mode produced no instructions (objdump can't find the
    # symbol even though readelf/nm can), retry with range mode
    if not instructions and not use_range_mode and symbol.size > 0:
        verbose("symbol mode produced no instructions, falling back to range mode")
        args2 = ["llvm-objdump", "-d"]
        if binary.arch in ("x86_64", "x86"):
            args2.append("--x86-asm-syntax=intel")
        args2.append("--no-show-raw-insn")
        if symbolize_operands and llvm_major >= MIN_LLVM_VERSION:
            args2.append("--symbolize-operands")
        start = symbol.address
        stop = symbol.address + symbol.size
        args2.extend([
            f"--start-address=0x{start:x}",
            f"--stop-address=0x{stop:x}",
        ])
        if source_mode:
            args2.append("-S")
        args2.append(binary.path)
        result2 = run_tool(args2, timeout=timeout, check=False)
        raw_text = result2.stdout
        instructions = parse_instructions(raw_text, binary.arch, max_instructions)

    return raw_text, instructions


def parse_instructions(
    text: str,
    arch: str,
    max_instructions: int = 0,
) -> list[Instruction]:
    """Parse llvm-objdump output into Instruction objects."""
    instructions: list[Instruction] = []
    current_label = ""
    in_disassembly = False

    # Pattern for instruction line:
    #   <addr>: <mnemonic> <operands>
    # With --no-show-raw-insn, no hex bytes between addr and mnemonic
    insn_re = re.compile(r"^\s*([0-9a-fA-F]+):\s+(\S+)(?:\s+(.*))?$")
    # Label pattern: <symbol+0xNN> or <LN>:
    label_re = re.compile(r"^([0-9a-fA-F]+)\s+<(.+)>:$")
    # Symbolized label: <LN>:
    sym_label_re = re.compile(r"^<(L\d+)>:$")

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue

        # Check for label
        lm = label_re.match(stripped)
        if lm:
            in_disassembly = True
            current_label = lm.group(2)
            continue

        # Check for symbolized label
        slm = sym_label_re.match(stripped)
        if slm:
            current_label = slm.group(1)
            continue

        if not in_disassembly:
            continue

        # Check for instruction
        im = insn_re.match(stripped)
        if not im:
            continue

        addr_str, mnemonic, operands = im.group(1), im.group(2), im.group(3) or ""
        try:
            addr = int(addr_str, 16)
        except ValueError:
            continue

        insn = Instruction(
            address=addr,
            raw_bytes="",
            mnemonic=mnemonic,
            operands=operands.strip(),
            raw_line=stripped,
            label=current_label,
        )
        current_label = ""

        # Classify instruction
        classify_instruction(insn, arch)
        instructions.append(insn)

        if max_instructions and len(instructions) >= max_instructions:
            break

    return instructions


def classify_instruction(insn: Instruction, arch: str) -> None:
    """Classify an instruction by type for the given architecture."""
    mn = insn.mnemonic.lower()
    ops = insn.operands.lower()

    if arch in ("x86_64", "x86"):
        _classify_x86(insn, mn, ops)
    elif arch == "aarch64":
        _classify_aarch64(insn, mn, ops)


def _classify_x86(insn: Instruction, mn: str, ops: str) -> None:
    """Classify x86/x86_64 instructions."""
    # Returns
    if mn in ("ret", "retq", "retl"):
        insn.is_ret = True
        return

    # Nops
    if mn in ("nop", "nopl", "nopw", "data16"):
        insn.is_nop = True
        return

    # Calls
    if mn.startswith("call"):
        insn.is_call = True
        return

    # Branches (conditional and unconditional)
    branch_mnemonics = {
        "jmp", "jmpq", "je", "jne", "jz", "jnz", "jg", "jge", "jl", "jle",
        "ja", "jae", "jb", "jbe", "jo", "jno", "js", "jns", "jp", "jnp",
        "jcxz", "jecxz", "jrcxz",
    }
    if mn in branch_mnemonics:
        insn.is_branch = True
        return

    # Spill detection: Intel syntax — mov to/from [rsp+...] or [rbp+...]
    # Store (spill): mov [rsp+N], reg  or  mov qword ptr [rsp+N], reg
    # Load (reload): mov reg, [rsp+N]
    if mn in ("mov", "movq", "movl", "movw", "movb", "movaps", "movups",
              "movapd", "movupd", "movdqa", "movdqu", "movsd", "movss",
              "vmovaps", "vmovups", "vmovapd", "vmovupd", "vmovdqa",
              "vmovdqu", "vmovsd", "vmovss"):
        # In Intel syntax: dst, src
        # [rsp+...] or [rbp-...] in dst = store (spill)
        # [rsp+...] or [rbp-...] in src = load (reload)
        parts = ops.split(",", 1)
        if len(parts) == 2:
            dst, src = parts[0].strip(), parts[1].strip()
            stack_pattern = re.compile(r"\[(?:rsp|rbp|esp|ebp)[^\]]*\]")
            if stack_pattern.search(dst):
                insn.is_spill_store = True
            elif stack_pattern.search(src):
                insn.is_spill_load = True


def _classify_aarch64(insn: Instruction, mn: str, ops: str) -> None:
    """Classify AArch64 instructions."""
    # Returns
    if mn == "ret":
        insn.is_ret = True
        return

    # Nops
    if mn == "nop":
        insn.is_nop = True
        return

    # Calls
    if mn in ("bl", "blr"):
        insn.is_call = True
        return

    # Branches
    if mn in ("b", "br", "cbz", "cbnz", "tbz", "tbnz"):
        insn.is_branch = True
        return
    if mn.startswith("b."):
        insn.is_branch = True
        return

    # Spill detection: str/stp to [sp,...] = store, ldr/ldp from [sp,...] = load
    if mn in ("str", "stp", "stur"):
        if "[sp" in ops:
            insn.is_spill_store = True
        return
    if mn in ("ldr", "ldp", "ldur"):
        if "[sp" in ops:
            insn.is_spill_load = True
        return


# ── CFG Construction ─────────────────────────────────────────────────────────

def build_cfg(instructions: list[Instruction]) -> list[BasicBlock]:
    """Build basic blocks and detect loop back-edges."""
    if not instructions:
        return []

    # Collect block boundaries: start of function + targets of branches + after branches
    block_starts: set[int] = {0}  # index into instructions list
    label_to_idx: dict[str, int] = {}

    for i, insn in enumerate(instructions):
        if insn.label:
            label_to_idx[insn.label] = i
            block_starts.add(i)
        if insn.is_branch or insn.is_ret:
            if i + 1 < len(instructions):
                block_starts.add(i + 1)

    sorted_starts = sorted(block_starts)
    blocks: list[BasicBlock] = []

    for bi, start in enumerate(sorted_starts):
        end = sorted_starts[bi + 1] if bi + 1 < len(sorted_starts) else len(instructions)
        block_insns = instructions[start:end]
        if not block_insns:
            continue

        label = block_insns[0].label or f"B{bi}"
        bb = BasicBlock(
            label=label,
            start_idx=start,
            end_idx=end,
            instructions=block_insns,
        )

        # Determine successors from last instruction
        last = block_insns[-1]
        if last.is_branch:
            # Try to find target label in operands (symbolized: <LN>)
            target_m = re.search(r"<(L\d+|[^>]+)>", last.operands)
            if target_m:
                bb.successors.append(target_m.group(1))
            # Conditional branches also fall through
            if last.mnemonic.lower() not in ("jmp", "jmpq", "b", "br"):
                if end < len(instructions):
                    next_label = instructions[end].label or f"B{bi+1}"
                    bb.successors.append(next_label)
        elif not last.is_ret:
            # Fall-through
            if end < len(instructions):
                next_label = instructions[end].label or f"B{bi+1}"
                bb.successors.append(next_label)

        # Detect landing pad (exception handling)
        for insn in block_insns:
            if insn.is_call and any(x in insn.operands for x in
                                     ("__cxa_begin_catch", "__cxa_throw",
                                      "__cxa_rethrow", "_Unwind_Resume",
                                      "__gxx_personality")):
                bb.is_landing_pad = True
                break

        blocks.append(bb)

    # Detect back-edges (loop headers) using a simple DFS
    block_map: dict[str, BasicBlock] = {b.label: b for b in blocks}
    visited: set[str] = set()
    on_stack: set[str] = set()

    def dfs(label: str) -> None:
        if label not in block_map:
            return
        visited.add(label)
        on_stack.add(label)
        for succ in block_map[label].successors:
            if succ in on_stack:
                # Back-edge: succ is a loop header
                if succ in block_map:
                    block_map[succ].is_loop_header = True
            elif succ not in visited:
                dfs(succ)
        on_stack.discard(label)

    if blocks:
        dfs(blocks[0].label)

    return blocks


# ── Phase: llvm-mca ──────────────────────────────────────────────────────────

def _extract_loop_asm(
    blocks: list[BasicBlock],
    instructions: list[Instruction],
    arch: str,
) -> list[tuple[str, str]]:
    """Extract assembly text for each detected loop body.

    Returns list of (label, asm_text) where asm_text is suitable for llvm-mca.
    Only includes loop header blocks and their fall-through successors up to
    the back-edge.
    """
    block_map: dict[str, BasicBlock] = {b.label: b for b in blocks}
    loops: list[tuple[str, str]] = []

    for bb in blocks:
        if not bb.is_loop_header:
            continue

        # Collect blocks in this loop: walk from the header following
        # fall-through and forward edges until we hit the back-edge target
        # or leave the loop. Simple heuristic: collect contiguous blocks
        # starting from the header until we find one that branches back.
        loop_insns: list[Instruction] = []
        visited: set[str] = set()
        queue = [bb.label]

        while queue:
            lbl = queue.pop(0)
            if lbl in visited or lbl not in block_map:
                continue
            visited.add(lbl)
            cur = block_map[lbl]
            loop_insns.extend(cur.instructions)

            for succ in cur.successors:
                if succ == bb.label:
                    # Back-edge, this closes the loop
                    continue
                if succ in block_map:
                    succ_bb = block_map[succ]
                    # Only follow forward edges (higher start_idx)
                    if succ_bb.start_idx > bb.start_idx:
                        queue.append(succ)

        if not loop_insns:
            continue

        # Build asm text: one instruction per line, mnemonic + operands
        # Skip calls and branches — MCA can't simulate control flow or
        # resolve symbolic targets. Also skip nops.
        asm_lines: list[str] = []
        for insn in loop_insns:
            if insn.is_nop or insn.is_call or insn.is_branch or insn.is_ret:
                continue
            # Clean operands: remove <LN> references and symbolic names
            ops = re.sub(r"\s*<[^>]+>", "", insn.operands).strip()
            if ops:
                asm_lines.append(f"{insn.mnemonic} {ops}")
            else:
                asm_lines.append(insn.mnemonic)

        # Only include if we have at least 2 computational instructions
        if len(asm_lines) >= 2:
            loops.append((bb.label, "\n".join(asm_lines)))

    return loops


def run_mca(
    loops: list[tuple[str, str]],
    arch: str,
    mcpu: str,
    timeout: int,
) -> list[McaResult]:
    """Run llvm-mca on extracted loop bodies."""
    results: list[McaResult] = []

    # Map our arch names to llvm-mca triple
    triple_map = {
        "x86_64": "x86_64-unknown-linux-gnu",
        "x86": "i386-unknown-linux-gnu",
        "aarch64": "aarch64-unknown-linux-gnu",
    }
    triple = triple_map.get(arch, f"{arch}-unknown-linux-gnu")

    for label, asm_text in loops:
        verbose(f"running llvm-mca on loop {label} ({len(asm_text.splitlines())} insns)")

        # Write asm to temp file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".s", delete=False, prefix="mca_"
        ) as f:
            f.write(asm_text)
            f.write("\n")
            tmp_path = f.name

        try:
            cmd = [
                "llvm-mca",
                f"--mtriple={triple}",
                f"--mcpu={mcpu}",
                "--iterations=100",
                "--timeline",
            ]
            if arch in ("x86_64", "x86"):
                cmd.append("--x86-asm-syntax=intel")
            cmd.append(tmp_path)
            result = run_tool(cmd, timeout=timeout, check=False)
            mca_out = result.stdout + result.stderr

            mr = McaResult(
                loop_label=label,
                raw_output=mca_out,
            )

            # Parse summary line: "Iterations: 100"
            m = re.search(r"Iterations:\s*(\d+)", mca_out)
            if m:
                mr.iterations = int(m.group(1))

            # Parse "Total Cycles: 450"
            m = re.search(r"Total Cycles:\s*(\d+)", mca_out)
            if m:
                mr.total_cycles = float(m.group(1))

            # Parse "Block RThroughput: 4.5"
            m = re.search(r"Block RThroughput:\s*([\d.]+)", mca_out)
            if m:
                mr.block_rthroughput = float(m.group(1))

            # Parse IPC directly from output (llvm-mca prints "IPC: X.XX")
            m = re.search(r"^IPC:\s*([\d.]+)", mca_out, re.MULTILINE)
            if m:
                mr.throughput_ipc = float(m.group(1))
            else:
                # Fallback: compute from total instructions and cycles
                m_insn = re.search(r"^Instructions:\s*(\d+)", mca_out, re.MULTILINE)
                if m_insn and mr.total_cycles > 0:
                    total_insns = int(m_insn.group(1))
                    mr.throughput_ipc = round(total_insns / mr.total_cycles, 2)

            # Parse resource pressure or bottleneck hints
            # Look for lines like "Resource pressure per iteration:"
            pressure_lines = []
            in_pressure = False
            for line in mca_out.splitlines():
                if "Resource pressure" in line:
                    in_pressure = True
                    continue
                if in_pressure:
                    if line.strip() and not line.startswith(" "):
                        in_pressure = False
                    elif line.strip():
                        pressure_lines.append(line.strip())
            mr.pressure_summary = pressure_lines[:5]

            # Try to identify bottleneck from timeline
            m = re.search(r"Bottleneck:\s*(.+)", mca_out)
            if m:
                mr.bottleneck = m.group(1).strip()

            if result.returncode != 0:
                verbose(f"llvm-mca returned {result.returncode} for loop {label}")
                # Still include partial results
                if not mr.raw_output.strip():
                    continue

            results.append(mr)
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    return results


def mca_findings(mca_results: list[McaResult]) -> list[Finding]:
    """Generate findings from MCA analysis."""
    findings: list[Finding] = []
    for mr in mca_results:
        if mr.block_rthroughput <= 0:
            continue

        evidence = []
        if mr.throughput_ipc > 0:
            evidence.append(f"IPC: {mr.throughput_ipc}")
        evidence.append(f"Block reciprocal throughput: {mr.block_rthroughput:.1f} cycles")
        if mr.bottleneck:
            evidence.append(f"Bottleneck: {mr.bottleneck}")
        for p in mr.pressure_summary[:3]:
            evidence.append(f"  {p}")

        severity = "low"
        if mr.block_rthroughput > 10:
            severity = "high"
        elif mr.block_rthroughput > 5:
            severity = "medium"

        findings.append(Finding(
            id=f"mca-throughput-{mr.loop_label}",
            severity=severity,
            confidence="medium",
            summary=(f"Loop {mr.loop_label}: throughput {mr.block_rthroughput:.1f} "
                     f"cycles/iter, IPC {mr.throughput_ipc}"),
            evidence=evidence,
            why=("llvm-mca estimates the reciprocal throughput of this loop body; "
                 "higher values indicate potential for microarchitectural optimization"),
        ))

    return findings


# ── Phase: Perf Map ──────────────────────────────────────────────────────────

def load_perf_map(path: str) -> tuple[list[PerfSample], PerfMapStats]:
    """Load and validate a JSONL perf map file.

    Returns (aggregated_samples, stats).
    """
    if not os.path.isfile(path):
        error(E_USAGE_BAD_ARGS, f"perf-map file not found: {redact(path)}", EXIT_USAGE)

    stats = PerfMapStats()
    ip_agg: dict[int, int] = {}  # ip -> total samples

    with open(path) as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            stats.total_rows += 1

            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                stats.rejected["json_parse_error"] = stats.rejected.get("json_parse_error", 0) + 1
                continue

            # Extract ip
            ip_raw = rec.get("ip")
            if ip_raw is None:
                stats.rejected["missing_ip"] = stats.rejected.get("missing_ip", 0) + 1
                continue

            try:
                if isinstance(ip_raw, str):
                    ip = int(ip_raw, 16) if ip_raw.startswith("0x") else int(ip_raw, 16)
                else:
                    ip = int(ip_raw)
            except (ValueError, TypeError):
                stats.rejected["invalid_ip"] = stats.rejected.get("invalid_ip", 0) + 1
                continue

            # Extract samples
            samples_raw = rec.get("samples", 1)
            try:
                samples = int(samples_raw)
            except (ValueError, TypeError):
                stats.rejected["bad_samples"] = stats.rejected.get("bad_samples", 0) + 1
                continue

            if samples <= 0:
                stats.rejected["bad_samples"] = stats.rejected.get("bad_samples", 0) + 1
                continue

            stats.valid_rows += 1
            stats.total_samples += samples
            ip_agg[ip] = ip_agg.get(ip, 0) + samples

    # Convert to PerfSample list
    samples_list = [
        PerfSample(ip=ip, samples=count)
        for ip, count in sorted(ip_agg.items())
    ]

    verbose(f"perf-map: {stats.valid_rows}/{stats.total_rows} valid rows, "
            f"{stats.total_samples} total samples, {len(ip_agg)} unique IPs")
    if stats.rejected:
        verbose(f"perf-map rejected: {stats.rejected}")

    return samples_list, stats


def compute_sample_weight(
    samples: list[PerfSample],
    symbol: SymbolCandidate,
    total_samples: int,
) -> float:
    """Compute the fraction of total samples that fall within symbol's address range."""
    if total_samples == 0:
        return 0.0
    start = symbol.address
    end = symbol.address + symbol.size
    sym_samples = sum(s.samples for s in samples if start <= s.ip < end)
    return sym_samples / total_samples


def apply_perf_weights(
    findings: list[Finding],
    samples: list[PerfSample],
    symbol: SymbolCandidate,
    total_samples: int,
) -> None:
    """Apply runtime sample weights to findings, modifying impact_score in place."""
    sample_weight = compute_sample_weight(samples, symbol, total_samples)

    severity_weights = {"high": 1.0, "medium": 0.6, "low": 0.3}
    confidence_weights = {"high": 1.0, "medium": 0.7, "low": 0.4}

    for f in findings:
        sw = severity_weights.get(f.severity, 0.5)
        cw = confidence_weights.get(f.confidence, 0.5)
        f.impact_score = round(sw * cw * sample_weight, 4)

    # Re-sort findings by impact_score descending
    findings.sort(key=lambda f: f.impact_score, reverse=True)


def format_ambiguous_lookup_message(
    identifier: str,
    candidates: list[SymbolCandidate],
    side_label: str = "",
) -> str:
    prefix = f"{side_label}: " if side_label else ""
    lines = [
        f"{prefix}{len(candidates)} symbols match `{identifier}`; use --select N or --all:",
    ]
    for i, candidate in enumerate(candidates, 1):
        lines.append(
            f"  {i}: 0x{candidate.address:x} ({candidate.size} bytes) {candidate.demangled}"
        )
    return "\n".join(lines)


# ── Phase: Diff Mode ─────────────────────────────────────────────────────────

def _analyze_one_side(
    binary_path: str,
    identifier: str,
    source_location: SourceLocation | None,
    tools: dict[str, ToolInfo],
    timeout: int,
    search_mode: bool,
    fuzzy_mode: bool,
    no_cache: bool,
    rebuild_cache: bool,
    source_mode: bool,
    use_symbolize: bool,
    thresholds: dict[str, float],
    max_findings: int,
    stats: RunStats,
    side_label: str,
    select_index: int | None,
) -> tuple[BinaryInfo, SymbolCandidate, Metrics, list[Finding], list[Instruction], list[BasicBlock], str]:
    """Analyze one side of a diff. Returns (binary, symbol, metrics, findings, instructions, blocks, raw_disasm)."""
    t0 = time.monotonic()
    binary = inspect_binary(binary_path, timeout)
    stats.timing_ms[f"inspect_{side_label}"] = round((time.monotonic() - t0) * 1000, 1)

    t0 = time.monotonic()
    candidates = resolve_identifier_candidates(
        binary, identifier, source_location, tools, timeout,
        search_mode=search_mode,
        fuzzy_mode=fuzzy_mode,
        no_cache=no_cache,
        rebuild_cache=rebuild_cache,
        stats=stats,
    )
    stats.timing_ms[f"resolve_{side_label}"] = round((time.monotonic() - t0) * 1000, 1)

    if not candidates:
        error(E_LOOKUP_NOT_FOUND,
              f"no symbols match `{identifier}` in {side_label} binary: {redact(binary_path)}",
              EXIT_LOOKUP)

    if select_index is not None:
        if select_index < 0 or select_index >= len(candidates):
            error(
                E_USAGE_BAD_ARGS,
                f"--select {select_index + 1} out of range for {side_label} side (1..{len(candidates)})",
                EXIT_USAGE,
            )
        target = candidates[select_index]
    else:
        if len(candidates) > 1:
            error(
                E_LOOKUP_AMBIGUOUS,
                format_ambiguous_lookup_message(identifier, candidates, side_label),
                EXIT_LOOKUP,
            )
        target = candidates[0]

    t0 = time.monotonic()
    raw_disasm, instructions = disassemble(
        binary, target, tools, timeout,
        source_mode=source_mode,
        max_instructions=0,
        symbolize_operands=use_symbolize,
    )
    stats.timing_ms[f"disassemble_{side_label}"] = round((time.monotonic() - t0) * 1000, 1)

    blocks = build_cfg(instructions)
    metrics = compute_metrics(instructions, blocks, target)
    findings = generate_findings(instructions, blocks, metrics, thresholds, max_findings)

    return binary, target, metrics, findings, instructions, blocks, raw_disasm


def run_diff(
    before_path: str,
    after_path: str,
    identifier: str,
    source_location: SourceLocation | None,
    tools: dict[str, ToolInfo],
    timeout: int,
    search_mode: bool,
    fuzzy_mode: bool,
    no_cache: bool,
    rebuild_cache: bool,
    source_mode: bool,
    use_symbolize: bool,
    thresholds: dict[str, float],
    max_findings: int,
    stats: RunStats,
    select_index: int | None,
) -> DiffResult:
    """Run diff mode: analyze both binaries and compute deltas."""
    diff = DiffResult()

    b_bin, b_sym, b_met, b_find, b_insns, b_blocks, b_raw = _analyze_one_side(
        before_path, identifier, source_location, tools, timeout,
        search_mode, fuzzy_mode, no_cache, rebuild_cache,
        source_mode, use_symbolize, thresholds, max_findings,
        stats, "before", select_index,
    )
    a_bin, a_sym, a_met, a_find, a_insns, a_blocks, a_raw = _analyze_one_side(
        after_path, identifier, source_location, tools, timeout,
        search_mode, fuzzy_mode, no_cache, rebuild_cache,
        source_mode, use_symbolize, thresholds, max_findings,
        stats, "after", select_index,
    )

    diff.before_binary = b_bin
    diff.after_binary = a_bin
    diff.before_target = b_sym
    diff.after_target = a_sym
    diff.before_metrics = b_met
    diff.after_metrics = a_met
    diff.before_findings = b_find
    diff.after_findings = a_find

    # Determine match method
    if b_sym.mangled == a_sym.mangled:
        diff.match_method = "mangled_symbol"
    elif b_sym.demangled == a_sym.demangled:
        diff.match_method = "demangled_name"
        diff.caveats.append(
            "resolved by demangled name only; mangled symbols differ across binaries"
        )
    else:
        diff.match_method = "unresolved"
        diff.caveats.append(
            f"symbol names differ: before=`{b_sym.demangled}`, after=`{a_sym.demangled}`"
        )

    # Check architecture match
    if b_bin.arch != a_bin.arch:
        diff.caveats.append(f"architecture mismatch: {b_bin.arch} vs {a_bin.arch}")

    if diff.match_method != "unresolved":
        # Compute deltas (absolute for counts, relative for densities)
        diff.deltas = {
            "instruction_count": a_met.instruction_count - b_met.instruction_count,
            "byte_size": a_met.byte_size - b_met.byte_size,
            "branch_count": a_met.branch_count - b_met.branch_count,
            "call_count": a_met.call_count - b_met.call_count,
            "spill_store_count": a_met.spill_store_count - b_met.spill_store_count,
            "spill_load_count": a_met.spill_load_count - b_met.spill_load_count,
            "spill_density": round(a_met.spill_density - b_met.spill_density, 4),
            "branch_density": round(a_met.branch_density - b_met.branch_density, 4),
            "call_density": round(a_met.call_density - b_met.call_density, 4),
            "block_count": a_met.block_count - b_met.block_count,
            "loop_count": a_met.loop_count - b_met.loop_count,
        }

    return diff


def render_diff_text(diff: DiffResult, stats: RunStats) -> str:
    """Render diff results as human-readable text."""
    lines: list[str] = []

    lines.append("=== Diff Mode ===")
    lines.append(f"Match method: {diff.match_method}")
    if diff.caveats:
        for c in diff.caveats:
            lines.append(f"  Caveat: {c}")
    lines.append("")

    # Before
    lines.append("--- Before ---")
    lines.append(f"Binary: {redact(diff.before_binary.path)}")
    lines.append(f"Function: {diff.before_target.demangled}")
    lines.append(f"Build ID: {diff.before_binary.build_id[:16]}...")
    bm = diff.before_metrics
    lines.append(f"Instructions: {bm.instruction_count}  Bytes: {bm.byte_size}")
    lines.append(f"Spill: {bm.spill_density:.1%}  Branch: {bm.branch_density:.1%}  Call: {bm.call_density:.1%}")
    lines.append(f"Blocks: {bm.block_count}  Loops: {bm.loop_count}")
    lines.append("")

    # After
    lines.append("+++ After +++")
    lines.append(f"Binary: {redact(diff.after_binary.path)}")
    lines.append(f"Function: {diff.after_target.demangled}")
    lines.append(f"Build ID: {diff.after_binary.build_id[:16]}...")
    am = diff.after_metrics
    lines.append(f"Instructions: {am.instruction_count}  Bytes: {am.byte_size}")
    lines.append(f"Spill: {am.spill_density:.1%}  Branch: {am.branch_density:.1%}  Call: {am.call_density:.1%}")
    lines.append(f"Blocks: {am.block_count}  Loops: {am.loop_count}")
    lines.append("")

    # Deltas
    if diff.match_method == "unresolved":
        lines.append("=== Deltas ===")
        lines.append("  omitted: unresolved target mapping")
        lines.append("")
    else:
        lines.append("=== Deltas ===")
        for key, val in diff.deltas.items():
            if isinstance(val, float):
                sign = "+" if val > 0 else ""
                if "density" in key:
                    lines.append(f"  {key}: {sign}{val:.2%}")
                else:
                    lines.append(f"  {key}: {sign}{val:.4f}")
            else:
                sign = "+" if val > 0 else ""
                lines.append(f"  {key}: {sign}{val}")
        lines.append("")

    # Findings comparison
    before_ids = {f.id for f in diff.before_findings}
    after_ids = {f.id for f in diff.after_findings}
    new_findings = after_ids - before_ids
    gone_findings = before_ids - after_ids

    if new_findings or gone_findings:
        lines.append("=== Finding Changes ===")
        for fid in sorted(new_findings):
            f = next(f for f in diff.after_findings if f.id == fid)
            lines.append(f"  + [{f.severity}] {f.summary}")
        for fid in sorted(gone_findings):
            f = next(f for f in diff.before_findings if f.id == fid)
            lines.append(f"  - [{f.severity}] {f.summary}")
        lines.append("")

    # Timing
    if stats.timing_ms:
        lines.append("=== Timing ===")
        for phase, ms in stats.timing_ms.items():
            lines.append(f"  {phase}: {ms:.0f}ms")
        lines.append("")

    return "\n".join(lines)


def render_diff_json(
    diff: DiffResult,
    stats: RunStats,
    tools: dict[str, ToolInfo],
) -> str:
    """Render diff results as JSON matching the spec's diff schema."""

    def _metrics_dict(m: Metrics) -> dict:
        return {
            "instruction_count": m.instruction_count,
            "byte_size": m.byte_size,
            "branch_density": m.branch_density,
            "call_density": m.call_density,
            "spill_density": m.spill_density,
            "branch_count": m.branch_count,
            "call_count": m.call_count,
            "spill_store_count": m.spill_store_count,
            "spill_load_count": m.spill_load_count,
            "block_count": m.block_count,
            "loop_count": m.loop_count,
        }

    def _binary_dict(b: BinaryInfo) -> dict:
        return {
            "path": redact(b.path),
            "format": b.format,
            "arch": b.arch,
            "build_id": b.build_id,
        }

    def _target_dict(s: SymbolCandidate) -> dict:
        return {
            "identifier": s.demangled,
            "mangled": s.mangled,
            "address_start": f"0x{s.address:x}",
            "address_end": f"0x{s.address + s.size:x}",
        }

    obj: dict[str, Any] = {
        "schema_version": 1,
        "mode": "diff",
        "tool": {"name": "analyze-assembly", "version": VERSION},
        "toolchain": {},
        "before": {
            "binary": _binary_dict(diff.before_binary),
            "target": _target_dict(diff.before_target),
            "metrics": _metrics_dict(diff.before_metrics),
            "findings": [
                {"id": f.id, "severity": f.severity, "summary": f.summary}
                for f in diff.before_findings
            ],
        },
        "after": {
            "binary": _binary_dict(diff.after_binary),
            "target": _target_dict(diff.after_target),
            "metrics": _metrics_dict(diff.after_metrics),
            "findings": [
                {"id": f.id, "severity": f.severity, "summary": f.summary}
                for f in diff.after_findings
            ],
        },
        "match_method": diff.match_method,
        "caveats": diff.caveats,
        "run_stats": {
            "timing_ms": stats.timing_ms,
            "cache": stats.cache,
            "input": stats.input_stats,
        },
    }

    if diff.match_method != "unresolved":
        obj["deltas"] = diff.deltas

    # Toolchain info
    for name, ti in tools.items():
        if ti.available:
            obj["toolchain"][name.replace("-", "_")] = ti.version

    return json.dumps(obj, indent=2)


# ── Metrics Computation ──────────────────────────────────────────────────────

def compute_metrics(
    instructions: list[Instruction],
    blocks: list[BasicBlock],
    symbol: SymbolCandidate,
) -> Metrics:
    """Compute analysis metrics from parsed instructions and CFG."""
    m = Metrics()
    m.instruction_count = len(instructions)
    m.byte_size = symbol.size

    for insn in instructions:
        if insn.is_branch:
            m.branch_count += 1
        if insn.is_call:
            m.call_count += 1
        if insn.is_spill_store:
            m.spill_store_count += 1
        if insn.is_spill_load:
            m.spill_load_count += 1
        if insn.is_ret:
            m.ret_count += 1
        if insn.is_nop:
            m.nop_count += 1

    n = m.instruction_count
    if n > 0:
        m.branch_density = round(m.branch_count / n, 4)
        m.call_density = round(m.call_count / n, 4)
        m.spill_density = round((m.spill_store_count + m.spill_load_count) / n, 4)

    # Block-level metrics
    m.block_count = len(blocks)
    m.loop_count = sum(1 for b in blocks if b.is_loop_header)

    for bb in blocks:
        if not bb.instructions or bb.is_landing_pad:
            continue
        bn = len(bb.instructions)
        if bn == 0:
            continue
        bb_branches = sum(1 for i in bb.instructions if i.is_branch)
        bb_spills = sum(1 for i in bb.instructions if i.is_spill_store or i.is_spill_load)
        bb_bd = bb_branches / bn
        bb_sd = bb_spills / bn
        m.max_block_branch_density = max(m.max_block_branch_density, round(bb_bd, 4))
        m.max_block_spill_density = max(m.max_block_spill_density, round(bb_sd, 4))

    return m


# ── Finding Generation ───────────────────────────────────────────────────────

def generate_findings(
    instructions: list[Instruction],
    blocks: list[BasicBlock],
    metrics: Metrics,
    thresholds: dict[str, float],
    max_findings: int,
) -> list[Finding]:
    """Generate findings based on metrics and thresholds."""
    findings: list[Finding] = []

    # Finding: high spill density
    if metrics.spill_density >= thresholds.get("spill_density", 0.10):
        evidence = []
        for insn in instructions:
            if insn.is_spill_store or insn.is_spill_load:
                evidence.append(f"0x{insn.address:x}: {insn.mnemonic} {insn.operands}")
                if len(evidence) >= 6:
                    break
        findings.append(Finding(
            id="spill-density-high",
            severity="high",
            confidence="medium",
            summary=f"High register spill density ({metrics.spill_density:.1%})",
            evidence=evidence,
            why="Excessive stack spills indicate register pressure; "
                "may cause extra memory traffic in hot paths",
        ))

    # Finding: high spill density in specific loop blocks
    for bb in blocks:
        if not bb.is_loop_header or bb.is_landing_pad:
            continue
        bn = len(bb.instructions)
        if bn < 4:
            continue
        bb_spills = sum(1 for i in bb.instructions if i.is_spill_store or i.is_spill_load)
        bb_sd = bb_spills / bn
        if bb_sd >= thresholds.get("spill_density", 0.10):
            evidence = []
            for insn in bb.instructions:
                if insn.is_spill_store or insn.is_spill_load:
                    evidence.append(f"0x{insn.address:x}: {insn.mnemonic} {insn.operands}")
                    if len(evidence) >= 4:
                        break
            findings.append(Finding(
                id=f"spill-in-loop-{bb.label}",
                severity="high",
                confidence="medium",
                summary=f"Register spills in loop at {bb.label} ({bb_sd:.1%} of {bn} insns)",
                evidence=evidence,
                why="Spills inside loop bodies multiply memory traffic by iteration count",
            ))

    # Finding: high branch density
    if metrics.branch_density >= thresholds.get("branch_density", 0.25):
        evidence = []
        for insn in instructions:
            if insn.is_branch:
                evidence.append(f"0x{insn.address:x}: {insn.mnemonic} {insn.operands}")
                if len(evidence) >= 6:
                    break
        findings.append(Finding(
            id="branch-density-high",
            severity="medium",
            confidence="medium",
            summary=f"High branch density ({metrics.branch_density:.1%})",
            evidence=evidence,
            why="Dense branching may stress the branch predictor; "
                "consider if scalar branches can be replaced with SIMD or branchless logic",
        ))

    # Finding: branch-heavy loop blocks
    for bb in blocks:
        if not bb.is_loop_header or bb.is_landing_pad:
            continue
        bn = len(bb.instructions)
        if bn < 4:
            continue
        bb_branches = sum(1 for i in bb.instructions if i.is_branch)
        bb_bd = bb_branches / bn
        if bb_bd >= thresholds.get("branch_density", 0.25):
            evidence = []
            for insn in bb.instructions:
                if insn.is_branch:
                    evidence.append(f"0x{insn.address:x}: {insn.mnemonic} {insn.operands}")
                    if len(evidence) >= 4:
                        break
            findings.append(Finding(
                id=f"branch-heavy-loop-{bb.label}",
                severity="medium",
                confidence="medium",
                summary=f"Branch-heavy loop at {bb.label} ({bb_bd:.1%}, {bb_branches} branches in {bn} insns)",
                evidence=evidence,
                why="Branch-heavy loops are candidates for vectorization or branchless rewriting",
            ))

    # Finding: high call density
    if metrics.call_density >= thresholds.get("call_density", 0.12):
        evidence = []
        for insn in instructions:
            if insn.is_call:
                evidence.append(f"0x{insn.address:x}: {insn.mnemonic} {insn.operands}")
                if len(evidence) >= 6:
                    break
        findings.append(Finding(
            id="call-density-high",
            severity="medium",
            confidence="low",
            summary=f"High call density ({metrics.call_density:.1%})",
            evidence=evidence,
            why="Frequent function calls may inhibit inlining and optimization; "
                "check if callees are small enough to inline",
        ))

    # Finding: very large function
    if metrics.instruction_count > 1000:
        findings.append(Finding(
            id="function-very-large",
            severity="low",
            confidence="high",
            summary=f"Very large function ({metrics.instruction_count} instructions, {metrics.byte_size} bytes)",
            evidence=[],
            why="Large functions may exceed instruction cache capacity and "
                "are harder for the compiler to optimize globally",
        ))

    # Cap findings
    if max_findings and len(findings) > max_findings:
        findings = findings[:max_findings]

    return findings


# ── Output: Text ─────────────────────────────────────────────────────────────

def render_text(
    binary: BinaryInfo,
    symbol: SymbolCandidate,
    metrics: Metrics,
    findings: list[Finding],
    raw_disasm: str,
    instructions: list[Instruction],
    blocks: list[BasicBlock],
    stats: RunStats,
    notes: list[str],
    max_instructions: int,
    emit_disasm_file: str,
    context_symbols: list[SymbolCandidate] | None = None,
    mca_results: list[McaResult] | None = None,
) -> str:
    """Render human-readable text output."""
    lines: list[str] = []

    # Target section
    lines.append("=== Target ===")
    lines.append(f"Function: {symbol.demangled}")
    lines.append(f"Binary: {redact(binary.path)}")
    lines.append(f"Format: {binary.format}")
    lines.append(f"Arch: {binary.arch}")
    if binary.build_id:
        lines.append(f"Build ID: {binary.build_id[:16]}...")
    lines.append(f"Address: 0x{symbol.address:x}..0x{symbol.address + symbol.size:x} ({symbol.size} bytes)")
    lines.append(f"Resolution confidence: high")
    lines.append("")

    # Metrics section
    lines.append("=== Metrics ===")
    lines.append(f"Instructions: {metrics.instruction_count}")
    lines.append(f"Byte size: {metrics.byte_size}")
    lines.append(f"Branch density: {metrics.branch_density:.1%} ({metrics.branch_count} branches)")
    lines.append(f"Call density: {metrics.call_density:.1%} ({metrics.call_count} calls)")
    lines.append(f"Spill density: {metrics.spill_density:.1%} "
                 f"({metrics.spill_store_count} stores + {metrics.spill_load_count} loads)")
    if blocks:
        lines.append(f"Basic blocks: {metrics.block_count}")
        lines.append(f"Loops detected: {metrics.loop_count}")
        if metrics.max_block_spill_density > 0:
            lines.append(f"Max block spill density: {metrics.max_block_spill_density:.1%}")
        if metrics.max_block_branch_density > 0:
            lines.append(f"Max block branch density: {metrics.max_block_branch_density:.1%}")
    lines.append("")

    # Findings section (includes MCA findings if any, since they're appended to the list)
    if findings:
        lines.append("=== Findings ===")
        for f in findings:
            score_str = f" (impact: {f.impact_score:.4f})" if f.impact_score > 0 else ""
            lines.append(f"[{f.severity}] {f.summary}{score_str}")
            for ev in f.evidence:
                lines.append(f"  {ev}")
            if f.why:
                lines.append(f"  Why: {f.why}")
            lines.append("")
    else:
        lines.append("=== Findings ===")
        lines.append("No findings exceed configured thresholds.")
        lines.append("")

    # MCA summary (if we have results, show a compact table)
    if mca_results:
        lines.append("=== llvm-mca Summary ===")
        for mr in mca_results:
            lines.append(f"  Loop {mr.loop_label}: "
                         f"rThroughput={mr.block_rthroughput:.1f} cycles  "
                         f"IPC={mr.throughput_ipc}")
            if mr.bottleneck:
                lines.append(f"    Bottleneck: {mr.bottleneck}")
        lines.append("")

    # Context symbols
    if context_symbols:
        lines.append("=== Context Symbols ===")
        for cs in context_symbols:
            lines.append(f"  0x{cs.address:x} ({cs.size} bytes): {cs.demangled}")
        lines.append("")

    # Disassembly
    if emit_disasm_file:
        lines.append(f"=== Disassembly written to: {redact(emit_disasm_file)} ===")
    elif instructions:
        n_shown = min(len(instructions), max_instructions) if max_instructions else len(instructions)
        lines.append(f"=== Disassembly ({n_shown} of {len(instructions)} instructions) ===")
        for insn in instructions[:n_shown]:
            lines.append(f"  0x{insn.address:x}: {insn.mnemonic} {insn.operands}")
        if max_instructions and len(instructions) > max_instructions:
            lines.append(f"  ... truncated ({len(instructions) - max_instructions} more instructions)")
        lines.append("")

    # Notes
    if notes:
        lines.append("=== Notes ===")
        for n in notes:
            lines.append(f"  {n}")
        lines.append("")

    # Stats
    if stats.timing_ms:
        lines.append("=== Timing ===")
        for phase, ms in stats.timing_ms.items():
            lines.append(f"  {phase}: {ms:.0f}ms")
        lines.append("")

    return "\n".join(lines)


# ── Output: JSON ─────────────────────────────────────────────────────────────

def render_json(
    binary: BinaryInfo,
    symbol: SymbolCandidate,
    metrics: Metrics,
    findings: list[Finding],
    raw_disasm: str,
    instructions: list[Instruction],
    blocks: list[BasicBlock],
    stats: RunStats,
    notes: list[str],
    tools: dict[str, ToolInfo],
    max_disasm_bytes: int,
    emit_disasm_file: str,
    context_symbols: list[SymbolCandidate] | None = None,
    mca_results: list[McaResult] | None = None,
    perf_stats: PerfMapStats | None = None,
) -> str:
    """Render JSON output matching the spec's schema."""
    obj: dict[str, Any] = {
        "schema_version": 1,
        "tool": {
            "name": "analyze-assembly",
            "version": VERSION,
        },
        "toolchain": {},
        "binary": {
            "path": redact(binary.path),
            "format": binary.format,
            "arch": binary.arch,
            "build_id": binary.build_id,
        },
        "target": {
            "identifier": symbol.demangled,
            "mangled": symbol.mangled,
            "address_start": f"0x{symbol.address:x}",
            "address_end": f"0x{symbol.address + symbol.size:x}",
            "resolution_confidence": "high",
        },
        "metrics": {
            "instruction_count": metrics.instruction_count,
            "byte_size": metrics.byte_size,
            "branch_density": metrics.branch_density,
            "call_density": metrics.call_density,
            "spill_density": metrics.spill_density,
            "branch_count": metrics.branch_count,
            "call_count": metrics.call_count,
            "spill_store_count": metrics.spill_store_count,
            "spill_load_count": metrics.spill_load_count,
            "block_count": metrics.block_count,
            "loop_count": metrics.loop_count,
            "max_block_branch_density": metrics.max_block_branch_density,
            "max_block_spill_density": metrics.max_block_spill_density,
        },
        "findings": [
            {
                "id": f.id,
                "severity": f.severity,
                "confidence": f.confidence,
                "summary": f.summary,
                "evidence": f.evidence,
                "why": f.why,
                "impact_score": f.impact_score,
            }
            for f in findings
        ],
        "notes": notes,
        "run_stats": {
            "timing_ms": stats.timing_ms,
            "cache": stats.cache,
            "input": stats.input_stats,
        },
    }

    # Toolchain info
    for name, ti in tools.items():
        if ti.available:
            key = name.replace("-", "_")
            obj["toolchain"][key] = ti.version

    # Disassembly
    if emit_disasm_file:
        obj["disassembly_file"] = redact(emit_disasm_file)
    else:
        disasm_text = raw_disasm
        if max_disasm_bytes and len(disasm_text.encode()) > max_disasm_bytes:
            disasm_text = disasm_text[:max_disasm_bytes] + "\n... truncated"
        obj["disassembly"] = disasm_text

    # Context symbols
    if context_symbols:
        obj["context_symbols"] = [
            {
                "demangled": cs.demangled,
                "mangled": cs.mangled,
                "address": f"0x{cs.address:x}",
                "size": cs.size,
            }
            for cs in context_symbols
        ]

    # MCA results
    if mca_results:
        obj["mca"] = [
            {
                "loop_label": mr.loop_label,
                "block_rthroughput": mr.block_rthroughput,
                "throughput_ipc": mr.throughput_ipc,
                "total_cycles": mr.total_cycles,
                "iterations": mr.iterations,
                "bottleneck": mr.bottleneck,
            }
            for mr in mca_results
        ]

    # Perf map stats
    if perf_stats:
        obj["perf_map"] = {
            "total_rows": perf_stats.total_rows,
            "valid_rows": perf_stats.valid_rows,
            "total_samples": perf_stats.total_samples,
            "rejected": perf_stats.rejected,
        }

    return json.dumps(obj, indent=2)


# ── Config File Loading ──────────────────────────────────────────────────────

def load_config(config_path: str) -> dict[str, Any]:
    """Load JSON config file with forward-compatible unknown key warnings."""
    try:
        with open(config_path) as f:
            cfg = json.load(f)
    except FileNotFoundError:
        error(E_USAGE_BAD_ARGS, f"config file not found: {redact(config_path)}", EXIT_USAGE)
    except json.JSONDecodeError as e:
        error(E_USAGE_BAD_ARGS, f"config file is not valid JSON: {e}", EXIT_USAGE)

    known_keys = {
        "format", "max_findings", "timeout_sec", "max_disassembly_bytes",
        "max_instructions", "threshold_profile", "thresholds",
        "ci_gate", "no_cache", "rebuild_cache", "no_redact_paths",
    }
    for key in cfg:
        if key not in known_keys:
            warn(f"unknown config key: {key}")

    return cfg


# ── Context Symbols ──────────────────────────────────────────────────────────

def find_context_symbols(
    all_candidates: list[SymbolCandidate],
    target: SymbolCandidate,
    context_n: int,
    binary: BinaryInfo,
    tools: dict[str, ToolInfo],
    timeout: int,
    stats: RunStats,
) -> list[SymbolCandidate]:
    """Find N symbols surrounding the target by address."""
    # We need the full symbol list for this, so re-read the cache
    key = _cache_key(binary)
    cdir = _cache_dir()
    raw_path = cdir / f"{key}.symbols.raw"
    dem_path = cdir / f"{key}.symbols.demangled"

    if not raw_path.exists():
        return []

    raw_lines = raw_path.read_text().splitlines()
    dem_lines = dem_path.read_text().splitlines()
    n = min(len(raw_lines), len(dem_lines))

    # Build a list of (addr, size, demangled, mangled) tuples
    all_syms: list[SymbolCandidate] = []
    for i in range(n):
        raw_parts = raw_lines[i].split(None, 3)
        dem_parts = dem_lines[i].split(None, 3)
        if len(raw_parts) < 4:
            continue
        try:
            addr = int(raw_parts[0], 16)
            size = int(raw_parts[1], 16)
        except ValueError:
            continue
        if size == 0:
            continue
        mangled = raw_parts[3]
        demangled = dem_parts[3] if len(dem_parts) >= 4 else mangled
        all_syms.append(SymbolCandidate(
            mangled=mangled, demangled=demangled,
            address=addr, size=size, sym_type=raw_parts[2],
        ))

    all_syms.sort(key=lambda s: s.address)

    # Find target index
    target_idx = -1
    for i, s in enumerate(all_syms):
        if s.address == target.address and s.mangled == target.mangled:
            target_idx = i
            break

    if target_idx < 0:
        return []

    # Get surrounding symbols
    start = max(0, target_idx - context_n)
    end = min(len(all_syms), target_idx + context_n + 1)
    return [s for s in all_syms[start:end] if s.address != target.address]


# ── CLI Parser ───────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="analyze-assembly",
        description="Analyze assembly for optimization opportunities",
    )
    p.add_argument("args", nargs="+", metavar="ARG",
                   help="<binary> <identifier> or just <identifier> in diff mode")
    p.add_argument("--source", "-S", action="store_true",
                   help="Interleave source lines (forces symbol-mode disassembly)")
    p.add_argument("--search", action="store_true",
                   help="Treat identifier as regex pattern")
    p.add_argument("--fuzzy", action="store_true",
                   help="Substring search mode for symbol lookup")
    p.add_argument("--select", type=int, metavar="N",
                   help="Select candidate symbol by index (1-based)")
    p.add_argument("--all", action="store_true",
                   help="Analyze all candidate symbols")
    p.add_argument("--context", type=int, default=0, metavar="N",
                   help="Include N surrounding symbols")
    p.add_argument("--format", choices=["text", "json"], default=None,
                   help="Output format (default: text)")
    p.add_argument("--verbose", "-v", action="store_true",
                   help="Print tool command traces to stderr")
    p.add_argument("--max-instructions", type=int, default=None,
                   metavar="N", help=f"Max instructions to show (default: {DEFAULT_MAX_INSTRUCTIONS})")
    p.add_argument("--max-disassembly-bytes", type=int,
                   default=None, metavar="N",
                   help="Max disassembly payload size in bytes")
    p.add_argument("--max-findings", type=int, default=None,
                   metavar="N", help=f"Max findings to emit (default: {DEFAULT_MAX_FINDINGS})")
    p.add_argument("--emit-disassembly-file", metavar="PATH",
                   help="Write disassembly to file instead of stdout")
    p.add_argument("--timeout-sec", type=int, default=None,
                   metavar="N", help=f"Timeout per tool invocation (default: {DEFAULT_TIMEOUT_SEC}s)")
    p.add_argument("--config", metavar="FILE",
                   help="Load defaults from JSON config file")
    p.add_argument("--no-cache", action="store_true",
                   help="Bypass symbol cache")
    p.add_argument("--rebuild-cache", action="store_true",
                   help="Force rebuild of symbol cache")
    p.add_argument("--no-redact-paths", action="store_true",
                   help="Show absolute paths in output")

    # Phase 3 flags
    p.add_argument("--mca", action="store_true",
                   help="Run llvm-mca on detected loop bodies")
    p.add_argument("--mcpu", metavar="NAME",
                   help="Target CPU model for llvm-mca (required with --mca)")
    p.add_argument("--perf-map", metavar="FILE",
                   help="JSONL file with sampled profile data for impact scoring")
    p.add_argument("--before", metavar="BINARY",
                   help="Diff mode: left-hand (old) binary")
    p.add_argument("--after", metavar="BINARY",
                   help="Diff mode: right-hand (new) binary")

    # Phase 4 flags: accepted but not yet implemented
    p.add_argument("--ci-gate", action="store_true",
                   help="CI gate mode (not yet implemented)")
    p.add_argument("--baseline", metavar="FILE",
                   help="Baseline JSON for --ci-gate (not yet implemented)")
    p.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")

    return p


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    global _verbose, _redact_paths

    parser = build_parser()
    args = parser.parse_args()

    # Load config first, then override with CLI
    config: dict[str, Any] = {}
    if args.config:
        config = load_config(args.config)

    # Apply defaults with explicit precedence: built-in < config < CLI.
    if args.format is None:
        args.format = config.get("format", "text")
    if args.max_findings is None:
        args.max_findings = config.get("max_findings", DEFAULT_MAX_FINDINGS)
    if args.timeout_sec is None:
        args.timeout_sec = config.get("timeout_sec", DEFAULT_TIMEOUT_SEC)
    if args.max_disassembly_bytes is None:
        args.max_disassembly_bytes = config.get("max_disassembly_bytes", DEFAULT_MAX_DISASSEMBLY_BYTES)
    if args.max_instructions is None:
        args.max_instructions = config.get("max_instructions", DEFAULT_MAX_INSTRUCTIONS)
    if config.get("no_cache"):
        args.no_cache = True
    if config.get("no_redact_paths"):
        args.no_redact_paths = True

    # Set globals
    _verbose = args.verbose
    _redact_paths = not args.no_redact_paths

    # Validate conflicts
    if args.search and args.fuzzy:
        error(E_USAGE_CONFLICT, "--search and --fuzzy are mutually exclusive", EXIT_USAGE)
    if args.select is not None and args.all:
        error(E_USAGE_CONFLICT, "--select and --all are mutually exclusive", EXIT_USAGE)

    # Parse positional args: <binary> <identifier> or just <identifier> in diff mode
    positional = args.args
    if args.before and args.after:
        if len(positional) == 1:
            args.binary = None
            args.identifier = positional[0]
        elif len(positional) == 2:
            args.binary = positional[0]
            args.identifier = positional[1]
        else:
            error(E_USAGE_BAD_ARGS,
                  "diff mode expects: --before <binary_a> --after <binary_b> <identifier>",
                  EXIT_USAGE)
    else:
        if len(positional) < 2:
            error(E_USAGE_BAD_ARGS, "usage: analyze-assembly <binary> <identifier>", EXIT_USAGE)
        args.binary = positional[0]
        args.identifier = positional[1]

    source_location = parse_source_location(args.identifier)

    # Validate Phase 3 feature requirements
    if args.mca and not args.mcpu:
        error(E_USAGE_BAD_ARGS,
              "--mca requires --mcpu (never silently assume host CPU equals target CPU)",
              EXIT_USAGE)
    if args.before and not args.after:
        error(E_USAGE_CONFLICT, "--before requires --after for diff mode", EXIT_USAGE)
    if args.after and not args.before:
        error(E_USAGE_CONFLICT, "--after requires --before for diff mode", EXIT_USAGE)

    # Check deferred features
    if args.ci_gate or args.baseline:
        error(E_USAGE_BAD_ARGS, "--ci-gate/--baseline not yet implemented (Phase 4)", EXIT_USAGE)

    stats = RunStats()
    notes: list[str] = []
    thresholds = dict(DEFAULT_THRESHOLDS)
    if config.get("thresholds"):
        thresholds.update(config["thresholds"])

    # ── Phase: Probe Tools ───────────────────────────────────────────────
    t0 = time.monotonic()
    tools = probe_tools(
        needs_mca=args.mca,
        needs_dwarfdump=bool(source_location),
    )
    llvm_major = get_llvm_major(tools)
    use_symbolize = llvm_major >= MIN_LLVM_VERSION
    stats.timing_ms["probe"] = round((time.monotonic() - t0) * 1000, 1)

    # ── Load perf map if provided ────────────────────────────────────────
    perf_samples: list[PerfSample] = []
    perf_stats: PerfMapStats | None = None
    if args.perf_map:
        t0 = time.monotonic()
        perf_samples, perf_stats = load_perf_map(args.perf_map)
        stats.timing_ms["perf_map"] = round((time.monotonic() - t0) * 1000, 1)
        stats.input_stats["perf_samples"] = perf_stats.total_samples
        stats.input_stats["perf_unique_ips"] = len(perf_samples)
        if perf_stats.rejected:
            for reason, count in perf_stats.rejected.items():
                notes.append(f"perf-map: {count} rows rejected ({reason})")
    else:
        notes.append("static-only ranking; runtime impact unknown")

    # ── Diff mode ────────────────────────────────────────────────────────
    if args.before and args.after:
        t0 = time.monotonic()
        diff = run_diff(
            args.before, args.after, args.identifier, source_location,
            tools, args.timeout_sec,
            search_mode=args.search,
            fuzzy_mode=args.fuzzy,
            no_cache=args.no_cache,
            rebuild_cache=args.rebuild_cache,
            source_mode=args.source,
            use_symbolize=use_symbolize,
            thresholds=thresholds,
            max_findings=args.max_findings,
            stats=stats,
            select_index=(args.select - 1) if args.select is not None else None,
        )
        stats.timing_ms["diff_total"] = round((time.monotonic() - t0) * 1000, 1)

        if args.format == "json":
            output = render_diff_json(diff, stats, tools)
        else:
            output = render_diff_text(diff, stats)
        print(output)
        return EXIT_OK

    # ── Normal (single-binary) mode ──────────────────────────────────────

    # ── Phase: Inspect Binary ────────────────────────────────────────────
    t0 = time.monotonic()
    binary = inspect_binary(args.binary, args.timeout_sec)
    stats.timing_ms["inspect"] = round((time.monotonic() - t0) * 1000, 1)

    if binary.arch not in ("x86_64", "x86", "aarch64"):
        warn(f"architecture {binary.arch} has limited classification support")

    # ── Phase: Resolve Symbols ───────────────────────────────────────────
    t0 = time.monotonic()
    candidates = resolve_identifier_candidates(
        binary, args.identifier, source_location, tools, args.timeout_sec,
        search_mode=args.search,
        fuzzy_mode=args.fuzzy,
        no_cache=args.no_cache,
        rebuild_cache=args.rebuild_cache,
        stats=stats,
    )
    stats.timing_ms["resolve"] = round((time.monotonic() - t0) * 1000, 1)

    if not candidates:
        if args.format == "json":
            err_json = json.dumps({
                "error_code": E_LOOKUP_NOT_FOUND,
                "message": f"no symbols match `{args.identifier}`",
                "exit_code": EXIT_LOOKUP,
            }, indent=2)
            print(err_json)
        error(E_LOOKUP_NOT_FOUND,
              f"no symbols match `{args.identifier}`", EXIT_LOOKUP)

    if len(candidates) > 1 and not args.all and args.select is None:
        msg = format_ambiguous_lookup_message(args.identifier, candidates)
        if args.format == "json":
            err_json = json.dumps({
                "error_code": E_LOOKUP_AMBIGUOUS,
                "message": msg,
                "exit_code": EXIT_LOOKUP,
                "candidates": [
                    {
                        "index": i,
                        "demangled": c.demangled,
                        "mangled": c.mangled,
                        "address": f"0x{c.address:x}",
                        "size": c.size,
                    }
                    for i, c in enumerate(candidates, 1)
                ],
            }, indent=2)
            print(err_json)
        error(E_LOOKUP_AMBIGUOUS, msg, EXIT_LOOKUP)

    # Select symbols to analyze
    if args.select is not None:
        idx = args.select - 1
        if idx < 0 or idx >= len(candidates):
            error(E_USAGE_BAD_ARGS,
                  f"--select {args.select} out of range (1..{len(candidates)})", EXIT_USAGE)
        targets = [candidates[idx]]
    elif args.all:
        targets = candidates
    else:
        targets = [candidates[0]]

    # ── Analyze each target ──────────────────────────────────────────────
    all_outputs: list[str] = []

    for target in targets:
        verbose(f"analyzing: {target.demangled} at 0x{target.address:x}")

        # ── Phase: Disassemble ───────────────────────────────────────────
        t0 = time.monotonic()
        raw_disasm, instructions = disassemble(
            binary, target, tools, args.timeout_sec,
            source_mode=args.source,
            max_instructions=0,  # get all, truncate in output
            symbolize_operands=use_symbolize,
        )
        stats.timing_ms["disassemble"] = round((time.monotonic() - t0) * 1000, 1)
        stats.input_stats["instructions_analyzed"] = len(instructions)

        # ── Phase: CFG ───────────────────────────────────────────────────
        t0 = time.monotonic()
        blocks = build_cfg(instructions)
        stats.timing_ms["cfg"] = round((time.monotonic() - t0) * 1000, 1)

        # ── Phase: Metrics ───────────────────────────────────────────────
        t0 = time.monotonic()
        metrics = compute_metrics(instructions, blocks, target)
        stats.timing_ms["metrics"] = round((time.monotonic() - t0) * 1000, 1)

        # ── Phase: Findings ──────────────────────────────────────────────
        t0 = time.monotonic()
        findings = generate_findings(
            instructions, blocks, metrics, thresholds, args.max_findings,
        )
        stats.timing_ms["findings"] = round((time.monotonic() - t0) * 1000, 1)

        # ── Phase: llvm-mca ──────────────────────────────────────────────
        mca_results: list[McaResult] = []
        if args.mca:
            t0 = time.monotonic()
            loops = _extract_loop_asm(blocks, instructions, binary.arch)
            verbose(f"extracted {len(loops)} loop bodies for llvm-mca")
            if loops:
                mca_results = run_mca(loops, binary.arch, args.mcpu, args.timeout_sec)
                mca_finds = mca_findings(mca_results)
                findings.extend(mca_finds)
            else:
                notes.append("no loop bodies detected for llvm-mca analysis")
            stats.timing_ms["mca"] = round((time.monotonic() - t0) * 1000, 1)

        # ── Phase: Perf weighting ────────────────────────────────────────
        if perf_samples and perf_stats:
            t0 = time.monotonic()
            apply_perf_weights(findings, perf_samples, target, perf_stats.total_samples)
            sample_weight = compute_sample_weight(perf_samples, target, perf_stats.total_samples)
            stats.timing_ms["perf_weight"] = round((time.monotonic() - t0) * 1000, 1)
            stats.input_stats["sample_weight"] = round(sample_weight * 10000)  # basis points
            if sample_weight < 0.001:
                notes.append(f"function has <0.1% of total samples ({sample_weight:.4%})")

        # Enforce global finding cap after all finding sources are combined.
        if args.max_findings and len(findings) > args.max_findings:
            findings = findings[:args.max_findings]

        stats.input_stats["findings_emitted"] = len(findings)

        # ── Context symbols ──────────────────────────────────────────────
        context_syms = None
        if args.context > 0:
            context_syms = find_context_symbols(
                candidates, target, args.context,
                binary, tools, args.timeout_sec, stats,
            )

        # ── Emit disassembly file if needed ──────────────────────────────
        emit_file = args.emit_disassembly_file
        disasm_bytes = len(raw_disasm.encode())
        if not emit_file and disasm_bytes > args.max_disassembly_bytes:
            # Auto-spill to file
            emit_file = f"tmp/{target.mangled[:80]}.asm"
            os.makedirs("tmp", exist_ok=True)
            warn(f"disassembly ({disasm_bytes} bytes) exceeds limit, "
                 f"writing to {emit_file}")

        if emit_file:
            os.makedirs(os.path.dirname(emit_file) or ".", exist_ok=True)
            with open(emit_file, "w") as f:
                f.write(raw_disasm)
            verbose(f"wrote disassembly to {emit_file}")

        # ── Phase: Render ────────────────────────────────────────────────
        t0 = time.monotonic()
        if args.format == "json":
            output = render_json(
                binary, target, metrics, findings,
                raw_disasm, instructions, blocks, stats, notes,
                tools, args.max_disassembly_bytes, emit_file or "",
                context_syms, mca_results, perf_stats,
            )
        else:
            output = render_text(
                binary, target, metrics, findings,
                raw_disasm, instructions, blocks, stats, notes,
                args.max_instructions, emit_file or "",
                context_syms, mca_results,
            )
        stats.timing_ms["render"] = round((time.monotonic() - t0) * 1000, 1)
        all_outputs.append(output)

    # Print all outputs
    print("\n".join(all_outputs))
    return EXIT_OK


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception as e:
        print(f"{E_INTERNAL_UNEXPECTED}: {e}", file=sys.stderr)
        if _verbose:
            import traceback
            traceback.print_exc()
        sys.exit(EXIT_INTERNAL)
