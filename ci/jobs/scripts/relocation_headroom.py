"""Headroom of the 32-bit PC-relative references of an x86-64 binary.

The small code model reaches +-2 GiB and lld adds no range extension thunks on x86-64, so these spans of the linked image
must stay below 2 GiB or the link fails with `relocation R_X86_64_PC32 out of range`.

Usage: python3 -m ci.jobs.scripts.relocation_headroom <binary>...
"""

import gzip
import json
import struct
import sys
import urllib.request
from pathlib import Path

from ci.praktika._environment import _Environment
from ci.praktika.result import Result
from ci.praktika.settings import Settings

NAME = "Relocation headroom"
BINARIES = ("programs/clickhouse", "src/unit_tests_dbms")
# Fails at about two weeks of master growth before the link does; warns when a change takes 10 MiB.
MIN_HEADROOM_MIB = 64
WARN_DECREASE_MIB = 10
MASTER_COMMITS_TO_TRY = 10

REACH = 1 << 31
MIB = 1 << 20
SHF_ALLOC, SHF_EXECINSTR, SHF_TLS, SHF_X86_64_LARGE = 0x2, 0x4, 0x400, 0x10000000


def read_sections(path):
    """(name, flags, address, size) of each section, from the ELF64 little-endian section header table."""
    with open(path, "rb") as f:
        header = f.read(64)
        assert header[:6] == b"\x7fELF\x02\x01", f"[{path}] is not a 64-bit little-endian ELF file"
        (offset,) = struct.unpack_from("<Q", header, 0x28)
        entry_size, count, names_index = struct.unpack_from("<HHH", header, 0x3A)
        f.seek(offset)
        table = f.read(entry_size * count)
        headers = [struct.unpack_from("<IIQQQQ", table, i * entry_size) for i in range(count)]
        f.seek(headers[names_index][4])
        names = f.read(headers[names_index][5])
    return [(names[n : names.index(b"\0", n)].decode(), flags, address, size) for n, _, flags, address, _, size in headers]


def headroom(path):
    """Span name -> bytes left of the 2 GiB reach."""
    sections = [s for s in read_sections(path) if s[1] & SHF_ALLOC and not s[1] & SHF_TLS]
    by_name = {s[0]: s for s in sections}
    small = [s for s in sections if not s[1] & SHF_X86_64_LARGE]
    code = [s for s in small if s[1] & SHF_EXECINSTR]
    all_code = [s for s in sections if s[1] & SHF_EXECINSTR]
    start = lambda group: min(s[2] for s in group)
    end = lambda group: max(s[2] + s[3] for s in group)
    eh_frame_hdr = by_name[".eh_frame_hdr"][2]
    spans = {
        ".rodata to end of code": end(code) - by_name[".rodata"][2],
        "start of code to end of data": end([s for s in small if not s[1] & SHF_EXECINSTR]) - start(code),
        ".eh_frame_hdr to code": max(end(all_code) - eh_frame_hdr, eh_frame_hdr - start(all_code)),
        "code": end(code) - start(code),
    }
    return {name: REACH - span for name, span in spans.items()}


def master_baseline(info):
    """(sha, headroom) of the nearest commit of this run's master chain whose job reported it, else (None, {})."""
    for sha in (info.get_kv_data("master_track_commits_sha") or [])[:MASTER_COMMITS_TO_TRY]:
        try:
            endpoint = Settings.S3_BUCKET_TO_HTTP_ENDPOINT[Settings.S3_REPORT_BUCKET]
            prefix = _Environment.get_s3_prefix_static(pr_number=0, branch="master", sha=sha, workflow_name="MasterCI")
            file_name = Path(Result.file_name_static(info.job_name)).name
            with urllib.request.urlopen(f"https://{endpoint}/{prefix}/{file_name}", timeout=10) as response:
                data = response.read()
            if data[:2] == b"\x1f\x8b":
                data = gzip.decompress(data)
            for result in json.loads(data).get("results", []):
                if result.get("name") == NAME and result.get("ext", {}).get("relocation_headroom"):
                    return sha, result["ext"]["relocation_headroom"]
        except Exception as e:
            print(f"No relocation headroom of master {sha}: {e}")
    return None, {}


def check(build_dir, info):
    try:
        measured = {binary: headroom(f"{build_dir}/{binary}") for binary in BINARIES}
    except Exception as e:
        return Result(name=NAME, status=Result.Status.ERROR, info=f"Failed to read the section headers: {e}")
    base_sha, base = master_baseline(info)
    lines = [
        "MiB left of the 2 GiB reach of 32-bit PC-relative references"
        + (f", change vs master {base_sha[:12]}" if base_sha else ", no master baseline")
    ]
    decreases = []
    for binary, spans in measured.items():
        parts = []
        for name, value in spans.items():
            part = f"{name} {value / MIB:.2f}"
            if name in base.get(binary, {}):
                delta = value - base[binary][name]
                decreases.append((-delta, binary, name))
                part += f" ({delta / MIB:+.2f})"
            parts.append(part)
        lines.append(f"{binary}: " + ", ".join(parts))
    lowest, binary, name = min((v, b, n) for b, spans in measured.items() for n, v in spans.items())
    ok = lowest >= MIN_HEADROOM_MIB * MIB
    lines.append(
        f"Lowest: {lowest / MIB:.2f} MiB ({binary}, {name}); the check fails below {MIN_HEADROOM_MIB} MiB"
        + ("" if ok else ". Shrink the code or the data before the link fails, see #126314")
    )
    if decreases and max(decreases)[0] >= WARN_DECREASE_MIB * MIB:
        decrease, binary, name = max(decreases)
        info.add_workflow_warning(
            f"The relocation headroom of {binary} ({name}) decreased by {decrease / MIB:.2f} MiB vs master {base_sha[:12]}"
        )
    text = "\n".join(lines)
    print(text)
    return Result(
        name=NAME,
        status=Result.Status.OK if ok else Result.Status.FAIL,
        info=text,
        ext={"relocation_headroom": measured},
    )


if __name__ == "__main__":
    for path in sys.argv[1:]:
        print(path, {name: round(value / MIB, 2) for name, value in headroom(path).items()})
