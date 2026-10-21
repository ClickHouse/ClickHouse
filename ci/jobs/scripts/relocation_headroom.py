"""Headroom of the 32-bit PC-relative references of an x86-64 binary.

The small code model reaches +-2 GiB and lld adds no range extension thunks on x86-64. lld places the read-only data, the unwind
tables and the code above the ELF header (which code references as `__ehdr_start`) and the writable data above the code, and the
unwind tables point at personality and type-info slots in `.data`, so these spans must stay below 2 GiB or the link fails with
`relocation R_X86_64_PC32 out of range`.

Usage: python3 -m ci.jobs.scripts.relocation_headroom <binary>...
"""

import gzip
import json
import struct
import sys
import urllib.error
import urllib.request
from pathlib import Path

from ci.praktika._environment import _Environment
from ci.praktika.result import Result
from ci.praktika.settings import Settings

NAME = "Relocation headroom"
BINARIES = ("programs/clickhouse", "src/unit_tests_dbms")
# A pull request that takes more than 5 MiB fails unless it has the label; below 64 MiB, about two weeks of master growth
# before the link fails, every run warns.
MAX_DECREASE_MIB = 5
WARN_HEADROOM_MIB = 64
ALLOW_DECREASE_LABEL = "ci-relocation-headroom-ok"
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
    end_of_code = max(s[2] + s[3] for s in code)
    end_of_data = max(s[2] + s[3] for s in small if not s[1] & SHF_EXECINSTR)
    unwind_tables = min(by_name[n][2] for n in (".gcc_except_table", ".eh_frame") if n in by_name)
    data = by_name[".data"]
    spans = {
        "start of image to end of code": end_of_code,
        "start of code to end of data": end_of_data - min(s[2] for s in code),
        "unwind tables to end of .data": data[2] + data[3] - unwind_tables,
    }
    return {name: REACH - span for name, span in spans.items()}


def master_baseline(info):
    """(commit, headroom, None) of the nearest commit of this run's master chain whose job reported it, else
    (None, {}, the last lookup error other than a missing report)."""
    error = None
    for i, sha in enumerate((info.get_kv_data("master_track_commits_sha") or [])[:MASTER_COMMITS_TO_TRY]):
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
                    behind = f", {i} commits before this run's master parent" if i else ""
                    return f"{sha[:12]}{behind}", result["ext"]["relocation_headroom"], None
        except Exception as e:
            print(f"No relocation headroom of master {sha}: {e}")
            # The bucket is not listable, so S3 answers 403 for a missing report.
            if not (isinstance(e, urllib.error.HTTPError) and e.code in (403, 404)):
                error = e
    return None, {}, error


def check(build_dir, info):
    try:
        measured = {binary: headroom(f"{build_dir}/{binary}") for binary in BINARIES}
    except Exception as e:
        return Result(name=NAME, status=Result.Status.ERROR, info=f"Failed to read the section headers: {e}")
    base_commit, base, error = master_baseline(info)
    lines = [
        "MiB left of the 2 GiB reach of 32-bit PC-relative references"
        + (f", change vs master {base_commit}" if base_commit else ", no master baseline")
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
    lines.append(f"Lowest: {lowest / MIB:.2f} MiB ({binary}, {name}); warns below {WARN_HEADROOM_MIB} MiB")
    if lowest < WARN_HEADROOM_MIB * MIB:
        info.add_workflow_warning(
            f"The relocation headroom of {binary} ({name}) is {lowest / MIB:.2f} MiB, below {WARN_HEADROOM_MIB} MiB:"
            " shrink the code or the data before the link fails, see #126314"
        )
    status, allowed = Result.Status.OK, False
    if decreases and max(decreases)[0] > MAX_DECREASE_MIB * MIB:
        decrease, binary, name = max(decreases)
        lines.append(f"{binary} ({name}) decreased by {decrease / MIB:.2f} MiB, more than {MAX_DECREASE_MIB} MiB")
        if info.pr_number > 0:
            allowed = ALLOW_DECREASE_LABEL in info.pr_labels
            status = Result.Status.OK if allowed else Result.Status.FAIL
            lines.append(
                f"Allowed by the `{ALLOW_DECREASE_LABEL}` label"
                if allowed
                else f"If this is expected, add the `{ALLOW_DECREASE_LABEL}` label and push or re-run all jobs"
            )
    if info.pr_number > 0 and error:
        status = Result.Status.ERROR
        lines.append(f"Failed to read the master baseline, re-run the job: {error}")
    text = "\n".join(lines)
    print(text)
    ext = {"relocation_headroom": measured}
    # The label is not a build digest input, so a later commit must not reuse a success that only the label allowed.
    if allowed:
        ext["do_not_cache"] = True
    return Result(name=NAME, status=status, info=text, ext=ext)


if __name__ == "__main__":
    for path in sys.argv[1:]:
        print(path, {name: round(value / MIB, 2) for name, value in headroom(path).items()})
