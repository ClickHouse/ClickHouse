import argparse
import mmap
import re
import struct
from pathlib import Path

from pip._vendor.packaging.version import Version

from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

IMAGE_UBUNTU = "clickhouse/test-old-ubuntu"
IMAGE_CENTOS = "clickhouse/test-old-centos"
DOWNLOAD_RETRIES_COUNT = 5

temp_path = Path(f"{Utils.cwd()}/ci/tmp")


def process_glibc_check():
    # See https://sourceware.org/glibc/wiki/Glibc%20Timeline
    if Utils.is_amd():
        max_glibc_version = "2.4"
    elif Utils.is_arm():
        max_glibc_version = "2.18"  # because of build with newer sysroot?
    else:
        raise RuntimeError("Can't determine max glibc version")

    commands = (
        [
            f"readelf -s --wide {temp_path}/clickhouse | grep '@GLIBC_' > {temp_path}/glibc.log",
            # FIXME: odbc bridge is not present in the deb package
            # f"readelf -s --wide {temp_path}/clickhouse-odbc-bridge | grep '@GLIBC_' >> {temp_path}/glibc.log",
            # FIXME: library bridge is not present in the deb package
            # f"readelf -s --wide {temp_path}/clickhouse-library-bridge | grep '@GLIBC_' >> {temp_path}/glibc.log",
        ],
    )

    for command in commands:
        Shell.check(command, verbose=True, strict=True)

    ok = True
    with open(f"{temp_path}/glibc.log", "r", encoding="utf-8") as log:
        for line in log:
            if line.strip():
                columns = line.strip().split(" ")
                symbol_with_glibc = columns[-2]  # sysconf@GLIBC_2.2.5
                _, version = symbol_with_glibc.split("@GLIBC_")
                if version == "PRIVATE":
                    print(f"FAILED: PRIVATE version: {symbol_with_glibc}")
                    ok = False
                elif Version(version) > Version(max_glibc_version):
                    print(
                        f"FAILED: version is more than max version [{max_glibc_version}]: [{symbol_with_glibc}]"
                    )
                    ok = False
    return ok


def process_pie_check():
    # Nothing in the build fails when the binary comes out position dependent: -fPIE only makes the
    # code relocatable, and whether the executable itself is position independent is the linker's
    # decision, so it is checked on the packaged binary instead.
    header = Shell.get_output(
        f"readelf -h --wide {temp_path}/clickhouse", verbose=True, strict=True
    )
    elf_type = re.search(r"^\s*Type:\s+(\w+)", header, re.MULTILINE)
    if not elf_type:
        print(f"FAILED: no ELF type in the header:\n{header}")
        return False

    ok = True
    if elf_type.group(1) != "DYN":
        print(
            f"FAILED: ELF type is [{elf_type.group(1)}], a position independent executable is [DYN]"
        )
        ok = False

    # A shared library is `DYN` as well, so the type alone does not say that this is a position
    # independent *executable*. `DF_1_PIE` is what the linker sets for `-pie` and nothing else.
    dynamic = Shell.get_output(
        f"readelf -d --wide {temp_path}/clickhouse", verbose=True, strict=True
    )
    flags = re.search(r"^.*\(FLAGS_1\)\s+Flags:(.*)$", dynamic, re.MULTILINE)
    if not flags or "PIE" not in flags.group(1).split():
        print(f"FAILED: DF_1_PIE is not set: [{flags.group(0).strip() if flags else 'no FLAGS_1'}]")
        ok = False

    return ok


def process_runpath_check():
    # Only system libraries are needed, so any library search path points into the build tree.
    dynamic = Shell.get_output(
        f"readelf -d --wide {temp_path}/clickhouse", verbose=True, strict=True
    )
    paths = re.findall(r"^.*\((?:RPATH|RUNPATH)\).*$", dynamic, re.MULTILINE)
    for path in paths:
        print(f"FAILED: [{path.strip()}]")
    return not paths


def process_pauth_check():
    # The binary is built with `-mbranch-protection=pac-ret` for a baseline without `FEAT_PAuth`, so
    # return addresses are signed with `paciasp` and `autiasp`, which are no-ops on older cores. In
    # a function compiled for ARMv8.3 or later, clang folds `autiasp; ret` into `retaa` instead,
    # which is not in the `NOP` space and raises SIGILL on such cores (Graviton2, Ampere Altra), see
    # https://github.com/ClickHouse/ClickHouse/issues/131748. A test run on a newer core would never
    # notice, so look for the instruction itself. Both have a single fixed encoding.
    forbidden = {0xD65F0BFF: "retaa", 0xD65F0FFF: "retab"}

    path = f"{temp_path}/clickhouse"
    with open(path, "rb") as file, mmap.mmap(
        file.fileno(), 0, access=mmap.ACCESS_READ
    ) as data:
        assert data[:4] == b"\x7fELF" and data[4] == 2 and data[5] == 1, "not ELF64 LE"
        shoff = struct.unpack_from("<Q", data, 0x28)[0]
        shentsize, shnum = struct.unpack_from("<HH", data, 0x3A)
        sections = [
            struct.unpack_from("<IIQQQQIIQQ", data, shoff + i * shentsize)
            for i in range(shnum)
        ]

        SHT_PROGBITS, SHF_EXECINSTR = 1, 4
        found = []
        scanned = 0
        for _, sh_type, sh_flags, sh_addr, sh_offset, sh_size, *_ in sections:
            if sh_type != SHT_PROGBITS or not sh_flags & SHF_EXECINSTR:
                continue
            scanned += sh_size
            for word, name in forbidden.items():
                pattern = struct.pack("<I", word)
                pos = data.find(pattern, sh_offset, sh_offset + sh_size)
                while pos != -1:
                    if (pos - sh_offset) % 4 == 0:
                        found.append((sh_addr + pos - sh_offset, name))
                    pos = data.find(pattern, pos + 1, sh_offset + sh_size)
        print(f"Scanned {scanned} bytes of code, found {len(found)} forbidden instructions")
        if not scanned:
            print("FAILED: no executable sections")
            return False
        if not found:
            return True

    # Name the functions, so that it is clear which library needs `-mbranch-protection=none`. The
    # package has no `.symtab`, but `system.symbols` reads the binary's own symbol table, and its
    # offsets are the virtual addresses above because the executable is linked at zero. The probe
    # only explains the failure, so its own errors are printed and do not change the verdict.
    addresses = ",".join(str(address) for address, _ in found)
    functions = Shell.get_output(
        f"{path} local --path {temp_path}/pauth_check_local --query \""
        f"SELECT a, demangle(any(symbol)) FROM system.symbols ARRAY JOIN [{addresses}] AS a"
        f" WHERE address_begin <= a AND a < address_end GROUP BY a FORMAT TSV\"",
        verbose=True,
    )
    names = dict(line.split("\t", 1) for line in functions.splitlines() if "\t" in line)
    for address, name in sorted(found):
        print(f"FAILED: [{name}] at [{address:#x}] in [{names.get(str(address), '<unknown>')}]")
    return False


def parse_args():
    parser = argparse.ArgumentParser("Check compatibility with old distributions")
    parser.add_argument("--check-name", required=False)
    return parser.parse_args()


def main():
    stopwatch = Utils.Stopwatch()

    check_name = Info().job_name
    assert check_name
    check_glibc = True
    # currently hardcoded to x86, don't enable for AARCH64
    check_old_distributions = (
        "aarch64" not in check_name.lower() and "arm" not in check_name.lower()
    )

    for package in temp_path.iterdir():
        if package.suffix == ".deb":
            Shell.check(
                f"dpkg -x {package} {temp_path} && rm {package}",
                verbose=True,
                strict=True,
            )
    Shell.check(
        f"mv {temp_path}/usr/bin/clickhouse {temp_path}/clickhouse",
        verbose=True,
        strict=True,
    )
    # Shell.check(f"chmod +x {temp_path}/clickhouse", verbose=True, strict=True)

    test_results = []

    if check_glibc:
        test_results.append(
            Result.from_commands_run(
                name="glibc version",
                command=process_glibc_check,
            )
        )

    test_results.append(
        Result.from_commands_run(
            name="position independent executable",
            command=process_pie_check,
        )
    )

    test_results.append(
        Result.from_commands_run(
            name="no RPATH or RUNPATH",
            command=process_runpath_check,
        )
    )

    if Utils.is_arm():
        test_results.append(
            Result.from_commands_run(
                name="no instructions that need FEAT_PAuth",
                command=process_pauth_check,
            )
        )

    if check_old_distributions:
        test_results.append(
            Result.from_commands_run(
                name="ubuntu12",
                command=[
                    f"docker run --volume={temp_path}/clickhouse:/clickhouse ubuntu:12.04 /clickhouse local --tmp --query 'select 1'",
                ],
                with_info=True,
            )
        )
        test_results.append(
            Result.from_commands_run(
                name="ubuntu12 (rseq unavailable)",
                command=[f"""
                docker run --volume={temp_path}/clickhouse:/clickhouse ubuntu:12.04 /clickhouse local --query "select throwIf(not(count())) from system.warnings where message like '%rseq%'"
                """.strip()],
                with_info=True,
            )
        )
        test_results.append(
            Result.from_commands_run(
                name="centos5",
                command=[
                    f"docker run --volume={temp_path}/clickhouse:/clickhouse centos:5 /clickhouse local --tmp --query 'select 1'"
                ],
                with_info=True,
            )
        )

    test_results.append(
        Result.from_commands_run(
            name="ubuntu22 (rseq available)",
            command=[f"""
            docker run --volume={temp_path}/clickhouse:/clickhouse ubuntu:22.04 /clickhouse local --query "select throwIf(count()) from system.warnings where message like '%rseq%'"
            """.strip()],
            with_info=True,
        )
    )
    test_results.append(
        Result.from_commands_run(
            name="ubuntu22 (rseq off)",
            command=[f"""
            docker run --volume={temp_path}/clickhouse:/clickhouse -e GLIBC_TUNABLES=glibc.pthread.rseq=0 ubuntu:22.04 /clickhouse local --query "select throwIf(not(count())) from system.warnings where message like '%rseq%'"
            """.strip()],
            with_info=True,
        )
    )

    Result.create_from(results=test_results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
