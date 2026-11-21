#!/usr/bin/env python3
"""Hash ELF64 sections from both ClickHouse executables for mismatch diagnosis."""

import argparse
import hashlib
import json
import mmap
from pathlib import Path
import struct


def sections(path):
    with path.open("rb") as source, mmap.mmap(source.fileno(), 0, access=mmap.ACCESS_READ) as data:
        if data[:6] != b"\x7fELF\x02\x01":
            raise ValueError(f"{path}: expected little-endian ELF64")
        header = struct.unpack_from("<HHIQQQIHHHHHH", data, 16)
        offset, size, count, names_index = header[5], header[10], header[11], header[12]
        if size != struct.calcsize("<IIQQQQIIQQ") or not offset:
            raise ValueError(f"{path}: invalid ELF section table")

        def section(index):
            start = offset + index * size
            if start < 0 or start + size > len(data):
                raise ValueError(f"{path}: section {index} outside file")
            return struct.unpack_from("<IIQQQQIIQQ", data, start)

        if not count:
            count = section(0)[5]
        if names_index == 0xffff:
            names_index = section(0)[6]
        if names_index >= count:
            raise ValueError(f"{path}: invalid section-name table")
        names = section(names_index)
        names_start, names_end = names[4], names[4] + names[5]
        if names_end > len(data):
            raise ValueError(f"{path}: section-name table outside file")
        result = []
        for index in range(1, count):
            name_at, kind, flags, address, start, length, link, info, align, entsize = section(index)
            name_start = names_start + name_at
            name_end = data.find(b"\0", name_start, names_end)
            if name_start >= names_end or name_end < 0:
                raise ValueError(f"{path}: section {index} has invalid name")
            name = data[name_start:name_end].decode("utf-8", errors="strict")
            digest = None
            if kind != 8:  # SHT_NOBITS has no bytes in the file.
                if start + length > len(data):
                    raise ValueError(f"{path}: section {index} {name} outside file")
                value = hashlib.sha256()
                for pos in range(start, start + length, 1024 * 1024):
                    value.update(data[pos:min(pos + 1024 * 1024, start + length)])
                digest = value.hexdigest()
            result.append({
                "index": index, "name": name, "type": kind, "flags": flags,
                "address": address, "offset": start, "size": length, "link": link,
                "info": info, "alignment": align, "entry_size": entsize, "sha256": digest,
            })
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cmake", type=Path)
    parser.add_argument("bazel", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    cmake, bazel = sections(args.cmake), sections(args.bazel)
    # Keep complete ordered lists, including duplicate names, in the report.
    # The index comparison reports an ordering difference without pairing the
    # wrong duplicate. 'cmp' is still required to check non-section file bytes.
    differences = []
    for index in range(max(len(cmake), len(bazel))):
        reference = cmake[index] if index < len(cmake) else None
        actual = bazel[index] if index < len(bazel) else None
        if reference != actual:
            differences.append({
                "index": index + 1,
                "cmake": reference,
                "bazel": actual,
            })
    report = {
        "cmake_sections": cmake, "bazel_sections": bazel,
        "different_sections": len(differences), "differences": differences,
        "binary_equality_verified": False,
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"PARITY_ELF_SECTIONS=cmake:{len(cmake)} bazel:{len(bazel)} differing:{len(differences)}")
    native_names = {s["name"] for s in cmake}
    bazel_names = {s["name"] for s in bazel}
    for name in sorted(native_names - bazel_names):
        print(f"PARITY_ELF_ONLY_CMAKE={name}")
    for name in sorted(bazel_names - native_names):
        print(f"PARITY_ELF_ONLY_BAZEL={name}")
    # Do not pair duplicated section names. The full ordered report above
    # retains them; here log only the unambiguous size and content differences.
    for name in (".text", ".rodata", ".eh_frame", ".gcc_except_table",
                 ".data.rel.ro", ".data", ".bss", ".symtab", ".strtab"):
        native = [s for s in cmake if s["name"] == name]
        actual = [s for s in bazel if s["name"] == name]
        if len(native) == len(actual) == 1:
            size_delta = actual[0]["size"] - native[0]["size"]
            same_bytes = actual[0]["sha256"] == native[0]["sha256"]
            print(f"PARITY_ELF_SECTION={name} cmake:{native[0]['size']} "
                  f"bazel:{actual[0]['size']} delta:{size_delta} contents_equal:{same_bytes}")


if __name__ == "__main__":
    main()
