#!/usr/bin/env python3
"""Compare executable function symbols without treating ICF aliases as bytes.

Run on the original CMake and Bazel ELFs, before llvm-strip --strip-all.
Writes a bounded JSON (or .json.gz) report; exact file equality is the job of
cmp, not this diagnostic. Address shifts alone do not imply different code.
"""

import argparse
from collections import Counter, defaultdict
import gzip
import json
import mmap
from pathlib import Path
import struct

from compare_elf_sections import sections


SYMBOL = struct.Struct("<IBBHQQ")  # Elf64_Sym: name, info, other, shndx, value, size
STT_FILE = 4
FUNCTION_TYPES = {2, 10}  # STT_FUNC, STT_GNU_IFUNC
SHF_EXECINSTR = 4


def symbol_key(key):
    section, name, source, binding, visibility, kind = key
    return {
        "section": section, "name": name, "file": source or None,
        "binding": binding, "visibility": visibility, "type": kind,
    }


def read_symbols(path):
    table = sections(path)
    by_index = {s["index"]: s for s in table}
    symtabs = [s for s in table if s["type"] == 2]  # SHT_SYMTAB
    if not symtabs:
        raise ValueError(
            f"{path}: missing .symtab; use the original executable before llvm-strip --strip-all"
        )
    if len(symtabs) != 1:
        raise ValueError(f"{path}: expected one SHT_SYMTAB, found {len(symtabs)}")
    symtab = symtabs[0]
    strings = by_index.get(symtab["link"])
    if not strings or strings["type"] != 3:
        raise ValueError(f"{path}: .symtab sh_link does not select SHT_STRTAB")
    if symtab["entry_size"] != SYMBOL.size or symtab["size"] % SYMBOL.size:
        raise ValueError(f"{path}: malformed Elf64_Sym entries")
    executable = {s["index"]: s for s in table if s["flags"] & SHF_EXECINSTR}
    # A list for every key preserves duplicate local and weak names. Never
    # infer that two same-named local functions from one STT_FILE are unique.
    functions = defaultdict(list)
    in_section = defaultdict(list)
    counts = Counter()
    file_names = Counter()
    current_file = ""
    with path.open("rb") as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as data:
        start, end = strings["offset"], strings["offset"] + strings["size"]

        def name_at(offset):
            pos = start + offset
            stop = data.find(b"\0", pos, end)
            if pos >= end or stop < 0:
                raise ValueError(f"{path}: invalid .strtab offset {offset}")
            return data[pos:stop].decode("utf-8", errors="surrogateescape")

        for offset in range(symtab["offset"], symtab["offset"] + symtab["size"], SYMBOL.size):
            index = (offset - symtab["offset"]) // SYMBOL.size
            name, info, other, shndx, value, size = SYMBOL.unpack_from(data, offset)
            binding, kind = info >> 4, info & 15
            counts["symtab_entries"] += 1
            if kind == STT_FILE:
                # ELF places each object's STT_FILE ahead of its STB_LOCAL
                # symbols. Globals must not inherit the last local's file.
                if binding != 0:
                    raise ValueError(f"{path}: STT_FILE #{index} is not STB_LOCAL")
                current_file = name_at(name)
                file_names[current_file] += 1
                continue
            section = executable.get(shndx)
            if section is None:
                continue
            if kind not in FUNCTION_TYPES:
                counts["other_symbols_in_exec"] += 1
                continue
            if not size:
                counts["zero_size_functions"] += 1
                continue
            relative = value - section["address"]
            if relative < 0 or relative + size > section["size"]:
                raise ValueError(f"{path}: function #{index} is outside {section['name']}")
            key = (
                section["name"], name_at(name), current_file if binding == 0 else "",
                binding, other & 3, kind,
            )
            entry = (relative, size, index)
            functions[key].append(entry)
            in_section[shndx].append((relative, size, index, key))
            counts["sized_functions"] += 1
    counts["file_entries"] = sum(file_names.values())
    counts["distinct_file_names"] = len(file_names)
    counts["duplicate_file_names"] = sum(n > 1 for n in file_names.values())
    counts["function_keys"] = len(functions)
    counts["duplicate_function_keys"] = sum(len(items) > 1 for items in functions.values())
    return {
        "sections": executable, "functions": functions, "in_section": in_section,
        "counts": dict(counts),
    }


def entry_sample(entries, limit):
    return [
        {"section_offset": pos, "size": size, "symtab_index": index}
        for pos, size, index in entries[:limit]
    ]


def section_measurement(section, entries):
    # Same address/size includes weak aliases and functions folded by
    # -Wl,--icf=safe. Range union also handles overlapping unequal sizes.
    groups = defaultdict(list)
    occupied = 0
    end = 0
    for pos, size, index, key in sorted(entries):
        occupied += max(0, pos + size - max(pos, end))
        end = max(end, pos + size)
        groups[(pos, size)].append(key)
    alias_counts = Counter()
    ordered = []
    for (pos, size), keys in sorted(groups.items()):
        names = tuple(sorted(keys))
        ordered.append((pos, size, names))
        if len(names) > 1:
            alias_counts[names] += 1
    return {
        "summary": {
            "section_size": section["size"], "sized_function_symbols": len(entries),
            "distinct_address_size_ranges": len(groups), "function_range_union_bytes": occupied,
            "uncovered_bytes": section["size"] - occupied,
            "alias_group_count": sum(alias_counts.values()),
        },
        "ordered": ordered, "aliases": alias_counts,
    }


def describe_group(group, limit):
    pos, size, keys = group
    return {
        "section_offset": pos, "size": size, "symbol_count": len(keys),
        "symbols": [symbol_key(k) for k in keys[:limit]],
    }


def compare(cmake, bazel, limit):
    cf, bf = cmake["functions"], bazel["functions"]
    ck, bk = cf.keys(), bf.keys()
    common = ck & bk

    def only_samples(keys, functions):
        ranked = sorted(keys, key=lambda k: max(e[1] for e in functions[k]), reverse=True)
        return [
            {"key": symbol_key(k), "occurrences": len(functions[k]),
             "entries": entry_sample(functions[k], 5)}
            for k in ranked[:limit]
        ]

    changed_sizes = []
    duplicate_differences = []
    same_sizes = 0
    for key in common:
        cs = sorted(item[1] for item in cf[key])
        bs = sorted(item[1] for item in bf[key])
        if cs == bs:
            same_sizes += 1
        else:
            # These are symbol sizes, not a contribution to .text size:
            # union-of-ranges in the section report is the byte accounting.
            score = max(abs(sum(bs) - sum(cs)), abs(max(bs) - max(cs)))
            changed_sizes.append((score, key))
            if len(cs) != 1 or len(bs) != 1:
                duplicate_differences.append(key)
    changed_sizes.sort(key=lambda item: item[0], reverse=True)

    section_reports = {}
    c_sections = {s["name"]: (i, s) for i, s in cmake["sections"].items()}
    b_sections = {s["name"]: (i, s) for i, s in bazel["sections"].items()}
    if len(c_sections) != len(cmake["sections"]) or len(b_sections) != len(bazel["sections"]):
        raise ValueError("duplicate executable section names; cannot pair sections by name")
    for name in sorted(c_sections.keys() | b_sections.keys()):
        sides = []
        for data, by_name in ((cmake, c_sections), (bazel, b_sections)):
            if name not in by_name:
                sides.append(None)
                continue
            index, section = by_name[name]
            sides.append(section_measurement(section, data["in_section"].get(index, [])))
        c, b = sides
        item = {"cmake": c["summary"] if c else None, "bazel": b["summary"] if b else None}
        if c and b:
            item["section_size_delta"] = b["summary"]["section_size"] - c["summary"]["section_size"]
            item["function_range_union_delta"] = (
                b["summary"]["function_range_union_bytes"] - c["summary"]["function_range_union_bytes"]
            )
            item["uncovered_delta"] = b["summary"]["uncovered_bytes"] - c["summary"]["uncovered_bytes"]
            # Ignore moved addresses and size changes here; compare which
            # function/alias groups appear in each position in the section.
            co, bo = c["ordered"], b["ordered"]
            first = None
            for index in range(max(len(co), len(bo))):
                if index >= len(co) or index >= len(bo) or co[index][2] != bo[index][2]:
                    first = index
                    break
            item["first_order_divergence"] = None if first is None else {
                "group_index": first,
                "cmake_context": [describe_group(g, 8) for g in co[max(0, first - 2):first + 3]],
                "bazel_context": [describe_group(g, 8) for g in bo[max(0, first - 2):first + 3]],
            }
            # Size-different alias groups still match by their member names;
            # the size report identifies their size changes separately.
            only_c = c["aliases"] - b["aliases"]
            only_b = b["aliases"] - c["aliases"]
            item["alias_groups_only_cmake"] = sum(only_c.values())
            item["alias_groups_only_bazel"] = sum(only_b.values())
            for side, aliases in (("cmake", only_c), ("bazel", only_b)):
                item["alias_samples_only_" + side] = [
                    {"occurrences": number, "symbol_count": len(keys),
                     "symbols": [symbol_key(k) for k in keys[:8]]}
                    for keys, number in aliases.most_common(limit)
                ]
        section_reports[name] = item
    return {
        "note": (
            "Only the originals contain .symtab. This diagnoses executable ranges; "
            "whole-file or stripped-binary equality requires sha256sum and cmp. "
            "Symbol sizes include aliases and must not be summed as section contributions."
        ),
        "cmake_counts": cmake["counts"], "bazel_counts": bazel["counts"],
        "only_cmake_count": len(ck - bk), "only_bazel_count": len(bk - ck),
        "common_key_count": len(common), "equal_size_multisets": same_sizes,
        "different_size_multisets": len(changed_sizes),
        "different_sizes_with_duplicate_keys": len(duplicate_differences),
        "only_cmake": only_samples(ck - bk, cf),
        "only_bazel": only_samples(bk - ck, bf),
        "different_sizes": [
            {"key": symbol_key(k), "cmake_occurrences": len(cf[k]), "bazel_occurrences": len(bf[k]),
             "cmake": entry_sample(cf[k], 8), "bazel": entry_sample(bf[k], 8)}
            for _, k in changed_sizes[:limit]
        ],
        "executable_sections": section_reports,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("cmake", type=Path)
    parser.add_argument("bazel", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--limit", type=int, default=80, help="Maximum samples in each report category")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    try:
        result = compare(read_symbols(args.cmake), read_symbols(args.bazel), args.limit)
    except (OSError, ValueError, struct.error) as exc:
        parser.exit(2, f"ELF_SYMBOL_DIAGNOSTIC_FAILED={exc}\n")
    opener = gzip.open if args.output.suffix == ".gz" else open
    with opener(args.output, "wt", encoding="utf-8") as output:
        json.dump(result, output, indent=2)
        output.write("\n")
    print(
        "PARITY_ELF_FUNCTIONS="
        f"cmake:{result['cmake_counts']['sized_functions']} "
        f"bazel:{result['bazel_counts']['sized_functions']} "
        f"only_cmake:{result['only_cmake_count']} only_bazel:{result['only_bazel_count']} "
        f"size_differences:{result['different_size_multisets']}"
    )
    for name, item in result["executable_sections"].items():
        if item["cmake"] and item["bazel"]:
            print(
                f"PARITY_ELF_FUNCTION_SECTION={name} delta:{item['section_size_delta']} "
                f"range_union_delta:{item['function_range_union_delta']} "
                f"uncovered_delta:{item['uncovered_delta']} "
                f"only_cmake_aliases:{item['alias_groups_only_cmake']} "
                f"only_bazel_aliases:{item['alias_groups_only_bazel']} "
                f"order_equal:{item['first_order_divergence'] is None}"
            )


if __name__ == "__main__":
    main()
