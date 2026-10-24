#!/usr/bin/env python3
"""Bounded Rust import observations from the original CPP DWYU outputs.

No Rust import is compilable CPP. A symbol observation means that this archive
defines an undefined symbol of a CPP object; it does not prove link extraction.
"""

import argparse
from collections import defaultdict
import json
from pathlib import Path
import re


FIELDS = {
    "public_includes_without_dep": dict, "private_includes_without_dep": dict,
    "unused_deps": list, "unused_implementation_deps": list,
    "deps_which_should_be_private": list,
}
HEX = re.compile(r"[0-9a-fA-F]+\Z")
MAX_OBJECTS = 128
MAX_BYTES = 262144


def symbol_record(line):
    fields = line.split()
    if len(fields) != 4 or len(fields[1]) != 1:
        raise ValueError("malformed llvm-nm symbol fields")
    symbol, kind, address, size = fields
    if not (HEX.fullmatch(address) or address in ("--------", "----------------")) or not HEX.fullmatch(size):
        raise ValueError("malformed llvm-nm symbol value")
    return symbol, kind


def records(path):
    with Path(path).open(encoding="utf-8", errors="strict") as stream:
        for line in stream:
            owner, sep, record = line.rstrip("\n").partition(": ")
            if not owner or not sep:
                raise ValueError("malformed llvm-nm owner")
            yield owner, *symbol_record(record)


def link_records(data, route, owner):
    if set(route) != {"target", "records"} or route["target"] != owner:
        raise ValueError("DWYU route differs from expected configured owner")
    index = route["records"]
    if (not isinstance(index, list) or len(set(index)) != len(index) or
            any(type(i) is not int or i < 0 or i >= len(data["inventory"]) for i in index)):
        raise ValueError("invalid or duplicated DWYU link inventory")
    return [data["inventory"][i] for i in index if data["inventory"][i]["owner"] == owner]


def inspect(data, consumer, imported):
    if (set(data) != {"owner", "deps", "implementation_deps", "inventory", "objects", "symbols", "archives"} or
            not isinstance(data["inventory"], list) or not isinstance(data["archives"], dict)):
        raise ValueError("incomplete original DWYU symbol manifest")
    own = link_records(data, data["owner"], consumer)
    if not own:
        raise ValueError("CPP caller owns no original DWYU object inputs")
    objects = {obj for record in own for lib in record["libraries"] for obj in lib["objects"]}
    provided = data["objects"]
    if (not objects or not isinstance(provided, list) or len(set(provided)) != len(provided) or
            not objects <= set(provided)):
        raise ValueError("CPP caller objects not owned original nm inputs")
    candidates = [dep for dep in data["deps"] + data["implementation_deps"] if dep["target"] == imported]
    if len(candidates) != 1:
        raise ValueError("import lacks one direct CPP compilation dependency")
    chosen = link_records(data, candidates[0], imported)
    if len(chosen) != 1:
        raise ValueError("import lacks one owned linker input")
    entry = chosen[0]
    if (entry["flags"] or entry["inputs"] or entry["linkstamps"] or len(entry["libraries"]) != 1):
        raise ValueError("import linker input has flags, stamps or extra libraries")
    lib = entry["libraries"][0]
    if (set(lib) != {"objects", "object_owners", "unchecked_objects", "static", "pic_static", "dynamic", "interface", "alwayslink"} or
            not isinstance(lib["object_owners"], dict) or lib["object_owners"] or
            lib["objects"] or lib["unchecked_objects"] or lib["dynamic"] or lib["interface"] or lib["alwayslink"]):
        raise ValueError("Rust import is not a single ordinary non-alwayslink archive")
    archives = [lib[k] for k in ("static", "pic_static") if lib[k]]
    if len(archives) != 1 or archives[0] not in data["archives"]:
        raise ValueError("import lacks one original DwyuArchiveSymbols scan")
    archive = archives[0]
    with Path(archive).open("rb") as stream:
        magic = stream.read(8)
        if magic != b"!<arch>\n":
            raise ValueError("Rust archive is absent, thin or unsupported")
    nm = data["archives"][archive]
    if (set(nm) != {"symbols", "status", "magic"} or Path(nm["status"]).read_bytes() != b"0\n"
            or Path(nm["magic"]).read_bytes() != magic):
        raise ValueError("original DwyuArchiveSymbols did not succeed")
    definitions = defaultdict(set)
    ambiguous = set()
    for filename, symbol, kind in records(nm["symbols"]):
        suffix = filename[len(archive):] if filename.startswith(archive) else ""
        if (len(suffix) < 3 or (suffix[0], suffix[-1]) not in (("[", "]"), ("(", ")")) or
                ":" in suffix[1:-1] or not suffix[1:-1]):
            raise ValueError("nm filename does not belong to configured archive member")
        if kind in "ABDGRST":
            definitions[symbol].add(suffix[1:-1])
        elif kind not in "Uwv":
            # Weak, common, unique and unsupported definitions cannot prove
            # which archive member supplies a reference.
            ambiguous.add(symbol)
    undefined = defaultdict(set)
    for filename, symbol, kind in records(data["symbols"]):
        if filename not in provided:
            raise ValueError("nm read an unconfigured object")
        if filename in objects and kind == "U":
            undefined[filename].add(symbol)
    matches = {}
    for obj in sorted(undefined):
        for symbol in sorted(undefined[obj]):
            members = definitions.get(symbol, ())
            if len(members) == 1 and symbol not in ambiguous:
                matches[obj] = {"symbol": symbol, "member": next(iter(members))}
                break
    limited = len(matches) > MAX_OBJECTS
    return {
        "archive": archive, "archive_scan": nm, "owned_objects": len(objects),
        "object_matches": dict(list(matches.items())[:MAX_OBJECTS]),
        "limited": limited,
        "symbol_scope": "CPP strong undefined / selected archive single strong member definition; no final-link extraction claim",
    }


def check_headers(owned, report, imported):
    if (set(owned) != {"public", "private", "required", "unchecked_link_inputs", "raw_assembly", "raw_link_candidates"} or
            not isinstance(owned["public"], dict) or not isinstance(owned["private"], dict) or
            not isinstance(owned["required"], list)):
        raise ValueError("incomplete imported header ownership")
    if (report.get("analyzed_target") != imported or
            any(not isinstance(report.get(k), typ) for k, typ in FIELDS.items()) or
            any(not isinstance(report.get(k), list) for k in
                ("unentered_owned_files", "unverified_compilation_deps", "unverified_link_deps"))):
        raise ValueError("incomplete DWYU imported-header report")
    exported = set(owned["public"].values()) | set(owned["private"].values())
    if exported != set(owned["required"]):
        raise ValueError("required headers do not cover every actual import-owned header")
    return {
        "headers_applicable": bool(exported),
        "import_headers": sorted(owned["public"]),
        "findings": {k: report[k] for k in FIELDS},
        "unentered_owned_files": report["unentered_owned_files"],
        "unverified_compilation_deps": report["unverified_compilation_deps"],
        "unverified_link_deps": report["unverified_link_deps"],
    }


def main():
    parser = argparse.ArgumentParser()
    for name in ("consumer", "import", "source", "preprocessed", "compiler-headers",
                 "import-headers", "header-report", "symbols", "output"):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args()
    imported = getattr(args, "import")
    # Missing preprocessing and NM artifacts must fail the action. Never
    # emit successful-looking evidence after a tool error.
    for name in (args.preprocessed, args.compiler_headers):
        if not Path(name).is_file():
            raise ValueError("original CPP preprocessing absent")
    value = {
        "kind": "RUST_IMPORT_CPP_OBSERVATION", "schema": 1,
        "consumer": args.consumer, "import": imported, "source": args.source,
        "preprocessed": args.preprocessed, "compiler_headers": args.compiler_headers,
        "cpp_qualified": False, "final_link_extraction": False,
    }
    value.update(inspect(json.loads(Path(args.symbols).read_bytes()), args.consumer, imported))
    value.update(check_headers(json.loads(Path(args.import_headers).read_bytes()),
                               json.loads(Path(args.header_report).read_bytes()), imported))
    raw = (json.dumps(value, sort_keys=True) + "\n").encode()
    if len(raw) > MAX_BYTES:
        # A large finding list is not clean. Do not truncate it into success.
        raise ValueError("Rust evidence exceeds bounded JSON limit")
    Path(args.output).write_bytes(raw)


if __name__ == "__main__":
    main()
