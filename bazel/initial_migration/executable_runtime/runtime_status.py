"""Execute actual zlib binaries and inspect their ELF preinit entries on Linux."""
import argparse
import hashlib
import json
import platform
from pathlib import Path
import struct
import subprocess

MACHINES = {"x86_64": 62, "aarch64": 183}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def string(table, offset):
    end = table.find(b"\0", offset)
    if end < 0:
        raise ValueError("ELF string table has an unterminated string")
    return table[offset:end].decode("utf-8", errors="strict")


def inspect_elf(path, arch):
    data = path.read_bytes()
    if data[:6] != b"\x7fELF\x02\x01":
        raise ValueError("%s is not little-endian ELF64" % path)
    header = struct.unpack_from("<16sHHIQQQIHHHHHH", data)
    _, elf_type, machine, _, _, _, section_offset, _, _, _, _, section_size, section_count, names_index = header
    if elf_type not in (2, 3) or machine != MACHINES[arch] or section_size != 64:
        raise ValueError("wrong ELF target/type/section layout: %s %s" % (path, header))
    if not section_count or names_index >= section_count:
        raise ValueError("ELF section table absent or extended indexes unsupported: " + str(path))
    sections = [struct.unpack_from("<IIQQQQIIQQ", data, section_offset + i * section_size)
                for i in range(section_count)]

    def section_bytes(section):
        raw = data[section[4]:section[4] + section[5]]
        if len(raw) != section[5]:
            raise ValueError("ELF section extends beyond file: " + str(path))
        return raw

    section_names = section_bytes(sections[names_index])
    preinits = [(i, s) for i, s in enumerate(sections)
                if string(section_names, s[0]) == ".preinit_array"]
    if len(preinits) != 1:
        raise ValueError("expected one .preinit_array: %s %s" % (path, preinits))
    pre_index, pre = preinits[0]
    if pre[1] != 16 or not pre[5] or pre[5] % 8:
        raise ValueError("invalid SHT_PREINIT_ARRAY: %s %s" % (path, pre))

    def symbols(table):
        if table[1] not in (2, 11) or table[9] != 24 or table[5] % 24 or table[6] >= len(sections):
            raise ValueError("invalid ELF64 symbol table: " + str(path))
        names = section_bytes(sections[table[6]])
        rows = []
        for offset in range(0, table[5], table[9]):
            name, info, other, shndx, value, size = struct.unpack_from(
                "<IBBHQQ", data, table[4] + offset)
            rows.append((string(names, name), info, shndx, value))
        return rows

    tables = {i: symbols(s) for i, s in enumerate(sections) if s[1] in (2, 11)}
    init_addresses = {value for rows in tables.values() for name, info, shndx, value in rows
                      if name == "__xray_init" and info & 15 == 2 and shndx != 0}
    if len(init_addresses) != 1:
        raise ValueError("missing or ambiguous defined __xray_init: %s %s" % (path, init_addresses))
    init_address = next(iter(init_addresses))
    pre_data = section_bytes(pre)
    values = {pre[3] + offset: struct.unpack_from("<Q", pre_data, offset)[0]
              for offset in range(0, len(pre_data), 8)}
    evidence = {address: {"value": value, "source": "file"} for address, value in values.items()}
    # For a PIE, .preinit_array entries can be zero until the loader applies
    # ELF64 RELA relocations. Compare link-time virtual addresses after RELA.
    relative, absolute = (8, 1) if machine == 62 else (1027, 257)
    relocated = set()
    for s in sections:
        if s[1] != 4:  # SHT_RELA
            continue
        if s[9] != 24 or s[5] % 24:
            raise ValueError("invalid ELF64 RELA section: " + str(path))
        for offset in range(0, s[5], s[9]):
            address, info, addend = struct.unpack_from("<QQq", data, s[4] + offset)
            if address not in values:
                continue
            if address in relocated:
                raise ValueError("two relocations for one preinit entry: " + str(path))
            relocated.add(address)
            rtype, symbol = info & 0xffffffff, info >> 32
            if rtype == relative and symbol == 0:
                value = addend
            elif rtype == absolute and s[6] in tables and symbol < len(tables[s[6]]):
                name, sym_info, shndx, sym_value = tables[s[6]][symbol]
                if shndx == 0:
                    raise ValueError("preinit absolute relocation references undefined symbol: " + name)
                value = sym_value + addend
            else:
                raise ValueError("unsupported preinit relocation %s for %s" % ((rtype, symbol), path))
            values[address] = value
            evidence[address] = {"value": value, "source": "rela", "relocation": rtype}
    matches = [address for address, value in values.items() if value == init_address]
    if len(matches) != 1:
        raise ValueError("expected exactly one __xray_init .preinit_array pointer: %s %s %s"
                         % (path, init_address, evidence))
    address = matches[0]
    return {"elf_bytes": len(data), "elf_sha256": sha(data), "machine": machine,
            "elf_type": elf_type, "preinit_section_index": pre_index,
            "preinit_bytes": len(pre_data), "preinit_address": address,
            "xray_init_address": init_address, "preinit_entry": evidence[address]}


def has_link_data(record):
    return bool(record["libraries"] or record["flags"] or record["additional_inputs"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--configured", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--test", type=Path, required=True)
    parser.add_argument("--status", type=Path, required=True)
    args = parser.parse_args()
    raw = args.configured.read_bytes()
    cfg = json.loads(raw)
    arch = cfg["arch"]
    cpu = platform.machine()
    if platform.system() != "Linux" or cpu != arch:
        raise ValueError("native executor is %s/%s for requested %s" % (platform.system(), cpu, arch))
    required = [r for r in cfg["required_runtime"] if has_link_data(r)]
    target = [r for r in cfg["target_runtime"] if has_link_data(r)]
    if not required or target != required:
        raise ValueError("target link_extra_libs does not forward the full configured runtime")
    if any(has_link_data(r) for r in cfg["exec_runtime"]):
        raise ValueError("exec link_extra_libs forwarded nonempty linker inputs")
    shard_owners = {r["owner"] for r in required if
                    ":clang_rt_builtins__compile_" in r["owner"] or
                    ":clang_rt_xray__compile_" in r["owner"]}
    if not shard_owners or any(r["owner"] in shard_owners and has_link_data(r)
                               for r in cfg["zlib_library"]):
        raise ValueError("zlib's CcInfo still forwards compiler-rt or XRay libraries")
    result = {"arch": arch, "executor": cpu,
              "configured_sha256": sha(raw), "programs": {},
              "actual_cpp_link_checked": False, "clickhouse_hashes_matched": False}
    for name, path in (("binary", args.binary), ("test", args.test)):
        if str(path) not in cfg["outputs"][name]:
            raise ValueError("executable not in configured target outputs: " + str(path))
        record = inspect_elf(path, arch)
        proc = subprocess.run([str(path.resolve())], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              timeout=90, check=False)
        record.update({"exit_code": proc.returncode,
                       "stdout_bytes": len(proc.stdout), "stdout_sha256": sha(proc.stdout),
                       "stderr_bytes": len(proc.stderr), "stderr_sha256": sha(proc.stderr)})
        if proc.returncode:
            raise ValueError("%s failed zlib round-trip: rc=%s stderr=%s" %
                             (name, proc.returncode, proc.stderr.decode("utf-8", errors="replace")[-4096:]))
        result["programs"][name] = record
    args.status.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")


if __name__ == "__main__":
    main()
