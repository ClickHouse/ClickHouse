"""Compile definitions of the ClickHouse program libraries that depend on dbms."""

load("@clickhouse//src:common_io_flags.bzl", "DBMS_INTERFACE_AARCH64_DEFINES", "DBMS_INTERFACE_X86_64_DEFINES")

CLICKHOUSE_PROGRAM_DEFINES = select({
    "@platforms//cpu:x86_64": DBMS_INTERFACE_X86_64_DEFINES,
    "@platforms//cpu:aarch64": DBMS_INTERFACE_AARCH64_DEFINES,
})

_COMPRESSION_DEFINES = [
    "SZ_DYNAMIC_DISPATCH=1",
    "XXH_INLINE_ALL",
]

CLICKHOUSE_PROGRAM_COMPRESSION_DEFINES = select({
    "@platforms//cpu:x86_64": sorted(DBMS_INTERFACE_X86_64_DEFINES + _COMPRESSION_DEFINES),
    "@platforms//cpu:aarch64": sorted(DBMS_INTERFACE_AARCH64_DEFINES + _COMPRESSION_DEFINES),
})
