"""First-party headers and generator inputs shared by native targets."""

load(":codegen_rules.bzl", "CODEGEN_RULES")
load(":headers.bzl", "HEADER_DIRS")

def declare_inputs():
    native.exports_files(["cmake/localize_rust_c_symbols.sh", "cmake/strip_rust_symbols.sh"])
    # ClickHouse's sysroot carries newer ARM capability bits than the base
    # kernel headers selected for glibc 2.28 by hermetic-llvm.
    native.alias(
        name = "linux_aarch64_hwcap",
        actual = "@clickhouse_sysroot//:linux_aarch64_hwcap",
    )
    native.filegroup(
        name = "delta_cbindgen_sources",
        srcs = [
            "@clickhouse_delta_kernel_rs//:cbindgen_sources",
            "@clickhouse_rust_vendor//:sources",
            "contrib/corrosion-cmake/config.toml.in",
        ],
    )
    native.filegroup(
        name = "openssl_configured_headers",
        srcs = native.glob(["contrib/openssl-cmake/common/include/**"]),
    )
    for name, source in {"krb5_gen_map": "gen_map", "krb5_gen": "gen"}.items():
        native.alias(
            name = name,
            actual = "@clickhouse_krb5//:" + source,
        )
    native.exports_files(sorted({
        src[3:]: True
        for attrs in CODEGEN_RULES.values()
        for src in attrs["srcs"]
        if src.startswith("//:")
    }))
    for name, directory in HEADER_DIRS.items():
        native.filegroup(
            name = name,
            srcs = native.glob(
                [directory + "/**"],
                exclude = [directory + "/**/BUILD", directory + "/**/BUILD.bazel"],
                allow_empty = True,
            ),
        )

