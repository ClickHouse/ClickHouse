"""Temporary root targets used before first_party_packages.py lays out BUILD files."""

load("//bazel/rules:codegen_rules.bzl", "CODEGEN_RULES")

def declare_inputs():
    native.exports_files(["cmake/localize_rust_c_symbols.sh", "cmake/strip_rust_symbols.sh"])

    native.filegroup(
        name = "cmake_cargo_config",
        srcs = ["contrib/corrosion-cmake/config.toml.in"],
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
