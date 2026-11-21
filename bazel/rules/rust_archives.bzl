"""Rust Cargo packages and the CMake archives consumed by ClickHouse."""

# Keep this assignment literal: bazel/initial_migration/rust_archives.py reads the same inventory.
RUST_ARCHIVES = {
    "chdig": {
        "archive": "contrib/chdig-cmake/libchdig.a",
        "package": "chdig",
        "features": ["tls"],
        "native_compilation": True,
        "manifest": "@clickhouse_chdig//:cargo_manifest",
        "repositories": {
            "@clickhouse_chdig//:cmake_cargo_sources": "contrib/chdig",
        },
        "rules_rs": ":chdig",
    },
    "delta_kernel_ffi": {
        "archive": "contrib/delta-kernel-rs-cmake/libdelta_kernel_ffi.a",
        "package": "delta_kernel_ffi",
        "features": ["default-engine-native-tls", "test-ffi", "tracing", "arrow"],
        "native_compilation": True,
        "manifest": "@clickhouse_delta_kernel_rs//:ffi/Cargo.toml",
        "repositories": {
            "@clickhouse_delta_kernel_rs//:cbindgen_sources": "contrib/delta-kernel-rs",
        },
        "rules_rs": ":delta_kernel_ffi",
    },
    "skim": {
        "archive": "rust/workspace/lib_ch_rust_skim_rust.a",
        "package": "_ch_rust_skim_rust",
        "native_compilation": True,
    },
    "prql": {
        "archive": "rust/workspace/lib_ch_rust_prql.a",
        "package": "_ch_rust_prql",
        "keep_symbols": ["prql_to_sql", "prql_free_pointer"],
    },
    "polyglot": {
        "archive": "rust/workspace/lib_ch_rust_polyglot.a",
        "package": "_ch_rust_polyglot",
        "keep_symbols": ["polyglot_transpile", "polyglot_free_pointer"],
    },
    "pco": {
        "archive": "rust/workspace/lib_ch_rust_pco.a",
        "package": "_ch_rust_pco",
        "path_crates": ["pco", "dtype_dispatch", "better_io"],
        "keep_symbols": ["pco_compress", "pco_decompress"],
    },
    "wasmtime": {
        "archive": "rust/workspace/lib_ch_rust_wasmtime.a",
        "package": "_ch_rust_wasmtime",
        "native_compilation": True,
    },
}
