# pylint: disable=redefined-outer-name

"""WebAssembly UDFs must fail close when the configured engine cannot run them.

Two situations are covered here, and neither can be reproduced by a stateless test because
both need the server to be restarted with a different configuration:

* an unsupported `webassembly_udf_engine` value has to be rejected on every build, including
  the ones that cannot build `wasmtime` at all (MemorySanitizer, `arm_v80compat`, `riscv64`,
  and any `ENABLE_RUST=0` build), so a stale configuration is never silently ignored;
* a persisted `CREATE FUNCTION ... LANGUAGE WASM` definition outlives the engine, so calling
  it after the engine is gone has to report that WebAssembly support is unavailable rather
  than that the function is unknown.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/wasm_udf.xml"],
    stay_alive=True,
)

CONFIG_PATH = "/etc/clickhouse-server/config.d/wasm_udf.xml"

# https://github.com/eliben/wasm-wat-samples/blob/125f27fa4bf7fb3c5aab5479136be4596150420a/prime-test/isprime.wat
IS_PRIME_MODULE_BASE64 = (
    "AGFzbQEAAAABBgFgAX8BfwMCAQAHDAEIaXNfcHJpbWUAAApUAVIBAX8gAEECSQRAQQAPCyAAQQJG"
    "BEBBAQ8LIABBAnBBAEYEQEEADwtBAyEBA0ACQCABIABPDQAgACABcEEARgRAQQAPCyABQQJqIQEM"
    "AQsLQQEL"
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def has_wasmtime():
    return (
        node.query(
            "SELECT value FROM system.build_options WHERE name = 'USE_WASMTIME'"
        ).strip()
        == "1"
    )


def test_unknown_engine_is_rejected_at_startup(start_cluster):
    # The engine name is validated on every build, so a configuration left over from the
    # removed `WasmEdge` engine keeps the server down instead of being ignored.
    node.stop_clickhouse()
    node.replace_in_config(
        CONFIG_PATH,
        "<webassembly_udf_engine>wasmtime</webassembly_udf_engine>",
        "<webassembly_udf_engine>wasmedge</webassembly_udf_engine>",
    )

    try:
        node.start_clickhouse(start_wait_sec=120, expected_to_fail=True)
        assert node.get_process_pid("clickhouse") is None
        assert node.contains_in_log(
            "Unknown WebAssembly engine"
        ) or node.contains_in_log(
            "Unknown WebAssembly engine", filename="clickhouse-server.err.log"
        )
    finally:
        node.replace_in_config(
            CONFIG_PATH,
            "<webassembly_udf_engine>wasmedge</webassembly_udf_engine>",
            "<webassembly_udf_engine>wasmtime</webassembly_udf_engine>",
        )
        node.start_clickhouse()
        assert node.get_process_pid("clickhouse") is not None
        node.rotate_logs()


def test_persisted_function_fails_close_without_an_engine(start_cluster):
    if not has_wasmtime():
        pytest.skip("the function cannot be created on a build without an engine")

    node.query("DROP FUNCTION IF EXISTS wasm_is_prime")
    node.query("DELETE FROM system.webassembly_modules WHERE name = 'is_prime_module'")
    node.query(
        f"INSERT INTO system.webassembly_modules (name, code) "
        f"SELECT 'is_prime_module', base64Decode('{IS_PRIME_MODULE_BASE64}')"
    )
    node.query(
        "CREATE FUNCTION wasm_is_prime LANGUAGE WASM ABI ROW_DIRECT "
        "FROM 'is_prime_module' :: 'is_prime' ARGUMENTS (num UInt32) RETURNS UInt32"
    )
    assert node.query("SELECT wasm_is_prime(7 :: UInt32)").strip() == "1"
    assert (
        node.query(
            "SELECT arrayMap(wasm_is_prime, [7 :: UInt32])",
            settings={"enable_analyzer": 1},
        ).strip()
        == "[1]"
    )

    # The definition stays in SQL object storage, so after the restart the name is still known
    # while nothing can run it. Both analyzers have their own resolution path for it.
    node.stop_clickhouse()
    node.replace_in_config(
        CONFIG_PATH,
        "<allow_experimental_webassembly_udf>1</allow_experimental_webassembly_udf>",
        "<allow_experimental_webassembly_udf>0</allow_experimental_webassembly_udf>",
    )

    try:
        node.start_clickhouse()
        assert (
            node.query(
                "SELECT count() FROM system.tables WHERE database = 'system' AND name = 'webassembly_modules'"
            ).strip()
            == "0"
        )
        for enable_analyzer in (0, 1):
            error = node.query_and_get_error(
                "SELECT wasm_is_prime(7 :: UInt32)",
                settings={"enable_analyzer": enable_analyzer},
            )
            assert "SUPPORT_IS_DISABLED" in error, error

        # A bare name passed to a higher-order function is rewritten to a lambda from the
        # stored definition, so it reports the same reason as a direct call rather than
        # degrading into an unknown identifier. The rewrite exists only in the analyzer.
        error = node.query_and_get_error(
            "SELECT arrayMap(wasm_is_prime, [7 :: UInt32])",
            settings={"enable_analyzer": 1},
        )
        assert "SUPPORT_IS_DISABLED" in error, error

        # The stored definition has no runtime registration, but it must stay discoverable:
        # `system.functions` is how the operator finds out which leftovers need removing.
        assert (
            node.query(
                "SELECT origin FROM system.functions WHERE name = 'wasm_is_prime'"
            ).strip()
            == "WasmUserDefined"
        )
        assert "CREATE FUNCTION wasm_is_prime" in node.query(
            "SELECT create_query FROM system.functions WHERE name = 'wasm_is_prime'"
        )

        # Failing close must not lock the definition in: dropping it is what removes the
        # leftover from a server that can no longer run it.
        node.query("DROP FUNCTION wasm_is_prime")
        assert (
            node.query(
                "SELECT count() FROM system.functions WHERE name = 'wasm_is_prime'"
            ).strip()
            == "0"
        )
    finally:
        node.stop_clickhouse()
        node.replace_in_config(
            CONFIG_PATH,
            "<allow_experimental_webassembly_udf>0</allow_experimental_webassembly_udf>",
            "<allow_experimental_webassembly_udf>1</allow_experimental_webassembly_udf>",
        )
        node.start_clickhouse()
        node.query("DROP FUNCTION IF EXISTS wasm_is_prime")
        node.query(
            "DELETE FROM system.webassembly_modules WHERE name = 'is_prime_module'"
        )
