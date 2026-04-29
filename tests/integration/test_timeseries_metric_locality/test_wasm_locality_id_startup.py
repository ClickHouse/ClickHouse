"""Regression: builtin SQL UDF registration at startup must not replace a same-name WASM UDF."""

import pytest

from helpers.cluster import ClickHouseCluster

# https://github.com/eliben/wasm-wat-samples/blob/125f27fa4bf7fb3c5aab5479136be4596150420a/prime-test/isprime.wat
_ISPRIME_WASM_BASE64 = (
    "AGFzbQEAAAABBgFgAX8BfwMCAQAHDAEIaXNfcHJpbWUAAApUAVIBAX8gAEECSQRAQQAPCyAAQQJG"
    "BEBBAQ8LIABBAnBBAEYEQEEADwtBAyEBA0ACQCABIABPDQAgACABcEEARgRAQQAPCyABQQJqIQEM"
    "AQsLQQEL"
)

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/wasm_udf.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.shutdown()


def test_startup_does_not_replace_wasm_time_series_metric_locality_id(start_cluster):
    if node.is_built_with_memory_sanitizer():
        pytest.skip("WebAssembly UDF is disabled in MSAN builds")

    engines = node.query(
        "SELECT name, value FROM system.build_options WHERE name IN ('USE_WASMTIME', 'USE_WASMEDGE') "
        "ORDER BY name FORMAT TabSeparated"
    )
    if "USE_WASMEDGE\t1" not in engines and "USE_WASMTIME\t1" not in engines:
        pytest.skip("Neither Wasmtime nor WasmEdge is enabled in this ClickHouse build")

    node.query("DROP FUNCTION IF EXISTS timeSeriesMetricLocalityId")
    node.query("DELETE FROM system.webassembly_modules WHERE name = 'ts_mlid_wasm'")
    node.query(
        "INSERT INTO system.webassembly_modules (name, code) "
        f"SELECT 'ts_mlid_wasm', base64Decode('{_ISPRIME_WASM_BASE64}')"
    )
    node.query(
        "CREATE FUNCTION timeSeriesMetricLocalityId LANGUAGE WASM ABI ROW_DIRECT "
        "FROM 'ts_mlid_wasm' :: 'is_prime' ARGUMENTS (num UInt32) RETURNS UInt32"
    )
    create_before = node.query(
        "SELECT create_query FROM system.functions WHERE name = 'timeSeriesMetricLocalityId'"
    )
    assert "WASM" in create_before

    node.restart_clickhouse()

    create_after = node.query(
        "SELECT create_query FROM system.functions WHERE name = 'timeSeriesMetricLocalityId'"
    )
    assert "WASM" in create_after
    assert node.query("SELECT timeSeriesMetricLocalityId(7 :: UInt32)").strip() == "1"

    node.query("DROP FUNCTION IF EXISTS timeSeriesMetricLocalityId")
    node.query("DELETE FROM system.webassembly_modules WHERE name = 'ts_mlid_wasm'")
