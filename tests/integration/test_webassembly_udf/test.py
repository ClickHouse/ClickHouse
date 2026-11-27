import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Instance without allow_experimental_webassembly_udf (default is disabled)
node0 = cluster.add_instance("node0")

# Instance with allow_experimental_webassembly_udf enabled
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/wasm_udf.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# A small WebAssembly modules encoded in base64

# https://github.com/eliben/wasm-wat-samples/blob/125f27fa4bf7fb3c5aab5479136be4596150420a/prime-test/isprime.wat
ISPRIME_WASM_BASE64 = (
    "AGFzbQEAAAABBgFgAX8BfwMCAQAHDAEIaXNfcHJpbWUAAApUAVIBAX8gAEECSQRAQQAPCyAAQQJG"
    "BEBBAQ8LIABBAnBBAEYEQEEADwtBAyEBA0ACQCABIABPDQAgACABcEEARgRAQQAPCyABQQJqIQEM"
    "AQsLQQEL"
)

# https://github.com/eliben/wasm-wat-samples/blob/125f27fa4bf7fb3c5aab5479136be4596150420a/select/select.wat
ADD_OR_SUB_WASM_BASE64 = (
    "AGFzbQEAAAABCAFgA39/fwF/AwIBAAcOAQphZGRfb3Jfc3ViAAAKFAESACAAIAFqIAAgAWsgAkEAThsL"
)


def test_disabled_not_available(start_cluster):
    """Without the flag, `system.webassembly_modules` must not exist and WASM functions must not be creatable."""

    # Table must not appear in system.tables
    result = node0.query(
        "SELECT name FROM system.tables WHERE database = 'system' AND name = 'webassembly_modules'"
    )
    assert result.strip() == ""

    # SELECT on the table must fail
    error = node0.query_and_get_error("SELECT name FROM system.webassembly_modules")
    assert "webassembly_modules" in error

    # INSERT must fail
    error = node0.query_and_get_error(
        f"INSERT INTO system.webassembly_modules (name, code) "
        f"SELECT 'module1', base64Decode('{ISPRIME_WASM_BASE64}')"
    )
    assert "webassembly_modules" in error

    # CREATE FUNCTION must fail
    error = node0.query_and_get_error(
        "CREATE FUNCTION is_prime LANGUAGE WASM ABI ROW_DIRECT "
        "FROM 'module1' ARGUMENTS (num UInt32) RETURNS UInt32"
    )
    assert "WebAssembly support is not enabled" in error


def test_enabled(start_cluster):
    """With the flag enabled: table is visible, modules and functions can be loaded, and they persist after restart."""

    if node1.is_built_with_memory_sanitizer():
        pytest.skip("Wasmtime is disabled in MSAN builds")

    # Table must appear in system.tables
    result = node1.query(
        "SELECT name FROM system.tables WHERE database = 'system' AND name = 'webassembly_modules'"
    )
    assert result.strip() == "webassembly_modules"

    node1.query("DROP FUNCTION IF EXISTS is_prime")
    node1.query("DELETE FROM system.webassembly_modules WHERE name = 'isprime'")

    node1.query(
        f"INSERT INTO system.webassembly_modules (name, code) "
        f"SELECT 'isprime', base64Decode('{ISPRIME_WASM_BASE64}')"
    )

    result = node1.query(
        "SELECT name FROM system.webassembly_modules WHERE name = 'isprime'"
    )
    assert result.strip() == "isprime"

    node1.query(
        "CREATE FUNCTION is_prime LANGUAGE WASM ABI ROW_DIRECT "
        "FROM 'isprime' ARGUMENTS (num UInt32) RETURNS UInt32"
    )

    # 2, 3, 5, 7 are prime; 4, 6, 8, 9 are not
    result = node1.query(
        "SELECT number, is_prime(number :: UInt32) FROM system.numbers "
        "WHERE number BETWEEN 2 AND 9 ORDER BY number"
    )
    assert result == "2\t1\n3\t1\n4\t0\n5\t1\n6\t0\n7\t1\n8\t0\n9\t0\n"

    assert (
        "Cannot delete WebAssembly module 'isprime' while it is in use. Drop all functions referring to it first"
        in node1.query_and_get_error(
            "DELETE FROM system.webassembly_modules WHERE name = 'isprime'"
        )
    )

    # After restart, module and function must still be available
    node1.restart_clickhouse()

    result = node1.query(
        "SELECT name FROM system.webassembly_modules WHERE name = 'isprime'"
    )
    assert result.strip() == "isprime"

    result = node1.query(
        "SELECT number, is_prime(number :: UInt32) FROM system.numbers "
        "WHERE number BETWEEN 2 AND 9 ORDER BY number"
    )
    assert result == "2\t1\n3\t1\n4\t0\n5\t1\n6\t0\n7\t1\n8\t0\n9\t0\n"

    # A .wasm file placed directly on disk (without INSERT INTO system.webassembly_modules)
    # must be loadable when referenced by CREATE FUNCTION.
    wasm_dir = "/var/lib/clickhouse/user_scripts/wasm"
    node1.exec_in_container(
        [
            "python3",
            "-c",
            f"import base64; open('{wasm_dir}/add_or_sub.wasm', 'wb')"
            f".write(base64.b64decode('{ADD_OR_SUB_WASM_BASE64}'))",
        ]
    )
    node1.query(
        "CREATE FUNCTION add_or_sub LANGUAGE WASM ABI ROW_DIRECT FROM 'add_or_sub'"
        "ARGUMENTS (a UInt32, b UInt32, control Int32) RETURNS UInt32"
    )
    result = node1.query(
        "SELECT add_or_sub(10 :: UInt32, 5 :: UInt32, 1 :: Int32), add_or_sub(10 :: UInt32, 5 :: UInt32, -1 :: Int32)"
    )
    assert result == "15\t5\n"

    # If the .wasm file is broken, CREATE FUNCTION must fail with an error,
    # and the broken module must not be added to system.webassembly_modules
    node1.exec_in_container(
        [
            "python3",
            "-c",
            f"open('{wasm_dir}/broken.wasm', 'wb')"
            f".write(b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00')",
        ]
    )
    error = node1.query_and_get_error(
        "CREATE FUNCTION some_func LANGUAGE WASM ABI ROW_DIRECT FROM 'broken'"
        "ARGUMENTS (a UInt32) RETURNS UInt32"
    )
    assert "Cannot load WebAssembly module" in error

    result = node1.query("SELECT name FROM system.webassembly_modules ORDER BY name")
    assert result == "add_or_sub\nisprime\n"

    # Cleanup
    node1.query("DROP FUNCTION IF EXISTS is_prime")
    node1.query("DROP FUNCTION IF EXISTS add_or_sub")
    node1.query("DELETE FROM system.webassembly_modules WHERE name = 'isprime'")
    node1.query("DELETE FROM system.webassembly_modules WHERE name = 'add_or_sub'")
