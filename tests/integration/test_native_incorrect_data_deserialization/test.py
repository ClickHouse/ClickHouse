# pylint: disable=redefined-outer-name

import logging

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


def error_code(text):
    for code in (
        "INCORRECT_DATA",
        "LOGICAL_ERROR",
        "CANNOT_READ_ALL_DATA",
        "CANNOT_PARSE_INPUT_ASSERTION_FAILED",
    ):
        if code in text:
            return code
    return "NO_ERROR_CODE"


def assert_error_code(error, expected, expected_message):
    code = error_code(error)
    logging.debug("Error code: %s", code)
    logging.debug("Error output:\n%s", error)
    assert code == expected
    assert expected_message in error


def write_var_uint(value):
    result = bytearray()
    while value >= 0x80:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def u64(value):
    return value.to_bytes(8, "little")


def native_block(name, type_name, rows, data):
    name = name.encode()
    type_name = type_name.encode()
    return (
        write_var_uint(1)
        + write_var_uint(rows)
        + write_var_uint(len(name))
        + name
        + write_var_uint(len(type_name))
        + type_name
        + data
    )


def native_stdin(payload):
    return payload.decode("latin1")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.query(
            "CREATE TABLE variant_data (x Variant(String, UInt8)) ENGINE = Memory",
            settings={"allow_experimental_variant_type": "1"},
        )
        yield cluster

    finally:
        cluster.shutdown()


def test_variant_size_less_than_discriminators_limit(started_cluster, tmp_path):
    payload = native_block(
        "x",
        "Variant(String, UInt8)",
        4,
        u64(0) + bytes([1, 1, 1, 1]) + bytes([7, 8]),
    )

    data_path = tmp_path / "variant_size_less_than_discriminators_limit.native"
    data_path.write_bytes(payload)
    node.copy_file_to_container(
        str(data_path), "/tmp/variant_size_less_than_discriminators_limit.native"
    )

    error = node.exec_in_container(
        [
            "bash",
            "-c",
            (
                "set +e; output=$(clickhouse local --send_logs_level=fatal "
                "--query \"SELECT count(), toTypeName(x) "
                "FROM file('/tmp/variant_size_less_than_discriminators_limit.native', 'Native', 'x Variant(String, UInt8)') "
                "FORMAT TSV\" 2>&1); printf '%s' \"$output\""
            ),
        ],
    )

    assert_error_code(
        error,
        "INCORRECT_DATA",
        "Size of variant UInt8 is expected to be not less than 4 according to discriminators, but it is 2",
    )
