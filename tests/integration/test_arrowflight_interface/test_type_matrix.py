# coding: utf-8

# The Arrow <-> ClickHouse column converters (`CHColumnToArrowColumn` for `DoGet`,
# `ArrowColumnToCHColumn` for `DoPut`) are reached from the Flight interface and from nothing else
# in a plain server run, so the type coverage of this file is the type coverage those converters
# get. The rest of test.py exercises them with `Int64` and `String` only, which leaves every
# per-type branch of the conversion (decimals, big integers, temporal units, containers,
# dictionary-encoded and view layouts) untested.
#
# Two directions need different fixtures, so there are two tests:
#
#   * `test_roundtrip_*` covers the types ClickHouse itself emits. It pulls a table over `DoGet`
#     and pushes the result straight back over `DoPut` into an identically typed table, then
#     compares the two tables as text. Writing the assertion in ClickHouse's own terms rather than
#     in Arrow's keeps it readable and keeps it from breaking when an Arrow mapping is refined:
#     what must hold is that a value survives the round trip.
#
#   * `test_doget_arrow_schema` pins the Arrow type `DoGet` emits for the mappings that are a
#     deliberate choice rather than the obvious one. The round trip alone cannot see those: the
#     import casts to the destination header type, so a wrong-but-castable schema would still
#     compare equal.
#
#   * `test_doput_arrow_type` covers the Arrow types ClickHouse never emits and therefore cannot
#     reach by a round trip: `date64`, `duration`, `time32`, the large and view string layouts,
#     dictionary encoding, `null`, `halffloat` and offset-named timestamp zones. These are built
#     with pyarrow and pushed into a column of the ClickHouse type they map to.

import pytest
import pyarrow as pa
import pyarrow.flight as flight

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/flight_port.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


def get_client():
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")
    token = client.authenticate_basic_token("user1", "qwe123")
    return client, flight.FlightCallOptions(headers=[token])


# Groups rather than one column per test case: a group is a single round trip, and a failure still
# points at a handful of columns instead of at the whole matrix.
ROUNDTRIP_GROUPS = {
    "integers": [
        ("u8", "UInt8", "1"),
        ("i8", "Int8", "-1"),
        ("u16", "UInt16", "2"),
        ("i16", "Int16", "-2"),
        ("u32", "UInt32", "3"),
        ("i32", "Int32", "-3"),
        ("u64", "UInt64", "4"),
        ("i64", "Int64", "-4"),
        ("nullable", "Nullable(Int32)", "NULL"),
        ("enum8", "Enum8('a' = 1, 'b' = 2)", "'b'"),
        ("enum16", "Enum16('a' = 1000, 'b' = 2000)", "'b'"),
    ],
    "big_integers": [
        ("i128", "Int128", "-170141183460469231731687303715884105728"),
        ("u128", "UInt128", "340282366920938463463374607431768211455"),
        (
            "i256",
            "Int256",
            "-57896044618658097711785492504343953926634992332820282019728792003956564819968",
        ),
        (
            "u256",
            "UInt256",
            "115792089237316195423570985008687907853269984665640564039457584007913129639935",
        ),
    ],
    "decimals": [
        ("dec32", "Decimal32(2)", "'1.25'"),
        ("dec64", "Decimal64(4)", "'-1.0625'"),
        ("dec128", "Decimal128(6)", "'3.141592'"),
        ("dec256", "Decimal256(8)", "'2.71828182'"),
    ],
    "floats_and_strings": [
        ("f32", "Float32", "1.5"),
        ("f64", "Float64", "-2.5"),
        ("boolean", "Bool", "true"),
        ("s", "String", "'abc'"),
        ("fs", "FixedString(4)", "'abcd'"),
        ("nullable_fs", "Nullable(FixedString(3))", "'abc'"),
        ("lc", "LowCardinality(String)", "'lc'"),
        ("lc_nullable", "LowCardinality(Nullable(String))", "NULL"),
    ],
    # Every emitted temporal variant, not one representative: `DateTime64` and `Time64` map onto a
    # different Arrow unit per scale, so each scale is a separate writer and reader branch.
    "temporal": [
        ("d", "Date", "'2026-09-20'"),
        ("d32", "Date32", "'2026-09-20'"),
        ("dt", "DateTime('UTC')", "'2026-09-20 10:11:12'"),
        ("dt64_0", "DateTime64(0, 'UTC')", "'2026-09-20 10:11:12'"),
        ("dt64_3", "DateTime64(3, 'UTC')", "'2026-09-20 10:11:12.345'"),
        ("dt64_6", "DateTime64(6, 'UTC')", "'2026-09-20 10:11:12.345678'"),
        ("dt64_9", "DateTime64(9, 'UTC')", "'2026-09-20 10:11:12.345678901'"),
        ("t64_0", "Time64(0)", "'10:11:12'"),
        ("t64_3", "Time64(3)", "'10:11:12.345'"),
        ("t64_6", "Time64(6)", "'10:11:12.345678'"),
        ("t64_9", "Time64(9)", "'10:11:12.345678901'"),
        ("interval", "IntervalSecond", "INTERVAL 5 SECOND"),
    ],
    "network_and_uuid": [
        ("uu", "UUID", "'61f0c404-5cb3-11e7-907b-a6006ad3dba0'"),
        ("ip4", "IPv4", "'1.2.3.4'"),
        ("ip6", "IPv6", "'2001:db8::1'"),
    ],
    "containers": [
        ("arr", "Array(Int32)", "[1, 2, 3]"),
        ("arr_nullable", "Array(Nullable(String))", "['a', NULL]"),
        ("arr_nested", "Array(Array(UInt8))", "[[1], [2, 3]]"),
        ("tup", "Tuple(a Int32, b String)", "tuple(7, 'x')"),
        ("tup_nullable", "Tuple(a Nullable(Int32), b Nullable(String))", "tuple(NULL, 'y')"),
        ("m", "Map(String, Int64)", "map('k', 9)"),
        ("m_nullable", "Map(String, Nullable(Int64))", "map('k', NULL)"),
        ("m_array", "Map(String, Array(Int32))", "map('k', [1, 2])"),
    ],
    "geo": [
        ("pt", "Point", "(1.5, 2.5)"),
        ("ring", "Ring", "[(0, 0), (1, 0), (1, 1), (0, 0)]"),
        ("polygon", "Polygon", "[[(0, 0), (1, 0), (1, 1), (0, 0)]]"),
    ],
}


@pytest.mark.parametrize("group", sorted(ROUNDTRIP_GROUPS))
def test_roundtrip(group):
    columns = ROUNDTRIP_GROUPS[group]
    schema = ", ".join(f"{name} {type_name}" for name, type_name, _ in columns)
    values = ", ".join(value for _, _, value in columns)

    node.query("DROP TABLE IF EXISTS matrix_src SYNC")
    node.query("DROP TABLE IF EXISTS matrix_dst SYNC")
    node.query(f"CREATE TABLE matrix_src ({schema}) ENGINE = MergeTree ORDER BY tuple()")
    node.query(f"CREATE TABLE matrix_dst ({schema}) ENGINE = MergeTree ORDER BY tuple()")
    node.query(f"INSERT INTO matrix_src VALUES ({values})")

    client, options = get_client()

    descriptor = flight.FlightDescriptor.for_command("SELECT * FROM matrix_src")
    flight_info = client.get_flight_info(descriptor, options)
    table = client.do_get(flight_info.endpoints[0].ticket, options).read_all()

    assert table.num_rows == 1
    assert table.schema.names == [name for name, _, _ in columns]

    descriptor = flight.FlightDescriptor.for_command("INSERT INTO matrix_dst FORMAT Arrow")
    writer, _ = client.do_put(descriptor, table.schema, options)
    writer.write_table(table)
    writer.close()

    expected = node.query("SELECT * FROM matrix_src FORMAT TSV")
    actual = node.query("SELECT * FROM matrix_dst FORMAT TSV")
    assert actual == expected

    node.query("DROP TABLE matrix_src SYNC")
    node.query("DROP TABLE matrix_dst SYNC")


# `DoGet` output types worth pinning: a mapping that is a deliberate choice, and that the round trip
# cannot check because the import casts to the destination header type anyway. Only the interesting
# ones are listed; there is no value in restating that `Int32` is `int32`.
DOGET_ARROW_TYPES = [
    # Deliberately uint32 rather than date64/timestamp: seconds are all a DateTime carries.
    ("dt", "DateTime('UTC')", "'2026-09-20 10:11:12'", pa.uint32()),
    ("d", "Date", "'2026-09-20'", pa.date32()),
    ("d32", "Date32", "'2026-09-20'", pa.date32()),
    # One Arrow unit per scale, and time32 below milliseconds where Arrow requires it.
    ("dt64_0", "DateTime64(0, 'UTC')", "'2026-09-20 10:11:12'", pa.timestamp("s", tz="UTC")),
    ("dt64_9", "DateTime64(9, 'UTC')", "'2026-09-20 10:11:12.345678901'", pa.timestamp("ns", tz="UTC")),
    ("t64_0", "Time64(0)", "'10:11:12'", pa.time32("s")),
    ("t64_9", "Time64(9)", "'10:11:12.345678901'", pa.time64("ns")),
    ("interval", "IntervalSecond", "INTERVAL 5 SECOND", pa.duration("s")),
    # Fixed-width rather than variable binary, so the width stays in the schema.
    ("fs", "FixedString(4)", "'abcd'", pa.binary(4)),
    ("ip6", "IPv6", "'2001:db8::1'", pa.binary(16)),
    ("i256", "Int256", "12345", pa.binary(32)),
    # IPv4 is 4 bytes but travels as a number, unlike IPv6.
    ("ip4", "IPv4", "'1.2.3.4'", pa.uint32()),
    # Full decimal256 width, not the narrowest that fits.
    ("dec256", "Decimal256(8)", "'2.71828182'", pa.decimal256(76, 8)),
    # Enums travel as their underlying integer, losing the names.
    ("e8", "Enum8('a' = 1, 'b' = 2)", "'b'", pa.int8()),
    ("s", "String", "'abc'", pa.string()),
]


def test_doget_arrow_schema():
    schema = ", ".join(f"{name} {type_name}" for name, type_name, _, _ in DOGET_ARROW_TYPES)
    values = ", ".join(value for _, _, value, _ in DOGET_ARROW_TYPES)

    node.query("DROP TABLE IF EXISTS arrow_schema SYNC")
    node.query(f"CREATE TABLE arrow_schema ({schema}) ENGINE = MergeTree ORDER BY tuple()")
    node.query(f"INSERT INTO arrow_schema VALUES ({values})")

    client, options = get_client()
    descriptor = flight.FlightDescriptor.for_command("SELECT * FROM arrow_schema")
    flight_info = client.get_flight_info(descriptor, options)
    table = client.do_get(flight_info.endpoints[0].ticket, options).read_all()

    actual = {field.name: field.type for field in table.schema}
    expected = {name: arrow_type for name, _, _, arrow_type in DOGET_ARROW_TYPES}
    assert actual == expected

    # None of the columns is Nullable, and that has to reach the schema: a reader that treats
    # everything as nullable cannot tell a null-free column from one that happens to have no nulls.
    assert all(not field.nullable for field in table.schema)

    node.query("DROP TABLE arrow_schema SYNC")


def test_doget_uuid_is_tagged():
    """`UUID` is the one type that travels as an Arrow extension rather than as a plain layout.

    Without the tag a client sees 16 opaque bytes, so the metadata is the whole point of the
    mapping and is worth asserting separately from the type.
    """
    node.query("DROP TABLE IF EXISTS arrow_uuid SYNC")
    node.query("CREATE TABLE arrow_uuid (v UUID) ENGINE = MergeTree ORDER BY tuple()")
    node.query("INSERT INTO arrow_uuid VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0')")

    client, options = get_client()
    descriptor = flight.FlightDescriptor.for_command("SELECT * FROM arrow_uuid")
    flight_info = client.get_flight_info(descriptor, options)
    table = client.do_get(flight_info.endpoints[0].ticket, options).read_all()

    field = table.schema.field("v")
    assert field.metadata[b"ARROW:extension:name"] == b"arrow.uuid"
    assert field.type.storage_type == pa.binary(16)

    node.query("DROP TABLE arrow_uuid SYNC")


def arrow_type_cases():
    """(case id, ClickHouse column type, Arrow array, expected `SELECT *` output in TSV).

    Every array is built from integers or bytes rather than from Python `datetime` and `Decimal`
    objects, so what reaches the server does not depend on how the installed pyarrow converts
    Python values, and the expected output can be stated exactly.
    """
    return [
        # A `date64` is milliseconds since the epoch constrained to whole days. 1789862400000 is
        # 2026-09-20T00:00:00Z.
        ("date64", "DateTime('UTC')", pa.array([1789862400000], type=pa.date64()), "2026-09-20 00:00:00"),
        # A `time32`/`time64` becomes `Time64` with the scale implied by the Arrow unit.
        ("time32_s", "Time64(0)", pa.array([3723], type=pa.time32("s")), "01:02:03"),
        ("time32_ms", "Time64(3)", pa.array([3723045], type=pa.time32("ms")), "01:02:03.045"),
        ("time64_us", "Time64(6)", pa.array([3600000007], type=pa.time64("us")), "01:00:00.000007"),
        ("time64_ns", "Time64(9)", pa.array([3600000000011], type=pa.time64("ns")), "01:00:00.000000011"),
        # A `duration` becomes the `Interval` of the matching unit.
        ("duration_s", "IntervalSecond", pa.array([5], type=pa.duration("s")), "5"),
        ("duration_ms", "IntervalMillisecond", pa.array([1500], type=pa.duration("ms")), "1500"),
        ("duration_us", "IntervalMicrosecond", pa.array([1500], type=pa.duration("us")), "1500"),
        ("duration_ns", "IntervalNanosecond", pa.array([1500], type=pa.duration("ns")), "1500"),
        # Arrow permits a numeric UTC offset or the marker "fixed" where ClickHouse expects an IANA
        # name; both are normalized instead of failing to load as a time zone. 1789899072000 is
        # 2026-09-20T10:11:12Z.
        (
            "timestamp_offset_zone",
            "DateTime64(3, 'UTC')",
            pa.array([1789899072000], type=pa.timestamp("ms", tz="+05:30")),
            "2026-09-20 10:11:12.000",
        ),
        (
            "timestamp_fixed_zone",
            "DateTime64(3, 'UTC')",
            pa.array([1789899072000], type=pa.timestamp("ms", tz="fixed")),
            "2026-09-20 10:11:12.000",
        ),
        # Dictionary encoding is what an Arrow producer emits for a low-cardinality column.
        (
            "dictionary",
            "LowCardinality(String)",
            pa.array(["x", "y", "x"], type=pa.dictionary(pa.int32(), pa.string())),
            "x\ny\nx",
        ),
        (
            "dictionary_nullable",
            "LowCardinality(Nullable(String))",
            pa.array(["x", None], type=pa.dictionary(pa.int32(), pa.string())),
            "x\n\\N",
        ),
        # The 64-bit-offset and view string layouts, which ClickHouse never writes.
        ("large_string", "String", pa.array(["big"], type=pa.large_string()), "big"),
        ("large_binary", "String", pa.array([b"bin"], type=pa.large_binary()), "bin"),
        ("string_view", "String", pa.array(["sv"], type=pa.string_view()), "sv"),
        ("binary_view", "String", pa.array([b"bv"], type=pa.binary_view()), "bv"),
        # The 64-bit-offset and fixed-width list layouts both become `Array`.
        ("large_list", "Array(Int32)", pa.array([[1, 2]], type=pa.large_list(pa.int32())), "[1,2]"),
        ("fixed_size_list", "Array(Int32)", pa.array([[1, 2]], type=pa.list_(pa.int32(), 2)), "[1,2]"),
        # The `null` type carries no values at all, only a length.
        ("null", "Nullable(Int32)", pa.array([None], type=pa.null()), "\\N"),
        ("halffloat", "Float32", pa.array([1.5], type=pa.float16()), "1.5"),
        # Binary payloads are reinterpreted according to the destination type: 16 bytes as a UUID or
        # an IPv6 address, and little-endian two's complement for the wide integers.
        (
            "uuid_from_binary",
            "UUID",
            pa.array([b"\x61\xf0\xc4\x04\x5c\xb3\x11\xe7\x90\x7b\xa6\x00\x6a\xd3\xdb\xa0"], type=pa.binary(16)),
            "61f0c404-5cb3-11e7-907b-a6006ad3dba0",
        ),
        (
            "ipv6_from_binary",
            "IPv6",
            pa.array([b"\x20\x01\x0d\xb8" + b"\x00" * 11 + b"\x01"], type=pa.binary(16)),
            "2001:db8::1",
        ),
        ("int128_from_binary", "Int128", pa.array([(12345).to_bytes(16, "little")], type=pa.binary()), "12345"),
        ("int256_from_binary", "Int256", pa.array([(12345).to_bytes(32, "little")], type=pa.binary(32)), "12345"),
    ]


@pytest.mark.parametrize(
    "clickhouse_type, array, expected",
    [case[1:] for case in arrow_type_cases()],
    ids=[case[0] for case in arrow_type_cases()],
)
def test_doput_arrow_type(clickhouse_type, array, expected):
    node.query("DROP TABLE IF EXISTS arrow_type SYNC")
    node.query(f"CREATE TABLE arrow_type (v {clickhouse_type}) ENGINE = MergeTree ORDER BY tuple()")

    schema = pa.schema([pa.field("v", array.type)])

    client, options = get_client()
    descriptor = flight.FlightDescriptor.for_command("INSERT INTO arrow_type FORMAT Arrow")
    writer, _ = client.do_put(descriptor, schema, options)
    writer.write_table(pa.table([array], schema=schema))
    writer.close()

    assert node.query("SELECT * FROM arrow_type FORMAT TSV").strip() == expected

    node.query("DROP TABLE arrow_type SYNC")
