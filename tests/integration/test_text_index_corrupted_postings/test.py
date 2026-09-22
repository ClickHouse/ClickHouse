import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")

READ_POSTINGS = "SETTINGS use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def patch_varint(data, index, value):
    """Rewrites the VarUInt number `index` (0-based) of `data` with `value`."""
    out = bytearray()
    pos = 0

    for i in range(index + 1):
        number = shift = 0
        while True:
            byte = data[pos]
            pos += 1
            number |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                break

        number = value if i == index else number
        while True:
            byte = number & 0x7F
            number >>= 7
            out.append(byte | 0x80 if number else byte)
            if not number:
                break

    return bytes(out) + data[pos:]


# A damaged posting list of a text index must be rejected while reading it: its header sizes the
# read buffers and its row ids go into the query as matching rows, so both are checked against the
# token metadata of the dictionary.
@pytest.mark.parametrize(
    "codec, number, value, expected_error",
    [
        # the declared size of an uncompressed posting list
        ("none", 0, 2000, "bitmap of 2000 bytes exceeds the upper bound"),
        # the declared number of row ids of a compressed posting list
        ("bitpacking", 2, 0, "cardinality 0 is not in the range"),
        # the first row id of a compressed posting list, which shifts all of its row ids
        ("bitpacking", 3, 5, "row ids from 5 to 104 while its row range is [0, 99]"),
    ],
)
def test_corrupted_posting_list_segment(
    started_cluster, tmp_path, codec, number, value, expected_error
):
    table = f"corrupted_postings_{codec}_{number}"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (id UInt32, s String,
            INDEX idx(s) TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = '{codec}', posting_list_block_size = 1024))
        ENGINE = MergeTree ORDER BY id
        -- min_bytes_for_full_part_storage = 0: a full part keeps the stream edited below in its own file.
        SETTINGS min_bytes_for_full_part_storage = 0
        """
    )

    try:
        node.query(f"INSERT INTO {table} SELECT number, 'foo' FROM numbers(100)")
        assert (
            node.query(
                f"SELECT sum(id) FROM {table} WHERE hasToken(s, 'foo') {READ_POSTINGS}"
            )
            == "4950\n"
        )

        part_path = node.query(
            f"SELECT path FROM system.parts WHERE table = '{table}' AND active"
        ).strip()
        postings_path = part_path + "skp_idx_idx.pst.idx"
        local_path = tmp_path / "skp_idx_idx.pst.idx"

        node.copy_file_from_container(postings_path, str(local_path))
        local_path.write_bytes(patch_varint(local_path.read_bytes(), number, value))
        node.copy_file_to_container(str(local_path), postings_path)

        # The posting list is served from the index caches until they are dropped.
        node.query("SYSTEM DROP TEXT INDEX CACHES")

        error = node.query_and_get_error(
            f"SELECT sum(id) FROM {table} WHERE hasToken(s, 'foo') {READ_POSTINGS}"
        )
        assert expected_error in error
    finally:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
