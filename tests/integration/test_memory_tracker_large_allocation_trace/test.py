import re

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

THRESHOLD = 64 * 1024 * 1024
LOG_MARKER = "Single allocation of"
# One PODArray doubling past the threshold: reallocs of 64, 128 and 256 MiB, no limit in the way.
TRIGGER = "SELECT length(groupArray(number)) FROM numbers(30000000) SETTINGS max_memory_usage = 0"

# A new log record ends the multi-line message that carries the stack.
LOG_LINE = re.compile(r"^\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.")
# "<index>. 0x<address> <symbol>" / "<index>. <symbol>", symbol resolved to something but "?".
FRAME_WITH_ADDRESS = re.compile(r"^\d+\. 0x[0-9a-f]+ (?!\?$)\S")
FRAME_WITHOUT_ADDRESS = re.compile(r"^\d+\. (?!\?$)\S")

node = cluster.add_instance("node", main_configs=["configs/with_addresses.yaml"])
# show_addresses_in_stack_traces is applied at startup only, so it needs its own instance rather
# than a config reload.
node_no_addresses = cluster.add_instance(
    "node_no_addresses", main_configs=["configs/without_addresses.yaml"]
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def stack_of_first_record(instance):
    """The frame lines of the first logged trace, and only those: the lines after the marker up to
    the next log record. Slicing to one record keeps unrelated log content out of the assertions."""
    lines = instance.grep_in_log(LOG_MARKER, after=40).splitlines()
    assert any(LOG_MARKER in line for line in lines), lines
    start = next(i for i, line in enumerate(lines) if LOG_MARKER in line)

    frames = []
    for line in lines[start + 1 :]:
        if LOG_LINE.match(line) or line == "--":
            break
        frames.append(line)
    assert frames, lines
    return frames


def test_setting_is_reported_as_changeable():
    assert (
        node.query(
            "SELECT value, changeable_without_restart FROM system.server_settings "
            "WHERE name = 'min_allocation_size_to_log_stack_trace'"
        ).strip()
        == f"{THRESHOLD}\tYes"
    )


def test_large_allocation_reaches_the_server_log_and_trace_log():
    assert not node.contains_in_log(LOG_MARKER)

    node.query(TRIGGER)

    frames = stack_of_first_record(node)
    # A resolved frame, rather than a specific function name: inlining makes a named frame brittle.
    assert any(FRAME_WITH_ADDRESS.match(frame) for frame in frames), frames

    node.query("SYSTEM FLUSH LOGS trace_log")
    count, memory_context, max_size = (
        node.query(
            "SELECT count(), any(memory_context), max(size) FROM system.trace_log "
            "WHERE trace_type = 'MemoryLargeAllocation'"
        )
        .strip()
        .split("\t")
    )
    assert int(count) > 0
    assert memory_context == "Global"
    assert int(max_size) >= THRESHOLD


def test_addresses_are_hidden_when_disabled():
    assert not node_no_addresses.contains_in_log(LOG_MARKER)

    node_no_addresses.query(TRIGGER)

    frames = stack_of_first_record(node_no_addresses)
    assert any(FRAME_WITHOUT_ADDRESS.match(frame) for frame in frames), frames
    assert not any("0x" in frame for frame in frames), frames
