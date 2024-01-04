from helpers.cluster import ClickHouseCluster
import pytest, time

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/heap_profiler.xml", "configs/dump_period.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_heap_profiler(started_cluster):
    if node.is_built_with_sanitizer():
        pytest.skip("Disabled for sanitizers")

    def get_latest_profile_id():
        return node.query(
            "select profile_id from system.trace_log where trace_type = 'MemoryProfile' group by profile_id order by max(event_time_microseconds) desc limit 1"
        ).strip()

    def find_symbol_in_profile(profile_id, symbol_name):
        return node.query(
            f"select weight, query_id from system.trace_log array join trace where trace_type = 'MemoryProfile' and profile_id = '{profile_id}' and demangle(addressToSymbol(trace)) like '%{symbol_name}%' limit 1 settings allow_introspection_functions=1"
        )

    # Start an infinite query that uses > 8 MB of memory in one allocation.
    node.get_query_request(
        "select max(number + sleep(2)) from system.numbers settings max_block_size = 2000000, preferred_block_size_bytes = 10000000"
    )

    # Wait to see a heap profile that contains "NumbersRangedSource" in stack trace.
    symbol_name = "NumbersRangedSource"
    query_id, profile_id = "", ""
    for _attempt in range(10):
        time.sleep(1)

        query_id = node.query(
            "select query_id from system.processes where query not like 'select query_id from system.processes%'"
        ).strip()
        if query_id == "":
            continue

        node.query("system flush logs")

        profile_id = get_latest_profile_id()
        if profile_id == "":
            continue

        res = find_symbol_in_profile(profile_id, symbol_name)
        if res == "":
            continue
        [weight, profile_query_id] = res.split()

        assert int(weight) >= 2**23
        assert profile_query_id == query_id
        break
    else:
        raise Exception(
            f"Didn't see a heap profile with the expected function in the stack trace! Found query: {query_id != ''}, found profile: {profile_id != ''}"
        )

    # Disable periodic dumping of heap profiles, check that it stopped.
    node.replace_config(
        "/etc/clickhouse-server/config.d/dump_period.xml",
        """
<clickhouse>
    <heap_profiler_dump_period_seconds>-1</heap_profiler_dump_period_seconds>
</clickhouse>
""",
    )
    node.query("system reload config")
    for _attempt in range(5):
        time.sleep(2)
        new_profile_id = get_latest_profile_id()
        assert new_profile_id != ""
        if new_profile_id == profile_id:
            break
        profile_id = new_profile_id

    # Kill the query, wait for it to die.
    node.query(f"kill query where query_id = '{query_id}'")
    while (
        node.query(
            f"select count() from system.processes where query_id = '{query_id}'"
        )
        != "0\n"
    ):
        time.sleep(1)

    # Trigger a heap profile dump manually, check that it doesn't contain our query anymore.
    node.query("system dump heap profile")
    node.query("system flush logs")
    new_profile_id = get_latest_profile_id()
    assert new_profile_id != ""
    assert new_profile_id != profile_id
    profile_id = new_profile_id
    res = find_symbol_in_profile(profile_id, symbol_name)
    assert res == ""

    # Trigger another, to make sure it works more than once.
    node.query("system dump heap profile")
    node.query("system flush logs")
    new_profile_id = get_latest_profile_id()
    assert new_profile_id != ""
    assert new_profile_id != profile_id
