import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["overrides/server.yaml"],
    user_configs=["overrides/users.yaml"],
    stay_alive=True,
)

RETRIES = 100


# We retry each query since max_untracked_memory=1Gi is not enough, because it can be parsed too late
def retry(check):
    last_exc = None
    for _ in range(RETRIES):
        try:
            result = check()
        except Exception as e:
            last_exc = e
            continue
        if result is False:
            continue
        return result
    if last_exc is not None:
        raise AssertionError("retry", last_exc)
    raise AssertionError("retry: check never returned non-False")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_early_memory_limit():
    instance.restart_clickhouse()

    instance.query("DROP USER IF EXISTS Alex")
    instance.query("DROP USER IF EXISTS Bob")
    instance.query("CREATE USER Alex", user="default")
    instance.query("CREATE USER Bob", user="default")

    # 2G should be probably enough
    instance.query("system allocate memory 2000000000", user="default")
    retry(lambda: instance.query("select 1", user="default"))
    retry(lambda: "(total) memory limit exceeded" in instance.query_and_get_error("select 1", user="Bob"))

    server_ip = cluster.get_instance_ip("instance")
    # Note, we cannot use output_format_parallel_formatting since it uses separate threads with default max_untracked_memory=4Mi
    endpoint = "'http://{}:8123/?query=SELECT+1&user=Alex&output_format_parallel_formatting=0'".format(server_ip)
    retry(lambda: "(total) memory limit exceeded" in instance.exec_in_container(["bash", "-c", f"curl {endpoint}"]))

    instance.replace_in_config(
        "/etc/clickhouse-server/config.d/server.yaml",
        "default",
        "default , Alex ",
    )
    retry(lambda: instance.query("system reload config", user="default"))

    retry(lambda: "(total) memory limit exceeded" not in instance.exec_in_container(["bash", "-c", f"curl {endpoint}"]))
    retry(lambda: instance.query("select 1", user="default"))

    retry(lambda: "(total) memory limit exceeded" in instance.query_and_get_error("select 1", user="Bob"))
    # "system free memory" frees deterministically, so it and the cleanup after it need no retry
    instance.query("system free memory")
    instance.query("DROP USER IF EXISTS Alex")
    instance.query("DROP USER IF EXISTS Bob")

    instance.replace_in_config(
        "/etc/clickhouse-server/config.d/server.yaml",
        "default , Alex ",
        "default",
    )
    retry(lambda: instance.query("system reload config", user="default"))
