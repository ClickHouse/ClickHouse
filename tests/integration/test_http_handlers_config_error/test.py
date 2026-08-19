import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The broken `<http_handlers>` section is copied in at runtime rather than passed as a
# `main_configs` entry, because the instance has to come up once with a valid configuration
# before the failing start can be observed.
node = cluster.add_instance("node", stay_alive=True)

BAD_CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/bad_handler.xml"

ERR_LOG = "clickhouse-server.err.log"


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_handler_config_error_is_not_reported_as_a_listen_failure(start_cluster):
    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/bad_handler.xml"),
        BAD_CONFIG_IN_CONTAINER,
    )
    try:
        node.start_clickhouse(expected_to_fail=True)

        # The handler type is rejected by `createHandlersFactoryFromConfig`, so the server must
        # refuse to start and name that as the reason.
        # `zgrep` treats the substring as a regular expression, so a bracketed literal such as
        # `Listen [0.0.0.0]:8123 failed` would match a character class and silently never fire.
        # Everything asserted here is therefore bracket-free.
        reported = node.grep_in_log(substring="Unknown handler type", filename=ERR_LOG)
        assert reported != ""

        # The reason must be reported on its own terms. Constructing the handler factory inside
        # the `createServer` callback used to wrap it into `Listen ...: failed: <cause>` and
        # re-code it as `NETWORK_ERROR`, which points the reader at the socket layer. Asserted on
        # the reporting line itself, so unrelated log contents cannot satisfy it either way.
        assert "Listen" not in reported
        assert "NETWORK_ERROR" not in reported
    finally:
        node.exec_in_container(["bash", "-c", f"rm -f {BAD_CONFIG_IN_CONTAINER}"], user="root")
        node.start_clickhouse()
