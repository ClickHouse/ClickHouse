import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The broken `<http_handlers>` section is copied in at runtime rather than passed as a
# `main_configs` entry, because the instance has to come up once with a valid configuration
# before the failing start can be observed.
node = cluster.add_instance("node", stay_alive=True)

BAD_CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/bad_handler.xml"
LISTEN_TRY_CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/listen_try.xml"
NO_HTTP_PORT_CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/no_http_port.xml"

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


def test_handler_config_error_is_not_discarded_when_listen_try_is_set(start_cluster):
    # `listen_try` reaches the other exit of the same handler in `createServer`: rather than
    # reporting the failure it logged a warning about `<listen_host>` and let the server run with
    # no HTTP interface at all. The setting is read from the config root by name, so it applies
    # alongside the explicit `<listen_host>` the integration harness supplies.
    node.stop_clickhouse()

    # The error log carries warnings too (`logger.errorlog_level` defaults to `notice`), which is
    # the severity of the message this case must not find, and `grep_in_log` globs every rotation
    # while these instances set `<rotateOnOpen>`. An earlier start's report would therefore be in
    # the search space and could satisfy the assertions below on its own. The harness greps
    # `clickhouse-server.log` and `stderr.log` for crashes, so those are left alone.
    node.exec_in_container(
        ["bash", "-c", "rm -f /var/log/clickhouse-server/clickhouse-server.err.log*"],
        user="root",
    )

    configs_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")
    node.copy_file_to_container(
        os.path.join(configs_dir, "bad_handler.xml"), BAD_CONFIG_IN_CONTAINER
    )
    node.copy_file_to_container(
        os.path.join(configs_dir, "listen_try.xml"), LISTEN_TRY_CONFIG_IN_CONTAINER
    )
    try:
        # The startup must fail. It used to succeed here, serving no HTTP.
        node.start_clickhouse(expected_to_fail=True)

        reported = node.grep_in_log(substring="Unknown handler type", filename=ERR_LOG)
        assert reported != ""

        # Bracket-free for the same reason as in the case above. `consider to` is the distinctive
        # part of the `<listen_host>` advice that accompanied the discarded error.
        assert "Listen" not in reported
        assert "NETWORK_ERROR" not in reported
        assert "consider to" not in reported
    finally:
        node.exec_in_container(
            ["bash", "-c", f"rm -f {BAD_CONFIG_IN_CONTAINER} {LISTEN_TRY_CONFIG_IN_CONTAINER}"],
            user="root",
        )
        node.start_clickhouse()


def test_handler_config_is_not_read_without_an_http_port(start_cluster):
    # With no `http_port` there is no HTTP listener to configure, so `<http_handlers>` must not be read
    # at all and a broken rule in it must not keep the server down. This is a lock on the port check
    # that keeps the parse out of that path; it holds before the change too, since the parse then sat
    # behind the same condition inside `createServer`.
    node.stop_clickhouse()

    # The assertion below is an ABSENCE, so an earlier case's report surviving in the search space
    # would fail it for the wrong reason: `grep_in_log` globs every rotation and these instances set
    # `<rotateOnOpen>`.
    node.exec_in_container(
        ["bash", "-c", "rm -f /var/log/clickhouse-server/clickhouse-server.err.log*"],
        user="root",
    )

    configs_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")
    node.copy_file_to_container(
        os.path.join(configs_dir, "bad_handler.xml"), BAD_CONFIG_IN_CONTAINER
    )
    node.copy_file_to_container(
        os.path.join(configs_dir, "no_http_port.xml"), NO_HTTP_PORT_CONFIG_IN_CONTAINER
    )
    try:
        node.start_clickhouse()

        assert node.query("SELECT 1").strip() == "1"
        assert node.grep_in_log(substring="Unknown handler type", filename=ERR_LOG) == ""
    finally:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"rm -f {BAD_CONFIG_IN_CONTAINER} {NO_HTTP_PORT_CONFIG_IN_CONTAINER}",
            ],
            user="root",
        )
        node.restart_clickhouse()
