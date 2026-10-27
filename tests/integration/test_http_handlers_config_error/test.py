import os
import time

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The broken `<http_handlers>` section is copied in at runtime rather than passed as a
# `main_configs` entry, because the instance has to come up once with a valid configuration
# before the failing start can be observed. The TLS material is a `main_configs` entry because it
# is inert on its own: only the `<openSSL>` section copied in later refers to it.
node = cluster.add_instance(
    "node",
    main_configs=["configs/server.crt", "configs/server.key", "configs/dhparam.pem"],
    stay_alive=True,
)

BAD_CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/bad_handler.xml"
LISTEN_TRY_CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/listen_try.xml"
NO_HTTP_PORT_CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/no_http_port.xml"
HTTPS_ONLY_CONFIG_IN_CONTAINER = "/etc/clickhouse-server/config.d/https_only.xml"
BAD_PROMETHEUS_CONFIG_IN_CONTAINER = (
    "/etc/clickhouse-server/config.d/bad_prometheus.xml"
)
BAD_PROMETHEUS_NO_PORT_CONFIG_IN_CONTAINER = (
    "/etc/clickhouse-server/config.d/bad_prometheus_no_port.xml"
)
GOOD_PROMETHEUS_CONFIG_IN_CONTAINER = (
    "/etc/clickhouse-server/config.d/good_prometheus.xml"
)
BAD_KEEPER_CONTROL_CONFIG_IN_CONTAINER = (
    "/etc/clickhouse-server/config.d/bad_keeper_control.xml"
)
BAD_KEEPER_CONTROL_SECURE_CONFIG_IN_CONTAINER = (
    "/etc/clickhouse-server/config.d/bad_keeper_control_secure.xml"
)
BAD_KEEPER_CONTROL_NO_PORT_CONFIG_IN_CONTAINER = (
    "/etc/clickhouse-server/config.d/bad_keeper_control_no_port.xml"
)

# Every error-log grep below passes `only_latest`. These instances set `<rotateOnOpen>`, so a start
# rotates the previous log aside instead of truncating it, and the cases share one container: a
# report left by an earlier start is a different start's evidence, whether it is being required or
# ruled out.
ERR_LOG = "clickhouse-server.err.log"


PROMETHEUS_PORT = 9363


def _prometheus_metrics_status(retries=30):
    # The listener binds after the server answers queries, so the first attempts can be refused.
    while True:
        try:
            return requests.get(
                f"http://{node.ip_address}:{PROMETHEUS_PORT}/metrics", timeout=5
            ).status_code
        except requests.exceptions.RequestException:
            if retries <= 0:
                raise
            retries -= 1
            time.sleep(0.5)


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
        reported = node.grep_in_log(
            substring="Unknown handler type", filename=ERR_LOG, only_latest=True
        )
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

        reported = node.grep_in_log(
            substring="Unknown handler type", filename=ERR_LOG, only_latest=True
        )
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
        assert node.grep_in_log(
            substring="Unknown handler type", filename=ERR_LOG, only_latest=True
        ) == ""
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


def test_handler_config_error_is_not_reported_as_a_listen_failure_on_https(start_cluster):
    # The HTTPS listener reads the same `<http_handlers>` section as the HTTP one and is configured
    # by its own copy of the same code, so it needs its own case. `http_port` is removed here, which
    # leaves HTTPS as the only listener that reads the section: a failure can then only be
    # attributed to the HTTPS path.
    node.stop_clickhouse()

    configs_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")
    node.copy_file_to_container(
        os.path.join(configs_dir, "bad_handler.xml"), BAD_CONFIG_IN_CONTAINER
    )
    node.copy_file_to_container(
        os.path.join(configs_dir, "https_only.xml"), HTTPS_ONLY_CONFIG_IN_CONTAINER
    )
    try:
        node.start_clickhouse(expected_to_fail=True)

        reported = node.grep_in_log(
            substring="Unknown handler type", filename=ERR_LOG, only_latest=True
        )
        assert reported != ""

        # Bracket-free for the same reason as in the cases above: the secure socket was bound
        # before the factory was built, so the cause used to come back as
        # `Listen ...:8443 failed: <cause>` re-coded to `NETWORK_ERROR`.
        assert "Listen" not in reported
        assert "NETWORK_ERROR" not in reported
    finally:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"rm -f {BAD_CONFIG_IN_CONTAINER} {HTTPS_ONLY_CONFIG_IN_CONTAINER}",
            ],
            user="root",
        )
        node.start_clickhouse()


def test_prometheus_handler_config_error_is_not_reported_as_a_listen_failure(
    start_cluster,
):
    # The standalone Prometheus listener builds its factory from `<prometheus.handlers>` the same
    # way, so it needs its own case. `http_port` is left in place: the Prometheus section is only
    # read by the Prometheus factory, so a failure can only be attributed to that path.
    node.stop_clickhouse()

    configs_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")
    node.copy_file_to_container(
        os.path.join(configs_dir, "bad_prometheus.xml"),
        BAD_PROMETHEUS_CONFIG_IN_CONTAINER,
    )
    try:
        node.start_clickhouse(expected_to_fail=True)

        reported = node.grep_in_log(
            substring="Unknown type no_such_prometheus_type",
            filename=ERR_LOG,
            only_latest=True,
        )
        assert reported != ""

        # Bracket-free for the same reason as in the cases above.
        assert "Listen" not in reported
        assert "NETWORK_ERROR" not in reported
    finally:
        node.exec_in_container(
            ["bash", "-c", f"rm -f {BAD_PROMETHEUS_CONFIG_IN_CONTAINER}"], user="root"
        )
        node.start_clickhouse()


def test_prometheus_handler_config_error_is_not_discarded_when_listen_try_is_set(
    start_cluster,
):
    # `listen_try` reaches the other exit of the same handler: the error was logged as a warning
    # about `<listen_host>` and the server ran on with no Prometheus endpoint at all.
    node.stop_clickhouse()

    configs_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs")
    node.copy_file_to_container(
        os.path.join(configs_dir, "bad_prometheus.xml"),
        BAD_PROMETHEUS_CONFIG_IN_CONTAINER,
    )
    node.copy_file_to_container(
        os.path.join(configs_dir, "listen_try.xml"), LISTEN_TRY_CONFIG_IN_CONTAINER
    )
    try:
        # The startup must fail. It used to succeed here, serving no Prometheus endpoint.
        node.start_clickhouse(expected_to_fail=True)

        reported = node.grep_in_log(
            substring="Unknown type no_such_prometheus_type",
            filename=ERR_LOG,
            only_latest=True,
        )
        assert reported != ""

        assert "Listen" not in reported
        assert "NETWORK_ERROR" not in reported
        assert "consider to" not in reported
    finally:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"rm -f {BAD_PROMETHEUS_CONFIG_IN_CONTAINER} {LISTEN_TRY_CONFIG_IN_CONTAINER}",
            ],
            user="root",
        )
        node.start_clickhouse()


def test_handler_config_error_on_reload_is_reported_to_the_client(start_cluster):
    # `updateServers` reuses `createServers`, so a runtime reload re-enters the same code with the
    # listener already stopped for recreation. The error must reach the `SYSTEM RELOAD CONFIG`
    # caller as the configuration error it is; it used to be reported as a listen failure, and
    # under `listen_try` swallowed entirely while HTTP stayed down.
    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "configs/listen_try.xml"
        ),
        LISTEN_TRY_CONFIG_IN_CONTAINER,
    )
    node.start_clickhouse()
    try:
        assert node.http_query("SELECT 1").strip() == "1"

        node.copy_file_to_container(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "configs/bad_handler.xml"
            ),
            BAD_CONFIG_IN_CONTAINER,
        )
        error = node.query_and_get_error("SYSTEM RELOAD CONFIG")

        # The cause must be named. Asserted on the error returned to the caller, so unrelated log
        # contents cannot satisfy it.
        assert "Unknown handler type" in error
        assert "Listen" not in error
        assert "NETWORK_ERROR" not in error
    finally:
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"rm -f {BAD_CONFIG_IN_CONTAINER} {LISTEN_TRY_CONFIG_IN_CONTAINER}",
            ],
            user="root",
        )
        node.restart_clickhouse()


def test_prometheus_handler_config_is_not_read_without_a_prometheus_port(start_cluster):
    # Without `prometheus.port` there is no standalone Prometheus listener to configure, so a
    # broken rule in `<prometheus.handlers>` must not keep the server down. This is a lock on the
    # port check that keeps the parse out of that path.
    node.stop_clickhouse()

    node.copy_file_to_container(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "configs/bad_prometheus_no_port.xml",
        ),
        BAD_PROMETHEUS_NO_PORT_CONFIG_IN_CONTAINER,
    )
    try:
        node.start_clickhouse()

        assert node.query("SELECT 1").strip() == "1"
        assert (
            node.grep_in_log(
                substring="Unknown type no_such_prometheus_type",
                filename=ERR_LOG,
                only_latest=True,
            )
            == ""
        )
    finally:
        node.exec_in_container(
            ["bash", "-c", f"rm -f {BAD_PROMETHEUS_NO_PORT_CONFIG_IN_CONTAINER}"],
            user="root",
        )
        node.restart_clickhouse()


def test_prometheus_handler_config_error_on_reload_is_reported_to_the_client(
    start_cluster,
):
    # A reload re-enters the Prometheus factory before `createServer` can early-return on the
    # already-running listener, so a broken `<prometheus.handlers>` must reach the
    # `SYSTEM RELOAD CONFIG` caller. It used to be accepted silently, leaving the running listener
    # on the superseded configuration with nothing reported anywhere.
    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "configs/good_prometheus.xml"
        ),
        GOOD_PROMETHEUS_CONFIG_IN_CONTAINER,
    )
    node.start_clickhouse()
    try:
        # The standalone listener has to be serving before the reload, otherwise the case would
        # pass against a build that never brought it up and would no longer reach the early return
        # for an already-running listener that the reload has to get past.
        assert _prometheus_metrics_status() == 200

        # Same file, so this replaces the valid section rather than adding a second one.
        node.copy_file_to_container(
            os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "configs/bad_prometheus.xml",
            ),
            GOOD_PROMETHEUS_CONFIG_IN_CONTAINER,
        )
        error = node.query_and_get_error("SYSTEM RELOAD CONFIG")

        # Asserted on the error returned to the caller, so unrelated log contents cannot satisfy it.
        assert "Unknown type no_such_prometheus_type" in error
        assert "Listen" not in error
        assert "NETWORK_ERROR" not in error

        # The reload failing must not take the server with it.
        assert node.query("SELECT 1").strip() == "1"
    finally:
        node.exec_in_container(
            ["bash", "-c", f"rm -f {GOOD_PROMETHEUS_CONFIG_IN_CONTAINER}"],
            user="root",
        )
        node.restart_clickhouse()


def _with_config(name, path_in_container):
    node.stop_clickhouse()
    node.copy_file_to_container(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "configs/" + name),
        path_in_container,
    )


def _without_config(path_in_container):
    node.exec_in_container(["bash", "-c", f"rm -f {path_in_container}"], user="root")
    node.start_clickhouse()


# The server runs Keeper embedded, and its control listeners are a separate copy of the
# construction from the standalone entrypoint, so they need their own cases here. The
# stateless test covers `clickhouse-keeper`, which this cannot reach.
def test_keeper_control_handler_config_error_is_not_reported_as_a_listen_failure(
    start_cluster,
):
    _with_config("bad_keeper_control.xml", BAD_KEEPER_CONTROL_CONFIG_IN_CONTAINER)
    try:
        node.start_clickhouse(expected_to_fail=True)

        reported = node.grep_in_log(
            substring="Unknown handler type", filename=ERR_LOG, only_latest=True
        )
        assert reported != ""
        assert "Listen" not in reported
        assert "NETWORK_ERROR" not in reported
    finally:
        _without_config(BAD_KEEPER_CONTROL_CONFIG_IN_CONTAINER)


# The secure control listener is built by its own guarded copy, so moving only that one back
# inside its callback has to be caught. No TLS material is configured: the handler section is
# read before any socket work, which is what this asserts.
def test_keeper_secure_control_handler_config_error_is_not_reported_as_a_listen_failure(
    start_cluster,
):
    _with_config(
        "bad_keeper_control_secure.xml",
        BAD_KEEPER_CONTROL_SECURE_CONFIG_IN_CONTAINER,
    )
    try:
        node.start_clickhouse(expected_to_fail=True)

        reported = node.grep_in_log(
            substring="Unknown handler type", filename=ERR_LOG, only_latest=True
        )
        assert reported != ""
        assert "Listen" not in reported
        assert "NETWORK_ERROR" not in reported
    finally:
        _without_config(BAD_KEEPER_CONTROL_SECURE_CONFIG_IN_CONTAINER)


# Without a control port there is no listener to configure, so a broken rule must not keep the
# server down. This is the lock on the port check that keeps the parse out of that path.
def test_keeper_control_handler_config_is_not_read_without_a_control_port(start_cluster):
    _with_config(
        "bad_keeper_control_no_port.xml",
        BAD_KEEPER_CONTROL_NO_PORT_CONFIG_IN_CONTAINER,
    )

    try:
        node.start_clickhouse()
        assert node.query("SELECT 1").strip() == "1"
        assert node.grep_in_log(
            substring="Unknown handler type", filename=ERR_LOG, only_latest=True
        ) == ""
    finally:
        _without_config(BAD_KEEPER_CONTROL_NO_PORT_CONFIG_IN_CONTAINER)
