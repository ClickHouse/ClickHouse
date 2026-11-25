import base64
import http.client
import ssl
from pathlib import Path

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.ssl_context import WrapSSLContextWithSNI
from helpers.test_tools import assert_eq_with_retry


CERT_DIR = Path(__file__).resolve().parent / "certs"
INTERSERVER_HTTPS_PORT = 9010
ENDPOINT = "/?endpoint=auth-probe&compress=false"
COMMON_CONFIGS = [
    "configs/00_ssl.xml",
    "certs/server-cert.pem",
    "certs/server-key.pem",
    "certs/ca-cert.pem",
]

cluster = ClickHouseCluster(__file__)
certificate_only = cluster.add_instance(
    "certificate_only",
    main_configs=[
        *COMMON_CONFIGS,
        "configs/certificate_only.xml",
        "certs/valid-a-cert.pem",
        "certs/valid-a-key.pem",
        "certs/wrong-san-cert.pem",
        "certs/wrong-san-key.pem",
        "certs/untrusted-cert.pem",
    ],
    with_zookeeper=True,
    use_keeper=False,
)
mixed = cluster.add_instance(
    "mixed",
    main_configs=[
        *COMMON_CONFIGS,
        "configs/mixed.xml",
        "certs/valid-b-cert.pem",
        "certs/valid-b-key.pem",
    ],
    with_zookeeper=True,
    use_keeper=False,
)
basic_only = cluster.add_instance(
    "basic_only",
    main_configs=[*COMMON_CONFIGS, "configs/basic_only.xml"],
    with_zookeeper=True,
    use_keeper=False,
)
certificate_and_anonymous = cluster.add_instance(
    "certificate_and_anonymous",
    main_configs=[
        *COMMON_CONFIGS,
        "configs/certificate_and_anonymous.xml",
        "certs/valid-a-cert.pem",
        "certs/valid-a-key.pem",
    ],
    use_keeper=False,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def request(node, cert_name=None, basic=False, basic_user="replica", basic_password="secret"):
    context = WrapSSLContextWithSNI("interserver.test", ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cafile=str(CERT_DIR / "ca-cert.pem"))
    context.check_hostname = True
    if cert_name:
        context.load_cert_chain(
            str(CERT_DIR / f"{cert_name}-cert.pem"),
            str(CERT_DIR / f"{cert_name}-key.pem"),
        )

    headers = {}
    if basic:
        token = base64.b64encode(f"{basic_user}:{basic_password}".encode()).decode("ascii")
        headers["Authorization"] = f"Basic {token}"

    connection = http.client.HTTPSConnection(
        node.ip_address, INTERSERVER_HTTPS_PORT, context=context, timeout=10
    )
    try:
        connection.request("GET", ENDPOINT, headers=headers)
        response = connection.getresponse()
        return response.status, response.read().decode("utf-8")
    finally:
        connection.close()


def assert_accepted(node, **kwargs):
    # Authentication precedes endpoint lookup. This error proves authentication passed.
    status, body = request(node, **kwargs)
    assert status == 500, body
    assert "No interserver IO endpoint named auth-probe" in body


def assert_rejected(node, expected_message, **kwargs):
    status, body = request(node, **kwargs)
    assert status == 401, body
    assert expected_message in body


def test_interserver_mtls_authentication():
    # Either configured SAN or the legacy CN may authenticate. Unlisted SANs do not prevent a match.
    assert_rejected(certificate_only, "requires a matching SSL certificate")
    assert_accepted(certificate_only, cert_name="valid-a")
    assert_accepted(certificate_only, cert_name="valid-b")
    assert_accepted(certificate_only, cert_name="cn-only")
    assert_accepted(certificate_only, cert_name="valid-a", basic=True)
    assert_rejected(
        certificate_only,
        "requires a matching SSL certificate",
        cert_name="wrong-san",
        basic=True,
    )
    assert_rejected(
        certificate_only,
        "requires a matching SSL certificate",
        cert_name="no-subjects",
    )
    assert_rejected(
        certificate_only,
        "requires a matching SSL certificate",
        basic=True,
    )

    # A matching certificate takes precedence; Basic is checked if the certificate does not match.
    assert_accepted(mixed, cert_name="valid-b")
    assert_accepted(mixed, cert_name="valid-a", basic=True)
    assert_accepted(
        mixed,
        cert_name="valid-a",
        basic=True,
        basic_password="wrong",
    )
    assert_accepted(mixed, cert_name="wrong-san", basic=True)
    assert_accepted(mixed, cert_name="no-subjects", basic=True)
    assert_accepted(mixed, basic=True)
    assert_rejected(
        mixed,
        "Incorrect user or password",
        cert_name="wrong-san",
        basic=True,
        basic_password="wrong",
    )
    assert_rejected(
        mixed,
        "requires HTTP Basic authentication",
        cert_name="wrong-san",
    )
    assert_rejected(mixed, "requires HTTP Basic authentication", cert_name="no-subjects")
    assert_rejected(mixed, "requires HTTP Basic authentication")

    # The CA rejects an untrusted certificate before application authentication runs.
    with pytest.raises((ssl.SSLError, OSError)):
        request(mixed, cert_name="untrusted", basic=True)

    # A Basic-only receiver ignores a presented certificate.
    assert_accepted(basic_only, basic=True)
    assert_accepted(basic_only, cert_name="valid-a", basic=True)
    assert_rejected(
        basic_only,
        "requires HTTP Basic authentication",
        cert_name="valid-a",
    )

    # allow_empty accepts anonymous requests alongside certificate authentication.
    assert_accepted(certificate_and_anonymous)
    assert_accepted(certificate_and_anonymous, cert_name="valid-a")
    assert_accepted(certificate_and_anonymous, cert_name="wrong-san")
    assert_rejected(certificate_and_anonymous, "Incorrect user or password", basic=True)



def test_interserver_mtls_part_fetch():
    # certificate_only's global client certificate has a rejected SAN and trusts the wrong CA.
    # Its interserver client overrides both the identity and CA, while inheriting other TLS settings.
    assert_rejected(mixed, "requires HTTP Basic authentication", cert_name="wrong-san")

    for replica in (certificate_only, mixed):
        replica.query(
            f"CREATE TABLE mtls_parts (id UInt64) "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/interserver_mtls/parts', '{replica.name}') ORDER BY id"
        )

    certificate_only.query("INSERT INTO mtls_parts VALUES (1)")
    assert_eq_with_retry(mixed, "SELECT id FROM mtls_parts ORDER BY id", "1")

    mixed.query("INSERT INTO mtls_parts VALUES (2)")
    assert_eq_with_retry(certificate_only, "SELECT id FROM mtls_parts ORDER BY id", "1\n2")

    # The mixed replica sends Basic and a certificate to a Basic-only receiver.
    for replica in (mixed, basic_only):
        replica.query(
            f"CREATE TABLE basic_parts (id UInt64) "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/interserver_mtls/basic_parts', '{replica.name}') ORDER BY id"
        )

    mixed.query("INSERT INTO basic_parts VALUES (10)")
    assert_eq_with_retry(basic_only, "SELECT id FROM basic_parts ORDER BY id", "10")

    basic_only.query("INSERT INTO basic_parts VALUES (20)")
    assert_eq_with_retry(mixed, "SELECT id FROM basic_parts ORDER BY id", "10\n20")


def test_interserver_client_certificate_rotation():
    for replica in (certificate_only, mixed):
        replica.query(
            f"CREATE TABLE rotation_parts (id UInt64) "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/interserver_mtls/rotation_parts', '{replica.name}') "
            "ORDER BY id SETTINGS max_postpone_time_for_failed_replicated_fetches_ms = 0"
        )

    config_path = "/etc/clickhouse-server/config.d/certificate_only.xml"
    original = (Path(__file__).parent / "configs/certificate_only.xml").read_text()
    rotated = original.replace("valid-a-cert.pem", "wrong-san-cert.pem").replace(
        "valid-a-key.pem", "wrong-san-key.pem"
    )

    certificate_only.replace_config(config_path, rotated)
    try:
        certificate_only.query("SYSTEM RELOAD CONFIG")
        mixed.query("INSERT INTO rotation_parts VALUES (1)")
        assert_eq_with_retry(
            certificate_only,
            "SELECT count() > 0 FROM system.replication_queue "
            "WHERE database = currentDatabase() AND table = 'rotation_parts' "
            "AND position(last_exception, 'HTTP status code: 401') > 0",
            "1",
        )
    finally:
        certificate_only.replace_config(config_path, original)
        certificate_only.query("SYSTEM RELOAD CONFIG")

    assert_eq_with_retry(certificate_only, "SELECT id FROM rotation_parts", "1")


def test_legacy_basic_configuration_remains_accepted():
    config_path = "/etc/clickhouse-server/config.d/basic_only.xml"
    original = (Path(__file__).parent / "configs/basic_only.xml").read_text()

    try:
        basic_only.replace_config(config_path, "<clickhouse/>")
        basic_only.query("SYSTEM RELOAD CONFIG")
        assert_accepted(basic_only)
        assert_rejected(basic_only, "Incorrect user or password", basic=True)

        basic_only.replace_config(
            config_path,
            "<clickhouse><interserver_http_credentials><user>replica</user></interserver_http_credentials></clickhouse>",
        )
        basic_only.query("SYSTEM RELOAD CONFIG")
        assert_accepted(basic_only, basic=True, basic_password="")
        assert_rejected(basic_only, "requires HTTP Basic authentication")

        basic_only.replace_config(
            config_path,
            "<clickhouse><interserver_http_credentials><password>secret</password></interserver_http_credentials></clickhouse>",
        )
        basic_only.query("SYSTEM RELOAD CONFIG")
        assert_accepted(basic_only, basic=True, basic_user="")
        assert_rejected(basic_only, "requires HTTP Basic authentication")
    finally:
        basic_only.replace_config(config_path, original)
        basic_only.query("SYSTEM RELOAD CONFIG")


def test_interserver_client_certificate_file_watcher():
    for replica in (certificate_only, mixed):
        replica.query(
            f"CREATE TABLE watched_parts (id UInt64) "
            f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/interserver_mtls/watched_parts', '{replica.name}') "
            "ORDER BY id SETTINGS max_postpone_time_for_failed_replicated_fetches_ms = 0"
        )

    config_dir = "/etc/clickhouse-server/config.d"
    active_cert = f"{config_dir}/valid-a-cert.pem"
    active_key = f"{config_dir}/valid-a-key.pem"
    backup_dir = f"{config_dir}/tmp"
    certificate_only.exec_in_container(["mkdir", "-p", backup_dir], user="root")
    certificate_only.exec_in_container(["cp", active_cert, f"{backup_dir}/valid-a-cert.pem"], user="root")
    certificate_only.exec_in_container(["cp", active_key, f"{backup_dir}/valid-a-key.pem"], user="root")

    mixed_config_path = f"{config_dir}/mixed.xml"
    mixed_original = (Path(__file__).parent / "configs/mixed.xml").read_text()
    mixed_rotated = mixed_original.replace("replica-a", "wrong").replace("replica-b", "wrong")

    certificate_only.query("SYSTEM STOP FETCHES watched_parts")
    try:
        mixed.replace_config(mixed_config_path, mixed_rotated)
        mixed.query("SYSTEM RELOAD CONFIG")
        assert_rejected(mixed, "requires HTTP Basic authentication", cert_name="valid-a")
        assert_accepted(mixed, cert_name="wrong-san")

        mixed.query("INSERT INTO watched_parts VALUES (1)")
        certificate_only.exec_in_container(["cp", f"{config_dir}/wrong-san-cert.pem", active_cert], user="root")
        certificate_only.exec_in_container(["cp", f"{config_dir}/wrong-san-key.pem", active_key], user="root")
        certificate_only.query("SYSTEM START FETCHES watched_parts")
        assert_eq_with_retry(certificate_only, "SELECT id FROM watched_parts", "1", retry_count=60)
    finally:
        mixed.replace_config(mixed_config_path, mixed_original)
        mixed.query("SYSTEM RELOAD CONFIG")
        certificate_only.exec_in_container(["cp", f"{backup_dir}/valid-a-cert.pem", active_cert], user="root")
        certificate_only.exec_in_container(["cp", f"{backup_dir}/valid-a-key.pem", active_key], user="root")
        certificate_only.query("SYSTEM RELOAD CONFIG")
        certificate_only.query("SYSTEM START FETCHES watched_parts")
