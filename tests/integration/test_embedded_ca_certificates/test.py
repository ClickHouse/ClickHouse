"""Test the CA certificates embedded into the binary.

When no CA certificates can be found on the filesystem (e.g. in a container
built "from scratch"), the certificates embedded into the binary at build time
are used, instead of failing to create every TLS context.
"""

import pytest
import requests
import urllib3

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/ssl_config.xml",
        "configs/server-cert.pem",
        "configs/server-key.pem",
    ],
    stay_alive=True,
)

# The locations probed by Poco::Net::Context for default CA certificates.
CA_LOCATIONS = [
    "/etc/ssl",
    "/etc/pki",
    "/etc/certs",
    "/etc/openssl",
    "/usr/local/etc/ssl",
    "/usr/local/share/certs",
]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def https_ping():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = requests.get(
        f"https://{node.ip_address}:8443/ping", verify=False, timeout=10
    )
    response.raise_for_status()
    return response.text


def test_embedded_ca_certificates(started_cluster):
    # With CA certificates present on the filesystem, they are used,
    # and the embedded ones are not.
    assert https_ping() == "Ok.\n"
    assert (
        node.query(
            "SELECT count() FROM system.certificates WHERE path = '(embedded)'"
        ).strip()
        == "0"
    )

    # Remove every location of CA certificates from the container.
    node.exec_in_container(
        ["bash", "-c", "rm -rf " + " ".join(CA_LOCATIONS)], privileged=True, user="root"
    )
    node.restart_clickhouse()

    # TLS still works: the certificates embedded into the binary are used.
    # Before they existed, the creation of every TLS context threw
    # "Cannot load default CA certificates", and the HTTPS listener did not start.
    assert https_ping() == "Ok.\n"
    assert (
        int(
            node.query(
                "SELECT count() FROM system.certificates WHERE path = '(embedded)'"
            ).strip()
        )
        > 100
    )

    # An existing but empty certificate directory must also engage the fallback:
    # a directory without hash-named files can never yield a certificate at
    # verification time, so it does not count as certificates being present.
    node.exec_in_container(
        ["bash", "-c", "mkdir -p /etc/ssl/certs"], privileged=True, user="root"
    )
    node.restart_clickhouse()

    assert https_ping() == "Ok.\n"
    assert (
        int(
            node.query(
                "SELECT count() FROM system.certificates WHERE path = '(embedded)'"
            ).strip()
        )
        > 100
    )
