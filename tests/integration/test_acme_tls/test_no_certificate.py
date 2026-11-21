import logging

import pytest

from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

no_certificate_cluster = ClickHouseCluster(__file__)
node = no_certificate_cluster.add_instance(
    "node_no_certificate",
    main_configs=["configs/config_no_certificate.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module")
def started_no_certificate_cluster():
    try:
        no_certificate_cluster.start()
        yield no_certificate_cluster
    finally:
        no_certificate_cluster.shutdown()


def test_show_certificate_without_certificate(started_no_certificate_cluster):
    # The certificate is provisioned by ACME, and the ACME server is unreachable, so the server
    # runs with an SSL context that has no certificate at all. `showCertificate` used to
    # dereference a null pointer in this case.
    assert node.query("SELECT showCertificate()") == "{}\n"
    assert node.query("SELECT 1") == "1\n"
