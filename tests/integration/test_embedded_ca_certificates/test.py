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
# A node with an explicitly configured `caConfig`: the embedded bundle must not be added
# to the trust store it defines.
node_ca_config = cluster.add_instance(
    "node_ca_config",
    main_configs=[
        "configs/ssl_config_ca_config.xml",
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


def default_ca_file():
    # `SSL_CERT_FILE` overrides the compiled-in default CA file (`/etc/ssl/cert.pem`,
    # as `OPENSSLDIR` of the bundled OpenSSL is `/etc/ssl`). Some cluster flavors
    # (e.g. "db disk") bring up minio, whose setup injects `SSL_CERT_FILE` into every
    # container of the cluster, so the effective path is read from the environment of
    # the container instead of being hardcoded.
    return node.exec_in_container(
        ["bash", "-c", 'echo -n "${SSL_CERT_FILE:-/etc/ssl/cert.pem}"']
    )


def https_ping(instance=None):
    instance = instance or node
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = requests.get(
        f"https://{instance.ip_address}:8443/ping", verify=False, timeout=10
    )
    response.raise_for_status()
    return response.text


def remove_ca_locations(instance):
    instance.exec_in_container(
        ["bash", "-c", "rm -rf " + " ".join(CA_LOCATIONS)], privileged=True, user="root"
    )


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

    # The outbound (client) TLS context is created from the same embedded certificates.
    # Verification of the self-signed certificate of the node fails, but the context is
    # created at all: without the embedded certificates, the creation of the client
    # context threw "Cannot load default CA certificates" before anything was sent.
    error = node.query_and_get_error(
        f"SELECT * FROM url('https://{node.ip_address}:8443/ping', LineAsString)"
    )
    assert "Cannot load default CA certificates" not in error
    assert "certificate" in error.lower()

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

    # An existing but empty default CA file must also engage the fallback:
    # `SSL_CTX_set_default_verify_paths` reports success for it while silently
    # yielding an empty trust store, so the file only counts as certificates
    # being present when at least one certificate can actually be loaded from it.
    ca_file = default_ca_file()
    node.exec_in_container(
        ["bash", "-c", f"mkdir -p $(dirname {ca_file}) && touch {ca_file}"],
        privileged=True,
        user="root",
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


def test_default_ca_file_does_not_shadow_default_dir(started_cluster):
    # A split trust store: a valid default CA file plus a root that exists only in the
    # default CA directory. Loading the file must not shadow the directory - OpenSSL's
    # own `SSL_CTX_set_default_verify_paths` loads both, and roots that exist only in
    # the directory must stay in the trust store.
    #
    # The server certificate doubles as the extra root: it is copied both to the
    # default CA file and, under its OpenSSL subject-hash name ("f1a05c1a.0", so that
    # the directory counts as containing certificates), to the default CA directory.
    cert = "/etc/clickhouse-server/config.d/server-cert.pem"
    ca_file = default_ca_file()
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p /etc/ssl/certs $(dirname {ca_file}) && cp {cert} {ca_file}"
            f" && cp {cert} /etc/ssl/certs/f1a05c1a.0",
        ],
        privileged=True,
        user="root",
    )
    node.restart_clickhouse()

    assert https_ping() == "Ok.\n"

    # The default CA file was loaded...
    assert (
        int(
            node.query(
                f"SELECT count() FROM system.certificates WHERE path = '{ca_file}'"
            ).strip()
        )
        > 0
    )
    # ...and the default CA directory was loaded as well, not shadowed by the file.
    assert (
        int(
            node.query(
                "SELECT count() FROM system.certificates WHERE path LIKE '/etc/ssl/certs%'"
            ).strip()
        )
        > 0
    )
    # Certificates were found on the filesystem, so the embedded ones are not used.
    assert (
        node.query(
            "SELECT count() FROM system.certificates WHERE path = '(embedded)'"
        ).strip()
        == "0"
    )


def test_unloadable_hash_named_file_engages_fallback(started_cluster):
    # A hash-named entry of the default CA directory that OpenSSL cannot load a certificate
    # from - a zero-byte placeholder or a stale symlink - must not count as certificates
    # being present: `SSL_CTX_load_verify_locations` succeeds for such a directory while the
    # trust store stays empty, which is exactly the handshake-time failure the fallback
    # exists to prevent.
    remove_ca_locations(node)
    node.exec_in_container(
        [
            "bash",
            "-c",
            "mkdir -p /etc/ssl/certs && : > /etc/ssl/certs/deadbeef.0",
        ],
        privileged=True,
        user="root",
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


def test_ca_config_is_not_widened_by_embedded_certificates(started_cluster):
    # `caConfig` defines the trust store the deployment asked for. The embedded bundle is a
    # substitute for a missing filesystem trust store, not an addition to a configured one:
    # appending its public roots would silently widen the trust surface of a deployment that
    # intended to trust only its own root.
    remove_ca_locations(node_ca_config)
    node_ca_config.restart_clickhouse()

    # The server starts and serves HTTPS: a configured `caConfig` is a usable trust store,
    # so the absence of the default CA locations is not an error either.
    assert https_ping(node_ca_config) == "Ok.\n"

    # The configured root is in the trust store...
    assert (
        int(
            node_ca_config.query(
                "SELECT count() FROM system.certificates"
                " WHERE path = '/etc/clickhouse-server/config.d/server-cert.pem'"
            ).strip()
        )
        > 0
    )
    # ...and the embedded certificates are not.
    assert (
        node_ca_config.query(
            "SELECT count() FROM system.certificates WHERE path = '(embedded)'"
        ).strip()
        == "0"
    )

    # The outbound (client) context is created from `caConfig` alone as well: without a usable
    # trust store its creation threw `Cannot load default CA certificates` before anything was
    # sent, and the certificates that make the connection verify come from `caConfig`, not from
    # the embedded bundle - `system.certificates` above shows none of the latter.
    answer, error = node_ca_config.query_and_get_answer_with_error(
        f"SELECT * FROM url('https://{node_ca_config.ip_address}:8443/ping', LineAsString)"
    )
    assert "Cannot load default CA certificates" not in error
    assert answer.strip() == "Ok." or "certificate" in error.lower()
