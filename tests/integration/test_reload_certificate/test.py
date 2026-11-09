import os

import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/first.crt",
        "configs/first.key",
        "configs/second.crt",
        "configs/second.key",
        "configs/ECcert.crt",
        "configs/ECcert.key",
        "configs/WithChain.crt",
        "configs/WithChain.key",
        "configs/WithPassPhrase.crt",
        "configs/WithPassPhrase.key",
        "configs/cert.xml",
    ],
)
PASS_PHRASE_TEMPLATE = """<privateKeyPassphraseHandler>
                <name>KeyFileHandler</name>
                <options>
                <password>{pass_phrase}</password>
                </options>
            </privateKeyPassphraseHandler>
"""


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def change_config_to_key(name, pass_phrase=""):
    """
    * Generate config with certificate/key name from args.
    * Reload config.
    """
    node.exec_in_container(
        [
            "bash",
            "-c",
            """cat > /etc/clickhouse-server/config.d/cert.xml << EOF
<clickhouse>
    <https_port>8443</https_port>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/config.d/{cur_name}.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/config.d/{cur_name}.key</privateKeyFile>
            <loadDefaultCAFile>true</loadDefaultCAFile>
            <cacheSessions>true</cacheSessions>
            <disableProtocols>sslv2,sslv3</disableProtocols>
            <preferServerCiphers>true</preferServerCiphers>
            {pass_phrase}
        </server>
    </openSSL>
</clickhouse>
EOF""".format(
                cur_name=name, pass_phrase=pass_phrase
            ),
        ]
    )
    node.query("SYSTEM RELOAD CONFIG")


def check_certificate_switch(
    first, second, pass_phrase_first="", pass_phrase_second=""
):
    # Set first key
    change_config_to_key(first, pass_phrase_first)

    # Command with correct certificate
    assert (
        node.exec_in_container(
            [
                "curl",
                "--silent",
                "--cacert",
                "/etc/clickhouse-server/config.d/{cur_name}.crt".format(cur_name=first),
                "https://localhost:8443/",
            ]
        )
        == "Ok.\n"
    )

    # Command with wrong certificate
    # This command don't use option '-k', so it will lead to error while execution.
    # That's why except will always work
    try:
        node.exec_in_container(
            [
                "curl",
                "--silent",
                "--cacert",
                "/etc/clickhouse-server/config.d/{cur_name}.crt".format(
                    cur_name=second
                ),
                "https://localhost:8443/",
            ]
        )
        assert False
    except:
        assert True

    # Change to other key
    change_config_to_key(second, pass_phrase_second)

    # Command with correct certificate
    assert (
        node.exec_in_container(
            [
                "curl",
                "--silent",
                "--cacert",
                "/etc/clickhouse-server/config.d/{cur_name}.crt".format(
                    cur_name=second
                ),
                "https://localhost:8443/",
            ]
        )
        == "Ok.\n"
    )

    # Command with wrong certificate
    # Same as previous
    try:
        node.exec_in_container(
            [
                "curl",
                "--silent",
                "--cacert",
                "/etc/clickhouse-server/config.d/{cur_name}.crt".format(cur_name=first),
                "https://localhost:8443/",
            ]
        )
        assert False
    except:
        assert True


def test_first_than_second_cert():
    """Consistently set first key and check that only it will be accepted, then repeat same for second key."""
    check_certificate_switch("first", "second")


def test_ECcert_reload():
    """Check EC certificate"""
    check_certificate_switch("first", "ECcert")


def test_cert_with_pass_phrase():
    pass_phrase_for_cert = PASS_PHRASE_TEMPLATE.format(pass_phrase="test")
    check_certificate_switch(
        "first", "WithPassPhrase", pass_phrase_second=pass_phrase_for_cert
    )


def test_chain_reload():
    """Check cert chain reload"""
    check_certificate_switch("first", "WithChain")
    assert (
        node.exec_in_container(
            [
                "bash",
                "-c",
                "openssl s_client -showcerts -servername localhost -connect localhost:8443 </dev/null 2>/dev/null | grep 'BEGIN CERTIFICATE' | wc -l",
            ]
        )
        == "2\n"
    )


CONFIG_DIR = "/etc/clickhouse-server/config.d"


def curl_ok(cert_name):
    return (
        node.exec_in_container(
            [
                "curl",
                "--silent",
                "--cacert",
                f"{CONFIG_DIR}/{cert_name}.crt",
                "https://localhost:8443/",
            ]
        )
        == "Ok.\n"
    )


def run_in_node(command):
    node.exec_in_container(["bash", "-c", command])


def test_mismatched_pair_is_rejected():
    """Replacing only the key of a cert/key pair must keep serving the previous pair until the cert is replaced too."""
    run_in_node(f"cp {CONFIG_DIR}/first.crt {CONFIG_DIR}/cur.crt && cp {CONFIG_DIR}/first.key {CONFIG_DIR}/cur.key")
    change_config_to_key("cur")
    assert curl_ok("first")

    # Only the key is rotated: the pair is inconsistent.
    log_anchor = max(1, node.count_log_lines())
    run_in_node(f"cp {CONFIG_DIR}/second.key {CONFIG_DIR}/cur.key")
    node.query("SYSTEM RELOAD CONFIG")
    node.wait_for_log_line("does not match the certificate", look_behind_lines=f"+{log_anchor}")
    # Leftover errors of the expected end of a certificate file must not leak into the message.
    assert int(node.count_in_log("does not match the certificate.*no start line")) == 0
    assert curl_ok("first")

    # The cert follows: the new pair is accepted.
    run_in_node(f"cp {CONFIG_DIR}/second.crt {CONFIG_DIR}/cur.crt")
    node.query("SYSTEM RELOAD CONFIG")
    assert curl_ok("second")
