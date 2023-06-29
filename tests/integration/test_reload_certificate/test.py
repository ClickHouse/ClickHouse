import pytest
import os
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
<?xml version="1.0"?>
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
