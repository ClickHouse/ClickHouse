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


def curl_ok(cert_name):
    return (
        node.exec_in_container(
            [
                "curl",
                "--silent",
                "--cacert",
                f"/etc/clickhouse-server/config.d/{cert_name}.crt",
                "https://localhost:8443/",
            ]
        )
        == "Ok.\n"
    )


def test_mismatched_pair_is_rejected():
    """Replacing only the key of a cert/key pair must keep serving the previous pair until the cert is replaced too."""
    config_dir = "/etc/clickhouse-server/config.d"
    node.exec_in_container(
        ["bash", "-c", f"cp {config_dir}/first.crt {config_dir}/cur.crt && cp {config_dir}/first.key {config_dir}/cur.key"]
    )
    change_config_to_key("cur")
    assert curl_ok("first")

    # Only the key is rotated: the pair is inconsistent.
    node.exec_in_container(["bash", "-c", f"cp {config_dir}/second.key {config_dir}/cur.key"])
    node.query("SYSTEM RELOAD CONFIG")
    assert node.contains_in_log("does not match the certificate (")
    assert curl_ok("first")

    # The cert follows: the new pair is accepted.
    node.exec_in_container(["bash", "-c", f"cp {config_dir}/second.crt {config_dir}/cur.crt"])
    node.query("SYSTEM RELOAD CONFIG")
    assert curl_ok("second")


CONFIG_DIR = "/etc/clickhouse-server/config.d"


def run_in_node(command):
    node.exec_in_container(["bash", "-c", command])


def use_pair_in_place(name):
    """Overwrite `cur.crt` and `cur.key` in place (no rename) with the pair `name`."""
    run_in_node(
        f"cat {CONFIG_DIR}/{name}.crt > {CONFIG_DIR}/cur.crt && cat {CONFIG_DIR}/{name}.key > {CONFIG_DIR}/cur.key"
    )


@pytest.mark.parametrize(
    "broken_file, break_command, expected_log",
    [
        ("crt", ": >", "PEM_read_bio_X509 failed"),
        ("key", ": >", "Failed to load key"),
        ("crt", "head -c 100 {dir}/second.crt >", "PEM_read_bio_X509 failed"),
        ("key", "head -c 100 {dir}/second.key >", "Failed to load key"),
    ],
    ids=["empty_cert", "empty_key", "truncated_cert", "truncated_key"],
)
def test_partially_written_file_keeps_previous_pair(broken_file, break_command, expected_log):
    """
    A file observed in the middle of an in-place rewrite (empty or truncated)
    must not replace the working pair, and the complete rewrite must be picked up afterwards.
    """
    use_pair_in_place("first")
    change_config_to_key("cur")
    assert curl_ok("first")

    run_in_node(f"{break_command.format(dir=CONFIG_DIR)} {CONFIG_DIR}/cur.{broken_file}")
    node.query("SYSTEM RELOAD CONFIG")
    assert node.contains_in_log(expected_log)
    assert curl_ok("first")

    use_pair_in_place("second")
    node.query("SYSTEM RELOAD CONFIG")
    assert curl_ok("second")
