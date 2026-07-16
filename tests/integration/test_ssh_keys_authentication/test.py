import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "node",
    user_configs=["configs/users.xml"],
)

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_ecdsa():
    # ECDSA is FIPS-approved, so this works on FIPS and non-FIPS builds.
    assert (
        instance.query(
            "SELECT currentUser()",
            user="john",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/ecdsa",
                "ssh-key-passphrase": "",
            },
        )
        == "john\n"
    )


def test_ed25519():
    try:
        result = instance.query(
            "SELECT currentUser()",
            user="john",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/ed25519",
                "ssh-key-passphrase": "",
            },
        )
    except Exception as err:
        # On a FIPS-enabled client, the Ed25519 key is rejected while it is imported
        # locally in clickhouse-client (ConnectionParameters -> SSHKeyFactory), before
        # any network authentication, so the failure is a client-side LIBSSH_ERROR, not
        # "Authentication failed". Keying off the client-side error (instead of probing
        # the remote server's FIPS mode) is correct even when a FIPS client talks to a
        # non-FIPS server.
        assert "Ed25519 SSH keys are not supported in FIPS mode" in str(err)
        return

    # Non-FIPS client: the Ed25519 key is accepted and authentication succeeds.
    assert result == "john\n"


def test_rsa():
    # RSA is FIPS-approved, so this works on FIPS and non-FIPS builds.
    assert (
        instance.query(
            "SELECT currentUser()",
            user="john",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/rsa",
                "ssh-key-passphrase": "",
            },
        )
        == "john\n"
    )


def test_wrong_key():
    with pytest.raises(Exception) as err:
        instance.query(
            "SELECT currentUser()",
            user="john",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/wrong",
                "ssh-key-passphrase": "",
            },
        )

    assert "Authentication failed" in str(err.value)


def test_key_with_passphrase():
    # Use a passphrase-protected RSA key (FIPS-approved) so the passphrase logic is
    # exercised on both FIPS and non-FIPS builds. lucy also has an Ed25519 key that
    # is dropped by UsersConfigParser on FIPS builds; the RSA key keeps her usable.
    assert (
        instance.query(
            "SELECT currentUser()",
            user="lucy",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/passphrase_rsa",
                "ssh-key-passphrase": "passphrase",
            },
        )
        == "lucy\n"
    )


def test_key_with_wrong_passphrase():
    with pytest.raises(Exception):
        instance.query(
            "SELECT currentUser()",
            user="lucy",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/passphrase_rsa",
                "ssh-key-passphrase": "wrong",
            },
        ) == "lucy\n"
