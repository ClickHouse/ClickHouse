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


def server_in_fips_mode():
    # MD5 is rejected with SUPPORT_IS_DISABLED under OpenSSL FIPS mode, using the
    # same isFIPSEnabled() signal that filters out Ed25519 SSH keys. If the query
    # succeeds the server is not in FIPS mode.
    try:
        instance.query("SELECT MD5('')")
        return False
    except Exception:
        return True


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
    if server_in_fips_mode():
        # Ed25519 is not FIPS-approved: it is rejected before it reaches libssh,
        # so authentication with the Ed25519 key must fail on FIPS builds.
        with pytest.raises(Exception) as err:
            instance.query(
                "SELECT currentUser()",
                user="john",
                settings={
                    "ssh-key-file": f"{SCRIPT_DIR}/keys/ed25519",
                    "ssh-key-passphrase": "",
                },
            )
        assert "Authentication failed" in str(err.value)
        return

    assert (
        instance.query(
            "SELECT currentUser()",
            user="john",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/ed25519",
                "ssh-key-passphrase": "",
            },
        )
        == "john\n"
    )


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
