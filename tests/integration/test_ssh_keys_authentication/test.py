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
    assert (
        instance.query(
            "SELECT currentUser()",
            user="lucy",
            settings={
                "ssh-key-file": f"{SCRIPT_DIR}/keys/passphrase",
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
                "ssh-key-file": f"{SCRIPT_DIR}/keys/passphrase",
                "ssh-key-passphrase": "wrong",
            },
        ) == "lucy\n"
