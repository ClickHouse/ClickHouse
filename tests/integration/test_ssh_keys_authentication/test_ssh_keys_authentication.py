import os

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("node", user_configs=["configs/users.xml"])

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def execute(arguments, environment=""):
    command = environment + " " + "/usr/bin/clickhouse client -q \"SELECT currentUser()\"" + " " + arguments
    return instance.exec_in_container(['sh', '-c', command], user='root').strip()


def test_ecdsa():
    assert execute(
        arguments=f"--user john --ssh-key-file {SCRIPT_DIR}/keys/ecdsa --ssh-key-passphrase \"\""
    ) == "john"


def test_ed25519():
    assert execute(
        arguments=f"--user john --ssh-key-file {SCRIPT_DIR}/keys/ed25519 --ssh-key-passphrase \"\""
    ) == "john"


def test_rsa():
    assert execute(
        arguments=f"--user john --ssh-key-file {SCRIPT_DIR}/keys/rsa --ssh-key-passphrase \"\""
    ) == "john"


def test_wrong_key():
    with pytest.raises(Exception) as err:
        execute(
            arguments=f"--user john --ssh-key-file {SCRIPT_DIR}/keys/wrong --ssh-key-passphrase \"\""
        )
    assert "Authentication failed" in str(err.value)


def test_key_with_passphrase():
    assert execute(
        arguments=f"--user lucy --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\""
    ) == "lucy"


def test_key_with_wrong_passphrase():
    with pytest.raises(Exception) as err:
        execute(
            arguments=f"--user lucy --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"wrong\""
        )
    assert "Can't import" in str(err.value)


def test_configuration_key():
    assert execute(
        arguments=f"--config-file {SCRIPT_DIR}/configs/client_key.xml",
        environment=f"TEST_SSH_KEY_FILE={SCRIPT_DIR}/keys/passphrase"
    ) == "lucy"


# CP AP -> AP: command line argument wins over configuration file
def test_configuration_password_argument_password():
    assert execute(
        arguments=f"--user larry --password password --config-file {SCRIPT_DIR}/configs/client_password_wrong_larry.xml"
    ) == "larry"


# CP AK -> AK: command line argument wins over configuration file
def test_configuration_password_argument_key():
    assert execute(
        arguments=f"--user lucy --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\" --config-file {SCRIPT_DIR}/configs/client_password_wrong_lucy.xml"
    ) == "lucy"


# CP EP -> CP: configuration file wins over environment variable
def test_configuration_password_environment_password():
    assert execute(
        arguments=f"--config-file {SCRIPT_DIR}/configs/client_password_larry.xml",
        environment="CLICKHOUSE_USER=larry CLICKHOUSE_PASSWORD=wrong"
    ) == "larry"


# AP AK: error if SSH key and password both specified
def test_argument_password_argument_key():
    with pytest.raises(Exception) as err:
        execute(
            arguments=f"--user larry --password foobar --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\""
        )
    assert "cannot be specified at the same time" in str(err.value)


# AP EP -> AP: command line argument wins over environment variable
def test_argument_password_environment_password():
    assert execute(
        arguments=f"--user larry --password password",
        environment="CLICKHOUSE_USER=larry CLICKHOUSE_PASSWORD=wrong"
    ) == "larry"


# AK EP -> AK: command line argument wins over environment variable
def test_argument_password_environment_password():
    assert execute(
        arguments=f"--user lucy --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\"",
        environment="CLICKHOUSE_USER=larry CLICKHOUSE_PASSWORD=wrong"
    ) == "lucy"


# CP AP AK: error if SSH key and password both specified
def test_configuration_password_argument_password_argument_key():
    with pytest.raises(Exception) as err:
        execute(
            arguments=f"--user lucy --password foobar --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\" --config-file {SCRIPT_DIR}/configs/client_password_wrong_lucy.xml"
        )
    assert "cannot be specified at the same time" in str(err.value)


# CP AP EP -> AP: command line argument wins over configuration file and environment variable
def test_configuration_password_argument_password_environment_password():
    assert execute(
        arguments=f"--user larry --password password --config-file {SCRIPT_DIR}/configs/client_password_wrong_larry.xml",
        environment="CLICKHOUSE_USER=larry CLICKHOUSE_PASSWORD=wrong"
    ) == "larry"


# CP AK EP -> AK: command line argument wins over configuration file and environment variable
def test_configuration_password_argument_key_environment_password():
    assert execute(
        arguments=f"--user lucy --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\" --config-file {SCRIPT_DIR}/configs/client_password_wrong_lucy.xml",
        environment="CLICKHOUSE_USER=lucy CLICKHOUSE_PASSWORD=wrong"
    ) == "lucy"


# AP AK EP: error if SSH key and password both specified
def test_argument_password_argument_key_environment_password():
    with pytest.raises(Exception) as err:
        execute(
            arguments=f"--user larry --password foobar --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\"",
            environment="CLICKHOUSE_USER=larry CLICKHOUSE_PASSWORD=password"
        )
    assert "cannot be specified at the same time" in str(err.value)


# CP AP AK EP: error if SSH key and password both specified
def test_configuration_password_argument_password_argument_key_environment_password():
    with pytest.raises(Exception) as err:
        execute(
            arguments=f"--user larry --password foobar --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\" --config-file {SCRIPT_DIR}/configs/client_password_wrong_larry.xml",
            environment="CLICKHOUSE_USER=larry CLICKHOUSE_PASSWORD=password"
        )
    assert "cannot be specified at the same time" in str(err.value)


# CK AP -> AP: command line argument wins over configuration file
def test_configuration_key_argument_password():
    assert execute(
        arguments=f"--user larry --password password --config-file {SCRIPT_DIR}/configs/client_key.xml",
        environment=f"TEST_SSH_KEY_FILE={SCRIPT_DIR}/keys/passphrase"
    ) == "larry"


# CK AK -> AK: command line argument wins over configuration file
def test_configuration_key_argument_key():
    assert execute(
        arguments=f"--user lucy --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\" --config-file {SCRIPT_DIR}/configs/client_key_wrong.xml",
        environment=f"TEST_SSH_KEY_FILE={SCRIPT_DIR}/keys/passphrase"
    ) == "lucy"


# CP CK: error if SSH key and password both specified
def test_configuration_password_configuration_key():
    with pytest.raises(Exception) as err:
        execute(
            arguments=f"--config-file {SCRIPT_DIR}/configs/client_key_and_password.xml",
            environment=f"TEST_SSH_KEY_FILE={SCRIPT_DIR}/keys/passphrase"
        )
    assert "cannot be specified at the same time" in str(err.value)


# CK EP -> CK: configuration file wins over environment variable
def test_configuration_key_environment_password():
    assert execute(
        arguments=f"--config-file {SCRIPT_DIR}/configs/client_key.xml",
        environment=f"TEST_SSH_KEY_FILE={SCRIPT_DIR}/keys/passphrase CLICKHOUSE_USER=larry CLICKHOUSE_PASSWORD=password"
    ) == "lucy"


# CK AP EP -> AP: command line argument wins over configuration file and environment variable
def test_configuration_key_argument_password_environment_password():
    assert execute(
        arguments=f"--user larry --password password --config-file {SCRIPT_DIR}/configs/client_key.xml",
        environment=f"TEST_SSH_KEY_FILE={SCRIPT_DIR}/keys/passphrase CLICKHOUSE_USER=larry CLICKHOUSE_PASSWORD=wrong"
    ) == "larry"


# CP CK AK: error if SSH key and password both specified
def test_configuration_password_configuration_key_argument_key():
    with pytest.raises(Exception) as err:
        execute(
            arguments=f"--user lucy --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\" --config-file {SCRIPT_DIR}/configs/client_key_and_password.xml",
            environment=f"TEST_SSH_KEY_FILE={SCRIPT_DIR}/keys/passphrase"
        )
    assert "cannot be specified at the same time" in str(err.value)


# CP CK AP: error if SSH key and password both specified
def test_configuration_password_configuration_key_argument_password():
    with pytest.raises(Exception) as err:
        execute(
            arguments=f"--user larry --password password --config-file {SCRIPT_DIR}/configs/client_key_and_password.xml",
            environment=f"TEST_SSH_KEY_FILE={SCRIPT_DIR}/keys/passphrase"
        )
    assert "cannot be specified at the same time" in str(err.value)


# CK AP AK: error if SSH key and password both specified
def test_configuration_key_argument_password_argument_key():
    with pytest.raises(Exception) as err:
        execute(
            arguments=f"--user lucy --password foobar --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\" --config-file {SCRIPT_DIR}/configs/client_key.xml",
            environment=f"TEST_SSH_KEY_FILE={SCRIPT_DIR}/keys/passphrase"
        )
    assert "cannot be specified at the same time" in str(err.value)


# CK AK EP -> AK: command line argument wins over configuration file and environment variable
def test_configuration_key_argument_key_environment_password():
    assert execute(
        arguments=f"--user lucy --ssh-key-file {SCRIPT_DIR}/keys/passphrase --ssh-key-passphrase \"passphrase\" --config-file {SCRIPT_DIR}/configs/client_key_wrong.xml",
        environment=f"TEST_SSH_KEY_FILE={SCRIPT_DIR}/keys/passphrase CLICKHOUSE_USER=larry CLICKHOUSE_PASSWORD=wrong"
    ) == "lucy"
