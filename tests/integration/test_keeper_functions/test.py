import re
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException


cluster = ClickHouseCluster(__file__, with_zookeeper=True)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/zookeeper_config.xml"],
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/zookeeper_config.xml"],
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# =====================================================================
# Tests for getKeeperConfig
# =====================================================================

def test_get_keeper_config_default():
    """Test getKeeperConfig() with default keeper (no arguments).
    The return value looks like:
        server.1=zoo1:9234;participant;1
        server.2=zoo2:9234;participant;1
        ...
    """
    result = node1.query("SELECT getKeeperConfig()").strip()
    assert len(result) > 0, "getKeeperConfig() should return a non-empty config string"
    # Each line should match the pattern: server.<id>=<host>:<port>;participant;<weight>
    lines = result.split("\n")
    for line in lines:
        assert re.match(r"server\.\d+=.+:\d+;participant;\d+", line.strip()), \
            f"Unexpected config line format: '{line}'"


def test_get_keeper_config_explicit_default():
    """Test getKeeperConfig('default') with explicitly specified default keeper."""
    result = node1.query("SELECT getKeeperConfig('default')").strip()
    assert len(result) > 0, "getKeeperConfig('default') should return a non-empty config string"
    # Verify the result contains server entries in the expected format
    assert "server." in result, "Config should contain 'server.' entries"
    assert "participant" in result, "Config should contain 'participant' entries"


def test_get_keeper_config_default_consistency():
    """Test that getKeeperConfig() and getKeeperConfig('default') return the same result."""
    result_no_arg = node1.query("SELECT getKeeperConfig()").strip()
    result_default = node1.query("SELECT getKeeperConfig('default')").strip()
    assert result_no_arg == result_default, \
        "getKeeperConfig() and getKeeperConfig('default') should return the same result"


def test_get_keeper_config_auxiliary_keeper():
    """Test getKeeperConfig('zookeeper2') with the auxiliary keeper."""
    result = node1.query("SELECT getKeeperConfig('zookeeper2')").strip()
    assert len(result) > 0, "getKeeperConfig('zookeeper2') should return a non-empty config string"
    # Verify the result follows the server.N=host:port;participant;weight format
    assert "server." in result, "Auxiliary keeper config should contain 'server.' entries"
    assert "participant" in result, "Auxiliary keeper config should contain 'participant' entries"


def test_get_keeper_config_different_keepers():
    """Test that default and auxiliary keepers may return different configs."""
    result_default = node1.query("SELECT getKeeperConfig()").strip()
    result_aux = node1.query("SELECT getKeeperConfig('zookeeper2')").strip()
    # Both should be non-empty and follow the server.N=... format
    assert len(result_default) > 0
    assert len(result_aux) > 0
    assert "server." in result_default
    assert "server." in result_aux


def test_get_keeper_config_nonexistent_keeper():
    """Test getKeeperConfig with a non-existent keeper name raises an error."""
    with pytest.raises(QueryRuntimeException):
        node1.query("SELECT getKeeperConfig('nonexistent_keeper')")


def test_get_keeper_config_on_different_workers():
    """Test that getKeeperConfig() returns the same result on different workers."""
    result_w1 = node1.query("SELECT getKeeperConfig()").strip()
    result_w2 = node2.query("SELECT getKeeperConfig()").strip()
    assert result_w1 == result_w2, \
        "getKeeperConfig() should return the same config from different workers"


# =====================================================================
# Tests for zookeeperFourLetterWordCommand
# =====================================================================

def test_four_letter_word_ruok():
    """Test the 'ruok' four-letter word command on the default keeper."""
    keeper_host_port = "zoo1:2181"
    result = node1.query(
        f"SELECT zookeeperFourLetterWordCommand('{keeper_host_port}', 'ruok')"
    ).strip()
    assert result == "imok", f"Expected 'imok', got '{result}'"


def test_four_letter_word_ruok_auxiliary_keeper():
    """Test the 'ruok' four-letter word command on the auxiliary keeper."""
    keeper_host_port = "zoo2:2181"
    result = node1.query(
        f"SELECT zookeeperFourLetterWordCommand('{keeper_host_port}', 'ruok')"
    ).strip()
    assert result == "imok", f"Expected 'imok', got '{result}'"


def test_four_letter_word_conf():
    """Test the 'conf' four-letter word command."""
    keeper_host_port = "zoo1:2181"
    result = node1.query(
        f"SELECT zookeeperFourLetterWordCommand('{keeper_host_port}', 'conf')"
    ).strip()
    # The 'conf' command returns configuration info, should be non-empty
    assert len(result) > 0, "'conf' command should return a non-empty string"


def test_four_letter_word_stat():
    """Test the 'stat' four-letter word command."""
    keeper_host_port = "zoo1:2181"
    result = node1.query(
        f"SELECT zookeeperFourLetterWordCommand('{keeper_host_port}', 'stat')"
    ).strip()
    assert len(result) > 0, "'stat' command should return a non-empty string"


def test_four_letter_word_mntr():
    """Test the 'mntr' four-letter word command."""
    keeper_host_port = "zoo1:2181"
    result = node1.query(
        f"SELECT zookeeperFourLetterWordCommand('{keeper_host_port}', 'mntr')"
    ).strip()
    assert len(result) > 0, "'mntr' command should return a non-empty string"


def test_four_letter_word_invalid_host():
    """Test zookeeperFourLetterWordCommand with invalid host:port raises an error."""
    with pytest.raises(QueryRuntimeException):
        node1.query(
            "SELECT zookeeperFourLetterWordCommand('invalid_host:9999', 'ruok')"
        )


def test_four_letter_word_invalid_format():
    """Test zookeeperFourLetterWordCommand with invalid address format (no port) raises an error."""
    with pytest.raises(QueryRuntimeException):
        node1.query(
            "SELECT zookeeperFourLetterWordCommand('zoo1', 'ruok')"
        )


def test_four_letter_word_on_different_workers():
    """Test that zookeeperFourLetterWordCommand returns the same result from different workers."""
    keeper_host_port = "zoo1:2181"
    result_w1 = node1.query(
        f"SELECT zookeeperFourLetterWordCommand('{keeper_host_port}', 'ruok')"
    ).strip()
    result_w2 = node2.query(
        f"SELECT zookeeperFourLetterWordCommand('{keeper_host_port}', 'ruok')"
    ).strip()
    assert result_w1 == result_w2 == "imok"