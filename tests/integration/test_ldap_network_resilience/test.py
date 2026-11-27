import ipaddress
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_manager import ConfigManager

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/ldap_servers.xml"],
    user_configs=["configs/users.xml"],
    with_ldap=True,
    # A fixed address keeps the RFC 3484 ordering of the resolved addresses in the failover test
    # independent of the address that docker assigns to the LDAP container.
    ipv4_address="10.5.201.10",
)

# The part of the message that `AccessControl` logs when `unreachable_cooldown` rejects a login.
COOLDOWN_LOG_LINE = "because of 'unreachable_cooldown'"


@pytest.fixture(scope="module", autouse=True)
def ldap_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def iptables(address, add):
    """Silently drop everything the instance sends to this address, so that connecting to it
    stalls until the connection timeout instead of failing right away."""
    node.exec_in_container(
        [
            "iptables",
            "--wait",
            "-A" if add else "-D",
            "OUTPUT",
            "-d",
            address,
            "-j",
            "DROP",
        ],
        user="root",
    )


def add_host(hostname, address):
    node.exec_in_container(
        ["bash", "-c", f"printf '%s {hostname}\\n' {address} >> /etc/hosts"],
        user="root",
    )


def remove_host(hostname):
    # `/etc/hosts` is bind-mounted into the container, so it can only be rewritten in place:
    # `sed -i` renames its temporary file over it and fails with `Device or resource busy`.
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"grep -v ' {hostname}$' /etc/hosts > /tmp/hosts && cat /tmp/hosts > /etc/hosts && rm /tmp/hosts",
        ],
        user="root",
    )


def probe_blackhole(server_ip):
    """An address for the black hole below. The resolver sorts the addresses of a host by the
    longest prefix they share with the source address (RFC 3484 rule 9), and the failover test
    needs the black hole sorted deterministically in front of the LDAP address, so the black hole
    has to share a strictly longer prefix with the instance than the LDAP server does."""
    client_address = int(ipaddress.IPv4Address(node.ip_address))
    server = int(ipaddress.IPv4Address(server_ip))
    server_prefix = 32 - (client_address ^ server).bit_length()
    for delta in range(1, 1 << max(0, 31 - server_prefix)):
        candidate = client_address ^ delta
        # The lowest and the highest address of a subnet, and the conventional address of the
        # gateway (the network address plus one), are never used as the black hole.
        if candidate & 0xFF in (0, 1, 255) or candidate == server:
            continue
        return str(ipaddress.IPv4Address(candidate))
    raise RuntimeError("Cannot find an address for the black hole")


def authenticate(user, password="qwerty"):
    return node.query("SELECT currentUser()", user=user, password=password).strip()


def test_unreachable_address_does_not_block_authentication():
    ldap_ip = cluster.get_instance_ip("openldap")
    blackhole = probe_blackhole(ldap_ip)
    iptables(blackhole, add=True)
    add_host("ldap-multi", blackhole)
    add_host("ldap-multi", ldap_ip)
    try:
        resolved = node.exec_in_container(["getent", "ahosts", "ldap-multi"])
        assert resolved.split()[0] == blackhole, (
            f"The resolver has to return the black hole {blackhole} first:\n{resolved}"
        )

        start = time.time()
        assert authenticate("user_failover") == "user_failover"
        elapsed = time.time() - start
        assert elapsed < 15, f"Authentication took {elapsed} seconds"
    finally:
        remove_host("ldap-multi")
        iptables(blackhole, add=False)


def test_unreachable_cooldown_rejects_subsequent_logins_fast():
    blackhole = probe_blackhole(cluster.get_instance_ip("openldap"))
    iptables(blackhole, add=True)
    add_host("ldap-blackhole", blackhole)
    try:
        start = time.time()
        node.query_and_get_error(
            "SELECT currentUser()", user="user_breaker", password="qwerty"
        )
        first_elapsed = time.time() - start
        # `network_timeout` is 10 seconds, so the first attempt has to spend the timeout on the
        # unreachable server and must not be rejected by a stale breaker state.
        assert 5 <= first_elapsed < 25, f"The first attempt took {first_elapsed} seconds"

        start = time.time()
        rejected_before = int(node.count_in_log(COOLDOWN_LOG_LINE))
        error = node.query_and_get_error(
            "SELECT currentUser()", user="user_breaker", password="qwerty"
        )
        second_elapsed = time.time() - start
        # The reason must not leak to the client: ClickHouse replaces every authentication failure
        # with the same generic message, so only the server log may name the breaker.
        assert "Authentication failed" in error, error
        assert "unreachable" not in error, error
        assert int(node.count_in_log(COOLDOWN_LOG_LINE)) > rejected_before, (
            "The rejection by 'unreachable_cooldown' is not logged"
        )
        assert int(node.count_in_log("is unreachable (last error:")) > 0
        assert second_elapsed < 2, f"The second attempt took {second_elapsed} seconds"
        assert second_elapsed < first_elapsed
    finally:
        remove_host("ldap-blackhole")
        iptables(blackhole, add=False)


def test_verification_cache_survives_config_reload():
    assert authenticate("user_cached") == "user_cached"

    ldap_ip = cluster.get_instance_ip("openldap")
    iptables(ldap_ip, add=True)
    try:
        # The cached verification is used, so blocking the server changes nothing yet.
        assert authenticate("user_cached") == "user_cached"

        # Before the fix, the reload discarded every cached verification, and this login would
        # contact the blocked server and fail after `network_timeout`.
        node.query("SYSTEM RELOAD CONFIG")
        assert authenticate("user_cached") == "user_cached"

        # A change of a parameter that participates in the cache hash must invalidate the
        # preserved entry, even though the entry itself survives the reload.
        with ConfigManager() as config_manager:
            config_manager.add_main_config(
                node, "configs/ldap_servers_cached_wrong_bind_dn.xml"
            )
            error = node.query_and_get_error(
                "SELECT currentUser()", user="user_cached", password="qwerty"
            )
            # The cached verification is unusable after the parameter change, so the login fails
            # against the blocked server while the client still receives the generic message.
            assert "Authentication failed" in error, error
    finally:
        iptables(ldap_ip, add=False)
