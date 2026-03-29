import pytest

from helpers.cluster import ClickHouseCluster, is_arm

if is_arm():
    pytestmark = pytest.mark.skip


cluster = ClickHouseCluster(__file__)
instance1 = cluster.add_instance(
    "instance1",
    main_configs=["configs/kerberos_with_keytab.xml"],
    user_configs=["configs/users.xml"],
    with_kerberos_kdc=True,
)
instance2 = cluster.add_instance(
    "instance2",
    main_configs=["configs/kerberos_without_keytab.xml"],
    user_configs=["configs/users.xml"],
    with_kerberos_kdc=True,
)
instance3 = cluster.add_instance(
    "instance3",
    main_configs=["configs/kerberos_bad_path_to_keytab.xml"],
    user_configs=["configs/users.xml"],
    with_kerberos_kdc=True,
)
client = cluster.add_instance(
    "client",
    main_configs=["configs/kerberos_without_keytab.xml"],
    user_configs=["configs/users.xml"],
    with_kerberos_kdc=True,
)


# Fixtures


@pytest.fixture(scope="module")
def kerberos_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# Tests


def make_auth(instance):
    instance_ip = cluster.get_instance_ip(instance.name)

    client.exec_in_container(
        ["bash", "-c", f"echo '{instance_ip} {instance.hostname}' >> /etc/hosts"]
    )

    client.exec_in_container(
        ["bash", "-c", "kinit -k -t /tmp/keytab/kuser.keytab kuser"]
    )
    return client.exec_in_container(
        [
            "bash",
            "-c",
            f"echo 'select currentUser()' | curl --negotiate -u : http://{instance.hostname}:8123/ --data-binary @-",
        ]
    )


def test_kerberos_auth_with_keytab(kerberos_cluster):
    assert make_auth(instance1) == "kuser\n"


def test_kerberos_auth_without_keytab(kerberos_cluster):
    assert (
        "DB::Exception: : Authentication failed: password is incorrect, or there is no user with such name."
        in make_auth(instance2)
    )


def test_bad_path_to_keytab(kerberos_cluster):
    assert (
        "DB::Exception: : Authentication failed: password is incorrect, or there is no user with such name."
        in make_auth(instance3)
    )
    assert instance3.contains_in_log("Keytab file not found")


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
