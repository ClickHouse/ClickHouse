import pytest
from helpers.cluster import ClickHouseCluster
from kazoo.client import KazooClient, KazooState
from kazoo.security import ACL, make_digest_acl, make_acl
from kazoo.exceptions import (
    AuthFailedError,
    InvalidACLError,
    NoAuthError,
    KazooException,
)

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml"],
    with_zookeeper=True,
    use_keeper=False,
    stay_alive=True,
)

SUPERAUTH = "super:admin"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_fake_zk(timeout=30.0):
    _fake_zk_instance = KazooClient(
        hosts=cluster.get_instance_ip("node") + ":9181", timeout=timeout
    )
    _fake_zk_instance.start()
    return _fake_zk_instance


def get_genuine_zk():
    print("Zoo1", cluster.get_instance_ip("zoo1"))
    return cluster.get_kazoo_client("zoo1")


@pytest.mark.parametrize(("get_zk"), [get_genuine_zk, get_fake_zk])
def test_remove_acl(started_cluster, get_zk):
    auth_connection = get_zk()

    auth_connection.add_auth("digest", "user1:password1")

    # Consistent with zookeeper, accept generated digest
    auth_connection.create(
        "/test_remove_acl1",
        b"dataX",
        acl=[
            make_acl(
                "digest",
                "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=",
                read=True,
                write=False,
                create=False,
                delete=False,
                admin=False,
            )
        ],
    )
    auth_connection.create(
        "/test_remove_acl2",
        b"dataX",
        acl=[
            make_acl(
                "digest",
                "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=",
                read=True,
                write=True,
                create=False,
                delete=False,
                admin=False,
            )
        ],
    )
    auth_connection.create(
        "/test_remove_acl3",
        b"dataX",
        acl=[make_acl("digest", "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=", all=True)],
    )

    auth_connection.delete("/test_remove_acl2")

    auth_connection.create(
        "/test_remove_acl4",
        b"dataX",
        acl=[
            make_acl(
                "digest",
                "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=",
                read=True,
                write=True,
                create=True,
                delete=False,
                admin=False,
            )
        ],
    )

    acls, stat = auth_connection.get_acls("/test_remove_acl3")

    assert stat.aversion == 0
    assert len(acls) == 1
    for acl in acls:
        assert acl.acl_list == ["ALL"]
        assert acl.perms == 31


@pytest.mark.parametrize(("get_zk"), [get_genuine_zk, get_fake_zk])
def test_digest_auth_basic(started_cluster, get_zk):
    auth_connection = get_zk()

    auth_connection.add_auth("digest", "user1:password1")

    auth_connection.create("/test_no_acl", b"")
    auth_connection.create(
        "/test_all_acl", b"data", acl=[make_acl("auth", "", all=True)]
    )
    # Consistent with zookeeper, accept generated digest
    auth_connection.create(
        "/test_all_digest_acl",
        b"dataX",
        acl=[make_acl("digest", "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=", all=True)],
    )

    assert auth_connection.get("/test_all_acl")[0] == b"data"
    assert auth_connection.get("/test_all_digest_acl")[0] == b"dataX"

    no_auth_connection = get_zk()
    no_auth_connection.set("/test_no_acl", b"hello")

    # no ACL, so cannot access these nodes
    assert no_auth_connection.get("/test_no_acl")[0] == b"hello"

    with pytest.raises(NoAuthError):
        no_auth_connection.set("/test_all_acl", b"hello")

    with pytest.raises(NoAuthError):
        no_auth_connection.get("/test_all_acl")

    with pytest.raises(NoAuthError):
        no_auth_connection.get("/test_all_digest_acl")

    # still doesn't help
    with pytest.raises(AuthFailedError):
        no_auth_connection.add_auth("world", "anyone")

    # session became broken, reconnect
    no_auth_connection = get_zk()

    # wrong auth
    no_auth_connection.add_auth("digest", "user2:password2")

    with pytest.raises(NoAuthError):
        no_auth_connection.set("/test_all_acl", b"hello")

    with pytest.raises(NoAuthError):
        no_auth_connection.set("/test_all_acl", b"hello")

    with pytest.raises(NoAuthError):
        no_auth_connection.get("/test_all_acl")

    with pytest.raises(NoAuthError):
        no_auth_connection.get("/test_all_digest_acl")

    # but can access some non restricted nodes
    no_auth_connection.create("/some_allowed_node", b"data")

    # auth added, go on
    no_auth_connection.add_auth("digest", "user1:password1")
    for path in ["/test_no_acl", "/test_all_acl"]:
        no_auth_connection.set(path, b"auth_added")
        assert no_auth_connection.get(path)[0] == b"auth_added"


def test_super_auth(started_cluster):
    auth_connection = get_fake_zk()

    auth_connection.add_auth("digest", "user1:password1")

    auth_connection.create("/test_super_no_acl", b"")
    auth_connection.create(
        "/test_super_all_acl", b"data", acl=[make_acl("auth", "", all=True)]
    )

    super_connection = get_fake_zk()
    super_connection.add_auth("digest", "super:admin")

    for path in ["/test_super_no_acl", "/test_super_all_acl"]:
        super_connection.set(path, b"value")
        assert super_connection.get(path)[0] == b"value"


@pytest.mark.parametrize(("get_zk"), [get_genuine_zk, get_fake_zk])
def test_digest_auth_multiple(started_cluster, get_zk):
    auth_connection = get_zk()
    auth_connection.add_auth("digest", "user1:password1")
    auth_connection.add_auth("digest", "user2:password2")
    auth_connection.add_auth("digest", "user3:password3")

    auth_connection.create(
        "/test_multi_all_acl", b"data", acl=[make_acl("auth", "", all=True)]
    )

    one_auth_connection = get_zk()
    one_auth_connection.add_auth("digest", "user1:password1")

    one_auth_connection.set("/test_multi_all_acl", b"X")
    assert one_auth_connection.get("/test_multi_all_acl")[0] == b"X"

    other_auth_connection = get_zk()
    other_auth_connection.add_auth("digest", "user2:password2")

    other_auth_connection.set("/test_multi_all_acl", b"Y")

    assert other_auth_connection.get("/test_multi_all_acl")[0] == b"Y"


@pytest.mark.parametrize(("get_zk"), [get_genuine_zk, get_fake_zk])
def test_partial_auth(started_cluster, get_zk):
    auth_connection = get_zk()
    auth_connection.add_auth("digest", "user1:password1")

    auth_connection.create(
        "/test_partial_acl",
        b"data",
        acl=[
            make_acl(
                "auth", "", read=False, write=True, create=True, delete=True, admin=True
            )
        ],
    )

    auth_connection.set("/test_partial_acl", b"X")
    auth_connection.create(
        "/test_partial_acl/subnode",
        b"X",
        acl=[
            make_acl(
                "auth", "", read=False, write=True, create=True, delete=True, admin=True
            )
        ],
    )

    with pytest.raises(NoAuthError):
        auth_connection.get("/test_partial_acl")

    with pytest.raises(NoAuthError):
        auth_connection.get_children("/test_partial_acl")

    # exists works without read perm
    assert auth_connection.exists("/test_partial_acl") is not None

    auth_connection.create(
        "/test_partial_acl_create",
        b"data",
        acl=[
            make_acl(
                "auth", "", read=True, write=True, create=False, delete=True, admin=True
            )
        ],
    )
    with pytest.raises(NoAuthError):
        auth_connection.create("/test_partial_acl_create/subnode")

    auth_connection.create(
        "/test_partial_acl_set",
        b"data",
        acl=[
            make_acl(
                "auth", "", read=True, write=False, create=True, delete=True, admin=True
            )
        ],
    )
    with pytest.raises(NoAuthError):
        auth_connection.set("/test_partial_acl_set", b"X")

    # not allowed to delete child node
    auth_connection.create(
        "/test_partial_acl_delete",
        b"data",
        acl=[
            make_acl(
                "auth", "", read=True, write=True, create=True, delete=False, admin=True
            )
        ],
    )
    auth_connection.create("/test_partial_acl_delete/subnode")
    with pytest.raises(NoAuthError):
        auth_connection.delete("/test_partial_acl_delete/subnode")


def test_bad_auth(started_cluster):
    auth_connection = get_fake_zk()

    with pytest.raises(AuthFailedError):
        auth_connection.add_auth("world", "anyone")

    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 1")
        auth_connection.add_auth("adssagf", "user1:password1")

    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 2")
        auth_connection.add_auth("digest", "")

    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 3")
        auth_connection.add_auth("", "user1:password1")

    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 4")
        auth_connection.add_auth("digest", "user1")

    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 5")
        auth_connection.add_auth("digest", "user1:password:otherpassword")

    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 6")
        auth_connection.add_auth("auth", "user1:password")

    auth_connection = get_fake_zk()
    with pytest.raises(AuthFailedError):
        print("Sending 7")
        auth_connection.add_auth("world", "somebody")

    auth_connection = get_fake_zk()
    with pytest.raises(InvalidACLError):
        print("Sending 8")
        auth_connection.create(
            "/test_bad_acl",
            b"data",
            acl=[
                make_acl(
                    "dasd",
                    "",
                    read=True,
                    write=False,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
        )

    auth_connection = get_fake_zk()
    with pytest.raises(InvalidACLError):
        print("Sending 9")
        auth_connection.create(
            "/test_bad_acl",
            b"data",
            acl=[
                make_acl(
                    "digest",
                    "",
                    read=True,
                    write=False,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
        )

    auth_connection = get_fake_zk()
    with pytest.raises(InvalidACLError):
        print("Sending 10")
        auth_connection.create(
            "/test_bad_acl",
            b"data",
            acl=[
                make_acl(
                    "", "", read=True, write=False, create=True, delete=True, admin=True
                )
            ],
        )

    auth_connection = get_fake_zk()
    with pytest.raises(InvalidACLError):
        print("Sending 11")
        auth_connection.create(
            "/test_bad_acl",
            b"data",
            acl=[
                make_acl(
                    "digest",
                    "dsdasda",
                    read=True,
                    write=False,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
        )

    auth_connection = get_fake_zk()
    with pytest.raises(InvalidACLError):
        print("Sending 12")
        auth_connection.create(
            "/test_bad_acl",
            b"data",
            acl=[
                make_acl(
                    "digest",
                    "dsad:DSAa:d",
                    read=True,
                    write=False,
                    create=True,
                    delete=True,
                    admin=True,
                )
            ],
        )


def test_auth_snapshot(started_cluster):
    connection = get_fake_zk()
    connection.add_auth("digest", "user1:password1")

    connection.create(
        "/test_snapshot_acl", b"data", acl=[make_acl("auth", "", all=True)]
    )

    connection1 = get_fake_zk()
    connection1.add_auth("digest", "user2:password2")

    connection1.create(
        "/test_snapshot_acl1", b"data", acl=[make_acl("auth", "", all=True)]
    )

    connection2 = get_fake_zk()

    connection2.create("/test_snapshot_acl2", b"data")

    for i in range(100):
        connection.create(
            f"/test_snapshot_acl/path{i}", b"data", acl=[make_acl("auth", "", all=True)]
        )

    node.restart_clickhouse()

    connection = get_fake_zk()

    with pytest.raises(NoAuthError):
        connection.get("/test_snapshot_acl")

    connection.add_auth("digest", "user1:password1")

    assert connection.get("/test_snapshot_acl")[0] == b"data"

    with pytest.raises(NoAuthError):
        connection.get("/test_snapshot_acl1")

    assert connection.get("/test_snapshot_acl2")[0] == b"data"

    for i in range(100):
        assert connection.get(f"/test_snapshot_acl/path{i}")[0] == b"data"

    connection1 = get_fake_zk()
    connection1.add_auth("digest", "user2:password2")

    assert connection1.get("/test_snapshot_acl1")[0] == b"data"

    with pytest.raises(NoAuthError):
        connection1.get("/test_snapshot_acl")

    connection2 = get_fake_zk()
    assert connection2.get("/test_snapshot_acl2")[0] == b"data"
    with pytest.raises(NoAuthError):
        connection2.get("/test_snapshot_acl")

    with pytest.raises(NoAuthError):
        connection2.get("/test_snapshot_acl1")


@pytest.mark.parametrize(("get_zk"), [get_genuine_zk, get_fake_zk])
def test_get_set_acl(started_cluster, get_zk):
    auth_connection = get_zk()
    auth_connection.add_auth("digest", "username1:secret1")
    auth_connection.add_auth("digest", "username2:secret2")

    auth_connection.create(
        "/test_set_get_acl", b"data", acl=[make_acl("auth", "", all=True)]
    )

    acls, stat = auth_connection.get_acls("/test_set_get_acl")

    assert stat.aversion == 0
    assert len(acls) == 2
    for acl in acls:
        assert acl.acl_list == ["ALL"]
        assert acl.id.scheme == "digest"
        assert acl.perms == 31
        assert acl.id.id in (
            "username1:eGncMdBgOfGS/TCojt51xWsWv/Y=",
            "username2:qgSSumukVlhftkVycylbHNvxhFU=",
        )

    other_auth_connection = get_zk()
    other_auth_connection.add_auth("digest", "username1:secret1")
    other_auth_connection.add_auth("digest", "username3:secret3")
    other_auth_connection.set_acls(
        "/test_set_get_acl",
        acls=[
            make_acl(
                "auth", "", read=True, write=False, create=True, delete=True, admin=True
            )
        ],
    )

    acls, stat = other_auth_connection.get_acls("/test_set_get_acl")

    assert stat.aversion == 1
    assert len(acls) == 2
    for acl in acls:
        assert acl.acl_list == ["READ", "CREATE", "DELETE", "ADMIN"]
        assert acl.id.scheme == "digest"
        assert acl.perms == 29
        assert acl.id.id in (
            "username1:eGncMdBgOfGS/TCojt51xWsWv/Y=",
            "username3:CvWITOxxTwk+u6S5PoGlQ4hNoWI=",
        )

    with pytest.raises(KazooException):
        other_auth_connection.set_acls(
            "/test_set_get_acl", acls=[make_acl("auth", "", all=True)], version=0
        )
