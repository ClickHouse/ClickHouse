#!/usr/bin/env python3

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "helpers"))
from native_session_client import CLICKHOUSE_DATABASE, NativeSessionClient, expect_error

PREFIX = f"reset_security_{CLICKHOUSE_DATABASE}"
OWNER, TARGET, LIMITED, READONLY, QUOTA_USER = (
    f"{PREFIX}_{suffix}"
    for suffix in ("owner", "target", "limited", "readonly", "quota_user")
)
QUOTA = f"{PREFIX}_quota"


def cleanup(admin):
    admin.query(f"DROP QUOTA IF EXISTS {QUOTA}")
    admin.query(
        f"DROP USER IF EXISTS {OWNER}, {TARGET}, {LIMITED}, {READONLY}, {QUOTA_USER}"
    )
    admin.query("DROP TABLE IF EXISTS reset_security_allowed")
    admin.query("DROP TABLE IF EXISTS reset_security_denied")


def test_impersonation(admin):
    admin.query(f"CREATE USER {OWNER}")
    admin.query(f"CREATE USER {TARGET}")
    admin.query(f"GRANT IMPERSONATE ON {TARGET} TO {OWNER}")
    with NativeSessionClient(user=OWNER) as client:
        client.query(f"EXECUTE AS {TARGET}")
        client.query("SET max_block_size = 12345")
        client.query("SET param_kept = 'kept'")
        expect_error("ACCESS_DENIED", client.query, f"EXECUTE AS {OWNER}")
        expect_error("ACCESS_DENIED", client.reset_session)
        assert client.query(
            "SELECT currentUser(), authenticatedUser(), toString(getSetting('max_block_size')), {kept:String}"
        ) == [[TARGET, OWNER, "12345", "kept"]]
        print("one-way impersonation survives refused reset")

        # Even permission to return does not let reset switch identities implicitly.
        admin.query(f"GRANT IMPERSONATE ON {OWNER} TO {TARGET}")
        expect_error("ACCESS_DENIED", client.reset_session)
        client.query(f"EXECUTE AS {OWNER}")
        client.reset_session()
        assert client.value("SELECT currentUser()") == OWNER
        expect_error("UNKNOWN_QUERY_PARAMETER", client.query, "SELECT {kept:String}")
        print("explicit authorized return permits reset")


def test_authentication_grants(admin):
    for table in ("reset_security_allowed", "reset_security_denied"):
        admin.query(f"CREATE TABLE {table} (x String) ENGINE = Memory")
        admin.query(f"INSERT INTO {table} VALUES ('row')")
    admin.query(
        f"CREATE USER {LIMITED} IDENTIFIED WITH plaintext_password BY 'full', plaintext_password BY 'limited' GRANTS (SELECT ON {CLICKHOUSE_DATABASE}.reset_security_allowed)"
    )
    admin.query(f"GRANT SELECT ON {CLICKHOUSE_DATABASE}.* TO {LIMITED}")
    with NativeSessionClient(user=LIMITED, password="full") as client:
        assert client.value("SELECT x FROM reset_security_denied") == "row"
    with NativeSessionClient(user=LIMITED, password="limited") as client:
        for _ in range(3):
            assert client.value("SELECT x FROM reset_security_allowed") == "row"
            expect_error(
                "ACCESS_DENIED", client.query, "SELECT x FROM reset_security_denied"
            )
            client.reset_session()
        expect_error(
            "ACCESS_DENIED", client.query, "SELECT x FROM reset_security_denied"
        )
    print("authentication grants survive repeated resets")


def test_restrictive_settings(admin):
    with NativeSessionClient() as client:
        for readonly in (1, 2):
            client.query(f"SET readonly = {readonly}")
            expect_error("READONLY", client.query, "SET readonly = 0")
            client.reset_session()
            assert client.value("SELECT toString(getSetting('readonly'))") == "0"
        client.query("SET allow_ddl = 0")
        expect_error("QUERY_IS_PROHIBITED", client.query, "SET allow_ddl = 1")
        client.reset_session()
        assert client.value("SELECT toString(getSetting('allow_ddl'))") == "true"
    admin.query(f"CREATE USER {READONLY} SETTINGS readonly = 1, allow_ddl = 0")
    with NativeSessionClient(user=READONLY) as client:
        client.reset_session()
        assert client.query(
            "SELECT toString(getSetting('readonly')), toString(getSetting('allow_ddl'))"
        ) == [["1", "false"]]
        expect_error("READONLY", client.query, "SET readonly = 0")
    print("reset restores policy defaults without relaxing restrictive defaults")


def test_quota(admin):
    admin.query(f"CREATE USER {QUOTA_USER}")
    admin.query(f"GRANT SELECT ON system.numbers TO {QUOTA_USER}")
    admin.query(
        f"CREATE QUOTA {QUOTA} FOR INTERVAL 1 HOUR MAX QUERIES = 2 TO {QUOTA_USER}"
    )
    with NativeSessionClient(user=QUOTA_USER) as client:
        # Constant `SELECT`s use `system.one` and do not consume the query quota.
        for used in (1, 2):
            assert (
                client.value("SELECT toString(number) FROM system.numbers LIMIT 1")
                == "0"
            )
            client.reset_session()
            assert admin.value(
                f"SELECT toString(ifNull(queries, 0)) FROM system.quotas_usage WHERE quota_name = '{QUOTA}'"
            ) == str(used)
        expect_error(
            "QUOTA_EXCEEDED",
            client.query,
            "SELECT toString(number) FROM system.numbers LIMIT 1",
        )
    print("reset preserves consumed quota")


with NativeSessionClient() as admin:
    cleanup(admin)
    try:
        test_impersonation(admin)
        test_authentication_grants(admin)
        test_restrictive_settings(admin)
        test_quota(admin)
    finally:
        cleanup(admin)
