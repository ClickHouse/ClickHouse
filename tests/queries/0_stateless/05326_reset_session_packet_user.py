#!/usr/bin/env python3

# The `ResetSession` packet makes the session again from access control, as at login: it restores the
# default roles of the user and applies changes that were made to the user and to its settings profile
# during the session. A session that runs as another user after `EXECUTE AS` cannot be reset.

import os
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "helpers"))

from native_session_client import CLICKHOUSE_DATABASE, NativeSessionClient, expect_error

PREFIX = f"reset_session_{CLICKHOUSE_DATABASE}"
USER = f"{PREFIX}_user"
TARGET_USER = f"{PREFIX}_target"
ROLE = f"{PREFIX}_role"
PROFILE = f"{PREFIX}_profile"
OTHER_DATABASE = f"{CLICKHOUSE_DATABASE}_reset_session_user"


def cleanup(admin):
    admin.query(f"DROP USER IF EXISTS {USER}, {TARGET_USER}")
    admin.query(f"DROP ROLE IF EXISTS {ROLE}")
    admin.query(f"DROP SETTINGS PROFILE IF EXISTS {PROFILE}")
    admin.query(f"DROP DATABASE IF EXISTS {OTHER_DATABASE}")


def state(client):
    return client.query(
        "SELECT arrayStringConcat(currentRoles(), ','), toString(getSetting('max_block_size')), currentDatabase()"
    )[0]


def main():
    with NativeSessionClient() as admin:
        cleanup(admin)
        admin.query(f"CREATE DATABASE {OTHER_DATABASE}")
        admin.query(f"CREATE ROLE {ROLE}")
        admin.query(f"CREATE SETTINGS PROFILE {PROFILE} SETTINGS max_block_size = 2222, max_threads = 2 MAX 8")
        admin.query(
            f"CREATE USER {USER} DEFAULT DATABASE {CLICKHOUSE_DATABASE} SETTINGS PROFILE {PROFILE}"
        )
        admin.query(f"CREATE USER {TARGET_USER}")
        admin.query(f"GRANT {ROLE} TO {USER}")
        admin.query(f"SET DEFAULT ROLE {ROLE} TO {USER}")
        admin.query(f"GRANT IMPERSONATE ON {TARGET_USER} TO {USER}")
        admin.query(f"GRANT IMPERSONATE ON {USER} TO {TARGET_USER}")

        # No database in the handshake: the session uses the default database of the user.
        with NativeSessionClient(user=USER, database="") as client:
            print("after login:", state(client) == [ROLE, "2222", CLICKHOUSE_DATABASE])

            client.query("SET ROLE NONE")
            client.query("SET max_block_size = 1111, max_threads = 8")
            print("changed:", state(client) == ["", "1111", CLICKHOUSE_DATABASE])

            # Changes to access control during the session.
            admin.query(f"ALTER SETTINGS PROFILE {PROFILE} SETTINGS max_block_size = 3333, max_threads = 2 MAX 4")
            admin.query(f"ALTER USER {USER} DEFAULT DATABASE {OTHER_DATABASE}")

            client.reset_session()

            print("after reset:", state(client) == [ROLE, "3333", OTHER_DATABASE])
            expect_error("SETTING_CONSTRAINT_VIOLATION", client.query, "SET max_threads = 8")
            print("new constraint applies")

            # The settings come from the profile, as at a new login, also a setting that the session
            # cannot change back itself.
            client.query("SET readonly = 1")
            expect_error("READONLY", client.query, "SET readonly = 0")
            client.reset_session()
            print("readonly from the profile:", client.value("SELECT toString(getSetting('readonly'))") == "0")

            # A session that runs as another user cannot be reset. It does not change, and the
            # connection stays open.
            client.query(f"EXECUTE AS {TARGET_USER}")
            print("impersonated:", client.value("SELECT currentUser()") == TARGET_USER)
            expect_error("ACCESS_DENIED", client.reset_session)
            print("reset refused:", client.value("SELECT currentUser()") == TARGET_USER)

            # Back as the authenticated user, the session can be reset.
            client.query(f"EXECUTE AS {USER}")
            client.reset_session()
            print("authenticated user:", client.query("SELECT currentUser(), authenticatedUser()")[0] == [USER, USER])
            print("after reset as the authenticated user:", state(client) == [ROLE, "3333", OTHER_DATABASE])

        cleanup(admin)


main()
