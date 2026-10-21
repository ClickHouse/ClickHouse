from uuid import uuid4

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def create_entities():
    instance.query(
        "CREATE SETTINGS PROFILE s1 SETTINGS max_memory_usage = 123456789 MIN 100000000 MAX 200000000"
    )
    instance.query("CREATE USER u1 SETTINGS PROFILE s1")
    instance.query("CREATE ROLE rx SETTINGS PROFILE s1")
    instance.query("CREATE USER u2 IDENTIFIED BY 'qwerty' HOST LOCAL DEFAULT ROLE rx")
    instance.query("CREATE SETTINGS PROFILE s2 SETTINGS PROFILE s1 TO u2")
    instance.query(
        "CREATE ROW POLICY p ON mydb.mytable FOR SELECT USING a<1000 TO u1, u2"
    )
    instance.query(
        "CREATE QUOTA q FOR INTERVAL 1 HOUR MAX QUERIES 100 TO ALL EXCEPT rx"
    )


@pytest.fixture(autouse=True)
def drop_entities():
    instance.query("DROP USER IF EXISTS u1, u2")
    instance.query("DROP ROLE IF EXISTS rx, ry")
    instance.query("DROP ROW POLICY IF EXISTS p ON mydb.mytable")
    instance.query("DROP QUOTA IF EXISTS q")
    instance.query("DROP SETTINGS PROFILE IF EXISTS s1, s2")


def test_create():
    create_entities()

    def check():
        assert (
            instance.query("SHOW CREATE USER u1")
            == "CREATE USER u1 IDENTIFIED WITH no_password SETTINGS PROFILE `s1`\n"
        )
        assert (
            instance.query("SHOW CREATE USER u2")
            == "CREATE USER u2 IDENTIFIED WITH sha256_password HOST LOCAL DEFAULT ROLE rx\n"
        )
        assert (
            instance.query("SHOW CREATE ROW POLICY p ON mydb.mytable")
            == "CREATE ROW POLICY p ON mydb.mytable FOR SELECT USING a < 1000 TO u1, u2\n"
        )
        assert (
            instance.query("SHOW CREATE QUOTA q")
            == "CREATE QUOTA q FOR INTERVAL 1 hour MAX queries = 100 TO ALL EXCEPT rx\n"
        )
        assert instance.query("SHOW GRANTS FOR u1") == ""
        assert instance.query("SHOW GRANTS FOR u2") == "GRANT rx TO u2\n"
        assert (
            instance.query("SHOW CREATE ROLE rx")
            == "CREATE ROLE rx SETTINGS PROFILE `s1`\n"
        )
        assert instance.query("SHOW GRANTS FOR rx") == ""
        assert (
            instance.query("SHOW CREATE SETTINGS PROFILE s1")
            == "CREATE SETTINGS PROFILE `s1` SETTINGS max_memory_usage = 123456789 MIN 100000000 MAX 200000000\n"
        )
        assert (
            instance.query("SHOW CREATE SETTINGS PROFILE s2")
            == "CREATE SETTINGS PROFILE `s2` SETTINGS INHERIT `s1` TO u2\n"
        )

    check()
    instance.restart_clickhouse()  # Check persistency
    check()


def test_alter():
    create_entities()
    instance.restart_clickhouse()

    instance.query("CREATE ROLE ry")
    instance.query("GRANT ry TO u2")
    instance.query("ALTER USER u2 DEFAULT ROLE ry")
    instance.query("GRANT rx TO ry WITH ADMIN OPTION")
    instance.query("ALTER ROLE rx SETTINGS PROFILE s2")
    instance.query("GRANT SELECT ON mydb.mytable TO u1")
    instance.query("GRANT SELECT ON mydb.* TO rx WITH GRANT OPTION")
    instance.query(
        "ALTER SETTINGS PROFILE s1 SETTINGS max_memory_usage = 987654321 CONST"
    )

    def check():
        assert (
            instance.query("SHOW CREATE USER u1")
            == "CREATE USER u1 IDENTIFIED WITH no_password SETTINGS PROFILE `s1`\n"
        )
        assert (
            instance.query("SHOW CREATE USER u2")
            == "CREATE USER u2 IDENTIFIED WITH sha256_password HOST LOCAL DEFAULT ROLE ry\n"
        )
        assert (
            instance.query("SHOW GRANTS FOR u1")
            == "GRANT SELECT ON mydb.mytable TO u1\n"
        )
        assert instance.query("SHOW GRANTS FOR u2") == "GRANT rx, ry TO u2\n"
        assert (
            instance.query("SHOW CREATE ROLE rx")
            == "CREATE ROLE rx SETTINGS PROFILE `s2`\n"
        )
        assert instance.query("SHOW CREATE ROLE ry") == "CREATE ROLE ry\n"
        assert (
            instance.query("SHOW GRANTS FOR rx")
            == "GRANT SELECT ON mydb.* TO rx WITH GRANT OPTION\n"
        )
        assert (
            instance.query("SHOW GRANTS FOR ry") == "GRANT rx TO ry WITH ADMIN OPTION\n"
        )
        assert (
            instance.query("SHOW CREATE SETTINGS PROFILE s1")
            == "CREATE SETTINGS PROFILE `s1` SETTINGS max_memory_usage = 987654321 CONST\n"
        )
        assert (
            instance.query("SHOW CREATE SETTINGS PROFILE s2")
            == "CREATE SETTINGS PROFILE `s2` SETTINGS INHERIT `s1` TO u2\n"
        )

    check()
    instance.restart_clickhouse()  # Check persistency
    check()


def test_drop():
    create_entities()
    instance.restart_clickhouse()

    instance.query("DROP USER u2")
    instance.query("DROP ROLE rx")
    instance.query("DROP ROW POLICY p ON mydb.mytable")
    instance.query("DROP QUOTA q")
    instance.query("DROP SETTINGS PROFILE s1")

    def check():
        assert (
            instance.query("SHOW CREATE USER u1")
            == "CREATE USER u1 IDENTIFIED WITH no_password\n"
        )
        assert (
            instance.query("SHOW CREATE SETTINGS PROFILE s2")
            == "CREATE SETTINGS PROFILE `s2`\n"
        )
        assert "There is no user `u2`" in instance.query_and_get_error(
            "SHOW CREATE USER u2"
        )
        assert (
            "There is no row policy `p ON mydb.mytable`"
            in instance.query_and_get_error("SHOW CREATE ROW POLICY p ON mydb.mytable")
        )
        assert "There is no quota `q`" in instance.query_and_get_error(
            "SHOW CREATE QUOTA q"
        )

    check()
    instance.restart_clickhouse()  # Check persistency
    check()


def test_drop_and_kill():
    instance.query(
        "CREATE SETTINGS PROFILE stale_profile SETTINGS max_memory_usage = 111"
    )

    instance.restart_clickhouse()
    instance.query("DROP SETTINGS PROFILE stale_profile")
    instance.restart_clickhouse(kill=True)

    assert instance.query("SELECT 1") == "1\n"
    assert "There is no settings profile `stale_profile`" in instance.query_and_get_error(
        "SHOW CREATE SETTINGS PROFILE stale_profile"
    )


def test_drop_role_cascades_to_disk():
    """
    Regression test for https://github.com/ClickHouse/ClickHouse/issues/104298.

    Before the fix, DROP ROLE only removed the role from the in-memory entity map. Other
    entities that referenced the dropped role (e.g. a user's DEFAULT ROLE list, a settings
    profile's TO list) still kept the dropped UUID in their on-disk *.sql files. The
    SHOW CREATE output looked clean only because the rendering layer filters unknown UUIDs,
    but on the next server restart those references were loaded back into memory and
    surfaced as `ACCESS_ENTITY_NOT_FOUND` errors during distributed/Remote query execution.

    This test creates a role, references it from several other entities, drops the role,
    and asserts that the dropped UUID is no longer present in any *.sql file under the
    access directory — and that after a restart no entity refers back to it.
    """
    instance.query(
        """
        DROP USER     IF EXISTS u_104298;
        DROP ROLE     IF EXISTS r_keep_104298, r_drop_104298, r_parent_104298;
        DROP SETTINGS PROFILE IF EXISTS sp_104298;
        DROP ROW POLICY IF EXISTS rp_104298 ON mydb.mytable;
        DROP QUOTA    IF EXISTS q_104298;
        """
    )

    instance.query("CREATE ROLE r_keep_104298")
    instance.query("CREATE ROLE r_drop_104298")
    instance.query("CREATE ROLE r_parent_104298")
    instance.query(
        "CREATE USER u_104298 DEFAULT ROLE r_keep_104298, r_drop_104298"
    )
    instance.query("GRANT r_keep_104298, r_drop_104298 TO u_104298")
    instance.query("GRANT r_drop_104298 TO r_parent_104298")
    instance.query(
        "CREATE SETTINGS PROFILE sp_104298 SETTINGS max_memory_usage = 1000000 TO r_drop_104298"
    )
    instance.query(
        "CREATE ROW POLICY rp_104298 ON mydb.mytable FOR SELECT USING 1 TO r_drop_104298"
    )
    instance.query(
        "CREATE QUOTA q_104298 FOR INTERVAL 1 HOUR MAX QUERIES 100 TO r_drop_104298"
    )

    drop_uuid = instance.query(
        "SELECT id FROM system.roles WHERE name = 'r_drop_104298'"
    ).strip()
    assert drop_uuid

    instance.query("DROP ROLE r_drop_104298")

    def grep_dropped_uuid():
        # Returns the names of any *.sql file under the access dir that still
        # references the dropped role's UUID.
        return instance.exec_in_container(
            [
                "bash",
                "-c",
                f"grep -l '{drop_uuid}' /var/lib/clickhouse/access/*.sql || true",
            ]
        ).strip()

    # The fix must propagate the cleanup to disk synchronously.
    assert grep_dropped_uuid() == "", (
        f"Dropped role UUID {drop_uuid} still found in access dir on disk"
    )

    # And after a restart the in-memory state must still be clean: nobody references
    # the dropped role any more.
    instance.restart_clickhouse()

    assert grep_dropped_uuid() == ""

    assert (
        instance.query("SHOW CREATE USER u_104298")
        == "CREATE USER u_104298 IDENTIFIED WITH no_password DEFAULT ROLE r_keep_104298\n"
    )
    assert (
        instance.query("SHOW GRANTS FOR u_104298") == "GRANT r_keep_104298 TO u_104298\n"
    )
    assert instance.query("SHOW GRANTS FOR r_parent_104298") == ""
    assert (
        instance.query("SHOW CREATE SETTINGS PROFILE sp_104298")
        == "CREATE SETTINGS PROFILE `sp_104298` SETTINGS max_memory_usage = 1000000\n"
    )
    assert (
        instance.query("SHOW CREATE ROW POLICY rp_104298 ON mydb.mytable")
        == "CREATE ROW POLICY rp_104298 ON mydb.mytable FOR SELECT USING 1\n"
    )
    assert (
        instance.query("SHOW CREATE QUOTA q_104298")
        == "CREATE QUOTA q_104298 FOR INTERVAL 1 hour MAX queries = 100\n"
    )

    # Cleanup so we don't leak state into other tests.
    instance.query(
        """
        DROP USER     IF EXISTS u_104298;
        DROP ROLE     IF EXISTS r_keep_104298, r_parent_104298;
        DROP SETTINGS PROFILE IF EXISTS sp_104298;
        DROP ROW POLICY IF EXISTS rp_104298 ON mydb.mytable;
        DROP QUOTA    IF EXISTS q_104298;
        """
    )


@pytest.mark.parametrize("grant_option", [False, True])
def test_self_authentication_grants_survive_restart(grant_option):
    direct = "self_auth_direct_05055"
    member = "self_auth_member_05055"
    recipient = "self_auth_recipient_05055"
    role = "self_auth_role_05055"
    password = "InitialPassword_05055"
    privilege = "ALTER USER SELF AUTHENTICATION"
    option = " WITH GRANT OPTION" if grant_option else ""

    def own_change(user):
        instance.query(
            f"ALTER USER {user} IDENTIFIED WITH sha256_password BY '{password}'",
            user=user,
            password=password,
        )
        assert instance.query("SELECT 1", user=user, password=password) == "1\n"

    def delegate():
        query = f"GRANT {privilege} TO {recipient}"
        if grant_option:
            instance.query(query, user=direct, password=password)
        else:
            assert "ACCESS_DENIED" in instance.query_and_get_error(
                query, user=direct, password=password
            )

    try:
        instance.query(
            f"""
            CREATE USER {direct} IDENTIFIED WITH sha256_password BY '{password}';
            CREATE USER {member} IDENTIFIED WITH sha256_password BY '{password}';
            CREATE USER {recipient};
            CREATE ROLE {role};
            GRANT {privilege} TO {direct}{option};
            GRANT SELECT, {privilege} ON *.* TO {role}{option};
            GRANT {role} TO {member};
            """
        )
        direct_grants = instance.query(f"SHOW GRANTS FOR {direct}")
        assert direct_grants == f"GRANT {privilege} TO {direct}{option}\n"
        role_grants = instance.query(f"SHOW GRANTS FOR {role}")
        assert privilege in role_grants and f"ON *.* TO {role}" in role_grants
        member_grants = instance.query(f"SHOW GRANTS FOR {member}")
        own_change(direct)
        own_change(member)
        delegate()

        # Disk loading parses serialized ATTACH grants through the same short-form parser.
        instance.restart_clickhouse()
        assert instance.query(f"SHOW GRANTS FOR {direct}") == direct_grants
        assert instance.query(f"SHOW GRANTS FOR {role}") == role_grants
        assert instance.query(f"SHOW GRANTS FOR {member}") == member_grants
        own_change(direct)
        own_change(member)
        delegate()

        instance.query(
            f"REVOKE GRANT OPTION FOR {privilege} FROM {direct};"
            f"GRANT ALTER USER ON {direct} TO {direct} WITH GRANT OPTION;"
        )
        revoked_grants = instance.query(f"SHOW GRANTS FOR {direct}")
        instance.restart_clickhouse()
        assert instance.query(f"SHOW GRANTS FOR {direct}") == revoked_grants
        assert "ACCESS_DENIED" in instance.query_and_get_error(
            f"GRANT {privilege} TO {recipient}", user=direct, password=password
        )
        own_change(direct)

        instance.query(f"REVOKE ALTER USER ON {direct} FROM {direct}")
        own_change(direct)
        instance.query(f"REVOKE {privilege} FROM {direct}")
        instance.restart_clickhouse()
        assert instance.query(f"SHOW GRANTS FOR {direct}") == ""
        assert "ACCESS_DENIED" in instance.query_and_get_error(
            f"ALTER USER {direct} IDENTIFIED BY '{password}'",
            user=direct,
            password=password,
        )
        own_change(member)
    finally:
        instance.query(
            f"DROP USER IF EXISTS {direct}, {member}, {recipient};"
            f"DROP ROLE IF EXISTS {role};"
        )


@pytest.mark.parametrize(
    "failpoint",
    ["self_authentication_before_identity_lookup", "self_authentication_before_update"],
)
@pytest.mark.parametrize("change", ["rename_replace", "drop", "drop_replace"])
def test_self_authentication_identity_race(failpoint, change):
    user = "self_auth_race_05055"
    renamed = "self_auth_renamed_05055"
    password = "InitialPassword_05055"
    replacement_password = "ReplacementPassword_05055"
    attempted_password = "DeniedPassword_05055"
    request = None

    try:
        instance.query(
            f"CREATE USER {user} IDENTIFIED WITH sha256_password BY '{password}';"
            f"GRANT ALTER USER SELF AUTHENTICATION TO {user};"
        )
        instance.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        if_exists = "IF EXISTS " if change.startswith("drop") else ""
        request = instance.get_query_request(
            f"ALTER USER {if_exists}{user} IDENTIFIED WITH sha256_password BY '{attempted_password}'",
            user=user,
            password=password,
            timeout=60,
        )
        # Wait for the exact execution boundary, rather than racing client connections with sleeps.
        instance.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=20)

        if change == "rename_replace":
            instance.query(f"ALTER USER {user} RENAME TO {renamed}")
        else:
            instance.query(f"DROP USER {user}")

        if change != "drop":
            instance.query(
                f"CREATE USER {user} IDENTIFIED WITH sha256_password BY '{replacement_password}'"
            )

        survivors = []
        if change == "rename_replace":
            survivors.append((renamed, password))
        if change != "drop":
            survivors.append((user, replacement_password))
        definitions = {
            name: instance.query(f"SHOW CREATE USER {name}")
            for name, _ in survivors
        }

        instance.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        completed_request = request
        request = None
        if change == "rename_replace" or (
            change == "drop_replace" and failpoint.endswith("identity_lookup")
        ):
            error = completed_request.get_error()
            assert "ACCESS_DENIED" in error
            expected_message = (
                "Self-service authentication requires the authenticated account"
                if failpoint.endswith("identity_lookup")
                else "Self-service authentication target changed during alteration"
            )
            assert expected_message in error
        else:
            # A disappeared bound ID is skipped; a replacement under the same name is never updated.
            assert completed_request.get_answer() == ""

        for name, expected_password in survivors:
            assert instance.query(f"SHOW CREATE USER {name}") == definitions[name]
            assert instance.query("SELECT 1", user=name, password=expected_password) == "1\n"
            assert "AUTHENTICATION_FAILED" in instance.query_and_get_error(
                "SELECT 1", user=name, password=attempted_password
            )
        assert instance.query(
            f"SELECT count() FROM system.users WHERE name IN ('{user}', '{renamed}')"
        ) == f"{len(survivors)}\n"
    finally:
        instance.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        try:
            if request is not None:
                request.get_answer_and_error()
        finally:
            instance.query(f"DROP USER IF EXISTS {user}, {renamed}")


def test_self_authentication_management_setting():
    setting = "experimental_self_service_password_change"
    privilege = "ALTER USER SELF AUTHENTICATION"
    direct, member, recipient = (
        f"self_auth_setting_{name}" for name in ("direct", "member", "recipient")
    )
    role = "self_auth_setting_role"
    password = "SettingPassword"
    config_path = "/etc/clickhouse-server/config.d/self_auth_setting.xml"
    users_config_path = "/etc/clickhouse-server/users.d/self_auth_setting.xml"
    actor = "self_auth_setting_actor"
    authorization_roles = ("self_auth_replace_absent", "self_auth_replace_present")
    replacement_cases = []
    requests = (
        ("select_absent", 0, "SELECT ON system.one", False),
        ("select_self", 1, "SELECT ON system.one", False),
        ("select_go", 2, "SELECT ON system.one", False),
        ("empty", 2, "USAGE ON *.*", False),
        ("all", 2, "ALL ON *.*", False),
        ("all_go", 2, "ALL ON *.*", True),
        ("access", 2, "ACCESS MANAGEMENT ON *.*", False),
        ("access_go", 2, "ACCESS MANAGEMENT ON *.*", True),
        ("self", 2, privilege, False),
        ("self_go", 2, privilege, True),
        ("mixed", 2, f"SELECT ON system.one, {privilege}", False),
        ("current", 2, "CURRENT GRANTS(SELECT ON system.one)", False),
    )
    for enabled in (False, True):
        for kind, initial, request, grant_option in requests:
            subject = f"self_auth_replace_{int(enabled)}_{kind}"
            option = " WITH GRANT OPTION" if grant_option else ""
            query = f"GRANT {request} TO {subject}{option} WITH REPLACE OPTION"
            replacement_cases.append(
                (enabled, kind, initial, grant_option, subject, query)
            )
    copy_roles = {
        (group, initial): f"self_auth_copy_{group.replace(' ', '_').lower()}_{initial}"
        for group in ("ALL", "ACCESS MANAGEMENT")
        for initial in (0, 1)
    }
    # The matrix uses literal ALL; the grantor must also hold named-collection secrets.
    matrix_cases = []
    for enabled in (False, True):
        for initial in (False, True):
            for kind, requested, scope in (
                ("self", privilege, ""),
                ("alter", "ALTER USER", " ON *"),
                ("all", "ALL", " ON *.*"),
                ("access", "ACCESS MANAGEMENT", " ON *.*"),
            ):
                for revoke in (False, True):
                    for grant_option in (False, True):
                        subject = f"self_auth_matrix_{int(enabled)}_{int(initial)}_{kind}_{int(revoke)}_{int(grant_option)}"
                        command = "REVOKE" if revoke else "GRANT"
                        option_prefix = (
                            " GRANT OPTION FOR" if revoke and grant_option else ""
                        )
                        option_suffix = (
                            " WITH GRANT OPTION" if not revoke and grant_option else ""
                        )
                        recipient_keyword = "FROM" if revoke else "TO"
                        query = f"{command}{option_prefix} {requested}{scope} {recipient_keyword} {subject}{option_suffix}"
                        matrix_cases.append(
                            (
                                enabled,
                                initial,
                                kind,
                                revoke,
                                grant_option,
                                subject,
                                query,
                            )
                        )

    def stored_bits(subject):
        # Global grants can be serialized as groups instead of individual privileges.
        self_types = f"('{privilege}', 'ACCESS MANAGEMENT', 'ALL')"
        alter_types = "('ALTER USER', 'ACCESS MANAGEMENT', 'ALL')"
        return tuple(
            map(
                int,
                instance.query(
                    f"SELECT countIf(access_type IN {self_types} AND NOT is_partial_revoke) > 0, "
                    f"maxIf(grant_option, access_type IN {self_types} AND NOT is_partial_revoke), "
                    f"countIf(access_type IN {alter_types} AND NOT is_partial_revoke) > 0, "
                    f"maxIf(grant_option, access_type IN {alter_types} AND NOT is_partial_revoke) "
                    f"FROM system.grants WHERE role_name = '{subject}'"
                )
                .strip()
                .split("\t"),
            )
        )

    def run_matrix(enabled):
        for (
            configured,
            initial,
            kind,
            revoke,
            grant_option,
            subject,
            query,
        ) in matrix_cases:
            if configured != enabled:
                continue
            # Grant-option additions start with the bit present but no grant option.
            initial_grant_option = int(initial and (revoke or not grant_option))
            before = (int(initial), initial_grant_option, int(revoke), int(revoke))
            assert stored_bits(subject) == before, query
            if kind == "self" and not enabled:
                error = instance.query_and_get_error(query)
                assert "SUPPORT_IS_DISABLED" in error and setting in error
                assert stored_bits(subject) == before, query
                continue
            instance.query(query)
            expected = list(before)
            if kind == "self" or (kind in ("all", "access") and enabled):
                if revoke:
                    expected[1] = 0
                    if not grant_option:
                        expected[0] = 0
                else:
                    expected[0] = 1
                    expected[1] = int(grant_option or initial)
            if kind != "self":
                expected[2] = int(not revoke or grant_option)
                expected[3] = int(not revoke and grant_option)
            assert stored_bits(subject) == tuple(expected), query

    def run_replacements(enabled):
        for (
            configured,
            kind,
            initial,
            grant_option,
            subject,
            query,
        ) in replacement_cases:
            if configured != enabled:
                continue
            before = instance.query(f"SHOW GRANTS FOR {subject}")
            assert stored_bits(subject)[:2] == (int(initial > 0), int(initial == 2))
            if not enabled and kind in ("self", "self_go", "mixed"):
                error = instance.query_and_get_error(query)
                assert "SUPPORT_IS_DISABLED" in error and setting in error
                assert instance.query(f"SHOW GRANTS FOR {subject}") == before
                continue
            instance.query(query)
            grants_self = enabled and kind in (
                "all",
                "all_go",
                "access",
                "access_go",
                "self",
                "self_go",
                "mixed",
            )
            assert stored_bits(subject)[:2] == (
                int(grants_self),
                int(grants_self and grant_option),
            ), query
            assert (
                instance.query(
                    "SELECT count() FROM system.grants "
                    f"WHERE role_name = '{subject}' AND access_type = 'INSERT' "
                    "AND database = 'system' AND table = 'numbers'"
                )
                == "0\n"
            )
            if kind.startswith("select") or kind == "current":
                assert (
                    instance.query(f"SHOW GRANTS FOR {subject}")
                    == f"GRANT SELECT ON system.one TO {subject}\n"
                )
            elif kind == "empty":
                assert instance.query(f"SHOW GRANTS FOR {subject}") == ""

    def configure(value):
        setting_xml = (
            f"<{setting}>{value}</{setting}>"
            if value is not None
            else f'<{setting} remove="remove"/>'
        )
        instance.replace_config(
            config_path,
            f"<clickhouse>{setting_xml}<backups><allowed_path>/var/lib/clickhouse/backups/</allowed_path></backups></clickhouse>",
        )
        instance.restart_clickhouse()

    def grants():
        return [
            instance.query(f"SHOW GRANTS FOR {name}")
            for name in (direct, member, recipient, role)
        ]

    def disabled(query, user="default", login_password=""):
        before = grants()
        error = instance.query_and_get_error(query, user=user, password=login_password)
        assert "SUPPORT_IS_DISABLED" in error and setting in error
        assert grants() == before

    def own_change(user):
        instance.query(
            f"ALTER USER {user} IDENTIFIED WITH sha256_password BY '{password}'",
            user=user,
            password=password,
        )
        assert instance.query("SELECT 1", user=user, password=password) == "1\n"

    def cleanup():
        subjects = ", ".join(
            [case[5] for case in matrix_cases]
            + list(copy_roles.values())
            + [case[4] for case in replacement_cases]
            + list(authorization_roles)
        )
        instance.query(
            f"DROP USER IF EXISTS {direct}, {member}, {recipient}, {actor};"
            f"DROP ROLE IF EXISTS {role}, {subjects};"
        )

    try:
        # Remove the shared test opt-in to exercise the actual default.
        instance.replace_config(
            users_config_path,
            "<clickhouse><users><default><named_collection_control>1</named_collection_control>"
            "<show_named_collections_secrets>1</show_named_collections_secrets></default></users></clickhouse>",
        )
        configure(None)
        instance.query(
            f"CREATE USER {direct}, {member}, {recipient}, {actor} IDENTIFIED WITH sha256_password BY '{password}';"
            f"CREATE ROLE {role};"
        )
        assert (
            instance.query(
                f"SELECT value, default, changeable_without_restart FROM system.server_settings WHERE name = '{setting}'"
            )
            == "0\t0\tNo\n"
        )
        assert (
            instance.query(
                f"SELECT count() FROM system.settings WHERE name = '{setting}'"
            )
            == "0\n"
        )
        assert instance.query(
            "SELECT version, change.name, change.previous_value, change.new_value, change.reason "
            "FROM system.settings_changes ARRAY JOIN changes AS change "
            f"WHERE type = 'Server' AND change.name = '{setting}'"
        ) == (
            f"26.10\t{setting}\t0\t0\t"
            "Added an experimental opt-in for granting and revoking ALTER USER SELF AUTHENTICATION.\n"
        )
        instance.query("SET compatibility = '26.9'; SELECT 1")
        assert (
            instance.query(
                f"SELECT value FROM system.server_settings WHERE name = '{setting}'"
            )
            == "0\n"
        )

        statements = (
            f"GRANT {privilege} TO {recipient}",
            f"REVOKE {privilege} FROM {recipient}",
            f"GRANT {privilege} TO {recipient} WITH GRANT OPTION",
            f"REVOKE GRANT OPTION FOR {privilege} FROM {recipient}",
            f"GRANT SELECT, {privilege} ON *.* TO {recipient}",
            f"REVOKE SELECT, {privilege} ON *.* FROM {recipient}",
            f"GRANT ALL, {privilege} ON *.* TO {recipient}",
            f"REVOKE ACCESS MANAGEMENT, {privilege} ON *.* FROM {recipient}",
            f"GRANT CURRENT GRANTS(ALL, {privilege} ON *.*) TO {recipient}",
            f"GRANT ON CLUSTER nonexistent_cluster {privilege} TO {recipient}",
            f"REVOKE ON CLUSTER nonexistent_cluster {privilege} FROM {recipient}",
        )
        for query in statements:
            disabled(query)
        # An explicit selection is rejected even when the grantable intersection is empty.
        disabled(f"GRANT CURRENT GRANTS({privilege}) TO {recipient}", direct, password)
        instance.query(f"GRANT CURRENT GRANTS(SELECT ON system.one) TO {recipient}")
        # Existing administration and token addition do not require experimental opt-in.
        instance.query(
            f"GRANT ALTER USER ON {recipient} TO {recipient};"
            f"GRANT CREATE TOKEN ON *.* TO {direct};"
        )
        own_change(recipient)
        instance.query(
            f"ALTER USER {direct} ADD IDENTIFIED WITH sha256_password BY 'TokenRoutePassword'",
            user=direct,
            password=password,
        )

        configure("true")
        instance.query(
            f"GRANT {privilege} TO {direct} WITH GRANT OPTION;"
            f"GRANT ACCESS MANAGEMENT ON *.* TO {role} WITH GRANT OPTION;"
            f"REVOKE ALTER USER ON * FROM {role};"
            f"REVOKE CREATE USER ON self_auth_excluded FROM {role};"
            f"REVOKE GRANT OPTION FOR {privilege} FROM {role};"
            f"GRANT {role} TO {member};"
        )
        # Seed both initial bit states while enabled, then reuse each server setting across cases.
        setup = []
        for _, initial, _, revoke, grant_option, subject, _ in matrix_cases:
            setup.append(f"CREATE ROLE {subject};")
            if initial:
                option = " WITH GRANT OPTION" if revoke or not grant_option else ""
                setup.append(f"GRANT {privilege} TO {subject}{option};")
            if revoke:
                setup.append(f"GRANT ALTER USER ON * TO {subject} WITH GRANT OPTION;")
        instance.query("".join(setup))
        for (_, initial), subject in copy_roles.items():
            instance.query(f"CREATE ROLE {subject}")
            if initial:
                instance.query(f"GRANT {privilege} TO {subject} WITH GRANT OPTION")
        setup = []
        for _, _, initial, _, subject, _ in replacement_cases:
            setup.append(
                f"CREATE ROLE {subject}; GRANT INSERT ON system.numbers TO {subject};"
            )
            if initial:
                option = " WITH GRANT OPTION" if initial == 2 else ""
                setup.append(f"GRANT {privilege} TO {subject}{option};")
        instance.query("".join(setup))
        instance.query(
            f"GRANT ALL ON *.* TO {actor} WITH GRANT OPTION; REVOKE {privilege} FROM {actor};"
            f"CREATE ROLE {authorization_roles[0]}, {authorization_roles[1]};"
            f"GRANT INSERT ON system.numbers TO {authorization_roles[0]}, {authorization_roles[1]};"
            f"GRANT {privilege} TO {authorization_roles[1]} WITH GRANT OPTION;"
        )
        run_matrix(True)
        run_replacements(True)
        expected = grants()
        configure("false")
        assert grants() == expected
        run_matrix(False)
        run_replacements(False)
        before = instance.query(f"SHOW GRANTS FOR {authorization_roles[1]}")
        replacement = f"GRANT SELECT ON system.one TO {authorization_roles[1]} WITH REPLACE OPTION"
        error = instance.query_and_get_error(replacement, user=actor, password=password)
        assert "ACCESS_DENIED" in error and privilege in error
        assert instance.query(f"SHOW GRANTS FOR {authorization_roles[1]}") == before
        instance.query(
            f"GRANT SELECT ON system.one TO {authorization_roles[0]} WITH REPLACE OPTION",
            user=actor,
            password=password,
        )
        assert (
            instance.query(f"SHOW GRANTS FOR {authorization_roles[0]}")
            == f"GRANT SELECT ON system.one TO {authorization_roles[0]}\n"
        )
        # Direct privilege replacement leaves role-derived authority as role-derived authority.
        instance.query(f"GRANT SELECT ON system.one TO {member} WITH REPLACE OPTION")
        assert (
            instance.query(
                f"SELECT count() FROM system.grants WHERE user_name = '{member}' AND access_type = '{privilege}'"
            )
            == "0\n"
        )
        assert f"GRANT {role} TO {member}" in instance.query(
            f"SHOW GRANTS FOR {member}"
        )
        instance.query(f"GRANT {role} TO {member} WITH REPLACE OPTION")
        own_change(direct)
        own_change(member)
        disabled(f"GRANT CURRENT GRANTS({privilege}) TO {recipient}", direct, password)
        instance.query(f"GRANT CREATE USER ON * TO {direct} WITH GRANT OPTION")
        for (group, initial), subject in copy_roles.items():
            instance.query(
                f"GRANT CURRENT GRANTS({group} ON *.*) TO {subject}",
                user=direct,
                password=password,
            )
            assert (
                instance.query(
                    f"SELECT countIf(access_type = '{privilege}'), "
                    f"maxIf(grant_option, access_type = '{privilege}'), "
                    "countIf(access_type = 'CREATE USER' AND NOT is_partial_revoke) "
                    f"FROM system.grants WHERE role_name = '{subject}'"
                )
                == f"{initial}\t{initial}\t1\n"
            )
        instance.query(
            f"GRANT CURRENT GRANTS(ALL ON *.*) TO {recipient}",
            user=direct,
            password=password,
        )
        assert (
            instance.query(
                f"SELECT count() FROM system.grants WHERE user_name = '{recipient}' AND access_type = '{privilege}'"
            )
            == "0\n"
        )
        # A disabled group revoke removes full administration while preserving self-service and its grant option.
        instance.query(
            f"GRANT ALTER USER ON * TO {direct}; REVOKE ACCESS MANAGEMENT ON *.* FROM {direct};"
        )
        own_change(direct)
        assert (
            instance.query(
                f"SELECT access_type, grant_option FROM system.grants WHERE user_name = '{direct}' AND access_type IN ('{privilege}', 'ALTER USER')"
            )
            == f"{privilege}\t1\n"
        )
        expected = grants()
        disabled(
            f"GRANT CURRENT GRANTS(SELECT ON system.one, {privilege}) TO {recipient}"
        )
        disabled(f"REVOKE {privilege} FROM {direct}")
        # Neither compatibility nor a config reload can enable the startup-only setting.
        instance.query("SET compatibility = '26.9'; SELECT 1")
        instance.replace_config(
            config_path, f"<clickhouse><{setting}>true</{setting}></clickhouse>"
        )
        instance.query("SYSTEM RELOAD CONFIG")
        disabled(f"GRANT {privilege} TO {recipient}")
        instance.replace_config(
            config_path,
            f"<clickhouse><{setting}>false</{setting}><backups><allowed_path>/var/lib/clickhouse/backups/</allowed_path></backups></clickhouse>",
        )
        instance.query("SYSTEM RELOAD CONFIG")

        # Back up and restore trusted access metadata while management remains disabled.
        backup = f"File('/var/lib/clickhouse/backups/self_auth_{uuid4().hex}')"
        instance.query(f"BACKUP TABLE system.users, TABLE system.roles TO {backup}")
        cleanup()
        instance.query(f"RESTORE ALL FROM {backup}")
        assert grants() == expected
        own_change(direct)
        own_change(member)

        # Replacing the supplying role removes its direct bit and the inherited authority.
        instance.query(f"GRANT SELECT ON system.one TO {role} WITH REPLACE OPTION")
        assert stored_bits(role)[:2] == (0, 0)
        assert "ACCESS_DENIED" in instance.query_and_get_error(
            f"ALTER USER {member} IDENTIFIED BY '{password}'",
            user=member,
            password=password,
        )
        configure("true")
        instance.query(f"REVOKE {privilege} FROM {direct};")
        for user in (direct, member):
            assert "ACCESS_DENIED" in instance.query_and_get_error(
                f"ALTER USER {user} IDENTIFIED BY '{password}'",
                user=user,
                password=password,
            )
    finally:
        cleanup()
        instance.exec_in_container(["rm", "-f", config_path, users_config_path])
        instance.restart_clickhouse()
