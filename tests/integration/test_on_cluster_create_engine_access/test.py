"""
`CREATE TABLE ... ON CLUSTER` must authorize, on the initiator, everything the engine
checks while its storage object is constructed on a host.

The `Remote`/`RemoteSecure` engines perform three access checks while building their
storage, and all of them live behind `StorageFactory::get`:

  * a local-shard `SELECT` + `INSERT` on the plain target
    (`registerStorageRemote`'s callback in `StorageDistributed.cpp`),
  * a local-shard `SHOW_COLUMNS` on the target when the structure is omitted
    (`getStructureOfRemoteTableInShard`),
  * a source `READ` (filter-aware) plus the target's own inference checks when the
    target is a table function and the cluster has a local shard - which fires even
    when the columns are given explicitly.

The `Merge` engine carries the same gap through a different check: its creator passes the
*local* context into the constructor, which performs a per-source-table `SHOW_COLUMNS`
while inferring an omitted structure. (Plain `Distributed` is not covered below: its creator
infers an omitted structure under the *local* context as well, so its own `SHOW_COLUMNS`
check applies to a local `CREATE` and not to this path - an uncovered parity gap.)

For `Merge` the initiator cannot borrow that inference to authorize the statement: it filters
on `isGranted(SHOW_TABLES)` before checking `SHOW_COLUMNS`, so a host, where the acting user
is `full_access`, reads every matching table, while on the initiator a source the issuing
user cannot see is skipped with no denial. The initiator therefore asks for `SHOW_COLUMNS`
on every table its own catalog matches, seen by the user or not. What remains is a match set
only a host has: the initiator's catalog cannot name those tables, so authorizing them needs
the host to run as the user, which is the open question in #111561.

On an `ON CLUSTER` query only each host's DDL worker reaches `StorageFactory::get`, and
its query context carries no user unless the server setting
`distributed_ddl_use_initial_user_and_roles` is enabled (default off), so
`full_access = !user_id` makes every one of those checks a no-op. The initiator
authorized only `InterpreterCreateQuery::getRequiredAccess`, which emits
`CREATE_TABLE`/`TABLE_ENGINE` and nothing about the engine's target.

Result before the fix: a user holding `CREATE TABLE` on its own database plus
`TABLE ENGINE ON Remote`, `REMOTE` and `CLUSTER` could create, via `ON CLUSTER`, a
`Remote` table pointed at a local table it may neither `SELECT` nor `INSERT` nor
`DESCRIBE`. The identical statement without `ON CLUSTER` was correctly rejected.

Every `ON CLUSTER` case below is paired with the same statement run without
`ON CLUSTER` (`test_control_without_on_cluster`, `test_merge_engine_control_without_on_cluster`),
so the test asserts that the two paths agree rather than that `ON CLUSTER` merely became
stricter.
"""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/allow_named_collection_control.xml"],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/allow_named_collection_control.xml"],
    with_zookeeper=True,
)

DB = "acl_db"
HIDDEN_DB = "acl_hidden_db"
DENIED = "ACCESS_DENIED"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for node in (node1, node2):
            node.query(f"CREATE DATABASE {DB}")
            node.query(
                f"CREATE TABLE {DB}.local_target (x UInt64) ENGINE = MergeTree ORDER BY x"
            )
            node.query(f"INSERT INTO {DB}.local_target VALUES (42)")
            # Two tables whose engine is inherited by `CREATE ... AS other_table`: one whose
            # engine needs nothing beyond the statement, one pointed at the guarded target.
            node.query(
                f"CREATE TABLE {DB}.plain_source (x UInt64) ENGINE = MergeTree ORDER BY x"
            )
            node.query(
                f"CREATE TABLE {DB}.remote_source (x UInt64) "
                f"ENGINE = Remote('127.0.0.1:9000', {DB}, local_target, 'default')"
            )
            # A source in a database no test user holds anything on, so it is invisible to the
            # user on the initiator while every host still infers from it.
            node.query(f"CREATE DATABASE {HIDDEN_DB}")
            node.query(
                f"CREATE TABLE {HIDDEN_DB}.hidden_target (h UInt64) ENGINE = MergeTree ORDER BY h"
            )
        yield cluster
    finally:
        cluster.shutdown()


def make_user(name, grant_target_access=False, url_filter=None, engines=("Remote",)):
    """Create `name` on both nodes with the rights needed to reach the engine's checks.

    The user may create and read tables in its own database, but (unless
    `grant_target_access`) holds nothing on `local_target`, so the engine's local-shard
    checks are exercised in isolation - the same fixture shape as the stateless test
    `04318_remote_storage_engine_access`.
    """
    for node in (node1, node2):
        node.query(f"DROP USER IF EXISTS {name}")
        node.query(f"CREATE USER {name}")
        node.query(f"GRANT CREATE TABLE, SELECT, INSERT ON {DB}.* TO {name}")
        for engine in engines:
            node.query(f"GRANT TABLE ENGINE ON {engine} TO {name}")
        node.query(f"GRANT REMOTE ON *.* TO {name}")
        node.query(f"GRANT CLUSTER ON *.* TO {name}")
        if not grant_target_access:
            node.query(f"REVOKE SELECT, INSERT ON {DB}.local_target FROM {name}")
        if url_filter is not None:
            node.query(f"GRANT READ ON URL('{url_filter}') TO {name}")
    return name


def unique(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def denial_subject(error):
    """The privilege named by an `ACCESS_DENIED` error, e.g. `SELECT ON acl_db.local_target`.

    The full error text also carries the offending query and a stack trace, both of which
    mention the engine's target, so a substring test over the whole text cannot tell *which*
    privilege was missing. Only this subject can.
    """
    assert DENIED in error, error
    marker = "necessary to have the grant "
    assert marker in error, error
    return error.split(marker, 1)[1].split(". Stack trace", 1)[0].strip()


def assert_denied_on_target(error, table):
    """The statement must be denied, and denied because of the ENGINE's target.

    Asserting that the *denied privilege* names the target rules out a denial for some
    unrelated missing grant, which would make the case pass without exercising the check
    under test.
    """
    assert error is not None, "the statement was accepted"
    assert "local_target" in denial_subject(error), denial_subject(error)
    assert_absent_everywhere(table)


def assert_absent_everywhere(table):
    """The statement must not have created the table on any host.

    A denial that still leaves the table behind on the remote hosts would mean the
    enqueue happened and only the initiator's own reply was an error.
    """
    for node in (node1, node2):
        assert node.query(f"EXISTS TABLE {DB}.{table}").strip() == "0", (
            f"{table} exists on {node.name}"
        )


def create_on_cluster(user, table, definition, settings=None, cluster_name="test_cluster"):
    """Run `CREATE TABLE <table> ON CLUSTER <cluster_name> <definition>` as `user`.

    Returns the error text, or None when the statement succeeded.
    """
    return _run(
        user,
        f"CREATE TABLE {DB}.{table} ON CLUSTER {cluster_name} {definition}",
        settings,
    )


def _run(user, query, settings=None):
    return _run_on(node1, user, query, settings)


def _run_on(node, user, query, settings=None):
    try:
        node.query(query, user=user, settings=settings)
        return None
    except Exception as e:  # noqa: BLE001 - the error text is the assertion subject
        return str(e)


def remote_over_local_target(port=9000):
    return f"ENGINE = Remote('127.0.0.1:{port}', {DB}, local_target, 'default')"


# ---------------------------------------------------------------------------
# plain target: the local-shard SELECT + INSERT check
# ---------------------------------------------------------------------------


def test_plain_target_requires_select_and_insert(started_cluster):
    user = make_user("u_plain")
    table = unique("t_plain")

    # 1. Neither SELECT nor INSERT on the target.
    error = create_on_cluster(user, table, f"(x UInt64) {remote_over_local_target()}")
    assert_denied_on_target(error, table)

    # 2. SELECT granted, INSERT not: a persistent table can be written through, so
    #    both privileges are required.
    for node in (node1, node2):
        node.query(f"GRANT SELECT ON {DB}.local_target TO {user}")
    error = create_on_cluster(user, table, f"(x UInt64) {remote_over_local_target()}")
    assert_denied_on_target(error, table)

    # 3. Both granted: the statement is allowed and the table works on both hosts.
    for node in (node1, node2):
        node.query(f"GRANT INSERT ON {DB}.local_target TO {user}")
    assert create_on_cluster(user, table, f"(x UInt64) {remote_over_local_target()}") is None
    for node in (node1, node2):
        assert node.query(f"SELECT x FROM {DB}.{table}", user=user).strip() == "42"
        node.query(f"DROP TABLE {DB}.{table} SYNC")


# ---------------------------------------------------------------------------
# omitted structure: the local-shard SHOW_COLUMNS check
# ---------------------------------------------------------------------------


def test_omitted_structure_requires_show_columns(started_cluster):
    # When the structure is omitted the engine infers it from the target, which requires
    # SHOW_COLUMNS on a local shard. The user below holds CREATE TABLE on the database
    # (so it can see the table exists) but no column-level right on the target.
    #
    # This case is the canary for taking the "structure omitted" signal from
    # `create.columns_list`'s nullity: `getTablePropertiesAndNormalizeCreateQuery`
    # materializes that node unconditionally *before* the enqueue, so a nullity test is
    # always false there and would silently skip this branch - passing on both a fixed
    # and an unfixed server.
    user = make_user("u_infer")
    table = unique("t_infer")

    error = create_on_cluster(user, table, remote_over_local_target())
    assert_denied_on_target(error, table)

    for node in (node1, node2):
        node.query(f"GRANT SHOW COLUMNS, SELECT, INSERT ON {DB}.local_target TO {user}")
    assert create_on_cluster(user, table, remote_over_local_target()) is None
    for node in (node1, node2):
        assert (
            node.query(
                f"SELECT name, type FROM system.columns "
                f"WHERE database = '{DB}' AND table = '{table}'"
            ).strip()
            == "x\tUInt64"
        )
        node.query(f"DROP TABLE {DB}.{table} SYNC")


# ---------------------------------------------------------------------------
# table-function target WITH explicit columns
# ---------------------------------------------------------------------------


def test_table_function_target_checked_even_with_explicit_columns(started_cluster):
    # A table-function target that can route back to a local shard is analyzed under the
    # user's context even when the columns are given explicitly: otherwise a persisted
    # `Remote('127.0.0.1', merge(db, '^local_target$'), ...)` would later read the local
    # target under the engine credentials, while CREATE never validated the creator's
    # access to it. The denial surfaces as SHOW_COLUMNS, raised by `merge`'s own
    # per-source-table check while its structure is resolved.
    user = make_user("u_tf")
    table = unique("t_tf")
    definition = (
        f"(x UInt64) ENGINE = Remote('127.0.0.1:9000', merge('{DB}', '^local_target$'), 'default')"
    )

    error = create_on_cluster(user, table, definition)
    assert_denied_on_target(error, table)

    for node in (node1, node2):
        node.query(f"GRANT SHOW COLUMNS, SELECT, INSERT ON {DB}.local_target TO {user}")
    assert create_on_cluster(user, table, definition) is None
    for node in (node1, node2):
        # Read through the table, so a table that was created but is unusable would fail here.
        assert node.query(f"SELECT x FROM {DB}.{table}", user=user).strip() == "42"
        node.query(f"DROP TABLE {DB}.{table} SYNC")


# ---------------------------------------------------------------------------
# filtered source grants keep working
# ---------------------------------------------------------------------------


def test_filtered_source_grant_is_honoured(started_cluster):
    # The source check for a table-function target is `checkAccessWithFilter`, which falls
    # back to matching the normalized URI against the regexps of the user's filtered
    # grants. A declarative `READ ON <SOURCE>` requirement could not express that
    # (`checkAccess` never reads an element's filter), so the initiator must run the
    # engine's own check. This case proves the filter semantics survived: the same user is
    # allowed for a URI its grant matches and denied for one it does not.
    user = make_user("u_filter", url_filter="http://127.0.0.1:8123/.*")
    allowed = unique("t_url_ok")
    denied = unique("t_url_bad")

    def url_target(uri):
        return (
            f"(x UInt64) ENGINE = Remote('127.0.0.1:9000', "
            f"url('{uri}', 'TSV', 'x UInt64'), 'default')"
        )

    error = create_on_cluster(user, denied, url_target("http://127.0.0.1:9999/x"))
    assert error is not None and DENIED in error, error
    assert_absent_everywhere(denied)

    assert (
        create_on_cluster(user, allowed, url_target("http://127.0.0.1:8123/?query=SELECT+1"))
        is None
    )
    for node in (node1, node2):
        # Read through the table, so a table that was created but is unusable would fail here.
        assert node.query(f"SELECT x FROM {DB}.{allowed}", user=user).strip() == "1"
        node.query(f"DROP TABLE {DB}.{allowed} SYNC")


# ---------------------------------------------------------------------------
# a malformed definition is reported by the initiator
# ---------------------------------------------------------------------------


def test_malformed_engine_arguments_reported_by_initiator(started_cluster):
    # The preflight parses the engine arguments, so a parse failure must propagate as the
    # parse error rather than being swallowed or deferred to a per-host DDL failure.
    user = make_user("u_bad_args", grant_target_access=True)
    table = unique("t_bad_args")

    error = create_on_cluster(user, table, "(x UInt64) ENGINE = Remote()")
    assert error is not None, "a Remote engine with no arguments must be rejected"
    assert "NUMBER_OF_ARGUMENTS_DOESNT_MATCH" in error, error
    assert_absent_everywhere(table)


# ---------------------------------------------------------------------------
# control: the same statements without ON CLUSTER
# ---------------------------------------------------------------------------


def test_control_without_on_cluster(started_cluster):
    # The point of the fix is that the two paths agree. These are the `Remote` statements
    # above with `ON CLUSTER` removed; they were already rejected before the fix, and must
    # stay rejected.
    user = make_user("u_control")

    for suffix, definition in [
        ("plain", f"(x UInt64) {remote_over_local_target()}"),
        ("infer", remote_over_local_target()),
        (
            "tf",
            f"(x UInt64) ENGINE = Remote('127.0.0.1:9000', merge('{DB}', '^local_target$'), 'default')",
        ),
    ]:
        table = unique(f"t_local_{suffix}")
        error = _run(user, f"CREATE TABLE {DB}.{table} {definition}")
        assert error is not None and DENIED in error, f"{suffix}: {error}"
        assert node1.query(f"EXISTS TABLE {DB}.{table}").strip() == "0"

    # With SELECT granted but not INSERT the plain target is still rejected.
    for node in (node1, node2):
        node.query(f"GRANT SELECT ON {DB}.local_target TO {user}")
    table = unique("t_local_select_only")
    error = _run(user, f"CREATE TABLE {DB}.{table} (x UInt64) {remote_over_local_target()}")
    assert error is not None and DENIED in error, error


# ---------------------------------------------------------------------------
# the legacy `distributed_ddl_entry_format_version < 3` funnel
# ---------------------------------------------------------------------------

LEGACY = {"distributed_ddl_entry_format_version": 2}


def test_legacy_funnel_explicit_engine_is_checked(started_cluster):
    # `distributed_ddl_entry_format_version` is a per-query setting. Below version 3 the
    # query is enqueued before `createTable` normalizes it, so this funnel needs the
    # preflight too - an explicit engine there is fully resolved and must be checked, or
    # the whole fix is bypassed by one setting.
    user = make_user("u_legacy_explicit")
    table = unique("t_legacy_explicit")

    error = create_on_cluster(
        user, table, f"(x UInt64) {remote_over_local_target()}", settings=LEGACY
    )
    assert_denied_on_target(error, table)

    for node in (node1, node2):
        node.query(f"GRANT SELECT, INSERT ON {DB}.local_target TO {user}")
    assert (
        create_on_cluster(
            user, table, f"(x UInt64) {remote_over_local_target()}", settings=LEGACY
        )
        is None
    )
    for node in (node1, node2):
        node.query(f"DROP TABLE {DB}.{table} SYNC")


def test_inherited_engine_is_checked_at_the_default_version(started_cluster):
    # `setEngine` resolves an engine inherited through `AS other_table`, and it runs before the
    # enqueue only from `NORMALIZE_CREATE_ON_INITIATOR_VERSION` on: there the preflight authorizes
    # the resolved engine's target like an explicit one. Below it every host resolves the source in
    # its own catalog, so nothing is authorized there - stateless
    # `05030_totime_on_cluster_as_settings_in_zk_version` pins that the statement stays accepted.
    user = make_user("u_as_default")
    table = unique("t_as_default")

    error = create_on_cluster(user, table, f"AS {DB}.remote_source")
    assert_denied_on_target(error, table)

    # The target's own grants are what unblock it, so the denial above was the engine's.
    for node in (node1, node2):
        node.query(f"GRANT SELECT, INSERT ON {DB}.local_target TO {user}")
    assert create_on_cluster(user, table, f"AS {DB}.remote_source") is None
    for node in (node1, node2):
        node.query(f"DROP TABLE {DB}.{table} SYNC")

    # A source whose engine needs no target access stays accepted: the preflight authorizes the
    # inherited engine, it does not refuse inheritance.
    table = unique("t_as_plain")
    assert create_on_cluster(user, table, f"AS {DB}.plain_source") is None
    for node in (node1, node2):
        node.query(f"DROP TABLE {DB}.{table} SYNC")


def test_statement_is_authorized_before_the_target_is_resolved(started_cluster):
    # Resolving the engine's target evaluates the user's expressions and can send a
    # `DESC TABLE` to a remote shard, so it must not run for a user who may not issue the
    # statement at all. On this funnel `execute()` returns before its own `checkAccess`, and
    # `executeDDLQueryOnCluster` checks only after the enqueue is prepared, so without an
    # explicit check the preflight would be the first thing an unprivileged user reaches.
    #
    # The oracle is *which* grant the error names: `local_target` means the target was
    # resolved first (the pre-auth surface), the missing statement-level grant means
    # authorization came first.
    user = "u_preauth"
    for node in (node1, node2):
        node.query(f"DROP USER IF EXISTS {user}")
        node.query(f"CREATE USER {user}")
        # Deliberately no CREATE TABLE and no TABLE ENGINE grant.
        node.query(f"GRANT CLUSTER ON *.* TO {user}")

    table = unique("t_preauth")
    error = create_on_cluster(
        user, table, f"(x UInt64) {remote_over_local_target()}", settings=LEGACY
    )
    assert error is not None, "the statement was accepted"
    subject = denial_subject(error)
    assert "local_target" not in subject, (
        f"the target was resolved before authorization: denied on {subject}"
    )
    assert "CREATE TABLE" in subject or "TABLE ENGINE" in subject, subject
    assert_absent_everywhere(table)


# ---------------------------------------------------------------------------
# ATTACH ... ON CLUSTER
# ---------------------------------------------------------------------------


def test_full_definition_attach_on_cluster_is_checked(started_cluster):
    # An `ATTACH` query carrying a full definition introduces a user-supplied definition
    # exactly like a `CREATE`, so it is checked. A short `ATTACH` is not: its definition is
    # read back from metadata already stored on this server, and re-checking it would make
    # a validly created table impossible to re-attach.
    user = make_user("u_attach")
    table = unique("t_attach")

    # An Atomic database requires an explicit UUID on a full-definition ATTACH, so the
    # statement carries one; without it the query is rejected as INCORRECT_QUERY before
    # any engine check is reached.
    error = _run(
        user,
        f"ATTACH TABLE {DB}.{table} UUID '{uuid.uuid4()}' ON CLUSTER test_cluster "
        f"(x UInt64) {remote_over_local_target()}",
    )
    assert_denied_on_target(error, table)

    # A short ATTACH of an existing, already-validated table must remain unaffected.
    short = unique("t_short_attach")
    for node in (node1, node2):
        node.query(f"GRANT SELECT, INSERT ON {DB}.local_target TO {user}")
    assert create_on_cluster(user, short, f"(x UInt64) {remote_over_local_target()}") is None
    for node in (node1, node2):
        node.query(f"REVOKE SELECT, INSERT ON {DB}.local_target FROM {user}")
    node1.query(f"DETACH TABLE {DB}.{short} ON CLUSTER test_cluster SYNC")
    assert _run(user, f"ATTACH TABLE {DB}.{short} ON CLUSTER test_cluster") is None
    for node in (node1, node2):
        assert node.query(f"EXISTS TABLE {DB}.{short}").strip() == "1"
        node.query(f"DROP TABLE {DB}.{short} SYNC")


# ---------------------------------------------------------------------------
# the Merge engine: a per-source-table SHOW_COLUMNS check during inference
# ---------------------------------------------------------------------------


def merge_over_local_target():
    return f"ENGINE = Merge('{DB}', '^local_target$')"


def make_merge_user(name):
    """A user that can create `Merge` tables and can see `local_target`.

    Visibility of the source table is what makes these cases about `SHOW_COLUMNS` alone, and
    `make_user`'s database-level `CREATE TABLE, SELECT, INSERT ON acl_db.*` already implies it
    (a table flag implies `SHOW_TABLES`, and the later `REVOKE SELECT, INSERT` leaves
    `CREATE TABLE` in place), so this grant is redundant and kept only to state that where the
    reader looks for it. An invisible source is a separate case, denied for a separate reason:
    `test_merge_source_hidden_from_the_user_is_still_checked`.
    """
    user = make_user(name, engines=("Merge",))
    for node in (node1, node2):
        node.query(f"GRANT SHOW TABLES ON {DB}.local_target TO {user}")
    return user


def test_merge_engine_omitted_structure_requires_show_columns(started_cluster):
    # `registerStorageMerge` passes the LOCAL context into the constructor, which infers an
    # omitted structure by reading each source table's columns under a per-table
    # SHOW_COLUMNS check. On a host replaying the DDL entry that context carries no user, so
    # the check is a no-op there and the initiator must run it.
    user = make_merge_user("u_merge")
    table = unique("t_merge")

    error = create_on_cluster(user, table, merge_over_local_target())
    assert error is not None, "the statement was accepted"
    subject = denial_subject(error)
    assert subject.startswith("SHOW COLUMNS"), subject
    assert "local_target" in subject, subject
    assert_absent_everywhere(table)

    for node in (node1, node2):
        node.query(f"GRANT SHOW COLUMNS ON {DB}.local_target TO {user}")
    assert create_on_cluster(user, table, merge_over_local_target()) is None
    for node in (node1, node2):
        # The structure was inferred from the target, so the column exists and is typed.
        assert (
            node.query(
                f"SELECT name, type FROM system.columns "
                f"WHERE database = '{DB}' AND table = '{table}'"
            ).strip()
            == "x\tUInt64"
        )
        # Reading through the table needs SELECT on the source too - that is the engine's own
        # read-time check, unchanged by this fix, and asserting it keeps the success non-vacuous.
        node.query(f"GRANT SELECT ON {DB}.local_target TO {user}")
        assert node.query(f"SELECT x FROM {DB}.{table}", user=user).strip() == "42"
        node.query(f"DROP TABLE {DB}.{table} SYNC")


def test_merge_source_hidden_from_the_user_is_still_checked(started_cluster):
    # Inference skips a source the acting user may not `SHOW TABLES`, and a host acts with no
    # user, so a source that is merely invisible on the initiator was inferred there anyway:
    # the statement was accepted and every host materialized the hidden table's columns.
    user = make_user(unique("u_merge_hidden"), engines=("Merge",))
    table = unique("t_merge_hidden")
    definition = f"ENGINE = Merge('{HIDDEN_DB}', '^hidden_target$')"

    error = create_on_cluster(user, table, definition)
    assert error is not None, "the statement was accepted"
    subject = denial_subject(error)
    assert subject.startswith("SHOW COLUMNS"), subject
    # The denial asks for the database the statement itself names, never the table: naming a table
    # the user may not see would disclose that it exists, and the regexp would make that an
    # enumeration oracle. With the database given as a regexp too, it names neither.
    assert HIDDEN_DB in subject, subject
    assert "hidden_target" not in subject, subject
    assert_absent_everywhere(table)

    by_regexp = create_on_cluster(
        user, unique("t_merge_hidden_re"), f"ENGINE = Merge(REGEXP('^{HIDDEN_DB}$'), '^hidden_target$')"
    )
    assert by_regexp is not None, "the statement was accepted"
    subject = denial_subject(by_regexp)
    assert subject.startswith("SHOW COLUMNS"), subject
    assert HIDDEN_DB not in subject and "hidden_target" not in subject, subject

    # Both paths agree, though not through the same code: without `ON CLUSTER` the constructor
    # infers under the user, sees nothing it may read, and has no structure to store.
    local = _run(user, f"CREATE TABLE {DB}.{unique('t_merge_hidden_local')} {definition}")
    assert local is not None and "CANNOT_EXTRACT_TABLE_STRUCTURE" in local, local

    # A grant on the table itself also makes it visible, so the per-table check applies and the
    # structure then stored is the hidden table's - which is what the denial above kept out of a
    # table the user can read.
    for node in (node1, node2):
        node.query(f"GRANT SHOW COLUMNS ON {HIDDEN_DB}.hidden_target TO {user}")
    assert create_on_cluster(user, table, definition) is None
    for node in (node1, node2):
        assert (
            node.query(
                f"SELECT name, type FROM system.columns "
                f"WHERE database = '{DB}' AND table = '{table}'"
            ).strip()
            == "h\tUInt64"
        )
        node.query(f"DROP TABLE {DB}.{table} SYNC")


def test_merge_engine_explicit_structure_is_not_checked(started_cluster):
    # The engine only reads the source tables while inferring, so a definition that carries
    # its own structure requires nothing on them. Checking it anyway would reject statements
    # the non-`ON CLUSTER` path accepts, which is the same divergence in the other direction.
    user = make_merge_user("u_merge_explicit")
    table = unique("t_merge_explicit")

    assert create_on_cluster(user, table, f"(x UInt64) {merge_over_local_target()}") is None
    for node in (node1, node2):
        assert node.query(f"EXISTS TABLE {DB}.{table}").strip() == "1"
        node.query(f"DROP TABLE {DB}.{table} SYNC")


def test_merge_engine_control_without_on_cluster(started_cluster):
    # The two paths must agree: without `ON CLUSTER` the same statement was already rejected
    # before the fix, because the creator gets the user's context. (This is what distinguishes
    # `Merge` from plain `Distributed`, whose creator gets the global context and so is
    # bypassed on both paths.)
    user = make_merge_user("u_merge_control")
    table = unique("t_merge_local")

    error = _run(user, f"CREATE TABLE {DB}.{table} {merge_over_local_target()}")
    assert error is not None, "the statement was accepted"
    subject = denial_subject(error)
    assert subject.startswith("SHOW COLUMNS"), subject
    assert "local_target" in subject, subject
    assert node1.query(f"EXISTS TABLE {DB}.{table}").strip() == "0"


def test_merge_engine_source_database_absent_on_initiator(started_cluster):
    # The initiator of an `ON CLUSTER` query need not host the source database: it need not
    # even be a member of the cluster. The `Merge` traversal resolves the database through
    # `DatabaseCatalog::getDatabase`, which throws `UNKNOWN_DATABASE` when it is absent, so
    # the preflight tolerates exactly that one code -- otherwise a statement that used to
    # succeed on every host that does have the database would now be rejected outright.
    #
    # Paired with `test_merge_engine_omitted_structure_requires_show_columns`, which proves
    # the tolerance did not widen into swallowing `ACCESS_DENIED`.
    user = "u_merge_absent"
    remote_db = unique("remote_only_db")
    table = unique("t_merge_absent")

    for node in (node1, node2):
        node.query(f"DROP USER IF EXISTS {user}")
        node.query(f"CREATE USER {user}")
        node.query(f"GRANT CREATE TABLE, SELECT ON {DB}.* TO {user}")
        node.query(f"GRANT TABLE ENGINE ON Merge TO {user}")
        node.query(f"GRANT CLUSTER ON *.* TO {user}")

    # The database, and the source table the structure is inferred from, exist on node2 only.
    node2.query(f"CREATE DATABASE {remote_db}")
    node2.query(f"CREATE TABLE {remote_db}.src (x UInt64) ENGINE = MergeTree ORDER BY x")
    node2.query(f"GRANT SHOW COLUMNS, SELECT ON {remote_db}.src TO {user}")
    assert node1.query(f"EXISTS DATABASE {remote_db}").strip() == "0"

    try:
        error = create_on_cluster(
            user,
            table,
            f"ENGINE = Merge('{remote_db}', '^src$')",
            cluster_name="node2_only_cluster",
        )
        assert error is None, error
        assert node2.query(f"EXISTS TABLE {DB}.{table}").strip() == "1"
        assert (
            node2.query(
                f"SELECT name, type FROM system.columns "
                f"WHERE database = '{DB}' AND table = '{table}'"
            ).strip()
            == "x\tUInt64"
        )
    finally:
        node2.query(f"DROP TABLE IF EXISTS {DB}.{table} SYNC")
        node2.query(f"DROP DATABASE IF EXISTS {remote_db} SYNC")


def test_merge_engine_denial_survives_the_absent_database_tolerance(started_cluster):
    # The regression guard for the tolerance above: with the source database present, the
    # preflight's denial must still reach the user. A tolerance that swallowed
    # `ACCESS_DENIED` would reinstate the whole bypass while every positive case stayed
    # green, so this asserts the code that is *not* tolerated on the same code path -- the
    # `Merge` branch of the preflight, reached with an omitted structure.
    user = make_merge_user("u_merge_denial")
    table = unique("t_merge_denial")

    error = create_on_cluster(user, table, merge_over_local_target())
    assert error is not None, "the statement was accepted"
    subject = denial_subject(error)
    assert subject.startswith("SHOW COLUMNS"), subject
    assert "local_target" in subject, subject
    assert_absent_everywhere(table)


# ---------------------------------------------------------------------------
# RemoteSecure shares the Remote branch
# ---------------------------------------------------------------------------


def test_remote_secure_engine_is_preflighted(started_cluster):
    # `RemoteSecure` reaches the same preflight branch as `Remote`, from which it differs only
    # in the `secure` flag - both are selected by one condition, so the secure engine cannot be
    # dropped from the branch without also changing `Remote`.
    #
    # Both directions are asserted: the denial proves the secure engine reaches the branch,
    # the grant proves the branch does not reject what it should allow. With the columns given
    # explicitly nothing on the create path opens a connection - the inference block is skipped
    # and so is `getStructureOfRemoteTable` in the constructor - so no TLS listener is needed.
    # Reading *through* the table would need one, so the granted case only asserts existence.
    #
    # The granted direction cannot redden on an over-broad guard here, and that is a property of
    # the privileges rather than of this case: the grant it needs is `SELECT`, a column-level
    # flag that implies `SHOW_COLUMNS`, so forcing the inference the guard suppresses requires
    # nothing the granted user lacks. `test_merge_engine_explicit_structure_is_not_checked`
    # covers that mutation class - it reddens when the preflight ignores `structure_given`.
    user = make_user("u_secure", engines=("RemoteSecure",))
    table = unique("t_secure")
    definition = f"(x UInt64) ENGINE = RemoteSecure('127.0.0.1:9440', {DB}, local_target, 'default')"

    error = create_on_cluster(user, table, definition)
    assert_denied_on_target(error, table)

    for node in (node1, node2):
        node.query(f"GRANT SELECT, INSERT ON {DB}.local_target TO {user}")
    assert create_on_cluster(user, table, definition) is None
    for node in (node1, node2):
        assert node.query(f"EXISTS TABLE {DB}.{table}").strip() == "1"
        node.query(f"DROP TABLE {DB}.{table} SYNC")


# ---------------------------------------------------------------------------
# The same gap in the second engine that checks its target while being constructed
# ---------------------------------------------------------------------------


def test_query_runner_cluster_requires_remote_source_access(started_cluster):
    # `QueryRunner` with a `cluster` setting requires READ + WRITE ON REMOTE, checked in its own
    # create callback and therefore only on the host that constructs the storage -- the same
    # bypass as the `Remote` engine, reached through a different engine.
    user = "u_runner"
    for node in (node1, node2):
        node.query(f"DROP USER IF EXISTS {user}")
        node.query(f"CREATE USER {user}")
        node.query(f"GRANT CREATE TABLE ON {DB}.* TO {user}")
        node.query(f"GRANT TABLE ENGINE ON QueryRunner TO {user}")
        node.query(f"GRANT CLUSTER ON *.* TO {user}")

    table = unique("t_runner")
    definition = "(query String) ENGINE = QueryRunner SETTINGS cluster = 'test_cluster'"

    error = create_on_cluster(user, table, definition)
    assert error is not None, "the statement was accepted"
    assert "REMOTE" in denial_subject(error), denial_subject(error)
    assert_absent_everywhere(table)

    for node in (node1, node2):
        node.query(f"GRANT READ, WRITE ON REMOTE TO {user}")
    assert create_on_cluster(user, table, definition) is None
    for node in (node1, node2):
        node.query(f"DROP TABLE {DB}.{table} SYNC")

    # A `QueryRunner` without a cluster needs no source access, and must not be rejected.
    local_only = unique("t_runner_local")
    for node in (node1, node2):
        node.query(f"REVOKE READ, WRITE ON REMOTE FROM {user}")
    assert create_on_cluster(user, local_only, "(query String) ENGINE = QueryRunner") is None
    for node in (node1, node2):
        node.query(f"DROP TABLE {DB}.{local_only} SYNC")


# ---------------------------------------------------------------------------
# the preflight registers no named-collection dependency
# ---------------------------------------------------------------------------


def test_denied_create_leaves_no_named_collection_dependency(started_cluster):
    # `parseRemoteFunctionArguments` is not side-effect free: given a named collection it
    # registers the passed table id as a dependent of the collection, which blocks
    # `DROP NAMED COLLECTION`. The initiator preflight must pass no table id, otherwise a
    # rejected (or later failed) statement would leave a dependency for a table that never
    # exists and the collection could never be dropped.
    user = make_user("u_nc")
    collection = unique("nc")
    table = unique("t_nc")

    for node in (node1, node2):
        node.query(
            f"CREATE NAMED COLLECTION {collection} AS host = '127.0.0.1:9000', "
            f"database = '{DB}', table = 'local_target', user = 'default'"
        )
        # The user must be allowed to USE the collection, so that the statement is denied by the
        # engine's target check (which is what registers the dependency) and not earlier by the
        # collection's own access check -- which would make this case pass for the wrong reason.
        node.query(f"GRANT NAMED COLLECTION ON {collection} TO {user}")

    error = create_on_cluster(user, table, f"(x UInt64) ENGINE = Remote({collection})")
    assert_denied_on_target(error, table)

    # Nothing was registered, so the collection is still droppable.
    for node in (node1, node2):
        node.query(f"DROP NAMED COLLECTION {collection}")

    # Positive direction: a table that really was created does register the dependency.
    for node in (node1, node2):
        node.query(
            f"CREATE NAMED COLLECTION {collection} AS host = '127.0.0.1:9000', "
            f"database = '{DB}', table = 'local_target', user = 'default'"
        )
        node.query(f"GRANT NAMED COLLECTION ON {collection} TO {user}")
        node.query(f"GRANT SELECT, INSERT ON {DB}.local_target TO {user}")
    assert create_on_cluster(user, table, f"(x UInt64) ENGINE = Remote({collection})") is None
    for node in (node1, node2):
        error = _run_on(node, "default", f"DROP NAMED COLLECTION {collection}")
        assert error is not None and "NAMED_COLLECTION_IS_USED" in error, f"{node.name}: {error}"
        node.query(f"DROP TABLE {DB}.{table} SYNC")
        node.query(f"DROP NAMED COLLECTION {collection}")


# ---------------------------------------------------------------------------
# the preflight runs behind the statement's own authorization boundary
# ---------------------------------------------------------------------------


def test_missing_cluster_grant_is_reported_before_target_work(started_cluster):
    # `executeDDLQueryOnCluster` checks `CLUSTER`, but only after the initiator preflight has
    # already resolved the engine's target -- which expands named collections and address globs
    # and can send the `DESC TABLE` that `getStructureOfRemoteTable` uses. A user holding neither
    # privilege must be stopped by `CLUSTER` first, so the *named* privilege pins the order:
    # naming the target would mean the engine work already ran. Both privileges are withheld
    # precisely so the two orderings are distinguishable -- granting the target would make either
    # order name `CLUSTER`, and the case would pass without exercising anything.
    user = make_user("u_no_cluster")
    for node in (node1, node2):
        node.query(f"REVOKE CLUSTER ON *.* FROM {user}")

    table = unique("t_no_cluster")
    definition = f"(x UInt64) {remote_over_local_target()}"
    error = create_on_cluster(user, table, definition)
    assert error is not None, "the statement was accepted"
    subject = denial_subject(error)
    assert "CLUSTER" in subject, subject
    assert "local_target" not in subject, subject
    assert_absent_everywhere(table)

    # With `CLUSTER` granted the statement reaches the engine's target check, which still denies
    # it, so the reordering does not drop either check.
    for node in (node1, node2):
        node.query(f"GRANT CLUSTER ON *.* TO {user}")
    assert_denied_on_target(create_on_cluster(user, table, definition), table)

    for node in (node1, node2):
        node.query(f"GRANT SELECT, INSERT ON {DB}.local_target TO {user}")
    assert create_on_cluster(user, table, definition) is None
    for node in (node1, node2):
        node.query(f"DROP TABLE {DB}.{table} SYNC")


# ---------------------------------------------------------------------------
# the preflight must not rewrite the query it is about to enqueue
# ---------------------------------------------------------------------------


def test_preflight_leaves_host_dependent_arguments_unevaluated(started_cluster):
    # Parsing the engine's arguments folds constant expressions into literals in place, and the
    # preflight runs before the query is enqueued, so validating the query's own arguments would
    # persist the initiator's values on every host. `hostName()` names a different database per
    # host, so a leak is visible in what each host stored.
    user = make_user("u_frozen_arg", grant_target_access=True)
    for node in (node1, node2):
        for db in (node1.name, node2.name):
            node.query(f"CREATE DATABASE IF NOT EXISTS {db}")
            node.query(
                f"CREATE TABLE IF NOT EXISTS {db}.local_target (x UInt64) ENGINE = MergeTree ORDER BY x"
            )
            node.query(f"GRANT SELECT, INSERT ON {db}.local_target TO {user}")

    # The expression must genuinely differ per host, or the case cannot fail.
    assert node1.query("SELECT hostName()").strip() != node2.query("SELECT hostName()").strip()

    table = unique("t_frozen_arg")
    assert (
        create_on_cluster(
            user,
            table,
            "(x UInt64) ENGINE = Remote('127.0.0.1:9000', hostName(), 'local_target', 'default')",
        )
        is None
    )

    try:
        for node in (node1, node2):
            # `TSVRaw`: the default escaping renders each quote of the stored definition as `\'`,
            # so no quoted name could be found in it.
            stored = node.query(
                f"SELECT engine_full FROM system.tables "
                f"WHERE database = '{DB}' AND name = '{table}' FORMAT TSVRaw"
            ).strip()
            assert f"'{node.name}'" in stored, f"{node.name} stored: {stored}"
    finally:
        for node in (node1, node2):
            node.query(f"DROP TABLE IF EXISTS {DB}.{table} SYNC")
