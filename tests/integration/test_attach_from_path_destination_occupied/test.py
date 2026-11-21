import re
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

# A `Replicated` database is the only shape where the guard's "move the occupied
# directory to trash" branch is reachable: it requires a non-initial
# `ZooKeeperMetadataTransaction`, which only a secondary replica gets.
main_node = cluster.add_instance(
    "main_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)
other_node = cluster.add_instance(
    "other_node",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)

USER_FILES = "/var/lib/clickhouse/user_files"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def sh(instance, command):
    return instance.exec_in_container(
        ["bash", "-c", command], privileged=True, user="root"
    )


def count_files(instance, path):
    return int(sh(instance, f"find {path} -type f 2>/dev/null | wc -l").strip())


def count_entries(instance, path):
    """Every entry under `path`, not just regular files.

    `count_files` cannot tell a missing directory from an existing empty one, and cannot
    see an empty directory that was moved into trash, so assertions about "nothing moved"
    need this instead.
    """
    return int(sh(instance, f"find {path} -mindepth 1 2>/dev/null | wc -l").strip())


def exists(instance, path):
    return sh(instance, f"test -e {path} && echo yes || echo no").strip()


def is_dir(instance, path):
    return sh(instance, f"test -d {path} && echo yes || echo no").strip()


def is_symlink(instance, path):
    return sh(instance, f"test -L {path} && echo yes || echo no").strip()


def exists_no_follow(instance, path):
    """`test -e` follows symlinks, so it reports a dangling link as absent. This reports any
    kind of object at the path, matching what the pre-flight guard checks."""
    return sh(instance, f"test -e {path} -o -L {path} && echo yes || echo no").strip()


def is_published(instance, database, table):
    return instance.query(
        f"SELECT count() FROM system.tables WHERE database = '{database}' AND name = '{table}'"
    ).strip()


def seed_into_user_files(instance, database, engine, rows, seed_name):
    """Build a real table, copy its data directory into user_files, drop the table.

    The copy is what `ATTACH ... FROM` will later relocate.
    """
    instance.query(f"CREATE TABLE {database}.seed_src (k UInt64) ENGINE = {engine}")
    instance.query(
        f"INSERT INTO {database}.seed_src SELECT number FROM numbers({rows})"
    )
    source = instance.query(
        f"SELECT data_paths[1] FROM system.tables "
        f"WHERE database = '{database}' AND name = 'seed_src'"
    ).strip()
    # For `File` the reported path is the data file itself, not the directory holding it, so
    # copy the containing directory: `ATTACH ... FROM` always relocates a directory.
    if is_dir(instance, source) == "no":
        source = source.rstrip("/").rsplit("/", 1)[0]
    sh(
        instance,
        f"rm -rf {USER_FILES}/{seed_name} && cp -r {source} {USER_FILES}/{seed_name}",
    )
    sh(instance, f"chmod -R a+rwX {USER_FILES}/{seed_name}")
    instance.query(f"DROP TABLE {database}.seed_src SYNC")
    return count_files(instance, f"{USER_FILES}/{seed_name}")


@pytest.mark.parametrize(
    "engine, rows, occupant",
    [
        # `MergeTree` rejects an occupied destination itself, with `DIRECTORY_ALREADY_EXISTS`.
        ("MergeTree ORDER BY k", 5, False),
        # `Log` relocates with a bare `fs::rename`, which succeeds onto an empty directory,
        # so an occupant is what makes this row fail on the merge-base. The empty-destination
        # case is covered separately by `test_empty_destination_is_rejected`.
        ("Log", 4, True),
        # `File` moves only its data file into the destination, so on the merge-base this
        # succeeded even with the directory occupied. It is the widest narrowing here.
        ("File(TSV)", 7, True),
    ],
)
def test_occupied_destination_is_rejected_before_publishing(
    started_cluster, engine, rows, occupant
):
    database = "occupied_" + re.sub(r"\W", "", engine.split()[0]).lower()
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database} ENGINE = Ordinary")

    seed_name = f"{database}_seed"
    seed_files = seed_into_user_files(node, database, engine, rows, seed_name)
    assert seed_files > 0

    destination = f"/var/lib/clickhouse/data/{database}/dst"
    sh(node, f"rm -rf {destination} && mkdir -p {destination}")
    if occupant:
        sh(node, f"echo occupant > {destination}/occupant")
    # `sh` runs as root, so the destination has to be made writable by the server explicitly.
    # Without this the `File` row reddens on the merge-base with a permission error from
    # `rename`, which looks like the narrowing under test but is not it.
    sh(node, f"chmod -R a+rwX {destination}")
    occupant_files = count_files(node, destination)
    sh(node, "rm -rf /var/lib/clickhouse/trash")

    attach = (
        f"ATTACH TABLE {database}.dst FROM '{seed_name}' (k UInt64) ENGINE = {engine}"
    )
    error = node.query_and_get_error(attach)

    # The statement is rejected by the pre-publication guard, which names the directory.
    assert "TABLE_ALREADY_EXISTS" in error, error
    assert "already exists" in error, error

    # Nothing was published: no table is attached under the destination name.
    assert is_published(node, database, "dst") == "0"

    # The source data was not touched, so the user can still recover it. This fails
    # loudly if the rejection is ever replaced by a rollback that drops the storage.
    assert count_files(node, f"{USER_FILES}/{seed_name}") == seed_files

    # The failed statement moved nothing: neither into trash nor out of the destination.
    # The destination has to be asserted to still EXIST, and trash to hold no entries at
    # all, because a file count alone reads 0 for a missing directory and 0 for an empty
    # one that was moved away.
    assert is_dir(node, destination) == "yes"
    assert count_entries(node, "/var/lib/clickhouse/trash") == 0
    assert count_files(node, destination) == occupant_files

    # Restarting must not resurrect a table, because no metadata was written.
    node.restart_clickhouse()
    assert is_published(node, database, "dst") == "0"

    # Once the destination is free the identical statement succeeds and the rows are there.
    sh(node, f"rm -rf {destination}")
    node.query(attach)
    assert node.query(f"SELECT count() FROM {database}.dst").strip() == str(rows)

    node.query(f"DROP DATABASE {database} SYNC")


def test_dangling_symlink_destination_is_rejected(started_cluster):
    """A dangling symlink occupies the destination while `fs::exists` reports false for it,
    so a guard built on `fs::exists` alone lets the statement through and the relocation
    then fails after the table is already published. `Log` is used because its relocation
    calls `createDirectories`, which fails with `EEXIST` on the link.

    Such a link is reachable: `DatabaseAtomic::tryCreateSymlink` writes links into the same
    `data/<db>/` directory that holds a non-`Atomic` table's data path."""
    database = "dangling_dst"
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database} ENGINE = Ordinary")

    seed_name = f"{database}_seed"
    seed_files = seed_into_user_files(node, database, "Log", 4, seed_name)
    assert seed_files > 0

    destination = f"/var/lib/clickhouse/data/{database}/dst"
    sh(node, "rm -rf /var/lib/clickhouse/trash")
    sh(
        node,
        f"rm -rf {destination} && ln -s /var/lib/clickhouse/does-not-exist {destination}",
    )
    # The premise of the row: the path is occupied by a link that resolves to nothing.
    assert is_symlink(node, destination) == "yes"
    assert exists(node, destination) == "no"

    attach = f"ATTACH TABLE {database}.dst FROM '{seed_name}' (k UInt64) ENGINE = Log"
    error = node.query_and_get_error(attach)

    assert "TABLE_ALREADY_EXISTS" in error, error
    assert "already exists" in error, error
    assert is_published(node, database, "dst") == "0"
    assert count_files(node, f"{USER_FILES}/{seed_name}") == seed_files
    assert count_entries(node, "/var/lib/clickhouse/trash") == 0
    # The link itself is left alone, so the statement is retryable once it is removed.
    assert is_symlink(node, destination) == "yes"

    node.restart_clickhouse()
    assert is_published(node, database, "dst") == "0"

    sh(node, f"rm -f {destination}")
    node.query(attach)
    assert node.query(f"SELECT count() FROM {database}.dst").strip() == "4"
    node.query(f"DROP DATABASE {database} SYNC")


@pytest.mark.parametrize(
    "shape, plant",
    [
        ("file", "echo occupant > {destination}"),
        ("fifo", "mkfifo {destination}"),
    ],
    ids=["file", "fifo"],
)
def test_non_directory_destination_is_rejected(started_cluster, shape, plant):
    """A table data path always ends in a slash, and with it the path is resolved as a
    directory, so a regular file or a FIFO at the destination reads as absent to
    `fs::exists`. Both block the relocation all the same, so a guard that only sees
    directories and symlinks admits the statement and the move then fails after the
    table is published."""
    database = f"nondir_{shape}"
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database} ENGINE = Ordinary")

    seed_name = f"{database}_seed"
    seed_files = seed_into_user_files(node, database, "Log", 4, seed_name)
    assert seed_files > 0

    destination = f"/var/lib/clickhouse/data/{database}/dst"
    sh(node, "rm -rf /var/lib/clickhouse/trash")
    sh(node, f"rm -rf {destination}")
    sh(node, plant.format(destination=destination))
    # The premise of the row: something is at the path, it is not a directory, and it reads
    # as absent through the trailing slash that a table data path always carries. That last
    # assertion is the blind spot itself, so it uses `{destination}/` and not `{destination}`.
    assert exists_no_follow(node, destination) == "yes"
    assert is_dir(node, destination) == "no"
    assert exists(node, f"{destination}/") == "no"

    attach = f"ATTACH TABLE {database}.dst FROM '{seed_name}' (k UInt64) ENGINE = Log"
    error = node.query_and_get_error(attach)

    assert "TABLE_ALREADY_EXISTS" in error, error
    assert "already exists" in error, error
    assert is_published(node, database, "dst") == "0"
    assert count_files(node, f"{USER_FILES}/{seed_name}") == seed_files
    assert count_entries(node, "/var/lib/clickhouse/trash") == 0
    # The obstruction is left alone, so the statement is retryable once it is removed.
    assert exists_no_follow(node, destination) == "yes"

    node.restart_clickhouse()
    assert is_published(node, database, "dst") == "0"

    sh(node, f"rm -f {destination}")
    node.query(attach)
    assert node.query(f"SELECT count() FROM {database}.dst").strip() == "4"
    node.query(f"DROP DATABASE {database} SYNC")


def test_uninspectable_destination_is_rejected(started_cluster):
    """The pre-flight cannot tell whether an uninspectable path is free, and it must not guess
    that it is: a "free" answer publishes the table and the relocation then hits the same
    error. `FS::existsNoFollow` reports absence only for `not_found` and throws otherwise, so
    the statement fails before publication.

    The obstruction is a symlink loop rather than an unreadable parent, because mode bits do
    not constrain uid 0 and the server's uid is a property of the environment: the instance
    container gets `user=os.getuid()`, which is the test runner's uid, and that is 0 whenever
    the runner itself runs as root. `ELOOP` is returned to every uid alike."""
    database = "uninspectable_dst"
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database} ENGINE = Ordinary")

    seed_name = f"{database}_seed"
    seed_files = seed_into_user_files(node, database, "Log", 4, seed_name)
    assert seed_files > 0

    parent = f"/var/lib/clickhouse/data/{database}"
    ring = f"{parent}-ring"
    saved = f"{parent}-saved"
    destination = f"{parent}/dst"
    sh(node, "rm -rf /var/lib/clickhouse/trash")
    # Stand a pair of symlinks pointing at each other where the database directory is, so
    # resolving anything below it returns `ELOOP`. The real directory is moved aside rather
    # than recreated afterwards, because `sh` runs as root and a directory it creates would
    # not be writable by the server's own uid. Both link names are cleared first so a rerun
    # cannot leave one of them pointing at a real directory.
    sh(node, f"rm -rf {destination} {ring} {saved}")
    sh(node, f"mv {parent} {saved}")
    sh(node, f"ln -s {database}-ring {parent}")
    sh(node, f"ln -s {database} {ring}")
    attach = f"ATTACH TABLE {database}.dst FROM '{seed_name}' (k UInt64) ENGINE = Log"
    try:
        # The premise of the row: the path cannot be inspected at all, by any uid. Asserting
        # it as root is what makes the row uid-independent, since root is the most privileged
        # uid the server can run as.
        assert "Too many levels of symbolic links" in sh(
            node, f"stat {destination} 2>&1 || true"
        )

        error = node.query_and_get_error(attach)
        assert "Cannot determine whether the path exists" in error, error
        assert "Too many levels of symbolic links" in error, error
        # Not the occupied-destination rejection: that one answers a question this path
        # cannot answer, so seeing it here would mean the guard guessed.
        assert "TABLE_ALREADY_EXISTS" not in error, error

        # Nothing was published, so the statement stays retryable.
        assert is_published(node, database, "dst") == "0"
        assert count_files(node, f"{USER_FILES}/{seed_name}") == seed_files
        assert count_entries(node, "/var/lib/clickhouse/trash") == 0
    finally:
        sh(node, f"rm -f {parent} {ring}")
        sh(node, f"mv {saved} {parent}")

    node.restart_clickhouse()
    assert is_published(node, database, "dst") == "0"

    # With the loop gone the destination is free, so the statement now succeeds: the refusal
    # was about not being able to look, not about the destination being taken.
    node.query(attach)
    assert node.query(f"SELECT count() FROM {database}.dst").strip() == "4"
    node.query(f"DROP DATABASE {database} SYNC")


def test_create_over_dangling_symlink_is_unchanged(started_cluster):
    """The symlink check belongs to `ATTACH ... FROM` only. A plain `CREATE` over a dangling
    symlink already fails on the merge-base, inside the storage (`Log` calls
    `createDirectories`, which returns `EEXIST` for the link), so this row is a
    no-regression guard rather than a fix row.

    What it does catch is the guard taking over that path: with the symlink term unscoped,
    this statement is rejected by the pre-flight guard with `TABLE_ALREADY_EXISTS` instead of
    failing inside the storage, so the negative assertion on that error code below is what
    reddens. The link is left in place either way on this node, because the "move the occupied
    directory to `trash/`" branch needs a `ZooKeeperMetadataTransaction` that only a
    `Replicated` secondary has; the `trash` and `is_symlink` assertions are therefore
    no-regression guards rather than the oracle for that mutation."""
    database = "create_dangling"
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database} ENGINE = Ordinary")

    # The other rows get `data/<db>/` created as a side effect of seeding a table into the
    # database. This row seeds nothing, so the parent is created through the server instead
    # of with `mkdir`, which would leave it owned by root and make the retry below fail on
    # permissions rather than on the state under test.
    node.query(f"CREATE TABLE {database}.warmup (k UInt64) ENGINE = Log")
    node.query(f"DROP TABLE {database}.warmup SYNC")

    destination = f"/var/lib/clickhouse/data/{database}/t"
    sh(node, "rm -rf /var/lib/clickhouse/trash")
    sh(
        node,
        f"rm -rf {destination} && ln -s /var/lib/clickhouse/does-not-exist {destination}",
    )
    # The premise of the row: the path is occupied by a link that resolves to nothing.
    assert is_symlink(node, destination) == "yes"
    assert exists(node, destination) == "no"

    create = f"CREATE TABLE {database}.t (k UInt64) ENGINE = Log"
    error = node.query_and_get_error(create)

    # No error code is asserted on purpose: the merge-base surfaces a `filesystem_error`
    # from `createDirectories` while the guard would surface `TABLE_ALREADY_EXISTS`, and
    # pinning either would encode the behaviour this row exists to leave undecided.
    assert error != "", "CREATE over a dangling symlink is expected to fail"
    # This is the assertion that catches the guard taking over the `CREATE` path: unscoping
    # the symlink term makes the pre-flight guard reject here with `TABLE_ALREADY_EXISTS`
    # instead of letting the storage fail inside `createDirectories`.
    assert "TABLE_ALREADY_EXISTS" not in error, error
    assert is_published(node, database, "t") == "0"
    assert count_entries(node, "/var/lib/clickhouse/trash") == 0
    # No-regression guard, not the oracle: the link is left in place on this node either way,
    # because the "move the occupied directory to `trash/`" branch needs a
    # `ZooKeeperMetadataTransaction` that only a `Replicated` secondary has. The oracle for
    # this row is the negative error-code assertion above.
    assert is_symlink(node, destination) == "yes"

    # Removing the link makes the identical statement succeed, so the failure is about the
    # occupied path and not about the statement itself.
    sh(node, f"rm -f {destination}")
    node.query(create)
    assert node.query(f"SELECT count() FROM {database}.t").strip() == "0"
    node.query(f"DROP DATABASE {database} SYNC")


def test_empty_destination_is_rejected(started_cluster):
    """An existing but EMPTY destination is rejected too. This is a deliberate narrowing:
    a bare `rename` succeeds onto an empty directory, so `Log`, `StripeLog`, `Set` and
    `File` used to accept one, while `MergeTree` already rejected it. Absorbing a directory
    of unknown provenance is what makes the silent cases silent, so the guard fails closed
    for every engine.

    Without this row every empty-destination cell is uncovered, and a revert of the
    narrowing stays green."""
    database = "empty_dst"
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database} ENGINE = Ordinary")

    seed_name = f"{database}_seed"
    seed_files = seed_into_user_files(node, database, "Log", 4, seed_name)
    assert seed_files > 0

    destination = f"/var/lib/clickhouse/data/{database}/dst"
    sh(node, "rm -rf /var/lib/clickhouse/trash")
    sh(node, f"rm -rf {destination} && mkdir -p {destination}")
    assert count_entries(node, destination) == 0

    attach = f"ATTACH TABLE {database}.dst FROM '{seed_name}' (k UInt64) ENGINE = Log"
    error = node.query_and_get_error(attach)

    assert "TABLE_ALREADY_EXISTS" in error, error
    assert "already exists" in error, error
    assert is_published(node, database, "dst") == "0"
    assert count_files(node, f"{USER_FILES}/{seed_name}") == seed_files
    assert count_entries(node, "/var/lib/clickhouse/trash") == 0
    # The empty destination is still there, untouched, so nothing was relocated into it.
    assert is_dir(node, destination) == "yes"
    assert count_entries(node, destination) == 0

    # Removing it makes the identical statement succeed, which is what keeps the
    # rejection a preflight rather than a permanent failure.
    sh(node, f"rmdir {destination}")
    node.query(attach)
    assert node.query(f"SELECT count() FROM {database}.dst").strip() == "4"
    node.query(f"DROP DATABASE {database} SYNC")


def test_attach_from_path_into_free_destination_still_works(started_cluster):
    node.query("DROP DATABASE IF EXISTS free_dst SYNC")
    node.query("CREATE DATABASE free_dst ENGINE = Ordinary")
    seed_into_user_files(node, "free_dst", "MergeTree ORDER BY k", 9, "free_dst_seed")

    sh(node, "rm -rf /var/lib/clickhouse/data/free_dst/dst")
    node.query(
        "ATTACH TABLE free_dst.dst FROM 'free_dst_seed' (k UInt64) "
        "ENGINE = MergeTree ORDER BY k"
    )
    assert node.query("SELECT count() FROM free_dst.dst").strip() == "9"
    node.query("DROP DATABASE free_dst SYNC")


@pytest.mark.parametrize("database_engine", ["Ordinary", "Atomic"])
def test_plain_attach_of_an_existing_table_still_works(
    started_cluster, database_engine
):
    """A plain `ATTACH` legitimately has an existing data directory, so the widened
    guard must keep ignoring it. This is the regression guard for the widening."""
    database = "plain_" + database_engine.lower()
    node.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    node.query(f"CREATE DATABASE {database} ENGINE = {database_engine}")
    node.query(f"CREATE TABLE {database}.t (k UInt64) ENGINE = MergeTree ORDER BY k")
    node.query(f"INSERT INTO {database}.t SELECT number FROM numbers(7)")

    data_path = node.query(
        f"SELECT data_paths[1] FROM system.tables "
        f"WHERE database = '{database}' AND name = 't'"
    ).strip()
    node.query(f"DETACH TABLE {database}.t")
    assert sh(node, f"test -d {data_path} && echo yes").strip() == "yes"

    node.query(f"ATTACH TABLE {database}.t")
    assert node.query(f"SELECT count() FROM {database}.t").strip() == "7"
    node.query(f"DROP DATABASE {database} SYNC")


SERVER_LOG = "/var/log/clickhouse-server/clickhouse-server.log"


def grep_server_log(instance, pattern, extra_grep_options=""):
    """Grep the server log INCLUDING its rotated parts, oldest first.

    A busy replica rotates `clickhouse-server.log` mid-test, and once it does, anything read
    from the live file alone silently loses every earlier line. That makes a negative
    assertion pass for the wrong reason, so every log-based assertion here has to go through
    this helper rather than reading `SERVER_LOG` directly.
    """
    return sh(
        instance,
        f"for f in $(ls -tr {SERVER_LOG}.*.gz 2>/dev/null) {SERVER_LOG}; do "
        f'  [ -f "$f" ] || continue; '
        f'  case "$f" in *.gz) zgrep -a {extra_grep_options} -E \'{pattern}\' "$f";; '
        f"            *) grep -a {extra_grep_options} -E '{pattern}' \"$f\";; esac; "
        f"done || true",
    )


def trash_moves_by_statement_kind(instance, table):
    """Count "Will move it to <trash>" warnings, split by the statement that caused them.

    A `Replicated` database replays its own metadata as `CREATE TABLE` during recovery, and
    for a `CREATE` the trash branch is both intended and pre-existing. Only a move caused
    by an `ATTACH` is a defect, so the two must be told apart rather than counted together.

    `ATTACH_MARKERS` reports how many `ATTACH TABLE` lines were seen at all, which tells the
    two apart, but note that `DDLWorker` logs that line BEFORE running the query, so it says
    only that the statement was reached. `attach_finished` counts the post-execution marker
    instead, and is what the caller must wait on.

    The caller scopes this by using a database name unique to its own run, which survives log
    rotation, unlike a line offset into the live log.
    """
    lines = grep_server_log(
        instance,
        # The pattern is passed through a single-quoted shell string, so it must not contain
        # an apostrophe. `.` stands in for the one in "wasn't".
        f"Executing CREATE TABLE [^ ]*{table}|ATTACH TABLE [^ ]*{table}|Will move it to"
        f"|wasn.t finished successfully",
        extra_grep_options="-o",
    ).splitlines()
    counts = {
        "CREATE": 0,
        "ATTACH": 0,
        "UNKNOWN": 0,
        "ATTACH_MARKERS": 0,
        "attach_finished": 0,
    }
    current = "UNKNOWN"
    for line in lines:
        if line.startswith("Executing CREATE TABLE"):
            current = "CREATE"
        elif line.startswith("ATTACH TABLE"):
            current = "ATTACH"
            counts["ATTACH_MARKERS"] += 1
        elif line.startswith("Will move it to"):
            counts[current] += 1
        elif line.startswith("wasn") and current == "ATTACH":
            counts["attach_finished"] += 1
    return counts


def test_replicated_secondary_moves_nothing_to_trash(started_cluster):
    """On a `Replicated` secondary all four environmental conditions of the trash branch hold
    (a non-initial ZooKeeper metadata transaction, no UUID mapping, the server fully started,
    and `allow_moving_table_directory_to_trash` enabled), so an `ATTACH ... FROM` reaching that
    branch would move the occupied directory away instead of failing. The only term that does
    not hold is the `!create.attach` one this PR added, which is deliberately false for the
    `ATTACH` under test. It must take the throw branch.

    A `Replicated` database is `Atomic`-shaped, so the destination is derived from the table
    UUID; the `ATTACH` therefore carries an explicit UUID, which makes both replicas
    compute the same directory and lets the test occupy it on one replica only.

    The guard is a per-node preflight, so the initiator (whose destination is free) may
    succeed; the assertion is that the replica with the occupied destination moved
    nothing."""
    database = "repl_trash"
    uuid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    destination = f"/var/lib/clickhouse/store/{uuid[:3]}/{uuid}"

    for instance in (main_node, other_node):
        instance.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    for replica, instance in enumerate((main_node, other_node), start=1):
        # `max_retries_before_automatic_recovery` defaults to 10. This test keeps the entry
        # failing forever, so on the tenth attempt the worker resets this replica's digest to
        # a sentinel, finds the replica lost, and recovers it from the Keeper snapshot.
        # Recovery replays that snapshot as a plain `CREATE TABLE` with no `FROM`, and for a
        # `CREATE` the trash branch is intended and pre-existing, so it publishes the table
        # and moves the occupied directory away. That is a different statement and unrelated
        # to the guard under test, but it lands while the initiator is still waiting, so the
        # assertions below would describe the recovery rather than the `ATTACH`. `0` disables
        # automatic recovery, keeping the entry retrying the `ATTACH` itself. The entry is
        # therefore never finished, which is why it is submitted asynchronously below.
        instance.query(
            f"CREATE DATABASE {database} "
            f"ENGINE = Replicated('/clickhouse/databases/{database}', 'shard1', 'replica{replica}') "
            f"SETTINGS max_retries_before_automatic_recovery = 0"
        )

    # Both replicas need their own copy of the source: the relocation is per node.
    seed = f"{database}_seed"
    seed_files = seed_into_user_files(
        main_node, database, "MergeTree ORDER BY k", 6, seed
    )
    payload = sh(main_node, f"tar -C {USER_FILES}/{seed} -cf - . | base64 -w0")
    sh(other_node, f"rm -rf {USER_FILES}/{seed} && mkdir -p {USER_FILES}/{seed}")
    sh(
        other_node,
        f"echo '{payload}' | base64 -d | tar -C {USER_FILES}/{seed} -xf - "
        f"&& chmod -R a+rwX {USER_FILES}/{seed}",
    )
    assert count_files(other_node, f"{USER_FILES}/{seed}") == seed_files

    # Occupy the destination on the SECONDARY only; start both replicas with an empty
    # trash so any side effect is unambiguous.
    for instance in (main_node, other_node):
        sh(instance, "rm -rf /var/lib/clickhouse/trash")
    sh(
        other_node,
        f"rm -rf {destination} && mkdir -p {destination} && echo occupant > {destination}/occupant",
    )
    assert count_files(other_node, destination) == 1

    # The secondary never finishes this entry, so a synchronous submission could only end in
    # `distributed_ddl_task_timeout`, which defaults to 180 seconds. `0` returns as soon as
    # the entry is queued, and the poll below waits for the replica to reach the statement
    # instead, which takes about a second.
    main_node.query_and_get_answer_with_error(
        f"ATTACH TABLE {database}.dst UUID '{uuid}' FROM '{seed}' (k UInt64) "
        f"ENGINE = MergeTree ORDER BY k",
        settings={"distributed_ddl_task_timeout": 0},
    )

    # The replica whose destination was occupied took the throw branch: it never entered
    # the trash branch, the occupied directory is untouched, and its source is intact.
    # Every assertion below is negative, so wait for the replica to have FINISHED the
    # statement first. `DDLWorker` logs "Executing query" before running it, so waiting on
    # that marker alone would let the assertions run while the statement is still in flight
    # and read as a clean pass. The post-execution marker is the one that means the guard has
    # already had its say.
    moves = {}
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        moves = trash_moves_by_statement_kind(other_node, "dst")
        if moves["attach_finished"] > 0:
            break
        time.sleep(1)
    assert moves["ATTACH_MARKERS"] > 0, moves
    assert moves["attach_finished"] > 0, moves
    assert moves["ATTACH"] == 0, moves
    assert moves["UNKNOWN"] == 0, moves
    # Automatic recovery is disabled for this database, so the replica must never have been
    # declared lost. Assert that directly: recovery replays the snapshot as a `CREATE`, whose
    # trash move and publication are legitimate, so if it ever fires the remaining assertions
    # stop describing the `ATTACH` under test and this test silently measures the wrong
    # statement. That is exactly how it used to fail.
    assert grep_server_log(other_node, "Replica seems to be lost").strip() == ""
    # `count_files` counts regular files only, so it cannot tell the occupant apart from a
    # replacement of the same size: a directory whose single `occupant` file was moved to
    # trash and replaced by one `format_version.txt` still reads 1. Assert the occupant
    # itself, and that nothing reached trash on this replica.
    assert exists(other_node, f"{destination}/occupant") == "yes"
    assert count_entries(other_node, destination) == 1
    assert count_entries(other_node, "/var/lib/clickhouse/trash") == 0
    assert count_files(other_node, f"{USER_FILES}/{seed}") == seed_files

    # Nothing was published on that replica either.
    assert (
        sh(
            other_node,
            f"ls /var/lib/clickhouse/metadata/{database}/ 2>/dev/null | grep -c dst || true",
        ).strip()
        == "0"
    )

    # Cleanup. With automatic recovery disabled above, a failed entry of a `Replicated`
    # database is retried indefinitely (`DDLWorker.cpp` turns any failure of a
    # `DatabaseReplicatedTask` into `UNFINISHED` and restarts its main thread), so the entry
    # has to be discarded explicitly: drop the database, then restart the replica so its DDL
    # worker stops retrying.
    for instance in (main_node, other_node):
        instance.query(f"DROP DATABASE IF EXISTS {database} SYNC")
    other_node.restart_clickhouse()
    for instance in (main_node, other_node):
        sh(
            instance,
            f"rm -rf {destination} {USER_FILES}/{seed} /var/lib/clickhouse/trash",
        )
