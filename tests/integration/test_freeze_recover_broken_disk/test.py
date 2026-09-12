import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/storage.xml"],
    stay_alive=True,
)

# A separate instance for the read-only disk: an unreachable disk also breaks the
# unrelated all-disk orphaned-parts scan that a non-default storage policy runs at
# table load, and the tests above depend on such policies.
node_read_only = cluster.add_instance(
    "node_read_only",
    main_configs=["configs/storage_read_only.xml"],
    user_configs=["configs/users_read_only.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_metric_value(instance, metric_name):
    result = instance.query(
        f"SELECT value FROM system.metrics WHERE metric = '{metric_name}'"
    ).strip()
    return int(result) if result else 0


# The server runs as root in these containers, and root is exempt from the
# permission-bit checks that decide disk state: `faccessat(W_OK)` succeeds on a
# mode-555 directory and a directory search bit does not restrict it either. So
# `chmod` cannot express "read-only" here. A read-only bind mount can: it denies
# writes with EROFS, which no uid is exempt from, while leaving the tree readable.
# This is the mechanism `test_disk_checker/test.py::test_disk_readonly_status`
# already uses, and the containers carry SYS_ADMIN for it.
def mount_read_only(instance, path):
    # `user="root"`, because mounting needs it and the container uid follows the uid
    # running the tests, which is not root outside CI.
    instance.exec_in_container(["mount", "--bind", path, path], user="root")
    # The disk checker writes a probe file into the root, so a remount can lose the
    # race against it and report EBUSY.
    for _ in range(10):
        try:
            instance.exec_in_container(
                ["mount", "-o", "remount,ro,bind", path], user="root"
            )
            return
        except Exception:
            time.sleep(0.5)
    raise AssertionError(f"could not remount {path} read-only")


def unmount(instance, path):
    try:
        instance.exec_in_container(["umount", path], user="root")
    except Exception:
        pass


def test_freeze_recovery_skips_broken_disk(started_cluster):
    # Regression test for https://github.com/ClickHouse/ClickHouse/issues/105719.
    #
    # FREEZE recovers an empty/missing shadow/increment.txt by scanning every
    # configured disk for the maximum numeric shadow/<N> directory, so the next
    # unnamed FREEZE allocates above it. A broken disk cannot be scanned, but it
    # is a routine state: it must be skipped (with a log message) rather than make
    # every FREEZE on the server fail. Once the disk is healthy again, recovery
    # scans it and allocates above the directories it holds.
    try:
        node.query("DROP TABLE IF EXISTS t_cold SYNC")
        node.query("DROP TABLE IF EXISTS t_warm SYNC")

        # A table on the broken-able cold disk, and one on the healthy default disk.
        node.query(
            "CREATE TABLE t_cold (id UInt64) ENGINE = MergeTree ORDER BY id "
            "SETTINGS storage_policy = 'cold_policy'"
        )
        node.query("INSERT INTO t_cold VALUES (1), (2), (3)")
        node.query("CREATE TABLE t_warm (id UInt64) ENGINE = MergeTree ORDER BY id")
        node.query("INSERT INTO t_warm VALUES (1), (2), (3)")

        # Create a numeric backup directory on the cold disk: cold/shadow/7/.
        node.query("ALTER TABLE t_cold FREEZE WITH NAME '7'")
        assert (
            node.exec_in_container(
                ["bash", "-c", "test -d /var/lib/clickhouse/cold/shadow/7 && echo yes || echo no"]
            ).strip()
            == "yes"
        )

        # Plant the broken counter state: an empty default shadow/increment.txt.
        node.exec_in_container(
            ["bash", "-c", "mkdir -p /var/lib/clickhouse/shadow && : > /var/lib/clickhouse/shadow/increment.txt"]
        )

        # Break the cold disk by moving its directory away; the disk checker
        # thread marks it broken within local_disk_check_period_ms.
        node.exec_in_container(
            ["bash", "-c", "mv /var/lib/clickhouse/cold /var/lib/clickhouse/cold_moved"]
        )
        wait_condition(
            func=lambda: get_metric_value(node, "BrokenDisks"),
            condition=lambda value: value >= 1,
            max_attempts=30,
            delay=1,
        )

        # A FREEZE WITH NAME takes its directory name from the given name, so it
        # must leave the counter completely alone: it neither runs the all-disks
        # scan (a disk it cannot inspect must not fail a query that does not need
        # the scan) nor repairs the counter (repairing it without a scan would let
        # the next unnamed FREEZE see a healthy counter, skip recovery and reuse an
        # existing shadow/<N>). The skip line is emitted by the scan, so its
        # absence pins that the scan did not run here.
        node.rotate_logs()
        node.query("ALTER TABLE t_warm FREEZE WITH NAME 'named_105719'")
        assert not node.contains_in_log("is broken and was skipped while recovering")
        # The counter must still be empty, so recovery is still pending.
        assert (
            node.exec_in_container(
                ["bash", "-c", "test -s /var/lib/clickhouse/shadow/increment.txt && echo yes || echo no"]
            ).strip()
            == "no"
        )

        # An unnamed FREEZE on the healthy default disk must SUCCEED even though
        # an unrelated disk is broken: recovery skips the broken disk and logs an
        # info message. Before this fix the recovery threw ABORTED, so every
        # FREEZE on the server failed for as long as any disk stayed broken.
        # The counter is still the one emptied above, deliberately not re-emptied
        # here, so this also pins that the named FREEZE did not consume recovery.
        node.rotate_logs()
        node.query("ALTER TABLE t_warm FREEZE")
        assert (
            node.exec_in_container(
                ["bash", "-c", "test -s /var/lib/clickhouse/shadow/increment.txt && echo yes || echo no"]
            ).strip()
            == "yes"
        )
        assert node.contains_in_log("is broken and was skipped while recovering")

        # Restore the disk and restart the server so it comes up healthy (a
        # moved-away local disk recovers on restart, see test_disk_checker).
        # The disk checker may have recreated an empty cold/ while it was broken,
        # so drop that before moving the original data directory back.
        node.exec_in_container(
            [
                "bash",
                "-c",
                "rm -rf /var/lib/clickhouse/cold && mv /var/lib/clickhouse/cold_moved /var/lib/clickhouse/cold",
            ]
        )
        node.restart_clickhouse()
        assert node.query("SELECT count() FROM t_cold").strip() == "3"

        # With every disk healthy, recovery scans cold/shadow/7 and allocates a
        # higher id, so the new backup does not reuse the existing directory.
        node.exec_in_container(
            [
                "bash",
                "-c",
                "mkdir -p /var/lib/clickhouse/shadow && "
                ": > /var/lib/clickhouse/shadow/increment.txt",
            ]
        )
        node.query("ALTER TABLE t_cold FREEZE")
        numeric_dirs = node.exec_in_container(
            [
                "bash",
                "-c",
                "ls /var/lib/clickhouse/cold/shadow | grep -E '^[0-9]+$' | sort -n | tr '\\n' ' '",
            ]
        ).split()
        # cold/shadow/7 must survive, and the new backup must be a higher id.
        assert "7" in numeric_dirs, numeric_dirs
        assert max(int(d) for d in numeric_dirs) > 7, numeric_dirs
    finally:
        node.exec_in_container(
            [
                "bash",
                "-c",
                "test -d /var/lib/clickhouse/cold_moved && "
                "{ rm -rf /var/lib/clickhouse/cold; mv /var/lib/clickhouse/cold_moved /var/lib/clickhouse/cold; } "
                "|| true",
            ]
        )
        node.query("DROP TABLE IF EXISTS t_cold SYNC")
        node.query("DROP TABLE IF EXISTS t_warm SYNC")
        node.exec_in_container(
            ["bash", "-c", "rm -rf /var/lib/clickhouse/shadow/named_105719"]
        )


def test_freeze_recovery_scans_path_wrapping_disk(started_cluster):
    # The recovery scan walks every configured disk, and a wrapping disk such as
    # DiskEncrypted reports its DELEGATE's already-wrapped path from
    # iterateDirectory. Feeding that path back to the same disk wraps it a second
    # time (`inner/outer/inner/outer/...`), so the lookup addresses a path that
    # does not exist and a real backup stops counting toward the bound. The next
    # unnamed FREEZE would then hand out an identifier that is already taken.
    # Entries must therefore be addressed in the disk's own logical namespace.
    try:
        node.query("DROP TABLE IF EXISTS t_plain SYNC")

        # A table on the doubly-wrapped encrypted disk, holding a high numeric
        # backup, and a table on the default disk to run recovery from.
        node.query("CREATE TABLE t_plain (id UInt64) ENGINE = MergeTree ORDER BY id")
        node.query("INSERT INTO t_plain VALUES (1), (2), (3)")

        # Plant the numeric backup directory on the wrapping disk directly rather
        # than through FREEZE: a nested encrypted disk currently cannot complete a
        # FREEZE at all (its setReadOnly wraps the path once per level and then
        # addresses `inner/outer/inner/outer/...`), which is a separate defect. The
        # scan under test only reads directory names, so the directory alone is the
        # relevant state.
        node.exec_in_container(
            [
                "bash",
                "-c",
                "mkdir -p /var/lib/clickhouse/enc_base/inner/outer/shadow/6000",
            ]
        )

        # Recover from the DEFAULT disk, which the encrypted backup is invisible
        # to unless the scan really reaches the wrapping disk.
        node.exec_in_container(
            [
                "bash",
                "-c",
                "mkdir -p /var/lib/clickhouse/shadow && "
                ": > /var/lib/clickhouse/shadow/increment.txt",
            ]
        )
        allocated = node.query(
            "ALTER TABLE t_plain FREEZE FORMAT TSVWithNames "
            "SETTINGS alter_partition_verbose_result = 1"
        ).splitlines()
        # backup_name is the fourth column of the verbose FREEZE result.
        allocated_id = allocated[1].split("\t")[3]
        assert int(allocated_id) > 6000, allocated

        # The encrypted backup must be untouched: the point is that it was seen,
        # not overwritten.
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -d /var/lib/clickhouse/enc_base/inner/outer/shadow/6000 "
                    "&& echo yes || echo no",
                ]
            ).strip()
            == "yes"
        )
    finally:
        node.query("DROP TABLE IF EXISTS t_plain SYNC")
        node.exec_in_container(
            [
                "bash",
                "-c",
                "rm -rf /var/lib/clickhouse/enc_base/inner/outer/shadow "
                "/var/lib/clickhouse/shadow/6*",
            ]
        )


def test_freeze_recovery_refuses_exhausted_namespace(started_cluster):
    # `shadow/9223372036854775807` (Int64 max) IS an id an unnamed FREEZE can
    # allocate, because recovery from 9223372036854775806 adds one, so it must
    # raise the recovery bound like any other numeric name. Treating it as out of
    # range (the way the neighbouring 2^63 name is treated) would make the bound
    # stop at a lower name and the next unnamed FREEZE reuse this directory. There
    # is no id above it, so the only safe outcome is a refusal, and the existing
    # backup must survive it.
    #
    # This lives here rather than in the stateless suite because the name cannot be
    # varied: it is the boundary itself. On the shared stateless server a copy
    # leaked by an interrupted run makes it unplantable, and while it exists every
    # recovery correctly refuses, which breaks unrelated scenarios. Each integration
    # instance has its own `shadow/`, so the fixed name is safe.
    max_id = "9223372036854775807"
    max_id_minus_one = "9223372036854775806"
    try:
        node.query("DROP TABLE IF EXISTS t_exhausted SYNC")
        node.query(
            "CREATE TABLE t_exhausted (id UInt64) ENGINE = MergeTree ORDER BY id"
        )
        node.query("INSERT INTO t_exhausted VALUES (1), (2), (3)")

        # The maximum must first be shown REACHABLE, or the refusal below would also
        # be satisfied by an implementation that treats `max - 1` as exhausted and
        # loses the last valid identifier. Recovery adds one to the bound, so a
        # backup named `max - 1` must produce exactly `max`, both as the allocated
        # name and in the counter.
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"mkdir -p /var/lib/clickhouse/shadow && "
                f"mkdir /var/lib/clickhouse/shadow/{max_id_minus_one} && "
                f": > /var/lib/clickhouse/shadow/increment.txt",
            ]
        )
        reached = node.query(
            "ALTER TABLE t_exhausted FREEZE FORMAT TSVWithNames "
            "SETTINGS alter_partition_verbose_result = 1"
        ).splitlines()
        # backup_name is the fourth column of the verbose FREEZE result.
        assert reached[1].split("\t")[3] == max_id, reached
        assert (
            node.exec_in_container(
                ["bash", "-c", "cat /var/lib/clickhouse/shadow/increment.txt"]
            ).strip()
            == max_id
        )
        # That allocation created shadow/<max> itself, so the namespace is now
        # exhausted exactly as the scenario needs. Empty the counter again so the
        # next FREEZE has to recover from the directory rather than read a value.
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"rm -rf /var/lib/clickhouse/shadow/{max_id_minus_one} && "
                f"test -d /var/lib/clickhouse/shadow/{max_id} && "
                f": > /var/lib/clickhouse/shadow/increment.txt",
            ]
        )

        with pytest.raises(Exception) as refusal:
            node.query("ALTER TABLE t_exhausted FREEZE")
        # The code and the offending name are both part of the contract: a
        # different code, or a diagnostic that does not say WHICH directory is in
        # the way, leaves the operator without the one action that resolves this.
        refusal_text = str(refusal.value)
        assert "Code: 290" in refusal_text, refusal_text
        assert "LIMIT_EXCEEDED" in refusal_text, refusal_text
        assert max_id in refusal_text, refusal_text
        assert "largest value the counter can hold" in refusal_text, refusal_text

        # The existing backup must survive, and the counter must still be empty: a
        # later unnamed FREEZE has to recover rather than continue from a value
        # that overflowed.
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"test -d /var/lib/clickhouse/shadow/{max_id} && echo yes || echo no",
                ]
            ).strip()
            == "yes"
        )
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -s /var/lib/clickhouse/shadow/increment.txt && echo yes || echo no",
                ]
            ).strip()
            == "no"
        )

        # A NON-NUMERIC named FREEZE takes its directory name from the given name and
        # can never collide with an allocated id, so the exhausted namespace must not
        # stop it. Without this the refusal above would also be satisfied by an
        # implementation that fails every FREEZE.
        node.query("ALTER TABLE t_exhausted FREEZE WITH NAME 'named_exhausted_105719'")
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -d /var/lib/clickhouse/shadow/named_exhausted_105719 "
                    "&& echo yes || echo no",
                ]
            ).strip()
            == "yes"
        )

        # A NUMERIC name must be refused instead, and this is the one exhausted state
        # where that matters. The name has to be reserved, but there is no id to
        # reserve it against, so the only alternative is to proceed unreserved - and
        # the state that would justify doing so is not durable: `SYSTEM UNFREEZE`
        # removes the boundary backup without taking the counter lock, and the backup
        # directory is created only after the lock is released. An unnamed FREEZE would
        # then allocate exactly this name and two tables would silently share one
        # backup directory. Refusing keeps that window closed; the operator is told
        # which directory to remove, and the non-numeric form above still works
        # meanwhile.
        with pytest.raises(Exception) as numeric_exhausted:
            node.query("ALTER TABLE t_exhausted FREEZE WITH NAME '4246'")
        numeric_exhausted_text = str(numeric_exhausted.value)
        assert "LIMIT_EXCEEDED" in numeric_exhausted_text, numeric_exhausted_text
        assert max_id in numeric_exhausted_text, numeric_exhausted_text
        # It must fail BEFORE creating anything, or the refusal would leave exactly the
        # unreserved directory it exists to prevent.
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -e /var/lib/clickhouse/shadow/4246 && echo yes || echo no",
                ]
            ).strip()
            == "no"
        )
        # The counter must still be empty: the refusal must not write a value.
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -s /var/lib/clickhouse/shadow/increment.txt && echo yes || echo no",
                ]
            ).strip()
            == "no"
        )

        # Once the boundary backup is gone the namespace has room again, so an
        # unnamed FREEZE must recover and allocate normally: the refusal must not
        # have left the counter in a state that fails every later FREEZE. The exact
        # boundary arithmetic is already pinned above; here the allocated name only
        # has to clear every remaining backup and be written back to the counter,
        # which is what proves recovery ran to completion. It is not compared
        # against a fixed value because recovery scans EVERY configured disk and
        # earlier tests in this module share the instance.
        node.exec_in_container(
            ["bash", "-c", f"rm -rf /var/lib/clickhouse/shadow/{max_id}"]
        )
        remaining_max = int(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "ls /var/lib/clickhouse/shadow | grep -E '^(0|[1-9][0-9]*)$' "
                    "| sort -n | tail -1 || true",
                ]
            ).strip()
            or 0
        )
        allocated = node.query(
            "ALTER TABLE t_exhausted FREEZE FORMAT TSVWithNames "
            "SETTINGS alter_partition_verbose_result = 1"
        ).splitlines()
        # backup_name is the fourth column of the verbose FREEZE result.
        allocated_id = allocated[1].split("\t")[3]
        assert remaining_max < int(allocated_id) < int(max_id), (
            allocated,
            remaining_max,
        )
        assert (
            node.exec_in_container(
                ["bash", "-c", "cat /var/lib/clickhouse/shadow/increment.txt"]
            ).strip()
            == allocated_id
        )
    finally:
        node.query("DROP TABLE IF EXISTS t_exhausted SYNC")
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"rm -rf /var/lib/clickhouse/shadow/{max_id} "
                f"/var/lib/clickhouse/shadow/{max_id_minus_one} "
                "/var/lib/clickhouse/shadow/named_exhausted_105719 "
                # Nothing should have created this - the numeric refusal above asserts
                # it does not - but a regression that leaks it must not also leak it
                # into every later test on this instance.
                "/var/lib/clickhouse/shadow/4246",
            ]
        )


def test_named_freeze_tolerates_only_recoverable_counter_states(started_cluster):
    # A named FREEZE takes its directory name from the given name, so it never needs
    # the counter to CHOOSE that name. It does still read the counter, because the
    # name may be the value an unnamed FREEZE would allocate next and consuming it is
    # what reserves it, so a healthy counter is advanced either way and a counter that
    # is present but broken (unparsable, unopenable) still fails the query.
    #
    # What the name decides is the MISSING or EMPTY counter. Those states have no
    # value to hand out, so they are tolerated in both cases, but only a NUMERIC name
    # needs the reservation and therefore recovers the counter; any other name leaves
    # it untouched, so recovery stays pending for the unnamed FREEZE that owns the
    # scan.
    #
    # Every state below is therefore driven with a non-numeric name AND with a numeric
    # one: the numeric cases are the only ones that enter the recovery-and-reserve
    # path, so they are what pin its error filter.
    #
    # These live here rather than in the stateless suite because each state has to be
    # written into `shadow/increment.txt` itself and none of them is recoverable -
    # this fix self-heals only an empty or missing counter. The shared stateless
    # `shadow/increment.txt` is deliberately never restored on cleanup, since other
    # tests use it, and a run killed inside one of these scenarios would leave every
    # later FREEZE on that server failing. Each integration instance owns its own
    # counter.
    max_id = "9223372036854775807"
    counter = "/var/lib/clickhouse/shadow/increment.txt"
    try:
        node.query("DROP TABLE IF EXISTS t_states SYNC")
        node.query("CREATE TABLE t_states (id UInt64) ENGINE = MergeTree ORDER BY id")
        node.query("INSERT INTO t_states VALUES (1), (2), (3)")

        # Tolerated: an exhausted counter. There is no next value for an unnamed
        # FREEZE to allocate, so there is no id to reserve either, and refusing here
        # would turn uniquely named backups into a permanent failure - the very shape
        # of breakage this test exists for. The counter must survive untouched so the
        # state stays diagnosable and no negative value is ever stored.
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"mkdir -p /var/lib/clickhouse/shadow && echo {max_id} > {counter}",
            ]
        )
        node.query("ALTER TABLE t_states FREEZE WITH NAME 'named_exhausted_states'")
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -d /var/lib/clickhouse/shadow/named_exhausted_states "
                    "&& echo yes || echo no",
                ]
            ).strip()
            == "yes"
        )
        assert (
            node.exec_in_container(["bash", "-c", f"cat {counter}"]).strip() == max_id
        )

        # Control: the same exhausted counter must still stop an UNNAMED FREEZE,
        # which does take its name from it. Without this the assertion above would
        # also pass an implementation that dropped the overflow guard and let the
        # counter wrap to a negative value.
        with pytest.raises(Exception) as wrapped:
            node.query("ALTER TABLE t_states FREEZE")
        assert "exceed the range of Int64" in str(wrapped.value), str(wrapped.value)
        assert (
            node.exec_in_container(["bash", "-c", f"cat {counter}"]).strip() == max_id
        )

        # NOT tolerated: a counter that is present but unparsable. The exhausted case
        # is recognised by its error code, so without this a blanket handler would
        # satisfy every assertion above. The query must fail, create nothing, and
        # leave the file untouched.
        node.exec_in_container(["bash", "-c", f"printf %s - > {counter}"])
        with pytest.raises(Exception) as malformed:
            node.query("ALTER TABLE t_states FREEZE WITH NAME 'named_malformed_states'")
        assert "CANNOT_PARSE_NUMBER" in str(malformed.value), str(malformed.value)
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -e /var/lib/clickhouse/shadow/named_malformed_states "
                    "&& echo yes || echo no",
                ]
            ).strip()
            == "no"
        )
        assert node.exec_in_container(["bash", "-c", f"cat {counter}"]).strip() == "-"

        # Tolerated: a MISSING counter. Creating it is the unnamed path's job, so the
        # query must succeed and must leave the file absent.
        node.exec_in_container(["bash", "-c", f"rm -f {counter}"])
        node.query("ALTER TABLE t_states FREEZE WITH NAME 'named_missing_states'")
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"test -d /var/lib/clickhouse/shadow/named_missing_states "
                    f"&& test ! -e {counter} && echo yes || echo no",
                ]
            ).strip()
            == "yes"
        )

        # NOT tolerated: a counter that exists but cannot be opened. This reaches the
        # named path as an errno failure of the SAME `open` call as the missing case,
        # and only the errno distinguishes them, so this half is what pins that
        # filter - without it the errno test could be dropped and every other
        # assertion here would stay green. A directory in its place makes `open` fail
        # with EISDIR; mode bits would not do, because the server runs as root and
        # root opens a mode-000 file successfully.
        node.exec_in_container(["bash", "-c", f"rm -f {counter} && mkdir {counter}"])
        with pytest.raises(Exception) as unopenable:
            node.query(
                "ALTER TABLE t_states FREEZE WITH NAME 'named_unopenable_states'"
            )
        assert "CANNOT_OPEN_FILE" in str(unopenable.value), str(unopenable.value)
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -e /var/lib/clickhouse/shadow/named_unopenable_states "
                    "&& echo yes || echo no",
                ]
            ).strip()
            == "no"
        )

        # The same matrix again with a NUMERIC name. Every case above uses a
        # non-numeric one, which never enters the reservation path, so on its own it
        # leaves that path's error filter untested: broadly suppressing counter errors
        # for numeric names keeps all of the assertions above green.
        #
        # Tolerated: an exhausted counter. No unnamed FREEZE can allocate anything in
        # that state, so a numeric name has no id to be reserved against either, and
        # the overflow guard throws before writing so the stored value survives.
        # `rm -rf` first: the unopenable case above leaves a DIRECTORY at this path,
        # so a plain redirect would fail on the harness rather than on the server.
        node.exec_in_container(
            ["bash", "-c", f"rm -rf {counter} && echo {max_id} > {counter}"]
        )
        node.query("ALTER TABLE t_states FREEZE WITH NAME '4242'")
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -d /var/lib/clickhouse/shadow/4242 && echo yes || echo no",
                ]
            ).strip()
            == "yes"
        )
        assert (
            node.exec_in_container(["bash", "-c", f"cat {counter}"]).strip() == max_id
        )

        # NOT tolerated: an unparsable counter. Without this a blanket handler on the
        # numeric path would satisfy the case above. The query must fail, create
        # nothing, and leave the file untouched.
        node.exec_in_container(["bash", "-c", f"printf %s - > {counter}"])
        with pytest.raises(Exception) as numeric_malformed:
            node.query("ALTER TABLE t_states FREEZE WITH NAME '4243'")
        assert "CANNOT_PARSE_NUMBER" in str(numeric_malformed.value), str(
            numeric_malformed.value
        )
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -e /var/lib/clickhouse/shadow/4243 && echo yes || echo no",
                ]
            ).strip()
            == "no"
        )
        assert node.exec_in_container(["bash", "-c", f"cat {counter}"]).strip() == "-"

        # NOT tolerated: a counter that cannot be opened. A directory in its place
        # makes `open` fail with EISDIR (mode bits would not do, the server is root).
        node.exec_in_container(["bash", "-c", f"rm -f {counter} && mkdir {counter}"])
        with pytest.raises(Exception) as numeric_unopenable:
            node.query("ALTER TABLE t_states FREEZE WITH NAME '4244'")
        assert "CANNOT_OPEN_FILE" in str(numeric_unopenable.value), str(
            numeric_unopenable.value
        )
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -e /var/lib/clickhouse/shadow/4244 && echo yes || echo no",
                ]
            ).strip()
            == "no"
        )

        # Tolerated, and RESERVED: a MISSING counter. Unlike a non-numeric name, a
        # numeric one must recover the counter here, because the lock is released
        # before the directory is created and a concurrent unnamed FREEZE would
        # otherwise pick the same name. The counter must end up AT the name, so the
        # next unnamed FREEZE allocates above it.
        node.exec_in_container(["bash", "-c", f"rm -rf {counter}"])
        node.query("ALTER TABLE t_states FREEZE WITH NAME '4245'")
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -d /var/lib/clickhouse/shadow/4245 && echo yes || echo no",
                ]
            ).strip()
            == "yes"
        )
        assert node.exec_in_container(["bash", "-c", f"cat {counter}"]).strip() == "4245"

        # The name is a FLOOR, not the final value: when the scan finds a backup above
        # it, the recovered counter must clear that maximum rather than settle on the
        # name, or the next unnamed FREEZE would hand out an id an existing backup
        # already holds. Every other recovery case here names a value above everything
        # present, so only this one distinguishes "floored by the name" from "the name
        # wins", and dropping the scan from the numeric branch leaves it the sole
        # failure.
        node.exec_in_container(
            ["bash", "-c", f"rm -rf {counter} && mkdir -p /var/lib/clickhouse/shadow/5000"]
        )
        node.query("ALTER TABLE t_states FREEZE WITH NAME '42'")
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "test -d /var/lib/clickhouse/shadow/42 && echo yes || echo no",
                ]
            ).strip()
            == "yes"
        )
        assert node.exec_in_container(["bash", "-c", f"cat {counter}"]).strip() == "5001"
        node.exec_in_container(
            ["bash", "-c", "rm -rf /var/lib/clickhouse/shadow/5000 /var/lib/clickhouse/shadow/42"]
        )

        # A HEALTHY counter is advanced by a non-numeric name too. Nothing else here
        # pins that: every non-numeric case above starts from a counter that is
        # missing, empty or broken, so replacing the read with a peek would keep them
        # all green while silently stopping advancement.
        node.exec_in_container(["bash", "-c", f"rm -rf {counter} && echo 5 > {counter}"])
        node.query("ALTER TABLE t_states FREEZE WITH NAME 'named_healthy_states'")
        assert node.exec_in_container(["bash", "-c", f"cat {counter}"]).strip() == "6"

        # `shadow/0` is the one numeric name that needs no reservation: a recovered
        # counter is the scan bound plus one, so zero is never allocated. It must
        # therefore leave a missing counter missing, exactly like a non-numeric name.
        node.exec_in_container(["bash", "-c", f"rm -rf {counter}"])
        node.query("ALTER TABLE t_states FREEZE WITH NAME '0'")
        assert (
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"test -d /var/lib/clickhouse/shadow/0 && test ! -e {counter} "
                    "&& echo yes || echo no",
                ]
            ).strip()
            == "yes"
        )
    finally:
        node.query("DROP TABLE IF EXISTS t_states SYNC")
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"rm -rf {counter} && "
                "rm -rf /var/lib/clickhouse/shadow/named_exhausted_states "
                "/var/lib/clickhouse/shadow/named_malformed_states "
                "/var/lib/clickhouse/shadow/named_missing_states "
                "/var/lib/clickhouse/shadow/named_unopenable_states "
                "/var/lib/clickhouse/shadow/named_healthy_states "
                "/var/lib/clickhouse/shadow/4242 /var/lib/clickhouse/shadow/4243 "
                "/var/lib/clickhouse/shadow/4244 /var/lib/clickhouse/shadow/4245 "
                "/var/lib/clickhouse/shadow/5000 /var/lib/clickhouse/shadow/42 "
                "/var/lib/clickhouse/shadow/0",
            ]
        )


def test_freeze_recovery_skips_read_only_disk(started_cluster):
    # A read-only disk can never be a FREEZE destination, so it holds no identifier
    # the recovery scan has to observe. Consulting it anyway makes an unreachable one
    # fail every FREEZE that has to recover the counter, including the very first
    # FREEZE on a server, where no counter file exists yet. The `web_unreachable`
    # disk resolves paths over HTTP against a port nothing listens on, so any attempt
    # to inspect it raises rather than answering.
    instance = node_read_only
    counter = "/var/lib/clickhouse/shadow/increment.txt"
    try:
        assert (
            instance.query(
                "SELECT is_read_only FROM system.disks WHERE name = 'web_unreachable'"
            ).strip()
            == "1"
        )
        # It must be READ-ONLY rather than broken, or the pre-existing broken-disk
        # branch would be what skips it and this test would pin nothing.
        assert (
            get_metric_value(instance, "BrokenDisks") == 0
        ), "the unreachable disk must not be counted as broken"

        instance.query("DROP TABLE IF EXISTS t_ro_skip SYNC")
        instance.query(
            "CREATE TABLE t_ro_skip (id UInt64) ENGINE = MergeTree ORDER BY id"
        )
        instance.query("INSERT INTO t_ro_skip VALUES (1), (2)")

        # No counter file at all: the state of a server that has never frozen. This is
        # the case a scan of the unreachable disk breaks most visibly, because
        # recovery runs before anything has ever been allocated.
        instance.exec_in_container(["bash", "-c", "rm -rf /var/lib/clickhouse/shadow"])
        instance.query("ALTER TABLE t_ro_skip FREEZE")
        assert (
            instance.exec_in_container(["bash", "-c", f"cat {counter}"]).strip() == "1"
        )

        # An EMPTY counter with a numeric name: the reservation path, which recovers
        # through the same scan.
        instance.exec_in_container(["bash", "-c", f": > {counter}"])
        instance.query("ALTER TABLE t_ro_skip FREEZE WITH NAME '4288'")
        assert (
            instance.exec_in_container(["bash", "-c", f"cat {counter}"]).strip()
            == "4288"
        )

        # A disk that turned read-only while still READABLE must be scanned, not
        # skipped. `DiskLocal` enters that state whenever write access is lost and
        # stays non-broken (`DiskLocal::checkAccessImpl`), so it can still hold
        # backups written while it was writable. Omitting it would persist a counter
        # below their maximum and let a later unnamed FREEZE reuse one of them.
        instance.exec_in_container(
            [
                "bash",
                "-c",
                f"rm -rf {counter} && mkdir -p /var/lib/clickhouse/ro_local/shadow/7000",
            ]
        )
        # The periodic disk checker (`local_disk_check_period_ms`) re-evaluates write
        # access, so the state appears on its own once the root is unwritable.
        mount_read_only(instance, "/var/lib/clickhouse/ro_local")
        wait_condition(
            func=lambda: instance.query(
                "SELECT is_read_only FROM system.disks WHERE name = 'ro_local'"
            ).strip(),
            condition=lambda value: value == "1",
            max_attempts=60,
        )
        # It must be read-only rather than broken, or the broken-disk branch would be
        # what skips it and this case would pin nothing.
        assert (
            get_metric_value(instance, "BrokenDisks") == 0
        ), "a readable read-only disk must not be counted as broken"
        instance.query("ALTER TABLE t_ro_skip FREEZE")
        assert (
            instance.exec_in_container(["bash", "-c", f"cat {counter}"]).strip()
            == "7001"
        )
        # Restore write access and remove that backup, so the bound below comes only
        # from the directory the next case plants.
        unmount(instance, "/var/lib/clickhouse/ro_local")
        instance.exec_in_container(
            ["bash", "-c", "rm -rf /var/lib/clickhouse/ro_local/shadow"]
        )

        # A read-only local disk that lists `shadow` but then fails to resolve an entry
        # in it must FAIL the recovery, not be skipped. Such a disk can hold backups
        # written while it was writable, so an entry it cannot resolve may be an
        # allocated identifier, and skipping it would persist a counter below that
        # identifier - the reuse this scan exists to prevent. Only a disk FREEZE can
        # never write to may be skipped.
        #
        # Inspecting a disk takes several metadata calls, and a disk can serve the
        # earlier ones and only reach its backing store on a later one, so the filter
        # must cover every call rather than just the first. A self-referential symlink
        # named like a backup produces exactly that shape: `iterateDirectory` yields
        # the name, and the `existsDirectory` that follows raises ELOOP. Permission
        # bits cannot express this here, because root resolves a directory regardless
        # of its search bit.
        instance.exec_in_container(
            [
                "bash",
                "-c",
                # The whole default shadow/ goes, not only the counter: a backup the
                # cases above allocated there would otherwise supply the bound, so
                # recovery would not run and this case would pin nothing.
                "rm -rf /var/lib/clickhouse/shadow && "
                "mkdir -p /var/lib/clickhouse/ro_local/shadow && "
                "ln -sfn 7100 /var/lib/clickhouse/ro_local/shadow/7100",
            ]
        )
        mount_read_only(instance, "/var/lib/clickhouse/ro_local")
        wait_condition(
            func=lambda: instance.query(
                "SELECT is_read_only FROM system.disks WHERE name = 'ro_local'"
            ).strip(),
            condition=lambda value: value == "1",
            max_attempts=60,
        )
        assert (
            get_metric_value(instance, "BrokenDisks") == 0
        ), "an unresolvable read-only disk must not be counted as broken"
        with pytest.raises(Exception) as unresolvable:
            instance.query("ALTER TABLE t_ro_skip FREEZE")
        assert "Too many levels of symbolic links" in str(
            unresolvable.value
        ) or "ELOOP" in str(unresolvable.value), str(unresolvable.value)
        # No identifier may have been handed out: no numeric backup directory, and
        # no counter VALUE. The counter FILE may exist and be empty - `Increment`
        # creates it before the scan that then fails, and an empty counter is one of
        # the two states this fix recovers from, so it is equivalent to no file at
        # all. Asserting its absence would pin the creation order rather than the
        # safety property, which is that the next FREEZE still recovers above every
        # existing backup (the case below proves it does).
        assert (
            instance.exec_in_container(
                ["bash", "-c", f"test -s {counter} && echo yes || echo no"]
            ).strip()
            == "no"
        ), "a failed recovery must not leave a counter value behind"
        assert (
            instance.exec_in_container(
                [
                    "bash",
                    "-c",
                    "ls /var/lib/clickhouse/shadow 2>/dev/null "
                    "| grep -cE '^[0-9]+$' || true",
                ]
            ).strip()
            == "0"
        ), "a failed recovery must not allocate a backup directory"
        unmount(instance, "/var/lib/clickhouse/ro_local")
        instance.exec_in_container(
            ["bash", "-c", "rm -rf /var/lib/clickhouse/ro_local/shadow"]
        )

        # A backup on a WRITABLE disk must still raise the bound: skipping the
        # unreachable disk must not turn into skipping the scan. This also covers
        # recovery from the empty counter the failed FREEZE above left behind: the
        # value must come from the scan, not restart at 1.
        instance.exec_in_container(
            [
                "bash",
                "-c",
                "rm -rf /var/lib/clickhouse/shadow && "
                "mkdir -p /var/lib/clickhouse/shadow/4289",
            ]
        )
        instance.query("ALTER TABLE t_ro_skip FREEZE")
        assert (
            instance.exec_in_container(["bash", "-c", f"cat {counter}"]).strip()
            == "4290"
        )
    finally:
        # The mount outlives a failed assertion and would keep the disk read-only for
        # every later test in this module, so drop it before anything else.
        unmount(instance, "/var/lib/clickhouse/ro_local")
        instance.query("DROP TABLE IF EXISTS t_ro_skip SYNC")
        instance.exec_in_container(
            [
                "bash",
                "-c",
                "rm -rf /var/lib/clickhouse/shadow /var/lib/clickhouse/ro_local/shadow",
            ]
        )
