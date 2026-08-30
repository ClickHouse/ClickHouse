import os
import threading
import time

from helpers.iceberg_utils import create_iceberg_table, get_uuid_str

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

FAILPOINT = "iceberg_mutation_pause_before_metadata_reread"


def test_mutation_schema_changed_while_running(started_cluster_iceberg_no_spark):
    """A mutation must not execute against a schema it was not validated for.

    `DB::Iceberg::mutate` re-selects the latest metadata file after
    `MutationsInterpreter::validate` has already run, and its retry loop does so again on every
    iteration, while the columns and the sample block still come from the validated
    incarnation. If another writer changes the schema in between, the mutation now fails
    instead of committing a plan that was built for the previous schema.
    """
    instance = started_cluster_iceberg_no_spark.instances["node1"]

    table_name = "test_mutation_schema_changed_" + get_uuid_str()

    create_iceberg_table("local", instance, table_name, started_cluster_iceberg_no_spark, "(x String, y Int64)")
    instance.query(f"INSERT INTO {table_name} VALUES ('a', 1), ('b', 2);")

    instance.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")

    error = []

    def run_mutation():
        try:
            instance.query(
                f"ALTER TABLE {table_name} DELETE WHERE y = 1;",
                settings={"mutations_sync": 2, "allow_insert_into_iceberg": 1},
            )
        except Exception as e:  # noqa: BLE001 - the assertion is on the message below
            error.append(str(e))

    mutation = threading.Thread(target=run_mutation)
    mutation.start()
    try:
        # Wait until the mutation is parked on the failpoint. Everything else it does on a
        # two-row local table takes milliseconds, so a query that has been running for seconds
        # is one that is waiting there.
        deadline = time.monotonic() + 60
        while True:
            parked = instance.query(
                "SELECT count() FROM system.processes "
                f"WHERE query ILIKE '%{table_name}%DELETE%' AND elapsed > 3"
            ).strip()
            if parked != "0":
                break
            assert time.monotonic() < deadline, "the mutation never reached the failpoint"
            time.sleep(0.5)

        # The mutation is now parked right after its validation and before it re-reads the
        # metadata, so the schema it committed to is the one it was validated against.
        instance.query(f"ALTER TABLE {table_name} ADD COLUMN z String;")
    finally:
        instance.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        mutation.join()

    assert error, "the mutation succeeded against a schema it was not validated for"
    assert "changed from" in error[0], error[0]

    # The table is intact: the mutation committed nothing.
    assert instance.query(f"SELECT x, y, z FROM {table_name} ORDER BY y").strip() == "a\t1\t\nb\t2\t"
