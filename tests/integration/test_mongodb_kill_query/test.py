import pytest
import uuid
import threading
import time
import pymongo
import urllib.parse
from helpers.config_cluster import mongo_pass
from helpers.cluster import ClickHouseCluster


escaped_mongo_pass = urllib.parse.quote_plus(mongo_pass)

SELECT_FROM_MONGODB = f"""SELECT
    sleepEachRow(0.0001),
    user_id,
    first_name,
    last_name,
    age
FROM mongodb(
    'mongo1:27017',
    'testdb',
    'users',
    'root',
    '{mongo_pass}',
    'user_id Int64, first_name String, last_name String, age Int64'
)
SETTINGS max_block_size = 1000"""

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_mongo=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        mongo_conn = pymongo.MongoClient(
            f"mongodb://root:{escaped_mongo_pass}@localhost:{cluster.mongo_port}/?authSource=admin"
        )
        mongo_conn.admin.command("ping")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def setup_mongodb_users(started_cluster):
    mongo_conn = pymongo.MongoClient(
        f"mongodb://root:{escaped_mongo_pass}@localhost:{cluster.mongo_port}/?authSource=admin"
    )
    db = mongo_conn["testdb"]

    db.command("dropAllUsersFromDatabase")
    db.command("createUser", "root", pwd=mongo_pass, roles=["readWrite"])

    if "users" in db.list_collection_names():
        db["users"].drop()

    users_collection = db["users"]

    batch_size = 1000
    num_batches = 1000
    users = []
    for i in range(1, num_batches * batch_size + 1):
        users.append(
            {
                "user_id": i,
                "first_name": f"User{i}_first",
                "last_name": f"User{i}_last",
                "age": (i % 80) + 18,
            }
        )

        if len(users) >= batch_size:
            users_collection.insert_many(users, ordered=False)
            users = []

    if users:
        users_collection.insert_many(users, ordered=False)

    yield cluster


def test_kill_query(setup_mongodb_users):
    query_id = str(uuid.uuid4())

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            SELECT_FROM_MONGODB,
            query_id=query_id,
        )
        assert "DB::Exception: Query was cancelled" in error

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Generate a chuck")
    time.sleep(1)

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")

    query_thread.join()

    result = node1.query(
        "SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    assert node1.contains_in_log("QUERY_WAS_CANCELLED")


def test_cancel_query(setup_mongodb_users):
    def execute_query():
        node1.exec_in_container(
            [
                "bash",
                "-c",
                f"""/usr/bin/clickhouse client --query "{SELECT_FROM_MONGODB}" --format Null""",
            ]
        )

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Generate a chuck")
    time.sleep(1)

    node1.stop_clickhouse_client()
    node1.wait_for_log_line("DB::Exception: Received 'Cancel' packet from the client")
    time.sleep(1)

    query_thread.join()
    assert node1.contains_in_log("QUERY_WAS_CANCELLED_BY_CLIENT")
