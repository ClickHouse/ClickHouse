# -*- coding: utf-8 -*-

import logging
import os
import socket
import struct
import time

import bson
import pymongo
import pytest

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
cluster.add_instance(
    "node",
    main_configs=[
        "configs/mongo.xml",
        "configs/log.xml",
        "configs/users.xml",
    ],
    user_configs=["configs/default_password.xml"],
    env_variables={"UBSAN_OPTIONS": "print_stacktrace=1"},
)

server_port = 27017

OP_MSG = 2013


def wait_for(condition, timeout=60):
    """An update is a mutation, which is asynchronous, so the new value is awaited rather than
    read straight back."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(1)
    return condition()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # Wait for the Mongo handler to start.
        # Cluster.start waits until port 9000 becomes accessible.
        # Server opens the Mongo compatibility port a bit later.
        cluster.instances["node"].wait_for_log_line("Mongo compatibility protocol")
        yield cluster
    except Exception as ex:
        logging.exception(ex)
        raise ex
    finally:
        cluster.shutdown()


def make_client(user="default", password="123", database="default"):
    node = cluster.instances["node"]
    return pymongo.MongoClient(
        f"mongodb://{user}:{password}@{node.ip_address}:{server_port}/{database}?authMechanism=PLAIN"
    )


def encode_op_msg(command, request_id=1):
    """Encodes a command document as a single OP_MSG frame with one body section."""
    body = b"\x00" + bson.encode(command)
    payload = struct.pack("<I", 0) + body  # flag bits, then the sections
    length = 16 + len(payload)
    header = struct.pack("<iiii", length, request_id, 0, OP_MSG)
    return header + payload


def read_op_msg(sock):
    """Reads exactly one reply frame and returns its body document."""
    header = b""
    while len(header) < 16:
        chunk = sock.recv(16 - len(header))
        assert chunk, "connection closed while reading a reply header"
        header += chunk
    length = struct.unpack("<i", header[:4])[0]
    payload = b""
    while len(payload) < length - 16:
        chunk = sock.recv(length - 16 - len(payload))
        assert chunk, "connection closed while reading a reply body"
        payload += chunk
    # 4 bytes of flag bits, then the section kind byte, then the document.
    return bson.decode(payload[5:])


def connect_raw():
    node = cluster.instances["node"]
    sock = socket.create_connection((node.ip_address, server_port), timeout=30)
    sock.settimeout(30)
    return sock


def test_count_query(started_cluster):
    client = make_client()
    db = client["db"]
    collection = db["count"]

    collection.drop()
    documents = [
        {"name": "Bob Johnson", "age": 32, "city": 1},
        {"name": "Charlie Brown", "age": 24, "city": 2},
        {"name": "David Williams", "age": 40, "city": 3},
    ]
    collection.insert_many(documents)

    assert collection.estimated_document_count() == 3

    collection.delete_many({"age": 24})

    assert collection.estimated_document_count() == 2


def test_find_query(started_cluster):
    client = make_client()
    db = client["db"]
    collection = db["find"]

    collection.drop()
    documents = [
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "David Williams", "age": 40, "city": "Chicago"},
    ]
    collection.insert_many(documents)

    find_docs = [doc for doc in collection.find({})]
    find_docs = sorted(find_docs, key=lambda x: x["age"])

    assert find_docs == [
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
        {"name": "David Williams", "age": 40, "city": "Chicago"},
    ]

    find_docs = [doc for doc in collection.find({}).limit(2)]
    assert len(find_docs) == 2

    find_docs = [doc for doc in collection.find({"age": 24})]
    assert find_docs == [
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
    ]

    find_docs = [doc for doc in collection.find(projection={"abacaba": "name"})]
    find_docs = sorted(find_docs, key=lambda x: x["abacaba"])
    assert find_docs == [
        {"abacaba": "Bob Johnson"},
        {"abacaba": "Charlie Brown"},
        {"abacaba": "David Williams"},
    ]

    find_docs = [doc for doc in collection.find().sort("city", 1)]
    assert find_docs == [
        {"name": "David Williams", "age": 40, "city": "Chicago"},
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
    ]


def test_index(started_cluster):
    client = make_client()
    db = client["db"]
    collection = db["index"]

    collection.drop()
    documents = [
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "David Williams", "age": 40, "city": "Chicago"},
    ]
    collection.insert_many(documents)

    collection.create_index("age")
    find_docs = [doc for doc in collection.find({})]
    find_docs = sorted(find_docs, key=lambda x: x["age"])

    assert find_docs == [
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
        {"name": "David Williams", "age": 40, "city": "Chicago"},
    ]


def test_authentication_user_differs_from_database(started_cluster):
    """The user name comes from the `PLAIN` payload, not from the authentication database."""
    node = cluster.instances["node"]
    node.query("DROP USER IF EXISTS mongo_user", password="123")
    node.query(
        "CREATE USER mongo_user IDENTIFIED WITH plaintext_password BY 'mongo_pass'", password="123"
    )
    node.query("GRANT CURRENT GRANTS ON *.* TO mongo_user", password="123")

    # The authentication database `admin` is neither the user name nor an existing database.
    client = make_client(user="mongo_user", password="mongo_pass", database="admin")
    collection = client["db_auth"]["users"]
    collection.drop()
    collection.insert_many([{"id": 1}])
    assert [doc for doc in collection.find({})] == [{"id": 1}]

    # A wrong password must be rejected rather than accepted because `$db` happens to match.
    bad_client = make_client(user="mongo_user", password="wrong", database="admin")
    with pytest.raises(pymongo.errors.PyMongoError):
        bad_client["db_auth"]["users"].insert_many([{"id": 2}])

    node.query("DROP USER mongo_user", password="123")


def test_same_collection_in_two_databases(started_cluster):
    """Two Mongo databases with the same collection name must not share a table."""
    client = make_client()
    first = client["db_first"]["shared"]
    second = client["db_second"]["shared"]

    first.drop()
    second.drop()

    first.insert_many([{"id": 1, "value": "first"}])
    second.insert_many([{"id": 2, "value": "second"}])

    assert [doc for doc in first.find({})] == [{"id": 1, "value": "first"}]
    assert [doc for doc in second.find({})] == [{"id": 2, "value": "second"}]

    assert first.estimated_document_count() == 1
    assert second.estimated_document_count() == 1

    # Dropping one of them must leave the other one alone.
    first.drop()
    assert second.estimated_document_count() == 1
    assert [doc for doc in second.find({})] == [{"id": 2, "value": "second"}]


def test_insert_special_values(started_cluster):
    """Values are never concatenated into the query text, so quotes are inserted verbatim."""
    client = make_client()
    collection = client["db"]["special_values"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "name": "O'Reilly"},
            {"id": 2, "name": "a'); DROP TABLE db.special_values; --"},
            {"id": 3, "name": 'back\\slash and "quotes"'},
        ]
    )

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert found == [
        {"id": 1, "name": "O'Reilly"},
        {"id": 2, "name": "a'); DROP TABLE db.special_values; --"},
        {"id": 3, "name": 'back\\slash and "quotes"'},
    ]


def test_insert_arrays(started_cluster):
    client = make_client()
    collection = client["db"]["arrays"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "tags": ["a", "b", "c"]},
            {"id": 2, "tags": []},
        ]
    )

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert found == [
        {"id": 1, "tags": ["a", "b", "c"]},
        {"id": 2, "tags": []},
    ]


def test_insert_heterogeneous_documents(started_cluster):
    """The schema comes from the first document: missing fields default, unknown ones fail."""
    client = make_client()
    collection = client["db"]["heterogeneous"]

    collection.drop()
    # The second document has no `b`, which gets the default value of its column.
    collection.insert_many([{"a": 1, "b": 2}, {"a": 3}])

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["a"])
    assert found == [{"a": 1, "b": 2}, {"a": 3, "b": 0}]

    # A field that is not in the schema is rejected instead of being written to a wrong column.
    with pytest.raises(pymongo.errors.PyMongoError):
        collection.insert_many([{"a": 4, "unknown_field": 5}])

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["a"])
    assert found == [{"a": 1, "b": 2}, {"a": 3, "b": 0}]


def test_create_collection(started_cluster):
    client = make_client()
    db = client["db_create"]
    db.drop_collection("explicit")

    db.create_collection("explicit")
    assert "explicit" in db.list_collection_names()

    # A collection created explicitly has no document to infer a schema from, so it is empty and
    # the first insert gives it the columns of the inserted document, exactly like a collection
    # created by that insert.
    assert [doc for doc in db["explicit"].find({})] == []

    db["explicit"].insert_many([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    found = sorted((doc for doc in db["explicit"].find({})), key=lambda x: x["a"])
    assert found == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

    assert [doc for doc in db["explicit"].find({"a": 2})] == [{"a": 2, "b": "y"}]

    db.drop_collection("explicit")
    assert "explicit" not in db.list_collection_names()


def test_increment_update(started_cluster):
    client = make_client()
    collection = client["db"]["increment"]

    collection.drop()
    collection.insert_many([{"id": 1, "counter": 10}, {"id": 2, "counter": 20}])

    collection.update_many({"id": 1}, {"$inc": {"counter": 5}})

    # Mutations are asynchronous, so wait for the new value to become visible.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
        if found == [{"id": 1, "counter": 15}, {"id": 2, "counter": 20}]:
            break
        time.sleep(1)

    assert found == [{"id": 1, "counter": 15}, {"id": 2, "counter": 20}]


def test_two_frames_in_one_packet(started_cluster):
    """Two messages sent in a single write must be answered as two separate messages."""
    sock = connect_raw()
    try:
        first = encode_op_msg({"isMaster": 1, "$db": "admin"}, request_id=1)
        second = encode_op_msg({"isMaster": 1, "$db": "admin"}, request_id=2)
        sock.sendall(first + second)

        assert read_op_msg(sock)["ok"] == 1.0
        assert read_op_msg(sock)["ok"] == 1.0
    finally:
        sock.close()


def test_frame_split_across_packets(started_cluster):
    """A message split across two writes must be reassembled instead of truncated."""
    sock = connect_raw()
    try:
        frame = encode_op_msg({"isMaster": 1, "$db": "admin"})
        sock.sendall(frame[:10])
        time.sleep(0.5)
        sock.sendall(frame[10:])

        assert read_op_msg(sock)["ok"] == 1.0
    finally:
        sock.close()


@pytest.mark.parametrize(
    "message_length",
    [
        0,  # shorter than the header
        15,  # still shorter than the header
        0x7FFFFFFF,  # larger than the advertised maximum message size
    ],
)
def test_invalid_message_length(started_cluster, message_length):
    """An out of range message length must be rejected without an unbounded allocation."""
    sock = connect_raw()
    try:
        sock.sendall(struct.pack("<iiii", message_length, 1, 0, OP_MSG))
        # The server closes the connection instead of trying to read the declared length.
        sock.settimeout(30)
        assert sock.recv(1) == b""
    finally:
        sock.close()

    # The server is still healthy and answers on a new connection.
    assert cluster.instances["node"].query("SELECT 1", password="123").strip() == "1"


@pytest.mark.parametrize("document_length", [0, 4, 0x7FFFFFFF])
def test_invalid_bson_length(started_cluster, document_length):
    """A BSON length that does not fit the frame must be rejected, not trusted."""
    sock = connect_raw()
    try:
        body = b"\x00" + struct.pack("<i", document_length) + b"\x00" * 8
        payload = struct.pack("<I", 0) + body
        sock.sendall(struct.pack("<iiii", 16 + len(payload), 1, 0, OP_MSG) + payload)
        sock.settimeout(30)
        # Either an error document or a closed connection is fine; a hang or a crash is not.
        sock.recv(4096)
    finally:
        sock.close()

    assert cluster.instances["node"].query("SELECT 1", password="123").strip() == "1"


def test_delete_one_is_rejected(started_cluster):
    """`delete_one` (limit: 1) cannot be expressed as a ClickHouse mutation, so it must be
    rejected instead of being silently widened into `deleteMany`."""
    client = make_client()
    collection = client["db"]["delete_one_rejected"]

    collection.drop()
    collection.insert_many([{"id": 1}, {"id": 2}])

    with pytest.raises(pymongo.errors.PyMongoError):
        collection.delete_one({"id": 1})

    # Nothing was deleted: the unsupported shape must not delete every matching row.
    assert collection.estimated_document_count() == 2


def test_bulk_delete_executes_every_spec(started_cluster):
    """A `delete` command may carry several delete specs; all of them must be executed."""
    client = make_client()
    collection = client["db"]["bulk_delete"]

    collection.drop()
    collection.insert_many([{"id": 1}, {"id": 2}, {"id": 3}])

    collection.bulk_write(
        [pymongo.DeleteMany({"id": 1}), pymongo.DeleteMany({"id": 2})]
    )

    assert collection.estimated_document_count() == 1


def test_update_one_and_upsert_are_rejected(started_cluster):
    """`update_one` (multi: false) and `upsert: true` cannot be expressed as a ClickHouse
    mutation, so they must be rejected instead of being silently widened or dropped."""
    client = make_client()
    collection = client["db"]["update_one_rejected"]

    collection.drop()
    collection.insert_many([{"id": 1, "counter": 10}, {"id": 2, "counter": 20}])

    with pytest.raises(pymongo.errors.PyMongoError):
        collection.update_one({"id": 1}, {"$set": {"counter": 0}})

    with pytest.raises(pymongo.errors.PyMongoError):
        collection.update_many({"id": 1}, {"$set": {"counter": 0}}, upsert=True)

    # Nothing was updated by the rejected commands.
    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert found == [{"id": 1, "counter": 10}, {"id": 2, "counter": 20}]


def test_find_by_bool_and_double(started_cluster):
    """Filters and comparison operators must cover every scalar type the insert path can
    create a column from, not only int and String."""
    client = make_client()
    collection = client["db"]["scalar_types"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "active": True, "score": 1.5},
            {"id": 2, "active": False, "score": 2.5},
        ]
    )

    assert [doc["id"] for doc in collection.find({"active": True})] == [1]
    assert [doc["id"] for doc in collection.find({"score": 2.5})] == [2]
    assert [doc["id"] for doc in collection.find({"score": {"$gt": 2.0}})] == [2]
    assert [doc["id"] for doc in collection.find({"active": {"$ne": True}})] == [2]


def test_unknown_operator_is_an_error(started_cluster):
    """An unsupported operator object must produce a controlled Mongo error, not a null
    AST inside the wire handlers."""
    client = make_client()
    collection = client["db"]["unknown_operator"]

    collection.drop()
    collection.insert_many([{"id": 1}, {"id": 2}])

    # `$typo` is not an operator at all, so it stays unsupported however much of the Mongo
    # surface is implemented, unlike the `$in` and `$mod` this test used before.
    with pytest.raises(pymongo.errors.PyMongoError):
        [doc for doc in collection.find({"id": {"$typo": 1}})]

    # The server is still healthy after the rejected query.
    assert [doc["id"] for doc in collection.find({"id": 1})] == [1]


def test_aggregate(started_cluster):
    """An aggregation pipeline becomes a chain of `SELECT`s, so the stages that fill a clause
    already filled by an earlier one have to continue on top of a subquery."""
    client = make_client()
    collection = client["db"]["aggregate"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "city": "berlin", "score": 10},
            {"id": 2, "city": "berlin", "score": 20},
            {"id": 3, "city": "paris", "score": 30},
            {"id": 4, "city": "paris", "score": 40},
            {"id": 5, "city": "rome", "score": 50},
        ]
    )

    assert list(collection.aggregate([{"$group": {"_id": None, "c": {"$sum": 1}, "s": {"$sum": "$score"}}}])) == [
        {"_id": None, "c": 5, "s": 150}
    ]

    assert list(
        collection.aggregate(
            [
                {"$match": {"city": {"$ne": "rome"}}},
                {"$group": {"_id": "$city", "c": {"$sum": 1}, "a": {"$avg": "$score"}}},
                {"$sort": {"_id": 1}},
            ]
        )
    ) == [{"_id": "berlin", "c": 2, "a": 15.0}, {"_id": "paris", "c": 2, "a": 35.0}]

    # A `$match` after a `$group` filters the groups rather than the documents.
    assert list(
        collection.aggregate(
            [
                {"$group": {"_id": "$city", "c": {"$sum": 1}}},
                {"$match": {"c": {"$gt": 1}}},
                {"$sort": {"_id": 1}},
            ]
        )
    ) == [{"_id": "berlin", "c": 2}, {"_id": "paris", "c": 2}]

    assert list(collection.aggregate([{"$match": {"score": {"$gte": 30}}}, {"$count": "c"}])) == [{"c": 3}]

    # `$skip` before `$limit` is a `LIMIT ... OFFSET ...`, the other order is not.
    assert [doc["score"] for doc in collection.aggregate([{"$sort": {"score": 1}}, {"$skip": 1}, {"$limit": 2}])] == [20, 30]
    assert [doc["score"] for doc in collection.aggregate([{"$sort": {"score": 1}}, {"$limit": 2}, {"$skip": 1}])] == [20]

    with pytest.raises(pymongo.errors.PyMongoError):
        list(collection.aggregate([{"$unwind": "$city"}]))

    # The server is still healthy after the rejected pipeline.
    assert list(collection.aggregate([{"$match": {"id": 1}}, {"$count": "c"}])) == [{"c": 1}]


def test_distinct(started_cluster):
    """`distinct` is a `$group` on the field, and it takes a filter like a `find` does."""
    client = make_client()
    collection = client["db"]["distinct_values"]

    collection.drop()
    collection.insert_many(
        [{"id": 1, "city": "berlin"}, {"id": 2, "city": "berlin"}, {"id": 3, "city": "paris"}]
    )

    assert sorted(collection.distinct("city")) == ["berlin", "paris"]
    assert sorted(collection.distinct("city", {"id": {"$gte": 3}})) == ["paris"]
    assert sorted(collection.distinct("id")) == [1, 2, 3]


def test_server_commands(started_cluster):
    """The commands a driver or a shell sends without touching a collection. They have to answer,
    because a client that cannot ping or read the version of the server does not get as far as a
    query."""
    client = make_client()
    database = client["db"]

    assert database.command("ping")["ok"] == 1
    assert database.command("buildInfo")["version"]
    assert len(database.command("buildInfo")["versionArray"]) == 4
    assert database.command("connectionStatus")["ok"] == 1

    # There are no server side cursors: the whole result is returned in the first batch.
    killed = database.command("killCursors", "any_collection", cursors=[])
    assert killed["cursorsKilled"] == []
    assert killed["ok"] == 1
    assert database.command("endSessions", [])["ok"] == 1


def test_drop_database(started_cluster):
    """A `dropDatabase` of a database that does not exist is a success in Mongo, so it stays one
    here as well."""
    client = make_client()
    client["dropped_db"]["users"].insert_one({"id": 1})
    assert "dropped_db" in client.list_database_names()

    assert client["dropped_db"].command("dropDatabase")["ok"] == 1
    assert "dropped_db" not in client.list_database_names()
    assert client["dropped_db"].command("dropDatabase")["ok"] == 1


def test_unwind_and_the_other_stages(started_cluster):
    """`$unwind` is an `ARRAY JOIN`, which drops a document whose array is empty unless the stage
    asks to keep it."""
    client = make_client()
    collection = client["db"]["unwound"]

    collection.drop()
    collection.insert_many([{"id": 1, "tags": ["red", "green"]}, {"id": 2, "tags": []}])

    assert [(d["id"], d["tags"]) for d in collection.aggregate([{"$unwind": "$tags"}, {"$sort": {"id": 1, "tags": 1}}])] == [
        (1, "green"),
        (1, "red"),
    ]
    assert [
        (d["id"], d["tags"])
        for d in collection.aggregate(
            [{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": True}}, {"$sort": {"id": 1, "tags": 1}}]
        )
    ] == [(1, "green"), (1, "red"), (2, "")]
    assert [
        d["position"]
        for d in collection.aggregate(
            [{"$unwind": {"path": "$tags", "includeArrayIndex": "position"}}, {"$sort": {"position": 1}}]
        )
    ] == [0, 1]

    assert list(collection.aggregate([{"$unwind": "$tags"}, {"$sortByCount": "$tags"}])) == [
        {"_id": "green", "count": 1},
        {"_id": "red", "count": 1},
    ]
    assert list(collection.aggregate([{"$match": {"id": 1}}, {"$unset": "tags"}])) == [{"id": 1}]
    assert list(collection.aggregate([{"$sample": {"size": 1}}, {"$count": "c"}])) == [{"c": 1}]


def test_update_operators(started_cluster):
    """The update operators all become the assignments of one `ALTER TABLE ... UPDATE`, so a
    statement that both sets and increments is a single mutation."""
    client = make_client()
    collection = client["db"]["updated"]

    collection.drop()
    collection.insert_many([{"id": 1, "name": "alpha", "age": 30, "tags": ["red"]}])

    collection.update_many({"id": 1}, {"$set": {"name": "beta"}, "$inc": {"age": 5}})
    assert wait_for(lambda: collection.find_one({"id": 1})["age"] == 35)
    assert collection.find_one({"id": 1})["name"] == "beta"

    collection.update_many({"id": 1}, {"$mul": {"age": 2}})
    assert wait_for(lambda: collection.find_one({"id": 1})["age"] == 70)

    collection.update_many({"id": 1}, {"$push": {"tags": "green"}})
    assert wait_for(lambda: collection.find_one({"id": 1})["tags"] == ["red", "green"])

    collection.update_many({"id": 1}, {"$addToSet": {"tags": "green"}})
    assert collection.find_one({"id": 1})["tags"] == ["red", "green"]

    collection.update_many({"id": 1}, {"$pull": {"tags": "red"}})
    assert wait_for(lambda: collection.find_one({"id": 1})["tags"] == ["green"])

    with pytest.raises(pymongo.errors.PyMongoError):
        collection.update_many({"id": 1}, {"$bit": {"age": {"and": 1}}})


def test_update_by_a_nested_field(started_cluster):
    """The filter of an `update` names a nested field the same way the filter of a `find` does:
    either as a subdocument or as a dotted path."""
    client = make_client()
    collection = client["db"]["nested_update"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "profile": {"name": "alpha"}, "flag": 0},
            {"id": 2, "profile": {"name": "beta"}, "flag": 0},
        ]
    )

    assert [doc["id"] for doc in collection.find({"profile": {"name": "alpha"}})] == [1]

    collection.update_many({"profile": {"name": "alpha"}}, {"$set": {"flag": 1}})
    assert wait_for(lambda: sorted(doc["flag"] for doc in collection.find({})) == [0, 1])

    collection.update_many({"profile.name": "beta"}, {"$set": {"flag": 2}})
    assert wait_for(lambda: sorted(doc["flag"] for doc in collection.find({})) == [1, 2])


def test_heterogeneous_array_insert(started_cluster):
    """Array schema inference must scan the whole array: a heterogeneous array becomes
    `Array(Dynamic)` instead of failing to insert the very document it was inferred from."""
    client = make_client()
    collection = client["db"]["hetero_array"]

    collection.drop()
    collection.insert_many([{"id": 1, "a": [1, "x"]}])

    assert collection.estimated_document_count() == 1


def test_find_skip(started_cluster):
    """`skip` is ordinary driver pagination, so it must offset the result rather than be
    silently ignored and return the first page again."""
    client = make_client()
    collection = client["db"]["skip"]

    collection.drop()
    collection.insert_many([{"id": i} for i in range(1, 11)])

    assert [doc["id"] for doc in collection.find({}).sort("id", 1).skip(7)] == [8, 9, 10]
    assert [doc["id"] for doc in collection.find({}).sort("id", 1).skip(2).limit(3)] == [
        3,
        4,
        5,
    ]
    assert [doc["id"] for doc in collection.find({"id": {"$gt": 5}}).sort("id", -1).skip(1).limit(2)] == [9, 8]


def test_count_with_query(started_cluster):
    """The `count` command carries a `query` filter and the `limit` and `skip` bounds, which
    take the same path as a `find` instead of counting the whole collection."""
    client = make_client()
    db = client["db"]
    collection = db["count_query"]

    collection.drop()
    collection.insert_many([{"id": i, "kind": "even" if i % 2 == 0 else "odd"} for i in range(1, 8)])

    assert db.command({"count": "count_query"})["n"] == 7
    assert db.command({"count": "count_query", "query": {"kind": "odd"}})["n"] == 4
    assert db.command({"count": "count_query", "query": {"id": {"$gt": 5}}})["n"] == 2
    assert db.command({"count": "count_query", "query": {"kind": "odd"}, "limit": 2})["n"] == 2
    assert db.command({"count": "count_query", "query": {"kind": "odd"}, "skip": 3})["n"] == 1


def test_insert_int64(started_cluster):
    """A value outside the 32-bit range is a BSON `int64`, whose column must be a valid
    ClickHouse type rather than the unregistered alias `long`."""
    client = make_client()
    collection = client["db"]["int64"]

    collection.drop()
    collection.insert_many([{"id": 1, "big": 2147483648}])

    assert [doc for doc in collection.find({})] == [{"id": 1, "big": 2147483648}]
    assert [doc["id"] for doc in collection.find({"big": {"$gt": 2}})] == [1]


def test_insert_datetime(started_cluster):
    """A driver sends a `datetime` as a BSON date, which arrives as the Extended JSON wrapper
    `{"$date": ...}`: it is one value of one field, not a subdocument to descend into."""
    import datetime

    client = make_client()
    collection = client["db"]["dates"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "when": datetime.datetime(2020, 5, 17, 10, 30, 0)},
            {"id": 2, "when": datetime.datetime(2021, 6, 18, 11, 45, 30, 123000)},
        ]
    )

    # The `DateTime64` column comes back as a BSON date, so the round trip returns exactly
    # the inserted values.
    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert found == [
        {"id": 1, "when": datetime.datetime(2020, 5, 17, 10, 30, 0)},
        {"id": 2, "when": datetime.datetime(2021, 6, 18, 11, 45, 30, 123000)},
    ]

    assert [doc["id"] for doc in collection.find({"when": {"$gt": datetime.datetime(2021, 1, 1)}})] == [2]


def test_insert_unsupported_bson_type(started_cluster):
    """A BSON type with no ClickHouse counterpart is rejected explicitly instead of silently
    becoming bogus `<field>.$<wrapper>` columns."""
    client = make_client()
    collection = client["db"]["unsupported_bson"]

    collection.drop()
    with pytest.raises(pymongo.errors.PyMongoError):
        collection.insert_many([{"id": 1, "raw": bson.Binary(b"\x00\x01")}])

    # The rejected insert must not have created the collection with rewritten field names.
    assert "unsupported_bson" not in client["db"].list_collection_names()


def test_aggregate_match_subdocument(started_cluster):
    """A `$match` uses the query syntax, so a subdocument names nested fields exactly like the
    filter of a `find` - including when the `$match` comes from the `query` of a `distinct`."""
    client = make_client()
    collection = client["db"]["match_subdocument"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "profile": {"name": "alpha"}},
            {"id": 2, "profile": {"name": "beta"}},
            {"id": 3, "profile": {"name": "alpha"}},
        ]
    )

    assert [doc["id"] for doc in collection.aggregate([{"$match": {"profile": {"name": "alpha"}}}, {"$sort": {"id": 1}}, {"$project": {"id": 1}}])] == [1, 3]

    assert collection.distinct("id", {"profile": {"name": "alpha"}}) == [1, 3]


def test_readback_preserves_document_shape(started_cluster):
    """What a driver reads back must have the shape and the types of what it inserted: a nested
    document comes back as a nested document rather than literal dotted keys, and a date as a
    BSON date rather than a string - for a `find`, an `aggregate` and a `distinct` alike."""
    import copy
    import datetime

    client = make_client()
    collection = client["db"]["readback_shape"]

    documents = [
        {"id": 1, "profile": {"name": "alpha", "age": 30}, "when": datetime.datetime(2021, 6, 1, 12, 0, 0)},
        {"id": 2, "profile": {"name": "beta", "age": 40}, "when": datetime.datetime(2022, 7, 2, 13, 30, 0)},
    ]

    collection.drop()
    # `insert_many` adds a client-generated `_id` to the documents it is given, which the server
    # does not store, so it must not reach the expected value.
    collection.insert_many(copy.deepcopy(documents))

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert found == documents

    # An empty projection asks for the whole document.
    found = sorted((doc for doc in collection.find({}, {})), key=lambda x: x["id"])
    assert found == documents

    # A projection of a nested field keeps its document shape.
    assert [doc for doc in collection.find({"id": 1}, {"profile.name": 1})] == [{"profile": {"name": "alpha"}}]

    assert [doc for doc in collection.aggregate([{"$match": {"id": 2}}, {"$project": {"when": 1}}])] == [
        {"when": datetime.datetime(2022, 7, 2, 13, 30, 0)}
    ]

    assert collection.distinct("when") == [
        datetime.datetime(2021, 6, 1, 12, 0, 0),
        datetime.datetime(2022, 7, 2, 13, 30, 0),
    ]


def test_union_with_nested_match(started_cluster):
    """The `$match` stages inside the pipeline of a `$unionWith` use the same query syntax as the
    outer ones, so their subdocument filters are normalized the same way."""
    client = make_client()
    collection = client["db"]["union_nested_match"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "profile": {"name": "alpha"}},
            {"id": 2, "profile": {"name": "beta"}},
        ]
    )

    other = client["db"]["union_nested_match_other"]
    other.drop()
    other.insert_many(
        [
            {"id": 3, "profile": {"name": "alpha"}},
            {"id": 4, "profile": {"name": "beta"}},
        ]
    )

    result = collection.aggregate(
        [
            {"$match": {"profile": {"name": "alpha"}}},
            {
                "$unionWith": {
                    "coll": "union_nested_match_other",
                    "pipeline": [{"$match": {"profile": {"name": "alpha"}}}],
                }
            },
            {"$sort": {"id": 1}},
            {"$project": {"id": 1}},
        ]
    )
    assert [doc["id"] for doc in result] == [1, 3]


def test_dollar_field_path_is_an_error(started_cluster):
    """`$` by itself is not a valid field path, so it must be a controlled Mongo error rather
    than an abort on an empty identifier."""
    client = make_client()
    collection = client["db"]["dollar_path"]

    collection.drop()
    collection.insert_many([{"id": 1}, {"id": 2}])

    with pytest.raises(pymongo.errors.PyMongoError):
        list(collection.aggregate([{"$group": {"_id": "$", "c": {"$sum": 1}}}]))

    with pytest.raises(pymongo.errors.PyMongoError):
        list(collection.aggregate([{"$unwind": "$"}]))

    # The server is still healthy after the rejected pipelines.
    assert sorted(doc["id"] for doc in collection.find({})) == [1, 2]


def test_insert_decimal_round_trip(started_cluster):
    """A BSON decimal128 arrives as `{"$numberDecimal": ...}` and becomes a `Decimal128`
    column of the scale of the value, and the reply encodes the column back as a BSON
    decimal128 - not as a double, which could not hold all of its digits."""
    from bson.decimal128 import Decimal128

    client = make_client()
    collection = client["db"]["decimals"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "price": Decimal128("12345678901234567890.1234567890")},
            {"id": 2, "price": Decimal128("-0.0000000001")},
        ]
    )

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert [type(doc["price"]) for doc in found] == [Decimal128, Decimal128]
    assert found == [
        {"id": 1, "price": Decimal128("12345678901234567890.1234567890")},
        {"id": 2, "price": Decimal128("-0.0000000001")},
    ]

    distinct = collection.distinct("price")
    assert all(type(value) is Decimal128 for value in distinct)
    assert sorted(distinct, key=lambda value: value.to_decimal()) == [
        Decimal128("-0.0000000001"),
        Decimal128("12345678901234567890.1234567890"),
    ]


def test_insert_arrays_of_bson_scalars(started_cluster):
    """An array of BSON-only scalars - dates, ObjectIds, decimals - is inserted through the same
    wrapper conversion as a scalar field, and an array of subdocuments may contain them too."""
    import datetime

    from bson.decimal128 import Decimal128
    from bson.objectid import ObjectId

    client = make_client()
    collection = client["db"]["wrapper_arrays"]

    collection.drop()
    oid = ObjectId("64c1f00000000000000000aa")
    collection.insert_many(
        [
            {
                "id": 1,
                "dates": [datetime.datetime(2020, 5, 17, 10, 30, 0), datetime.datetime(2021, 6, 18, 11, 45, 30)],
                "ids": [oid],
                "prices": [Decimal128("1.5000000000"), Decimal128("-2.2500000000")],
            }
        ]
    )

    found = list(collection.find({}, {"_id": 0}))
    assert found == [
        {
            "id": 1,
            "dates": [datetime.datetime(2020, 5, 17, 10, 30, 0), datetime.datetime(2021, 6, 18, 11, 45, 30)],
            "ids": [str(oid)],
            "prices": [Decimal128("1.5000000000"), Decimal128("-2.2500000000")],
        }
    ]

    # A subdocument inside an array keeps its shape; the wrapper inside it becomes the value it
    # wraps, kept structurally by the `JSON` column the array of subdocuments lands in.
    events = client["db"]["wrapper_array_subdocs"]
    events.drop()
    events.insert_many([{"id": 1, "events": [{"name": "start", "at": datetime.datetime(2020, 1, 1, 0, 0, 0)}]}])
    found = list(events.find({}, {"_id": 0}))
    assert found[0]["id"] == 1
    assert found[0]["events"][0]["name"] == "start"


def test_find_projection_excludes_id(started_cluster):
    """`{"name": 1, "_id": 0}` is the usual way to ask for "only these fields", so an inclusion
    projection accepts an exclusion of `_id` - and only of `_id`."""
    client = make_client()
    collection = client["db"]["projection_id"]

    collection.drop()
    collection.insert_many([{"id": 1, "name": "alpha", "age": 30}, {"id": 2, "name": "beta", "age": 40}])

    found = sorted((doc for doc in collection.find({}, {"name": 1, "_id": 0})), key=lambda x: x["name"])
    assert found == [{"name": "alpha"}, {"name": "beta"}]

    with pytest.raises(pymongo.errors.PyMongoError):
        list(collection.find({}, {"name": 1, "age": 0}))


def test_insert_explicit_null(started_cluster):
    """An explicit `null` is a real Mongo value, not an omitted field: it must survive the
    round trip rather than be erased, both as the first value of its field - which fixes the
    column type to `Dynamic` - and as the only field of the first document."""
    client = make_client()
    collection = client["db"]["nulls"]

    collection.drop()
    collection.insert_many([{"id": 1, "note": None}, {"id": 2, "note": "set"}])

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert found == [{"id": 1, "note": None}, {"id": 2, "note": "set"}]

    only_null = client["db"]["only_null"]
    only_null.drop()
    only_null.insert_many([{"note": None}])
    assert [doc for doc in only_null.find({})] == [{"note": None}]


def test_find_limit_zero_means_no_limit(started_cluster):
    """`limit: 0` means no limit in MongoDB, so it must return every matching document
    rather than an empty cursor. A negative limit reads as its absolute value."""
    client = make_client()
    collection = client["db"]["limit_zero"]

    collection.drop()
    collection.insert_many([{"id": i} for i in range(1, 6)])

    assert [doc["id"] for doc in collection.find({}).sort("id", 1).limit(0)] == [1, 2, 3, 4, 5]
    assert [doc["id"] for doc in collection.find({"id": {"$gt": 2}}).sort("id", 1).limit(0)] == [3, 4, 5]
    assert [doc["id"] for doc in collection.find({}).sort("id", 1).limit(-2)] == [1, 2]


def test_current_date_forms(started_cluster):
    """`$currentDate` accepts `true` and `{"$type": "date"}`. `{"$type": "timestamp"}` asks for
    the BSON timestamp, which does not exist here, and `false` is an error in MongoDB as well -
    neither may mutate the row."""
    import datetime

    client = make_client()
    collection = client["db"]["current_date"]

    collection.drop()
    collection.insert_many([{"id": 1, "seen": datetime.datetime(2000, 1, 1)}])

    def seen(): return collection.find({})[0]["seen"]

    collection.update_many({"id": 1}, {"$currentDate": {"seen": True}})
    assert wait_for(lambda: seen() > datetime.datetime(2020, 1, 1))

    collection.update_many({"id": 1}, {"$set": {"seen": datetime.datetime(2000, 1, 1)}})
    wait_for(lambda: seen() == datetime.datetime(2000, 1, 1))
    collection.update_many({"id": 1}, {"$currentDate": {"seen": {"$type": "date"}}})
    assert wait_for(lambda: seen() > datetime.datetime(2020, 1, 1))

    collection.update_many({"id": 1}, {"$set": {"seen": datetime.datetime(2000, 1, 1)}})
    wait_for(lambda: seen() == datetime.datetime(2000, 1, 1))
    with pytest.raises(pymongo.errors.PyMongoError):
        collection.update_many({"id": 1}, {"$currentDate": {"seen": False}})
    with pytest.raises(pymongo.errors.PyMongoError):
        collection.update_many({"id": 1}, {"$currentDate": {"seen": {"$type": "timestamp"}}})
    # The rejected forms did not mutate the row.
    time.sleep(2)
    assert seen() == datetime.datetime(2000, 1, 1)


def test_oversized_result_is_an_error(started_cluster):
    """The reply to a `find` is a single BSON document and the cursor id is always 0, so a
    result that does not fit into the advertised `maxBsonObjectSize` must be a controlled
    error rather than an oversized reply the driver would refuse to read."""
    client = make_client()
    collection = client["db"]["oversized"]

    collection.drop()
    # Five documents of ~4 MiB each: each fits comfortably, together they exceed the 16 MiB
    # `maxBsonObjectSize` a single reply may hold.
    for i in range(5):
        collection.insert_many([{"id": i, "payload": "x" * (4 * 1024 * 1024)}])

    with pytest.raises(pymongo.errors.PyMongoError):
        list(collection.find({}))

    # A bounded ask still works.
    assert [doc["id"] for doc in collection.find({}, {"id": 1}).sort("id", 1)] == [0, 1, 2, 3, 4]
    assert len(list(collection.find({}).limit(2))) == 2


def test_oversized_reply_of_many_small_rows_is_an_error(started_cluster):
    """The size bound of a reply must hold for the document sent on the wire, not only for the
    sum of the embedded row documents: every row also costs an element header in the
    `firstBatch` array, and with many small rows those headers alone add megabytes."""
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.small_rows (a Int32) ENGINE = MergeTree ORDER BY a",
        password="123",
    )
    # 1.2 million rows of `{"a": <int32>}` embed as ~14.4 MB of row documents - under the
    # 16 MiB `maxBsonObjectSize` - but the array headers add ~10 MB more on the wire.
    node.query(
        "INSERT INTO db.small_rows SELECT number FROM numbers(1200000)", password="123"
    )

    client = make_client()
    collection = client["db"]["small_rows"]

    with pytest.raises(pymongo.errors.PyMongoError):
        list(collection.find({}))

    # A bounded ask still works.
    assert [doc["a"] for doc in collection.find({}).sort("a", 1).limit(3)] == [0, 1, 2]

    node.query("DROP TABLE db.small_rows", password="123")


def test_oversized_distinct_is_an_error(started_cluster):
    """The reply to `distinct` holds all the values in one document, so it has the same size
    bound as the reply to `find`."""
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.distinct_oversized (id Int32, s String) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    # 20 distinct strings of 1 MB each: any one value fits, together they exceed the 16 MiB
    # `maxBsonObjectSize` a single reply may hold.
    node.query(
        "INSERT INTO db.distinct_oversized SELECT number, repeat(char(65 + number), 1000000) FROM numbers(20)",
        password="123",
    )

    client = make_client()
    collection = client["db"]["distinct_oversized"]

    with pytest.raises(pymongo.errors.PyMongoError):
        collection.distinct("s")

    # A bounded ask still works.
    assert collection.distinct("s", {"id": {"$lt": 2}}) == ["A" * 1000000, "B" * 1000000]

    node.query("DROP TABLE db.distinct_oversized", password="123")


def test_float_denormals_round_trip(started_cluster):
    """BSON doubles can hold `NaN` and the infinities, so a `Float32` / `Float64` column that
    contains them reads back as those values rather than as BSON `null`."""
    import math

    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.denormals (id Int32, f32 Float32, f64 Float64) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    node.query(
        "INSERT INTO db.denormals VALUES (1, nan, nan), (2, inf, inf), (3, -inf, -inf), (4, 1.5, 2.5)",
        password="123",
    )

    client = make_client()
    collection = client["db"]["denormals"]

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert len(found) == 4
    assert math.isnan(found[0]["f32"]) and math.isnan(found[0]["f64"])
    assert found[1]["f32"] == math.inf and found[1]["f64"] == math.inf
    assert found[2]["f32"] == -math.inf and found[2]["f64"] == -math.inf
    assert found[3]["f32"] == 1.5 and found[3]["f64"] == 2.5

    node.query("DROP TABLE db.denormals", password="123")


def test_insert_decimal_scale_is_preserved(started_cluster):
    """A `$numberDecimal` keeps the scale of its value: `0.00000000001` has eleven fractional
    digits, which a fixed `Decimal128(10)` column would silently round away. An array of
    decimals of different scales becomes one column of the widest scale, so its values read
    back padded to that scale - other members of the same cohorts."""
    from bson.decimal128 import Decimal128

    client = make_client()

    # One collection per value: the column type is inferred from the first document of a
    # collection, so this is what pins the scale each value gets for itself.
    eleven = client["db"]["decimal_scale_eleven"]
    eleven.drop()
    eleven.insert_one({"id": 1, "d": Decimal128("0.00000000001")})
    assert list(eleven.find({})) == [{"id": 1, "d": Decimal128("0.00000000001")}]

    twenty = client["db"]["decimal_scale_twenty"]
    twenty.drop()
    twenty.insert_one({"id": 1, "d": Decimal128("1E-20")})
    assert list(twenty.find({})) == [
        {"id": 1, "d": Decimal128("0.00000000000000000001")}
    ]

    arrays = client["db"]["decimal_scale_arrays"]
    arrays.drop()
    arrays.insert_one({"id": 1, "prices": [Decimal128("1.5"), Decimal128("1.25")]})
    assert list(arrays.find({})) == [
        {"id": 1, "prices": [Decimal128("1.50"), Decimal128("1.25")]}
    ]


def test_insert_decimal_that_fits_no_decimal128_is_an_error(started_cluster):
    """Mongo's decimal128 is a floating point type with an exponent of its own, so some of its
    values fit no ClickHouse `Decimal128` of any scale. Such a value is rejected with an error
    rather than silently rounded."""
    from bson.decimal128 import Decimal128

    client = make_client()
    collection = client["db"]["decimal_rejects"]
    collection.drop()

    with pytest.raises(pymongo.errors.PyMongoError):
        collection.insert_one({"id": 1, "d": Decimal128("1E+40")})
    with pytest.raises(pymongo.errors.PyMongoError):
        collection.insert_one({"id": 2, "d": Decimal128("NaN")})


def test_in_on_array_field_tests_the_elements(started_cluster):
    """Mongo applies `$in` to an array field by asking whether any element of it is among the
    candidates, and `$nin` is its negation. `$size` matches arrays only: a string of the right
    length is not a match."""
    client = make_client()
    collection = client["db"]["tagged"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "name": "alpha", "tags": ["red", "blue"]},
            {"id": 2, "name": "beta", "tags": ["green"]},
        ]
    )

    assert [doc["id"] for doc in collection.find({"tags": {"$in": ["red"]}})] == [1]
    assert [doc["id"] for doc in collection.find({"tags": {"$nin": ["red"]}})] == [2]
    assert [doc["id"] for doc in collection.find({"name": {"$in": ["alpha"]}})] == [1]
    assert [doc["id"] for doc in collection.find({"tags": {"$size": 2}})] == [1]
    assert [doc["id"] for doc in collection.find({"name": {"$size": 5}})] == []


def test_scalar_equality_matches_array_elements(started_cluster):
    """Mongo's scalar equality is also its canonical array membership form: `{tags: "red"}`
    and `{tags: {"$eq": "red"}}` must match a document whose `tags` array holds the value,
    and `$ne` is the negation."""
    client = make_client()
    collection = client["db"]["equality_on_arrays"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "name": "alpha", "tags": ["red", "blue"]},
            {"id": 2, "name": "beta", "tags": ["green"]},
        ]
    )

    assert [doc["id"] for doc in collection.find({"tags": "red"})] == [1]
    assert [doc["id"] for doc in collection.find({"tags": {"$eq": "green"}})] == [2]
    assert [doc["id"] for doc in collection.find({"tags": {"$ne": "red"}})] == [2]
    # A scalar field keeps plain equality semantics through the same lowering.
    assert [doc["id"] for doc in collection.find({"name": "beta"})] == [2]
    assert [doc["id"] for doc in collection.find({"name": {"$ne": "beta"}})] == [1]


def test_connection_status_reports_the_authenticated_user(started_cluster):
    """`connectionStatus` must report the principal `saslStart` authenticated and its roles,
    not an anonymous connection: shells and tools use it to verify the login state."""
    node = cluster.instances["node"]
    node.query("DROP USER IF EXISTS mongo_status_user", password="123")
    node.query("DROP ROLE IF EXISTS mongo_status_role", password="123")
    node.query("CREATE ROLE mongo_status_role", password="123")
    node.query(
        "CREATE USER mongo_status_user IDENTIFIED WITH plaintext_password BY 'mongo_pass'",
        password="123",
    )
    node.query("GRANT CURRENT GRANTS ON *.* TO mongo_status_user", password="123")
    node.query("GRANT mongo_status_role TO mongo_status_user", password="123")

    client = make_client(user="mongo_status_user", password="mongo_pass", database="admin")
    status = client["admin"].command("connectionStatus")
    assert status["ok"] == 1.0
    assert status["authInfo"]["authenticatedUsers"] == [
        {"user": "mongo_status_user", "db": "admin"}
    ]
    assert status["authInfo"]["authenticatedUserRoles"] == [
        {"role": "mongo_status_role", "db": "admin"}
    ]

    node.query("DROP USER mongo_status_user", password="123")
    node.query("DROP ROLE mongo_status_role", password="123")


def test_create_of_an_existing_collection_is_an_error(started_cluster):
    """Mongo's `createCollection` is not idempotent: creating a namespace that already exists
    raises `NamespaceExists` (code 48), which callers rely on to detect that somebody else
    created the collection first."""
    client = make_client()
    database = client["db"]

    database["created_twice"].drop()
    database.command("create", "created_twice")

    with pytest.raises(pymongo.errors.OperationFailure) as error:
        database.command("create", "created_twice")
    assert error.value.code == 48
    assert "already exists" in str(error.value)

    database["created_twice"].drop()


def test_missing_collection_reads_as_empty(started_cluster):
    """Mongo treats a collection that does not exist as empty on the read commands: `find`
    returns an empty cursor, `count` returns 0 and `distinct` returns no values, rather than
    a missing-table error. A malformed query is still an error."""
    client = make_client()
    database = client["db"]
    collection = database["never_created"]
    collection.drop()

    assert list(collection.find({})) == []
    assert collection.find_one({"id": 1}) is None
    assert database.command("count", "never_created")["n"] == 0
    assert collection.distinct("id") == []
    with pytest.raises(pymongo.errors.OperationFailure):
        collection.find_one({"id": {"$typo": 1}})


def test_distinct_on_an_array_field_returns_the_elements(started_cluster):
    """Mongo's `distinct` on an array field is element-wise: a document whose `tags` holds
    `["red", "blue"]` contributes the elements, not the array, and elements shared between
    documents are returned once. The values come back in ascending order."""
    client = make_client()
    collection = client["db"]["distinct_on_arrays"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "name": "alpha", "tags": ["red", "blue"]},
            {"id": 2, "name": "beta", "tags": ["green", "red"]},
        ]
    )

    assert collection.distinct("tags") == ["blue", "green", "red"]
    assert collection.distinct("tags", {"id": 2}) == ["green", "red"]
    # A scalar field keeps its per-document values through the same normalization.
    assert collection.distinct("name") == ["alpha", "beta"]


def test_mutations_on_a_missing_collection_are_noops(started_cluster):
    """Mongo treats a mutation of a namespace that does not exist the same way as a read of it:
    nothing matches, so `update_many` and `delete_many` report zero affected documents instead
    of a missing-table error. A malformed statement is still an error."""
    client = make_client()
    collection = client["db"]["never_created_mutations"]
    collection.drop()

    result = collection.update_many({"x": 1}, {"$set": {"y": 1}})
    assert result.matched_count == 0
    assert result.modified_count == 0

    assert collection.delete_many({"x": 1}).deleted_count == 0

    with pytest.raises(pymongo.errors.OperationFailure):
        collection.update_many({"x": 1}, {"$typo": {"y": 1}})
    with pytest.raises(pymongo.errors.OperationFailure):
        collection.delete_many({"x": {"$typo": 1}})


def test_oversized_incoming_document_is_an_error(started_cluster):
    """`maxBsonObjectSize` of the handshake promises the client that a document of more than
    16 MiB is refused. A message may hold several documents and may be much larger than one of
    them, so the limit of a message does not enforce it: the document itself is checked."""
    # The document is hand built rather than encoded by the driver, which refuses to send one
    # this large in the first place: an int32 length, one string element, the terminator.
    padding = 17 * 1024 * 1024
    element = (
        b"\x02" + b"pad\x00" + struct.pack("<i", padding + 1) + b"a" * padding + b"\x00"
    )
    document = struct.pack("<i", 4 + len(element) + 1) + element + b"\x00"
    payload = struct.pack("<I", 0) + b"\x00" + document

    sock = connect_raw()
    try:
        sock.sendall(struct.pack("<iiii", 16 + len(payload), 1, 0, OP_MSG) + payload)
        # Either an error document or a closed connection is fine; accepting it is not.
        reply = sock.recv(4096)
        if reply:
            assert b"larger than the maximum" in reply
    finally:
        sock.close()

    # The server is still healthy and answers on a new connection.
    assert cluster.instances["node"].query("SELECT 1", password="123").strip() == "1"


def test_large_uint64_round_trips_as_a_decimal(started_cluster):
    """BSON has no unsigned 64-bit integer, so a `UInt64` above the signed maximum cannot come
    back as an `int64`. A `double` would lose its low digits, so it comes back as a decimal128,
    which holds every `UInt64` exactly; the smaller values stay `int64`."""
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query("DROP TABLE IF EXISTS db.uint64_values", password="123")
    node.query(
        "CREATE TABLE db.uint64_values (id UInt8, big UInt64) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    node.query(
        "INSERT INTO db.uint64_values VALUES (1, 42), (2, 18446744073709551615)",
        password="123",
    )

    client = make_client()
    collection = client["db"]["uint64_values"]

    documents = sorted(collection.find({}), key=lambda document: document["id"])
    assert documents[0]["big"] == 42
    assert isinstance(documents[0]["big"], int)
    assert documents[1]["big"] == bson.Decimal128("18446744073709551615")

    node.query("DROP TABLE db.uint64_values", password="123")


def test_update_operator_values_are_data(started_cluster):
    """An update statement carries data, not an aggregation pipeline: a string that starts with
    a dollar sign is stored as it is written rather than read as a field path, and a document
    assigns the fields it names."""
    client = make_client()
    collection = client["db"]["update_values"]

    collection.drop()
    collection.insert_many(
        [{"id": 1, "name": "alpha", "other": "beta", "profile": {"name": "x"}}]
    )

    collection.update_many({"id": 1}, {"$set": {"name": "$other"}})
    assert wait_for(lambda: collection.find_one({"id": 1})["name"] == "$other")

    collection.update_many({"id": 1}, {"$set": {"profile": {"name": "y"}}})
    assert wait_for(lambda: collection.find_one({"id": 1})["profile"] == {"name": "y"})


def test_aggregation_regex_options_are_applied(started_cluster):
    """`$regexMatch` and `$regexFind` take the options of the match as a sibling field of the
    pattern, so a case insensitive match must not be lowered as a case sensitive one."""
    client = make_client()
    collection = client["db"]["regex_options"]

    collection.drop()
    collection.insert_many([{"id": 1, "name": "Abc"}, {"id": 2, "name": "abc"}])

    pipeline = [
        {
            "$project": {
                "id": 1,
                "matched": {
                    "$regexMatch": {"input": "$name", "regex": "^a", "options": "i"}
                },
            }
        },
        {"$sort": {"id": 1}},
    ]
    assert [document["matched"] for document in collection.aggregate(pipeline)] == [
        True,
        True,
    ]
