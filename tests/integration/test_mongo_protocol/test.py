# -*- coding: utf-8 -*-

import datetime
import logging
import os
import socket
import struct
import time
import uuid

import bson
import pymongo
import pytest
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
cluster.add_instance(
    "node",
    main_configs=[
        "configs/mongo.xml",
        "configs/log.xml",
        "configs/users.xml",
        "configs/ssl_conf.xml",
        "../helpers/fake_cert.pem",
    ],
    user_configs=["configs/default_password.xml"],
    env_variables={"UBSAN_OPTIONS": "print_stacktrace=1"},
)

server_port = 27017
secure_server_port = 27018

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


def encode_op_msg(command, request_id=1, flags=0):
    """Encodes a command document as a single OP_MSG frame with one body section."""
    body = b"\x00" + bson.encode(command)
    payload = struct.pack("<I", flags) + body  # flag bits, then the sections
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


def encode_op_msg_with_sequence(command, identifier, documents, request_id=1):
    """Encodes a command as an OP_MSG frame whose write batch is a kind-`1` document sequence
    named `identifier`, which is the shape every driver sends."""
    body = b"\x00" + bson.encode(command)
    encoded = b"".join(bson.encode(document) for document in documents)
    section = identifier.encode() + b"\x00" + encoded
    sequence = b"\x01" + struct.pack("<i", 4 + len(section)) + section
    payload = struct.pack("<I", 0) + body + sequence
    header = struct.pack("<iiii", 16 + len(payload), request_id, 0, OP_MSG)
    return header + payload


def authenticate_raw(sock, user="default", password="123"):
    """The `PLAIN` SASL handshake of a hand built connection: `authzid \0 authcid \0 password`."""
    sock.sendall(
        encode_op_msg(
            {
                "saslStart": 1,
                "mechanism": "PLAIN",
                "payload": bson.Binary(b"\x00" + user.encode() + b"\x00" + password.encode()),
                "$db": "admin",
            }
        )
    )
    reply = read_op_msg(sock)
    assert reply["ok"] == 1, reply
    assert reply["done"] is True, reply


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


def test_created_collection_without_documents_reads_as_empty(started_cluster):
    """Until its first insert, a collection created explicitly has no columns of its documents, so
    a filter naming a field matches nothing instead of failing on an unknown column."""
    client = make_client()
    db = client["db_create"]
    db.drop_collection("placeholder")
    db.create_collection("placeholder")

    collection = db["placeholder"]
    assert list(collection.find({"a": 1})) == []
    assert collection.count_documents({"a": 1}) == 0
    assert collection.distinct("a", {"a": 1}) == []
    assert collection.update_many({}, {"$set": {"a": 1}}).matched_count == 0
    assert collection.delete_many({"a": 1}).deleted_count == 0

    # The placeholder is still there, and the first insert still gives it its columns.
    collection.insert_one({"a": 1})
    assert list(collection.find({"a": 1})) == [{"a": 1}]
    db.drop_collection("placeholder")


def test_empty_documents(started_cluster):
    client = make_client()
    collection = client["db"]["empty_documents"]
    collection.drop()

    # A new collection gets its columns from the first document, and an empty one has none.
    with pytest.raises(pymongo.errors.OperationFailure, match="no fields that map onto columns"):
        collection.insert_one({})

    # An empty nested document has no column to go to, so it is rejected rather than dropped.
    with pytest.raises(pymongo.errors.OperationFailure, match="empty embedded document"):
        collection.insert_one({"id": 1, "profile": {}})

    # In a collection that has its columns, an empty document is a row of default values.
    collection.insert_one({"id": 1, "name": "x"})
    collection.insert_one({})
    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert found == [{"id": 0, "name": ""}, {"id": 1, "name": "x"}]
    collection.drop()


def test_secure_port(started_cluster):
    """The `mongo_port_secure` speaks TLS from the first byte, as a client with `tls=true` expects."""
    node = cluster.instances["node"]
    client = pymongo.MongoClient(
        f"mongodb://default:123@{node.ip_address}:{secure_server_port}/default?authMechanism=PLAIN",
        tls=True,
        tlsAllowInvalidCertificates=True,
    )
    collection = client["db"]["over_tls"]
    collection.drop()
    collection.insert_one({"id": 1, "name": "secure"})
    assert list(collection.find({"id": 1})) == [{"id": 1, "name": "secure"}]
    collection.drop()


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
    # A kept document with an empty array has no element: the field is null rather than the default
    # value of the element type.
    assert [
        (d["id"], d.get("tags"))
        for d in collection.aggregate(
            [{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": True}}, {"$sort": {"id": 1, "tags": 1}}]
        )
    ] == [(1, "green"), (1, "red"), (2, None)]
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

    found = list(collection.find({}))
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
    found = list(events.find({}))
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

    # The whole result is returned in the first batch. Without `singleBatch` a `batchSize` only
    # splits the result into batches, so every document is still returned; with `singleBatch` the
    # cursor is closed after the first batch, so `batchSize` bounds the result.
    def find(**options):
        reply = client["db"].command({"find": "limit_zero", "sort": {"id": 1}, **options})
        assert reply["cursor"]["id"] == 0
        return [doc["id"] for doc in reply["cursor"]["firstBatch"]]

    assert find(batchSize=2) == [1, 2, 3, 4, 5]
    assert find(batchSize=2, singleBatch=True) == [1, 2]
    assert find(batchSize=4, limit=3, singleBatch=True) == [1, 2, 3]
    assert find(batchSize=2, limit=3, singleBatch=True) == [1, 2]
    assert find(singleBatch=True) == [1, 2, 3, 4, 5]
    for options in [{"batchSize": -1}, {"batchSize": 0, "singleBatch": True}, {"singleBatch": "yes"}]:
        with pytest.raises(pymongo.errors.OperationFailure):
            find(**options)


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


def test_oversized_result_fails_while_the_query_runs(started_cluster):
    """The text output of a query is bounded while the query runs, so a result far larger than
    any reply that can be sent fails inside the query rather than being collected whole into
    memory and rejected only while the reply is built."""
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.huge_output (id Int32, s String) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    # 200 rows of 1 MB each: the `FORMAT JSON` output exceeds the 128 MiB bound of the text.
    node.query(
        "INSERT INTO db.huge_output SELECT number, repeat('x', 1000000) FROM numbers(200)",
        password="123",
    )

    client = make_client()
    collection = client["db"]["huge_output"]

    with pytest.raises(pymongo.errors.PyMongoError, match="its text exceeds"):
        list(collection.find({}))

    # The query itself failed, so the limit was hit while it ran.
    node.query("SYSTEM FLUSH LOGS query_log", password="123")
    assert (
        node.query(
            "SELECT count() FROM system.query_log WHERE type = 'ExceptionWhileProcessing' "
            "AND query_id LIKE 'mongo:%' AND query LIKE '%huge_output%' "
            "AND exception LIKE '%its text exceeds%'",
            password="123",
        ).strip()
        != "0"
    )

    node.query("DROP TABLE db.huge_output", password="123")


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


def test_insert_keeps_doubles_and_decimals_exact(started_cluster):
    """A BSON double is a `Float64` column, even when the first value is within the range of a
    `Float32`. A decimal of a larger scale than the one of its column - in the same batch or in
    a later insert - is rejected rather than rounded. A profile that tolerates malformed rows
    does not let an acknowledged insert drop one."""
    from bson.decimal128 import Decimal128

    node = cluster.instances["node"]
    client = make_client()

    doubles = client["db"]["insert_doubles"]
    doubles.drop()
    doubles.insert_one({"id": 1, "x": 1.23456789012345, "xs": [1.5, 1.23456789012345]})
    assert list(doubles.find({}, {"_id": 0})) == [{"id": 1, "x": 1.23456789012345, "xs": [1.5, 1.23456789012345]}]
    assert doubles.count_documents({"x": 1.23456789012345}) == 1

    decimals = client["db"]["insert_decimal_scales"]
    decimals.drop()
    with pytest.raises(pymongo.errors.OperationFailure, match="fractional digits"):
        decimals.insert_many([{"id": 1, "d": Decimal128("1.5")}, {"id": 2, "d": Decimal128("1.25")}])
    decimals.insert_one({"id": 1, "d": Decimal128("1.5")})
    with pytest.raises(pymongo.errors.OperationFailure, match="fractional digits"):
        decimals.insert_one({"id": 2, "d": Decimal128("1.25")})
    decimals.insert_one({"id": 3, "d": Decimal128("2")})
    assert [doc["d"] for doc in sorted(decimals.find({}), key=lambda doc: doc["id"])] == [Decimal128("1.5"), Decimal128("2.0")]

    node.query("DROP USER IF EXISTS mongo_tolerant", password="123")
    node.query(
        "CREATE USER mongo_tolerant IDENTIFIED WITH plaintext_password BY 'mongo_pass' "
        "SETTINGS input_format_allow_errors_num = 10, input_format_allow_errors_ratio = 1",
        password="123",
    )
    node.query("GRANT SELECT, INSERT, SHOW ON db.* TO mongo_tolerant", password="123")
    typed = client["db"]["insert_tolerant"]
    typed.drop()
    typed.insert_one({"a": 0})
    tolerant = make_client(user="mongo_tolerant", password="mongo_pass", database="db")["db"]["insert_tolerant"]
    with pytest.raises(pymongo.errors.PyMongoError):
        tolerant.insert_many([{"a": 1}, {"a": "bad"}, {"a": 3}])
    assert typed.count_documents({}) == 1
    node.query("DROP USER mongo_tolerant", password="123")


def test_rename_of_a_subdocument_replaces_the_target(started_cluster):
    """`$rename` of a subdocument replaces the target field as a whole, so a leaf of the target
    that the source does not have goes back to its default."""
    collection = make_client()["db"]["rename_replaces_target"]
    collection.drop()
    collection.insert_one({"id": 1, "profile": {"name": "alpha"}, "user": {"name": "old", "age": 99}})
    collection.update_many({}, {"$rename": {"profile": "user"}})
    assert wait_for(lambda: collection.find_one({}, {"_id": 0})["user"] == {"name": "alpha", "age": 0})


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
    # The reply carries no `nModified`, which `pymongo` reports as "the server did not say".
    assert result.modified_count is None

    assert collection.delete_many({"x": 1}).deleted_count == 0

    with pytest.raises(pymongo.errors.OperationFailure):
        collection.update_many({"x": 1}, {"$typo": {"y": 1}})
    with pytest.raises(pymongo.errors.OperationFailure):
        collection.delete_many({"x": {"$typo": 1}})


def test_write_command_reply_counts_are_truthful(started_cluster):
    """`update` and `delete` report the number of documents their filter matches, so a client
    that branches on `matched_count` / `deleted_count` takes the same branch it would take
    against MongoDB. `nModified` is not known here, so it is omitted rather than reported as
    zero, and `pymongo` reads a missing one as `None`."""
    client = make_client()
    collection = client["db"]["truthful_write_counts"]

    collection.drop()
    collection.insert_many([{"id": i, "k": i % 2, "v": 0} for i in range(6)])

    result = collection.update_many({"k": 1}, {"$set": {"v": 1}})
    assert result.matched_count == 3
    assert result.modified_count is None
    assert wait_for(lambda: collection.count_documents({"v": 1}) == 3)

    # A filter that matches nothing still reports zero.
    assert collection.update_many({"k": 7}, {"$set": {"v": 2}}).matched_count == 0

    assert collection.delete_many({"k": 1}).deleted_count == 3
    assert wait_for(lambda: collection.count_documents({}) == 3)

    assert collection.delete_many({"k": 7}).deleted_count == 0
    assert collection.delete_many({}).deleted_count == 3


def test_bulk_write_command_reply_counts_are_truthful(started_cluster):
    """A `delete` or an `update` command carries a list of specs that Mongo applies one after
    another, so a spec sees what the previous ones did. The mutation of each spec is therefore
    awaited before the next one is counted: otherwise two overlapping specs would both count the
    same rows and `n` would report more documents than the command touched."""
    client = make_client()
    database = client["db"]
    collection = database["bulk_write_counts"]

    collection.drop()
    collection.insert_many([{"id": i, "k": i % 2, "v": 0} for i in range(6)])

    # The second spec matches nothing any more: the first one has renamed the value of `k`.
    reply = database.command(
        {
            "update": "bulk_write_counts",
            "updates": [
                {"q": {"k": 1}, "u": {"$set": {"k": 2}}, "multi": True},
                {"q": {"k": 1}, "u": {"$set": {"v": 1}}, "multi": True},
            ],
        }
    )
    assert reply["ok"] == 1
    assert reply["n"] == 3

    # Two identical delete specs: the first one removes the rows, the second one finds none.
    reply = database.command(
        {
            "delete": "bulk_write_counts",
            "deletes": [
                {"q": {"k": 2}, "limit": 0},
                {"q": {"k": 2}, "limit": 0},
            ],
        }
    )
    assert reply["ok"] == 1
    assert reply["n"] == 3
    assert collection.count_documents({}) == 3


def test_an_existing_table_is_not_rewritten_as_a_placeholder(started_cluster):
    """`createCollection` leaves behind a placeholder table of a single `json JSON` column that
    the first insert replaces with a column per field. That column layout is also an ordinary
    table somebody may have created in ClickHouse directly, so the rewrite is keyed off the
    marker comment `createCollection` writes, not off the layout: an insert into a table without
    the marker fails instead of retyping it."""
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query("DROP TABLE IF EXISTS db.a_user_table", password="123")
    node.query(
        "CREATE TABLE db.a_user_table (json JSON) ENGINE = MergeTree ORDER BY tuple()",
        password="123",
    )

    collection = make_client()["db"]["a_user_table"]
    with pytest.raises(pymongo.errors.OperationFailure):
        collection.insert_one({"a": 1})

    # The table kept its own schema.
    assert (
        node.query(
            "SELECT name, type FROM system.columns WHERE database = 'db' AND table = 'a_user_table' FORMAT TSV",
            password="123",
        ).strip()
        == "json\tJSON"
    )

    # A collection the Mongo path did create is still given the schema of the first document.
    client_database = make_client()["db"]
    client_database["a_mongo_collection"].drop()
    client_database.create_collection("a_mongo_collection")
    client_database["a_mongo_collection"].insert_one({"a": 1})
    assert (
        node.query(
            "SELECT name, type FROM system.columns WHERE database = 'db' AND table = 'a_mongo_collection' FORMAT TSV",
            password="123",
        ).strip()
        == "a\tInt32"
    )


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


def test_aggregate_of_a_missing_collection_is_empty(started_cluster):
    """`aggregate` honors the same missing-namespace contract as the other read commands: a
    collection that does not exist is read as empty rather than as a missing-table error, and so
    is a missing collection of a `$unionWith`. A malformed pipeline is still an error, and a
    pipeline that unions an existing collection cannot be answered without the aggregated one,
    so it is rejected explicitly."""
    client = make_client()
    collection = client["db"]["never_created_aggregate"]
    collection.drop()

    assert list(collection.aggregate([])) == []
    assert list(collection.aggregate([{"$match": {"id": 1}}, {"$sort": {"id": 1}}])) == []

    with pytest.raises(pymongo.errors.OperationFailure):
        list(collection.aggregate([{"$typo": {}}]))

    other = client["db"]["never_created_aggregate_other"]
    other.drop()
    other.insert_many([{"id": 1}])
    with pytest.raises(pymongo.errors.OperationFailure) as error:
        list(collection.aggregate([{"$unionWith": {"coll": "never_created_aggregate_other"}}]))
    assert "does not exist" in str(error.value)

    # A `$unionWith` of a missing collection contributes nothing, whether or not the aggregated
    # collection exists.
    assert list(collection.aggregate([{"$unionWith": "never_created_aggregate"}])) == []
    assert [
        document["id"]
        for document in other.aggregate(
            [{"$unionWith": {"coll": "never_created_aggregate", "pipeline": [{"$match": {"id": 1}}]}}]
        )
    ] == [1]

    other.drop()


def test_dialect_writes_the_embedded_documents_the_wire_path_creates(started_cluster):
    """An array of embedded documents is inferred as `Array(JSON)` by the wire insert path, and
    the same shape must be writable through `dialect = 'mongo'`: the elements of the array and
    the document `$push` appends become `JSON` values."""
    client = make_client()
    collection = client["db"]["dialect_embedded_documents"]
    collection.drop()

    collection.insert_one({"id": 1, "events": [{"name": "start"}]})

    node = cluster.instances["node"]
    dialect_settings = {"dialect": "mongo", "allow_experimental_mongo_dialect": 1}
    node.query(
        'db.dialect_embedded_documents.insertOne({"id": 2, "events": [{"name": "second"}]})',
        password="123",
        database="db",
        settings=dialect_settings,
    )
    node.query(
        'db.dialect_embedded_documents.updateMany({"id": 1}, {"$push": {"events": {"name": "stop"}}})',
        password="123",
        database="db",
        settings=dict(dialect_settings, mutations_sync=1),
    )

    documents = {document["id"]: document["events"] for document in collection.find({})}
    assert documents == {
        1: [{"name": "start"}, {"name": "stop"}],
        2: [{"name": "second"}],
    }

    collection.drop()


def test_create_index_rejects_unsupported_semantics(started_cluster):
    """`createIndexes` implements one thing: a single-field `bloom_filter` data skipping index.
    An index whose semantics the server cannot honor - `unique`, a compound key, a TTL, a
    special index type - must be an error rather than an acknowledged no-op, or duplicate
    writes would start succeeding after a migration that asked for a unique index."""
    client = make_client()
    collection = client["db"]["index_options"]
    collection.drop()
    collection.insert_one({"email": "a@b.c", "a": 1, "b": 2})

    with pytest.raises(pymongo.errors.OperationFailure) as error:
        collection.create_index("email", unique=True)
    assert "unique" in str(error.value)

    with pytest.raises(pymongo.errors.OperationFailure) as error:
        collection.create_index([("a", 1), ("b", 1)])
    assert "Compound indexes" in str(error.value)

    with pytest.raises(pymongo.errors.OperationFailure) as error:
        collection.create_index([("email", "text")])
    assert "must be 1 or -1" in str(error.value)

    with pytest.raises(pymongo.errors.OperationFailure):
        collection.create_index("email", expireAfterSeconds=3600)

    collection.create_index([("email", -1)])

    collection.drop()


def test_find_sort_rejects_invalid_directions(started_cluster):
    """The direction of a `find` sort must be 1 or -1, as in MongoDB: `0`, another integer, a
    fraction or a string orders nothing and must be a controlled error rather than a silently
    accepted or an unvalidated value."""
    client = make_client()
    database = client["db"]
    collection = database["sort_directions"]
    collection.drop()
    collection.insert_many([{"id": 2}, {"id": 1}])

    for direction in (0, 5, 1.5, "asc"):
        with pytest.raises(pymongo.errors.OperationFailure) as error:
            database.command({"find": "sort_directions", "sort": {"id": direction}})
        assert "must be 1 or -1" in str(error.value)

    ids = [document["id"] for document in collection.find({}).sort("id", -1)]
    assert ids == [2, 1]

    collection.drop()


def test_find_and_count_bounds_must_be_whole_numbers(started_cluster):
    """The `limit` and the `skip` of a wire `find` or `count` must be whole numbers: a BSON
    double such as 1.5 used to be silently truncated to a bound the client never asked for.
    An integral double stays accepted, because the shell sends every number as a double."""
    client = make_client()
    database = client["db"]
    collection = database["whole_bounds"]
    collection.drop()
    collection.insert_many([{"id": i} for i in range(1, 6)])

    for command in (
        {"find": "whole_bounds", "limit": 1.5},
        {"find": "whole_bounds", "skip": 2.5},
        {"count": "whole_bounds", "limit": 1.5},
        {"count": "whole_bounds", "skip": 2.5},
        {"find": "whole_bounds", "limit": "3"},
        {"count": "whole_bounds", "skip": True},
    ):
        with pytest.raises(pymongo.errors.OperationFailure) as error:
            database.command(command)
        assert "must be a whole number" in str(error.value) or "must be a number" in str(error.value)

    assert database.command({"count": "whole_bounds", "limit": 2.0})["n"] == 2
    reply = database.command({"find": "whole_bounds", "limit": 2.0, "skip": 1.0, "sort": {"id": 1}})
    assert [document["id"] for document in reply["cursor"]["firstBatch"]] == [2, 3]

    with pytest.raises(pymongo.errors.OperationFailure) as error:
        database.command({"find": "whole_bounds", "skip": -1})
    assert "must not be negative" in str(error.value)

    collection.drop()


def test_distinct_rejects_a_malformed_query(started_cluster):
    """The `query` of a `distinct` is a filter, so a value that is not a document is a malformed
    request, the way it is for a `find` and a `count`. Reading it as no filter at all would answer
    with the distinct values of the whole collection - a wider result than the one asked for."""
    client = make_client()
    database = client["db"]
    collection = database["distinct_query"]
    collection.drop()
    collection.insert_many([{"k": "a", "v": 1}, {"k": "b", "v": 2}])

    for query in (42, "a", [{"k": "a"}], True):
        with pytest.raises(pymongo.errors.OperationFailure) as error:
            database.command({"distinct": "distinct_query", "key": "k", "query": query})
        assert "must be a document" in str(error.value)

    # An absent and an empty `query` both mean the whole collection.
    assert sorted(database.command({"distinct": "distinct_query", "key": "k"})["values"]) == ["a", "b"]
    assert sorted(database.command({"distinct": "distinct_query", "key": "k", "query": {}})["values"]) == ["a", "b"]
    assert database.command({"distinct": "distinct_query", "key": "k", "query": {"v": 2}})["values"] == ["b"]

    collection.drop()


def test_handshake_local_time_is_a_date(started_cluster):
    """`localTime` of `isMaster`/`hello` is the time of the server, which is a BSON date: an
    integer of the same milliseconds is a different wire type, and a driver or a tool that reads
    the field expects to get a date out of it."""
    client = make_client()
    database = client["db"]

    for command in ("isMaster", "hello"):
        reply = database.command(command)
        local_time = reply["localTime"]
        assert isinstance(local_time, datetime.datetime), f"{command} returned {type(local_time)}"
        # A BSON date carries no zone, and the driver decodes it as a naive UTC value by default.
        if local_time.tzinfo is None:
            local_time = local_time.replace(tzinfo=datetime.timezone.utc)
        # The clocks of the server and of the test runner are the same clock here.
        assert abs((datetime.datetime.now(datetime.timezone.utc) - local_time).total_seconds()) < 600


def test_create_index_needs_the_field_to_be_a_column(started_cluster):
    """An index is a data skipping index over a column, so the field has to be one. In MongoDB an
    index may be created on a collection that does not exist yet - which creates it - and on a
    field no document has; here both have nothing to index, and the error says what to do instead
    rather than being the `UNKNOWN_TABLE`/`UNKNOWN_IDENTIFIER` of the DDL underneath."""
    client = make_client()
    database = client["db"]
    collection = database["index_pre_schema"]
    collection.drop()

    # The collection does not exist at all.
    with pytest.raises(pymongo.errors.OperationFailure) as error:
        collection.create_index("email")
    assert "does not exist" in str(error.value)

    # Created explicitly, it has no document and hence no columns yet - not even one named `json`,
    # the column of the placeholder table, which is not a field of any document.
    database.create_collection("index_pre_schema")
    for field in ["email", "json"]:
        with pytest.raises(pymongo.errors.OperationFailure) as error:
            collection.create_index(field)
        assert "has no documents yet" in str(error.value)

    # A field the documents do have can be indexed.
    collection.insert_one({"email": "a@b.c"})
    collection.create_index("email")

    with pytest.raises(pymongo.errors.OperationFailure) as error:
        collection.create_index("nothing_has_this_field")
    assert "not a column" in str(error.value)

    collection.drop()


def test_write_batches_are_accepted_in_the_command_body(started_cluster):
    """`OP_MSG` allows the write batch of an `insert`, an `update` or a `delete` either as a
    kind-`1` document sequence or as an array of the command body itself. Every driver sends the
    sequence - `pymongo` moves the array out of the command document even for a raw
    `database.command` - so the body shape is exercised over a hand built connection here. It used
    to be rejected with "does not contain any document"."""
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query("DROP TABLE IF EXISTS db.body_batches", password="123")

    sock = connect_raw()
    try:
        authenticate_raw(sock)

        sock.sendall(
            encode_op_msg(
                {
                    "insert": "body_batches",
                    "documents": [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}],
                    "$db": "db",
                }
            )
        )
        reply = read_op_msg(sock)
        assert reply["ok"] == 1, reply
        assert reply["n"] == 2, reply

        sock.sendall(
            encode_op_msg(
                {
                    "update": "body_batches",
                    "updates": [{"q": {"id": 1}, "u": {"$set": {"v": "c"}}, "multi": True}],
                    "$db": "db",
                }
            )
        )
        reply = read_op_msg(sock)
        assert reply["ok"] == 1, reply
        assert reply["n"] == 1, reply

        sock.sendall(
            encode_op_msg(
                {
                    "delete": "body_batches",
                    "deletes": [{"q": {"id": 2}, "limit": 0}],
                    "$db": "db",
                }
            )
        )
        reply = read_op_msg(sock)
        assert reply["ok"] == 1, reply
        assert reply["n"] == 1, reply

        # A document sequence is bound by its identifier and not by its position, so a sequence
        # standing for another field of the command is a controlled error rather than the batch.
        sock.sendall(
            encode_op_msg_with_sequence(
                {"insert": "body_batches", "$db": "db"}, "deletes", [{"id": 3}]
            )
        )
        reply = read_op_msg(sock)
        assert reply["ok"] == 0, reply
        assert "document sequence 'deletes'" in reply["errmsg"], reply
    finally:
        sock.close()

    collection = make_client()["db"]["body_batches"]
    assert sorted(
        (document["id"], document["v"]) for document in collection.find({})
    ) == [(1, "c")]

    node.query("DROP TABLE db.body_batches", password="123")


def test_an_ordered_bulk_write_keeps_the_writes_before_the_error(started_cluster):
    """Mongo applies the specs of a write command in order by default, so a later unsupported spec
    must not undo the writes of the earlier ones: each spec is translated and executed before the
    next one is read. The command still stops at the failing spec."""
    client = make_client()
    database = client["db"]
    collection = database["ordered_bulk_write"]

    collection.drop()
    collection.insert_many([{"id": i, "k": i} for i in range(4)])

    # The second spec deletes a limited number of documents, which is not supported.
    with pytest.raises(pymongo.errors.OperationFailure) as error:
        database.command(
            {
                "delete": "ordered_bulk_write",
                "deletes": [{"q": {"k": 0}, "limit": 0}, {"q": {"k": 1}, "limit": 1}],
            }
        )
    assert "only 'limit: 0'" in str(error.value)

    # The first spec was applied before the second one failed, and nothing after it was.
    assert wait_for(lambda: collection.count_documents({"k": 0}) == 0)
    assert collection.count_documents({"k": 1}) == 1

    # The same for `update`, whose second spec updates a single document.
    with pytest.raises(pymongo.errors.OperationFailure) as error:
        database.command(
            {
                "update": "ordered_bulk_write",
                "updates": [
                    {"q": {"k": 2}, "u": {"$set": {"k": 20}}, "multi": True},
                    {"q": {"k": 3}, "u": {"$set": {"k": 30}}, "multi": False},
                ],
            }
        )
    assert "only 'multi: true'" in str(error.value)

    assert wait_for(lambda: collection.count_documents({"k": 20}) == 1)
    assert collection.count_documents({"k": 3}) == 1

    # Only ordered batches are implemented, so an unordered one is rejected rather than executed
    # with the opposite semantics, and a delete spec without the required `limit` is malformed
    # rather than a `deleteMany`.
    for command in [
        {"delete": "ordered_bulk_write", "deletes": [{"q": {"k": 3}, "limit": 0}], "ordered": False},
        {
            "update": "ordered_bulk_write",
            "updates": [{"q": {"k": 3}, "u": {"$set": {"k": 30}}, "multi": True}],
            "ordered": False,
        },
        {"delete": "ordered_bulk_write", "deletes": [{"q": {"k": 3}}]},
    ]:
        with pytest.raises(pymongo.errors.OperationFailure):
            database.command(command)
    assert collection.count_documents({"k": 3}) == 1

    # A database name that `dropDatabase` would reject is rejected by the collection commands too.
    with pytest.raises(pymongo.errors.OperationFailure) as error:
        client["foo+bar"]["ordered_bulk_write"].insert_one({"id": 1})
    assert "Invalid Mongo database name" in str(error.value)

    collection.drop()


def test_a_dotted_collection_name_is_a_namespace(started_cluster):
    """A `.` is legal in a Mongo collection name, and namespaces such as `fs.files` of GridFS use
    it. The dialect reads a statement as `<database>.<collection>.<operation>`, so the wire path
    passes the collection it took from the command itself instead of letting the text be split at
    the second `.`; before that, every command on `fs.files` addressed a collection `fs`."""
    client = make_client()
    database = client["db"]
    collection = database["fs.files"]

    collection.drop()
    collection.insert_many([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "b"}])

    assert sorted(document["id"] for document in collection.find({})) == [1, 2, 3]
    assert [document["id"] for document in collection.find({"v": "a"})] == [1]
    assert collection.count_documents({"v": "b"}) == 2
    assert sorted(collection.distinct("v")) == ["a", "b"]
    assert [
        document["_id"]
        for document in collection.aggregate(
            [{"$group": {"_id": "$v", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
        )
    ] == ["a", "b"]

    assert collection.update_many({"v": "a"}, {"$set": {"v": "c"}}).matched_count == 1
    assert wait_for(lambda: collection.count_documents({"v": "c"}) == 1)

    assert collection.delete_many({"v": "b"}).deleted_count == 2
    assert wait_for(lambda: collection.count_documents({}) == 1)

    collection.drop()


def test_list_collections_and_databases_honor_filter_and_name_only(started_cluster):
    """A `listCollections` and a `listDatabases` carry a `filter` on the `name` and a `nameOnly`
    flag. The filter narrows the listing; one that is not understood is an error rather than an
    unfiltered listing that reports names the client did not ask for."""
    node = cluster.instances["node"]
    client = make_client()
    database = client["db_listing"]
    for name in ["users", "orders", "user_events"]:
        database[name].drop()
        database[name].insert_one({"id": 1})

    def collections(**options):
        reply = database.command({"listCollections": 1, **options})
        return sorted(reply["cursor"]["firstBatch"], key=lambda entry: entry["name"])

    assert [entry["name"] for entry in collections()] == ["orders", "user_events", "users"]
    assert [entry["name"] for entry in collections(filter={"name": "users"})] == ["users"]
    assert [entry["name"] for entry in collections(filter={"name": {"$eq": "orders"}})] == ["orders"]
    assert [entry["name"] for entry in collections(filter={"name": {"$in": ["orders", "users", "absent"]}})] == ["orders", "users"]
    assert [entry["name"] for entry in collections(filter={"name": {"$regex": "^user"}})] == ["user_events", "users"]
    assert [entry["name"] for entry in collections(filter={"name": {"$regex": "^USER", "$options": "i"}})] == ["user_events", "users"]
    assert [entry["name"] for entry in collections(filter={"name": {"$regularExpression": {"pattern": "^ord", "options": ""}}})] == ["orders"]
    assert collections(filter={"name": "absent"}) == []
    assert collections(filter={}) == collections()

    # `nameOnly` answers with the name and the type alone, like a Mongo server does.
    full = collections(filter={"name": "users"})
    assert set(full[0].keys()) == {"name", "type", "options", "idIndex", "info"}
    assert collections(filter={"name": "users"}, nameOnly=True) == [{"name": "users", "type": "collection"}]

    # A filter on another field, or one with an operator that is not understood, must not widen
    # into the full listing.
    for bad_filter in [
        {"type": "view"},
        {"name": "users", "type": "collection"},
        {"name": {"$ne": "users"}},
        {"name": {"$regex": "^user", "$ne": "users"}},
        {"name": 1},
        "users",
    ]:
        with pytest.raises(pymongo.errors.OperationFailure):
            database.command({"listCollections": 1, "filter": bad_filter})
    with pytest.raises(pymongo.errors.OperationFailure):
        database.command({"listCollections": 1, "nameOnly": "yes"})

    def databases(**options):
        reply = client.admin.command({"listDatabases": 1, **options})
        return sorted(reply["databases"], key=lambda entry: entry["name"])

    assert "db_listing" in [entry["name"] for entry in databases()]
    assert [entry["name"] for entry in databases(filter={"name": "db_listing"})] == ["db_listing"]
    assert [entry["name"] for entry in databases(filter={"name": {"$in": ["db_listing", "absent"]}})] == ["db_listing"]
    assert [entry["name"] for entry in databases(filter={"name": {"$regex": "^db_list"}})] == ["db_listing"]
    assert databases(filter={"name": "absent"}) == []
    assert databases(filter={"name": "db_listing"}) == [{"name": "db_listing", "empty": False}]
    node.query("CREATE DATABASE IF NOT EXISTS db_listing_empty", password="123")
    assert databases(filter={"name": "db_listing_empty"}) == [{"name": "db_listing_empty", "empty": True}]
    node.query("DROP DATABASE db_listing_empty", password="123")
    assert databases(filter={"name": "db_listing"}, nameOnly=True) == [{"name": "db_listing"}]
    assert "db_listing" in client.list_database_names()

    # A ClickHouse database whose name is not a legal Mongo database name cannot be used by any
    # other command, so it is not listed.
    node.query("CREATE DATABASE IF NOT EXISTS `db+listing`", password="123")
    assert "db+listing" not in client.list_database_names()
    node.query("DROP DATABASE `db+listing`", password="123")

    for bad_filter in [{"sizeOnDisk": 0}, {"name": {"$ne": "db_listing"}}, {"name": {"$in": [1]}}]:
        with pytest.raises(pymongo.errors.OperationFailure):
            client.admin.command({"listDatabases": 1, "filter": bad_filter})

    database.command("dropDatabase")


def test_unsupported_command_options_are_rejected(started_cluster):
    """Options that change the result of a command - a `collation`, the `arrayFilters` of an update,
    the `validator` of a collection - are not implemented, so a command carrying one is an error
    rather than being answered as if the option were absent. A `collation` with the `simple` locale
    asks for the binary comparison the translation does, and is accepted."""
    client = make_client()
    database = client["db_options"]
    collection = database["t"]

    collection.drop()
    collection.insert_many([{"id": 1, "name": "foo"}, {"id": 2, "name": "FOO"}])

    case_insensitive = {"locale": "en", "strength": 2}
    for command in [
        {"find": "t", "filter": {"name": "foo"}, "collation": case_insensitive},
        {"count": "t", "query": {"name": "foo"}, "collation": case_insensitive},
        {"distinct": "t", "key": "name", "collation": case_insensitive},
        {"aggregate": "t", "pipeline": [{"$match": {"name": "foo"}}], "cursor": {}, "collation": case_insensitive},
        {"delete": "t", "deletes": [{"q": {"name": "foo"}, "limit": 0, "collation": case_insensitive}]},
        {
            "update": "t",
            "updates": [{"q": {"name": "foo"}, "u": {"$set": {"id": 3}}, "multi": True, "collation": case_insensitive}],
        },
        {
            "update": "t",
            "updates": [{"q": {"name": "foo"}, "u": {"$set": {"id": 3}}, "multi": True, "arrayFilters": [{"x": 1}]}],
        },
        {"find": "t", "filter": {"name": "foo"}, "let": {"x": 1}},
    ]:
        with pytest.raises(pymongo.errors.OperationFailure) as error:
            database.command(command)
        assert "is not supported" in str(error.value)

    assert [doc["id"] for doc in collection.find({"name": "foo"}, collation={"locale": "simple"})] == [1]
    assert collection.count_documents({}) == 2

    # A malformed `upsert` is an error of its own rather than being read as an absent one.
    for upsert in [1, "false"]:
        with pytest.raises(pymongo.errors.OperationFailure) as error:
            database.command(
                {"update": "t", "updates": [{"q": {"id": 1}, "u": {"$set": {"name": "bar"}}, "multi": True, "upsert": upsert}]}
            )
        assert "must be a boolean" in str(error.value)
    assert collection.count_documents({"name": "foo"}) == 1

    # The options of `create` that ask for a collection with semantics of its own are rejected.
    for options in [{"validator": {"age": {"$gte": 0}}}, {"capped": True, "size": 4096}, {"collation": case_insensitive}]:
        with pytest.raises(pymongo.errors.OperationFailure) as error:
            database.command({"create": "with_options", **options})
        assert "is not supported" in str(error.value)
    assert "with_options" not in database.list_collection_names()

    database.command("dropDatabase")


def test_unsupported_concerns_are_rejected(started_cluster):
    """The server is a single one, which applies a write before it replies. A `writeConcern` that
    waits for other members or for a flush to disk, and a `readConcern` with stronger guarantees
    than reading the committed data, cannot be honoured, so a command carrying one is an error,
    whether it reads or writes. The concerns a single server honours are accepted."""
    client = make_client()
    database = client["db_concerns"]
    collection = database["t"]

    collection.drop()
    collection.insert_many([{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}])

    for write_concern in [{"w": 2}, {"w": "tagged"}, {"j": True}, {"fsync": True}, {"w": 1, "x": 1}]:
        for command in [
            {"insert": "t", "documents": [{"id": 3, "name": "baz"}], "writeConcern": write_concern},
            {"delete": "t", "deletes": [{"q": {"id": 1}, "limit": 0}], "writeConcern": write_concern},
            {"update": "t", "updates": [{"q": {"id": 1}, "u": {"$set": {"name": "qux"}}, "multi": True}], "writeConcern": write_concern},
            {"create": "other", "writeConcern": write_concern},
        ]:
            with pytest.raises(pymongo.errors.OperationFailure) as error:
                database.command(command)
            assert "is not supported" in str(error.value) or "supports only" in str(error.value)

    for read_concern in [{"level": "linearizable"}, {"level": "snapshot"}, {"afterClusterTime": bson.Timestamp(1, 1)}]:
        for command in [
            {"find": "t", "filter": {}, "readConcern": read_concern},
            {"count": "t", "query": {}, "readConcern": read_concern},
            {"distinct": "t", "key": "name", "readConcern": read_concern},
            {"aggregate": "t", "pipeline": [], "cursor": {}, "readConcern": read_concern},
        ]:
            with pytest.raises(pymongo.errors.OperationFailure) as error:
                database.command(command)
            assert "is not supported" in str(error.value) or "supports only" in str(error.value)

    assert sorted(doc["id"] for doc in collection.find()) == [1, 2]
    assert "other" not in database.list_collection_names()

    for write_concern in [WriteConcern(w=1), WriteConcern(w="majority", wtimeout=1000), WriteConcern(w=1, j=False)]:
        collection.with_options(write_concern=write_concern).insert_one({"id": 3, "name": "baz"})
        collection.with_options(write_concern=write_concern).delete_many({"id": 3})
    for level in ["local", "available", "majority"]:
        assert collection.with_options(read_concern=ReadConcern(level)).count_documents({}) == 2

    database.command("dropDatabase")


def test_insert_batch_is_all_or_nothing(started_cluster):
    """An `insert` writes its batch as one block: a document that does not fit fails the whole
    command before anything is written, so a retry of the batch does not duplicate a prefix of it.
    An unordered batch asks for the opposite semantics and is rejected."""
    client = make_client()
    collection = client["db"]["insert_all_or_nothing"]

    collection.drop()
    collection.insert_one({"id": 0, "name": "first"})

    with pytest.raises(pymongo.errors.PyMongoError):
        collection.insert_many([{"id": 1, "name": "a"}, {"id": 2, "unknown_field": "b"}, {"id": 3, "name": "c"}])
    assert [doc["id"] for doc in collection.find({})] == [0]

    with pytest.raises(pymongo.errors.PyMongoError):
        collection.insert_many([{"id": 4, "name": "d"}], ordered=False)
    assert [doc["id"] for doc in collection.find({})] == [0]

    collection.drop()


def test_more_to_come_gets_no_reply(started_cluster):
    """A message with the `moreToCome` flag is executed but not answered: the client does not wait
    for a reply and sends the next message right away, so a reply would be read as the answer to
    that next message."""
    client = make_client()
    collection = client["db"]["more_to_come"]
    collection.drop()
    collection.insert_one({"id": 0})

    more_to_come = 1 << 1
    sock = connect_raw()
    try:
        authenticate_raw(sock)
        sock.sendall(
            encode_op_msg(
                {"insert": "more_to_come", "documents": [{"id": 1}], "$db": "db"}, request_id=1, flags=more_to_come
            )
            + encode_op_msg({"find": "more_to_come", "filter": {"id": 1}, "$db": "db"}, request_id=2)
        )
        reply = read_op_msg(sock)
        assert reply["ok"] == 1.0, reply
        assert [doc["id"] for doc in reply["cursor"]["firstBatch"]] == [1]
    finally:
        sock.close()

    collection.drop()


def test_unset_and_rename_of_a_subdocument(started_cluster):
    """A nested document is stored as one column per leaf, so `$unset` and `$rename` of the parent
    path apply to all of its leaves, the same way `$set` of a subdocument does."""
    client = make_client()
    collection = client["db"]["subdocument_unset_rename"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "profile": {"name": "alpha", "age": 30}, "user": {"name": "", "age": 0}},
            {"id": 2, "profile": {"name": "beta", "age": 40}, "user": {"name": "", "age": 0}},
        ]
    )

    collection.update_many({"id": 1}, {"$rename": {"profile": "user"}})
    assert wait_for(lambda: collection.find_one({"id": 1})["user"] == {"name": "alpha", "age": 30})
    assert collection.find_one({"id": 1})["profile"] == {"name": "", "age": 0}

    collection.update_many({"id": 2}, {"$unset": {"profile": ""}})
    assert wait_for(lambda: collection.find_one({"id": 2})["profile"] == {"name": "", "age": 0})

    collection.drop()


def test_every_field_is_updatable(started_cluster):
    """No field is a sorting key, so the first field of the first document can be updated like any
    other. A field that is not a column of the collection is rejected by name, since an update does
    not add columns."""
    client = make_client()
    collection = client["db"]["every_field_updatable"]

    collection.drop()
    collection.insert_many([{"name": "alpha", "visits": 0}, {"name": "gamma", "visits": 0}])

    collection.update_many({"name": "alpha"}, {"$set": {"name": "beta"}})
    assert wait_for(lambda: sorted(doc["name"] for doc in collection.find({})) == ["beta", "gamma"])

    with pytest.raises(pymongo.errors.OperationFailure, match="new_field"):
        collection.update_many({"name": "beta"}, {"$set": {"new_field": 1}})
    with pytest.raises(pymongo.errors.OperationFailure, match="new_visits"):
        collection.update_many({"name": "beta"}, {"$rename": {"visits": "new_visits"}})

    collection.drop()


def test_collection_name_with_parentheses(started_cluster):
    """The text that a command is translated through does not hold the collection name, so a name
    with `(`, `)` or `;` in it is addressed like any other."""
    client = make_client()
    collection = client["db"]["sales(2026);x"]

    collection.drop()
    collection.insert_many([{"id": 1, "amount": 10}, {"id": 2, "amount": 20}])

    assert [doc["id"] for doc in collection.find({"amount": {"$gt": 15}})] == [2]
    assert collection.count_documents({}) == 2
    assert sorted(collection.distinct("id")) == [1, 2]
    assert [doc["id"] for doc in collection.aggregate([{"$match": {"id": 1}}])] == [1]

    collection.update_many({"id": 1}, {"$inc": {"amount": 1}})
    assert wait_for(lambda: collection.find_one({"id": 1})["amount"] == 11)

    collection.delete_many({"id": 2})
    assert wait_for(lambda: collection.count_documents({}) == 1)

    collection.drop()


def test_aggregate_cursor_must_be_well_formed(started_cluster):
    """The whole result of an `aggregate` is returned in its first batch, so `cursor.batchSize` only
    says how it is split, but a malformed `cursor` is still an error."""
    client = make_client()
    database = client["db"]
    collection = database["aggregate_cursor"]
    collection.drop()
    collection.insert_many([{"id": 1}, {"id": 2}])

    reply = database.command({"aggregate": "aggregate_cursor", "pipeline": [{"$sort": {"id": 1}}], "cursor": {"batchSize": 1}})
    assert [doc["id"] for doc in reply["cursor"]["firstBatch"]] == [1, 2]

    for cursor in [{"batchSize": -1}, {"batchSize": 1.5}, 1]:
        with pytest.raises(pymongo.errors.OperationFailure):
            database.command({"aggregate": "aggregate_cursor", "pipeline": [], "cursor": cursor})

    collection.drop()


def test_read_commands_apply_max_time_ms(started_cluster):
    """The `maxTimeMS` of a read command becomes the `max_execution_time` of the query that answers
    it, rather than being dropped, and `allowDiskUse: false`, which ClickHouse cannot promise, is
    rejected."""
    node = cluster.instances["node"]
    client = make_client()
    database = client["db"]
    collection = database["max_time_ms"]
    collection.drop()
    collection.insert_many([{"id": 1, "tags": ["a"]}, {"id": 2, "tags": ["b"]}])

    assert len(database.command({"find": "max_time_ms", "maxTimeMS": 4321})["cursor"]["firstBatch"]) == 2
    assert database.command({"count": "max_time_ms", "maxTimeMS": 4321})["n"] == 2
    assert sorted(database.command({"distinct": "max_time_ms", "key": "id", "maxTimeMS": 4321})["values"]) == [1, 2]
    reply = database.command({"aggregate": "max_time_ms", "pipeline": [], "cursor": {}, "maxTimeMS": 4321})
    assert len(reply["cursor"]["firstBatch"]) == 2

    node.query("SYSTEM FLUSH LOGS query_log", password="123")
    timeouts = node.query(
        "SELECT DISTINCT Settings['max_execution_time'] FROM system.query_log "
        "WHERE type = 'QueryFinish' AND query LIKE '%max_time_ms%' AND query_kind = 'Select' AND has(tables, 'db.max_time_ms')",
        password="123",
    ).split()
    assert timeouts == ["4.321"]

    with pytest.raises(pymongo.errors.OperationFailure):
        database.command({"count": "max_time_ms", "maxTimeMS": -1})

    assert len(list(collection.aggregate([], allowDiskUse=True))) == 2
    with pytest.raises(pymongo.errors.OperationFailure, match="allowDiskUse"):
        list(collection.aggregate([], allowDiskUse=False))
    with pytest.raises(pymongo.errors.OperationFailure, match="allowDiskUse"):
        database.command({"find": "max_time_ms", "sort": {"id": 1}, "allowDiskUse": False})

    # A write command runs as an awaited mutation, which `max_execution_time` does not bound.
    with pytest.raises(pymongo.errors.OperationFailure, match="maxTimeMS"):
        database.command({"delete": "max_time_ms", "deletes": [{"q": {"id": 1}, "limit": 0}], "maxTimeMS": 1})
    with pytest.raises(pymongo.errors.OperationFailure, match="maxTimeMS"):
        database.command(
            {"update": "max_time_ms", "updates": [{"q": {"id": 1}, "u": {"$set": {"id": 3}}}], "maxTimeMS": 1}
        )
    assert collection.count_documents({}) == 2
    # So is the DDL of `create`.
    with pytest.raises(pymongo.errors.OperationFailure, match="maxTimeMS"):
        database.command({"create": "max_time_ms_created", "maxTimeMS": 1})
    assert "max_time_ms_created" not in database.list_collection_names()

    # A non-boolean `preserveNullAndEmptyArrays` is an error rather than an inner unwind.
    with pytest.raises(pymongo.errors.OperationFailure, match="preserveNullAndEmptyArrays"):
        list(collection.aggregate([{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": 1}}]))

    # The text a command is translated through is bounded by the `max_query_size` of the session,
    # not by a limit of its own.
    assert collection.count_documents({"id": {"$in": list(range(5000))}}) == 2

    collection.drop()


def test_insert_into_an_existing_collection_needs_no_ddl_grants(started_cluster):
    """An `insert` into a collection that already exists issues no `CREATE` or `ALTER`, so the
    `INSERT` grant is enough for it."""
    node = cluster.instances["node"]
    node.query("DROP USER IF EXISTS mongo_writer", password="123")
    node.query("CREATE USER mongo_writer IDENTIFIED WITH plaintext_password BY 'mongo_pass'", password="123")

    owner = make_client()
    collection = owner["db"]["insert_grants"]
    collection.drop()
    collection.insert_many([{"id": 1}])

    node.query("GRANT SELECT, INSERT ON db.insert_grants TO mongo_writer", password="123")
    writer = make_client(user="mongo_writer", password="mongo_pass", database="db")
    writer["db"]["insert_grants"].insert_many([{"id": 2}])
    assert sorted(doc["id"] for doc in collection.find({})) == [1, 2]

    collection.drop()
    node.query("DROP USER mongo_writer", password="123")


def test_parser_limits_come_from_the_user_profile(started_cluster):
    """The text a command is translated through is bounded by the `max_query_size` of the
    authenticated user, not by the one of the server."""
    node = cluster.instances["node"]
    node.query("DROP USER IF EXISTS mongo_small_queries", password="123")
    node.query(
        "CREATE USER mongo_small_queries IDENTIFIED WITH plaintext_password BY 'mongo_pass' SETTINGS max_query_size = 1000",
        password="123",
    )
    node.query("GRANT SELECT, INSERT, CREATE TABLE, CREATE DATABASE ON db.* TO mongo_small_queries", password="123")

    collection = make_client()["db"]["profile_limits"]
    collection.drop()
    collection.insert_many([{"id": 1}, {"id": 2}])

    small = make_client(user="mongo_small_queries", password="mongo_pass", database="db")["db"]["profile_limits"]
    assert small.count_documents({"id": 1}) == 1
    with pytest.raises(pymongo.errors.OperationFailure, match="[Mm]ax query size"):
        small.count_documents({"id": {"$in": list(range(1000))}})
    assert collection.count_documents({"id": {"$in": list(range(1000))}}) == 2

    collection.drop()
    node.query("DROP USER mongo_small_queries", password="123")


def test_list_collections_needs_a_database_grant_and_reports_views(started_cluster):
    """`listCollections` needs a grant on the whole database, unless it asks only for the names of
    the collections the user has a grant on. A view is listed as a read-only view. `listDatabases`
    lists only the databases the user has a grant on, unless the user has the global grant."""
    node = cluster.instances["node"]
    database = make_client()["db_list_grants"]
    database["visible"].drop()
    database["hidden"].drop()
    database["visible"].insert_one({"id": 1})
    database["hidden"].insert_one({"id": 1})
    node.query("CREATE OR REPLACE VIEW db_list_grants.visible_view AS SELECT * FROM db_list_grants.visible", password="123")

    entries = {entry["name"]: entry for entry in database.command({"listCollections": 1})["cursor"]["firstBatch"]}
    assert entries["visible"]["type"] == "collection"
    assert entries["visible"]["info"]["readOnly"] is False
    assert entries["visible_view"]["type"] == "view"
    assert entries["visible_view"]["info"]["readOnly"] is True

    node.query("DROP USER IF EXISTS mongo_lister", password="123")
    node.query("CREATE USER mongo_lister IDENTIFIED WITH plaintext_password BY 'mongo_pass'", password="123")
    node.query("GRANT SELECT ON db_list_grants.visible TO mongo_lister", password="123")
    lister = make_client(user="mongo_lister", password="mongo_pass", database="db_list_grants")["db_list_grants"]

    for options in [{}, {"nameOnly": True}, {"authorizedCollections": True}, {"authorizedCollections": False, "nameOnly": True}]:
        with pytest.raises(pymongo.errors.OperationFailure, match="Not authorized"):
            lister.command({"listCollections": 1, **options})
    reply = lister.command({"listCollections": 1, "authorizedCollections": True, "nameOnly": True})
    assert reply["cursor"]["firstBatch"] == [{"name": "visible", "type": "collection"}]

    # Without the global `SHOW DATABASES` grant `listDatabases` lists only the databases the user has
    # a grant on, and the full listing of `authorizedDatabases: false` is refused.
    node.query("CREATE DATABASE IF NOT EXISTS db_list_hidden", password="123")
    admin = make_client(user="mongo_lister", password="mongo_pass", database="admin")["admin"]
    for options in [{}, {"authorizedDatabases": True}]:
        names = [entry["name"] for entry in admin.command({"listDatabases": 1, **options})["databases"]]
        assert "db_list_grants" in names
        assert "db_list_hidden" not in names
    with pytest.raises(pymongo.errors.OperationFailure, match="Not authorized"):
        admin.command({"listDatabases": 1, "authorizedDatabases": False})
    node.query("GRANT SHOW DATABASES ON *.* TO mongo_lister", password="123")
    names = [entry["name"] for entry in admin.command({"listDatabases": 1, "authorizedDatabases": False})["databases"]]
    assert "db_list_hidden" in names
    node.query("DROP DATABASE db_list_hidden", password="123")

    node.query("DROP USER mongo_lister", password="123")
    node.query("DROP DATABASE db_list_grants", password="123")


def test_parent_path_of_a_subdocument_over_the_wire(started_cluster):
    """A subdocument is stored as one column per leaf, so a projection of its path keeps every
    leaf, and `$set` of it replaces the whole subdocument, resetting the leaves it does not name."""
    collection = make_client()["db"]["parent_path"]
    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "profile": {"name": "alpha", "age": 30}, "profiles": 5},
            {"id": 2, "profile": {"name": "beta", "age": 40}, "profiles": 6},
        ]
    )

    assert list(collection.find({"id": 1}, {"profile": 1})) == [{"profile": {"name": "alpha", "age": 30}}]
    assert list(collection.find({"id": 1}, {"profile": 0, "_id": 0})) == [{"id": 1, "profiles": 5}]

    collection.update_many({"id": 1}, {"$set": {"profile": {"name": "gamma"}}})
    assert wait_for(lambda: collection.find_one({"id": 1}, {"profile": 1}) == {"profile": {"name": "gamma", "age": 0}})
    assert collection.find_one({"id": 2}, {"profile": 1}) == {"profile": {"name": "beta", "age": 40}}

    collection.drop()


def test_insert_is_all_or_nothing_with_small_block_settings(started_cluster):
    """The block thresholds of the profile do not split the batch of an `insert` into blocks that
    are written one by one: with one-row blocks a valid prefix would be written before a later
    malformed document fails the command."""
    node = cluster.instances["node"]
    node.query("DROP USER IF EXISTS mongo_small_blocks", password="123")
    node.query(
        "CREATE USER mongo_small_blocks IDENTIFIED WITH plaintext_password BY 'mongo_pass' "
        "SETTINGS min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0, input_format_max_block_size_bytes = 1",
        password="123",
    )
    node.query("GRANT SELECT, INSERT, SHOW ON db.* TO mongo_small_blocks", password="123")

    collection = make_client()["db"]["insert_small_blocks"]
    collection.drop()
    collection.insert_one({"id": 0, "name": "first"})

    small = make_client(user="mongo_small_blocks", password="mongo_pass", database="db")["db"]["insert_small_blocks"]
    with pytest.raises(pymongo.errors.PyMongoError):
        small.insert_many([{"id": i, "name": "valid"} for i in range(1, 1001)] + [{"id": 1001, "unknown_field": "bad"}])
    assert collection.count_documents({}) == 1

    small.insert_many([{"id": i, "name": "valid"} for i in range(1, 1001)])
    assert collection.count_documents({}) == 1001

    collection.drop()
    node.query("DROP USER mongo_small_blocks", password="123")


def test_internal_queries_ignore_the_format_overrides_of_the_profile(started_cluster):
    """The `input_format`, `output_format`, `format` and `default_format` of the profile do not
    change the formats of the queries the handlers generate and parse back: a document is stored as
    its fields rather than as its text, and the replies are built from the output they expect."""
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query("DROP TABLE IF EXISTS db.format_overrides", password="123")
    node.query("CREATE TABLE db.format_overrides (v String) ENGINE = MergeTree ORDER BY tuple()", password="123")

    for user, settings in [
        ("mongo_input_format", "input_format = 'JSONAsString', output_format = 'CSV', default_format = 'Vertical'"),
        ("mongo_format", "format = 'JSONAsString'"),
    ]:
        node.query(f"DROP USER IF EXISTS {user}", password="123")
        node.query(
            f"CREATE USER {user} IDENTIFIED WITH plaintext_password BY 'mongo_pass' SETTINGS {settings}",
            password="123",
        )
        node.query(f"GRANT SELECT, INSERT, SHOW ON db.* TO {user}", password="123")

        node.query("TRUNCATE TABLE db.format_overrides", password="123")
        database = make_client(user=user, password="mongo_pass", database="db")["db"]
        collection = database["format_overrides"]
        collection.insert_one({"v": "x"})
        assert node.query("SELECT v FROM db.format_overrides", password="123") == "x\n"

        assert list(collection.find({}, {"_id": 0})) == [{"v": "x"}]
        assert collection.count_documents({"v": "x"}) == 1
        assert collection.distinct("v") == ["x"]
        assert "format_overrides" in database.list_collection_names()

        node.query(f"DROP USER {user}", password="123")

    node.query("DROP TABLE db.format_overrides", password="123")


def test_insert_rejects_integers_out_of_the_range_of_the_column(started_cluster):
    """A small first integer makes an `Int32` column, and a later integer it cannot hold - in the
    same batch, in a later insert or as an element of an array - is rejected rather than stored
    wrapped around."""
    client = make_client()

    scalars = client["db"]["insert_integer_ranges"]
    scalars.drop()
    with pytest.raises(pymongo.errors.OperationFailure, match="out of the range"):
        scalars.insert_many([{"x": 1}, {"x": 2147483648}])
    assert scalars.count_documents({}) == 0

    scalars.insert_one({"x": 1})
    for value in [2147483648, -2147483649]:
        with pytest.raises(pymongo.errors.OperationFailure, match="out of the range"):
            scalars.insert_one({"x": value})
    scalars.insert_one({"x": -2147483648})
    assert sorted(doc["x"] for doc in scalars.find({})) == [-2147483648, 1]

    arrays = client["db"]["insert_integer_array_ranges"]
    arrays.drop()
    arrays.insert_one({"xs": [1, 2]})
    with pytest.raises(pymongo.errors.OperationFailure, match="out of the range"):
        arrays.insert_one({"xs": [3, 2147483648]})
    assert [doc["xs"] for doc in arrays.find({})] == [[1, 2]]

    scalars.drop()
    arrays.drop()


def test_transaction_envelopes_are_rejected(started_cluster):
    """A command that is a part of a multi-document transaction is rejected before it changes
    anything, while a retryable write, which carries `lsid` and `txnNumber` alone, is executed."""
    collection = make_client()["db_transactions"]["t"]
    collection.drop()
    collection.insert_one({"id": 1})

    session = {"id": bson.Binary(uuid.uuid4().bytes, 4)}
    sock = connect_raw()
    try:
        authenticate_raw(sock)
        for command in [
            {
                "insert": "t",
                "documents": [{"id": 2}],
                "lsid": session,
                "txnNumber": bson.Int64(1),
                "startTransaction": True,
                "autocommit": False,
            },
            {"delete": "t", "deletes": [{"q": {"id": 1}, "limit": 0}], "lsid": session, "txnNumber": bson.Int64(1), "autocommit": False},
            {
                "update": "t",
                "updates": [{"q": {"id": 1}, "u": {"$set": {"id": 3}}, "multi": True}],
                "lsid": session,
                "txnNumber": bson.Int64(1),
                "autocommit": False,
            },
            {"find": "t", "filter": {}, "lsid": session, "txnNumber": bson.Int64(1), "autocommit": False},
        ]:
            sock.sendall(encode_op_msg({**command, "$db": "db_transactions"}))
            reply = read_op_msg(sock)
            assert reply["ok"] == 0, reply
            assert "transactions are not supported" in reply["errmsg"], reply

        sock.sendall(
            encode_op_msg(
                {"insert": "t", "documents": [{"id": 4}], "lsid": session, "txnNumber": bson.Int64(2), "$db": "db_transactions"}
            )
        )
        reply = read_op_msg(sock)
        assert reply["ok"] == 1, reply
        assert reply["n"] == 1, reply
    finally:
        sock.close()

    assert sorted(doc["id"] for doc in collection.find({})) == [1, 4]
    collection.database.command("dropDatabase")


def test_dates_keep_their_instant(started_cluster):
    """A date is encoded from its instant rather than from its local time: the two instants of an
    hour repeated by the end of the daylight saving time stay apart, and a column without an
    explicit time zone reads the same under a profile with a `session_timezone`."""
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query("DROP TABLE IF EXISTS db.date_instants", password="123")
    node.query(
        "CREATE TABLE db.date_instants (id Int32, t DateTime('America/New_York'), t64 DateTime64(3, 'America/New_York'), "
        "i DateTime, i64 DateTime64(3)) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    # 1636263000 and 1636266600 are both 01:30 in New York on 2021-11-07.
    node.query(
        "INSERT INTO db.date_instants SELECT number + 1, 1636263000 + number * 3600 AS s, toDateTime64(s, 3), s, toDateTime64(s, 3) "
        "FROM numbers(2)",
        password="123",
    )

    node.query("DROP USER IF EXISTS mongo_tokyo", password="123")
    node.query(
        "CREATE USER mongo_tokyo IDENTIFIED WITH plaintext_password BY 'mongo_pass' SETTINGS session_timezone = 'Asia/Tokyo'",
        password="123",
    )
    node.query("GRANT SELECT ON db.date_instants TO mongo_tokyo", password="123")

    expected = [
        datetime.datetime(2021, 11, 7, 5, 30),
        datetime.datetime(2021, 11, 7, 6, 30),
    ]
    for client in [make_client(), make_client(user="mongo_tokyo", password="mongo_pass", database="db")]:
        documents = sorted(client["db"]["date_instants"].find({}, {"_id": 0}), key=lambda doc: doc["id"])
        assert [{key: doc[key] for key in ["t", "t64", "i", "i64"]} for doc in documents] == [
            {"t": instant, "t64": instant, "i": instant, "i64": instant} for instant in expected
        ]

    node.query("DROP USER mongo_tokyo", password="123")
    node.query("DROP TABLE db.date_instants", password="123")


def test_max_time_ms_is_subject_to_the_constraints_of_the_profile(started_cluster):
    """`maxTimeMS` cannot loosen the `max_execution_time` of the profile, and it cannot set a value
    the constraints of the profile forbid."""
    node = cluster.instances["node"]
    collection = make_client()["db"]["max_time_constrained"]
    collection.drop()
    collection.insert_many([{"id": 1}, {"id": 2}])

    node.query("DROP USER IF EXISTS mongo_time_limited", password="123")
    node.query(
        "CREATE USER mongo_time_limited IDENTIFIED WITH plaintext_password BY 'mongo_pass' "
        "SETTINGS max_execution_time = 10 MIN 5 MAX 10",
        password="123",
    )
    node.query("GRANT SELECT ON db.max_time_constrained TO mongo_time_limited", password="123")

    limited = make_client(user="mongo_time_limited", password="mongo_pass", database="db")["db"]
    assert limited.command({"count": "max_time_constrained", "maxTimeMS": 60000})["n"] == 2
    assert limited.command({"count": "max_time_constrained", "maxTimeMS": 6000})["n"] == 2
    with pytest.raises(pymongo.errors.OperationFailure, match="max_execution_time"):
        limited.command({"count": "max_time_constrained", "maxTimeMS": 1000})

    node.query("SYSTEM FLUSH LOGS query_log", password="123")
    timeouts = node.query(
        "SELECT DISTINCT Settings['max_execution_time'] FROM system.query_log "
        "WHERE type = 'QueryFinish' AND user = 'mongo_time_limited' AND query_kind = 'Select' "
        "AND has(tables, 'db.max_time_constrained') ORDER BY 1",
        password="123",
    ).split()
    assert timeouts == ["10", "6"]

    node.query("DROP USER mongo_time_limited", password="123")
    collection.drop()


def test_generated_queries_ignore_the_dialect_of_the_profile(started_cluster):
    """The statements the handlers generate are ClickHouse SQL, whatever `dialect` the profile of
    the authenticated user selects."""
    node = cluster.instances["node"]
    collection = make_client()["db"]["dialect_profile"]
    collection.drop()
    collection.insert_many([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

    node.query("DROP USER IF EXISTS mongo_dialect_user", password="123")
    node.query(
        "CREATE USER mongo_dialect_user IDENTIFIED WITH plaintext_password BY 'mongo_pass' "
        "SETTINGS dialect = 'mongo', allow_experimental_mongo_dialect = 1",
        password="123",
    )
    node.query("GRANT SELECT, INSERT ON db.dialect_profile TO mongo_dialect_user", password="123")

    dialect_collection = make_client(user="mongo_dialect_user", password="mongo_pass", database="db")["db"]["dialect_profile"]
    assert sorted(doc["id"] for doc in dialect_collection.find({"id": {"$gt": 0}})) == [1, 2]
    assert dialect_collection.count_documents({"name": "a"}) == 1
    dialect_collection.insert_one({"id": 3, "name": "c"})
    assert sorted(doc["id"] for doc in collection.find({})) == [1, 2, 3]

    node.query("DROP USER mongo_dialect_user", password="123")
    collection.drop()


def test_authentication_initializes_the_session(started_cluster):
    """A connection gets a session of its own once it authenticates, which enforces the
    `max_sessions_for_user` of the user. A connection whose authentication failed runs no
    command, and authenticating again on the same connection replaces its session."""
    node = cluster.instances["node"]
    node.query("DROP USER IF EXISTS mongo_single_session", password="123")
    node.query("DROP SETTINGS PROFILE IF EXISTS mongo_single_session_profile", password="123")
    # `max_sessions_for_user` can only be set by a settings profile.
    node.query(
        "CREATE SETTINGS PROFILE mongo_single_session_profile SETTINGS max_sessions_for_user = 1",
        password="123",
    )
    node.query(
        "CREATE USER mongo_single_session IDENTIFIED WITH plaintext_password BY 'mongo_pass' "
        "SETTINGS PROFILE 'mongo_single_session_profile'",
        password="123",
    )
    node.query("GRANT SELECT ON *.* TO mongo_single_session", password="123")

    find = {"find": "single_session", "filter": {}, "$db": "db"}
    first = connect_raw()
    second = connect_raw()
    try:
        authenticate_raw(first, user="mongo_single_session", password="mongo_pass")
        authenticate_raw(first, user="mongo_single_session", password="mongo_pass")

        second.sendall(
            encode_op_msg(
                {
                    "saslStart": 1,
                    "mechanism": "PLAIN",
                    "payload": bson.Binary(b"\x00mongo_single_session\x00mongo_pass"),
                    "$db": "admin",
                }
            )
        )
        reply = read_op_msg(second)
        assert reply["ok"] == 0, reply
        assert "overflown session count" in reply["errmsg"], reply

        second.sendall(encode_op_msg(find, request_id=2))
        reply = read_op_msg(second)
        assert reply["ok"] == 0, reply
        assert "requires authentication" in reply["errmsg"], reply

        first.sendall(encode_op_msg(find, request_id=3))
        assert read_op_msg(first)["ok"] == 1
    finally:
        first.close()
        second.close()
        node.query("DROP USER mongo_single_session", password="123")
        node.query("DROP SETTINGS PROFILE mongo_single_session_profile", password="123")


def test_deeply_dotted_column_is_rejected(started_cluster):
    """Every dot of a column name is a level of the nested reply document, so a name with more
    dots than the nesting MongoDB allows is rejected instead of being built recursively."""
    node = cluster.instances["node"]
    column = "a." * 150 + "b"
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        f"CREATE OR REPLACE TABLE db.deeply_dotted (`{column}` Int32) ENGINE = Memory", password="123"
    )
    node.query("INSERT INTO db.deeply_dotted VALUES (1)", password="123")

    client = make_client(database="db")
    with pytest.raises(pymongo.errors.OperationFailure, match="nested deeper than"):
        list(client["db"]["deeply_dotted"].find({}))
    assert client["admin"].command("ping")["ok"] == 1.0

    node.query("DROP TABLE db.deeply_dotted", password="123")
