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
    node.query("GRANT ALL ON *.* TO mongo_user", password="123")

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
