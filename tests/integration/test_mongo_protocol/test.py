# -*- coding: utf-8 -*-

import datetime
import logging
import os
import socket
import struct
import time

import bson
import pymongo
import pytest

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


def without_ids(documents):
    """The documents without their object ids. A collection of documents returns the `_id` of every
    document it reads, as MongoDB does; a test that is about the fields the document holds compares
    them without an id nobody chose."""
    return [
        {name: value for name, value in document.items() if name != "_id"}
        for document in documents
    ]


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

    assert without_ids(find_docs) == [
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
        {"name": "David Williams", "age": 40, "city": "Chicago"},
    ]

    # Every document is read back with the object id the insert gave it.
    assert all(isinstance(doc["_id"], str) and doc["_id"] for doc in find_docs)

    find_docs = [doc for doc in collection.find({}).limit(2)]
    assert len(find_docs) == 2

    find_docs = [doc for doc in collection.find({"age": 24})]
    assert without_ids(find_docs) == [
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
    assert without_ids(find_docs) == [
        {"name": "David Williams", "age": 40, "city": "Chicago"},
        {"name": "Charlie Brown", "age": 24, "city": "Los Angeles"},
        {"name": "Bob Johnson", "age": 32, "city": "New York"},
    ]


def test_index(started_cluster):
    """An index is created on a column, and a field of a collection of documents is a path of the
    document rather than a column, so an index over one is an error - and the reads it would have
    sped up answer the same without it."""
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

    with pytest.raises(pymongo.errors.OperationFailure) as error:
        collection.create_index("age")
    assert "not a column of the collection" in str(error.value)

    find_docs = [doc for doc in collection.find({})]
    find_docs = sorted(find_docs, key=lambda x: x["age"])

    assert without_ids(find_docs) == [
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
    assert without_ids(collection.find({})) == [{"id": 1}]

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

    assert without_ids(first.find({})) == [{"id": 1, "value": "first"}]
    assert without_ids(second.find({})) == [{"id": 2, "value": "second"}]

    assert first.estimated_document_count() == 1
    assert second.estimated_document_count() == 1

    # Dropping one of them must leave the other one alone.
    first.drop()
    assert second.estimated_document_count() == 1
    assert without_ids(second.find({})) == [{"id": 2, "value": "second"}]


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
    assert without_ids(found) == [
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
    assert without_ids(found) == [
        {"id": 1, "tags": ["a", "b", "c"]},
        {"id": 2, "tags": []},
    ]


def test_insert_heterogeneous_documents(started_cluster):
    """A collection keeps whole documents, so there is no schema for a later document to contradict:
    a document may leave out a field another one has, and may hold a field no document before it
    had."""
    client = make_client()
    collection = client["db"]["heterogeneous"]

    collection.drop()
    # The second document has no `b`, and it stays a document without one.
    collection.insert_many([{"a": 1, "b": 2}, {"a": 3}])

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["a"])
    assert without_ids(found) == [{"a": 1, "b": 2}, {"a": 3}]

    # A field no document had before is written as well.
    collection.insert_many([{"a": 4, "new_field": 5}])

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["a"])
    assert without_ids(found) == [
        {"a": 1, "b": 2},
        {"a": 3},
        {"a": 4, "new_field": 5},
    ]

    # A field a document does not have holds no value, which is what `$exists` answers.
    found = sorted((doc for doc in collection.find({"b": {"$exists": True}})), key=lambda x: x["a"])
    assert without_ids(found) == [{"a": 1, "b": 2}]


def test_create_collection(started_cluster):
    client = make_client()
    db = client["db_create"]
    db.drop_collection("explicit")

    db.create_collection("explicit")
    assert "explicit" in db.list_collection_names()

    # A collection created explicitly keeps whole documents, exactly like one created by the first
    # insert into it, so it starts out empty and holds whatever the documents that arrive say.
    assert [doc for doc in db["explicit"].find({})] == []

    db["explicit"].insert_many([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
    found = sorted((doc for doc in db["explicit"].find({})), key=lambda x: x["a"])
    assert without_ids(found) == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

    assert without_ids(db["explicit"].find({"a": 2})) == [{"a": 2, "b": "y"}]

    db.drop_collection("explicit")
    assert "explicit" not in db.list_collection_names()


def test_create_collection_rejects_unsupported_options(started_cluster):
    """A plain `MergeTree` collection must not acknowledge Mongo collection semantics it cannot provide."""
    client = make_client()
    db = client["db_create"]
    db.drop_collection("capped")

    with pytest.raises(pymongo.errors.OperationFailure, match="not supported"):
        db.command("create", "capped", capped=True)
    assert "capped" not in db.list_collection_names()


def test_schemaful_json_id_shape_is_not_document_collection(started_cluster):
    """A user table with the document collection's columns stays a schemaful table."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["schemaful_json_id"]

    collection.drop()
    node.query(
        "CREATE TABLE db.schemaful_json_id (_id String, json JSON) ENGINE = MergeTree ORDER BY _id",
        password="123",
    )
    node.query(
        'INSERT INTO db.schemaful_json_id FORMAT JSONEachRow\n{"_id":"external", "json":{"field":1}}',
        password="123",
    )

    assert list(collection.find({})) == [{"_id": "external", "json": {"field": 1}}]

    collection.drop()


def test_ordered_insert_keeps_the_successful_prefix(started_cluster):
    """A later bad document in an ordered insert reports a write error without rolling back the prefix."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["ordered_insert_prefix"]

    collection.drop()
    node.query("CREATE TABLE db.ordered_insert_prefix (id Int64) ENGINE = MergeTree ORDER BY id", password="123")

    with pytest.raises(pymongo.errors.BulkWriteError):
        collection.insert_many([{"id": 1}, {"id": "not an integer"}])
    assert list(collection.find({}, {"_id": 0})) == [{"id": 1}]

    collection.drop()


def test_increment_update(started_cluster):
    """An update of a collection of documents rewrites the document rather than the columns of a
    row, which is not supported yet: it is refused with an error, and the documents are left as
    they were. The same update of a table created in ClickHouse works - `test_update_operators`
    covers it."""
    client = make_client()
    collection = client["db"]["increment"]

    collection.drop()
    collection.insert_many([{"id": 1, "counter": 10}, {"id": 2, "counter": 20}])

    with pytest.raises(pymongo.errors.OperationFailure) as error:
        collection.update_many({"id": 1}, {"$inc": {"counter": 5}})
    assert "is not supported yet" in str(error.value)

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert without_ids(found) == [{"id": 1, "counter": 10}, {"id": 2, "counter": 20}]


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

    collection.bulk_write([pymongo.DeleteMany({"id": 1}), pymongo.DeleteMany({"id": 2})])

    # A mutation is asynchronous, so the reply carries no count of its own -
    # `test_mutation_replies_report_unknown_counts` is about that. What every spec did is what
    # the collection holds afterwards.
    assert wait_for(lambda: collection.estimated_document_count() == 1)
    assert [document["id"] for document in collection.find({})] == [3]


def test_mutation_replies_report_unknown_counts(started_cluster):
    """An asynchronous ClickHouse mutation cannot report its exact affected-row count when it
    is acknowledged."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["mutation_reply_counts"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.mutation_reply_counts (id Int64, value Int64) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    collection.insert_many([{"id": 1, "value": 1}, {"id": 2, "value": 2}])

    result = collection.update_many({"id": {"$gte": 1}}, {"$inc": {"value": 1}})
    assert result.matched_count == 0
    assert result.modified_count == 0
    assert wait_for(lambda: collection.find_one({"id": 1})["value"] == 2)

    result = collection.delete_many({"id": {"$gte": 1}})
    assert result.deleted_count == 0
    assert wait_for(lambda: collection.estimated_document_count() == 0)


def test_ordered_bulk_mutation_reports_the_successful_prefix(started_cluster):
    """A failure in a later bulk spec leaves the preceding mutation acknowledged rather than
    turning the whole command into a retry-unsafe error."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["ordered_bulk_mutation_prefix"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.ordered_bulk_mutation_prefix (id Int64, value Int64) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    collection.insert_many([{"id": 1, "value": 1}, {"id": 2, "value": 2}])

    with pytest.raises(pymongo.errors.BulkWriteError) as error:
        collection.bulk_write(
            [
                pymongo.UpdateMany({"id": 1}, {"$inc": {"value": 1}}),
                pymongo.UpdateMany({"id": 2}, {"$set": {"unknown_column": 1}}),
            ]
        )
    # The count of a mutation is not known when it is acknowledged, so what says that the first
    # spec ran is the value it wrote.
    assert error.value.details["writeErrors"][0]["index"] == 1
    assert wait_for(lambda: collection.find_one({"id": 1})["value"] == 2)

    with pytest.raises(pymongo.errors.BulkWriteError) as error:
        collection.bulk_write(
            [
                pymongo.DeleteMany({"id": 1}),
                pymongo.DeleteMany({"unknown_column": 1}),
            ]
        )
    assert error.value.details["writeErrors"][0]["index"] == 1
    assert wait_for(lambda: collection.estimated_document_count() == 1)


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
    assert without_ids(found) == [{"id": 1, "counter": 10}, {"id": 2, "counter": 20}]


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
    already filled by an earlier one have to continue on top of a subquery.

    The accumulators read a number, and a path of a stored document is a `Dynamic` value whose type
    is a property of the row - `test_accumulators_over_a_document_collection_are_an_error` covers
    what such a collection answers - so the table here is one created in ClickHouse."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["aggregate"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.aggregate (id Int64, city String, score Int64) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
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
    asks to keep it. It walks an array, which a path of a stored document is only for some of its
    rows, so the table is one created in ClickHouse - a collection of documents answers the error
    `test_unwind_of_a_document_collection_is_an_error` pins."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["unwound"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.unwound (id Int64, tags Array(String)) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
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
    ] == [(1, "green"), (1, "red"), (2, None)]
    assert [
        d["position"]
        for d in collection.aggregate(
            [{"$unwind": {"path": "$tags", "includeArrayIndex": "position"}}, {"$sort": {"position": 1}}]
        )
    ] == [0, 1]
    # A document kept although its array held nothing has no element and no position of one.
    assert [
        (d["id"], d["tags"], d["position"])
        for d in collection.aggregate(
            [
                {
                    "$unwind": {
                        "path": "$tags",
                        "preserveNullAndEmptyArrays": True,
                        "includeArrayIndex": "position",
                    }
                },
                {"$sort": {"id": 1, "tags": 1}},
            ]
        )
    ] == [(1, "green", 1), (1, "red", 0), (2, None, None)]

    assert list(collection.aggregate([{"$unwind": "$tags"}, {"$sortByCount": "$tags"}])) == [
        {"_id": "green", "count": 1},
        {"_id": "red", "count": 1},
    ]
    assert list(collection.aggregate([{"$match": {"id": 1}}, {"$unset": "tags"}])) == [{"id": 1}]
    assert list(collection.aggregate([{"$sample": {"size": 1}}, {"$count": "c"}])) == [{"c": 1}]


def test_update_operators(started_cluster):
    """The update operators all become the assignments of one `ALTER TABLE ... UPDATE`, so a
    statement that both sets and increments is a single mutation. An update writes columns, so it
    is a table created in ClickHouse that it is tested over - `test_increment_update` covers what a
    collection of documents answers instead."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["updated"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.updated (id Int64, name String, age Int64, tags Array(String)) "
        "ENGINE = MergeTree ORDER BY id",
        password="123",
    )
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
    either as a subdocument or as a dotted path. An update writes columns, so the table is one
    created in ClickHouse, whose nested field is the column `profile.name`."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["nested_update"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.nested_update (id Int64, `profile.name` String, flag Int64) "
        "ENGINE = MergeTree ORDER BY id",
        password="123",
    )
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

    assert without_ids(collection.find({})) == [{"id": 1, "big": 2147483648}]
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
    assert without_ids(found) == [
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
    assert without_ids(found) == documents

    # An empty projection asks for the whole document.
    found = sorted((doc for doc in collection.find({}, {})), key=lambda x: x["id"])
    assert without_ids(found) == documents

    # A projection of a nested field keeps its document shape.
    assert [doc for doc in collection.find({"id": 1}, {"profile.name": 1})] == [{"profile": {"name": "alpha"}}]

    # A value a projection or a pipeline computes is returned with the type its JSON carries: the
    # documents a `find` reads as they are stored are the ones that keep their dates.
    assert [doc for doc in collection.aggregate([{"$match": {"id": 2}}, {"$project": {"when": 1}}])] == [
        {"when": "2022-07-02 13:30:00.000000000"}
    ]

    assert collection.distinct("when") == [
        "2021-06-01 12:00:00.000000000",
        "2022-07-02 13:30:00.000000000",
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

    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["decimals"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.decimals (id Int64, price Decimal128(10)) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    collection.insert_many(
        [
            {"id": 1, "price": Decimal128("12345678901234567890.1234567890")},
            {"id": 2, "price": Decimal128("-0.0000000001")},
        ]
    )

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert [type(doc["price"]) for doc in found] == [Decimal128, Decimal128]
    assert without_ids(found) == [
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

    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["wrapper_arrays"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.wrapper_arrays (id Int64, dates Array(DateTime64(3, 'UTC')), "
        "ids Array(String), prices Array(Decimal128(10))) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
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
    """An explicit `null` is a real Mongo value, not an omitted field: over a table created in
    ClickHouse, whose column holds it, it survives the round trip rather than being erased.

    A collection of documents keeps the documents in a `JSON` column, which has no path that holds
    nothing but a `null`, so there the field is read back as one the document does not have - the
    documented limitation this also pins."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["nulls"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.nulls (id Int64, note Dynamic) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    collection.insert_many([{"id": 1, "note": None}, {"id": 2, "note": "set"}])

    found = sorted((doc for doc in collection.find({})), key=lambda x: x["id"])
    assert without_ids(found) == [{"id": 1, "note": None}, {"id": 2, "note": "set"}]

    only_null = client["db"]["only_null"]
    only_null.drop()
    only_null.insert_many([{"id": 1, "note": None}])
    assert without_ids(only_null.find({})) == [{"id": 1}]


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

    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["current_date"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.current_date (id Int64, seen DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
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
    """A `$numberDecimal` is stored with every digit it has: `0.00000000001` has eleven fractional
    digits, and a value far from the scales a fixed column would offer keeps them all.

    A collection of documents has no decimal among the types the `JSON` type infers, so the digits
    read back as the text of the number rather than as a `decimal128` - the documented limitation
    this also pins. What matters is that they are all still there."""
    from decimal import Decimal

    from bson.decimal128 import Decimal128

    client = make_client()

    eleven = client["db"]["decimal_scale_eleven"]
    eleven.drop()
    eleven.insert_one({"id": 1, "d": Decimal128("0.00000000001")})
    found = list(eleven.find({}))
    assert len(found) == 1 and Decimal(found[0]["d"]) == Decimal("0.00000000001")

    twenty = client["db"]["decimal_scale_twenty"]
    twenty.drop()
    twenty.insert_one({"id": 1, "d": Decimal128("1E-20")})
    found = list(twenty.find({}))
    assert len(found) == 1 and Decimal(found[0]["d"]) == Decimal("1E-20")

    arrays = client["db"]["decimal_scale_arrays"]
    arrays.drop()
    arrays.insert_one({"id": 1, "prices": [Decimal128("1.5"), Decimal128("1.25")]})
    found = list(arrays.find({}))
    assert len(found) == 1
    assert [Decimal(value) for value in found[0]["prices"]] == [Decimal("1.5"), Decimal("1.25")]


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
    length is not a match.

    The elements of an array are compared, which a collection of documents cannot express - see
    `test_element_wise_match_over_a_document_collection_compares_the_value` - so the table is one
    created in ClickHouse."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["tagged"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.tagged (id Int64, name String, tags Array(String)) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
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
    and `$ne` is the negation. The table is one created in ClickHouse, whose column is an array -
    see `test_element_wise_match_over_a_document_collection_compares_the_value`."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["equality_on_arrays"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.equality_on_arrays (id Int64, name String, tags Array(String)) "
        "ENGINE = MergeTree ORDER BY id",
        password="123",
    )
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
    documents are returned once. The values come back in ascending order. The table is one created
    in ClickHouse, whose column is an array."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["distinct_on_arrays"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.distinct_on_arrays (id Int64, name String, tags Array(String)) "
        "ENGINE = MergeTree ORDER BY id",
        password="123",
    )
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
    assigns the fields it names. An update writes columns, so the table is one created in
    ClickHouse - `test_increment_update` covers what a collection of documents answers."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["update_values"]

    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.update_values (id Int64, name String, other String, `profile.name` String) "
        "ENGINE = MergeTree ORDER BY id",
        password="123",
    )
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
    collection that does not exist is read as empty rather than as a missing-table error. A
    malformed pipeline is still an error, and a pipeline that unions another collection cannot
    be answered without the aggregated one, so it is rejected explicitly."""
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

    other.drop()


def test_dialect_writes_the_embedded_documents_the_wire_path_creates(started_cluster):
    """An array of embedded documents is inferred as `Array(JSON)` by the wire insert path, and
    the same shape must be writable through `dialect = 'mongo'`: the elements of the array and
    the document `$push` appends become `JSON` values.

    The dialect addresses the columns of a table, so the collection is one created in ClickHouse -
    `test_dialect_over_a_document_collection_is_an_error` covers what it answers over a collection
    of documents."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["dialect_embedded_documents"]
    collection.drop()

    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.dialect_embedded_documents (id Int64, events Array(JSON)) "
        "ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    collection.insert_one({"id": 1, "events": [{"name": "start"}]})

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
    writes would start succeeding after a migration that asked for a unique index.

    An index is created on a column, and a field of a collection of documents is a path rather than
    one, so the table is one created in ClickHouse."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["index_options"]
    collection.drop()
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.index_options (email String, a Int64, b Int64) ENGINE = MergeTree ORDER BY email",
        password="123",
    )
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

    # A collection of documents has no column to index, whichever field is asked for: the
    # documents it holds live in one `JSON` column, and a field of them is a path of it.
    database.create_collection("index_pre_schema")
    with pytest.raises(pymongo.errors.OperationFailure) as error:
        collection.create_index("email")
    assert "not a column" in str(error.value)

    collection.insert_one({"email": "a@b.c"})
    with pytest.raises(pymongo.errors.OperationFailure) as error:
        collection.create_index("email")
    assert "not a column" in str(error.value)

    collection.drop()

    # A table created in ClickHouse has columns, and a field that is one can be indexed.
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query(
        "CREATE TABLE db.index_pre_schema (email String) ENGINE = MergeTree ORDER BY email",
        password="123",
    )
    collection.insert_one({"email": "a@b.c"})
    collection.create_index("email")

    with pytest.raises(pymongo.errors.OperationFailure) as error:
        collection.create_index("nothing_has_this_field")
    assert "not a column" in str(error.value)

    collection.drop()


def test_accumulators_over_a_document_collection_are_an_error(started_cluster):
    """An accumulator reads a number, and a path of a stored document is a `Dynamic` value whose
    type is a property of the row rather than of the column, so `$sum` over one is a controlled
    error rather than a wrong number. Counting the documents does not read a path and works."""
    client = make_client()
    collection = client["db"]["document_accumulators"]

    collection.drop()
    collection.insert_many([{"id": 1, "score": 10}, {"id": 2, "score": 20}])

    with pytest.raises(pymongo.errors.OperationFailure):
        list(collection.aggregate([{"$group": {"_id": None, "s": {"$sum": "$score"}}}]))

    assert list(collection.aggregate([{"$group": {"_id": None, "c": {"$sum": 1}}}])) == [
        {"_id": None, "c": 2}
    ]

    # The server is still healthy after the rejected pipeline.
    assert sorted(document["id"] for document in collection.find({})) == [1, 2]

    collection.drop()


def test_unwind_of_a_document_collection_is_an_error(started_cluster):
    """`$unwind` walks an array, which a path of a stored document is only for some of its rows,
    so it is a controlled error over a collection of documents."""
    client = make_client()
    collection = client["db"]["document_unwind"]

    collection.drop()
    collection.insert_many([{"id": 1, "tags": ["red", "green"]}])

    with pytest.raises(pymongo.errors.OperationFailure):
        list(collection.aggregate([{"$unwind": "$tags"}]))

    assert [document["id"] for document in collection.find({})] == [1]

    collection.drop()


def test_element_wise_match_over_a_document_collection_compares_the_value(started_cluster):
    """Equality and `$in` over a collection of documents compare the value a field holds rather
    than the elements of an array it holds: the array functions ClickHouse would need for that do
    not accept the `Dynamic` value a path of a document is. A scalar field is unaffected; over a
    field that holds an array there is no such comparison, so it is an error."""
    client = make_client()
    collection = client["db"]["document_arrays"]

    collection.drop()
    collection.insert_many(
        [
            {"id": 1, "name": "alpha", "tags": ["red", "blue"]},
            {"id": 2, "name": "beta", "tags": ["green"]},
        ]
    )

    # A scalar field keeps plain equality, and `$in` compares the candidates one by one.
    assert [document["id"] for document in collection.find({"name": "beta"})] == [2]
    assert [document["id"] for document in collection.find({"name": {"$in": ["alpha"]}})] == [1]
    assert sorted(
        document["id"] for document in collection.find({"name": {"$in": ["alpha", "beta"]}})
    ) == [1, 2]
    assert [document["id"] for document in collection.find({"name": {"$nin": ["alpha"]}})] == [2]

    # An array field is compared as the value it holds, and comparing an array with a scalar has no
    # answer, so it is an error rather than a match of the elements MongoDB would report.
    with pytest.raises(pymongo.errors.OperationFailure):
        list(collection.find({"tags": "green"}))
    with pytest.raises(pymongo.errors.OperationFailure):
        list(collection.find({"tags": {"$in": ["green"]}}))

    collection.drop()


def test_dialect_over_a_document_collection_is_an_error(started_cluster):
    """The `mongo` dialect addresses the columns of a table, and a collection of documents has one
    column for all of them, so a statement of the dialect over such a collection is an error rather
    than a query about a column that is not there. The wire protocol is the way to read one."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["document_dialect"]

    collection.drop()
    collection.insert_many([{"id": 1, "name": "alpha"}])

    with pytest.raises(Exception):
        node.query(
            'db.document_dialect.find({"id": 1})',
            password="123",
            database="db",
            settings={"dialect": "mongo", "allow_experimental_mongo_dialect": 1},
        )

    assert [document["id"] for document in collection.find({})] == [1]

    collection.drop()


def test_a_column_named_like_the_document_alias_is_not_a_document(started_cluster):
    """The reply of a read of a collection of whole documents is built out of the document of each
    row, and what says that a result is of that shape is the rewrite of the query, not the name of
    a column: a table of ClickHouse that has a column named like the internal alias keeps its own
    columns."""
    node = cluster.instances["node"]
    client = make_client()
    collection = client["db"]["alias_named_column"]

    collection.drop()
    node.query(
        "CREATE TABLE db.alias_named_column (id Int64, __mongo_document String) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    node.query(
        "INSERT INTO db.alias_named_column VALUES (1, 'not a document')",
        password="123",
    )

    assert list(collection.find({})) == [{"id": 1, "__mongo_document": "not a document"}]

    collection.drop()


def test_an_object_id_of_a_failed_document_stays_free(started_cluster):
    """An object id addresses a document that exists, so an unordered insert whose document fails
    leaves its id free: a later document of the same batch may use it."""
    client = make_client()
    collection = client["db"]["free_object_id"]

    collection.drop()
    # The first document fails to convert - `$bad` is no operator of a stored document - and the
    # third one, which is valid, takes the very id the failed one named.
    with pytest.raises(pymongo.errors.BulkWriteError):
        collection.insert_many(
            [
                {"_id": "x", "$bad": 1},
                {"_id": "y", "value": "second"},
                {"_id": "x", "value": "third"},
            ],
            ordered=False,
        )

    assert sorted(
        (document["_id"], document["value"]) for document in collection.find({})
    ) == [("x", "third"), ("y", "second")]

    collection.drop()


def test_a_write_concern_that_cannot_be_honoured_is_an_error(started_cluster):
    """A write goes to one table and is acknowledged when it is written. A write concern that asks
    for more than that must be an error rather than an `ok` for a weaker write than the client
    asked for."""
    collection = make_client()["db"]["write_concern"]
    collection.drop()

    # What the endpoint does satisfy is accepted.
    collection.with_options(
        write_concern=WriteConcern(w=1)
    ).insert_one({"id": 1})
    collection.with_options(
        write_concern=WriteConcern(w="majority")
    ).insert_one({"id": 2})

    with pytest.raises(pymongo.errors.PyMongoError):
        collection.with_options(
            write_concern=WriteConcern(w=2)
        ).insert_one({"id": 3})
    with pytest.raises(pymongo.errors.PyMongoError):
        collection.with_options(
            write_concern=WriteConcern(j=True)
        ).insert_one({"id": 4})

    assert sorted(document["id"] for document in collection.find({})) == [1, 2]

    collection.drop()


def test_an_update_option_that_is_not_implemented_is_an_error(started_cluster):
    """`arrayFilters` chooses which elements of an array a positional update writes and a
    `collation` changes which documents a filter matches. Neither is translated, so a command that
    asks for one is refused rather than answered with `ok` for a different write."""
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query("DROP TABLE IF EXISTS db.update_options SYNC", password="123")
    node.query(
        "CREATE TABLE db.update_options (id Int64, value Int64) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    node.query("INSERT INTO db.update_options VALUES (1, 1)", password="123")

    database = make_client()["db"]

    with pytest.raises(pymongo.errors.OperationFailure):
        database.command(
            {
                "update": "update_options",
                "updates": [
                    {
                        "q": {"id": 1},
                        "u": {"$set": {"value": 2}},
                        "multi": True,
                        "collation": {"locale": "en"},
                    }
                ],
            }
        )
    with pytest.raises(pymongo.errors.OperationFailure):
        database.command(
            {
                "update": "update_options",
                "updates": [
                    {
                        "q": {"id": 1},
                        "u": {"$set": {"value": 2}},
                        "multi": True,
                        "arrayFilters": [{"element.value": 1}],
                    }
                ],
            }
        )
    with pytest.raises(pymongo.errors.OperationFailure):
        database.command(
            {
                "delete": "update_options",
                "deletes": [{"q": {"id": 1}, "limit": 0, "collation": {"locale": "en"}}],
            }
        )

    # Nothing of the refused commands was written.
    assert node.query("SELECT value FROM db.update_options", password="123").strip() == "1"

    node.query("DROP TABLE db.update_options SYNC", password="123")


def test_date_to_string_milliseconds(started_cluster):
    """Mongo's `%L` is the milliseconds of a date, zero-padded to three digits. ClickHouse has no
    format token for it - `%f` is always the six digits of the microseconds - so it is formatted
    apart from the rest of the format."""
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query("DROP TABLE IF EXISTS db.date_format SYNC", password="123")
    node.query(
        "CREATE TABLE db.date_format (id Int64, at DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY id",
        password="123",
    )
    node.query(
        "INSERT INTO db.date_format VALUES (1, '2026-08-20 12:34:56.789')",
        password="123",
    )

    collection = make_client()["db"]["date_format"]
    result = list(
        collection.aggregate(
            [
                {
                    "$project": {
                        "text": {
                            "$dateToString": {
                                "date": "$at",
                                "format": "%H:%M:%S.%L",
                                "timezone": "UTC",
                            }
                        }
                    }
                }
            ]
        )
    )
    assert without_ids(result) == [{"text": "12:34:56.789"}]

    node.query("DROP TABLE db.date_format SYNC", password="123")


def test_an_unacknowledged_write_concern_is_an_error(started_cluster):
    """`w: 0` asks for a write the client is not told about: it does not wait for a reply and no
    write error is reported. Every write of this endpoint runs the statement and answers with its
    result, so the weaker contract is refused rather than answered as the stronger one."""
    database = make_client()["db"]
    database.drop_collection("unacknowledged")

    with pytest.raises(pymongo.errors.OperationFailure):
        database.command(
            {
                "insert": "unacknowledged",
                "documents": [{"id": 1}],
                "writeConcern": {"w": 0},
            }
        )

    assert "unacknowledged" not in database.list_collection_names()


def test_a_write_concern_of_a_drop_or_an_index_is_checked(started_cluster):
    """`drop`, `dropDatabase` and `createIndexes` write as much as an `insert` does, so a write
    concern this server cannot honour has to be an error for them too, rather than an `ok` for a
    weaker write than the one asked for."""
    database = make_client()["db"]
    collection = database["write_concern_of_a_drop"]
    collection.drop()
    collection.insert_one({"id": 1})

    with pytest.raises(pymongo.errors.OperationFailure):
        database.command({"drop": "write_concern_of_a_drop", "writeConcern": {"w": 2}})
    with pytest.raises(pymongo.errors.OperationFailure):
        database.command(
            {
                "createIndexes": "write_concern_of_a_drop",
                "indexes": [{"key": {"id": 1}, "name": "id_1"}],
                "writeConcern": {"j": True},
            }
        )
    with pytest.raises(pymongo.errors.OperationFailure):
        make_client()["db_of_a_dropped_database"].command(
            {"dropDatabase": 1, "writeConcern": {"w": 0}}
        )

    # The refused commands changed nothing.
    assert [document["id"] for document in collection.find({})] == [1]

    # What the endpoint does satisfy is still accepted.
    database.command({"drop": "write_concern_of_a_drop", "writeConcern": {"w": 1}})
    assert "write_concern_of_a_drop" not in database.list_collection_names()


def test_a_read_command_option_that_is_not_implemented_is_an_error(started_cluster):
    """A `collation` decides which documents a filter matches and which values are the same one,
    and a `batchSize` bounds how many documents a batch carries while everything is answered in the
    first one. Neither is translated, so a read command that asks for one is refused rather than
    answered as a query that does not ask for it."""
    database = make_client()["db"]
    collection = database["read_options"]
    collection.drop()
    collection.insert_many([{"id": 1, "value": "a"}, {"id": 2, "value": "b"}])

    for command in [
        {"find": "read_options", "filter": {}, "collation": {"locale": "en"}},
        {"find": "read_options", "filter": {}, "batchSize": 1},
        {"count": "read_options", "query": {}, "collation": {"locale": "en"}},
        {"distinct": "read_options", "key": "value", "collation": {"locale": "en"}},
        {
            "aggregate": "read_options",
            "pipeline": [],
            "cursor": {},
            "collation": {"locale": "en"},
        },
        {"aggregate": "read_options", "pipeline": [], "cursor": {"batchSize": 1}},
        {"find": "read_options", "filter": {}, "maxTimeMS": 1000},
    ]:
        with pytest.raises(pymongo.errors.OperationFailure):
            database.command(command)

    # The same commands without the option they cannot honour are answered.
    assert database.command({"count": "read_options", "query": {}})["n"] == 2
    assert sorted(
        document["id"]
        for document in database.command({"find": "read_options", "filter": {}})[
            "cursor"
        ]["firstBatch"]
    ) == [1, 2]
    assert sorted(
        database.command({"distinct": "read_options", "key": "value"})["values"]
    ) == ["a", "b"]

    collection.drop()


def test_read_concern_levels(started_cluster):
    """A read here sees everything that was written to the table, which is what `local`, `available`
    and `majority` ask for, so those are answered as a read without one. A `snapshot` and a
    `linearizable` read ask for a guarantee across the whole cluster that nothing gives here, and
    are refused rather than answered by a read of a different contract."""
    database = make_client()["db"]
    collection = database["read_concern"]
    collection.drop()
    collection.insert_many([{"id": 1}, {"id": 2}])

    for level in ["local", "available", "majority"]:
        assert (
            database.command(
                {"count": "read_concern", "query": {}, "readConcern": {"level": level}}
            )["n"]
            == 2
        )
        assert sorted(
            document["id"]
            for document in database.command(
                {"find": "read_concern", "filter": {}, "readConcern": {"level": level}}
            )["cursor"]["firstBatch"]
        ) == [1, 2]

    for level in ["snapshot", "linearizable"]:
        for command in [
            {"find": "read_concern", "filter": {}, "readConcern": {"level": level}},
            {"count": "read_concern", "query": {}, "readConcern": {"level": level}},
            {
                "aggregate": "read_concern",
                "pipeline": [],
                "cursor": {},
                "readConcern": {"level": level},
            },
        ]:
            with pytest.raises(pymongo.errors.OperationFailure):
                database.command(command)

    collection.drop()


def test_a_projection_of_a_field_takes_the_fields_below_it(started_cluster):
    """A nested document of a table created in ClickHouse is a set of columns whose names are the
    dotted paths of its fields, and a reply rebuilds the document out of them. So a projection of
    the field they belong to is a projection of all of them, the way MongoDB answers with the whole
    subdocument, and an exclusion of it removes all of them."""
    node = cluster.instances["node"]
    node.query("CREATE DATABASE IF NOT EXISTS db", password="123")
    node.query("DROP TABLE IF EXISTS db.subtree", password="123")
    node.query(
        "CREATE TABLE db.subtree (id Int64, `profile.name` String, `profile.age` Int64, other String) "
        "ENGINE = MergeTree ORDER BY id",
        password="123",
    )

    collection = make_client()["db"]["subtree"]
    collection.insert_one(
        {"id": 1, "profile": {"name": "alpha", "age": 30}, "other": "x"}
    )

    assert list(collection.find({}, {"profile": 1})) == [
        {"profile": {"name": "alpha", "age": 30}}
    ]
    assert list(collection.find({}, {"profile": 0})) == [{"id": 1, "other": "x"}]
    assert list(collection.aggregate([{"$unset": "profile"}])) == [
        {"id": 1, "other": "x"}
    ]

    node.query("DROP TABLE db.subtree", password="123")


def test_a_write_command_option_that_is_not_implemented_is_an_error(started_cluster):
    """`maxTimeMS` bounds how long a command may run and `commitQuorum` asks for acknowledgements
    the endpoint cannot give. A write command that asks for either is refused before anything is
    written, rather than acknowledged as a plain local write that ignored the contract."""
    database = make_client()["db"]
    collection = database["write_options"]
    collection.drop()
    database.create_collection("write_options")

    for command in [
        {"insert": "write_options", "documents": [{"id": 1}], "maxTimeMS": 1},
        {
            "update": "write_options",
            "updates": [{"q": {"id": 1}, "u": {"$set": {"id": 2}}}],
            "maxTimeMS": 1,
        },
        {
            "delete": "write_options",
            "deletes": [{"q": {"id": 1}, "limit": 0}],
            "maxTimeMS": 1,
        },
        {
            "createIndexes": "write_options",
            "indexes": [{"key": {"id": 1}, "name": "id_1"}],
            "commitQuorum": "majority",
        },
        {"drop": "write_options", "maxTimeMS": 1},
        {"create": "write_options_created", "maxTimeMS": 1},
    ]:
        with pytest.raises(pymongo.errors.OperationFailure):
            database.command(command)
    with pytest.raises(pymongo.errors.OperationFailure):
        make_client()["db_of_a_timed_drop"].command({"dropDatabase": 1, "maxTimeMS": 1})

    # The refused commands changed nothing.
    assert list(collection.find({})) == []
    assert "write_options" in database.list_collection_names()
    assert "write_options_created" not in database.list_collection_names()

    # `maxTimeMS: 0` is what a driver sends when no timeout is configured, and asks for nothing.
    database.command(
        {"insert": "write_options", "documents": [{"id": 1}], "maxTimeMS": 0}
    )
    assert [document["id"] for document in collection.find({})] == [1]

    collection.drop()


def test_list_command_options_are_honoured_or_refused(started_cluster):
    """`listCollections` and `listDatabases` must honour or refuse an option that changes their
    answer rather than drop it: `nameOnly` and an equality-on-`name` filter are honoured, any
    other `filter` is refused, and `authorizedCollections` / `authorizedDatabases` ask for the
    listing the authenticated user gets here anyway."""
    client = make_client()
    database = client["db"]
    collection = database["list_options"]
    collection.drop()
    database.create_collection("list_options")

    # pymongo's `list_collection_names` sends `nameOnly: true, authorizedCollections: true`.
    assert "list_options" in database.list_collection_names()

    # Without `nameOnly` a collection carries its options, id index and info; with it, the name
    # and the type alone.
    verbose = database.command({"listCollections": 1})["cursor"]["firstBatch"]
    entry = next(e for e in verbose if e["name"] == "list_options")
    assert entry["type"] == "collection"
    assert "options" in entry and "idIndex" in entry and "info" in entry

    names_only = database.command({"listCollections": 1, "nameOnly": True})["cursor"][
        "firstBatch"
    ]
    entry = next(e for e in names_only if e["name"] == "list_options")
    assert set(entry) == {"name", "type"}

    # A `filter` is honoured as an equality on `name` - which is how a driver itself probes for
    # one collection - and refused beyond that; `cursor` options are refused because everything
    # is answered in the first batch.
    assert (
        database.command({"listCollections": 1, "filter": {}, "cursor": {}})["ok"]
        == 1.0
    )
    filtered = database.command(
        {"listCollections": 1, "filter": {"name": "list_options"}}
    )["cursor"]["firstBatch"]
    assert [e["name"] for e in filtered] == ["list_options"]
    with pytest.raises(pymongo.errors.OperationFailure):
        database.command({"listCollections": 1, "filter": {"type": "view"}})
    with pytest.raises(pymongo.errors.OperationFailure):
        database.command({"listCollections": 1, "cursor": {"batchSize": 1}})

    # The same contract for `listDatabases`; pymongo's `list_database_names` sends
    # `nameOnly: true`.
    assert "db" in client.list_database_names()
    verbose = client["admin"].command({"listDatabases": 1})["databases"]
    assert set(next(d for d in verbose if d["name"] == "db")) == {"name", "empty"}
    names_only = client["admin"].command({"listDatabases": 1, "nameOnly": True})[
        "databases"
    ]
    assert set(next(d for d in names_only if d["name"] == "db")) == {"name"}
    filtered = client["admin"].command({"listDatabases": 1, "filter": {"name": "db"}})[
        "databases"
    ]
    assert [d["name"] for d in filtered] == ["db"]
    with pytest.raises(pymongo.errors.OperationFailure):
        client["admin"].command(
            {"listDatabases": 1, "filter": {"sizeOnDisk": {"$gt": 0}}}
        )

    collection.drop()
