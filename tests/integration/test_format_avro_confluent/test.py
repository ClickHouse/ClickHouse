import io
import json
import logging
import urllib.request
from urllib import parse

import avro.schema
import pytest
from confluent_kafka.avro.cached_schema_registry_client import (
    CachedSchemaRegistryClient,
)
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer

from helpers.cluster import ClickHouseCluster, ClickHouseInstance


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("dummy", with_kafka=True, with_secrets=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def run_query(instance, query, data=None, settings=None):
    # type: (ClickHouseInstance, str, object, dict) -> str

    logging.info("Running query '{}'...".format(query))
    # use http to force parsing on server
    if not data:
        data = " "  # make POST request
    result = instance.http_query(query, data=data, params=settings)
    logging.info("Query finished")

    return result


def test_select(started_cluster):
    # type: (ClickHouseCluster) -> None

    reg_url = "http://localhost:{}".format(started_cluster.schema_registry_port)
    arg = {"url": reg_url}

    schema_registry_client = CachedSchemaRegistryClient(arg)
    serializer = MessageSerializer(schema_registry_client)

    schema = avro.schema.make_avsc_object(
        {
            "name": "test_record1",
            "type": "record",
            "fields": [{"name": "value", "type": "long"}],
        }
    )

    buf = io.BytesIO()
    for x in range(0, 3):
        message = serializer.encode_record_with_schema(
            "test_subject1", schema, {"value": x}
        )
        buf.write(message)
    data = buf.getvalue()

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}".format(
        started_cluster.schema_registry_host, started_cluster.schema_registry_port
    )

    run_query(instance, "create table avro_data(value Int64) engine = Memory()")
    settings = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(instance, "insert into avro_data format AvroConfluent", data, settings)
    stdout = run_query(instance, "select * from avro_data")
    assert list(map(str.split, stdout.splitlines())) == [
        ["0"],
        ["1"],
        ["2"],
    ]


def test_select_auth(started_cluster):
    # type: (ClickHouseCluster) -> None

    reg_url = "http://localhost:{}".format(started_cluster.schema_registry_auth_port)
    arg = {
        "url": reg_url,
        "basic.auth.credentials.source": "USER_INFO",
        "basic.auth.user.info": "schemauser:letmein",
    }

    schema_registry_client = CachedSchemaRegistryClient(arg)
    serializer = MessageSerializer(schema_registry_client)

    schema = avro.schema.make_avsc_object(
        {
            "name": "test_record_auth",
            "type": "record",
            "fields": [{"name": "value", "type": "long"}],
        }
    )

    buf = io.BytesIO()
    for x in range(0, 3):
        message = serializer.encode_record_with_schema(
            "test_subject_auth", schema, {"value": x}
        )
        buf.write(message)
    data = buf.getvalue()

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}@{}:{}".format(
        "schemauser",
        "letmein",
        started_cluster.schema_registry_auth_host,
        started_cluster.schema_registry_auth_port,
    )

    run_query(instance, "create table avro_data_auth(value Int64) engine = Memory()")
    settings = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(
        instance, "insert into avro_data_auth format AvroConfluent", data, settings
    )
    stdout = run_query(instance, "select * from avro_data_auth")
    assert list(map(str.split, stdout.splitlines())) == [
        ["0"],
        ["1"],
        ["2"],
    ]


def test_select_auth_encoded(started_cluster):
    # type: (ClickHouseCluster) -> None

    reg_url = "http://localhost:{}".format(started_cluster.schema_registry_auth_port)
    arg = {
        "url": reg_url,
        "basic.auth.credentials.source": "USER_INFO",
        "basic.auth.user.info": "schemauser:letmein",
    }

    schema_registry_client = CachedSchemaRegistryClient(arg)
    serializer = MessageSerializer(schema_registry_client)

    schema = avro.schema.make_avsc_object(
        {
            "name": "test_record_auth_encoded",
            "type": "record",
            "fields": [{"name": "value", "type": "long"}],
        }
    )

    buf = io.BytesIO()
    for x in range(0, 3):
        message = serializer.encode_record_with_schema(
            "test_subject_auth_encoded", schema, {"value": x}
        )
        buf.write(message)
    data = buf.getvalue()

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}@{}:{}".format(
        parse.quote_plus("schemauser/slash"),
        parse.quote_plus("letmein"),
        started_cluster.schema_registry_auth_host,
        started_cluster.schema_registry_auth_port,
    )

    run_query(
        instance, "create table avro_data_auth_encoded(value Int64) engine = Memory()"
    )
    settings = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(
        instance,
        "insert into avro_data_auth_encoded format AvroConfluent",
        data,
        settings,
    )
    stdout = run_query(instance, "select * from avro_data_auth_encoded")
    assert list(map(str.split, stdout.splitlines())) == [
        ["0"],
        ["1"],
        ["2"],
    ]


def test_output(started_cluster):
    # type: (ClickHouseCluster) -> None

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}".format(
        started_cluster.schema_registry_host, started_cluster.schema_registry_port
    )

    # Write rows using AvroConfluent output format (registers schema automatically),
    # then read them back using AvroConfluent input format.
    run_query(instance, "create table avro_output_source(value Int64) engine = Memory()")
    run_query(instance, "insert into avro_output_source values (10),(20),(30)")

    settings = {
        "format_avro_schema_registry_url": schema_registry_url,
        "output_format_avro_confluent_subject": "test_output_subject",
    }
    data = run_query(
        instance, "select * from avro_output_source format AvroConfluent", settings=settings
    )

    # Feed the Confluent-framed binary back into a table via AvroConfluent input
    run_query(instance, "create table avro_output_sink(value Int64) engine = Memory()")
    settings_in = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(
        instance,
        "insert into avro_output_sink format AvroConfluent",
        data,
        settings_in,
    )
    stdout = run_query(instance, "select * from avro_output_sink")
    assert list(map(str.split, stdout.splitlines())) == [
        ["10"],
        ["20"],
        ["30"],
    ]


def test_output_multiple_columns(started_cluster):
    # type: (ClickHouseCluster) -> None

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}".format(
        started_cluster.schema_registry_host, started_cluster.schema_registry_port
    )

    run_query(
        instance,
        "create table avro_output_multi_source(id Int64, name String) engine = Memory()",
    )
    run_query(
        instance,
        "insert into avro_output_multi_source values (1,'alice'),(2,'bob'),(3,'charlie')",
    )

    settings = {
        "format_avro_schema_registry_url": schema_registry_url,
        "output_format_avro_confluent_subject": "test_output_multi_subject",
        "output_format_avro_string_column_pattern": "^name$",
    }
    data = run_query(
        instance,
        "select * from avro_output_multi_source format AvroConfluent",
        settings=settings,
    )

    run_query(
        instance,
        "create table avro_output_multi_sink(id Int64, name String) engine = Memory()",
    )
    settings_in = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(
        instance,
        "insert into avro_output_multi_sink format AvroConfluent",
        data,
        settings_in,
    )
    stdout = run_query(instance, "select * from avro_output_multi_sink")
    assert list(map(str.split, stdout.splitlines())) == [
        ["1", "alice"],
        ["2", "bob"],
        ["3", "charlie"],
    ]


def test_output_subject_encoding(started_cluster):
    # type: (ClickHouseCluster) -> None

    # Subject names with reserved URI characters (/, ?, #) must be properly
    # encoded when registering with the Schema Registry. First register a
    # schema under such a subject using the official Python client (ground
    # truth), then do the same from ClickHouse and compare the stored names.

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}".format(
        started_cluster.schema_registry_host, started_cluster.schema_registry_port
    )
    local_registry_url = "http://localhost:{}".format(
        started_cluster.schema_registry_port
    )

    subject_with_special_chars = "my/topic?key#value"

    # Now register a schema under the same subject from ClickHouse
    run_query(
        instance,
        "create table avro_output_enc_source(id Int64) engine = Memory()",
    )
    run_query(instance, "insert into avro_output_enc_source values (1),(2),(3)")

    settings = {
        "format_avro_schema_registry_url": schema_registry_url,
        "output_format_avro_confluent_subject": subject_with_special_chars,
    }
    data = run_query(
        instance,
        "select * from avro_output_enc_source format AvroConfluent",
        settings=settings,
    )

    # Read back via AvroConfluent input to verify the framing is valid
    run_query(
        instance,
        "create table avro_output_enc_sink(id Int64) engine = Memory()",
    )
    settings_in = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(
        instance,
        "insert into avro_output_enc_sink format AvroConfluent",
        data,
        settings_in,
    )
    stdout = run_query(instance, "select * from avro_output_enc_sink")
    assert list(map(str.split, stdout.splitlines())) == [
        ["1"],
        ["2"],
        ["3"],
    ]

    # Verify ClickHouse registered the schema under the same subject name
    # as the Python client (not under a mangled/encoded variant)
    subjects_after = json.loads(
        urllib.request.urlopen(local_registry_url + "/subjects").read()
    )
    logging.info(f"Subjects in Schema Registry after registration: {subjects_after}")
    assert subject_with_special_chars in subjects_after, (
        f"ClickHouse registered under wrong subject: {subjects_after}"
    )


def test_output_register_same_schema_is_idempotent(started_cluster):
    # type: (ClickHouseCluster) -> None

    # Registering a schema is just a translation from a schema definition to a
    # schema id: if an identical schema is already registered under the
    # subject, the Schema Registry returns the existing id and no error is
    # raised. Verify by running the AvroConfluent output twice with the same
    # subject and schema, comparing the schema id embedded in the Confluent
    # framing (magic byte + 4-byte big-endian id) and asserting that the
    # registry still only holds a single version under the subject.

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}".format(
        started_cluster.schema_registry_host, started_cluster.schema_registry_port
    )
    local_registry_url = "http://localhost:{}".format(
        started_cluster.schema_registry_port
    )

    subject = "test_output_idempotent_subject"

    run_query(
        instance,
        "create table avro_output_idempotent_source(value Int64) engine = Memory()",
    )
    run_query(instance, "insert into avro_output_idempotent_source values (1),(2),(3)")

    settings = {
        "format_avro_schema_registry_url": schema_registry_url,
        "output_format_avro_confluent_subject": subject,
    }

    def get_schema_id(framed_bytes):
        # Confluent framing: 1 magic byte (0x00) + 4-byte big-endian schema id
        # followed by the Avro-encoded payload.
        assert len(framed_bytes) >= 5
        assert framed_bytes[0] == 0x00
        return int.from_bytes(framed_bytes[1:5], byteorder="big")

    data_first = instance.http_query(
        "select * from avro_output_idempotent_source format AvroConfluent",
        data=" ",
        params=settings,
        content=True,
    )

    # Drop the schema registry cache so the second registration goes back over
    # the wire. Otherwise the in-process register cache would short-circuit the
    # call and we wouldn't actually exercise the server's idempotency.
    run_query(instance, "system drop avro schema cache")

    data_second = instance.http_query(
        "select * from avro_output_idempotent_source format AvroConfluent",
        data=" ",
        params=settings,
        content=True,
    )

    first_id = get_schema_id(data_first)
    second_id = get_schema_id(data_second)
    assert first_id == second_id, (
        f"Re-registering the same schema produced a different id: "
        f"{first_id} vs {second_id}"
    )

    versions = json.loads(
        urllib.request.urlopen(
            local_registry_url + "/subjects/" + parse.quote(subject, safe="") + "/versions"
        ).read()
    )
    assert versions == [1], (
        f"Expected exactly one version under subject '{subject}', got {versions}"
    )


def test_output_incompatible_schema(started_cluster):
    # type: (ClickHouseCluster) -> None

    # Pre-register a schema under a subject with a required `value` field,
    # then try to register an incompatible schema (different required field,
    # no default) from ClickHouse. The Schema Registry returns HTTP 409 under
    # the default BACKWARD compatibility, and ClickHouse should surface a
    # clear INCOMPATIBLE_SCHEMA error that includes the registry's own
    # `message` and the hint about setting compatibility to NONE.

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}".format(
        started_cluster.schema_registry_host, started_cluster.schema_registry_port
    )
    local_registry_url = "http://localhost:{}".format(
        started_cluster.schema_registry_port
    )

    subject = "test_output_incompatible_subject"
    # Distinctive field name we can look for in the registry's error message
    # to verify we correctly propagate the server-side details.
    unique_field_name = "clickhouse_test_field_qwerty_12345"

    # Ensure strict compatibility on the subject (BACKWARD is the global default,
    # but we set it explicitly so the test does not depend on registry config).
    req = urllib.request.Request(
        local_registry_url + "/config/" + parse.quote(subject, safe=""),
        data=b'{"compatibility": "BACKWARD"}',
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="PUT",
    )
    urllib.request.urlopen(req).read()

    # Register a prior version of the schema with a required `value` field.
    prior_schema = {
        "type": "record",
        "name": "test_record_incompatible",
        "fields": [{"name": "value", "type": "long"}],
    }
    req = urllib.request.Request(
        local_registry_url + "/subjects/" + parse.quote(subject, safe="") + "/versions",
        data=json.dumps({"schema": json.dumps(prior_schema)}).encode(),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    urllib.request.urlopen(req).read()

    # Now try to register an incompatible schema from ClickHouse: the column
    # has a distinctive name that does not exist in the prior schema and has
    # no default value. Under BACKWARD compat this is rejected as
    # READER_FIELD_MISSING_DEFAULT_VALUE, and the registry's response
    # includes the offending field name.
    run_query(
        instance,
        "create table avro_output_incompatible({0} Int64) engine = Memory()".format(
            unique_field_name
        ),
    )
    run_query(instance, "insert into avro_output_incompatible values (1),(2),(3)")

    settings = {
        "format_avro_schema_registry_url": schema_registry_url,
        "output_format_avro_confluent_subject": subject,
    }
    error = instance.http_query_and_get_error(
        "select * from avro_output_incompatible format AvroConfluent",
        params=settings,
    )

    logging.info("ClickHouse error for incompatible schema: %s", error)

    assert "INCOMPATIBLE_SCHEMA" in error
    assert "HTTP 409 Conflict" in error
    assert subject in error
    # Message coming from Confluent Schema Registry itself, if flaky can be removed
    assert "incompatible with an earlier schema" in error


def test_select_auth_encoded_complex(started_cluster):
    # type: (ClickHouseCluster) -> None

    reg_url = "http://localhost:{}".format(started_cluster.schema_registry_auth_port)
    arg = {
        "url": reg_url,
        "basic.auth.credentials.source": "USER_INFO",
        "basic.auth.user.info": "schemauser:letmein",
    }

    schema_registry_client = CachedSchemaRegistryClient(arg)
    serializer = MessageSerializer(schema_registry_client)

    schema = avro.schema.make_avsc_object(
        {
            "name": "test_record_auth_encoded_complex",
            "type": "record",
            "fields": [{"name": "value", "type": "long"}],
        }
    )

    buf = io.BytesIO()
    for x in range(0, 3):
        message = serializer.encode_record_with_schema(
            "test_subject_auth_encoded_complex", schema, {"value": x}
        )
        buf.write(message)
    data = buf.getvalue()

    instance = started_cluster.instances["dummy"]  # type: ClickHouseInstance
    schema_registry_url = "http://{}:{}@{}:{}".format(
        parse.quote_plus("complexschemauser"),
        parse.quote_plus("letmein%@:/"),
        started_cluster.schema_registry_auth_host,
        started_cluster.schema_registry_auth_port,
    )

    run_query(
        instance,
        "create table avro_data_auth_encoded_complex(value Int64) engine = Memory()",
    )
    settings = {"format_avro_schema_registry_url": schema_registry_url}
    run_query(
        instance,
        "insert into avro_data_auth_encoded_complex format AvroConfluent",
        data,
        settings,
    )
    stdout = run_query(instance, "select * from avro_data_auth_encoded_complex")
    assert list(map(str.split, stdout.splitlines())) == [
        ["0"],
        ["1"],
        ["2"],
    ]
