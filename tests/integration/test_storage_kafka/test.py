import json
import os.path as p
import random
import socket
import subprocess
import threading
import time
import logging
import io
import string

import avro.schema
import avro.io
import avro.datafile
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from confluent_kafka import admin

import kafka.errors
import pytest
from google.protobuf.internal.encoder import _VarintBytes
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import TSV
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer, BrokerConnection
from kafka.protocol.admin import DescribeGroupsRequest_v1
from kafka.protocol.group import MemberAssignment
from kafka.admin import NewTopic


# protoc --version
# libprotoc 3.0.0
# # to create kafka_pb2.py
# protoc --python_out=. kafka.proto

from . import kafka_pb2
from . import social_pb2
from . import message_with_repeated_pb2


# TODO: add test for run-time offset update in CH, if we manually update it on Kafka side.
# TODO: add test for SELECT LIMIT is working.

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                main_configs=['configs/kafka.xml'],
                                with_kafka=True,
                                with_zookeeper=True, # For Replicated Table
                                macros={"kafka_broker":"kafka1",
                                        "kafka_topic_old":"old",
                                        "kafka_group_name_old":"old",
                                        "kafka_topic_new":"new",
                                        "kafka_group_name_new":"new",
                                        "kafka_client_id":"instance",
                                        "kafka_format_json_each_row":"JSONEachRow"},
                                clickhouse_path_dir='clickhouse_path')


def get_kafka_producer(port, serializer, retries):
    errors = []
    for _ in range(retries):
        try:
            producer = KafkaProducer(bootstrap_servers="localhost:{}".format(port), value_serializer=serializer)
            logging.debug("Kafka Connection establised: localhost:{}".format(port))
            return producer
        except Exception as e:
            errors += [str(e)]
            time.sleep(1)
    
    raise Exception("Connection not establised, {}".format(errors))

def producer_serializer(x):
    return x.encode() if isinstance(x, str) else x

def kafka_produce(kafka_cluster, topic, messages, timestamp=None, retries=15):
    logging.debug("kafka_produce server:{}:{} topic:{}".format("localhost", kafka_cluster.kafka_port, topic))
    producer = get_kafka_producer(kafka_cluster.kafka_port, producer_serializer, retries)
    for message in messages:
        producer.send(topic=topic, value=message, timestamp_ms=timestamp)
        producer.flush()

## just to ensure the python client / producer is working properly
def kafka_producer_send_heartbeat_msg(max_retries=50):
    kafka_produce(kafka_cluster, 'test_heartbeat_topic', ['test'], retries=max_retries)

def kafka_consume(kafka_cluster, topic):
    consumer = KafkaConsumer(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port), auto_offset_reset="earliest")
    consumer.subscribe(topics=(topic))
    for toppar, messages in list(consumer.poll(5000).items()):
        if toppar.topic == topic:
            for message in messages:
                yield message.value.decode()
    consumer.unsubscribe()
    consumer.close()


def kafka_produce_protobuf_messages(kafka_cluster, topic, start_index, num_messages):
    data = b''
    for i in range(start_index, start_index + num_messages):
        msg = kafka_pb2.KeyValuePair()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    producer = KafkaProducer(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port), value_serializer=producer_serializer)
    producer.send(topic=topic, value=data)
    producer.flush()
    logging.debug(("Produced {} messages for topic {}".format(num_messages, topic)))

def kafka_produce_protobuf_messages_no_delimeters(kafka_cluster, topic, start_index, num_messages):
    data = ''
    producer = KafkaProducer(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))
    for i in range(start_index, start_index + num_messages):
        msg = kafka_pb2.KeyValuePair()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        producer.send(topic=topic, value=serialized_msg)
    producer.flush()
    logging.debug("Produced {} messages for topic {}".format(num_messages, topic))

def kafka_produce_protobuf_social(kafka_cluster,topic, start_index, num_messages):
    data = b''
    for i in range(start_index, start_index + num_messages):
        msg = social_pb2.User()
        msg.username='John Doe {}'.format(i)
        msg.timestamp=1000000+i
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    producer = KafkaProducer(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port), value_serializer=producer_serializer)
    producer.send(topic=topic, value=data)
    producer.flush()
    logging.debug(("Produced {} messages for topic {}".format(num_messages, topic)))

def avro_message(value):
    schema = avro.schema.make_avsc_object({
        'name': 'row',
        'type': 'record',
        'fields': [
            {'name': 'id', 'type': 'long'},
            {'name': 'blockNo', 'type': 'int'},
            {'name': 'val1', 'type': 'string'},
            {'name': 'val2', 'type': 'float'},
            {'name': 'val3', 'type': 'int'}
        ]
    })
    bytes_writer = io.BytesIO()
    # writer = avro.io.DatumWriter(schema)
    # encoder = avro.io.BinaryEncoder(bytes_writer)
    # writer.write(value, encoder)


    # DataFileWrite seems to be mandatory to get schema encoded
    writer = avro.datafile.DataFileWriter(bytes_writer, avro.io.DatumWriter(), schema)
    if isinstance(value, list):
        for v in value:
            writer.append(v)
    else:
        writer.append(value)
    writer.flush()
    raw_bytes = bytes_writer.getvalue()

    writer.close()
    bytes_writer.close()
    return raw_bytes

def avro_confluent_message(schema_registry_client, value):
    # type: (CachedSchemaRegistryClient, dict) -> str

    serializer = MessageSerializer(schema_registry_client)
    schema = avro.schema.make_avsc_object({
        'name': 'row',
        'type': 'record',
        'fields': [
            {'name': 'id', 'type': 'long'},
            {'name': 'blockNo', 'type': 'int'},
            {'name': 'val1', 'type': 'string'},
            {'name': 'val2', 'type': 'float'},
            {'name': 'val3', 'type': 'int'}
        ]
    })
    return serializer.encode_record_with_schema('test_subject', schema, value)

# Since everything is async and shaky when receiving messages from Kafka,
# we may want to try and check results multiple times in a loop.
def kafka_check_result(result, check=False, ref_file='test_kafka_json.reference'):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)

def describe_consumer_group(kafka_cluster, name):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))
    consumer_groups = admin_client.describe_consumer_groups([name])
    res = []
    for member in consumer_groups[0].members:
        member_info = {}
        member_info['member_id'] = member.member_id
        member_info['client_id'] = member.client_id
        member_info['client_host'] = member.client_host
        member_topics_assignment = []
        for (topic, partitions) in member.member_assignment.assignment:
            member_topics_assignment.append({'topic': topic, 'partitions': partitions})
        member_info['assignment'] = member_topics_assignment
        res.append(member_info)
    return res

# Fixtures

@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        global kafka_id
        cluster.start()
        kafka_id = instance.cluster.kafka_docker_id
        print(("kafka_id is {}".format(kafka_id)))
        yield cluster

    finally:
        cluster.shutdown()

@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query('DROP DATABASE IF EXISTS test; CREATE DATABASE test;')
    wait_kafka_is_available() # ensure kafka is alive
    kafka_producer_send_heartbeat_msg() # ensure python kafka client is ok
    # print("kafka is available - running test")
    yield  # run test

# Tests

def test_kafka_settings_old_syntax(kafka_cluster):
    assert TSV(instance.query("SELECT * FROM system.macros WHERE macro like 'kafka%' ORDER BY macro",
                              ignore_error=True)) == TSV('''kafka_broker	kafka1
kafka_client_id	instance
kafka_format_json_each_row	JSONEachRow
kafka_group_name_new	new
kafka_group_name_old	old
kafka_topic_new	new
kafka_topic_old	old
''')

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka('{kafka_broker}:19092', '{kafka_topic_old}', '{kafka_group_name_old}', '{kafka_format_json_each_row}', '\\n');
        ''')

    # Don't insert malformed messages since old settings syntax
    # doesn't support skipping of broken messages.
    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce(kafka_cluster, 'old', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)

    members = describe_consumer_group('old')
    assert members[0]['client_id'] == 'ClickHouse-instance-test-kafka'
    # text_desc = kafka_cluster.exec_in_container(kafka_cluster.get_container_id('kafka1'),"kafka-consumer-groups --bootstrap-server localhost:9092 --describe --members --group old --verbose"))


def test_kafka_settings_new_syntax(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = '{kafka_broker}:19092',
                     kafka_topic_list = '{kafka_topic_new}',
                     kafka_group_name = '{kafka_group_name_new}',
                     kafka_format = '{kafka_format_json_each_row}',
                     kafka_row_delimiter = '\\n',
                     kafka_client_id = '{kafka_client_id} test 1234',
                     kafka_skip_broken_messages = 1;
        ''')

    messages = []
    for i in range(25):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce(kafka_cluster, 'new', messages)

    # Insert couple of malformed messages.
    kafka_produce(kafka_cluster, 'new', ['}{very_broken_message,'])
    kafka_produce(kafka_cluster, 'new', ['}another{very_broken_message,'])

    messages = []
    for i in range(25, 50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce(kafka_cluster, 'new', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)

    members = describe_consumer_group('new')
    assert members[0]['client_id'] == 'instance test 1234'


def test_kafka_json_as_string(kafka_cluster):
    kafka_produce(kafka_cluster, 'kafka_json_as_string', ['{"t": 123, "e": {"x": "woof"} }', '', '{"t": 124, "e": {"x": "test"} }',
                                           '{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}'])

    # 'tombstone' record (null value) = marker of deleted record
    producer = KafkaProducer(bootstrap_servers="localhost:{}".format(cluster.kafka_port), value_serializer=producer_serializer, key_serializer=producer_serializer)
    producer.send(topic='kafka_json_as_string', key='xxx')
    producer.flush()

    instance.query('''
        CREATE TABLE test.kafka (field String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'kafka_json_as_string',
                     kafka_group_name = 'kafka_json_as_string',
                     kafka_format = 'JSONAsString',
                     kafka_flush_interval_ms=1000;
        ''')

    result = instance.query('SELECT * FROM test.kafka;')
    expected = '''\
{"t": 123, "e": {"x": "woof"} }
{"t": 124, "e": {"x": "test"} }
{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}
'''
    assert TSV(result) == TSV(expected)
    assert instance.contains_in_log(
        "Parsing of message (topic: kafka_json_as_string, partition: 0, offset: [0-9]*) return no rows")


def test_kafka_formats(kafka_cluster):
    schema_registry_client = CachedSchemaRegistryClient('http://localhost:{}'.format(kafka_cluster.schema_registry_port))

    # data was dumped from clickhouse itself in a following manner
    # clickhouse-client --format=Native --query='SELECT toInt64(number) as id, toUInt16( intDiv( id, 65536 ) ) as blockNo, reinterpretAsString(19777) as val1, toFloat32(0.5) as val2, toUInt8(1) as val3 from numbers(100) ORDER BY id' | xxd -ps | tr -d '\n' | sed 's/\(..\)/\\x\1/g'

    all_formats = {
        ## Text formats ##
        # dumped with clickhouse-client ... | perl -pe 's/\n/\\n/; s/\t/\\t/g;'
        'JSONEachRow': {
            'data_sample': [
                '{"id":"0","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
                '{"id":"1","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"2","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"3","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"4","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"5","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"6","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"7","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"8","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"9","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"10","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"11","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"12","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"13","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"14","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"15","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
                '{"id":"0","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
            ],
            'supports_empty_value': True,
        },
        # JSONAsString doesn't fit to that test, and tested separately
        'JSONCompactEachRow': {
            'data_sample': [
                '["0", 0, "AM", 0.5, 1]\n',
                '["1", 0, "AM", 0.5, 1]\n["2", 0, "AM", 0.5, 1]\n["3", 0, "AM", 0.5, 1]\n["4", 0, "AM", 0.5, 1]\n["5", 0, "AM", 0.5, 1]\n["6", 0, "AM", 0.5, 1]\n["7", 0, "AM", 0.5, 1]\n["8", 0, "AM", 0.5, 1]\n["9", 0, "AM", 0.5, 1]\n["10", 0, "AM", 0.5, 1]\n["11", 0, "AM", 0.5, 1]\n["12", 0, "AM", 0.5, 1]\n["13", 0, "AM", 0.5, 1]\n["14", 0, "AM", 0.5, 1]\n["15", 0, "AM", 0.5, 1]\n',
                '["0", 0, "AM", 0.5, 1]\n',
            ],
            'supports_empty_value': True,
        },
        'JSONCompactEachRowWithNamesAndTypes': {
            'data_sample': [
                '["id", "blockNo", "val1", "val2", "val3"]\n["Int64", "UInt16", "String", "Float32", "UInt8"]\n["0", 0, "AM", 0.5, 1]\n',
                '["id", "blockNo", "val1", "val2", "val3"]\n["Int64", "UInt16", "String", "Float32", "UInt8"]\n["1", 0, "AM", 0.5, 1]\n["2", 0, "AM", 0.5, 1]\n["3", 0, "AM", 0.5, 1]\n["4", 0, "AM", 0.5, 1]\n["5", 0, "AM", 0.5, 1]\n["6", 0, "AM", 0.5, 1]\n["7", 0, "AM", 0.5, 1]\n["8", 0, "AM", 0.5, 1]\n["9", 0, "AM", 0.5, 1]\n["10", 0, "AM", 0.5, 1]\n["11", 0, "AM", 0.5, 1]\n["12", 0, "AM", 0.5, 1]\n["13", 0, "AM", 0.5, 1]\n["14", 0, "AM", 0.5, 1]\n["15", 0, "AM", 0.5, 1]\n',
                '["id", "blockNo", "val1", "val2", "val3"]\n["Int64", "UInt16", "String", "Float32", "UInt8"]\n["0", 0, "AM", 0.5, 1]\n',
                # ''
                # On empty message exception: Cannot parse input: expected '[' at end of stream., Stack trace (when copying this message, always include the lines below):
                # /src/IO/ReadHelpers.h:175: DB::assertChar(char, DB::ReadBuffer&) @ 0x15db231a in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/JSONCompactEachRowRowInputFormat.cpp:0: DB::JSONCompactEachRowRowInputFormat::readPrefix() @ 0x1dee6bd6 in /usr/bin/clickhouse
                # /src/Processors/Formats/IRowInputFormat.cpp:0: DB::IRowInputFormat::generate() @ 0x1de72710 in /usr/bin/clickhouse
            ],
        },
        'TSKV': {
            'data_sample': [
                'id=0\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\n',
                'id=1\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=2\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=3\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=4\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=5\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=6\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=7\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=8\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=9\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=10\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=11\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=12\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=13\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=14\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=15\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\n',
                'id=0\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\n',
                # ''
                # On empty message exception: Unexpected end of stream while reading key name from TSKV format
                # /src/Processors/Formats/Impl/TSKVRowInputFormat.cpp:88: DB::readName(DB::ReadBuffer&, StringRef&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >&) @ 0x1df8c098 in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/TSKVRowInputFormat.cpp:114: DB::TSKVRowInputFormat::readRow(std::__1::vector<COW<DB::IColumn>::mutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::mutable_ptr<DB::IColumn> > >&, DB::RowReadExtension&) @ 0x1df8ae3e in /usr/bin/clickhouse
                # /src/Processors/Formats/IRowInputFormat.cpp:64: DB::IRowInputFormat::generate() @ 0x1de727cf in /usr/bin/clickhouse
            ],
        },
        'CSV': {
            'data_sample': [
                '0,0,"AM",0.5,1\n',
                '1,0,"AM",0.5,1\n2,0,"AM",0.5,1\n3,0,"AM",0.5,1\n4,0,"AM",0.5,1\n5,0,"AM",0.5,1\n6,0,"AM",0.5,1\n7,0,"AM",0.5,1\n8,0,"AM",0.5,1\n9,0,"AM",0.5,1\n10,0,"AM",0.5,1\n11,0,"AM",0.5,1\n12,0,"AM",0.5,1\n13,0,"AM",0.5,1\n14,0,"AM",0.5,1\n15,0,"AM",0.5,1\n',
                '0,0,"AM",0.5,1\n',
            ],
            'supports_empty_value': True,
        },
        'TSV': {
            'data_sample': [
                '0\t0\tAM\t0.5\t1\n',
                '1\t0\tAM\t0.5\t1\n2\t0\tAM\t0.5\t1\n3\t0\tAM\t0.5\t1\n4\t0\tAM\t0.5\t1\n5\t0\tAM\t0.5\t1\n6\t0\tAM\t0.5\t1\n7\t0\tAM\t0.5\t1\n8\t0\tAM\t0.5\t1\n9\t0\tAM\t0.5\t1\n10\t0\tAM\t0.5\t1\n11\t0\tAM\t0.5\t1\n12\t0\tAM\t0.5\t1\n13\t0\tAM\t0.5\t1\n14\t0\tAM\t0.5\t1\n15\t0\tAM\t0.5\t1\n',
                '0\t0\tAM\t0.5\t1\n',
            ],
            'supports_empty_value': True,
        },
        'CSVWithNames': {
            'data_sample': [
                '"id","blockNo","val1","val2","val3"\n0,0,"AM",0.5,1\n',
                '"id","blockNo","val1","val2","val3"\n1,0,"AM",0.5,1\n2,0,"AM",0.5,1\n3,0,"AM",0.5,1\n4,0,"AM",0.5,1\n5,0,"AM",0.5,1\n6,0,"AM",0.5,1\n7,0,"AM",0.5,1\n8,0,"AM",0.5,1\n9,0,"AM",0.5,1\n10,0,"AM",0.5,1\n11,0,"AM",0.5,1\n12,0,"AM",0.5,1\n13,0,"AM",0.5,1\n14,0,"AM",0.5,1\n15,0,"AM",0.5,1\n',
                '"id","blockNo","val1","val2","val3"\n0,0,"AM",0.5,1\n',
                # '',
                # On empty message exception happens: Attempt to read after eof
                # /src/IO/VarInt.h:122: DB::throwReadAfterEOF() @ 0x15c34487 in /usr/bin/clickhouse
                # /src/IO/ReadHelpers.cpp:583: void DB::readCSVStringInto<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > >(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >&, DB::ReadBuffer&, DB::FormatSettings::CSV const&) @ 0x15c961e1 in /usr/bin/clickhouse
                # /src/IO/ReadHelpers.cpp:678: DB::readCSVString(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >&, DB::ReadBuffer&, DB::FormatSettings::CSV const&) @ 0x15c8dfae in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/CSVRowInputFormat.cpp:170: DB::CSVRowInputFormat::readPrefix() @ 0x1dec46f7 in /usr/bin/clickhouse
                # /src/Processors/Formats/IRowInputFormat.cpp:0: DB::IRowInputFormat::generate() @ 0x1de72710 in /usr/bin/clickhouse
                # /src/Processors/ISource.cpp:48: DB::ISource::work() @ 0x1dd79737 in /usr/bin/clickhouse
            ],
        },
        'Values': {
            'data_sample': [
                "(0,0,'AM',0.5,1)",
                "(1,0,'AM',0.5,1),(2,0,'AM',0.5,1),(3,0,'AM',0.5,1),(4,0,'AM',0.5,1),(5,0,'AM',0.5,1),(6,0,'AM',0.5,1),(7,0,'AM',0.5,1),(8,0,'AM',0.5,1),(9,0,'AM',0.5,1),(10,0,'AM',0.5,1),(11,0,'AM',0.5,1),(12,0,'AM',0.5,1),(13,0,'AM',0.5,1),(14,0,'AM',0.5,1),(15,0,'AM',0.5,1)",
                "(0,0,'AM',0.5,1)",
            ],
            'supports_empty_value': True,
        },
        'TSVWithNames': {
            'data_sample': [
                'id\tblockNo\tval1\tval2\tval3\n0\t0\tAM\t0.5\t1\n',
                'id\tblockNo\tval1\tval2\tval3\n1\t0\tAM\t0.5\t1\n2\t0\tAM\t0.5\t1\n3\t0\tAM\t0.5\t1\n4\t0\tAM\t0.5\t1\n5\t0\tAM\t0.5\t1\n6\t0\tAM\t0.5\t1\n7\t0\tAM\t0.5\t1\n8\t0\tAM\t0.5\t1\n9\t0\tAM\t0.5\t1\n10\t0\tAM\t0.5\t1\n11\t0\tAM\t0.5\t1\n12\t0\tAM\t0.5\t1\n13\t0\tAM\t0.5\t1\n14\t0\tAM\t0.5\t1\n15\t0\tAM\t0.5\t1\n',
                'id\tblockNo\tval1\tval2\tval3\n0\t0\tAM\t0.5\t1\n',
            ],
            'supports_empty_value': True,
        },
        'TSVWithNamesAndTypes': {
            'data_sample': [
                'id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n0\t0\tAM\t0.5\t1\n',
                'id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n1\t0\tAM\t0.5\t1\n2\t0\tAM\t0.5\t1\n3\t0\tAM\t0.5\t1\n4\t0\tAM\t0.5\t1\n5\t0\tAM\t0.5\t1\n6\t0\tAM\t0.5\t1\n7\t0\tAM\t0.5\t1\n8\t0\tAM\t0.5\t1\n9\t0\tAM\t0.5\t1\n10\t0\tAM\t0.5\t1\n11\t0\tAM\t0.5\t1\n12\t0\tAM\t0.5\t1\n13\t0\tAM\t0.5\t1\n14\t0\tAM\t0.5\t1\n15\t0\tAM\t0.5\t1\n',
                'id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n0\t0\tAM\t0.5\t1\n',
                # '',
                # On empty message exception happens: Cannot parse input: expected '\n' at end of stream.
                # /src/IO/ReadHelpers.cpp:84: DB::throwAtAssertionFailed(char const*, DB::ReadBuffer&) @ 0x15c8d8ec in /usr/bin/clickhouse
                # /src/IO/ReadHelpers.h:175: DB::assertChar(char, DB::ReadBuffer&) @ 0x15db231a in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/TabSeparatedRowInputFormat.cpp:24: DB::skipTSVRow(DB::ReadBuffer&, unsigned long) @ 0x1df92fac in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/TabSeparatedRowInputFormat.cpp:168: DB::TabSeparatedRowInputFormat::readPrefix() @ 0x1df92df0 in /usr/bin/clickhouse
                # /src/Processors/Formats/IRowInputFormat.cpp:0: DB::IRowInputFormat::generate() @ 0x1de72710 in /usr/bin/clickhouse
            ],
        },
        # 'Template' : {
        #     'data_sample' : [
        #         '(id = 0, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)',
        #        # '(id = 1, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 2, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 3, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 4, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 5, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 6, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 7, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 8, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 9, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 10, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 11, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 12, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 13, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 14, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 15, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)',
        #        # '(id = 0, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)',
        #         # '' # tolerates
        #     ],
        #     'extra_settings': ", format_template_row='template_row.format'"
        # },
        'Regexp': {
            'data_sample': [
                '(id = 0, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)',
                '(id = 1, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 2, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 3, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 4, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 5, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 6, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 7, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 8, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 9, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 10, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 11, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 12, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 13, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 14, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 15, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)',
                '(id = 0, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)',
                # ''
                # On empty message exception happens: Line "" doesn't match the regexp.: (at row 1)
                # /src/Processors/Formats/Impl/RegexpRowInputFormat.cpp:140: DB::RegexpRowInputFormat::readRow(std::__1::vector<COW<DB::IColumn>::mutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::mutable_ptr<DB::IColumn> > >&, DB::RowReadExtension&) @ 0x1df82fcb in /usr/bin/clickhouse
            ],
            'extra_settings': r", format_regexp='\(id = (.+?), blockNo = (.+?), val1 = \"(.+?)\", val2 = (.+?), val3 = (.+?)\)', format_regexp_escaping_rule='Escaped'"
        },

        ## BINARY FORMATS
        # dumped with
        # clickhouse-client ... | xxd -ps -c 200 | tr -d '\n' | sed 's/\(..\)/\\x\1/g'
        'Native': {
            'data_sample': [
                b'\x05\x01\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x00\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01',
                b'\x05\x0f\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01',
                b'\x05\x01\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x00\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01',
                # ''
                # On empty message exception happens: DB::Exception: Attempt to read after eof
                # /src/IO/VarInt.h:122: DB::throwReadAfterEOF() @ 0x15c34487 in /usr/bin/clickhouse
                # /src/IO/VarInt.h:135: void DB::readVarUIntImpl<false>(unsigned long&, DB::ReadBuffer&) @ 0x15c68bb7 in /usr/bin/clickhouse
                # /src/IO/VarInt.h:149: DB::readVarUInt(unsigned long&, DB::ReadBuffer&) @ 0x15c68844 in /usr/bin/clickhouse
                # /src/DataStreams/NativeBlockInputStream.cpp:124: DB::NativeBlockInputStream::readImpl() @ 0x1d3e2778 in /usr/bin/clickhouse
                # /src/DataStreams/IBlockInputStream.cpp:60: DB::IBlockInputStream::read() @ 0x1c9c92fd in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/NativeFormat.h:42: DB::NativeInputFormatFromNativeBlockInputStream::generate() @ 0x1df1ea79 in /usr/bin/clickhouse
                # /src/Processors/ISource.cpp:48: DB::ISource::work() @ 0x1dd79737 in /usr/bin/clickhouse
            ],
        },
        'MsgPack': {
            'data_sample': [
                b'\x00\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01',
                b'\x01\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x02\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x03\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x04\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x05\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x06\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x07\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x08\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x09\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0a\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0b\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0c\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0d\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0e\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0f\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01',
                b'\x00\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01',
                # ''
                # On empty message exception happens: Unexpected end of file while parsing msgpack object.: (at row 1)
                # coming from Processors/Formats/Impl/MsgPackRowInputFormat.cpp:170
            ],
        },
        'RowBinary': {
            'data_sample': [
                b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                # ''
                # On empty message exception happens: DB::Exception: Cannot read all data. Bytes read: 0. Bytes expected: 8.
                # /src/IO/ReadBuffer.h:157: DB::ReadBuffer::readStrict(char*, unsigned long) @ 0x15c6894d in /usr/bin/clickhouse
                # /src/IO/ReadHelpers.h:108: void DB::readPODBinary<long>(long&, DB::ReadBuffer&) @ 0x15c67715 in /usr/bin/clickhouse
                # /src/IO/ReadHelpers.h:737: std::__1::enable_if<is_arithmetic_v<long>, void>::type DB::readBinary<long>(long&, DB::ReadBuffer&) @ 0x15e7afbd in /usr/bin/clickhouse
                # /src/DataTypes/DataTypeNumberBase.cpp:180: DB::DataTypeNumberBase<long>::deserializeBinary(DB::IColumn&, DB::ReadBuffer&) const @ 0x1cace581 in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/BinaryRowInputFormat.cpp:22: DB::BinaryRowInputFormat::readRow(std::__1::vector<COW<DB::IColumn>::mutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::mutable_ptr<DB::IColumn> > >&, DB::RowReadExtension&) @ 0x1dea2c0b in /usr/bin/clickhouse
            ],
        },
        'RowBinaryWithNamesAndTypes': {
            'data_sample': [
                b'\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                b'\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                b'\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                # ''
                # !!! On empty message segfault: Address not mapped to object
                # /contrib/FastMemcpy/FastMemcpy.h:666: memcpy_fast @ 0x21742d65 in /usr/bin/clickhouse
                # /contrib/FastMemcpy/memcpy_wrapper.c:5: memcpy @ 0x21738235 in /usr/bin/clickhouse
                # /src/IO/ReadBuffer.h:145: DB::ReadBuffer::read(char*, unsigned long) @ 0x15c369d7 in /usr/bin/clickhouse
                # /src/IO/ReadBuffer.h:155: DB::ReadBuffer::readStrict(char*, unsigned long) @ 0x15c68878 in /usr/bin/clickhouse
                # /src/DataTypes/DataTypeString.cpp:84: DB::DataTypeString::deserializeBinary(DB::IColumn&, DB::ReadBuffer&) const @ 0x1cad12e7 in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/BinaryRowInputFormat.cpp:22: DB::BinaryRowInputFormat::readRow(std::__1::vector<COW<DB::IColumn>::mutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::mutable_ptr<DB::IColumn> > >&, DB::RowReadExtension&) @ 0x1dea2c0b in /usr/bin/clickhouse
            ],
        },
        'Protobuf': {
            'data_sample': [
                b'\x0b\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01',
                b'\x0d\x08\x01\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x02\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x03\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x04\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x05\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x06\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x07\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x08\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x09\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0a\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0b\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0c\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0d\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0e\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0f\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01',
                b'\x0b\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01',
                # ''
                # On empty message exception: Attempt to read after eof
                # /src/IO/ReadBuffer.h:184: DB::ReadBuffer::throwReadAfterEOF() @ 0x15c9699b in /usr/bin/clickhouse
                # /src/Formats/ProtobufReader.h:115: DB::ProtobufReader::SimpleReader::startMessage() @ 0x1df4f828 in /usr/bin/clickhouse
                # /src/Formats/ProtobufReader.cpp:1119: DB::ProtobufReader::startMessage() @ 0x1df5356c in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/ProtobufRowInputFormat.cpp:25: DB::ProtobufRowInputFormat::readRow(std::__1::vector<COW<DB::IColumn>::mutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::mutable_ptr<DB::IColumn> > >&, DB::RowReadExtension&) @ 0x1df4cc71 in /usr/bin/clickhouse
                # /src/Processors/Formats/IRowInputFormat.cpp:64: DB::IRowInputFormat::generate() @ 0x1de727cf in /usr/bin/clickhouse
            ],
            'extra_settings': ", kafka_schema='test:TestMessage'"
        },
        'ORC': {
            'data_sample': [
                b'\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x01\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x46\x25\x0e\x2e\x46\x03\x21\x46\x03\x09\xa6\x00\x06\x00\x32\x00\x00\xe3\x92\xe4\x62\x65\x00\x01\x21\x01\x0e\x46\x25\x2e\x2e\x26\x47\x5f\x21\x20\x96\x60\x09\x60\x00\x00\x36\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x46\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x10\x11\xc0\x00\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x05\x00\x00\xff\x00\x03\x00\x00\x30\x07\x00\x00\x40\x00\x80\x05\x00\x00\x41\x4d\x07\x00\x00\x42\x00\x80\x03\x00\x00\x0a\x07\x00\x00\x42\x00\x80\x05\x00\x00\xff\x01\x88\x00\x00\x4d\xca\xc1\x0a\x80\x30\x0c\x03\xd0\x2e\x6b\xcb\x98\x17\xf1\x14\x50\xfc\xff\xcf\xb4\x66\x1e\x3c\x84\x47\x9a\xce\x1c\xb9\x1b\xb7\xf9\xda\x48\x09\x9e\xb2\xf3\x92\xce\x5b\x86\xf6\x56\x7f\x21\x41\x2f\x51\xa6\x7a\xd7\x1d\xe5\xea\xae\x3d\xca\xd5\x83\x71\x60\xd8\x17\xfc\x62\x0f\xa8\x00\x00\xe3\x4a\xe6\x62\xe1\x60\x0c\x60\xe0\xe2\xe3\x60\x14\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\x60\x54\xe2\xe0\x62\x34\x10\x62\x34\x90\x60\x02\x8a\x70\x71\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\x82\x05\x28\xc6\xcd\x25\xca\xc1\x68\xc4\x0b\x52\xc5\x6c\xa0\x67\x2a\x05\x22\xc0\x4a\x21\x86\x31\x09\x30\x81\xb5\xb2\x02\x00\x36\x01\x00\x25\x8c\xbd\x0a\xc2\x30\x14\x85\x73\x6f\x92\xf6\x92\x6a\x09\x01\x21\x64\x92\x4e\x75\x91\x58\x71\xc9\x64\x27\x5d\x2c\x1d\x5d\xfd\x59\xc4\x42\x37\x5f\xc0\x17\xe8\x23\x9b\xc6\xe1\x3b\x70\x0f\xdf\xb9\xc4\xf5\x17\x5d\x41\x5c\x4f\x60\x37\xeb\x53\x0d\x55\x4d\x0b\x23\x01\xb9\x90\x2e\xbf\x0f\xe3\xe3\xdd\x8d\x0e\x5f\x4f\x27\x3e\xb7\x61\x97\xb2\x49\xb9\xaf\x90\x20\x92\x27\x32\x2a\x6b\xf4\xf3\x0d\x1e\x82\x20\xe8\x59\x28\x09\x4c\x46\x4c\x33\xcb\x7a\x76\x95\x41\x47\x9f\x14\x78\x03\xde\x62\x6c\x54\x30\xb1\x51\x0a\xdb\x8b\x89\x58\x11\xbb\x22\xac\x08\x9a\xe5\x6c\x71\xbf\x3d\xb8\x39\x92\xfa\x7f\x86\x1a\xd3\x54\x1e\xa7\xee\xcc\x7e\x08\x9e\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x57\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18',
                b'\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x0f\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x0f\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x7e\x25\x0e\x2e\x46\x43\x21\x46\x4b\x09\xad\x00\x06\x00\x33\x00\x00\x0a\x17\x0a\x03\x00\x00\x00\x12\x10\x08\x0f\x22\x0a\x0a\x02\x41\x4d\x12\x02\x41\x4d\x18\x3c\x50\x00\x3a\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x7e\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x66\x73\x3d\xd3\x00\x06\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x0f\x12\x06\x08\x02\x10\x02\x18\x1e\x50\x00\x05\x00\x00\x0c\x00\x2b\x00\x00\x31\x32\x33\x34\x35\x36\x37\x38\x39\x31\x30\x31\x31\x31\x32\x31\x33\x31\x34\x31\x35\x09\x00\x00\x06\x01\x03\x02\x09\x00\x00\xc0\x0e\x00\x00\x07\x00\x00\x42\x00\x80\x05\x00\x00\x41\x4d\x0a\x00\x00\xe3\xe2\x42\x01\x00\x09\x00\x00\xc0\x0e\x02\x00\x05\x00\x00\x0c\x01\x94\x00\x00\x2d\xca\xc1\x0e\x80\x30\x08\x03\xd0\xc1\x60\x2e\xf3\x62\x76\x6a\xe2\x0e\xfe\xff\x57\x5a\x3b\x0f\xe4\x51\xe8\x68\xbd\x5d\x05\xe7\xf8\x34\x40\x3a\x6e\x59\xb1\x64\xe0\x91\xa9\xbf\xb1\x97\xd2\x95\x9d\x1e\xca\x55\x3a\x6d\xb4\xd2\xdd\x0b\x74\x9a\x74\xf7\x12\x39\xbd\x97\x7f\x7c\x06\xbb\xa6\x8d\x97\x17\xb4\x00\x00\xe3\x4a\xe6\x62\xe1\xe0\x0f\x60\xe0\xe2\xe3\xe0\x17\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\xe0\x57\xe2\xe0\x62\x34\x14\x62\xb4\x94\xd0\x02\x8a\xc8\x73\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\xc2\x06\x28\x26\xc4\x25\xca\xc1\x6f\xc4\xcb\xc5\x68\x20\xc4\x6c\xa0\x67\x2a\xc5\x6c\xae\x67\x0a\x14\xe6\x87\x1a\xc6\x24\xc0\x24\x21\x07\x32\x0c\x00\x4a\x01\x00\xe3\x60\x16\x58\xc3\x24\xc5\xcd\xc1\x2c\x30\x89\x51\xc2\x4b\xc1\x57\x83\x5f\x49\x83\x83\x47\x88\x95\x91\x89\x99\x85\x55\x8a\x3d\x29\x27\x3f\x39\xdb\x2f\x5f\x8a\x29\x33\x45\x8a\xa5\x2c\x31\xc7\x10\x4c\x1a\x81\x49\x63\x25\x26\x0e\x46\x20\x66\x07\x63\x36\x0e\x3e\x0d\x26\x03\x10\x9f\xd1\x80\xdf\x8a\x85\x83\x3f\x80\xc1\x8a\x8f\x83\x5f\x88\x8d\x83\x41\x80\x41\x82\x21\x80\x21\x82\xd5\x4a\x80\x83\x5f\x89\x83\x8b\xd1\x50\x88\xd1\x52\x42\x0b\x28\x22\x6f\x25\x04\x14\xe1\xe2\x62\x72\xf4\x15\x02\x62\x09\x1b\xa0\x98\x90\x95\x28\x07\xbf\x11\x2f\x17\xa3\x81\x10\xb3\x81\x9e\xa9\x14\xb3\xb9\x9e\x29\x50\x98\x1f\x6a\x18\x93\x00\x93\x84\x1c\xc8\x30\x87\x09\x7e\x1e\x0c\x00\x08\xa8\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x5d\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18',
                b'\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x01\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x46\x25\x0e\x2e\x46\x03\x21\x46\x03\x09\xa6\x00\x06\x00\x32\x00\x00\xe3\x92\xe4\x62\x65\x00\x01\x21\x01\x0e\x46\x25\x2e\x2e\x26\x47\x5f\x21\x20\x96\x60\x09\x60\x00\x00\x36\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x46\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x10\x11\xc0\x00\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x05\x00\x00\xff\x00\x03\x00\x00\x30\x07\x00\x00\x40\x00\x80\x05\x00\x00\x41\x4d\x07\x00\x00\x42\x00\x80\x03\x00\x00\x0a\x07\x00\x00\x42\x00\x80\x05\x00\x00\xff\x01\x88\x00\x00\x4d\xca\xc1\x0a\x80\x30\x0c\x03\xd0\x2e\x6b\xcb\x98\x17\xf1\x14\x50\xfc\xff\xcf\xb4\x66\x1e\x3c\x84\x47\x9a\xce\x1c\xb9\x1b\xb7\xf9\xda\x48\x09\x9e\xb2\xf3\x92\xce\x5b\x86\xf6\x56\x7f\x21\x41\x2f\x51\xa6\x7a\xd7\x1d\xe5\xea\xae\x3d\xca\xd5\x83\x71\x60\xd8\x17\xfc\x62\x0f\xa8\x00\x00\xe3\x4a\xe6\x62\xe1\x60\x0c\x60\xe0\xe2\xe3\x60\x14\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\x60\x54\xe2\xe0\x62\x34\x10\x62\x34\x90\x60\x02\x8a\x70\x71\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\x82\x05\x28\xc6\xcd\x25\xca\xc1\x68\xc4\x0b\x52\xc5\x6c\xa0\x67\x2a\x05\x22\xc0\x4a\x21\x86\x31\x09\x30\x81\xb5\xb2\x02\x00\x36\x01\x00\x25\x8c\xbd\x0a\xc2\x30\x14\x85\x73\x6f\x92\xf6\x92\x6a\x09\x01\x21\x64\x92\x4e\x75\x91\x58\x71\xc9\x64\x27\x5d\x2c\x1d\x5d\xfd\x59\xc4\x42\x37\x5f\xc0\x17\xe8\x23\x9b\xc6\xe1\x3b\x70\x0f\xdf\xb9\xc4\xf5\x17\x5d\x41\x5c\x4f\x60\x37\xeb\x53\x0d\x55\x4d\x0b\x23\x01\xb9\x90\x2e\xbf\x0f\xe3\xe3\xdd\x8d\x0e\x5f\x4f\x27\x3e\xb7\x61\x97\xb2\x49\xb9\xaf\x90\x20\x92\x27\x32\x2a\x6b\xf4\xf3\x0d\x1e\x82\x20\xe8\x59\x28\x09\x4c\x46\x4c\x33\xcb\x7a\x76\x95\x41\x47\x9f\x14\x78\x03\xde\x62\x6c\x54\x30\xb1\x51\x0a\xdb\x8b\x89\x58\x11\xbb\x22\xac\x08\x9a\xe5\x6c\x71\xbf\x3d\xb8\x39\x92\xfa\x7f\x86\x1a\xd3\x54\x1e\xa7\xee\xcc\x7e\x08\x9e\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x57\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18',
                # ''
                # On empty message exception:  IOError: File size too small, Stack trace (when copying this message, always include the lines below):
                # /src/Processors/Formats/Impl/ORCBlockInputFormat.cpp:36: DB::ORCBlockInputFormat::generate() @ 0x1df282a6 in /usr/bin/clickhouse
                # /src/Processors/ISource.cpp:48: DB::ISource::work() @ 0x1dd79737 in /usr/bin/clickhouse
            ],
        },
        'CapnProto': {
            'data_sample': [
                b'\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00',
                b'\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00',
                b'\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00',
                # ''
                # On empty message exception: Cannot read all data. Bytes read: 0. Bytes expected: 4.
                # /src/IO/ReadBuffer.h:157: DB::ReadBuffer::readStrict(char*, unsigned long) @ 0x15c6894d in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/CapnProtoRowInputFormat.cpp:212: DB::CapnProtoRowInputFormat::readMessage() @ 0x1ded1cab in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/CapnProtoRowInputFormat.cpp:241: DB::CapnProtoRowInputFormat::readRow(std::__1::vector<COW<DB::IColumn>::mutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::mutable_ptr<DB::IColumn> > >&, DB::RowReadExtension&) @ 0x1ded205d in /usr/bin/clickhouse
            ],
            'extra_settings': ", kafka_schema='test:TestRecordStruct'"
        },
        'Parquet' : {
            'data_sample': [
                 b'\x50\x41\x52\x31\x15\x04\x15\x10\x15\x14\x4c\x15\x02\x15\x04\x12\x00\x00\x08\x1c\x00\x00\x00\x00\x00\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xbc\x01\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x02\x16\xac\x01\x16\xb4\x01\x26\x38\x26\x08\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xc8\x03\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xfc\x02\x26\xd4\x02\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x0c\x15\x10\x4c\x15\x02\x15\x04\x12\x00\x00\x06\x14\x02\x00\x00\x00\x41\x4d\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x03\x08\x01\x02\x00\x26\xa2\x05\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x02\x16\x68\x16\x70\x26\xde\x04\x26\xb2\x04\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x3f\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x03\x08\x01\x02\x00\x26\x8a\x07\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x02\x16\x84\x01\x16\x8c\x01\x26\xa6\x06\x26\xfe\x05\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x01\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xfe\x08\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xb2\x08\x26\x8a\x08\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x15\x02\x19\x6c\x35\x00\x18\x06\x73\x63\x68\x65\x6d\x61\x15\x0a\x00\x15\x04\x25\x00\x18\x02\x69\x64\x00\x15\x02\x25\x00\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x25\x18\x4c\xac\x13\x10\x12\x00\x00\x00\x15\x0c\x25\x00\x18\x04\x76\x61\x6c\x31\x25\x00\x4c\x1c\x00\x00\x00\x15\x08\x25\x00\x18\x04\x76\x61\x6c\x32\x00\x15\x02\x25\x00\x18\x04\x76\x61\x6c\x33\x25\x16\x4c\xac\x13\x08\x12\x00\x00\x00\x16\x02\x19\x1c\x19\x5c\x26\xbc\x01\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x02\x16\xac\x01\x16\xb4\x01\x26\x38\x26\x08\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x26\xc8\x03\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xfc\x02\x26\xd4\x02\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x26\xa2\x05\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x02\x16\x68\x16\x70\x26\xde\x04\x26\xb2\x04\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x26\x8a\x07\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x02\x16\x84\x01\x16\x8c\x01\x26\xa6\x06\x26\xfe\x05\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x26\xfe\x08\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xb2\x08\x26\x8a\x08\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x16\x98\x05\x16\x02\x00\x28\x22\x70\x61\x72\x71\x75\x65\x74\x2d\x63\x70\x70\x20\x76\x65\x72\x73\x69\x6f\x6e\x20\x31\x2e\x35\x2e\x31\x2d\x53\x4e\x41\x50\x53\x48\x4f\x54\x19\x5c\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x00\xc4\x01\x00\x00\x50\x41\x52\x31',
                 b'\x50\x41\x52\x31\x15\x04\x15\xf0\x01\x15\x90\x01\x4c\x15\x1e\x15\x04\x12\x00\x00\x78\x04\x01\x00\x09\x01\x00\x02\x09\x07\x04\x00\x03\x0d\x08\x00\x04\x0d\x08\x00\x05\x0d\x08\x00\x06\x0d\x08\x00\x07\x0d\x08\x00\x08\x0d\x08\x00\x09\x0d\x08\x00\x0a\x0d\x08\x00\x0b\x0d\x08\x00\x0c\x0d\x08\x00\x0d\x0d\x08\x3c\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x15\x00\x15\x14\x15\x18\x2c\x15\x1e\x15\x04\x15\x06\x15\x06\x1c\x18\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x24\x04\x05\x10\x32\x54\x76\x98\xba\xdc\x0e\x26\xca\x02\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x1e\x16\x9e\x03\x16\xc2\x02\x26\xb8\x01\x26\x08\x1c\x18\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x1e\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x03\x08\x01\x1e\x00\x26\xd8\x04\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x1e\x16\x6c\x16\x74\x26\x8c\x04\x26\xe4\x03\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x0c\x15\x10\x4c\x15\x02\x15\x04\x12\x00\x00\x06\x14\x02\x00\x00\x00\x41\x4d\x15\x00\x15\x06\x15\x0a\x2c\x15\x1e\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x03\x08\x01\x1e\x00\x26\xb2\x06\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x1e\x16\x68\x16\x70\x26\xee\x05\x26\xc2\x05\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x3f\x15\x00\x15\x06\x15\x0a\x2c\x15\x1e\x15\x04\x15\x06\x15\x06\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x03\x08\x01\x1e\x00\x26\x9a\x08\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x1e\x16\x84\x01\x16\x8c\x01\x26\xb6\x07\x26\x8e\x07\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x01\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x1e\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x03\x08\x01\x1e\x00\x26\x8e\x0a\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x1e\x16\x6c\x16\x74\x26\xc2\x09\x26\x9a\x09\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x15\x02\x19\x6c\x35\x00\x18\x06\x73\x63\x68\x65\x6d\x61\x15\x0a\x00\x15\x04\x25\x00\x18\x02\x69\x64\x00\x15\x02\x25\x00\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x25\x18\x4c\xac\x13\x10\x12\x00\x00\x00\x15\x0c\x25\x00\x18\x04\x76\x61\x6c\x31\x25\x00\x4c\x1c\x00\x00\x00\x15\x08\x25\x00\x18\x04\x76\x61\x6c\x32\x00\x15\x02\x25\x00\x18\x04\x76\x61\x6c\x33\x25\x16\x4c\xac\x13\x08\x12\x00\x00\x00\x16\x1e\x19\x1c\x19\x5c\x26\xca\x02\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x1e\x16\x9e\x03\x16\xc2\x02\x26\xb8\x01\x26\x08\x1c\x18\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x26\xd8\x04\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x1e\x16\x6c\x16\x74\x26\x8c\x04\x26\xe4\x03\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x26\xb2\x06\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x1e\x16\x68\x16\x70\x26\xee\x05\x26\xc2\x05\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x26\x9a\x08\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x1e\x16\x84\x01\x16\x8c\x01\x26\xb6\x07\x26\x8e\x07\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x26\x8e\x0a\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x1e\x16\x6c\x16\x74\x26\xc2\x09\x26\x9a\x09\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x16\xa6\x06\x16\x1e\x00\x28\x22\x70\x61\x72\x71\x75\x65\x74\x2d\x63\x70\x70\x20\x76\x65\x72\x73\x69\x6f\x6e\x20\x31\x2e\x35\x2e\x31\x2d\x53\x4e\x41\x50\x53\x48\x4f\x54\x19\x5c\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x00\xc5\x01\x00\x00\x50\x41\x52\x31',
                 b'\x50\x41\x52\x31\x15\x04\x15\x10\x15\x14\x4c\x15\x02\x15\x04\x12\x00\x00\x08\x1c\x00\x00\x00\x00\x00\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xbc\x01\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x02\x16\xac\x01\x16\xb4\x01\x26\x38\x26\x08\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xc8\x03\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xfc\x02\x26\xd4\x02\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x0c\x15\x10\x4c\x15\x02\x15\x04\x12\x00\x00\x06\x14\x02\x00\x00\x00\x41\x4d\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x03\x08\x01\x02\x00\x26\xa2\x05\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x02\x16\x68\x16\x70\x26\xde\x04\x26\xb2\x04\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x3f\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x03\x08\x01\x02\x00\x26\x8a\x07\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x02\x16\x84\x01\x16\x8c\x01\x26\xa6\x06\x26\xfe\x05\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x01\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xfe\x08\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xb2\x08\x26\x8a\x08\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x15\x02\x19\x6c\x35\x00\x18\x06\x73\x63\x68\x65\x6d\x61\x15\x0a\x00\x15\x04\x25\x00\x18\x02\x69\x64\x00\x15\x02\x25\x00\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x25\x18\x4c\xac\x13\x10\x12\x00\x00\x00\x15\x0c\x25\x00\x18\x04\x76\x61\x6c\x31\x25\x00\x4c\x1c\x00\x00\x00\x15\x08\x25\x00\x18\x04\x76\x61\x6c\x32\x00\x15\x02\x25\x00\x18\x04\x76\x61\x6c\x33\x25\x16\x4c\xac\x13\x08\x12\x00\x00\x00\x16\x02\x19\x1c\x19\x5c\x26\xbc\x01\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x02\x16\xac\x01\x16\xb4\x01\x26\x38\x26\x08\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x26\xc8\x03\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xfc\x02\x26\xd4\x02\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x26\xa2\x05\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x02\x16\x68\x16\x70\x26\xde\x04\x26\xb2\x04\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x26\x8a\x07\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x02\x16\x84\x01\x16\x8c\x01\x26\xa6\x06\x26\xfe\x05\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x26\xfe\x08\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xb2\x08\x26\x8a\x08\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x16\x98\x05\x16\x02\x00\x28\x22\x70\x61\x72\x71\x75\x65\x74\x2d\x63\x70\x70\x20\x76\x65\x72\x73\x69\x6f\x6e\x20\x31\x2e\x35\x2e\x31\x2d\x53\x4e\x41\x50\x53\x48\x4f\x54\x19\x5c\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x00\xc4\x01\x00\x00\x50\x41\x52\x31',
            ],
        },
        'AvroConfluent': {
            'data_sample': [
                avro_confluent_message(schema_registry_client,
                                       {'id': 0, 'blockNo': 0, 'val1': str('AM'), 'val2': 0.5, "val3": 1}),

                b''.join([avro_confluent_message(schema_registry_client,
                                                              {'id': id, 'blockNo': 0, 'val1': str('AM'),
                                                               'val2': 0.5, "val3": 1}) for id in range(1, 16)]),

                avro_confluent_message(schema_registry_client,
                                       {'id': 0, 'blockNo': 0, 'val1': str('AM'), 'val2': 0.5, "val3": 1}),
            ],
            'extra_settings': ", format_avro_schema_registry_url='http://{}:{}'".format(
                kafka_cluster.schema_registry_host,
                8081
            ),
            'supports_empty_value': True,
        },
        'Avro': {
            # It seems impossible to send more than one avro file per a message
            #   because of nature of Avro: blocks go one after another
            'data_sample': [
                avro_message({'id': 0, 'blockNo': 0, 'val1': str('AM'), 'val2': 0.5, "val3": 1}),

                avro_message([{'id': id, 'blockNo': 0, 'val1': str('AM'),
                                                               'val2': 0.5, "val3": 1} for id in range(1, 16)]),

                avro_message({'id': 0, 'blockNo': 0, 'val1': str('AM'), 'val2': 0.5, "val3": 1}),
            ],
            'supports_empty_value': False,
        },
        'Arrow' : {
            'data_sample' : [
                b'\x41\x52\x52\x4f\x57\x31\x00\x00\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x18\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00\x10\x00\x00\x00\x0c\x00\x14\x00\x06\x00\x08\x00\x0c\x00\x10\x00\x0c\x00\x00\x00\x00\x00\x03\x00\x3c\x00\x00\x00\x28\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x58\x01\x00\x00\x00\x00\x00\x00\x60\x01\x00\x00\x00\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\x78\x01\x00\x00\x41\x52\x52\x4f\x57\x31',
                b'\x41\x52\x52\x4f\x57\x31\x00\x00\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x48\x01\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x98\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x98\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\xd8\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\xf8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf8\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x38\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x38\x01\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x06\x00\x00\x00\x08\x00\x00\x00\x0a\x00\x00\x00\x0c\x00\x00\x00\x0e\x00\x00\x00\x10\x00\x00\x00\x12\x00\x00\x00\x14\x00\x00\x00\x16\x00\x00\x00\x18\x00\x00\x00\x1a\x00\x00\x00\x1c\x00\x00\x00\x1e\x00\x00\x00\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x00\xff\xff\xff\xff\x00\x00\x00\x00\x10\x00\x00\x00\x0c\x00\x14\x00\x06\x00\x08\x00\x0c\x00\x10\x00\x0c\x00\x00\x00\x00\x00\x03\x00\x3c\x00\x00\x00\x28\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x58\x01\x00\x00\x00\x00\x00\x00\x60\x01\x00\x00\x00\x00\x00\x00\x48\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\x78\x01\x00\x00\x41\x52\x52\x4f\x57\x31',
                b'\x41\x52\x52\x4f\x57\x31\x00\x00\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x18\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00\x10\x00\x00\x00\x0c\x00\x14\x00\x06\x00\x08\x00\x0c\x00\x10\x00\x0c\x00\x00\x00\x00\x00\x03\x00\x3c\x00\x00\x00\x28\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x58\x01\x00\x00\x00\x00\x00\x00\x60\x01\x00\x00\x00\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\x78\x01\x00\x00\x41\x52\x52\x4f\x57\x31',
            ],
        },
        'ArrowStream' : {
            'data_sample' : [
                b'\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x18\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00',
                b'\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x48\x01\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x98\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x98\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\xd8\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\xf8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf8\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x38\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x38\x01\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x06\x00\x00\x00\x08\x00\x00\x00\x0a\x00\x00\x00\x0c\x00\x00\x00\x0e\x00\x00\x00\x10\x00\x00\x00\x12\x00\x00\x00\x14\x00\x00\x00\x16\x00\x00\x00\x18\x00\x00\x00\x1a\x00\x00\x00\x1c\x00\x00\x00\x1e\x00\x00\x00\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x00\xff\xff\xff\xff\x00\x00\x00\x00',
                b'\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x18\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00',
            ],
        },
    }

    for format_name, format_opts in list(all_formats.items()):
        logging.debug(('Set up {}'.format(format_name)))
        topic_name = 'format_tests_{}'.format(format_name)
        data_sample = format_opts['data_sample']
        data_prefix = []
        # prepend empty value when supported
        if format_opts.get('supports_empty_value', False):
            data_prefix = data_prefix + ['']
        kafka_produce(kafka_cluster, topic_name, data_prefix + data_sample)
        instance.query('''
            DROP TABLE IF EXISTS test.kafka_{format_name};

            CREATE TABLE test.kafka_{format_name} (
                id Int64,
                blockNo UInt16,
                val1 String,
                val2 Float32,
                val3 UInt8
            ) ENGINE = Kafka()
                SETTINGS kafka_broker_list = 'kafka1:19092',
                        kafka_topic_list = '{topic_name}',
                        kafka_group_name = '{topic_name}_group',
                        kafka_format = '{format_name}',
                        kafka_flush_interval_ms = 1000 {extra_settings};

            DROP TABLE IF EXISTS test.kafka_{format_name}_mv;

            CREATE MATERIALIZED VIEW test.kafka_{format_name}_mv Engine=Log AS
                SELECT *, _topic, _partition, _offset FROM test.kafka_{format_name};
            '''.format(topic_name=topic_name, format_name=format_name,
                       extra_settings=format_opts.get('extra_settings') or ''))

    instance.wait_for_log_line('kafka.*Committed offset [0-9]+.*format_tests_', repetitions=len(all_formats.keys()), look_behind_lines=12000)

    for format_name, format_opts in list(all_formats.items()):
        logging.debug(('Checking {}'.format(format_name)))
        topic_name = 'format_tests_{}'.format(format_name)
        # shift offsets by 1 if format supports empty value
        offsets = [1, 2, 3] if format_opts.get('supports_empty_value', False) else [0, 1, 2]
        result = instance.query('SELECT * FROM test.kafka_{format_name}_mv;'.format(format_name=format_name))
        expected = '''\
0	0	AM	0.5	1	{topic_name}	0	{offset_0}
1	0	AM	0.5	1	{topic_name}	0	{offset_1}
2	0	AM	0.5	1	{topic_name}	0	{offset_1}
3	0	AM	0.5	1	{topic_name}	0	{offset_1}
4	0	AM	0.5	1	{topic_name}	0	{offset_1}
5	0	AM	0.5	1	{topic_name}	0	{offset_1}
6	0	AM	0.5	1	{topic_name}	0	{offset_1}
7	0	AM	0.5	1	{topic_name}	0	{offset_1}
8	0	AM	0.5	1	{topic_name}	0	{offset_1}
9	0	AM	0.5	1	{topic_name}	0	{offset_1}
10	0	AM	0.5	1	{topic_name}	0	{offset_1}
11	0	AM	0.5	1	{topic_name}	0	{offset_1}
12	0	AM	0.5	1	{topic_name}	0	{offset_1}
13	0	AM	0.5	1	{topic_name}	0	{offset_1}
14	0	AM	0.5	1	{topic_name}	0	{offset_1}
15	0	AM	0.5	1	{topic_name}	0	{offset_1}
0	0	AM	0.5	1	{topic_name}	0	{offset_2}
'''.format(topic_name=topic_name, offset_0=offsets[0], offset_1=offsets[1], offset_2=offsets[2])
        assert TSV(result) == TSV(expected), 'Proper result for format: {}'.format(format_name)


# Since everything is async and shaky when receiving messages from Kafka,
# we may want to try and check results multiple times in a loop.
def kafka_check_result(result, check=False, ref_file='test_kafka_json.reference'):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)


# https://stackoverflow.com/a/57692111/1555175
def describe_consumer_group(kafka_cluster, name):
    client = BrokerConnection('localhost', kafka_cluster.kafka_port, socket.AF_INET)
    client.connect_blocking()

    list_members_in_groups = DescribeGroupsRequest_v1(groups=[name])
    future = client.send(list_members_in_groups)
    while not future.is_done:
        for resp, f in client.recv():
            f.success(resp)

    (error_code, group_id, state, protocol_type, protocol, members) = future.value.groups[0]

    res = []
    for member in members:
        (member_id, client_id, client_host, member_metadata, member_assignment) = member
        member_info = {}
        member_info['member_id'] = member_id
        member_info['client_id'] = client_id
        member_info['client_host'] = client_host
        member_topics_assignment = []
        for (topic, partitions) in MemberAssignment.decode(member_assignment).assignment:
            member_topics_assignment.append({'topic': topic, 'partitions': partitions})
        member_info['assignment'] = member_topics_assignment
        res.append(member_info)
    return res


# Fixtures

@pytest.fixture(scope="module")
def kafka_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query('DROP DATABASE IF EXISTS test; CREATE DATABASE test;')
    # logging.debug("kafka is available - running test")
    yield  # run test


# Tests

def test_kafka_settings_old_syntax(kafka_cluster):
    assert TSV(instance.query("SELECT * FROM system.macros WHERE macro like 'kafka%' ORDER BY macro",
                              ignore_error=True)) == TSV('''kafka_broker	kafka1
kafka_client_id	instance
kafka_format_json_each_row	JSONEachRow
kafka_group_name_new	new
kafka_group_name_old	old
kafka_topic_new	new
kafka_topic_old	old
''')

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka('{kafka_broker}:19092', '{kafka_topic_old}', '{kafka_group_name_old}', '{kafka_format_json_each_row}', '\\n');
        ''')

    # Don't insert malformed messages since old settings syntax
    # doesn't support skipping of broken messages.
    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce(kafka_cluster, 'old', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)

    members = describe_consumer_group(kafka_cluster, 'old')
    assert members[0]['client_id'] == 'ClickHouse-instance-test-kafka'
    # text_desc = kafka_cluster.exec_in_container(kafka_cluster.get_container_id('kafka1'),"kafka-consumer-groups --bootstrap-server localhost:{} --describe --members --group old --verbose".format(cluster.kafka_port)))


def test_kafka_settings_new_syntax(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = '{kafka_broker}:19092',
                     kafka_topic_list = '{kafka_topic_new}',
                     kafka_group_name = '{kafka_group_name_new}',
                     kafka_format = '{kafka_format_json_each_row}',
                     kafka_row_delimiter = '\\n',
                     kafka_client_id = '{kafka_client_id} test 1234',
                     kafka_skip_broken_messages = 1;
        ''')

    messages = []
    for i in range(25):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce(kafka_cluster, 'new', messages)

    # Insert couple of malformed messages.
    kafka_produce(kafka_cluster, 'new', ['}{very_broken_message,'])
    kafka_produce(kafka_cluster, 'new', ['}another{very_broken_message,'])

    messages = []
    for i in range(25, 50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce(kafka_cluster, 'new', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)

    members = describe_consumer_group(kafka_cluster, 'new')
    assert members[0]['client_id'] == 'instance test 1234'


def test_kafka_issue11308(kafka_cluster):
    # Check that matview does respect Kafka SETTINGS
    kafka_produce(kafka_cluster, 'issue11308', ['{"t": 123, "e": {"x": "woof"} }', '{"t": 123, "e": {"x": "woof"} }',
                                 '{"t": 124, "e": {"x": "test"} }'])

    instance.query('''
        CREATE TABLE test.persistent_kafka (
            time UInt64,
            some_string String
        )
        ENGINE = MergeTree()
        ORDER BY time;

        CREATE TABLE test.kafka (t UInt64, `e.x` String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'issue11308',
                     kafka_group_name = 'issue11308',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n',
                     kafka_flush_interval_ms=1000,
                     input_format_import_nested_json = 1;

        CREATE MATERIALIZED VIEW test.persistent_kafka_mv TO test.persistent_kafka AS
        SELECT
            `t` AS `time`,
            `e.x` AS `some_string`
        FROM test.kafka;
        ''')

    while int(instance.query('SELECT count() FROM test.persistent_kafka')) < 3:
        time.sleep(1)

    result = instance.query('SELECT * FROM test.persistent_kafka ORDER BY time;')

    instance.query('''
        DROP TABLE test.persistent_kafka;
        DROP TABLE test.persistent_kafka_mv;
    ''')

    expected = '''\
123	woof
123	woof
124	test
'''
    assert TSV(result) == TSV(expected)


def test_kafka_issue4116(kafka_cluster):
    # Check that format_csv_delimiter parameter works now - as part of all available format settings.
    kafka_produce(kafka_cluster, 'issue4116', ['1|foo', '2|bar', '42|answer', '100|multi\n101|row\n103|message'])

    instance.query('''
        CREATE TABLE test.kafka (a UInt64, b String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'issue4116',
                     kafka_group_name = 'issue4116',
                     kafka_format = 'CSV',
                     kafka_row_delimiter = '\\n',
                     format_csv_delimiter = '|';
        ''')

    result = instance.query('SELECT * FROM test.kafka ORDER BY a;')

    expected = '''\
1	foo
2	bar
42	answer
100	multi
101	row
103	message
'''
    assert TSV(result) == TSV(expected)


def test_kafka_consumer_hang(kafka_cluster):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))

    topic_list = []
    topic_list.append(NewTopic(name="consumer_hang", num_partitions=8, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    instance.query('''
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'consumer_hang',
                     kafka_group_name = 'consumer_hang',
                     kafka_format = 'JSONEachRow',
                     kafka_num_consumers = 8;
        CREATE TABLE test.view (key UInt64, value UInt64) ENGINE = Memory();
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.kafka;
        ''')

    instance.wait_for_log_line('kafka.*Stalled', repetitions=20)

    # This should trigger heartbeat fail,
    # which will trigger REBALANCE_IN_PROGRESS,
    # and which can lead to consumer hang.
    kafka_cluster.pause_container('kafka1')
    instance.wait_for_log_line('heartbeat error')
    kafka_cluster.unpause_container('kafka1')

    # logging.debug("Attempt to drop")
    instance.query('DROP TABLE test.kafka')

    # kafka_cluster.open_bash_shell('instance')

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    # original problem appearance was a sequence of the following messages in librdkafka logs:
    # BROKERFAIL -> |ASSIGN| -> REBALANCE_IN_PROGRESS -> "waiting for rebalance_cb" (repeated forever)
    # so it was waiting forever while the application will execute queued rebalance callback

    # from a user perspective: we expect no hanging 'drop' queries
    # 'dr'||'op' to avoid self matching
    assert int(instance.query("select count() from system.processes where position(lower(query),'dr'||'op')>0")) == 0


def test_kafka_consumer_hang2(kafka_cluster):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))

    topic_list = []
    topic_list.append(NewTopic(name="consumer_hang2", num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    instance.query('''
        DROP TABLE IF EXISTS test.kafka;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'consumer_hang2',
                     kafka_group_name = 'consumer_hang2',
                     kafka_format = 'JSONEachRow';

        CREATE TABLE test.kafka2 (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'consumer_hang2',
                     kafka_group_name = 'consumer_hang2',
                     kafka_format = 'JSONEachRow';
        ''')

    # first consumer subscribe the topic, try to poll some data, and go to rest
    instance.query('SELECT * FROM test.kafka')

    # second consumer do the same leading to rebalance in the first
    # consumer, try to poll some data
    instance.query('SELECT * FROM test.kafka2')

    # echo 'SELECT * FROM test.kafka; SELECT * FROM test.kafka2; DROP TABLE test.kafka;' | clickhouse client -mn &
    #    kafka_cluster.open_bash_shell('instance')

    # first consumer has pending rebalance callback unprocessed (no poll after select)
    # one of those queries was failing because of
    # https://github.com/edenhill/librdkafka/issues/2077
    # https://github.com/edenhill/librdkafka/issues/2898
    instance.query('DROP TABLE test.kafka')
    instance.query('DROP TABLE test.kafka2')

    # from a user perspective: we expect no hanging 'drop' queries
    # 'dr'||'op' to avoid self matching
    assert int(instance.query("select count() from system.processes where position(lower(query),'dr'||'op')>0")) == 0


def test_kafka_csv_with_delimiter(kafka_cluster):
    messages = []
    for i in range(50):
        messages.append('{i}, {i}'.format(i=i))
    kafka_produce(kafka_cluster, 'csv', messages)

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'csv',
                     kafka_group_name = 'csv',
                     kafka_format = 'CSV';
        ''')

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


def test_kafka_tsv_with_delimiter(kafka_cluster):
    messages = []
    for i in range(50):
        messages.append('{i}\t{i}'.format(i=i))
    kafka_produce(kafka_cluster, 'tsv', messages)

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'tsv',
                     kafka_group_name = 'tsv',
                     kafka_format = 'TSV';
        ''')

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


def test_kafka_select_empty(kafka_cluster):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))
    topic_list = []
    topic_list.append(NewTopic(name="empty", num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    instance.query('''
        CREATE TABLE test.kafka (key UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'empty',
                     kafka_group_name = 'empty',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';
        ''')

    assert int(instance.query('SELECT count() FROM test.kafka')) == 0


def test_kafka_json_without_delimiter(kafka_cluster):
    messages = ''
    for i in range(25):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce(kafka_cluster, 'json', [messages])

    messages = ''
    for i in range(25, 50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce(kafka_cluster, 'json', [messages])

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'json',
                     kafka_group_name = 'json',
                     kafka_format = 'JSONEachRow';
        ''')

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


def test_kafka_protobuf(kafka_cluster):
    kafka_produce_protobuf_messages(kafka_cluster, 'pb', 0, 20)
    kafka_produce_protobuf_messages(kafka_cluster, 'pb', 20, 1)
    kafka_produce_protobuf_messages(kafka_cluster, 'pb', 21, 29)

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'pb',
                     kafka_group_name = 'pb',
                     kafka_format = 'Protobuf',
                     kafka_schema = 'kafka.proto:KeyValuePair';
        ''')

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


def test_kafka_string_field_on_first_position_in_protobuf(kafka_cluster):
# https://github.com/ClickHouse/ClickHouse/issues/12615
    kafka_produce_protobuf_social(kafka_cluster, 'string_field_on_first_position_in_protobuf', 0, 20)
    kafka_produce_protobuf_social(kafka_cluster, 'string_field_on_first_position_in_protobuf', 20, 1)
    kafka_produce_protobuf_social(kafka_cluster, 'string_field_on_first_position_in_protobuf', 21, 29)

    instance.query('''
CREATE TABLE test.kafka (
    username String,
    timestamp Int32
  ) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092',
    kafka_topic_list = 'string_field_on_first_position_in_protobuf',
    kafka_group_name = 'string_field_on_first_position_in_protobuf',
    kafka_format = 'Protobuf',
    kafka_schema = 'social:User';
        ''')

    result = instance.query('SELECT * FROM test.kafka', ignore_error=True)
    expected = '''\
John Doe 0	1000000
John Doe 1	1000001
John Doe 2	1000002
John Doe 3	1000003
John Doe 4	1000004
John Doe 5	1000005
John Doe 6	1000006
John Doe 7	1000007
John Doe 8	1000008
John Doe 9	1000009
John Doe 10	1000010
John Doe 11	1000011
John Doe 12	1000012
John Doe 13	1000013
John Doe 14	1000014
John Doe 15	1000015
John Doe 16	1000016
John Doe 17	1000017
John Doe 18	1000018
John Doe 19	1000019
John Doe 20	1000020
John Doe 21	1000021
John Doe 22	1000022
John Doe 23	1000023
John Doe 24	1000024
John Doe 25	1000025
John Doe 26	1000026
John Doe 27	1000027
John Doe 28	1000028
John Doe 29	1000029
John Doe 30	1000030
John Doe 31	1000031
John Doe 32	1000032
John Doe 33	1000033
John Doe 34	1000034
John Doe 35	1000035
John Doe 36	1000036
John Doe 37	1000037
John Doe 38	1000038
John Doe 39	1000039
John Doe 40	1000040
John Doe 41	1000041
John Doe 42	1000042
John Doe 43	1000043
John Doe 44	1000044
John Doe 45	1000045
John Doe 46	1000046
John Doe 47	1000047
John Doe 48	1000048
John Doe 49	1000049
'''
    assert TSV(result) == TSV(expected)

def test_kafka_protobuf_no_delimiter(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'pb_no_delimiter',
                     kafka_group_name = 'pb_no_delimiter',
                     kafka_format = 'ProtobufSingle',
                     kafka_schema = 'kafka.proto:KeyValuePair';
        ''')

    kafka_produce_protobuf_messages_no_delimeters(kafka_cluster, 'pb_no_delimiter', 0, 20)
    kafka_produce_protobuf_messages_no_delimeters(kafka_cluster, 'pb_no_delimiter', 20, 1)
    kafka_produce_protobuf_messages_no_delimeters(kafka_cluster, 'pb_no_delimiter', 21, 29)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)

    instance.query('''
    CREATE TABLE test.kafka_writer (key UInt64, value String)
        ENGINE = Kafka
        SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'pb_no_delimiter',
                    kafka_group_name = 'pb_no_delimiter',
                    kafka_format = 'ProtobufSingle',
                    kafka_schema = 'kafka.proto:KeyValuePair';
    ''')

    instance.query("INSERT INTO test.kafka_writer VALUES (13,'Friday'),(42,'Answer to the Ultimate Question of Life, the Universe, and Everything'), (110, 'just a number')")

    time.sleep(1)

    result = instance.query("SELECT * FROM test.kafka ORDER BY key", ignore_error=True)

    expected = '''\
13	Friday
42	Answer to the Ultimate Question of Life, the Universe, and Everything
110	just a number
'''
    assert TSV(result) == TSV(expected)



def test_kafka_materialized_view(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'mv',
                     kafka_group_name = 'mv',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce(kafka_cluster, 'mv', messages)

    while True:
        result = instance.query('SELECT * FROM test.view')
        if kafka_check_result(result):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    kafka_check_result(result, True)


def test_kafka_recreate_kafka_table(kafka_cluster):
    '''
        Checks that materialized view work properly after dropping and recreating the Kafka table.
    '''
    # line for backporting:
    # admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))

    topic_list = []
    topic_list.append(NewTopic(name="recreate_kafka_table", num_partitions=6, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'recreate_kafka_table',
                     kafka_group_name = 'recreate_kafka_table_group',
                     kafka_format = 'JSONEachRow',
                     kafka_num_consumers = 6,
                     kafka_flush_interval_ms = 1000,
                     kafka_skip_broken_messages = 1048577;

        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    messages = []
    for i in range(120):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce(kafka_cluster,'recreate_kafka_table', messages)

    instance.wait_for_log_line('kafka.*Committed offset [0-9]+.*recreate_kafka_table', repetitions=6, look_behind_lines=100)

    instance.query('''
        DROP TABLE test.kafka;
    ''')

    kafka_produce(kafka_cluster,'recreate_kafka_table', messages)

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'recreate_kafka_table',
                     kafka_group_name = 'recreate_kafka_table_group',
                     kafka_format = 'JSONEachRow',
                     kafka_num_consumers = 6,
                     kafka_flush_interval_ms = 1000,
                     kafka_skip_broken_messages = 1048577;
    ''')

    instance.wait_for_log_line('kafka.*Committed offset [0-9]+.*recreate_kafka_table', repetitions=6, look_behind_lines=100)

    # data was not flushed yet (it will be flushed 7.5 sec after creating MV)
    assert int(instance.query("SELECT count() FROM test.view")) == 240

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')


def test_librdkafka_compression(kafka_cluster):
    """
    Regression for UB in snappy-c (that is used in librdkafka),
    backport pr is [1].

      [1]: https://github.com/ClickHouse-Extras/librdkafka/pull/3

    Example of corruption:

        2020.12.10 09:59:56.831507 [ 20 ] {} <Error> void DB::StorageKafka::threadFunc(size_t): Code: 27, e.displayText() = DB::Exception: Cannot parse input: expected '"' before: 'foo"}': (while reading the value of key value): (at row 1)

    To trigger this regression there should duplicated messages

    Orignal reproducer is:
    $ gcc --version |& fgrep gcc
    gcc (GCC) 10.2.0
    $ yes foobarbaz | fold -w 80 | head -n10 >| in-…
    $ make clean && make CFLAGS='-Wall -g -O2 -ftree-loop-vectorize -DNDEBUG=1 -DSG=1 -fPIC'
    $ ./verify in
    final comparision of in failed at 20 of 100

    """

    supported_compression_types = ['gzip', 'snappy', 'lz4', 'zstd', 'uncompressed']

    messages = []
    expected = []

    value = 'foobarbaz'*10
    number_of_messages = 50
    for i in range(number_of_messages):
        messages.append(json.dumps({'key': i, 'value': value}))
        expected.append(f'{i}\t{value}')

    expected = '\n'.join(expected)

    for compression_type in supported_compression_types:
        logging.debug(('Check compression {}'.format(compression_type)))

        topic_name = 'test_librdkafka_compression_{}'.format(compression_type)
        admin_client = admin.AdminClient({'bootstrap.servers': "localhost:{}".format(kafka_cluster.kafka_port)})

        topic = admin.NewTopic(topic=topic_name, num_partitions=1, replication_factor=1, config={
            'compression.type': compression_type,
        })
        admin_client.create_topics(new_topics=[topic], validate_only=False)

        instance.query('''
            CREATE TABLE test.kafka (key UInt64, value String)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                        kafka_topic_list = '{topic_name}',
                        kafka_group_name = '{topic_name}_group',
                        kafka_format = 'JSONEachRow',
                        kafka_flush_interval_ms = 1000;
            CREATE MATERIALIZED VIEW test.consumer Engine=Log AS
                SELECT * FROM test.kafka;
        '''.format(topic_name=topic_name) )

        kafka_produce(kafka_cluster, topic_name, messages)

        instance.wait_for_log_line("Committed offset {}".format(number_of_messages))

        result = instance.query('SELECT * FROM test.consumer')
        assert TSV(result) == TSV(expected)

        instance.query('DROP TABLE test.kafka SYNC')
        instance.query('DROP TABLE test.consumer SYNC')

def test_kafka_materialized_view_with_subquery(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'mvsq',
                     kafka_group_name = 'mvsq',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.kafka);
    ''')

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce(kafka_cluster, 'mvsq', messages)

    while True:
        result = instance.query('SELECT * FROM test.view')
        if kafka_check_result(result):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    kafka_check_result(result, True)


def test_kafka_many_materialized_views(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view1;
        DROP TABLE IF EXISTS test.view2;
        DROP TABLE IF EXISTS test.consumer1;
        DROP TABLE IF EXISTS test.consumer2;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'mmv',
                     kafka_group_name = 'mmv',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view1 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer1 TO test.view1 AS
            SELECT * FROM test.kafka;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.kafka;
    ''')

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce(kafka_cluster, 'mmv', messages)

    while True:
        result1 = instance.query('SELECT * FROM test.view1')
        result2 = instance.query('SELECT * FROM test.view2')
        if kafka_check_result(result1) and kafka_check_result(result2):
            break

    instance.query('''
        DROP TABLE test.consumer1;
        DROP TABLE test.consumer2;
        DROP TABLE test.view1;
        DROP TABLE test.view2;
    ''')

    kafka_check_result(result1, True)
    kafka_check_result(result2, True)


def test_kafka_flush_on_big_message(kafka_cluster):
    # Create batchs of messages of size ~100Kb
    kafka_messages = 1000
    batch_messages = 1000
    messages = [json.dumps({'key': i, 'value': 'x' * 100}) * batch_messages for i in range(kafka_messages)]
    kafka_produce(kafka_cluster, 'flush', messages)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'flush',
                     kafka_group_name = 'flush',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 10;
        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))
    received = False
    while not received:
        try:
            offsets = client.list_consumer_group_offsets('flush')
            for topic, offset in list(offsets.items()):
                if topic.topic == 'flush' and offset.offset == kafka_messages:
                    received = True
                    break
        except kafka.errors.GroupCoordinatorNotAvailableError:
            continue

    while True:
        result = instance.query('SELECT count() FROM test.view')
        if int(result) == kafka_messages * batch_messages:
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert int(result) == kafka_messages * batch_messages, 'ClickHouse lost some messages: {}'.format(result)


def test_kafka_virtual_columns(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'virt1',
                     kafka_group_name = 'virt1',
                     kafka_format = 'JSONEachRow';
        ''')

    messages = ''
    for i in range(25):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce(kafka_cluster, 'virt1', [messages], 0)

    messages = ''
    for i in range(25, 50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce(kafka_cluster, 'virt1', [messages], 0)

    result = ''
    while True:
        result += instance.query(
            '''SELECT _key, key, _topic, value, _offset, _partition, _timestamp = 0 ? '0000-00-00 00:00:00' : toString(_timestamp) AS _timestamp FROM test.kafka''',
            ignore_error=True)
        if kafka_check_result(result, False, 'test_kafka_virtual1.reference'):
            break

    kafka_check_result(result, True, 'test_kafka_virtual1.reference')


def test_kafka_virtual_columns_with_materialized_view(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'virt2',
                     kafka_group_name = 'virt2',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64, kafka_key String, topic String, offset UInt64, partition UInt64, timestamp Nullable(DateTime('UTC')))
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT *, _key as kafka_key, _topic as topic, _offset as offset, _partition as partition, _timestamp = 0 ? '0000-00-00 00:00:00' : toString(_timestamp) as timestamp FROM test.kafka;
    ''')

    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce(kafka_cluster, 'virt2', messages, 0)

    while True:
        result = instance.query('SELECT kafka_key, key, topic, value, offset, partition, timestamp FROM test.view')
        if kafka_check_result(result, False, 'test_kafka_virtual2.reference'):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    kafka_check_result(result, True, 'test_kafka_virtual2.reference')


def test_kafka_insert(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'insert1',
                     kafka_group_name = 'insert1',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';
    ''')

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ','.join(values)

    while True:
        try:
            instance.query("INSERT INTO test.kafka VALUES {}".format(values))
            break
        except QueryRuntimeException as e:
            if 'Local: Timed out.' in str(e):
                continue
            else:
                raise

    messages = []
    while True:
        messages.extend(kafka_consume(kafka_cluster, 'insert1'))
        if len(messages) == 50:
            break

    result = '\n'.join(messages)
    kafka_check_result(result, True)


def test_kafka_produce_consume(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'insert2',
                     kafka_group_name = 'insert2',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    messages_num = 10000

    def insert():
        values = []
        for i in range(messages_num):
            values.append("({i}, {i})".format(i=i))
        values = ','.join(values)

        while True:
            try:
                instance.query("INSERT INTO test.kafka VALUES {}".format(values))
                break
            except QueryRuntimeException as e:
                if 'Local: Timed out.' in str(e):
                    continue
                else:
                    raise

    threads = []
    threads_num = 16
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result = instance.query('SELECT count() FROM test.view')
        time.sleep(1)
        if int(result) == messages_num * threads_num:
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    for thread in threads:
        thread.join()

    assert int(result) == messages_num * threads_num, 'ClickHouse lost some messages: {}'.format(result)


def test_kafka_commit_on_block_write(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'block',
                     kafka_group_name = 'block',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 100,
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    cancel = threading.Event()

    i = [0]

    def produce():
        while not cancel.is_set():
            messages = []
            for _ in range(101):
                messages.append(json.dumps({'key': i[0], 'value': i[0]}))
                i[0] += 1
            kafka_produce(kafka_cluster, 'block', messages)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()

    while int(instance.query('SELECT count() FROM test.view')) == 0:
        time.sleep(1)

    cancel.set()

    instance.query('''
        DROP TABLE test.kafka;
    ''')

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'block',
                     kafka_group_name = 'block',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 100,
                     kafka_row_delimiter = '\\n';
    ''')

    while int(instance.query('SELECT uniqExact(key) FROM test.view')) < i[0]:
        time.sleep(1)

    result = int(instance.query('SELECT count() == uniqExact(key) FROM test.view'))

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    kafka_thread.join()

    assert result == 1, 'Messages from kafka get duplicated!'


def test_kafka_virtual_columns2(kafka_cluster):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))
    topic_list = []
    topic_list.append(NewTopic(name="virt2_0", num_partitions=2, replication_factor=1))
    topic_list.append(NewTopic(name="virt2_1", num_partitions=2, replication_factor=1))

    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    instance.query('''
        CREATE TABLE test.kafka (value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'virt2_0,virt2_1',
                     kafka_group_name = 'virt2',
                     kafka_num_consumers = 2,
                     kafka_format = 'JSONEachRow';

        CREATE MATERIALIZED VIEW test.view Engine=Log AS
        SELECT value, _key, _topic, _partition, _offset, toUnixTimestamp(_timestamp), toUnixTimestamp64Milli(_timestamp_ms), _headers.name, _headers.value FROM test.kafka;
        ''')

    producer = KafkaProducer(bootstrap_servers="localhost:{}".format(cluster.kafka_port), value_serializer=producer_serializer, key_serializer=producer_serializer)

    producer.send(topic='virt2_0', value=json.dumps({'value': 1}), partition=0, key='k1', timestamp_ms=1577836801001,
                  headers=[('content-encoding', b'base64')])
    producer.send(topic='virt2_0', value=json.dumps({'value': 2}), partition=0, key='k2', timestamp_ms=1577836802002,
                  headers=[('empty_value', b''), ('', b'empty name'), ('', b''), ('repetition', b'1'), ('repetition', b'2')])
    producer.flush()

    producer.send(topic='virt2_0', value=json.dumps({'value': 3}), partition=1, key='k3', timestamp_ms=1577836803003,
                  headers=[('b', b'b'), ('a', b'a')])
    producer.send(topic='virt2_0', value=json.dumps({'value': 4}), partition=1, key='k4', timestamp_ms=1577836804004,
                  headers=[('a', b'a'), ('b', b'b')])
    producer.flush()

    producer.send(topic='virt2_1', value=json.dumps({'value': 5}), partition=0, key='k5', timestamp_ms=1577836805005)
    producer.send(topic='virt2_1', value=json.dumps({'value': 6}), partition=0, key='k6', timestamp_ms=1577836806006)
    producer.flush()

    producer.send(topic='virt2_1', value=json.dumps({'value': 7}), partition=1, key='k7', timestamp_ms=1577836807007)
    producer.send(topic='virt2_1', value=json.dumps({'value': 8}), partition=1, key='k8', timestamp_ms=1577836808008)
    producer.flush()

    instance.wait_for_log_line('kafka.*Committed offset 2.*virt2_[01]', repetitions=4, look_behind_lines=6000)

    members = describe_consumer_group(kafka_cluster, 'virt2')
    # pprint.pprint(members)
    members[0]['client_id'] = 'ClickHouse-instance-test-kafka-0'
    members[1]['client_id'] = 'ClickHouse-instance-test-kafka-1'

    result = instance.query("SELECT * FROM test.view ORDER BY value", ignore_error=True)

    expected = '''\
1	k1	virt2_0	0	0	1577836801	1577836801001	['content-encoding']	['base64']
2	k2	virt2_0	0	1	1577836802	1577836802002	['empty_value','','','repetition','repetition']	['','empty name','','1','2']
3	k3	virt2_0	1	0	1577836803	1577836803003	['b','a']	['b','a']
4	k4	virt2_0	1	1	1577836804	1577836804004	['a','b']	['a','b']
5	k5	virt2_1	0	0	1577836805	1577836805005	[]	[]
6	k6	virt2_1	0	1	1577836806	1577836806006	[]	[]
7	k7	virt2_1	1	0	1577836807	1577836807007	[]	[]
8	k8	virt2_1	1	1	1577836808	1577836808008	[]	[]
'''

    assert TSV(result) == TSV(expected)


def test_kafka_produce_key_timestamp(kafka_cluster):

    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))

    topic_list = []
    topic_list.append(NewTopic(name="insert3", num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka_writer (key UInt64, value UInt64, _key String, _timestamp DateTime('UTC'))
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'insert3',
                     kafka_group_name = 'insert3',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';

        CREATE TABLE test.kafka (key UInt64, value UInt64, inserted_key String, inserted_timestamp DateTime('UTC'))
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'insert3',
                     kafka_group_name = 'insert3',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';

        CREATE MATERIALIZED VIEW test.view Engine=Log AS
            SELECT key, value, inserted_key, toUnixTimestamp(inserted_timestamp), _key, _topic, _partition, _offset, toUnixTimestamp(_timestamp) FROM test.kafka;
    ''')

    instance.query("INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(1, 1, 'k1', 1577836801))
    instance.query("INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(2, 2, 'k2', 1577836802))
    instance.query(
        "INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({})),({},{},'{}',toDateTime({}))".format(3, 3,
                                                                                                              'k3',
                                                                                                              1577836803,
                                                                                                              4, 4,
                                                                                                              'k4',
                                                                                                              1577836804))
    instance.query("INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(5, 5, 'k5', 1577836805))

    instance.wait_for_log_line("Committed offset 5")

    result = instance.query("SELECT * FROM test.view ORDER BY value", ignore_error=True)

    # logging.debug(result)

    expected = '''\
1	1	k1	1577836801	k1	insert3	0	0	1577836801
2	2	k2	1577836802	k2	insert3	0	1	1577836802
3	3	k3	1577836803	k3	insert3	0	2	1577836803
4	4	k4	1577836804	k4	insert3	0	3	1577836804
5	5	k5	1577836805	k5	insert3	0	4	1577836805
'''

    assert TSV(result) == TSV(expected)


def test_kafka_flush_by_time(kafka_cluster):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))
    topic_list = []
    topic_list.append(NewTopic(name="flush_by_time", num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'flush_by_time',
                     kafka_group_name = 'flush_by_time',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 100,
                     kafka_row_delimiter = '\\n';

        SELECT * FROM test.kafka;

        CREATE TABLE test.view (key UInt64, value UInt64, ts DateTime64(3) MATERIALIZED now64(3))
            ENGINE = MergeTree()
            ORDER BY key;
    ''')

    cancel = threading.Event()

    def produce():
        while not cancel.is_set():
            messages = []
            messages.append(json.dumps({'key': 0, 'value': 0}))
            kafka_produce(kafka_cluster, 'flush_by_time', messages)
            time.sleep(0.8)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()

    instance.query('''
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    time.sleep(18)

    result = instance.query('SELECT uniqExact(ts) = 2, count() >= 15 FROM test.view')

    cancel.set()
    kafka_thread.join()

    # kafka_cluster.open_bash_shell('instance')

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert TSV(result) == TSV('1	1')


def test_kafka_flush_by_block_size(kafka_cluster):
    cancel = threading.Event()

    def produce():
        while not cancel.is_set():
            messages = []
            messages.append(json.dumps({'key': 0, 'value': 0}))
            kafka_produce(kafka_cluster, 'flush_by_block_size', messages)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'flush_by_block_size',
                     kafka_group_name = 'flush_by_block_size',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 100,
                     kafka_poll_max_batch_size = 1,
                     kafka_flush_interval_ms = 120000, /* should not flush by time during test */
                     kafka_row_delimiter = '\\n';

        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;

        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    # Wait for Kafka engine to consume this data
    while 1 != int(instance.query(
            "SELECT count() FROM system.parts WHERE database = 'test' AND table = 'view' AND name = 'all_1_1_0'")):
        time.sleep(0.5)

    cancel.set()
    kafka_thread.join()

    # more flushes can happens during test, we need to check only result of first flush (part named all_1_1_0).
    result = instance.query("SELECT count() FROM test.view WHERE _part='all_1_1_0'")
    # logging.debug(result)

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    # 100 = first poll should return 100 messages (and rows)
    # not waiting for stream_flush_interval_ms
    assert int(
        result) == 100, 'Messages from kafka should be flushed when block of size kafka_max_block_size is formed!'


def test_kafka_lot_of_partitions_partial_commit_of_bulk(kafka_cluster):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))

    topic_list = []
    topic_list.append(NewTopic(name="topic_with_multiple_partitions2", num_partitions=10, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'topic_with_multiple_partitions2',
                     kafka_group_name = 'topic_with_multiple_partitions2',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 211,
                     kafka_flush_interval_ms = 500;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    messages = []
    count = 0
    for dummy_msg in range(1000):
        rows = []
        for dummy_row in range(random.randrange(3, 10)):
            count = count + 1
            rows.append(json.dumps({'key': count, 'value': count}))
        messages.append("\n".join(rows))
    kafka_produce(kafka_cluster, 'topic_with_multiple_partitions2', messages)

    instance.wait_for_log_line('kafka.*Stalled', repetitions=5)

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.view')
    logging.debug(result)
    assert TSV(result) == TSV('{0}\t{0}\t{0}'.format(count))

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')


def test_kafka_rebalance(kafka_cluster):
    NUMBER_OF_CONSURRENT_CONSUMERS = 11

    instance.query('''
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination (
            key UInt64,
            value UInt64,
            _topic String,
            _key String,
            _offset UInt64,
            _partition UInt64,
            _timestamp Nullable(DateTime('UTC')),
            _consumed_by LowCardinality(String)
        )
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    # kafka_cluster.open_bash_shell('instance')

    # time.sleep(2)

    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))
    topic_list = []
    topic_list.append(NewTopic(name="topic_with_multiple_partitions", num_partitions=11, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    cancel = threading.Event()

    msg_index = [0]

    def produce():
        while not cancel.is_set():
            messages = []
            for _ in range(59):
                messages.append(json.dumps({'key': msg_index[0], 'value': msg_index[0]}))
                msg_index[0] += 1
            kafka_produce(kafka_cluster, 'topic_with_multiple_partitions', messages)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()

    for consumer_index in range(NUMBER_OF_CONSURRENT_CONSUMERS):
        table_name = 'kafka_consumer{}'.format(consumer_index)
        logging.debug(("Setting up {}".format(table_name)))

        instance.query('''
            DROP TABLE IF EXISTS test.{0};
            DROP TABLE IF EXISTS test.{0}_mv;
            CREATE TABLE test.{0} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                        kafka_topic_list = 'topic_with_multiple_partitions',
                        kafka_group_name = 'rebalance_test_group',
                        kafka_format = 'JSONEachRow',
                        kafka_max_block_size = 33,
                        kafka_flush_interval_ms = 500;
            CREATE MATERIALIZED VIEW test.{0}_mv TO test.destination AS
                SELECT
                key,
                value,
                _topic,
                _key,
                _offset,
                _partition,
                _timestamp,
                '{0}' as _consumed_by
            FROM test.{0};
        '''.format(table_name))
        # kafka_cluster.open_bash_shell('instance')
        # Waiting for test.kafka_consumerX to start consume ...
        instance.wait_for_log_line('kafka_consumer{}.*Polled offset [0-9]+'.format(consumer_index))

    cancel.set()

    # I leave last one working by intent (to finish consuming after all rebalances)
    for consumer_index in range(NUMBER_OF_CONSURRENT_CONSUMERS - 1):
        logging.debug(("Dropping test.kafka_consumer{}".format(consumer_index)))
        instance.query('DROP TABLE IF EXISTS test.kafka_consumer{} SYNC'.format(consumer_index))

    # logging.debug(instance.query('SELECT count(), uniqExact(key), max(key) + 1 FROM test.destination'))
    # kafka_cluster.open_bash_shell('instance')

    while 1:
        messages_consumed = int(instance.query('SELECT uniqExact(key) FROM test.destination'))
        if messages_consumed >= msg_index[0]:
            break
        time.sleep(1)
        logging.debug(("Waiting for finishing consuming (have {}, should be {})".format(messages_consumed, msg_index[0])))

    logging.debug((instance.query('SELECT count(), uniqExact(key), max(key) + 1 FROM test.destination')))

    # Some queries to debug...
    # SELECT * FROM test.destination where key in (SELECT key FROM test.destination group by key having count() <> 1)
    # select number + 1 as key from numbers(4141) x left join test.destination using (key) where  test.destination.key = 0;
    # SELECT * FROM test.destination WHERE key between 2360 and 2370 order by key;
    # select _partition from test.destination group by _partition having count() <> max(_offset) + 1;
    # select toUInt64(0) as _partition, number + 1 as _offset from numbers(400) x left join test.destination using (_partition,_offset) where test.destination.key = 0 order by _offset;
    # SELECT * FROM test.destination WHERE _partition = 0 and _offset between 220 and 240 order by _offset;

    # CREATE TABLE test.reference (key UInt64, value UInt64) ENGINE = Kafka SETTINGS kafka_broker_list = 'kafka1:19092',
    #             kafka_topic_list = 'topic_with_multiple_partitions',
    #             kafka_group_name = 'rebalance_test_group_reference',
    #             kafka_format = 'JSONEachRow',
    #             kafka_max_block_size = 100000;
    #
    # CREATE MATERIALIZED VIEW test.reference_mv Engine=Log AS
    #     SELECT  key, value, _topic,_key,_offset, _partition, _timestamp, 'reference' as _consumed_by
    # FROM test.reference;
    #
    # select * from test.reference_mv left join test.destination using (key,_topic,_offset,_partition) where test.destination._consumed_by = '';

    result = int(instance.query('SELECT count() == uniqExact(key) FROM test.destination'))

    for consumer_index in range(NUMBER_OF_CONSURRENT_CONSUMERS):
        logging.debug(("kafka_consumer{}".format(consumer_index)))
        table_name = 'kafka_consumer{}'.format(consumer_index)
        instance.query('''
            DROP TABLE IF EXISTS test.{0};
            DROP TABLE IF EXISTS test.{0}_mv;
        '''.format(table_name))

    instance.query('''
        DROP TABLE IF EXISTS test.destination;
    ''')

    kafka_thread.join()

    assert result == 1, 'Messages from kafka get duplicated!'


def test_kafka_no_holes_when_write_suffix_failed(kafka_cluster):
    messages = [json.dumps({'key': j + 1, 'value': 'x' * 300}) for j in range(22)]
    kafka_produce(kafka_cluster, 'no_holes_when_write_suffix_failed', messages)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'no_holes_when_write_suffix_failed',
                     kafka_group_name = 'no_holes_when_write_suffix_failed',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 20,
                     kafka_flush_interval_ms = 2000;

        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = ReplicatedMergeTree('/clickhouse/kafkatest/tables/no_holes_when_write_suffix_failed', 'node1')
            ORDER BY key;
    ''')

    # init PartitionManager (it starts container) earlier
    pm = PartitionManager()

    instance.query('''
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka
            WHERE NOT sleepEachRow(0.25);
    ''')

    instance.wait_for_log_line("Polled batch of 20 messages")
    # the tricky part here is that disconnect should happen after write prefix, but before write suffix
    # we have 0.25 (sleepEachRow) * 20 ( Rows ) = 5 sec window after "Polled batch of 20 messages"
    # while materialized view is working to inject zookeeper failure
    pm.drop_instance_zk_connections(instance)
    instance.wait_for_log_line("Error.*(session has been expired|Connection loss).*while write prefix to view")
    pm.heal_all()
    instance.wait_for_log_line("Committed offset 22")

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.view')
    logging.debug(result)

    # kafka_cluster.open_bash_shell('instance')

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert TSV(result) == TSV('22\t22\t22')


def test_exception_from_destructor(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'xyz',
                     kafka_group_name = '',
                     kafka_format = 'JSONEachRow';
    ''')
    instance.query_and_get_error('''
        SELECT * FROM test.kafka;
    ''')
    instance.query('''
        DROP TABLE test.kafka;
    ''')

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'xyz',
                     kafka_group_name = '',
                     kafka_format = 'JSONEachRow';
    ''')
    instance.query('''
        DROP TABLE test.kafka;
    ''')

    # kafka_cluster.open_bash_shell('instance')
    assert TSV(instance.query('SELECT 1')) == TSV('1')


def test_commits_of_unprocessed_messages_on_drop(kafka_cluster):
    messages = [json.dumps({'key': j + 1, 'value': j + 1}) for j in range(1)]
    kafka_produce(kafka_cluster, 'commits_of_unprocessed_messages_on_drop', messages)

    instance.query('''
        DROP TABLE IF EXISTS test.destination SYNC;
        CREATE TABLE test.destination (
            key UInt64,
            value UInt64,
            _topic String,
            _key String,
            _offset UInt64,
            _partition UInt64,
            _timestamp Nullable(DateTime('UTC')),
            _consumed_by LowCardinality(String)
        )
        ENGINE = MergeTree()
        ORDER BY key;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'commits_of_unprocessed_messages_on_drop',
                    kafka_group_name = 'commits_of_unprocessed_messages_on_drop_test_group',
                    kafka_format = 'JSONEachRow',
                    kafka_max_block_size = 1000,
                    kafka_flush_interval_ms = 1000;

        CREATE MATERIALIZED VIEW test.kafka_consumer TO test.destination AS
            SELECT
            key,
            value,
            _topic,
            _key,
            _offset,
            _partition,
            _timestamp
        FROM test.kafka;
    ''')

    # Waiting for test.kafka_consumer to start consume
    instance.wait_for_log_line('Committed offset [0-9]+')

    cancel = threading.Event()

    i = [2]

    def produce():
        while not cancel.is_set():
            messages = []
            for _ in range(113):
                messages.append(json.dumps({'key': i[0], 'value': i[0]}))
                i[0] += 1
            kafka_produce(kafka_cluster, 'commits_of_unprocessed_messages_on_drop', messages)
            time.sleep(0.5)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()
    time.sleep(4)

    instance.query('''
        DROP TABLE test.kafka SYNC;
    ''')

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'commits_of_unprocessed_messages_on_drop',
                    kafka_group_name = 'commits_of_unprocessed_messages_on_drop_test_group',
                    kafka_format = 'JSONEachRow',
                    kafka_max_block_size = 10000,
                    kafka_flush_interval_ms = 1000;
    ''')

    cancel.set()
    instance.wait_for_log_line('kafka.*Stalled', repetitions=5)

    # kafka_cluster.open_bash_shell('instance')
    # SELECT key, _timestamp, _offset FROM test.destination where runningDifference(key) <> 1 ORDER BY key;

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.destination')
    logging.debug(result)

    instance.query('''
        DROP TABLE test.kafka_consumer SYNC;
        DROP TABLE test.destination SYNC;
    ''')

    kafka_thread.join()
    assert TSV(result) == TSV('{0}\t{0}\t{0}'.format(i[0] - 1)), 'Missing data!'


def test_bad_reschedule(kafka_cluster):
    messages = [json.dumps({'key': j + 1, 'value': j + 1}) for j in range(20000)]
    kafka_produce(kafka_cluster, 'test_bad_reschedule', messages)

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'test_bad_reschedule',
                    kafka_group_name = 'test_bad_reschedule',
                    kafka_format = 'JSONEachRow',
                    kafka_max_block_size = 1000,
                    kafka_flush_interval_ms = 1000;

        CREATE MATERIALIZED VIEW test.destination Engine=Log AS
        SELECT
            key,
            now() as consume_ts,
            value,
            _topic,
            _key,
            _offset,
            _partition,
            _timestamp
        FROM test.kafka;
    ''')

    instance.wait_for_log_line("Committed offset 20000")

    assert int(instance.query("SELECT max(consume_ts) - min(consume_ts) FROM test.destination")) < 8


def test_kafka_duplicates_when_commit_failed(kafka_cluster):
    messages = [json.dumps({'key': j + 1, 'value': 'x' * 300}) for j in range(22)]
    kafka_produce(kafka_cluster, 'duplicates_when_commit_failed', messages)

    instance.query('''
        DROP TABLE IF EXISTS test.view SYNC;
        DROP TABLE IF EXISTS test.consumer SYNC;

        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'duplicates_when_commit_failed',
                     kafka_group_name = 'duplicates_when_commit_failed',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 20,
                     kafka_flush_interval_ms = 1000;

        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree()
            ORDER BY key;
    ''')

    instance.query('''
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka
            WHERE NOT sleepEachRow(0.25);
    ''')

    instance.wait_for_log_line("Polled batch of 20 messages")
    # the tricky part here is that disconnect should happen after write prefix, but before we do commit
    # we have 0.25 (sleepEachRow) * 20 ( Rows ) = 5 sec window after "Polled batch of 20 messages"
    # while materialized view is working to inject zookeeper failure
    kafka_cluster.pause_container('kafka1')

    # if we restore the connection too fast (<30sec) librdkafka will not report any timeout
    # (alternative is to decrease the default session timeouts for librdkafka)
    #
    # when the delay is too long (>50sec) broker will decide to remove us from the consumer group,
    # and will start answering "Broker: Unknown member"
    instance.wait_for_log_line("Exception during commit attempt: Local: Waiting for coordinator", timeout=45)
    instance.wait_for_log_line("All commit attempts failed", look_behind_lines=500)

    kafka_cluster.unpause_container('kafka1')

    # kafka_cluster.open_bash_shell('instance')
    instance.wait_for_log_line("Committed offset 22")

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.view')
    logging.debug(result)

    instance.query('''
        DROP TABLE test.consumer SYNC;
        DROP TABLE test.view SYNC;
    ''')

    # After https://github.com/edenhill/librdkafka/issues/2631
    # timeout triggers rebalance, making further commits to the topic after getting back online
    # impossible. So we have a duplicate in that scenario, but we report that situation properly.
    assert TSV(result) == TSV('42\t22\t22')


# if we came to partition end we will repeat polling until reaching kafka_max_block_size or flush_interval
# that behavior is a bit quesionable - we can just take a bigger pauses between polls instead -
# to do more job in a single pass, and give more rest for a thread.
# But in cases of some peaky loads in kafka topic the current contract sounds more predictable and
# easier to understand, so let's keep it as is for now.
# also we can came to eof because we drained librdkafka internal queue too fast
def test_premature_flush_on_eof(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'premature_flush_on_eof',
                    kafka_group_name = 'premature_flush_on_eof',
                    kafka_format = 'JSONEachRow';
        SELECT * FROM test.kafka LIMIT 1;
        CREATE TABLE test.destination (
            key UInt64,
            value UInt64,
            _topic String,
            _key String,
            _offset UInt64,
            _partition UInt64,
            _timestamp Nullable(DateTime('UTC')),
            _consumed_by LowCardinality(String)
        )
        ENGINE = MergeTree()
        ORDER BY key;
    ''')

    # messages created here will be consumed immedeately after MV creation
    # reaching topic EOF.
    # But we should not do flush immedeately after reaching EOF, because
    # next poll can return more data, and we should respect kafka_flush_interval_ms
    # and try to form bigger block
    messages = [json.dumps({'key': j + 1, 'value': j + 1}) for j in range(1)]
    kafka_produce(kafka_cluster, 'premature_flush_on_eof', messages)

    instance.query('''
        CREATE MATERIALIZED VIEW test.kafka_consumer TO test.destination AS
        SELECT
            key,
            value,
            _topic,
            _key,
            _offset,
            _partition,
            _timestamp
        FROM test.kafka;
    ''')

    # all subscriptions/assignments done during select, so it start sending data to test.destination
    # immediately after creation of MV

    instance.wait_for_log_line("Polled batch of 1 messages")
    instance.wait_for_log_line("Stalled")

    # produce more messages after delay
    kafka_produce(kafka_cluster, 'premature_flush_on_eof', messages)

    # data was not flushed yet (it will be flushed 7.5 sec after creating MV)
    assert int(instance.query("SELECT count() FROM test.destination")) == 0

    instance.wait_for_log_line("Committed offset 2")

    # it should be single part, i.e. single insert
    result = instance.query('SELECT _part, count() FROM test.destination group by _part')
    assert TSV(result) == TSV('all_1_1_0\t2')

    instance.query('''
        DROP TABLE test.kafka_consumer;
        DROP TABLE test.destination;
    ''')


def test_kafka_unavailable(kafka_cluster):
    messages = [json.dumps({'key': j + 1, 'value': j + 1}) for j in range(20000)]
    kafka_produce(kafka_cluster, 'test_bad_reschedule', messages)

    kafka_cluster.pause_container('kafka1')

    instance.query('''
        CREATE TABLE test.test_bad_reschedule (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'test_bad_reschedule',
                    kafka_group_name = 'test_bad_reschedule',
                    kafka_format = 'JSONEachRow',
                    kafka_max_block_size = 1000;

        CREATE MATERIALIZED VIEW test.destination_unavailable Engine=Log AS
        SELECT
            key,
            now() as consume_ts,
            value,
            _topic,
            _key,
            _offset,
            _partition,
            _timestamp
        FROM test.test_bad_reschedule;
    ''')

    instance.query("SELECT * FROM test.test_bad_reschedule")
    instance.query("SELECT count() FROM test.destination_unavailable")

    # enough to trigger issue
    time.sleep(30)
    kafka_cluster.unpause_container('kafka1')

    while int(instance.query("SELECT count() FROM test.destination_unavailable")) < 20000:
        print("Waiting for consume")
        time.sleep(1)


def test_kafka_issue14202(kafka_cluster):
    """
    INSERT INTO Kafka Engine from an empty SELECT sub query was leading to failure
    """

    instance.query('''
        CREATE TABLE test.empty_table (
            dt Date,
            some_string String
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(dt)
        ORDER BY some_string;

        CREATE TABLE test.kafka_q (t UInt64, `some_string` String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'issue14202',
                     kafka_group_name = 'issue14202',
                     kafka_format = 'JSONEachRow';
        ''')

    instance.query(
        'INSERT INTO test.kafka_q SELECT t, some_string  FROM ( SELECT dt AS t, some_string FROM test.empty_table )')
    # check instance is alive
    assert TSV(instance.query('SELECT 1')) == TSV('1')
    instance.query('''
        DROP TABLE test.empty_table;
        DROP TABLE test.kafka_q;
    ''')


def test_kafka_csv_with_thread_per_consumer(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'csv_with_thread_per_consumer',
                     kafka_group_name = 'csv_with_thread_per_consumer',
                     kafka_format = 'CSV',
                     kafka_row_delimiter = '\\n',
                     kafka_num_consumers = 4,
                     kafka_thread_per_consumer = 1;
        ''')

    messages = []
    for i in range(50):
        messages.append('{i}, {i}'.format(i=i))
    kafka_produce(kafka_cluster, 'csv_with_thread_per_consumer', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)

def random_string(size=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=size))

def test_kafka_engine_put_errors_to_stream(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka_data;
        DROP TABLE IF EXISTS test.kafka_errors;
        CREATE TABLE test.kafka (i Int64, s String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'kafka_engine_put_errors_to_stream',
                     kafka_group_name = 'kafka_engine_put_errors_to_stream',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 128,
                     kafka_handle_error_mode = 'stream';
        CREATE MATERIALIZED VIEW test.kafka_data (i Int64, s String)
            ENGINE = MergeTree
            ORDER BY i
            AS SELECT i, s FROM test.kafka WHERE length(_error) == 0;
        CREATE MATERIALIZED VIEW test.kafka_errors (topic String, partition Int64, offset Int64, raw String, error String)
            ENGINE = MergeTree
            ORDER BY (topic, offset)
            AS SELECT
               _topic AS topic,
               _partition AS partition,
               _offset AS offset,
               _raw_message AS raw,
               _error AS error
               FROM test.kafka WHERE length(_error) > 0;
        ''')

    messages = []
    for i in range(128):
        if i % 2 == 0:
            messages.append(json.dumps({'i': i, 's': random_string(8)}))
        else:
            # Unexpected json content for table test.kafka.
            messages.append(json.dumps({'i': 'n_' + random_string(4), 's': random_string(8)}))

    kafka_produce(kafka_cluster, 'kafka_engine_put_errors_to_stream', messages)
    instance.wait_for_log_line("Committed offset 128")

    assert TSV(instance.query('SELECT count() FROM test.kafka_data')) == TSV('64')
    assert TSV(instance.query('SELECT count() FROM test.kafka_errors')) == TSV('64')

    instance.query('''
        DROP TABLE test.kafka;
        DROP TABLE test.kafka_data;
        DROP TABLE test.kafka_errors;
    ''')

def gen_normal_json():
    return '{"i":1000, "s":"ABC123abc"}'

def gen_malformed_json():
    return '{"i":"n1000", "s":"1000"}'

def gen_message_with_jsons(jsons = 10, malformed = 0):
    s = io.StringIO()

    # we don't care on which position error will be added
    # (we skip whole broken message), but we need to be
    # sure that at least one error will be added,
    # otherwise test will fail.
    error_pos = random.randint(0,jsons-1)

    for i in range (jsons):
        if malformed and i == error_pos:
            s.write(gen_malformed_json())
        else:
            s.write(gen_normal_json())
        s.write(' ')
    return s.getvalue()


def test_kafka_engine_put_errors_to_stream_with_random_malformed_json(kafka_cluster):
    instance.query('''
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka_data;
        DROP TABLE IF EXISTS test.kafka_errors;
        CREATE TABLE test.kafka (i Int64, s String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'kafka_engine_put_errors_to_stream_with_random_malformed_json',
                     kafka_group_name = 'kafka_engine_put_errors_to_stream_with_random_malformed_json',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 100,
                     kafka_poll_max_batch_size = 1,
                     kafka_handle_error_mode = 'stream';
        CREATE MATERIALIZED VIEW test.kafka_data (i Int64, s String)
            ENGINE = MergeTree
            ORDER BY i
            AS SELECT i, s FROM test.kafka WHERE length(_error) == 0;
        CREATE MATERIALIZED VIEW test.kafka_errors (topic String, partition Int64, offset Int64, raw String, error String)
            ENGINE = MergeTree
            ORDER BY (topic, offset)
            AS SELECT
               _topic AS topic,
               _partition AS partition,
               _offset AS offset,
               _raw_message AS raw,
               _error AS error
               FROM test.kafka WHERE length(_error) > 0;
        ''')

    messages = []
    for i in range(128):
        if i % 2 == 0:
            messages.append(gen_message_with_jsons(10, 1))
        else:
            messages.append(gen_message_with_jsons(10, 0))

    kafka_produce(kafka_cluster,  'kafka_engine_put_errors_to_stream_with_random_malformed_json', messages)

    instance.wait_for_log_line("Committed offset 128")
    # 64 good messages, each containing 10 rows
    assert TSV(instance.query('SELECT count() FROM test.kafka_data')) == TSV('640')
    # 64 bad messages, each containing some broken row
    assert TSV(instance.query('SELECT count() FROM test.kafka_errors')) == TSV('64')

    instance.query('''
        DROP TABLE test.kafka;
        DROP TABLE test.kafka_data;
        DROP TABLE test.kafka_errors;
    ''')

def test_kafka_formats_with_broken_message(kafka_cluster):
    # data was dumped from clickhouse itself in a following manner
    # clickhouse-client --format=Native --query='SELECT toInt64(number) as id, toUInt16( intDiv( id, 65536 ) ) as blockNo, reinterpretAsString(19777) as val1, toFloat32(0.5) as val2, toUInt8(1) as val3 from numbers(100) ORDER BY id' | xxd -ps | tr -d '\n' | sed 's/\(..\)/\\x\1/g'

    all_formats = {
        ## Text formats ##
        # dumped with clickhouse-client ... | perl -pe 's/\n/\\n/; s/\t/\\t/g;'
        'JSONEachRow': {
            'data_sample': [
                '{"id":"0","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
                '{"id":"1","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"2","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"3","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"4","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"5","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"6","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"7","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"8","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"9","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"10","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"11","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"12","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"13","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"14","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"15","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
                '{"id":"0","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
                # broken message
                '{"id":"0","blockNo":"BAD","val1":"AM","val2":0.5,"val3":1}',
            ],
            'expected':'''{"raw_message":"{\\"id\\":\\"0\\",\\"blockNo\\":\\"BAD\\",\\"val1\\":\\"AM\\",\\"val2\\":0.5,\\"val3\\":1}","error":"Cannot parse input: expected '\\"' before: 'BAD\\",\\"val1\\":\\"AM\\",\\"val2\\":0.5,\\"val3\\":1}': (while reading the value of key blockNo)"}''',
            'supports_empty_value': True,
            'printable': True,
        },
        # JSONAsString doesn't fit to that test, and tested separately
        'JSONCompactEachRow': {
            'data_sample': [
                '["0", 0, "AM", 0.5, 1]\n',
                '["1", 0, "AM", 0.5, 1]\n["2", 0, "AM", 0.5, 1]\n["3", 0, "AM", 0.5, 1]\n["4", 0, "AM", 0.5, 1]\n["5", 0, "AM", 0.5, 1]\n["6", 0, "AM", 0.5, 1]\n["7", 0, "AM", 0.5, 1]\n["8", 0, "AM", 0.5, 1]\n["9", 0, "AM", 0.5, 1]\n["10", 0, "AM", 0.5, 1]\n["11", 0, "AM", 0.5, 1]\n["12", 0, "AM", 0.5, 1]\n["13", 0, "AM", 0.5, 1]\n["14", 0, "AM", 0.5, 1]\n["15", 0, "AM", 0.5, 1]\n',
                '["0", 0, "AM", 0.5, 1]\n',
                # broken message
                '["0", "BAD", "AM", 0.5, 1]',
            ],
            'expected':'''{"raw_message":"[\\"0\\", \\"BAD\\", \\"AM\\", 0.5, 1]","error":"Cannot parse input: expected '\\"' before: 'BAD\\", \\"AM\\", 0.5, 1]': (while reading the value of key blockNo)"}''',
            'supports_empty_value': True,
            'printable':True,
        },
        'JSONCompactEachRowWithNamesAndTypes': {
            'data_sample': [
                '["id", "blockNo", "val1", "val2", "val3"]\n["Int64", "UInt16", "String", "Float32", "UInt8"]\n["0", 0, "AM", 0.5, 1]\n',
                '["id", "blockNo", "val1", "val2", "val3"]\n["Int64", "UInt16", "String", "Float32", "UInt8"]\n["1", 0, "AM", 0.5, 1]\n["2", 0, "AM", 0.5, 1]\n["3", 0, "AM", 0.5, 1]\n["4", 0, "AM", 0.5, 1]\n["5", 0, "AM", 0.5, 1]\n["6", 0, "AM", 0.5, 1]\n["7", 0, "AM", 0.5, 1]\n["8", 0, "AM", 0.5, 1]\n["9", 0, "AM", 0.5, 1]\n["10", 0, "AM", 0.5, 1]\n["11", 0, "AM", 0.5, 1]\n["12", 0, "AM", 0.5, 1]\n["13", 0, "AM", 0.5, 1]\n["14", 0, "AM", 0.5, 1]\n["15", 0, "AM", 0.5, 1]\n',
                '["id", "blockNo", "val1", "val2", "val3"]\n["Int64", "UInt16", "String", "Float32", "UInt8"]\n["0", 0, "AM", 0.5, 1]\n',
                # broken message
                '["0", "BAD", "AM", 0.5, 1]',
            ],
            'expected':'''{"raw_message":"[\\"0\\", \\"BAD\\", \\"AM\\", 0.5, 1]","error":"Cannot parse JSON string: expected opening quote"}''',
            'printable':True,
        },
        'TSKV': {
            'data_sample': [
                'id=0\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\n',
                'id=1\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=2\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=3\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=4\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=5\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=6\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=7\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=8\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=9\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=10\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=11\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=12\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=13\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=14\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=15\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\n',
                'id=0\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\n',
                # broken message
                'id=0\tblockNo=BAD\tval1=AM\tval2=0.5\tval3=1\n',
            ],
            'expected':'{"raw_message":"id=0\\tblockNo=BAD\\tval1=AM\\tval2=0.5\\tval3=1\\n","error":"Found garbage after field in TSKV format: blockNo: (at row 1)\\n"}',
            'printable':True,
        },
        'CSV': {
            'data_sample': [
                '0,0,"AM",0.5,1\n',
                '1,0,"AM",0.5,1\n2,0,"AM",0.5,1\n3,0,"AM",0.5,1\n4,0,"AM",0.5,1\n5,0,"AM",0.5,1\n6,0,"AM",0.5,1\n7,0,"AM",0.5,1\n8,0,"AM",0.5,1\n9,0,"AM",0.5,1\n10,0,"AM",0.5,1\n11,0,"AM",0.5,1\n12,0,"AM",0.5,1\n13,0,"AM",0.5,1\n14,0,"AM",0.5,1\n15,0,"AM",0.5,1\n',
                '0,0,"AM",0.5,1\n',
                # broken message
                '0,"BAD","AM",0.5,1\n',
            ],
            'expected':'''{"raw_message":"0,\\"BAD\\",\\"AM\\",0.5,1\\n","error":"Cannot parse input: expected '\\"' before: 'BAD\\",\\"AM\\",0.5,1\\\\n': Could not print diagnostic info because two last rows aren't in buffer (rare case)\\n"}''',
            'printable':True,
            'supports_empty_value': True,
        },
        'TSV': {
            'data_sample': [
                '0\t0\tAM\t0.5\t1\n',
                '1\t0\tAM\t0.5\t1\n2\t0\tAM\t0.5\t1\n3\t0\tAM\t0.5\t1\n4\t0\tAM\t0.5\t1\n5\t0\tAM\t0.5\t1\n6\t0\tAM\t0.5\t1\n7\t0\tAM\t0.5\t1\n8\t0\tAM\t0.5\t1\n9\t0\tAM\t0.5\t1\n10\t0\tAM\t0.5\t1\n11\t0\tAM\t0.5\t1\n12\t0\tAM\t0.5\t1\n13\t0\tAM\t0.5\t1\n14\t0\tAM\t0.5\t1\n15\t0\tAM\t0.5\t1\n',
                '0\t0\tAM\t0.5\t1\n',
                # broken message
                '0\tBAD\tAM\t0.5\t1\n',
            ],
            'expected':'''{"raw_message":"0\\tBAD\\tAM\\t0.5\\t1\\n","error":"Cannot parse input: expected '\\\\t' before: 'BAD\\\\tAM\\\\t0.5\\\\t1\\\\n': Could not print diagnostic info because two last rows aren't in buffer (rare case)\\n"}''',
            'supports_empty_value': True,
            'printable':True,
        },
        'CSVWithNames': {
            'data_sample': [
                '"id","blockNo","val1","val2","val3"\n0,0,"AM",0.5,1\n',
                '"id","blockNo","val1","val2","val3"\n1,0,"AM",0.5,1\n2,0,"AM",0.5,1\n3,0,"AM",0.5,1\n4,0,"AM",0.5,1\n5,0,"AM",0.5,1\n6,0,"AM",0.5,1\n7,0,"AM",0.5,1\n8,0,"AM",0.5,1\n9,0,"AM",0.5,1\n10,0,"AM",0.5,1\n11,0,"AM",0.5,1\n12,0,"AM",0.5,1\n13,0,"AM",0.5,1\n14,0,"AM",0.5,1\n15,0,"AM",0.5,1\n',
                '"id","blockNo","val1","val2","val3"\n0,0,"AM",0.5,1\n',
                # broken message
                '"id","blockNo","val1","val2","val3"\n0,"BAD","AM",0.5,1\n',
            ],
            'expected':'''{"raw_message":"\\"id\\",\\"blockNo\\",\\"val1\\",\\"val2\\",\\"val3\\"\\n0,\\"BAD\\",\\"AM\\",0.5,1\\n","error":"Cannot parse input: expected '\\"' before: 'BAD\\",\\"AM\\",0.5,1\\\\n': Could not print diagnostic info because two last rows aren't in buffer (rare case)\\n"}''',
            'printable':True,
        },
        'Values': {
            'data_sample': [
                "(0,0,'AM',0.5,1)",
                "(1,0,'AM',0.5,1),(2,0,'AM',0.5,1),(3,0,'AM',0.5,1),(4,0,'AM',0.5,1),(5,0,'AM',0.5,1),(6,0,'AM',0.5,1),(7,0,'AM',0.5,1),(8,0,'AM',0.5,1),(9,0,'AM',0.5,1),(10,0,'AM',0.5,1),(11,0,'AM',0.5,1),(12,0,'AM',0.5,1),(13,0,'AM',0.5,1),(14,0,'AM',0.5,1),(15,0,'AM',0.5,1)",
                "(0,0,'AM',0.5,1)",
                # broken message
                "(0,'BAD','AM',0.5,1)",
            ],
            'expected':r'''{"raw_message":"(0,'BAD','AM',0.5,1)","error":"Cannot parse string 'BAD' as UInt16: syntax error at begin of string. Note: there are toUInt16OrZero and toUInt16OrNull functions, which returns zero\/NULL instead of throwing exception.: while executing 'FUNCTION CAST(assumeNotNull(_dummy_0) :: 2, 'UInt16' :: 1) -> CAST(assumeNotNull(_dummy_0), 'UInt16') UInt16 : 4'"}''',
            'supports_empty_value': True,
            'printable':True,
        },
        'TSVWithNames': {
            'data_sample': [
                'id\tblockNo\tval1\tval2\tval3\n0\t0\tAM\t0.5\t1\n',
                'id\tblockNo\tval1\tval2\tval3\n1\t0\tAM\t0.5\t1\n2\t0\tAM\t0.5\t1\n3\t0\tAM\t0.5\t1\n4\t0\tAM\t0.5\t1\n5\t0\tAM\t0.5\t1\n6\t0\tAM\t0.5\t1\n7\t0\tAM\t0.5\t1\n8\t0\tAM\t0.5\t1\n9\t0\tAM\t0.5\t1\n10\t0\tAM\t0.5\t1\n11\t0\tAM\t0.5\t1\n12\t0\tAM\t0.5\t1\n13\t0\tAM\t0.5\t1\n14\t0\tAM\t0.5\t1\n15\t0\tAM\t0.5\t1\n',
                'id\tblockNo\tval1\tval2\tval3\n0\t0\tAM\t0.5\t1\n',
                # broken message
                'id\tblockNo\tval1\tval2\tval3\n0\tBAD\tAM\t0.5\t1\n',
            ],
            'expected':'''{"raw_message":"id\\tblockNo\\tval1\\tval2\\tval3\\n0\\tBAD\\tAM\\t0.5\\t1\\n","error":"Cannot parse input: expected '\\\\t' before: 'BAD\\\\tAM\\\\t0.5\\\\t1\\\\n': Could not print diagnostic info because two last rows aren't in buffer (rare case)\\n"}''',
            'supports_empty_value': True,
            'printable':True,
        },
        'TSVWithNamesAndTypes': {
            'data_sample': [
                'id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n0\t0\tAM\t0.5\t1\n',
                'id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n1\t0\tAM\t0.5\t1\n2\t0\tAM\t0.5\t1\n3\t0\tAM\t0.5\t1\n4\t0\tAM\t0.5\t1\n5\t0\tAM\t0.5\t1\n6\t0\tAM\t0.5\t1\n7\t0\tAM\t0.5\t1\n8\t0\tAM\t0.5\t1\n9\t0\tAM\t0.5\t1\n10\t0\tAM\t0.5\t1\n11\t0\tAM\t0.5\t1\n12\t0\tAM\t0.5\t1\n13\t0\tAM\t0.5\t1\n14\t0\tAM\t0.5\t1\n15\t0\tAM\t0.5\t1\n',
                'id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n0\t0\tAM\t0.5\t1\n',
                # broken message
                'id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n0\tBAD\tAM\t0.5\t1\n',
            ],
            'expected':'''{"raw_message":"id\\tblockNo\\tval1\\tval2\\tval3\\nInt64\\tUInt16\\tString\\tFloat32\\tUInt8\\n0\\tBAD\\tAM\\t0.5\\t1\\n","error":"Cannot parse input: expected '\\\\t' before: 'BAD\\\\tAM\\\\t0.5\\\\t1\\\\n': Could not print diagnostic info because two last rows aren't in buffer (rare case)\\n"}''',
            'printable':True,
        },
      'Native': {
            'data_sample': [
                b'\x05\x01\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x00\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01',
                b'\x05\x0f\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01',
                b'\x05\x01\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x00\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01',
                # broken message
           b'\x05\x01\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x00\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x53\x74\x72\x69\x6e\x67\x03\x42\x41\x44\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01',
            ],
            'expected':'''{"raw_message":"050102696405496E743634000000000000000007626C6F636B4E6F06537472696E67034241440476616C3106537472696E6702414D0476616C3207466C6F617433320000003F0476616C330555496E743801","error":"Cannot convert: String to UInt16"}''',
            'printable':False,
        },
        'RowBinary': {
            'data_sample': [
                b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                b'\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                # broken message
                b'\x00\x00\x00\x00\x00\x00\x00\x00\x03\x42\x41\x44\x02\x41\x4d\x00\x00\x00\x3f\x01',
            ],
            'expected':'{"raw_message":"00000000000000000342414402414D0000003F01","error":"Cannot read all data. Bytes read: 9. Bytes expected: 65.: (at row 1)\\n"}',
            'printable':False,
        },
        'RowBinaryWithNamesAndTypes': {
            'data_sample': [
                b'\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                b'\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                b'\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                # broken message
            b'\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x53\x74\x72\x69\x6e\x67\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x00\x00\x00\x00\x00\x00\x00\x00\x03\x42\x41\x44\x02\x41\x4d\x00\x00\x00\x3f\x01',
            ],
            'expected':'{"raw_message":"0502696407626C6F636B4E6F0476616C310476616C320476616C3305496E74363406537472696E6706537472696E6707466C6F617433320555496E743800000000000000000342414402414D0000003F01","error":"Cannot read all data. Bytes read: 9. Bytes expected: 65.: (at row 1)\\n"}',
            'printable':False,
        },
        'ORC': {
            'data_sample': [
                b'\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x01\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x46\x25\x0e\x2e\x46\x03\x21\x46\x03\x09\xa6\x00\x06\x00\x32\x00\x00\xe3\x92\xe4\x62\x65\x00\x01\x21\x01\x0e\x46\x25\x2e\x2e\x26\x47\x5f\x21\x20\x96\x60\x09\x60\x00\x00\x36\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x46\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x10\x11\xc0\x00\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x05\x00\x00\xff\x00\x03\x00\x00\x30\x07\x00\x00\x40\x00\x80\x05\x00\x00\x41\x4d\x07\x00\x00\x42\x00\x80\x03\x00\x00\x0a\x07\x00\x00\x42\x00\x80\x05\x00\x00\xff\x01\x88\x00\x00\x4d\xca\xc1\x0a\x80\x30\x0c\x03\xd0\x2e\x6b\xcb\x98\x17\xf1\x14\x50\xfc\xff\xcf\xb4\x66\x1e\x3c\x84\x47\x9a\xce\x1c\xb9\x1b\xb7\xf9\xda\x48\x09\x9e\xb2\xf3\x92\xce\x5b\x86\xf6\x56\x7f\x21\x41\x2f\x51\xa6\x7a\xd7\x1d\xe5\xea\xae\x3d\xca\xd5\x83\x71\x60\xd8\x17\xfc\x62\x0f\xa8\x00\x00\xe3\x4a\xe6\x62\xe1\x60\x0c\x60\xe0\xe2\xe3\x60\x14\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\x60\x54\xe2\xe0\x62\x34\x10\x62\x34\x90\x60\x02\x8a\x70\x71\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\x82\x05\x28\xc6\xcd\x25\xca\xc1\x68\xc4\x0b\x52\xc5\x6c\xa0\x67\x2a\x05\x22\xc0\x4a\x21\x86\x31\x09\x30\x81\xb5\xb2\x02\x00\x36\x01\x00\x25\x8c\xbd\x0a\xc2\x30\x14\x85\x73\x6f\x92\xf6\x92\x6a\x09\x01\x21\x64\x92\x4e\x75\x91\x58\x71\xc9\x64\x27\x5d\x2c\x1d\x5d\xfd\x59\xc4\x42\x37\x5f\xc0\x17\xe8\x23\x9b\xc6\xe1\x3b\x70\x0f\xdf\xb9\xc4\xf5\x17\x5d\x41\x5c\x4f\x60\x37\xeb\x53\x0d\x55\x4d\x0b\x23\x01\xb9\x90\x2e\xbf\x0f\xe3\xe3\xdd\x8d\x0e\x5f\x4f\x27\x3e\xb7\x61\x97\xb2\x49\xb9\xaf\x90\x20\x92\x27\x32\x2a\x6b\xf4\xf3\x0d\x1e\x82\x20\xe8\x59\x28\x09\x4c\x46\x4c\x33\xcb\x7a\x76\x95\x41\x47\x9f\x14\x78\x03\xde\x62\x6c\x54\x30\xb1\x51\x0a\xdb\x8b\x89\x58\x11\xbb\x22\xac\x08\x9a\xe5\x6c\x71\xbf\x3d\xb8\x39\x92\xfa\x7f\x86\x1a\xd3\x54\x1e\xa7\xee\xcc\x7e\x08\x9e\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x57\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18',
                b'\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x0f\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x0f\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x7e\x25\x0e\x2e\x46\x43\x21\x46\x4b\x09\xad\x00\x06\x00\x33\x00\x00\x0a\x17\x0a\x03\x00\x00\x00\x12\x10\x08\x0f\x22\x0a\x0a\x02\x41\x4d\x12\x02\x41\x4d\x18\x3c\x50\x00\x3a\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x7e\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x66\x73\x3d\xd3\x00\x06\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x0f\x12\x06\x08\x02\x10\x02\x18\x1e\x50\x00\x05\x00\x00\x0c\x00\x2b\x00\x00\x31\x32\x33\x34\x35\x36\x37\x38\x39\x31\x30\x31\x31\x31\x32\x31\x33\x31\x34\x31\x35\x09\x00\x00\x06\x01\x03\x02\x09\x00\x00\xc0\x0e\x00\x00\x07\x00\x00\x42\x00\x80\x05\x00\x00\x41\x4d\x0a\x00\x00\xe3\xe2\x42\x01\x00\x09\x00\x00\xc0\x0e\x02\x00\x05\x00\x00\x0c\x01\x94\x00\x00\x2d\xca\xc1\x0e\x80\x30\x08\x03\xd0\xc1\x60\x2e\xf3\x62\x76\x6a\xe2\x0e\xfe\xff\x57\x5a\x3b\x0f\xe4\x51\xe8\x68\xbd\x5d\x05\xe7\xf8\x34\x40\x3a\x6e\x59\xb1\x64\xe0\x91\xa9\xbf\xb1\x97\xd2\x95\x9d\x1e\xca\x55\x3a\x6d\xb4\xd2\xdd\x0b\x74\x9a\x74\xf7\x12\x39\xbd\x97\x7f\x7c\x06\xbb\xa6\x8d\x97\x17\xb4\x00\x00\xe3\x4a\xe6\x62\xe1\xe0\x0f\x60\xe0\xe2\xe3\xe0\x17\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\xe0\x57\xe2\xe0\x62\x34\x14\x62\xb4\x94\xd0\x02\x8a\xc8\x73\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\xc2\x06\x28\x26\xc4\x25\xca\xc1\x6f\xc4\xcb\xc5\x68\x20\xc4\x6c\xa0\x67\x2a\xc5\x6c\xae\x67\x0a\x14\xe6\x87\x1a\xc6\x24\xc0\x24\x21\x07\x32\x0c\x00\x4a\x01\x00\xe3\x60\x16\x58\xc3\x24\xc5\xcd\xc1\x2c\x30\x89\x51\xc2\x4b\xc1\x57\x83\x5f\x49\x83\x83\x47\x88\x95\x91\x89\x99\x85\x55\x8a\x3d\x29\x27\x3f\x39\xdb\x2f\x5f\x8a\x29\x33\x45\x8a\xa5\x2c\x31\xc7\x10\x4c\x1a\x81\x49\x63\x25\x26\x0e\x46\x20\x66\x07\x63\x36\x0e\x3e\x0d\x26\x03\x10\x9f\xd1\x80\xdf\x8a\x85\x83\x3f\x80\xc1\x8a\x8f\x83\x5f\x88\x8d\x83\x41\x80\x41\x82\x21\x80\x21\x82\xd5\x4a\x80\x83\x5f\x89\x83\x8b\xd1\x50\x88\xd1\x52\x42\x0b\x28\x22\x6f\x25\x04\x14\xe1\xe2\x62\x72\xf4\x15\x02\x62\x09\x1b\xa0\x98\x90\x95\x28\x07\xbf\x11\x2f\x17\xa3\x81\x10\xb3\x81\x9e\xa9\x14\xb3\xb9\x9e\x29\x50\x98\x1f\x6a\x18\x93\x00\x93\x84\x1c\xc8\x30\x87\x09\x7e\x1e\x0c\x00\x08\xa8\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x5d\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18',
                b'\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x01\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x46\x25\x0e\x2e\x46\x03\x21\x46\x03\x09\xa6\x00\x06\x00\x32\x00\x00\xe3\x92\xe4\x62\x65\x00\x01\x21\x01\x0e\x46\x25\x2e\x2e\x26\x47\x5f\x21\x20\x96\x60\x09\x60\x00\x00\x36\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x46\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x10\x11\xc0\x00\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x05\x00\x00\xff\x00\x03\x00\x00\x30\x07\x00\x00\x40\x00\x80\x05\x00\x00\x41\x4d\x07\x00\x00\x42\x00\x80\x03\x00\x00\x0a\x07\x00\x00\x42\x00\x80\x05\x00\x00\xff\x01\x88\x00\x00\x4d\xca\xc1\x0a\x80\x30\x0c\x03\xd0\x2e\x6b\xcb\x98\x17\xf1\x14\x50\xfc\xff\xcf\xb4\x66\x1e\x3c\x84\x47\x9a\xce\x1c\xb9\x1b\xb7\xf9\xda\x48\x09\x9e\xb2\xf3\x92\xce\x5b\x86\xf6\x56\x7f\x21\x41\x2f\x51\xa6\x7a\xd7\x1d\xe5\xea\xae\x3d\xca\xd5\x83\x71\x60\xd8\x17\xfc\x62\x0f\xa8\x00\x00\xe3\x4a\xe6\x62\xe1\x60\x0c\x60\xe0\xe2\xe3\x60\x14\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\x60\x54\xe2\xe0\x62\x34\x10\x62\x34\x90\x60\x02\x8a\x70\x71\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\x82\x05\x28\xc6\xcd\x25\xca\xc1\x68\xc4\x0b\x52\xc5\x6c\xa0\x67\x2a\x05\x22\xc0\x4a\x21\x86\x31\x09\x30\x81\xb5\xb2\x02\x00\x36\x01\x00\x25\x8c\xbd\x0a\xc2\x30\x14\x85\x73\x6f\x92\xf6\x92\x6a\x09\x01\x21\x64\x92\x4e\x75\x91\x58\x71\xc9\x64\x27\x5d\x2c\x1d\x5d\xfd\x59\xc4\x42\x37\x5f\xc0\x17\xe8\x23\x9b\xc6\xe1\x3b\x70\x0f\xdf\xb9\xc4\xf5\x17\x5d\x41\x5c\x4f\x60\x37\xeb\x53\x0d\x55\x4d\x0b\x23\x01\xb9\x90\x2e\xbf\x0f\xe3\xe3\xdd\x8d\x0e\x5f\x4f\x27\x3e\xb7\x61\x97\xb2\x49\xb9\xaf\x90\x20\x92\x27\x32\x2a\x6b\xf4\xf3\x0d\x1e\x82\x20\xe8\x59\x28\x09\x4c\x46\x4c\x33\xcb\x7a\x76\x95\x41\x47\x9f\x14\x78\x03\xde\x62\x6c\x54\x30\xb1\x51\x0a\xdb\x8b\x89\x58\x11\xbb\x22\xac\x08\x9a\xe5\x6c\x71\xbf\x3d\xb8\x39\x92\xfa\x7f\x86\x1a\xd3\x54\x1e\xa7\xee\xcc\x7e\x08\x9e\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x57\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18',
                # broken message
            b'\x4f\x52\x43\x0a\x0b\x0a\x03\x00\x00\x00\x12\x04\x08\x01\x50\x00\x0a\x15\x0a\x05\x00\x00\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x0a\x12\x0a\x06\x00\x00\x00\x00\x00\x00\x12\x08\x08\x01\x42\x02\x08\x06\x50\x00\x0a\x12\x0a\x06\x00\x00\x00\x00\x00\x00\x12\x08\x08\x01\x42\x02\x08\x04\x50\x00\x0a\x29\x0a\x04\x00\x00\x00\x00\x12\x21\x08\x01\x1a\x1b\x09\x00\x00\x00\x00\x00\x00\xe0\x3f\x11\x00\x00\x00\x00\x00\x00\xe0\x3f\x19\x00\x00\x00\x00\x00\x00\xe0\x3f\x50\x00\x0a\x15\x0a\x05\x00\x00\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\xff\x80\xff\x80\xff\x00\xff\x80\xff\x03\x42\x41\x44\xff\x80\xff\x02\x41\x4d\xff\x80\x00\x00\x00\x3f\xff\x80\xff\x01\x0a\x06\x08\x06\x10\x00\x18\x0d\x0a\x06\x08\x06\x10\x01\x18\x17\x0a\x06\x08\x06\x10\x02\x18\x14\x0a\x06\x08\x06\x10\x03\x18\x14\x0a\x06\x08\x06\x10\x04\x18\x2b\x0a\x06\x08\x06\x10\x05\x18\x17\x0a\x06\x08\x00\x10\x00\x18\x02\x0a\x06\x08\x00\x10\x01\x18\x02\x0a\x06\x08\x01\x10\x01\x18\x02\x0a\x06\x08\x00\x10\x02\x18\x02\x0a\x06\x08\x02\x10\x02\x18\x02\x0a\x06\x08\x01\x10\x02\x18\x03\x0a\x06\x08\x00\x10\x03\x18\x02\x0a\x06\x08\x02\x10\x03\x18\x02\x0a\x06\x08\x01\x10\x03\x18\x02\x0a\x06\x08\x00\x10\x04\x18\x02\x0a\x06\x08\x01\x10\x04\x18\x04\x0a\x06\x08\x00\x10\x05\x18\x02\x0a\x06\x08\x01\x10\x05\x18\x02\x12\x04\x08\x00\x10\x00\x12\x04\x08\x00\x10\x00\x12\x04\x08\x00\x10\x00\x12\x04\x08\x00\x10\x00\x12\x04\x08\x00\x10\x00\x12\x04\x08\x00\x10\x00\x1a\x03\x47\x4d\x54\x0a\x59\x0a\x04\x08\x01\x50\x00\x0a\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x0a\x08\x08\x01\x42\x02\x08\x06\x50\x00\x0a\x08\x08\x01\x42\x02\x08\x04\x50\x00\x0a\x21\x08\x01\x1a\x1b\x09\x00\x00\x00\x00\x00\x00\xe0\x3f\x11\x00\x00\x00\x00\x00\x00\xe0\x3f\x19\x00\x00\x00\x00\x00\x00\xe0\x3f\x50\x00\x0a\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x08\x03\x10\xec\x02\x1a\x0c\x08\x03\x10\x8e\x01\x18\x1d\x20\xc1\x01\x28\x01\x22\x2e\x08\x0c\x12\x05\x01\x02\x03\x04\x05\x1a\x02\x69\x64\x1a\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x1a\x04\x76\x61\x6c\x31\x1a\x04\x76\x61\x6c\x32\x1a\x04\x76\x61\x6c\x33\x20\x00\x28\x00\x30\x00\x22\x08\x08\x04\x20\x00\x28\x00\x30\x00\x22\x08\x08\x08\x20\x00\x28\x00\x30\x00\x22\x08\x08\x08\x20\x00\x28\x00\x30\x00\x22\x08\x08\x05\x20\x00\x28\x00\x30\x00\x22\x08\x08\x01\x20\x00\x28\x00\x30\x00\x30\x01\x3a\x04\x08\x01\x50\x00\x3a\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x3a\x08\x08\x01\x42\x02\x08\x06\x50\x00\x3a\x08\x08\x01\x42\x02\x08\x04\x50\x00\x3a\x21\x08\x01\x1a\x1b\x09\x00\x00\x00\x00\x00\x00\xe0\x3f\x11\x00\x00\x00\x00\x00\x00\xe0\x3f\x19\x00\x00\x00\x00\x00\x00\xe0\x3f\x50\x00\x3a\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x40\x90\x4e\x48\x01\x08\xd5\x01\x10\x00\x18\x80\x80\x04\x22\x02\x00\x0b\x28\x5b\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18',
            ],
            'expected':r'''{"raw_message":"4F52430A0B0A030000001204080150000A150A050000000000120C0801120608001000180050000A120A06000000000000120808014202080650000A120A06000000000000120808014202080450000A290A0400000000122108011A1B09000000000000E03F11000000000000E03F19000000000000E03F50000A150A050000000000120C080112060802100218025000FF80FF80FF00FF80FF03424144FF80FF02414DFF800000003FFF80FF010A0608061000180D0A060806100118170A060806100218140A060806100318140A0608061004182B0A060806100518170A060800100018020A060800100118020A060801100118020A060800100218020A060802100218020A060801100218030A060800100318020A060802100318020A060801100318020A060800100418020A060801100418040A060800100518020A060801100518021204080010001204080010001204080010001204080010001204080010001204080010001A03474D540A590A04080150000A0C0801120608001000180050000A0808014202080650000A0808014202080450000A2108011A1B09000000000000E03F11000000000000E03F19000000000000E03F50000A0C080112060802100218025000080310EC021A0C0803108E01181D20C1012801222E080C120501020304051A0269641A07626C6F636B4E6F1A0476616C311A0476616C321A0476616C33200028003000220808042000280030002208080820002800300022080808200028003000220808052000280030002208080120002800300030013A04080150003A0C0801120608001000180050003A0808014202080650003A0808014202080450003A2108011A1B09000000000000E03F11000000000000E03F19000000000000E03F50003A0C08011206080210021802500040904E480108D5011000188080042202000B285B300682F403034F524318","error":"Cannot parse string 'BAD' as UInt16: syntax error at begin of string. Note: there are toUInt16OrZero and toUInt16OrNull functions, which returns zero\/NULL instead of throwing exception."}''',
            'printable':False,
        }
    }

    topic_name_prefix = 'format_tests_4_stream_'
    for format_name, format_opts in list(all_formats.items()):
        print(('Set up {}'.format(format_name)))
        topic_name = topic_name_prefix + '{}'.format(format_name)
        data_sample = format_opts['data_sample']
        data_prefix = []
        raw_message = '_raw_message'
        # prepend empty value when supported
        if format_opts.get('supports_empty_value', False):
            data_prefix = data_prefix + ['']
        if format_opts.get('printable', False) == False:
            raw_message = 'hex(_raw_message)'
        kafka_produce(kafka_cluster, topic_name, data_prefix + data_sample)
        instance.query('''
            DROP TABLE IF EXISTS test.kafka_{format_name};

            CREATE TABLE test.kafka_{format_name} (
                id Int64,
                blockNo UInt16,
                val1 String,
                val2 Float32,
                val3 UInt8
            ) ENGINE = Kafka()
                SETTINGS kafka_broker_list = 'kafka1:19092',
                        kafka_topic_list = '{topic_name}',
                        kafka_group_name = '{topic_name}',
                        kafka_format = '{format_name}',
                        kafka_handle_error_mode = 'stream',
                        kafka_flush_interval_ms = 1000 {extra_settings};

            DROP TABLE IF EXISTS test.kafka_data_{format_name}_mv;
            CREATE MATERIALIZED VIEW test.kafka_data_{format_name}_mv Engine=Log AS
                SELECT *, _topic, _partition, _offset FROM test.kafka_{format_name}
                WHERE length(_error) = 0;

            DROP TABLE IF EXISTS test.kafka_errors_{format_name}_mv;
            CREATE MATERIALIZED VIEW test.kafka_errors_{format_name}_mv Engine=Log AS
                SELECT {raw_message} as raw_message, _error as error, _topic as topic, _partition as partition, _offset as offset FROM test.kafka_{format_name}
                WHERE length(_error) > 0;
            '''.format(topic_name=topic_name, format_name=format_name, raw_message=raw_message,
                       extra_settings=format_opts.get('extra_settings') or ''))

    for format_name, format_opts in list(all_formats.items()):
        print(('Checking {}'.format(format_name)))
        topic_name = topic_name_prefix + '{}'.format(format_name)
        # shift offsets by 1 if format supports empty value
        offsets = [1, 2, 3] if format_opts.get('supports_empty_value', False) else [0, 1, 2]
        result = instance.query('SELECT * FROM test.kafka_data_{format_name}_mv;'.format(format_name=format_name))
        expected = '''\
0	0	AM	0.5	1	{topic_name}	0	{offset_0}
1	0	AM	0.5	1	{topic_name}	0	{offset_1}
2	0	AM	0.5	1	{topic_name}	0	{offset_1}
3	0	AM	0.5	1	{topic_name}	0	{offset_1}
4	0	AM	0.5	1	{topic_name}	0	{offset_1}
5	0	AM	0.5	1	{topic_name}	0	{offset_1}
6	0	AM	0.5	1	{topic_name}	0	{offset_1}
7	0	AM	0.5	1	{topic_name}	0	{offset_1}
8	0	AM	0.5	1	{topic_name}	0	{offset_1}
9	0	AM	0.5	1	{topic_name}	0	{offset_1}
10	0	AM	0.5	1	{topic_name}	0	{offset_1}
11	0	AM	0.5	1	{topic_name}	0	{offset_1}
12	0	AM	0.5	1	{topic_name}	0	{offset_1}
13	0	AM	0.5	1	{topic_name}	0	{offset_1}
14	0	AM	0.5	1	{topic_name}	0	{offset_1}
15	0	AM	0.5	1	{topic_name}	0	{offset_1}
0	0	AM	0.5	1	{topic_name}	0	{offset_2}
'''.format(topic_name=topic_name, offset_0=offsets[0], offset_1=offsets[1], offset_2=offsets[2])
        # print(('Checking result\n {result} \n expected \n {expected}\n'.format(result=str(result), expected=str(expected))))
        assert TSV(result) == TSV(expected), 'Proper result for format: {}'.format(format_name)
        errors_result = instance.query('SELECT raw_message, error FROM test.kafka_errors_{format_name}_mv format JSONEachRow'.format(format_name=format_name))
        errors_expected = format_opts['expected']
        # print(errors_result.strip())
        # print(errors_expected.strip())
        assert  errors_result.strip() == errors_expected.strip(), 'Proper errors for format: {}'.format(format_name)

def wait_for_new_data(table_name, prev_count = 0, max_retries = 120):
    retries = 0
    while True:
        new_count = int(instance.query("SELECT count() FROM {}".format(table_name)))
        print(new_count)
        if new_count > prev_count:
            return new_count
        else:
            retries += 1
            time.sleep(0.5)
            if retries > max_retries:
                raise Exception("No new data :(")

def test_kafka_consumer_failover(kafka_cluster):

    # for backporting:
    # admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))

    topic_list = []
    topic_list.append(NewTopic(name="kafka_consumer_failover", num_partitions=2, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    instance.query('''
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka2;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'kafka_consumer_failover',
                     kafka_group_name = 'kafka_consumer_failover_group',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 1,
                     kafka_poll_timeout_ms = 200;

        CREATE TABLE test.kafka2 (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'kafka_consumer_failover',
                     kafka_group_name = 'kafka_consumer_failover_group',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 1,
                     kafka_poll_timeout_ms = 200;

        CREATE TABLE test.kafka3 (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'kafka_consumer_failover',
                     kafka_group_name = 'kafka_consumer_failover_group',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 1,
                     kafka_poll_timeout_ms = 200;

        CREATE TABLE test.destination (
            key UInt64,
            value UInt64,
            _consumed_by LowCardinality(String)
        )
        ENGINE = MergeTree()
        ORDER BY key;

        CREATE MATERIALIZED VIEW test.kafka_mv TO test.destination AS
        SELECT key, value, 'kafka' as _consumed_by
        FROM test.kafka;

        CREATE MATERIALIZED VIEW test.kafka2_mv TO test.destination AS
        SELECT key, value, 'kafka2' as _consumed_by
        FROM test.kafka2;

        CREATE MATERIALIZED VIEW test.kafka3_mv TO test.destination AS
        SELECT key, value, 'kafka3' as _consumed_by
        FROM test.kafka3;
        ''')


    producer = KafkaProducer(bootstrap_servers="localhost:{}".format(cluster.kafka_port), value_serializer=producer_serializer, key_serializer=producer_serializer)

    ## all 3 attached, 2 working
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':1,'value': 1}), partition=0)
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':1,'value': 1}), partition=1)
    producer.flush()
    prev_count = wait_for_new_data('test.destination')

    ## 2 attached, 2 working
    instance.query('DETACH TABLE test.kafka')
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':2,'value': 2}), partition=0)
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':2,'value': 2}), partition=1)
    producer.flush()
    prev_count = wait_for_new_data('test.destination', prev_count)

    ## 1 attached, 1 working
    instance.query('DETACH TABLE test.kafka2')
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':3,'value': 3}), partition=0)
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':3,'value': 3}), partition=1)
    producer.flush()
    prev_count = wait_for_new_data('test.destination', prev_count)

    ## 2 attached, 2 working
    instance.query('ATTACH TABLE test.kafka')
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':4,'value': 4}), partition=0)
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':4,'value': 4}), partition=1)
    producer.flush()
    prev_count = wait_for_new_data('test.destination', prev_count)

    ## 1 attached, 1 working
    instance.query('DETACH TABLE test.kafka3')
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':5,'value': 5}), partition=0)
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':5,'value': 5}), partition=1)
    producer.flush()
    prev_count = wait_for_new_data('test.destination', prev_count)

    ## 2 attached, 2 working
    instance.query('ATTACH TABLE test.kafka2')
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':6,'value': 6}), partition=0)
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':6,'value': 6}), partition=1)
    producer.flush()
    prev_count = wait_for_new_data('test.destination', prev_count)

    ## 3 attached, 2 working
    instance.query('ATTACH TABLE test.kafka3')
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':7,'value': 7}), partition=0)
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':7,'value': 7}), partition=1)
    producer.flush()
    prev_count = wait_for_new_data('test.destination', prev_count)

    ## 2 attached, same 2 working
    instance.query('DETACH TABLE test.kafka3')
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':8,'value': 8}), partition=0)
    producer.send(topic='kafka_consumer_failover', value=json.dumps({'key':8,'value': 8}), partition=1)
    producer.flush()
    prev_count = wait_for_new_data('test.destination', prev_count)


# https://github.com/ClickHouse/ClickHouse/issues/26643
def test_issue26643(kafka_cluster):

    # for backporting:
    # admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port))
    producer = KafkaProducer(bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port), value_serializer=producer_serializer)

    topic_list = []
    topic_list.append(NewTopic(name="test_issue26643", num_partitions=4, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    msg = message_with_repeated_pb2.Message(
        tnow=1629000000,
        server='server1',
        clien='host1',
        sPort=443,
        cPort=50000,
        r=[
            message_with_repeated_pb2.dd(name='1', type=444, ttl=123123, data=b'adsfasd'),
            message_with_repeated_pb2.dd(name='2')
        ],
        method='GET'
    )

    data = b''
    serialized_msg = msg.SerializeToString()
    data = data + _VarintBytes(len(serialized_msg)) + serialized_msg

    msg = message_with_repeated_pb2.Message(
        tnow=1629000002
    )

    serialized_msg = msg.SerializeToString()
    data = data + _VarintBytes(len(serialized_msg)) + serialized_msg

    producer.send(topic="test_issue26643", value=data)

    data = _VarintBytes(len(serialized_msg)) + serialized_msg
    producer.send(topic="test_issue26643", value=data)
    producer.flush()

    instance.query('''
        CREATE TABLE IF NOT EXISTS test.test_queue
        (
            `tnow` UInt32,
            `server` String,
            `client` String,
            `sPort` UInt16,
            `cPort` UInt16,
            `r.name` Array(String),
            `r.class` Array(UInt16),
            `r.type` Array(UInt16),
            `r.ttl` Array(UInt32),
            `r.data` Array(String),
            `method` String
        )
        ENGINE = Kafka
        SETTINGS
            kafka_broker_list = 'kafka1:19092',
            kafka_topic_list = 'test_issue26643',
            kafka_group_name = 'test_issue26643_group',
            kafka_format = 'Protobuf',
            kafka_schema = 'message_with_repeated.proto:Message',
            kafka_num_consumers = 4,
            kafka_skip_broken_messages = 10000;

        SET allow_suspicious_low_cardinality_types=1; 

        CREATE TABLE test.log
        (
            `tnow` DateTime CODEC(DoubleDelta, LZ4),
            `server` LowCardinality(String),
            `client` LowCardinality(String),
            `sPort` LowCardinality(UInt16),
            `cPort` UInt16 CODEC(T64, LZ4),
            `r.name` Array(String),
            `r.class` Array(LowCardinality(UInt16)),
            `r.type` Array(LowCardinality(UInt16)),
            `r.ttl` Array(LowCardinality(UInt32)),
            `r.data` Array(String),
            `method` LowCardinality(String)
        )
        ENGINE = MergeTree
        PARTITION BY toYYYYMMDD(tnow)
        ORDER BY (tnow, server)
        TTL toDate(tnow) + toIntervalMonth(1000)
        SETTINGS index_granularity = 16384, merge_with_ttl_timeout = 7200;

        CREATE MATERIALIZED VIEW test.test_consumer TO test.log AS
        SELECT
            toDateTime(a.tnow) AS tnow,
            a.server AS server,
            a.client AS client,
            a.sPort AS sPort,
            a.cPort AS cPort,
            a.`r.name` AS `r.name`,
            a.`r.class` AS `r.class`,
            a.`r.type` AS `r.type`,
            a.`r.ttl` AS `r.ttl`,
            a.`r.data` AS `r.data`,
            a.method AS method
        FROM test.test_queue AS a;
        ''')

    instance.wait_for_log_line("Committed offset")
    result = instance.query('SELECT * FROM test.log')

    expected = '''\
2021-08-15 07:00:00	server1		443	50000	['1','2']	[0,0]	[444,0]	[123123,0]	['adsfasd','']	GET
2021-08-15 07:00:02			0	0	[]	[]	[]	[]	[]	
2021-08-15 07:00:02			0	0	[]	[]	[]	[]	[]
'''
    assert TSV(result) == TSV(expected)

    # kafka_cluster.open_bash_shell('instance')


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
