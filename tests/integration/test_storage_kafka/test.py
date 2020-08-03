import os.path as p
import random
import threading
import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.client import QueryRuntimeException
from helpers.network import PartitionManager

import json
import subprocess
import kafka.errors
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer, BrokerConnection
from kafka.admin import NewTopic
from kafka.protocol.admin import DescribeGroupsResponse_v1, DescribeGroupsRequest_v1
from kafka.protocol.group import MemberAssignment
import socket
from google.protobuf.internal.encoder import _VarintBytes

"""
protoc --version
libprotoc 3.0.0

# to create kafka_pb2.py
protoc --python_out=. kafka.proto
"""
import kafka_pb2


# TODO: add test for run-time offset update in CH, if we manually update it on Kafka side.
# TODO: add test for SELECT LIMIT is working.

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir='configs',
                                main_configs=['configs/kafka.xml', 'configs/log_conf.xml' ],
                                with_kafka=True,
                                with_zookeeper=True,
                                clickhouse_path_dir='clickhouse_path')
kafka_id = ''


# Helpers

def check_kafka_is_available():
    p = subprocess.Popen(('docker',
                          'exec',
                          '-i',
                          kafka_id,
                          '/usr/bin/kafka-broker-api-versions',
                          '--bootstrap-server',
                          'INSIDE://localhost:9092'),
                         stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


def wait_kafka_is_available(max_retries=50):
    retries = 0
    while True:
        if check_kafka_is_available():
            break
        else:
            retries += 1
            if retries > max_retries:
                raise "Kafka is not available"
            print("Waiting for Kafka to start up")
            time.sleep(1)


def kafka_produce(topic, messages, timestamp=None):
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    for message in messages:
        producer.send(topic=topic, value=message, timestamp_ms=timestamp)
        producer.flush()
#    print ("Produced {} messages for topic {}".format(len(messages), topic))


def kafka_consume(topic):
    consumer = KafkaConsumer(bootstrap_servers="localhost:9092", auto_offset_reset="earliest")
    consumer.subscribe(topics=(topic))
    for toppar, messages in consumer.poll(5000).items():
        if toppar.topic == topic:
            for message in messages:
                yield message.value
    consumer.unsubscribe()
    consumer.close()


def kafka_produce_protobuf_messages(topic, start_index, num_messages):
    data = ''
    for i in range(start_index, start_index + num_messages):
        msg = kafka_pb2.KeyValuePair()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    producer.send(topic=topic, value=data)
    producer.flush()
    print("Produced {} messages for topic {}".format(num_messages, topic))


@pytest.mark.timeout(180)
def test_kafka_json_as_string(kafka_cluster):
    kafka_produce('kafka_json_as_string', ['{"t": 123, "e": {"x": "woof"} }', '', '{"t": 124, "e": {"x": "test"} }', '{"F1":"V1","F2":{"F21":"V21","F22":{},"F23":"V23","F24":"2019-12-24T16:28:04"},"F3":"V3"}'])

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
    assert instance.contains_in_log("Parsing of message (topic: kafka_json_as_string, partition: 0, offset: 1) return no rows")

@pytest.mark.timeout(300)
def test_kafka_formats(kafka_cluster):
    # data was dumped from clickhouse itself in a following manner
    # clickhouse-client --format=Native --query='SELECT toInt64(number) as id, toUInt16( intDiv( id, 65536 ) ) as blockNo, reinterpretAsString(19777) as val1, toFloat32(0.5) as val2, toUInt8(1) as val3 from numbers(100) ORDER BY id' | xxd -ps | tr -d '\n' | sed 's/\(..\)/\\x\1/g'

    all_formats = {
        ## Text formats ##
        # dumped with clickhouse-client ... | perl -pe 's/\n/\\n/; s/\t/\\t/g;'
        'JSONEachRow' : {
            'data_sample' : [
                '{"id":"0","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
                '{"id":"1","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"2","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"3","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"4","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"5","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"6","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"7","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"8","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"9","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"10","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"11","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"12","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"13","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"14","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"15","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
                '{"id":"0","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
                '' # tolerates
            ],
        },
        # JSONAsString doesn't fit to that test, and tested separately
        'JSONCompactEachRow' : {
            'data_sample' : [
                '["0", 0, "AM", 0.5, 1]\n',
                '["1", 0, "AM", 0.5, 1]\n["2", 0, "AM", 0.5, 1]\n["3", 0, "AM", 0.5, 1]\n["4", 0, "AM", 0.5, 1]\n["5", 0, "AM", 0.5, 1]\n["6", 0, "AM", 0.5, 1]\n["7", 0, "AM", 0.5, 1]\n["8", 0, "AM", 0.5, 1]\n["9", 0, "AM", 0.5, 1]\n["10", 0, "AM", 0.5, 1]\n["11", 0, "AM", 0.5, 1]\n["12", 0, "AM", 0.5, 1]\n["13", 0, "AM", 0.5, 1]\n["14", 0, "AM", 0.5, 1]\n["15", 0, "AM", 0.5, 1]\n',
                '["0", 0, "AM", 0.5, 1]\n',
                '' # tolerates
            ],
        },
        'JSONCompactEachRowWithNamesAndTypes' : {
            'data_sample' : [
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
        'TSKV' : {
            'data_sample' : [
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
        'CSV' : {
            'data_sample' : [
                '0,0,"AM",0.5,1\n',
                '1,0,"AM",0.5,1\n2,0,"AM",0.5,1\n3,0,"AM",0.5,1\n4,0,"AM",0.5,1\n5,0,"AM",0.5,1\n6,0,"AM",0.5,1\n7,0,"AM",0.5,1\n8,0,"AM",0.5,1\n9,0,"AM",0.5,1\n10,0,"AM",0.5,1\n11,0,"AM",0.5,1\n12,0,"AM",0.5,1\n13,0,"AM",0.5,1\n14,0,"AM",0.5,1\n15,0,"AM",0.5,1\n',
                '0,0,"AM",0.5,1\n',
                '' # tolerates
            ],
        },
        'TSV' : {
            'data_sample' : [
                '0\t0\tAM\t0.5\t1\n',
                '1\t0\tAM\t0.5\t1\n2\t0\tAM\t0.5\t1\n3\t0\tAM\t0.5\t1\n4\t0\tAM\t0.5\t1\n5\t0\tAM\t0.5\t1\n6\t0\tAM\t0.5\t1\n7\t0\tAM\t0.5\t1\n8\t0\tAM\t0.5\t1\n9\t0\tAM\t0.5\t1\n10\t0\tAM\t0.5\t1\n11\t0\tAM\t0.5\t1\n12\t0\tAM\t0.5\t1\n13\t0\tAM\t0.5\t1\n14\t0\tAM\t0.5\t1\n15\t0\tAM\t0.5\t1\n',
                '0\t0\tAM\t0.5\t1\n',
                '' # tolerates
            ],
        },
        'CSVWithNames' : {
            'data_sample' : [
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
        'Values' : {
            'data_sample' : [
                "(0,0,'AM',0.5,1)",
                "(1,0,'AM',0.5,1),(2,0,'AM',0.5,1),(3,0,'AM',0.5,1),(4,0,'AM',0.5,1),(5,0,'AM',0.5,1),(6,0,'AM',0.5,1),(7,0,'AM',0.5,1),(8,0,'AM',0.5,1),(9,0,'AM',0.5,1),(10,0,'AM',0.5,1),(11,0,'AM',0.5,1),(12,0,'AM',0.5,1),(13,0,'AM',0.5,1),(14,0,'AM',0.5,1),(15,0,'AM',0.5,1)",
                "(0,0,'AM',0.5,1)",
                '' # tolerates
            ],
        },
        'TSVWithNames' : {
            'data_sample' : [
                'id\tblockNo\tval1\tval2\tval3\n0\t0\tAM\t0.5\t1\n',
                'id\tblockNo\tval1\tval2\tval3\n1\t0\tAM\t0.5\t1\n2\t0\tAM\t0.5\t1\n3\t0\tAM\t0.5\t1\n4\t0\tAM\t0.5\t1\n5\t0\tAM\t0.5\t1\n6\t0\tAM\t0.5\t1\n7\t0\tAM\t0.5\t1\n8\t0\tAM\t0.5\t1\n9\t0\tAM\t0.5\t1\n10\t0\tAM\t0.5\t1\n11\t0\tAM\t0.5\t1\n12\t0\tAM\t0.5\t1\n13\t0\tAM\t0.5\t1\n14\t0\tAM\t0.5\t1\n15\t0\tAM\t0.5\t1\n',
                'id\tblockNo\tval1\tval2\tval3\n0\t0\tAM\t0.5\t1\n',
                '' # tolerates
            ],
        },
        'TSVWithNamesAndTypes' : {
            'data_sample' : [
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
        'Regexp' : {
            'data_sample' : [
                '(id = 0, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)',
                '(id = 1, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 2, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 3, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 4, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 5, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 6, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 7, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 8, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 9, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 10, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 11, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 12, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 13, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 14, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)\n(id = 15, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)',
                '(id = 0, blockNo = 0, val1 = "AM", val2 = 0.5, val3 = 1)',
                # ''
                # On empty message exception happens: Line "" doesn't match the regexp.: (at row 1)
                # /src/Processors/Formats/Impl/RegexpRowInputFormat.cpp:140: DB::RegexpRowInputFormat::readRow(std::__1::vector<COW<DB::IColumn>::mutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::mutable_ptr<DB::IColumn> > >&, DB::RowReadExtension&) @ 0x1df82fcb in /usr/bin/clickhouse
            ],
            'extra_settings': ", format_regexp='\(id = (.+?), blockNo = (.+?), val1 = \"(.+?)\", val2 = (.+?), val3 = (.+?)\)', format_regexp_escaping_rule='Escaped'"
        },

        ## BINARY FORMATS
        # dumped with
        # clickhouse-client ... | xxd -ps -c 200 | tr -d '\n' | sed 's/\(..\)/\\x\1/g'
        'Native' : {
            'data_sample': [
                '\x05\x01\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x00\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01',
                '\x05\x0f\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01',
                '\x05\x01\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x00\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01',
                # ''
                # On empty message exception happens: DB::Exception: Attempt to read after eof
                # /src/IO/VarInt.h:122: DB::throwReadAfterEOF() @ 0x15c34487 in /usr/bin/clickhouse
                # /src/IO/VarInt.h:135: void DB::readVarUIntImpl<false>(unsigned long&, DB::ReadBuffer&) @ 0x15c68bb7 in /usr/bin/clickhouse
                # /src/IO/VarInt.h:149: DB::readVarUInt(unsigned long&, DB::ReadBuffer&) @ 0x15c68844 in /usr/bin/clickhouse
                # /src/DataStreams/NativeBlockInputStream.cpp:124: DB::NativeBlockInputStream::readImpl() @ 0x1d3e2778 in /usr/bin/clickhouse
                # /src/DataStreams/IBlockInputStream.cpp:60: DB::IBlockInputStream::read() @ 0x1c9c92fd in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/NativeFormat.cpp:42: DB::NativeInputFormatFromNativeBlockInputStream::generate() @ 0x1df1ea79 in /usr/bin/clickhouse
                # /src/Processors/ISource.cpp:48: DB::ISource::work() @ 0x1dd79737 in /usr/bin/clickhouse
            ],
        },
        'MsgPack' : {
            'data_sample' : [
                '\x00\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01',
                '\x01\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x02\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x03\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x04\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x05\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x06\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x07\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x08\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x09\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0a\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0b\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0c\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0d\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0e\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01\x0f\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01',
                '\x00\x00\xa2\x41\x4d\xca\x3f\x00\x00\x00\x01',
                # ''
                # On empty message exception happens: Unexpected end of file while parsing msgpack object.: (at row 1)
                # coming from Processors/Formats/Impl/MsgPackRowInputFormat.cpp:170
            ],
        },
        'RowBinary' : {
            'data_sample' : [
                '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                '\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                # ''
                # On empty message exception happens: DB::Exception: Cannot read all data. Bytes read: 0. Bytes expected: 8.
                # /src/IO/ReadBuffer.h:157: DB::ReadBuffer::readStrict(char*, unsigned long) @ 0x15c6894d in /usr/bin/clickhouse
                # /src/IO/ReadHelpers.h:108: void DB::readPODBinary<long>(long&, DB::ReadBuffer&) @ 0x15c67715 in /usr/bin/clickhouse
                # /src/IO/ReadHelpers.h:737: std::__1::enable_if<is_arithmetic_v<long>, void>::type DB::readBinary<long>(long&, DB::ReadBuffer&) @ 0x15e7afbd in /usr/bin/clickhouse
                # /src/DataTypes/DataTypeNumberBase.cpp:180: DB::DataTypeNumberBase<long>::deserializeBinary(DB::IColumn&, DB::ReadBuffer&) const @ 0x1cace581 in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/BinaryRowInputFormat.cpp:22: DB::BinaryRowInputFormat::readRow(std::__1::vector<COW<DB::IColumn>::mutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::mutable_ptr<DB::IColumn> > >&, DB::RowReadExtension&) @ 0x1dea2c0b in /usr/bin/clickhouse
            ],
        },
        'RowBinaryWithNamesAndTypes' : {
            'data_sample' : [
                '\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                '\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                '\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01',
                #''
                # !!! On empty message segfault: Address not mapped to object
                # /contrib/FastMemcpy/FastMemcpy.h:666: memcpy_fast @ 0x21742d65 in /usr/bin/clickhouse
                # /contrib/FastMemcpy/memcpy_wrapper.c:5: memcpy @ 0x21738235 in /usr/bin/clickhouse
                # /src/IO/ReadBuffer.h:145: DB::ReadBuffer::read(char*, unsigned long) @ 0x15c369d7 in /usr/bin/clickhouse
                # /src/IO/ReadBuffer.h:155: DB::ReadBuffer::readStrict(char*, unsigned long) @ 0x15c68878 in /usr/bin/clickhouse
                # /src/DataTypes/DataTypeString.cpp:84: DB::DataTypeString::deserializeBinary(DB::IColumn&, DB::ReadBuffer&) const @ 0x1cad12e7 in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/BinaryRowInputFormat.cpp:22: DB::BinaryRowInputFormat::readRow(std::__1::vector<COW<DB::IColumn>::mutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::mutable_ptr<DB::IColumn> > >&, DB::RowReadExtension&) @ 0x1dea2c0b in /usr/bin/clickhouse
            ],
        },
        'Protobuf' : {
            'data_sample' : [
                '\x0b\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01',
                '\x0d\x08\x01\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x02\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x03\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x04\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x05\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x06\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x07\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x08\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x09\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0a\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0b\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0c\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0d\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0e\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01\x0d\x08\x0f\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01',
                '\x0b\x1a\x02\x41\x4d\x25\x00\x00\x00\x3f\x28\x01',
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
        'ORC' : {
            'data_sample' : [
                '\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x01\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x46\x25\x0e\x2e\x46\x03\x21\x46\x03\x09\xa6\x00\x06\x00\x32\x00\x00\xe3\x92\xe4\x62\x65\x00\x01\x21\x01\x0e\x46\x25\x2e\x2e\x26\x47\x5f\x21\x20\x96\x60\x09\x60\x00\x00\x36\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x46\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x10\x11\xc0\x00\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x05\x00\x00\xff\x00\x03\x00\x00\x30\x07\x00\x00\x40\x00\x80\x05\x00\x00\x41\x4d\x07\x00\x00\x42\x00\x80\x03\x00\x00\x0a\x07\x00\x00\x42\x00\x80\x05\x00\x00\xff\x01\x88\x00\x00\x4d\xca\xc1\x0a\x80\x30\x0c\x03\xd0\x2e\x6b\xcb\x98\x17\xf1\x14\x50\xfc\xff\xcf\xb4\x66\x1e\x3c\x84\x47\x9a\xce\x1c\xb9\x1b\xb7\xf9\xda\x48\x09\x9e\xb2\xf3\x92\xce\x5b\x86\xf6\x56\x7f\x21\x41\x2f\x51\xa6\x7a\xd7\x1d\xe5\xea\xae\x3d\xca\xd5\x83\x71\x60\xd8\x17\xfc\x62\x0f\xa8\x00\x00\xe3\x4a\xe6\x62\xe1\x60\x0c\x60\xe0\xe2\xe3\x60\x14\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\x60\x54\xe2\xe0\x62\x34\x10\x62\x34\x90\x60\x02\x8a\x70\x71\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\x82\x05\x28\xc6\xcd\x25\xca\xc1\x68\xc4\x0b\x52\xc5\x6c\xa0\x67\x2a\x05\x22\xc0\x4a\x21\x86\x31\x09\x30\x81\xb5\xb2\x02\x00\x36\x01\x00\x25\x8c\xbd\x0a\xc2\x30\x14\x85\x73\x6f\x92\xf6\x92\x6a\x09\x01\x21\x64\x92\x4e\x75\x91\x58\x71\xc9\x64\x27\x5d\x2c\x1d\x5d\xfd\x59\xc4\x42\x37\x5f\xc0\x17\xe8\x23\x9b\xc6\xe1\x3b\x70\x0f\xdf\xb9\xc4\xf5\x17\x5d\x41\x5c\x4f\x60\x37\xeb\x53\x0d\x55\x4d\x0b\x23\x01\xb9\x90\x2e\xbf\x0f\xe3\xe3\xdd\x8d\x0e\x5f\x4f\x27\x3e\xb7\x61\x97\xb2\x49\xb9\xaf\x90\x20\x92\x27\x32\x2a\x6b\xf4\xf3\x0d\x1e\x82\x20\xe8\x59\x28\x09\x4c\x46\x4c\x33\xcb\x7a\x76\x95\x41\x47\x9f\x14\x78\x03\xde\x62\x6c\x54\x30\xb1\x51\x0a\xdb\x8b\x89\x58\x11\xbb\x22\xac\x08\x9a\xe5\x6c\x71\xbf\x3d\xb8\x39\x92\xfa\x7f\x86\x1a\xd3\x54\x1e\xa7\xee\xcc\x7e\x08\x9e\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x57\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18',
                '\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x0f\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x0f\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x7e\x25\x0e\x2e\x46\x43\x21\x46\x4b\x09\xad\x00\x06\x00\x33\x00\x00\x0a\x17\x0a\x03\x00\x00\x00\x12\x10\x08\x0f\x22\x0a\x0a\x02\x41\x4d\x12\x02\x41\x4d\x18\x3c\x50\x00\x3a\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x7e\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x66\x73\x3d\xd3\x00\x06\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x0f\x12\x06\x08\x02\x10\x02\x18\x1e\x50\x00\x05\x00\x00\x0c\x00\x2b\x00\x00\x31\x32\x33\x34\x35\x36\x37\x38\x39\x31\x30\x31\x31\x31\x32\x31\x33\x31\x34\x31\x35\x09\x00\x00\x06\x01\x03\x02\x09\x00\x00\xc0\x0e\x00\x00\x07\x00\x00\x42\x00\x80\x05\x00\x00\x41\x4d\x0a\x00\x00\xe3\xe2\x42\x01\x00\x09\x00\x00\xc0\x0e\x02\x00\x05\x00\x00\x0c\x01\x94\x00\x00\x2d\xca\xc1\x0e\x80\x30\x08\x03\xd0\xc1\x60\x2e\xf3\x62\x76\x6a\xe2\x0e\xfe\xff\x57\x5a\x3b\x0f\xe4\x51\xe8\x68\xbd\x5d\x05\xe7\xf8\x34\x40\x3a\x6e\x59\xb1\x64\xe0\x91\xa9\xbf\xb1\x97\xd2\x95\x9d\x1e\xca\x55\x3a\x6d\xb4\xd2\xdd\x0b\x74\x9a\x74\xf7\x12\x39\xbd\x97\x7f\x7c\x06\xbb\xa6\x8d\x97\x17\xb4\x00\x00\xe3\x4a\xe6\x62\xe1\xe0\x0f\x60\xe0\xe2\xe3\xe0\x17\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\xe0\x57\xe2\xe0\x62\x34\x14\x62\xb4\x94\xd0\x02\x8a\xc8\x73\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\xc2\x06\x28\x26\xc4\x25\xca\xc1\x6f\xc4\xcb\xc5\x68\x20\xc4\x6c\xa0\x67\x2a\xc5\x6c\xae\x67\x0a\x14\xe6\x87\x1a\xc6\x24\xc0\x24\x21\x07\x32\x0c\x00\x4a\x01\x00\xe3\x60\x16\x58\xc3\x24\xc5\xcd\xc1\x2c\x30\x89\x51\xc2\x4b\xc1\x57\x83\x5f\x49\x83\x83\x47\x88\x95\x91\x89\x99\x85\x55\x8a\x3d\x29\x27\x3f\x39\xdb\x2f\x5f\x8a\x29\x33\x45\x8a\xa5\x2c\x31\xc7\x10\x4c\x1a\x81\x49\x63\x25\x26\x0e\x46\x20\x66\x07\x63\x36\x0e\x3e\x0d\x26\x03\x10\x9f\xd1\x80\xdf\x8a\x85\x83\x3f\x80\xc1\x8a\x8f\x83\x5f\x88\x8d\x83\x41\x80\x41\x82\x21\x80\x21\x82\xd5\x4a\x80\x83\x5f\x89\x83\x8b\xd1\x50\x88\xd1\x52\x42\x0b\x28\x22\x6f\x25\x04\x14\xe1\xe2\x62\x72\xf4\x15\x02\x62\x09\x1b\xa0\x98\x90\x95\x28\x07\xbf\x11\x2f\x17\xa3\x81\x10\xb3\x81\x9e\xa9\x14\xb3\xb9\x9e\x29\x50\x98\x1f\x6a\x18\x93\x00\x93\x84\x1c\xc8\x30\x87\x09\x7e\x1e\x0c\x00\x08\xa8\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x5d\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18',
                '\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x01\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x46\x25\x0e\x2e\x46\x03\x21\x46\x03\x09\xa6\x00\x06\x00\x32\x00\x00\xe3\x92\xe4\x62\x65\x00\x01\x21\x01\x0e\x46\x25\x2e\x2e\x26\x47\x5f\x21\x20\x96\x60\x09\x60\x00\x00\x36\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x46\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x10\x11\xc0\x00\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x05\x00\x00\xff\x00\x03\x00\x00\x30\x07\x00\x00\x40\x00\x80\x05\x00\x00\x41\x4d\x07\x00\x00\x42\x00\x80\x03\x00\x00\x0a\x07\x00\x00\x42\x00\x80\x05\x00\x00\xff\x01\x88\x00\x00\x4d\xca\xc1\x0a\x80\x30\x0c\x03\xd0\x2e\x6b\xcb\x98\x17\xf1\x14\x50\xfc\xff\xcf\xb4\x66\x1e\x3c\x84\x47\x9a\xce\x1c\xb9\x1b\xb7\xf9\xda\x48\x09\x9e\xb2\xf3\x92\xce\x5b\x86\xf6\x56\x7f\x21\x41\x2f\x51\xa6\x7a\xd7\x1d\xe5\xea\xae\x3d\xca\xd5\x83\x71\x60\xd8\x17\xfc\x62\x0f\xa8\x00\x00\xe3\x4a\xe6\x62\xe1\x60\x0c\x60\xe0\xe2\xe3\x60\x14\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\x60\x54\xe2\xe0\x62\x34\x10\x62\x34\x90\x60\x02\x8a\x70\x71\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\x82\x05\x28\xc6\xcd\x25\xca\xc1\x68\xc4\x0b\x52\xc5\x6c\xa0\x67\x2a\x05\x22\xc0\x4a\x21\x86\x31\x09\x30\x81\xb5\xb2\x02\x00\x36\x01\x00\x25\x8c\xbd\x0a\xc2\x30\x14\x85\x73\x6f\x92\xf6\x92\x6a\x09\x01\x21\x64\x92\x4e\x75\x91\x58\x71\xc9\x64\x27\x5d\x2c\x1d\x5d\xfd\x59\xc4\x42\x37\x5f\xc0\x17\xe8\x23\x9b\xc6\xe1\x3b\x70\x0f\xdf\xb9\xc4\xf5\x17\x5d\x41\x5c\x4f\x60\x37\xeb\x53\x0d\x55\x4d\x0b\x23\x01\xb9\x90\x2e\xbf\x0f\xe3\xe3\xdd\x8d\x0e\x5f\x4f\x27\x3e\xb7\x61\x97\xb2\x49\xb9\xaf\x90\x20\x92\x27\x32\x2a\x6b\xf4\xf3\x0d\x1e\x82\x20\xe8\x59\x28\x09\x4c\x46\x4c\x33\xcb\x7a\x76\x95\x41\x47\x9f\x14\x78\x03\xde\x62\x6c\x54\x30\xb1\x51\x0a\xdb\x8b\x89\x58\x11\xbb\x22\xac\x08\x9a\xe5\x6c\x71\xbf\x3d\xb8\x39\x92\xfa\x7f\x86\x1a\xd3\x54\x1e\xa7\xee\xcc\x7e\x08\x9e\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x57\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18',
                #''
                # On empty message exception:  IOError: File size too small, Stack trace (when copying this message, always include the lines below):
                # /src/Processors/Formats/Impl/ORCBlockInputFormat.cpp:36: DB::ORCBlockInputFormat::generate() @ 0x1df282a6 in /usr/bin/clickhouse
                # /src/Processors/ISource.cpp:48: DB::ISource::work() @ 0x1dd79737 in /usr/bin/clickhouse
            ],
        },
        'CapnProto' : {
            'data_sample' : [
                '\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00',
                '\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00',
                '\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x02\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x3f\x01\x00\x00\x00\x1a\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00',
                # ''
                # On empty message exception: Cannot read all data. Bytes read: 0. Bytes expected: 4.
                # /src/IO/ReadBuffer.h:157: DB::ReadBuffer::readStrict(char*, unsigned long) @ 0x15c6894d in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/CapnProtoRowInputFormat.cpp:212: DB::CapnProtoRowInputFormat::readMessage() @ 0x1ded1cab in /usr/bin/clickhouse
                # /src/Processors/Formats/Impl/CapnProtoRowInputFormat.cpp:241: DB::CapnProtoRowInputFormat::readRow(std::__1::vector<COW<DB::IColumn>::mutable_ptr<DB::IColumn>, std::__1::allocator<COW<DB::IColumn>::mutable_ptr<DB::IColumn> > >&, DB::RowReadExtension&) @ 0x1ded205d in /usr/bin/clickhouse
            ],
            'extra_settings': ", kafka_schema='test:TestRecordStruct'"
        },


        # 'Parquet' : {
        # not working at all with Kafka: DB::Exception: IOError: Invalid Parquet file size is 0 bytes
        # /contrib/libcxx/include/exception:129: std::exception::capture() @ 0x15c33fe8 in /usr/bin/clickhouse
        # /contrib/libcxx/include/exception:109: std::exception::exception() @ 0x15c33fb5 in /usr/bin/clickhouse
        # /contrib/poco/Foundation/src/Exception.cpp:27: Poco::Exception::Exception(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, int) @ 0x21877833 in /usr/bin/clickhouse
        # /src/Common/Exception.cpp:37: DB::Exception::Exception(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, int) @ 0x15c2d2a3 in /usr/bin/clickhouse
        # /src/Processors/Formats/Impl/ParquetBlockInputFormat.cpp:70: DB::ParquetBlockInputFormat::prepareReader() @ 0x1df2b0c2 in /usr/bin/clickhouse
        # /src/Processors/Formats/Impl/ParquetBlockInputFormat.cpp:36: DB::ParquetBlockInputFormat::ParquetBlockInputFormat(DB::ReadBuffer&, DB::Block) @ 0x1df2af8b in /usr/bin/clickhouse
        # /contrib/libcxx/include/memory:2214: std::__1::__compressed_pair_elem<DB::ParquetBlockInputFormat, 1, false>::__compressed_pair_elem<DB::ReadBuffer&, DB::Block const&, 0ul, 1ul>(std::__1::piecewise_construct_t, std::__1::tuple<DB::ReadBuffer&, DB::Block const&>, std::__1::__tuple_indices<0ul, 1ul>) @ 0x1df2dc88 in /usr/bin/clickhouse
        # /contrib/libcxx/include/memory:2299: std::__1::__compressed_pair<std::__1::allocator<DB::ParquetBlockInputFormat>, DB::ParquetBlockInputFormat>::__compressed_pair<std::__1::allocator<DB::ParquetBlockInputFormat>&, DB::ReadBuffer&, DB::Block const&>(std::__1::piecewise_construct_t, std::__1::tuple<std::__1::allocator<DB::ParquetBlockInputFormat>&>, std::__1::tuple<DB::ReadBuffer&, DB::Block const&>) @ 0x1df2d9c8 in /usr/bin/clickhouse
        # /contrib/libcxx/include/memory:3569: std::__1::__shared_ptr_emplace<DB::ParquetBlockInputFormat, std::__1::allocator<DB::ParquetBlockInputFormat> >::__shared_ptr_emplace<DB::ReadBuffer&, DB::Block const&>(std::__1::allocator<DB::ParquetBlockInputFormat>, DB::ReadBuffer&, DB::Block const&) @ 0x1df2d687 in /usr/bin/clickhouse
        # /contrib/libcxx/include/memory:4400: std::__1::enable_if<!(is_array<DB::ParquetBlockInputFormat>::value), std::__1::shared_ptr<DB::ParquetBlockInputFormat> >::type std::__1::make_shared<DB::ParquetBlockInputFormat, DB::ReadBuffer&, DB::Block const&>(DB::ReadBuffer&, DB::Block const&) @ 0x1df2d455 in /usr/bin/clickhouse
        # /src/Processors/Formats/Impl/ParquetBlockInputFormat.cpp:95: DB::registerInputFormatProcessorParquet(DB::FormatFactory&)::$_0::operator()(DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) const @ 0x1df2cec7 in /usr/bin/clickhouse
        # /contrib/libcxx/include/type_traits:3519: decltype(std::__1::forward<DB::registerInputFormatProcessorParquet(DB::FormatFactory&)::$_0&>(fp)(std::__1::forward<DB::ReadBuffer&>(fp0), std::__1::forward<DB::Block const&>(fp0), std::__1::forward<DB::RowInputFormatParams const&>(fp0), std::__1::forward<DB::FormatSettings const&>(fp0))) std::__1::__invoke<DB::registerInputFormatProcessorParquet(DB::FormatFactory&)::$_0&, DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&>(DB::registerInputFormatProcessorParquet(DB::FormatFactory&)::$_0&, DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) @ 0x1df2ce6a in /usr/bin/clickhouse
        # /contrib/libcxx/include/__functional_base:317: std::__1::shared_ptr<DB::IInputFormat> std::__1::__invoke_void_return_wrapper<std::__1::shared_ptr<DB::IInputFormat> >::__call<DB::registerInputFormatProcessorParquet(DB::FormatFactory&)::$_0&, DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&>(DB::registerInputFormatProcessorParquet(DB::FormatFactory&)::$_0&, DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) @ 0x1df2cd7d in /usr/bin/clickhouse
        # /contrib/libcxx/include/functional:1540: std::__1::__function::__alloc_func<DB::registerInputFormatProcessorParquet(DB::FormatFactory&)::$_0, std::__1::allocator<DB::registerInputFormatProcessorParquet(DB::FormatFactory&)::$_0>, std::__1::shared_ptr<DB::IInputFormat> (DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&)>::operator()(DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) @ 0x1df2ccda in /usr/bin/clickhouse
        # /contrib/libcxx/include/functional:1714: std::__1::__function::__func<DB::registerInputFormatProcessorParquet(DB::FormatFactory&)::$_0, std::__1::allocator<DB::registerInputFormatProcessorParquet(DB::FormatFactory&)::$_0>, std::__1::shared_ptr<DB::IInputFormat> (DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&)>::operator()(DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) @ 0x1df2bdec in /usr/bin/clickhouse
        # /contrib/libcxx/include/functional:1867: std::__1::__function::__value_func<std::__1::shared_ptr<DB::IInputFormat> (DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&)>::operator()(DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) const @ 0x1dd14dbd in /usr/bin/clickhouse
        # /contrib/libcxx/include/functional:2473: std::__1::function<std::__1::shared_ptr<DB::IInputFormat> (DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&)>::operator()(DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) const @ 0x1dd07035 in /usr/bin/clickhouse
        # /src/Formats/FormatFactory.cpp:258: DB::FormatFactory::getInputFormat(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, DB::ReadBuffer&, DB::Block const&, DB::Context const&, unsigned long, std::__1::function<void ()>) const @ 0x1dd04007 in /usr/bin/clickhouse
        # /src/Storages/Kafka/KafkaBlockInputStream.cpp:76: DB::KafkaBlockInputStream::readImpl() @ 0x1d8f6559 in /usr/bin/clickhouse
        # /src/DataStreams/IBlockInputStream.cpp:60: DB::IBlockInputStream::read() @ 0x1c9c92fd in /usr/bin/clickhouse
        # /src/DataStreams/copyData.cpp:26: void DB::copyDataImpl<DB::copyData(DB::IBlockInputStream&, DB::IBlockOutputStream&, std::__1::atomic<bool>*)::$_0&, void (&)(DB::Block const&)>(DB::IBlockInputStream&, DB::IBlockOutputStream&, DB::copyData(DB::IBlockInputStream&, DB::IBlockOutputStream&, std::__1::atomic<bool>*)::$_0&, void (&)(DB::Block const&)) @ 0x1c9ea01c in /usr/bin/clickhouse
        # /src/DataStreams/copyData.cpp:63: DB::copyData(DB::IBlockInputStream&, DB::IBlockOutputStream&, std::__1::atomic<bool>*) @ 0x1c9e9fc7 in /usr/bin/clickhouse
        # /src/Storages/Kafka/StorageKafka.cpp:565: DB::StorageKafka::streamToViews() @ 0x1d8cc3fa in /usr/bin/clickhouse
        #     # 'data_sample' : [
        #     #     '\x50\x41\x52\x31\x15\x04\x15\x10\x15\x14\x4c\x15\x02\x15\x04\x12\x00\x00\x08\x1c\x00\x00\x00\x00\x00\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xbc\x01\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x02\x16\xac\x01\x16\xb4\x01\x26\x38\x26\x08\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xc8\x03\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xfc\x02\x26\xd4\x02\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x0c\x15\x10\x4c\x15\x02\x15\x04\x12\x00\x00\x06\x14\x02\x00\x00\x00\x41\x4d\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x03\x08\x01\x02\x00\x26\xa2\x05\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x02\x16\x68\x16\x70\x26\xde\x04\x26\xb2\x04\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x3f\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x03\x08\x01\x02\x00\x26\x8a\x07\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x02\x16\x84\x01\x16\x8c\x01\x26\xa6\x06\x26\xfe\x05\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x01\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xfe\x08\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xb2\x08\x26\x8a\x08\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x15\x02\x19\x6c\x35\x00\x18\x06\x73\x63\x68\x65\x6d\x61\x15\x0a\x00\x15\x04\x25\x00\x18\x02\x69\x64\x00\x15\x02\x25\x00\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x25\x18\x4c\xac\x13\x10\x12\x00\x00\x00\x15\x0c\x25\x00\x18\x04\x76\x61\x6c\x31\x25\x00\x4c\x1c\x00\x00\x00\x15\x08\x25\x00\x18\x04\x76\x61\x6c\x32\x00\x15\x02\x25\x00\x18\x04\x76\x61\x6c\x33\x25\x16\x4c\xac\x13\x08\x12\x00\x00\x00\x16\x02\x19\x1c\x19\x5c\x26\xbc\x01\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x02\x16\xac\x01\x16\xb4\x01\x26\x38\x26\x08\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x26\xc8\x03\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xfc\x02\x26\xd4\x02\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x26\xa2\x05\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x02\x16\x68\x16\x70\x26\xde\x04\x26\xb2\x04\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x26\x8a\x07\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x02\x16\x84\x01\x16\x8c\x01\x26\xa6\x06\x26\xfe\x05\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x26\xfe\x08\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xb2\x08\x26\x8a\x08\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x16\x98\x05\x16\x02\x00\x28\x22\x70\x61\x72\x71\x75\x65\x74\x2d\x63\x70\x70\x20\x76\x65\x72\x73\x69\x6f\x6e\x20\x31\x2e\x35\x2e\x31\x2d\x53\x4e\x41\x50\x53\x48\x4f\x54\x19\x5c\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x00\xc4\x01\x00\x00\x50\x41\x52\x31',
        #     #     '\x50\x41\x52\x31\x15\x04\x15\xf0\x01\x15\x90\x01\x4c\x15\x1e\x15\x04\x12\x00\x00\x78\x04\x01\x00\x09\x01\x00\x02\x09\x07\x04\x00\x03\x0d\x08\x00\x04\x0d\x08\x00\x05\x0d\x08\x00\x06\x0d\x08\x00\x07\x0d\x08\x00\x08\x0d\x08\x00\x09\x0d\x08\x00\x0a\x0d\x08\x00\x0b\x0d\x08\x00\x0c\x0d\x08\x00\x0d\x0d\x08\x3c\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x15\x00\x15\x14\x15\x18\x2c\x15\x1e\x15\x04\x15\x06\x15\x06\x1c\x18\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x24\x04\x05\x10\x32\x54\x76\x98\xba\xdc\x0e\x26\xca\x02\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x1e\x16\x9e\x03\x16\xc2\x02\x26\xb8\x01\x26\x08\x1c\x18\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x1e\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x03\x08\x01\x1e\x00\x26\xd8\x04\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x1e\x16\x6c\x16\x74\x26\x8c\x04\x26\xe4\x03\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x0c\x15\x10\x4c\x15\x02\x15\x04\x12\x00\x00\x06\x14\x02\x00\x00\x00\x41\x4d\x15\x00\x15\x06\x15\x0a\x2c\x15\x1e\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x03\x08\x01\x1e\x00\x26\xb2\x06\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x1e\x16\x68\x16\x70\x26\xee\x05\x26\xc2\x05\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x3f\x15\x00\x15\x06\x15\x0a\x2c\x15\x1e\x15\x04\x15\x06\x15\x06\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x03\x08\x01\x1e\x00\x26\x9a\x08\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x1e\x16\x84\x01\x16\x8c\x01\x26\xb6\x07\x26\x8e\x07\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x01\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x1e\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x03\x08\x01\x1e\x00\x26\x8e\x0a\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x1e\x16\x6c\x16\x74\x26\xc2\x09\x26\x9a\x09\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x15\x02\x19\x6c\x35\x00\x18\x06\x73\x63\x68\x65\x6d\x61\x15\x0a\x00\x15\x04\x25\x00\x18\x02\x69\x64\x00\x15\x02\x25\x00\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x25\x18\x4c\xac\x13\x10\x12\x00\x00\x00\x15\x0c\x25\x00\x18\x04\x76\x61\x6c\x31\x25\x00\x4c\x1c\x00\x00\x00\x15\x08\x25\x00\x18\x04\x76\x61\x6c\x32\x00\x15\x02\x25\x00\x18\x04\x76\x61\x6c\x33\x25\x16\x4c\xac\x13\x08\x12\x00\x00\x00\x16\x1e\x19\x1c\x19\x5c\x26\xca\x02\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x1e\x16\x9e\x03\x16\xc2\x02\x26\xb8\x01\x26\x08\x1c\x18\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x0f\x00\x00\x00\x00\x00\x00\x00\x18\x08\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x26\xd8\x04\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x1e\x16\x6c\x16\x74\x26\x8c\x04\x26\xe4\x03\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x26\xb2\x06\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x1e\x16\x68\x16\x70\x26\xee\x05\x26\xc2\x05\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x26\x9a\x08\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x1e\x16\x84\x01\x16\x8c\x01\x26\xb6\x07\x26\x8e\x07\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x26\x8e\x0a\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x1e\x16\x6c\x16\x74\x26\xc2\x09\x26\x9a\x09\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x16\xa6\x06\x16\x1e\x00\x28\x22\x70\x61\x72\x71\x75\x65\x74\x2d\x63\x70\x70\x20\x76\x65\x72\x73\x69\x6f\x6e\x20\x31\x2e\x35\x2e\x31\x2d\x53\x4e\x41\x50\x53\x48\x4f\x54\x19\x5c\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x00\xc5\x01\x00\x00\x50\x41\x52\x31',
        #     #     '\x50\x41\x52\x31\x15\x04\x15\x10\x15\x14\x4c\x15\x02\x15\x04\x12\x00\x00\x08\x1c\x00\x00\x00\x00\x00\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xbc\x01\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x02\x16\xac\x01\x16\xb4\x01\x26\x38\x26\x08\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xc8\x03\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xfc\x02\x26\xd4\x02\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x15\x04\x15\x0c\x15\x10\x4c\x15\x02\x15\x04\x12\x00\x00\x06\x14\x02\x00\x00\x00\x41\x4d\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x03\x08\x01\x02\x00\x26\xa2\x05\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x02\x16\x68\x16\x70\x26\xde\x04\x26\xb2\x04\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x00\x00\x00\x3f\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x03\x08\x01\x02\x00\x26\x8a\x07\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x02\x16\x84\x01\x16\x8c\x01\x26\xa6\x06\x26\xfe\x05\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x15\x04\x15\x08\x15\x0c\x4c\x15\x02\x15\x04\x12\x00\x00\x04\x0c\x01\x00\x00\x00\x15\x00\x15\x06\x15\x0a\x2c\x15\x02\x15\x04\x15\x06\x15\x06\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x03\x08\x01\x02\x00\x26\xfe\x08\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xb2\x08\x26\x8a\x08\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x15\x02\x19\x6c\x35\x00\x18\x06\x73\x63\x68\x65\x6d\x61\x15\x0a\x00\x15\x04\x25\x00\x18\x02\x69\x64\x00\x15\x02\x25\x00\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x25\x18\x4c\xac\x13\x10\x12\x00\x00\x00\x15\x0c\x25\x00\x18\x04\x76\x61\x6c\x31\x25\x00\x4c\x1c\x00\x00\x00\x15\x08\x25\x00\x18\x04\x76\x61\x6c\x32\x00\x15\x02\x25\x00\x18\x04\x76\x61\x6c\x33\x25\x16\x4c\xac\x13\x08\x12\x00\x00\x00\x16\x02\x19\x1c\x19\x5c\x26\xbc\x01\x1c\x15\x04\x19\x35\x04\x00\x06\x19\x18\x02\x69\x64\x15\x02\x16\x02\x16\xac\x01\x16\xb4\x01\x26\x38\x26\x08\x1c\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x16\x00\x28\x08\x00\x00\x00\x00\x00\x00\x00\x00\x18\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x26\xc8\x03\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xfc\x02\x26\xd4\x02\x1c\x36\x00\x28\x04\x00\x00\x00\x00\x18\x04\x00\x00\x00\x00\x00\x00\x00\x26\xa2\x05\x1c\x15\x0c\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x31\x15\x02\x16\x02\x16\x68\x16\x70\x26\xde\x04\x26\xb2\x04\x1c\x36\x00\x28\x02\x41\x4d\x18\x02\x41\x4d\x00\x00\x00\x26\x8a\x07\x1c\x15\x08\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x32\x15\x02\x16\x02\x16\x84\x01\x16\x8c\x01\x26\xa6\x06\x26\xfe\x05\x1c\x18\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x16\x00\x28\x04\x00\x00\x00\x3f\x18\x04\x00\x00\x00\x3f\x00\x00\x00\x26\xfe\x08\x1c\x15\x02\x19\x35\x04\x00\x06\x19\x18\x04\x76\x61\x6c\x33\x15\x02\x16\x02\x16\x6c\x16\x74\x26\xb2\x08\x26\x8a\x08\x1c\x36\x00\x28\x04\x01\x00\x00\x00\x18\x04\x01\x00\x00\x00\x00\x00\x00\x16\x98\x05\x16\x02\x00\x28\x22\x70\x61\x72\x71\x75\x65\x74\x2d\x63\x70\x70\x20\x76\x65\x72\x73\x69\x6f\x6e\x20\x31\x2e\x35\x2e\x31\x2d\x53\x4e\x41\x50\x53\x48\x4f\x54\x19\x5c\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x1c\x00\x00\x00\xc4\x01\x00\x00\x50\x41\x52\x31',
        #     #     ''
        #     # ],
        # },
        # 'Avro' : {
        #     # TODO: Not working at all: avro::Exception, e.what() = EOF reached
        #     #./contrib/libcxx/src/support/runtime/stdexcept_default.ipp:33: std::runtime_error::runtime_error(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&) @ 0x22ce2080 in /usr/bin/clickhouse
        #     #./contrib/avro/lang/c++/api/Exception.hh:36: avro::Exception::Exception(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&) @ 0x1de48a6e in /usr/bin/clickhouse
        #     #./contrib/avro/lang/c++/api/Stream.hh:336: avro::StreamReader::more() @ 0x22717f56 in /usr/bin/clickhouse
        #     #./contrib/avro/lang/c++/api/Stream.hh:0: avro::StreamReader::readBytes(unsigned char*, unsigned long) @ 0x22717d22 in /usr/bin/clickhouse
        #     #./contrib/avro/lang/c++/impl/BinaryDecoder.cc:170: avro::BinaryDecoder::decodeFixed(unsigned long, std::__1::vector<unsigned char, std::__1::allocator<unsigned char> >&) @ 0x227177cb in /usr/bin/clickhouse
        #     #./contrib/avro/lang/c++/api/Specific.hh:216: avro::codec_traits<std::__1::array<unsigned char, 4ul> >::decode(avro::Decoder&, std::__1::array<unsigned char, 4ul>&) @ 0x22743624 in /usr/bin/clickhouse
        #     #./contrib/avro/lang/c++/api/Specific.hh:342: void avro::decode<std::__1::array<unsigned char, 4ul> >(avro::Decoder&, std::__1::array<unsigned char, 4ul>&) @ 0x2272970d in /usr/bin/clickhouse
        #     #./contrib/avro/lang/c++/impl/DataFile.cc:487: avro::DataFileReaderBase::readHeader() @ 0x2272608d in /usr/bin/clickhouse
        #     #./contrib/avro/lang/c++/impl/DataFile.cc:280: avro::DataFileReaderBase::DataFileReaderBase(std::__1::unique_ptr<avro::InputStream, std::__1::default_delete<avro::InputStream> >) @ 0x22726923 in /usr/bin/clickhouse
        #     #./src/Processors/Formats/Impl/AvroRowInputFormat.cpp:571: DB::AvroRowInputFormat::AvroRowInputFormat(DB::Block const&, DB::ReadBuffer&, DB::RowInputFormatParams) @ 0x1de19c9b in /usr/bin/clickhouse
        #     'data_sample' : [
        #         #'\x4f\x62\x6a\x01\x04\x14\x61\x76\x72\x6f\x2e\x63\x6f\x64\x65\x63\x0c\x73\x6e\x61\x70\x70\x79\x16\x61\x76\x72\x6f\x2e\x73\x63\x68\x65\x6d\x61\x80\x03\x7b\x22\x74\x79\x70\x65\x22\x3a\x22\x72\x65\x63\x6f\x72\x64\x22\x2c\x22\x6e\x61\x6d\x65\x22\x3a\x22\x72\x6f\x77\x22\x2c\x22\x66\x69\x65\x6c\x64\x73\x22\x3a\x5b\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x69\x64\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x6c\x6f\x6e\x67\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x62\x6c\x6f\x63\x6b\x4e\x6f\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x69\x6e\x74\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x76\x61\x6c\x31\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x62\x79\x74\x65\x73\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x76\x61\x6c\x32\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x66\x6c\x6f\x61\x74\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x76\x61\x6c\x33\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x69\x6e\x74\x22\x7d\x5d\x7d\x00\x73\x6e\x66\xa3\x62\x9f\x88\xed\x28\x08\x67\xf0\x75\xaf\x23\x83\x02\x20\x0a\x24\x00\x00\x04\x41\x4d\x00\x00\x00\x3f\x02\x80\xaa\x4a\xe3\x73\x6e\x66\xa3\x62\x9f\x88\xed\x28\x08\x67\xf0\x75\xaf\x23\x83',
        #         #'\x4f\x62\x6a\x01\x04\x14\x61\x76\x72\x6f\x2e\x63\x6f\x64\x65\x63\x0c\x73\x6e\x61\x70\x70\x79\x16\x61\x76\x72\x6f\x2e\x73\x63\x68\x65\x6d\x61\x80\x03\x7b\x22\x74\x79\x70\x65\x22\x3a\x22\x72\x65\x63\x6f\x72\x64\x22\x2c\x22\x6e\x61\x6d\x65\x22\x3a\x22\x72\x6f\x77\x22\x2c\x22\x66\x69\x65\x6c\x64\x73\x22\x3a\x5b\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x69\x64\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x6c\x6f\x6e\x67\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x62\x6c\x6f\x63\x6b\x4e\x6f\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x69\x6e\x74\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x76\x61\x6c\x31\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x62\x79\x74\x65\x73\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x76\x61\x6c\x32\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x66\x6c\x6f\x61\x74\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x76\x61\x6c\x33\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x69\x6e\x74\x22\x7d\x5d\x7d\x00\x73\x6e\x66\xa3\x62\x9f\x88\xed\x28\x08\x67\xf0\x75\xaf\x23\x83\x1e\x9e\x01\x96\x01\x28\x02\x00\x04\x41\x4d\x00\x00\x00\x3f\x02\x04\x15\x0a\x00\x06\x15\x0a\x00\x08\x15\x0a\x00\x0a\x15\x0a\x00\x0c\x15\x0a\x00\x0e\x15\x0a\x00\x10\x15\x0a\x00\x12\x15\x0a\x00\x14\x15\x0a\x00\x16\x15\x0a\x00\x18\x15\x0a\x00\x1a\x15\x0a\x00\x1c\x15\x0a\x24\x1e\x00\x04\x41\x4d\x00\x00\x00\x3f\x02\x49\x73\x4d\xca\x73\x6e\x66\xa3\x62\x9f\x88\xed\x28\x08\x67\xf0\x75\xaf\x23\x83',
        #         #'\x4f\x62\x6a\x01\x04\x14\x61\x76\x72\x6f\x2e\x63\x6f\x64\x65\x63\x0c\x73\x6e\x61\x70\x70\x79\x16\x61\x76\x72\x6f\x2e\x73\x63\x68\x65\x6d\x61\x80\x03\x7b\x22\x74\x79\x70\x65\x22\x3a\x22\x72\x65\x63\x6f\x72\x64\x22\x2c\x22\x6e\x61\x6d\x65\x22\x3a\x22\x72\x6f\x77\x22\x2c\x22\x66\x69\x65\x6c\x64\x73\x22\x3a\x5b\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x69\x64\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x6c\x6f\x6e\x67\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x62\x6c\x6f\x63\x6b\x4e\x6f\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x69\x6e\x74\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x76\x61\x6c\x31\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x62\x79\x74\x65\x73\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x76\x61\x6c\x32\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x66\x6c\x6f\x61\x74\x22\x7d\x2c\x7b\x22\x6e\x61\x6d\x65\x22\x3a\x22\x76\x61\x6c\x33\x22\x2c\x22\x74\x79\x70\x65\x22\x3a\x22\x69\x6e\x74\x22\x7d\x5d\x7d\x00\x73\x6e\x66\xa3\x62\x9f\x88\xed\x28\x08\x67\xf0\x75\xaf\x23\x83\x02\x20\x0a\x24\x00\x00\x04\x41\x4d\x00\x00\x00\x3f\x02\x80\xaa\x4a\xe3\x73\x6e\x66\xa3\x62\x9f\x88\xed\x28\x08\x67\xf0\x75\xaf\x23\x83',
        #         # ''
        #     ],
        # },
        # TODO: test for AvroConfluence
        # 'Arrow' : {
        #     # Not working at all: DB::Exception: Error while opening a table: Invalid: File is too small: 0, Stack trace (when copying this message, always include the lines below):
        #     # /src/Common/Exception.cpp:37: DB::Exception::Exception(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, int) @ 0x15c2d2a3 in /usr/bin/clickhouse
        #     # /src/Processors/Formats/Impl/ArrowBlockInputFormat.cpp:88: DB::ArrowBlockInputFormat::prepareReader() @ 0x1ddff1c3 in /usr/bin/clickhouse
        #     # /src/Processors/Formats/Impl/ArrowBlockInputFormat.cpp:26: DB::ArrowBlockInputFormat::ArrowBlockInputFormat(DB::ReadBuffer&, DB::Block const&, bool) @ 0x1ddfef63 in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/memory:2214: std::__1::__compressed_pair_elem<DB::ArrowBlockInputFormat, 1, false>::__compressed_pair_elem<DB::ReadBuffer&, DB::Block const&, bool&&, 0ul, 1ul, 2ul>(std::__1::piecewise_construct_t, std::__1::tuple<DB::ReadBuffer&, DB::Block const&, bool&&>, std::__1::__tuple_indices<0ul, 1ul, 2ul>) @ 0x1de0470f in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/memory:2299: std::__1::__compressed_pair<std::__1::allocator<DB::ArrowBlockInputFormat>, DB::ArrowBlockInputFormat>::__compressed_pair<std::__1::allocator<DB::ArrowBlockInputFormat>&, DB::ReadBuffer&, DB::Block const&, bool&&>(std::__1::piecewise_construct_t, std::__1::tuple<std::__1::allocator<DB::ArrowBlockInputFormat>&>, std::__1::tuple<DB::ReadBuffer&, DB::Block const&, bool&&>) @ 0x1de04375 in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/memory:3569: std::__1::__shared_ptr_emplace<DB::ArrowBlockInputFormat, std::__1::allocator<DB::ArrowBlockInputFormat> >::__shared_ptr_emplace<DB::ReadBuffer&, DB::Block const&, bool>(std::__1::allocator<DB::ArrowBlockInputFormat>, DB::ReadBuffer&, DB::Block const&, bool&&) @ 0x1de03f97 in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/memory:4400: std::__1::enable_if<!(is_array<DB::ArrowBlockInputFormat>::value), std::__1::shared_ptr<DB::ArrowBlockInputFormat> >::type std::__1::make_shared<DB::ArrowBlockInputFormat, DB::ReadBuffer&, DB::Block const&, bool>(DB::ReadBuffer&, DB::Block const&, bool&&) @ 0x1de03d4c in /usr/bin/clickhouse
        #     # /src/Processors/Formats/Impl/ArrowBlockInputFormat.cpp:107: DB::registerInputFormatProcessorArrow(DB::FormatFactory&)::$_0::operator()(DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) const @ 0x1de010df in /usr/bin/clickhouse
        #     'data_sample' : [
        #         '\x41\x52\x52\x4f\x57\x31\x00\x00\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x18\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00\x10\x00\x00\x00\x0c\x00\x14\x00\x06\x00\x08\x00\x0c\x00\x10\x00\x0c\x00\x00\x00\x00\x00\x03\x00\x3c\x00\x00\x00\x28\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x58\x01\x00\x00\x00\x00\x00\x00\x60\x01\x00\x00\x00\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\x78\x01\x00\x00\x41\x52\x52\x4f\x57\x31',
        #         '\x41\x52\x52\x4f\x57\x31\x00\x00\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x48\x01\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x98\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x98\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\xd8\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\xf8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf8\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x38\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x38\x01\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x06\x00\x00\x00\x08\x00\x00\x00\x0a\x00\x00\x00\x0c\x00\x00\x00\x0e\x00\x00\x00\x10\x00\x00\x00\x12\x00\x00\x00\x14\x00\x00\x00\x16\x00\x00\x00\x18\x00\x00\x00\x1a\x00\x00\x00\x1c\x00\x00\x00\x1e\x00\x00\x00\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x00\xff\xff\xff\xff\x00\x00\x00\x00\x10\x00\x00\x00\x0c\x00\x14\x00\x06\x00\x08\x00\x0c\x00\x10\x00\x0c\x00\x00\x00\x00\x00\x03\x00\x3c\x00\x00\x00\x28\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x58\x01\x00\x00\x00\x00\x00\x00\x60\x01\x00\x00\x00\x00\x00\x00\x48\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\x78\x01\x00\x00\x41\x52\x52\x4f\x57\x31',
        #         '\x41\x52\x52\x4f\x57\x31\x00\x00\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x18\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00\x10\x00\x00\x00\x0c\x00\x14\x00\x06\x00\x08\x00\x0c\x00\x10\x00\x0c\x00\x00\x00\x00\x00\x03\x00\x3c\x00\x00\x00\x28\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00\x58\x01\x00\x00\x00\x00\x00\x00\x60\x01\x00\x00\x00\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\x78\x01\x00\x00\x41\x52\x52\x4f\x57\x31',
        #     ],
        # },
        # 'ArrowStream' : {
        #     # Not working at all:
        #     # Error while opening a table: Invalid: Tried reading schema message, was null or length 0, Stack trace (when copying this message, always include the lines below):
        #     # /src/Processors/Formats/Impl/ArrowBlockInputFormat.cpp:88: DB::ArrowBlockInputFormat::prepareReader() @ 0x1ddff1c3 in /usr/bin/clickhouse
        #     # /src/Processors/Formats/Impl/ArrowBlockInputFormat.cpp:26: DB::ArrowBlockInputFormat::ArrowBlockInputFormat(DB::ReadBuffer&, DB::Block const&, bool) @ 0x1ddfef63 in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/memory:2214: std::__1::__compressed_pair_elem<DB::ArrowBlockInputFormat, 1, false>::__compressed_pair_elem<DB::ReadBuffer&, DB::Block const&, bool&&, 0ul, 1ul, 2ul>(std::__1::piecewise_construct_t, std::__1::tuple<DB::ReadBuffer&, DB::Block const&, bool&&>, std::__1::__tuple_indices<0ul, 1ul, 2ul>) @ 0x1de0470f in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/memory:2299: std::__1::__compressed_pair<std::__1::allocator<DB::ArrowBlockInputFormat>, DB::ArrowBlockInputFormat>::__compressed_pair<std::__1::allocator<DB::ArrowBlockInputFormat>&, DB::ReadBuffer&, DB::Block const&, bool&&>(std::__1::piecewise_construct_t, std::__1::tuple<std::__1::allocator<DB::ArrowBlockInputFormat>&>, std::__1::tuple<DB::ReadBuffer&, DB::Block const&, bool&&>) @ 0x1de04375 in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/memory:3569: std::__1::__shared_ptr_emplace<DB::ArrowBlockInputFormat, std::__1::allocator<DB::ArrowBlockInputFormat> >::__shared_ptr_emplace<DB::ReadBuffer&, DB::Block const&, bool>(std::__1::allocator<DB::ArrowBlockInputFormat>, DB::ReadBuffer&, DB::Block const&, bool&&) @ 0x1de03f97 in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/memory:4400: std::__1::enable_if<!(is_array<DB::ArrowBlockInputFormat>::value), std::__1::shared_ptr<DB::ArrowBlockInputFormat> >::type std::__1::make_shared<DB::ArrowBlockInputFormat, DB::ReadBuffer&, DB::Block const&, bool>(DB::ReadBuffer&, DB::Block const&, bool&&) @ 0x1de03d4c in /usr/bin/clickhouse
        #     # /src/Processors/Formats/Impl/ArrowBlockInputFormat.cpp:117: DB::registerInputFormatProcessorArrow(DB::FormatFactory&)::$_1::operator()(DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) const @ 0x1de0273f in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/type_traits:3519: decltype(std::__1::forward<DB::registerInputFormatProcessorArrow(DB::FormatFactory&)::$_1&>(fp)(std::__1::forward<DB::ReadBuffer&>(fp0), std::__1::forward<DB::Block const&>(fp0), std::__1::forward<DB::RowInputFormatParams const&>(fp0), std::__1::forward<DB::FormatSettings const&>(fp0))) std::__1::__invoke<DB::registerInputFormatProcessorArrow(DB::FormatFactory&)::$_1&, DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&>(DB::registerInputFormatProcessorArrow(DB::FormatFactory&)::$_1&, DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) @ 0x1de026da in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/__functional_base:317: std::__1::shared_ptr<DB::IInputFormat> std::__1::__invoke_void_return_wrapper<std::__1::shared_ptr<DB::IInputFormat> >::__call<DB::registerInputFormatProcessorArrow(DB::FormatFactory&)::$_1&, DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&>(DB::registerInputFormatProcessorArrow(DB::FormatFactory&)::$_1&, DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) @ 0x1de025ed in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/functional:1540: std::__1::__function::__alloc_func<DB::registerInputFormatProcessorArrow(DB::FormatFactory&)::$_1, std::__1::allocator<DB::registerInputFormatProcessorArrow(DB::FormatFactory&)::$_1>, std::__1::shared_ptr<DB::IInputFormat> (DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&)>::operator()(DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) @ 0x1de0254a in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/functional:1714: std::__1::__function::__func<DB::registerInputFormatProcessorArrow(DB::FormatFactory&)::$_1, std::__1::allocator<DB::registerInputFormatProcessorArrow(DB::FormatFactory&)::$_1>, std::__1::shared_ptr<DB::IInputFormat> (DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&)>::operator()(DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) @ 0x1de0165c in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/functional:1867: std::__1::__function::__value_func<std::__1::shared_ptr<DB::IInputFormat> (DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&)>::operator()(DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) const @ 0x1dd14dbd in /usr/bin/clickhouse
        #     # /contrib/libcxx/include/functional:2473: std::__1::function<std::__1::shared_ptr<DB::IInputFormat> (DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&)>::operator()(DB::ReadBuffer&, DB::Block const&, DB::RowInputFormatParams const&, DB::FormatSettings const&) const @ 0x1dd07035 in /usr/bin/clickhouse
        #     # /src/Formats/FormatFactory.cpp:258: DB::FormatFactory::getInputFormat(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> > const&, DB::ReadBuffer&, DB::Block const&, DB::Context const&, unsigned long, std::__1::function<void ()>) const @ 0x1dd04007 in /usr/bin/clickhouse
        #     # /src/Storages/Kafka/KafkaBlockInputStream.cpp:76: DB::KafkaBlockInputStream::readImpl() @ 0x1d8f6559 in /usr/bin/clickhouse
        #     # /src/DataStreams/IBlockInputStream.cpp:60: DB::IBlockInputStream::read() @ 0x1c9c92fd in /usr/bin/clickhouse
        #     # /src/DataStreams/copyData.cpp:26: void DB::copyDataImpl<DB::copyData(DB::IBlockInputStream&, DB::IBlockOutputStream&, std::__1::atomic<bool>*)::$_0&, void (&)(DB::Block const&)>(DB::IBlockInputStream&, DB::IBlockOutputStream&, DB::copyData(DB::IBlockInputStream&, DB::IBlockOutputStream&, std::__1::atomic<bool>*)::$_0&, void (&)(DB::Block const&)) @ 0x1c9ea01c in /usr/bin/clickhouse
        #     'data_sample' : [
        #         '\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x18\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00',
        #         '\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x48\x01\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x78\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x98\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x98\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\xd8\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\xf8\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf8\x00\x00\x00\x00\x00\x00\x00\x40\x00\x00\x00\x00\x00\x00\x00\x38\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x38\x01\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x06\x00\x00\x00\x08\x00\x00\x00\x0a\x00\x00\x00\x0c\x00\x00\x00\x0e\x00\x00\x00\x10\x00\x00\x00\x12\x00\x00\x00\x14\x00\x00\x00\x16\x00\x00\x00\x18\x00\x00\x00\x1a\x00\x00\x00\x1c\x00\x00\x00\x1e\x00\x00\x00\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x41\x4d\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x00\xff\xff\xff\xff\x00\x00\x00\x00',
        #         '\xff\xff\xff\xff\x48\x01\x00\x00\x10\x00\x00\x00\x00\x00\x0a\x00\x0c\x00\x06\x00\x05\x00\x08\x00\x0a\x00\x00\x00\x00\x01\x03\x00\x0c\x00\x00\x00\x08\x00\x08\x00\x00\x00\x04\x00\x08\x00\x00\x00\x04\x00\x00\x00\x05\x00\x00\x00\xe4\x00\x00\x00\x9c\x00\x00\x00\x6c\x00\x00\x00\x34\x00\x00\x00\x04\x00\x00\x00\x40\xff\xff\xff\x00\x00\x00\x02\x18\x00\x00\x00\x0c\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x72\xff\xff\xff\x08\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x33\x00\x00\x00\x00\x6c\xff\xff\xff\x00\x00\x00\x03\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x06\x00\x06\x00\x00\x00\x00\x00\x01\x00\x04\x00\x00\x00\x76\x61\x6c\x32\x00\x00\x00\x00\xa0\xff\xff\xff\x00\x00\x00\x05\x18\x00\x00\x00\x10\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x04\x00\x04\x00\x04\x00\x00\x00\x04\x00\x00\x00\x76\x61\x6c\x31\x00\x00\x00\x00\xcc\xff\xff\xff\x00\x00\x00\x02\x20\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\x08\x00\x04\x00\x06\x00\x00\x00\x10\x00\x00\x00\x07\x00\x00\x00\x62\x6c\x6f\x63\x6b\x4e\x6f\x00\x10\x00\x14\x00\x08\x00\x00\x00\x07\x00\x0c\x00\x00\x00\x10\x00\x10\x00\x00\x00\x00\x00\x00\x02\x24\x00\x00\x00\x14\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x08\x00\x0c\x00\x08\x00\x07\x00\x08\x00\x00\x00\x00\x00\x00\x01\x40\x00\x00\x00\x02\x00\x00\x00\x69\x64\x00\x00\xff\xff\xff\xff\x58\x01\x00\x00\x14\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x16\x00\x06\x00\x05\x00\x08\x00\x0c\x00\x0c\x00\x00\x00\x00\x03\x03\x00\x18\x00\x00\x00\x30\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x18\x00\x0c\x00\x04\x00\x08\x00\x0a\x00\x00\x00\xcc\x00\x00\x00\x10\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x18\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x20\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x28\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x41\x4d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x3f\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff\x00\x00\x00\x00',
        #     ],
        # },
    }

    for format_name in all_formats:
        print('Set up {}'.format(format_name))
        topic_name='format_tests_{}'.format(format_name)
        kafka_produce(topic_name, all_formats[format_name]['data_sample'])
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
            '''.format(topic_name=topic_name, format_name=format_name, extra_settings=all_formats[format_name].get('extra_settings') or ''))

    time.sleep(12)

    for format_name in all_formats:
        print('Checking {}'.format(format_name))
        topic_name='format_tests_{}'.format(format_name)

        result = instance.query('SELECT * FROM test.kafka_{format_name}_mv;'.format(format_name=format_name))
        expected = '''\
0	0	AM	0.5	1	{topic_name}	0	0
1	0	AM	0.5	1	{topic_name}	0	1
2	0	AM	0.5	1	{topic_name}	0	1
3	0	AM	0.5	1	{topic_name}	0	1
4	0	AM	0.5	1	{topic_name}	0	1
5	0	AM	0.5	1	{topic_name}	0	1
6	0	AM	0.5	1	{topic_name}	0	1
7	0	AM	0.5	1	{topic_name}	0	1
8	0	AM	0.5	1	{topic_name}	0	1
9	0	AM	0.5	1	{topic_name}	0	1
10	0	AM	0.5	1	{topic_name}	0	1
11	0	AM	0.5	1	{topic_name}	0	1
12	0	AM	0.5	1	{topic_name}	0	1
13	0	AM	0.5	1	{topic_name}	0	1
14	0	AM	0.5	1	{topic_name}	0	1
15	0	AM	0.5	1	{topic_name}	0	1
0	0	AM	0.5	1	{topic_name}	0	2
'''.format(topic_name=topic_name)
        assert TSV(result) == TSV(expected), 'Proper result for format: {}'.format(format_name)


# Since everything is async and shaky when receiving messages from Kafka,
# we may want to try and check results multiple times in a loop.
def  kafka_check_result(result, check=False, ref_file='test_kafka_json.reference'):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)

# https://stackoverflow.com/a/57692111/1555175
def describe_consumer_group(name):
    client = BrokerConnection('localhost', 9092, socket.AF_INET)
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
            member_topics_assignment.append({'topic':topic, 'partitions':partitions})
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
        print("kafka_id is {}".format(kafka_id))
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def kafka_setup_teardown():
    instance.query('DROP DATABASE IF EXISTS test; CREATE DATABASE test;')
    wait_kafka_is_available()
    # print("kafka is available - running test")
    yield  # run test

# Tests

@pytest.mark.timeout(180)
def test_kafka_settings_old_syntax(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka('kafka1:19092', 'old', 'old', 'JSONEachRow', '\\n');
        ''')

    # Don't insert malformed messages since old settings syntax
    # doesn't support skipping of broken messages.
    messages = []
    for i in range(50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce('old', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)

    members = describe_consumer_group('old')
    assert members[0]['client_id'] == u'ClickHouse-instance-test-kafka'
    # text_desc = kafka_cluster.exec_in_container(kafka_cluster.get_container_id('kafka1'),"kafka-consumer-groups --bootstrap-server localhost:9092 --describe --members --group old --verbose"))

@pytest.mark.timeout(180)
def test_kafka_settings_new_syntax(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'new',
                     kafka_group_name = 'new',
                     kafka_format = 'JSONEachRow',
                     kafka_row_delimiter = '\\n',
                     kafka_client_id = '{instance} test 1234',
                     kafka_skip_broken_messages = 1;
        ''')

    messages = []
    for i in range(25):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce('new', messages)

    # Insert couple of malformed messages.
    kafka_produce('new', ['}{very_broken_message,'])
    kafka_produce('new', ['}another{very_broken_message,'])

    messages = []
    for i in range(25, 50):
        messages.append(json.dumps({'key': i, 'value': i}))
    kafka_produce('new', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)

    members = describe_consumer_group('new')
    assert members[0]['client_id'] == u'instance test 1234'


@pytest.mark.timeout(180)
def test_kafka_issue11308(kafka_cluster):
    # Check that matview does respect Kafka SETTINGS
    kafka_produce('issue11308', ['{"t": 123, "e": {"x": "woof"} }', '{"t": 123, "e": {"x": "woof"} }', '{"t": 124, "e": {"x": "test"} }'])

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


@pytest.mark.timeout(180)
def test_kafka_issue4116(kafka_cluster):
    # Check that format_csv_delimiter parameter works now - as part of all available format settings.
    kafka_produce('issue4116', ['1|foo', '2|bar', '42|answer','100|multi\n101|row\n103|message'])

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


@pytest.mark.timeout(180)
def test_kafka_consumer_hang(kafka_cluster):

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
                     kafka_num_consumers = 8,
                     kafka_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64) ENGINE = Memory();
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS SELECT * FROM test.kafka;
        ''')

    time.sleep(10)
    instance.query('SELECT * FROM test.view')

    # This should trigger heartbeat fail,
    # which will trigger REBALANCE_IN_PROGRESS,
    # and which can lead to consumer hang.
    kafka_cluster.pause_container('kafka1')
    time.sleep(0.5)
    kafka_cluster.unpause_container('kafka1')

    # print("Attempt to drop")
    instance.query('DROP TABLE test.kafka')

    #kafka_cluster.open_bash_shell('instance')

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

@pytest.mark.timeout(180)
def test_kafka_consumer_hang2(kafka_cluster):

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

#echo 'SELECT * FROM test.kafka; SELECT * FROM test.kafka2; DROP TABLE test.kafka;' | clickhouse client -mn &
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


@pytest.mark.timeout(180)
def test_kafka_csv_with_delimiter(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'csv',
                     kafka_group_name = 'csv',
                     kafka_format = 'CSV',
                     kafka_row_delimiter = '\\n';
        ''')

    messages = []
    for i in range(50):
        messages.append('{i}, {i}'.format(i=i))
    kafka_produce('csv', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
def test_kafka_tsv_with_delimiter(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'tsv',
                     kafka_group_name = 'tsv',
                     kafka_format = 'TSV',
                     kafka_row_delimiter = '\\n';
        ''')

    messages = []
    for i in range(50):
        messages.append('{i}\t{i}'.format(i=i))
    kafka_produce('tsv', messages)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
def test_kafka_select_empty(kafka_cluster):
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


@pytest.mark.timeout(180)
def test_kafka_json_without_delimiter(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'json',
                     kafka_group_name = 'json',
                     kafka_format = 'JSONEachRow';
        ''')

    messages = ''
    for i in range(25):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce('json', [messages])

    messages = ''
    for i in range(25, 50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce('json', [messages])

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
def test_kafka_protobuf(kafka_cluster):
    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'pb',
                     kafka_group_name = 'pb',
                     kafka_format = 'Protobuf',
                     kafka_schema = 'kafka.proto:KeyValuePair';
        ''')

    kafka_produce_protobuf_messages('pb', 0, 20)
    kafka_produce_protobuf_messages('pb', 20, 1)
    kafka_produce_protobuf_messages('pb', 21, 29)

    result = ''
    while True:
        result += instance.query('SELECT * FROM test.kafka', ignore_error=True)
        if kafka_check_result(result):
            break

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
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
    kafka_produce('mv', messages)

    while True:
        result = instance.query('SELECT * FROM test.view')
        if kafka_check_result(result):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
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
    kafka_produce('mvsq', messages)

    while True:
        result = instance.query('SELECT * FROM test.view')
        if kafka_check_result(result):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    kafka_check_result(result, True)


@pytest.mark.timeout(180)
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
    kafka_produce('mmv', messages)

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


@pytest.mark.timeout(300)
def test_kafka_flush_on_big_message(kafka_cluster):
    # Create batchs of messages of size ~100Kb
    kafka_messages = 1000
    batch_messages = 1000
    messages = [json.dumps({'key': i, 'value': 'x' * 100}) * batch_messages for i in range(kafka_messages)]
    kafka_produce('flush', messages)

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

    client = KafkaAdminClient(bootstrap_servers="localhost:9092")
    received = False
    while not received:
        try:
            offsets = client.list_consumer_group_offsets('flush')
            for topic, offset in offsets.items():
                if topic.topic == 'flush' and offset.offset == kafka_messages:
                    received = True
                    break
        except kafka.errors.GroupCoordinatorNotAvailableError:
            continue

    while True:
        result = instance.query('SELECT count() FROM test.view')
        if int(result) == kafka_messages*batch_messages:
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert int(result) == kafka_messages*batch_messages, 'ClickHouse lost some messages: {}'.format(result)


@pytest.mark.timeout(180)
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
    kafka_produce('virt1', [messages], 0)

    messages = ''
    for i in range(25, 50):
        messages += json.dumps({'key': i, 'value': i}) + '\n'
    kafka_produce('virt1', [messages], 0)

    result = ''
    while True:
        result += instance.query('''SELECT _key, key, _topic, value, _offset, _partition, _timestamp = 0 ? '0000-00-00 00:00:00' : toString(_timestamp) AS _timestamp FROM test.kafka''', ignore_error=True)
        if kafka_check_result(result, False, 'test_kafka_virtual1.reference'):
            break

    kafka_check_result(result, True, 'test_kafka_virtual1.reference')


@pytest.mark.timeout(180)
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
    kafka_produce('virt2', messages, 0)

    while True:
        result = instance.query('SELECT kafka_key, key, topic, value, offset, partition, timestamp FROM test.view')
        if kafka_check_result(result, False, 'test_kafka_virtual2.reference'):
            break

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    kafka_check_result(result, True, 'test_kafka_virtual2.reference')


@pytest.mark.timeout(180)
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
        messages.extend(kafka_consume('insert1'))
        if len(messages) == 50:
            break

    result = '\n'.join(messages)
    kafka_check_result(result, True)


@pytest.mark.timeout(240)
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


@pytest.mark.timeout(300)
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
            kafka_produce('block', messages)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()

    while int(instance.query('SELECT count() FROM test.view')) == 0:
        time.sleep(1)

    cancel.set()

    instance.query('''
        DROP TABLE test.kafka;
    ''')

    while int(instance.query("SELECT count() FROM system.tables WHERE database='test' AND name='kafka'")) == 1:
        time.sleep(1)

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


@pytest.mark.timeout(180)
def test_kafka_virtual_columns2(kafka_cluster):

    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
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

    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    producer.send(topic='virt2_0', value=json.dumps({'value': 1}), partition=0, key='k1', timestamp_ms=1577836801001, headers=[('content-encoding', b'base64')])
    producer.send(topic='virt2_0', value=json.dumps({'value': 2}), partition=0, key='k2', timestamp_ms=1577836802002, headers=[('empty_value', ''),('', 'empty name'), ('',''), ('repetition', '1'), ('repetition', '2')])
    producer.flush()
    time.sleep(1)

    producer.send(topic='virt2_0', value=json.dumps({'value': 3}), partition=1, key='k3', timestamp_ms=1577836803003, headers=[('b', 'b'),('a', 'a')])
    producer.send(topic='virt2_0', value=json.dumps({'value': 4}), partition=1, key='k4', timestamp_ms=1577836804004, headers=[('a', 'a'),('b', 'b')])
    producer.flush()
    time.sleep(1)

    producer.send(topic='virt2_1', value=json.dumps({'value': 5}), partition=0, key='k5', timestamp_ms=1577836805005)
    producer.send(topic='virt2_1', value=json.dumps({'value': 6}), partition=0, key='k6', timestamp_ms=1577836806006)
    producer.flush()
    time.sleep(1)

    producer.send(topic='virt2_1', value=json.dumps({'value': 7}), partition=1, key='k7', timestamp_ms=1577836807007)
    producer.send(topic='virt2_1', value=json.dumps({'value': 8}), partition=1, key='k8', timestamp_ms=1577836808008)
    producer.flush()

    time.sleep(10)

    members = describe_consumer_group('virt2')
    #pprint.pprint(members)
    members[0]['client_id'] = u'ClickHouse-instance-test-kafka-0'
    members[1]['client_id'] = u'ClickHouse-instance-test-kafka-1'

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



@pytest.mark.timeout(240)
def test_kafka_produce_key_timestamp(kafka_cluster):
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

    instance.query("INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(1,1,'k1',1577836801))
    instance.query("INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(2,2,'k2',1577836802))
    instance.query("INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({})),({},{},'{}',toDateTime({}))".format(3,3,'k3',1577836803,4,4,'k4',1577836804))
    instance.query("INSERT INTO test.kafka_writer VALUES ({},{},'{}',toDateTime({}))".format(5,5,'k5',1577836805))

    while int(instance.query("SELECT count() FROM test.view")) < 5:
        time.sleep(1)

    result = instance.query("SELECT * FROM test.view ORDER BY value", ignore_error=True)

    # print(result)

    expected = '''\
1	1	k1	1577836801	k1	insert3	0	0	1577836801
2	2	k2	1577836802	k2	insert3	0	1	1577836802
3	3	k3	1577836803	k3	insert3	0	2	1577836803
4	4	k4	1577836804	k4	insert3	0	3	1577836804
5	5	k5	1577836805	k5	insert3	0	4	1577836805
'''

    assert TSV(result) == TSV(expected)



@pytest.mark.timeout(600)
def test_kafka_flush_by_time(kafka_cluster):
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
            kafka_produce('flush_by_time', messages)
            time.sleep(0.8)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()

    instance.query('''
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    time.sleep(18)

    result = instance.query('SELECT uniqExact(ts) = 2, count() > 15 FROM test.view')

    cancel.set()
    kafka_thread.join()

    # kafka_cluster.open_bash_shell('instance')

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert TSV(result) == TSV('1	1')


@pytest.mark.timeout(600)
def test_kafka_flush_by_block_size(kafka_cluster):
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
                     kafka_row_delimiter = '\\n';

        SELECT * FROM test.kafka;

        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;

        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka;
    ''')

    messages = []
    for _ in range(101):
        messages.append(json.dumps({'key': 0, 'value': 0}))
    kafka_produce('flush_by_block_size', messages)

    # Wait for Kafka engine to consume this data
    while 1 != int(instance.query("SELECT count() FROM system.parts WHERE database = 'test' AND table = 'view' AND name = 'all_1_1_0'")):
        time.sleep(1)

    # TODO: due to https://github.com/ClickHouse/ClickHouse/issues/11216
    # second flush happens earlier than expected, so we have 2 parts here instead of one
    # flush by block size works correctly, so the feature checked by the test is working correctly
    result = instance.query("SELECT count() FROM test.view WHERE _part='all_1_1_0'")
    # print(result)

    # kafka_cluster.open_bash_shell('instance')

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    # 100 = first poll should return 100 messages (and rows)
    # not waiting for stream_flush_interval_ms
    assert int(result) == 100, 'Messages from kafka should be flushed at least every stream_flush_interval_ms!'


@pytest.mark.timeout(600)
def test_kafka_lot_of_partitions_partial_commit_of_bulk(kafka_cluster):
    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")

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
                     kafka_max_block_size = 211;
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
        for dummy_row in range(random.randrange(3,10)):
            count = count + 1
            rows.append(json.dumps({'key': count, 'value': count}))
        messages.append("\n".join(rows))
    kafka_produce('topic_with_multiple_partitions2', messages)

    time.sleep(30)

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.view')
    print(result)
    assert TSV(result) == TSV('{0}\t{0}\t{0}'.format(count) )

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

@pytest.mark.timeout(1200)
def test_kafka_rebalance(kafka_cluster):

    NUMBER_OF_CONSURRENT_CONSUMERS=11

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

    #time.sleep(2)

    admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
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
            kafka_produce('topic_with_multiple_partitions', messages)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()

    for consumer_index in range(NUMBER_OF_CONSURRENT_CONSUMERS):
        table_name = 'kafka_consumer{}'.format(consumer_index)
        print("Setting up {}".format(table_name))

        instance.query('''
            DROP TABLE IF EXISTS test.{0};
            DROP TABLE IF EXISTS test.{0}_mv;
            CREATE TABLE test.{0} (key UInt64, value UInt64)
                ENGINE = Kafka
                SETTINGS kafka_broker_list = 'kafka1:19092',
                        kafka_topic_list = 'topic_with_multiple_partitions',
                        kafka_group_name = 'rebalance_test_group',
                        kafka_format = 'JSONEachRow',
                        kafka_max_block_size = 33;
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
        while int(instance.query("SELECT count() FROM test.destination WHERE _consumed_by='{}'".format(table_name))) == 0:
            print("Waiting for test.kafka_consumer{} to start consume".format(consumer_index))
            time.sleep(1)

    cancel.set()

    # I leave last one working by intent (to finish consuming after all rebalances)
    for consumer_index in range(NUMBER_OF_CONSURRENT_CONSUMERS-1):
        print("Dropping test.kafka_consumer{}".format(consumer_index))
        instance.query('DROP TABLE IF EXISTS test.kafka_consumer{}'.format(consumer_index))
        while int(instance.query("SELECT count() FROM system.tables WHERE database='test' AND name='kafka_consumer{}'".format(consumer_index))) == 1:
            time.sleep(1)

    # print(instance.query('SELECT count(), uniqExact(key), max(key) + 1 FROM test.destination'))
    # kafka_cluster.open_bash_shell('instance')

    while 1:
        messages_consumed = int(instance.query('SELECT uniqExact(key) FROM test.destination'))
        if messages_consumed >= msg_index[0]:
            break
        time.sleep(1)
        print("Waiting for finishing consuming (have {}, should be {})".format(messages_consumed,msg_index[0]))

    print(instance.query('SELECT count(), uniqExact(key), max(key) + 1 FROM test.destination'))

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
        print("kafka_consumer{}".format(consumer_index))
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

@pytest.mark.timeout(1200)
def test_kafka_no_holes_when_write_suffix_failed(kafka_cluster):
    messages = [json.dumps({'key': j+1, 'value': 'x' * 300}) for j in range(22)]
    kafka_produce('no_holes_when_write_suffix_failed', messages)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'no_holes_when_write_suffix_failed',
                     kafka_group_name = 'no_holes_when_write_suffix_failed',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 20;

        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = ReplicatedMergeTree('/clickhouse/kafkatest/tables/no_holes_when_write_suffix_failed', 'node1')
            ORDER BY key;

        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka
            WHERE NOT sleepEachRow(1);
    ''')
    # the tricky part here is that disconnect should happen after write prefix, but before write suffix
    # so i use sleepEachRow
    with PartitionManager() as pm:
        time.sleep(12)
        pm.drop_instance_zk_connections(instance)
        time.sleep(20)
        pm.heal_all

    # connection restored and it will take a while until next block will be flushed
    # it takes years on CI :\
    time.sleep(90)

    # as it's a bit tricky to hit the proper moment - let's check in logs if we did it correctly
    assert instance.contains_in_log("ZooKeeper session has been expired.: while write prefix to view")

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.view')
    print(result)

    # kafka_cluster.open_bash_shell('instance')

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    ''')

    assert TSV(result) == TSV('22\t22\t22')


@pytest.mark.timeout(120)
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

    #kafka_cluster.open_bash_shell('instance')
    assert TSV(instance.query('SELECT 1')) == TSV('1')


@pytest.mark.timeout(120)
def test_commits_of_unprocessed_messages_on_drop(kafka_cluster):
    messages = [json.dumps({'key': j+1, 'value': j+1}) for j in range(1)]
    kafka_produce('commits_of_unprocessed_messages_on_drop', messages)

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

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'commits_of_unprocessed_messages_on_drop',
                    kafka_group_name = 'commits_of_unprocessed_messages_on_drop_test_group',
                    kafka_format = 'JSONEachRow',
                    kafka_max_block_size = 1000;

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

    while int(instance.query("SELECT count() FROM test.destination")) == 0:
        print("Waiting for test.kafka_consumer to start consume")
        time.sleep(1)

    cancel = threading.Event()

    i = [2]
    def produce():
        while not cancel.is_set():
            messages = []
            for _ in range(113):
                messages.append(json.dumps({'key': i[0], 'value': i[0]}))
                i[0] += 1
            kafka_produce('commits_of_unprocessed_messages_on_drop', messages)
            time.sleep(1)

    kafka_thread = threading.Thread(target=produce)
    kafka_thread.start()
    time.sleep(12)

    instance.query('''
        DROP TABLE test.kafka;
    ''')

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'commits_of_unprocessed_messages_on_drop',
                    kafka_group_name = 'commits_of_unprocessed_messages_on_drop_test_group',
                    kafka_format = 'JSONEachRow',
                    kafka_max_block_size = 10000;
    ''')

    cancel.set()
    time.sleep(15)

    #kafka_cluster.open_bash_shell('instance')
    # SELECT key, _timestamp, _offset FROM test.destination where runningDifference(key) <> 1 ORDER BY key;

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.destination')
    print(result)

    instance.query('''
        DROP TABLE test.kafka_consumer;
        DROP TABLE test.destination;
    ''')

    kafka_thread.join()
    assert TSV(result) == TSV('{0}\t{0}\t{0}'.format(i[0]-1)), 'Missing data!'



@pytest.mark.timeout(120)
def test_bad_reschedule(kafka_cluster):
    messages = [json.dumps({'key': j+1, 'value': j+1}) for j in range(20000)]
    kafka_produce('test_bad_reschedule', messages)

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'test_bad_reschedule',
                    kafka_group_name = 'test_bad_reschedule',
                    kafka_format = 'JSONEachRow',
                    kafka_max_block_size = 1000;

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

    while int(instance.query("SELECT count() FROM test.destination")) < 20000:
        print("Waiting for consume")
        time.sleep(1)

    assert int(instance.query("SELECT max(consume_ts) - min(consume_ts) FROM test.destination")) < 8


@pytest.mark.timeout(1200)
def test_kafka_duplicates_when_commit_failed(kafka_cluster):
    messages = [json.dumps({'key': j+1, 'value': 'x' * 300}) for j in range(22)]
    kafka_produce('duplicates_when_commit_failed', messages)

    instance.query('''
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.kafka (key UInt64, value String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = 'duplicates_when_commit_failed',
                     kafka_group_name = 'duplicates_when_commit_failed',
                     kafka_format = 'JSONEachRow',
                     kafka_max_block_size = 20;

        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree()
            ORDER BY key;

        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.kafka
            WHERE NOT sleepEachRow(0.5);
    ''')

    #print time.strftime("%m/%d/%Y %H:%M:%S")
    time.sleep(12) # 5-6 sec to connect to kafka, do subscription, and fetch 20 rows, another 10 sec for MV, after that commit should happen

    #print time.strftime("%m/%d/%Y %H:%M:%S")
    kafka_cluster.pause_container('kafka1')
    # that timeout it VERY important, and picked after lot of experiments
    # when too low (<30sec) librdkafka will not report any timeout (alternative is to decrease the default session timeouts for librdkafka)
    # when too high (>50sec) broker will decide to remove us from the consumer group, and will start answering "Broker: Unknown member"
    time.sleep(40)

    #print time.strftime("%m/%d/%Y %H:%M:%S")
    kafka_cluster.unpause_container('kafka1')

    #kafka_cluster.open_bash_shell('instance')

    # connection restored and it will take a while until next block will be flushed
    # it takes years on CI :\
    time.sleep(30)

    # as it's a bit tricky to hit the proper moment - let's check in logs if we did it correctly
    assert instance.contains_in_log("Local: Waiting for coordinator")
    assert instance.contains_in_log("All commit attempts failed")

    result = instance.query('SELECT count(), uniqExact(key), max(key) FROM test.view')
    print(result)

    instance.query('''
        DROP TABLE test.consumer;
        DROP TABLE test.view;
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
@pytest.mark.timeout(120)
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

    messages = [json.dumps({'key': j+1, 'value': j+1}) for j in range(1)]
    kafka_produce('premature_flush_on_eof', messages)

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
    time.sleep(2)
    # produce more messages after delay
    kafka_produce('premature_flush_on_eof', messages)
    # data was not flushed yet (it will be flushed 7.5 sec after creating MV)
    assert int(instance.query("SELECT count() FROM test.destination")) == 0
    time.sleep(6)

    # it should be single part, i.e. single insert
    result = instance.query('SELECT _part, count() FROM test.destination group by _part')
    assert TSV(result) == TSV('all_1_1_0\t2')

    instance.query('''
        DROP TABLE test.kafka_consumer;
        DROP TABLE test.destination;
    ''')


@pytest.mark.timeout(180)
def test_kafka_unavailable(kafka_cluster):
    messages = [json.dumps({'key': j+1, 'value': j+1}) for j in range(20000)]
    kafka_produce('test_bad_reschedule', messages)

    kafka_cluster.pause_container('kafka1')

    instance.query('''
        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                    kafka_topic_list = 'test_bad_reschedule',
                    kafka_group_name = 'test_bad_reschedule',
                    kafka_format = 'JSONEachRow',
                    kafka_max_block_size = 1000;

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

    instance.query("SELECT * FROM test.kafka")
    instance.query("SELECT count() FROM test.destination")

    # enough to trigger issue
    time.sleep(30)
    kafka_cluster.unpause_container('kafka1')

    while int(instance.query("SELECT count() FROM test.destination")) < 20000:
        print("Waiting for consume")
        time.sleep(1)

if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()
