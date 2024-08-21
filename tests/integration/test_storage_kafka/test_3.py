import pytest
import json
import os.path as p
import random
import time
import logging
import io
import string
import ast


import kafka.errors
from google.protobuf.internal.encoder import _VarintBytes
from helpers.test_tools import TSV
from kafka import KafkaAdminClient, KafkaProducer, KafkaConsumer, BrokerConnection

# protoc --version
# libprotoc 3.0.0
# # to create kafka_pb2.py
# protoc --python_out=. kafka.proto

from . import message_with_repeated_pb2
from .kafka_fixtures import *


# Tests
def random_string(size=8):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=size))


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_engine_put_errors_to_stream(kafka_cluster, create_query_generator):
    topic_name = "kafka_engine_put_errors_to_stream" + get_topic_postfix(
        create_query_generator
    )
    create_query = create_query_generator(
        "kafka",
        "i Int64, s String",
        topic_list=topic_name,
        consumer_group=topic_name,
        settings={
            "kafka_max_block_size": 128,
            "kafka_handle_error_mode": "stream",
        },
    )
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka_data;
        DROP TABLE IF EXISTS test.kafka_errors;
        {create_query};
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
        """
    )

    messages = []
    for i in range(128):
        if i % 2 == 0:
            messages.append(json.dumps({"i": i, "s": random_string(8)}))
        else:
            # Unexpected json content for table test.kafka.
            messages.append(
                json.dumps({"i": "n_" + random_string(4), "s": random_string(8)})
            )

    kafka_produce(kafka_cluster, topic_name, messages)
    with existing_kafka_topic(get_admin_client(kafka_cluster), topic_name):
        instance.wait_for_log_line("Committed offset 128")

        assert TSV(instance.query("SELECT count() FROM test.kafka_data")) == TSV("64")
        assert TSV(instance.query("SELECT count() FROM test.kafka_errors")) == TSV("64")

        instance.query(
            """
            DROP TABLE test.kafka;
            DROP TABLE test.kafka_data;
            DROP TABLE test.kafka_errors;
        """
        )


def gen_normal_json():
    return '{"i":1000, "s":"ABC123abc"}'


def gen_malformed_json():
    return '{"i":"n1000", "s":"1000"}'


def gen_message_with_jsons(jsons=10, malformed=0):
    s = io.StringIO()

    # we don't care on which position error will be added
    # (we skip whole broken message), but we need to be
    # sure that at least one error will be added,
    # otherwise test will fail.
    error_pos = random.randint(0, jsons - 1)

    for i in range(jsons):
        if malformed and i == error_pos:
            s.write(gen_malformed_json())
        else:
            s.write(gen_normal_json())
        s.write(" ")
    return s.getvalue()


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_engine_put_errors_to_stream_with_random_malformed_json(
    kafka_cluster, create_query_generator
):
    topic_name = (
        "kafka_engine_put_errors_to_stream_with_random_malformed_json"
        + get_topic_postfix(create_query_generator)
    )
    create_query = create_query_generator(
        "kafka",
        "i Int64, s String",
        topic_list=topic_name,
        consumer_group=topic_name,
        settings={
            "kafka_max_block_size": 100,
            "kafka_poll_max_batch_size": 1,
            "kafka_handle_error_mode": "stream",
        },
    )

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka_data;
        DROP TABLE IF EXISTS test.kafka_errors;
        {create_query};
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
        """
    )

    messages = []
    for i in range(128):
        if i % 2 == 0:
            messages.append(gen_message_with_jsons(10, 1))
        else:
            messages.append(gen_message_with_jsons(10, 0))

    kafka_produce(kafka_cluster, topic_name, messages)
    with existing_kafka_topic(get_admin_client(kafka_cluster), topic_name):
        instance.wait_for_log_line("Committed offset 128")
        # 64 good messages, each containing 10 rows
        assert TSV(instance.query("SELECT count() FROM test.kafka_data")) == TSV("640")
        # 64 bad messages, each containing some broken row
        assert TSV(instance.query("SELECT count() FROM test.kafka_errors")) == TSV("64")

        instance.query(
            """
            DROP TABLE test.kafka;
            DROP TABLE test.kafka_data;
            DROP TABLE test.kafka_errors;
        """
        )


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_formats_with_broken_message(kafka_cluster, create_query_generator):
    # data was dumped from clickhouse itself in a following manner
    # clickhouse-client --format=Native --query='SELECT toInt64(number) as id, toUInt16( intDiv( id, 65536 ) ) as blockNo, reinterpretAsString(19777) as val1, toFloat32(0.5) as val2, toUInt8(1) as val3 from numbers(100) ORDER BY id' | xxd -ps | tr -d '\n' | sed 's/\(..\)/\\x\1/g'
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    all_formats = {
        ## Text formats ##
        # dumped with clickhouse-client ... | perl -pe 's/\n/\\n/; s/\t/\\t/g;'
        "JSONEachRow": {
            "data_sample": [
                '{"id":"0","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
                '{"id":"1","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"2","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"3","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"4","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"5","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"6","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"7","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"8","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"9","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"10","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"11","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"12","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"13","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"14","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n{"id":"15","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
                '{"id":"0","blockNo":0,"val1":"AM","val2":0.5,"val3":1}\n',
                # broken message
                '{"id":"0","blockNo":"BAD","val1":"AM","val2":0.5,"val3":1}',
            ],
            "expected": {
                "raw_message": '{"id":"0","blockNo":"BAD","val1":"AM","val2":0.5,"val3":1}',
                "error": 'Cannot parse input: expected \'"\' before: \'BAD","val1":"AM","val2":0.5,"val3":1}\': (while reading the value of key blockNo)',
            },
            "supports_empty_value": True,
            "printable": True,
        },
        # JSONAsString doesn't fit to that test, and tested separately
        "JSONCompactEachRow": {
            "data_sample": [
                '["0", 0, "AM", 0.5, 1]\n',
                '["1", 0, "AM", 0.5, 1]\n["2", 0, "AM", 0.5, 1]\n["3", 0, "AM", 0.5, 1]\n["4", 0, "AM", 0.5, 1]\n["5", 0, "AM", 0.5, 1]\n["6", 0, "AM", 0.5, 1]\n["7", 0, "AM", 0.5, 1]\n["8", 0, "AM", 0.5, 1]\n["9", 0, "AM", 0.5, 1]\n["10", 0, "AM", 0.5, 1]\n["11", 0, "AM", 0.5, 1]\n["12", 0, "AM", 0.5, 1]\n["13", 0, "AM", 0.5, 1]\n["14", 0, "AM", 0.5, 1]\n["15", 0, "AM", 0.5, 1]\n',
                '["0", 0, "AM", 0.5, 1]\n',
                # broken message
                '["0", "BAD", "AM", 0.5, 1]',
            ],
            "expected": {
                "raw_message": '["0", "BAD", "AM", 0.5, 1]',
                "error": "Cannot parse input: expected '\"' before: 'BAD\", \"AM\", 0.5, 1]': (while reading the value of key blockNo)",
            },
            "supports_empty_value": True,
            "printable": True,
        },
        "JSONCompactEachRowWithNamesAndTypes": {
            "data_sample": [
                '["id", "blockNo", "val1", "val2", "val3"]\n["Int64", "UInt16", "String", "Float32", "UInt8"]\n["0", 0, "AM", 0.5, 1]\n',
                '["id", "blockNo", "val1", "val2", "val3"]\n["Int64", "UInt16", "String", "Float32", "UInt8"]\n["1", 0, "AM", 0.5, 1]\n["2", 0, "AM", 0.5, 1]\n["3", 0, "AM", 0.5, 1]\n["4", 0, "AM", 0.5, 1]\n["5", 0, "AM", 0.5, 1]\n["6", 0, "AM", 0.5, 1]\n["7", 0, "AM", 0.5, 1]\n["8", 0, "AM", 0.5, 1]\n["9", 0, "AM", 0.5, 1]\n["10", 0, "AM", 0.5, 1]\n["11", 0, "AM", 0.5, 1]\n["12", 0, "AM", 0.5, 1]\n["13", 0, "AM", 0.5, 1]\n["14", 0, "AM", 0.5, 1]\n["15", 0, "AM", 0.5, 1]\n',
                '["id", "blockNo", "val1", "val2", "val3"]\n["Int64", "UInt16", "String", "Float32", "UInt8"]\n["0", 0, "AM", 0.5, 1]\n',
                # broken message
                '["0", "BAD", "AM", 0.5, 1]',
            ],
            "expected": {
                "raw_message": '["0", "BAD", "AM", 0.5, 1]',
                "error": "Cannot parse JSON string: expected opening quote",
            },
            "printable": True,
        },
        "TSKV": {
            "data_sample": [
                "id=0\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\n",
                "id=1\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=2\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=3\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=4\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=5\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=6\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=7\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=8\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=9\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=10\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=11\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=12\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=13\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=14\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\nid=15\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\n",
                "id=0\tblockNo=0\tval1=AM\tval2=0.5\tval3=1\n",
                # broken message
                "id=0\tblockNo=BAD\tval1=AM\tval2=0.5\tval3=1\n",
            ],
            "expected": {
                "raw_message": "id=0\tblockNo=BAD\tval1=AM\tval2=0.5\tval3=1\n",
                "error": "Found garbage after field in TSKV format: blockNo: (at row 1)\n",
            },
            "printable": True,
        },
        "CSV": {
            "data_sample": [
                '0,0,"AM",0.5,1\n',
                '1,0,"AM",0.5,1\n2,0,"AM",0.5,1\n3,0,"AM",0.5,1\n4,0,"AM",0.5,1\n5,0,"AM",0.5,1\n6,0,"AM",0.5,1\n7,0,"AM",0.5,1\n8,0,"AM",0.5,1\n9,0,"AM",0.5,1\n10,0,"AM",0.5,1\n11,0,"AM",0.5,1\n12,0,"AM",0.5,1\n13,0,"AM",0.5,1\n14,0,"AM",0.5,1\n15,0,"AM",0.5,1\n',
                '0,0,"AM",0.5,1\n',
                # broken message
                '0,"BAD","AM",0.5,1\n',
            ],
            "expected": {
                "raw_message": '0,"BAD","AM",0.5,1\n',
                "error": "Cannot parse input: expected '\"' before: 'BAD\",\"AM\",0.5,1\\n'",
            },
            "printable": True,
            "supports_empty_value": True,
        },
        "TSV": {
            "data_sample": [
                "0\t0\tAM\t0.5\t1\n",
                "1\t0\tAM\t0.5\t1\n2\t0\tAM\t0.5\t1\n3\t0\tAM\t0.5\t1\n4\t0\tAM\t0.5\t1\n5\t0\tAM\t0.5\t1\n6\t0\tAM\t0.5\t1\n7\t0\tAM\t0.5\t1\n8\t0\tAM\t0.5\t1\n9\t0\tAM\t0.5\t1\n10\t0\tAM\t0.5\t1\n11\t0\tAM\t0.5\t1\n12\t0\tAM\t0.5\t1\n13\t0\tAM\t0.5\t1\n14\t0\tAM\t0.5\t1\n15\t0\tAM\t0.5\t1\n",
                "0\t0\tAM\t0.5\t1\n",
                # broken message
                "0\tBAD\tAM\t0.5\t1\n",
            ],
            "expected": {
                "raw_message": "0\tBAD\tAM\t0.5\t1\n",
                "error": "Cannot parse input: expected '\\t' before: 'BAD\\tAM\\t0.5\\t1\\n'",
            },
            "supports_empty_value": True,
            "printable": True,
        },
        "CSVWithNames": {
            "data_sample": [
                '"id","blockNo","val1","val2","val3"\n0,0,"AM",0.5,1\n',
                '"id","blockNo","val1","val2","val3"\n1,0,"AM",0.5,1\n2,0,"AM",0.5,1\n3,0,"AM",0.5,1\n4,0,"AM",0.5,1\n5,0,"AM",0.5,1\n6,0,"AM",0.5,1\n7,0,"AM",0.5,1\n8,0,"AM",0.5,1\n9,0,"AM",0.5,1\n10,0,"AM",0.5,1\n11,0,"AM",0.5,1\n12,0,"AM",0.5,1\n13,0,"AM",0.5,1\n14,0,"AM",0.5,1\n15,0,"AM",0.5,1\n',
                '"id","blockNo","val1","val2","val3"\n0,0,"AM",0.5,1\n',
                # broken message
                '"id","blockNo","val1","val2","val3"\n0,"BAD","AM",0.5,1\n',
            ],
            "expected": {
                "raw_message": '"id","blockNo","val1","val2","val3"\n0,"BAD","AM",0.5,1\n',
                "error": "Cannot parse input: expected '\"' before: 'BAD\",\"AM\",0.5,1\\n'",
            },
            "printable": True,
        },
        "Values": {
            "data_sample": [
                "(0,0,'AM',0.5,1)",
                "(1,0,'AM',0.5,1),(2,0,'AM',0.5,1),(3,0,'AM',0.5,1),(4,0,'AM',0.5,1),(5,0,'AM',0.5,1),(6,0,'AM',0.5,1),(7,0,'AM',0.5,1),(8,0,'AM',0.5,1),(9,0,'AM',0.5,1),(10,0,'AM',0.5,1),(11,0,'AM',0.5,1),(12,0,'AM',0.5,1),(13,0,'AM',0.5,1),(14,0,'AM',0.5,1),(15,0,'AM',0.5,1)",
                "(0,0,'AM',0.5,1)",
                # broken message
                "(0,'BAD','AM',0.5,1)",
            ],
            "expected": {
                "raw_message": "(0,'BAD','AM',0.5,1)",
                "error": "Cannot parse string 'BAD' as UInt16: syntax error at begin of string. Note: there are toUInt16OrZero and toUInt16OrNull functions, which returns zero/NULL instead of throwing exception",
            },
            "supports_empty_value": True,
            "printable": True,
        },
        "TSVWithNames": {
            "data_sample": [
                "id\tblockNo\tval1\tval2\tval3\n0\t0\tAM\t0.5\t1\n",
                "id\tblockNo\tval1\tval2\tval3\n1\t0\tAM\t0.5\t1\n2\t0\tAM\t0.5\t1\n3\t0\tAM\t0.5\t1\n4\t0\tAM\t0.5\t1\n5\t0\tAM\t0.5\t1\n6\t0\tAM\t0.5\t1\n7\t0\tAM\t0.5\t1\n8\t0\tAM\t0.5\t1\n9\t0\tAM\t0.5\t1\n10\t0\tAM\t0.5\t1\n11\t0\tAM\t0.5\t1\n12\t0\tAM\t0.5\t1\n13\t0\tAM\t0.5\t1\n14\t0\tAM\t0.5\t1\n15\t0\tAM\t0.5\t1\n",
                "id\tblockNo\tval1\tval2\tval3\n0\t0\tAM\t0.5\t1\n",
                # broken message
                "id\tblockNo\tval1\tval2\tval3\n0\tBAD\tAM\t0.5\t1\n",
            ],
            "expected": {
                "raw_message": "id\tblockNo\tval1\tval2\tval3\n0\tBAD\tAM\t0.5\t1\n",
                "error": "Cannot parse input: expected '\\t' before: 'BAD\\tAM\\t0.5\\t1\\n",
            },
            "supports_empty_value": True,
            "printable": True,
        },
        "TSVWithNamesAndTypes": {
            "data_sample": [
                "id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n0\t0\tAM\t0.5\t1\n",
                "id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n1\t0\tAM\t0.5\t1\n2\t0\tAM\t0.5\t1\n3\t0\tAM\t0.5\t1\n4\t0\tAM\t0.5\t1\n5\t0\tAM\t0.5\t1\n6\t0\tAM\t0.5\t1\n7\t0\tAM\t0.5\t1\n8\t0\tAM\t0.5\t1\n9\t0\tAM\t0.5\t1\n10\t0\tAM\t0.5\t1\n11\t0\tAM\t0.5\t1\n12\t0\tAM\t0.5\t1\n13\t0\tAM\t0.5\t1\n14\t0\tAM\t0.5\t1\n15\t0\tAM\t0.5\t1\n",
                "id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n0\t0\tAM\t0.5\t1\n",
                # broken message
                "id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n0\tBAD\tAM\t0.5\t1\n",
            ],
            "expected": {
                "raw_message": "id\tblockNo\tval1\tval2\tval3\nInt64\tUInt16\tString\tFloat32\tUInt8\n0\tBAD\tAM\t0.5\t1\n",
                "error": "Cannot parse input: expected '\\t' before: 'BAD\\tAM\\t0.5\\t1\\n'",
            },
            "printable": True,
        },
        "Native": {
            "data_sample": [
                b"\x05\x01\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x00\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01",
                b"\x05\x0f\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x01\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x06\x00\x00\x00\x00\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x0a\x00\x00\x00\x00\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x0c\x00\x00\x00\x00\x00\x00\x00\x0d\x00\x00\x00\x00\x00\x00\x00\x0e\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01\x01",
                b"\x05\x01\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x00\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x55\x49\x6e\x74\x31\x36\x00\x00\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01",
                # broken message
                b"\x05\x01\x02\x69\x64\x05\x49\x6e\x74\x36\x34\x00\x00\x00\x00\x00\x00\x00\x00\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x06\x53\x74\x72\x69\x6e\x67\x03\x42\x41\x44\x04\x76\x61\x6c\x31\x06\x53\x74\x72\x69\x6e\x67\x02\x41\x4d\x04\x76\x61\x6c\x32\x07\x46\x6c\x6f\x61\x74\x33\x32\x00\x00\x00\x3f\x04\x76\x61\x6c\x33\x05\x55\x49\x6e\x74\x38\x01",
            ],
            "expected": {
                "raw_message": "050102696405496E743634000000000000000007626C6F636B4E6F06537472696E67034241440476616C3106537472696E6702414D0476616C3207466C6F617433320000003F0476616C330555496E743801",
                "error": "Cannot convert: String to UInt16",
            },
            "printable": False,
        },
        "RowBinary": {
            "data_sample": [
                b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01",
                b"\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01",
                b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01",
                # broken message
                b"\x00\x00\x00\x00\x00\x00\x00\x00\x03\x42\x41\x44\x02\x41\x4d\x00\x00\x00\x3f\x01",
            ],
            "expected": {
                "raw_message": "00000000000000000342414402414D0000003F01",
                "error": "Cannot read all data. Bytes read: 9. Bytes expected: 65.: (at row 1)\n",
            },
            "printable": False,
        },
        "RowBinaryWithNamesAndTypes": {
            "data_sample": [
                b"\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01",
                b"\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x04\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x08\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0e\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01",
                b"\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x55\x49\x6e\x74\x31\x36\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x41\x4d\x00\x00\x00\x3f\x01",
                # broken message
                b"\x05\x02\x69\x64\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x04\x76\x61\x6c\x31\x04\x76\x61\x6c\x32\x04\x76\x61\x6c\x33\x05\x49\x6e\x74\x36\x34\x06\x53\x74\x72\x69\x6e\x67\x06\x53\x74\x72\x69\x6e\x67\x07\x46\x6c\x6f\x61\x74\x33\x32\x05\x55\x49\x6e\x74\x38\x00\x00\x00\x00\x00\x00\x00\x00\x03\x42\x41\x44\x02\x41\x4d\x00\x00\x00\x3f\x01",
            ],
            "expected": {
                "raw_message": "0502696407626C6F636B4E6F0476616C310476616C320476616C3305496E74363406537472696E6706537472696E6707466C6F617433320555496E743800000000000000000342414402414D0000003F01",
                "error": "Type of 'blockNo' must be UInt16, not String",
            },
            "printable": False,
        },
        "ORC": {
            "data_sample": [
                b"\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x01\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x46\x25\x0e\x2e\x46\x03\x21\x46\x03\x09\xa6\x00\x06\x00\x32\x00\x00\xe3\x92\xe4\x62\x65\x00\x01\x21\x01\x0e\x46\x25\x2e\x2e\x26\x47\x5f\x21\x20\x96\x60\x09\x60\x00\x00\x36\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x46\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x10\x11\xc0\x00\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x05\x00\x00\xff\x00\x03\x00\x00\x30\x07\x00\x00\x40\x00\x80\x05\x00\x00\x41\x4d\x07\x00\x00\x42\x00\x80\x03\x00\x00\x0a\x07\x00\x00\x42\x00\x80\x05\x00\x00\xff\x01\x88\x00\x00\x4d\xca\xc1\x0a\x80\x30\x0c\x03\xd0\x2e\x6b\xcb\x98\x17\xf1\x14\x50\xfc\xff\xcf\xb4\x66\x1e\x3c\x84\x47\x9a\xce\x1c\xb9\x1b\xb7\xf9\xda\x48\x09\x9e\xb2\xf3\x92\xce\x5b\x86\xf6\x56\x7f\x21\x41\x2f\x51\xa6\x7a\xd7\x1d\xe5\xea\xae\x3d\xca\xd5\x83\x71\x60\xd8\x17\xfc\x62\x0f\xa8\x00\x00\xe3\x4a\xe6\x62\xe1\x60\x0c\x60\xe0\xe2\xe3\x60\x14\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\x60\x54\xe2\xe0\x62\x34\x10\x62\x34\x90\x60\x02\x8a\x70\x71\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\x82\x05\x28\xc6\xcd\x25\xca\xc1\x68\xc4\x0b\x52\xc5\x6c\xa0\x67\x2a\x05\x22\xc0\x4a\x21\x86\x31\x09\x30\x81\xb5\xb2\x02\x00\x36\x01\x00\x25\x8c\xbd\x0a\xc2\x30\x14\x85\x73\x6f\x92\xf6\x92\x6a\x09\x01\x21\x64\x92\x4e\x75\x91\x58\x71\xc9\x64\x27\x5d\x2c\x1d\x5d\xfd\x59\xc4\x42\x37\x5f\xc0\x17\xe8\x23\x9b\xc6\xe1\x3b\x70\x0f\xdf\xb9\xc4\xf5\x17\x5d\x41\x5c\x4f\x60\x37\xeb\x53\x0d\x55\x4d\x0b\x23\x01\xb9\x90\x2e\xbf\x0f\xe3\xe3\xdd\x8d\x0e\x5f\x4f\x27\x3e\xb7\x61\x97\xb2\x49\xb9\xaf\x90\x20\x92\x27\x32\x2a\x6b\xf4\xf3\x0d\x1e\x82\x20\xe8\x59\x28\x09\x4c\x46\x4c\x33\xcb\x7a\x76\x95\x41\x47\x9f\x14\x78\x03\xde\x62\x6c\x54\x30\xb1\x51\x0a\xdb\x8b\x89\x58\x11\xbb\x22\xac\x08\x9a\xe5\x6c\x71\xbf\x3d\xb8\x39\x92\xfa\x7f\x86\x1a\xd3\x54\x1e\xa7\xee\xcc\x7e\x08\x9e\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x57\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18",
                b"\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x0f\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x0f\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x7e\x25\x0e\x2e\x46\x43\x21\x46\x4b\x09\xad\x00\x06\x00\x33\x00\x00\x0a\x17\x0a\x03\x00\x00\x00\x12\x10\x08\x0f\x22\x0a\x0a\x02\x41\x4d\x12\x02\x41\x4d\x18\x3c\x50\x00\x3a\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x7e\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x66\x73\x3d\xd3\x00\x06\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x0f\x12\x06\x08\x02\x10\x02\x18\x1e\x50\x00\x05\x00\x00\x0c\x00\x2b\x00\x00\x31\x32\x33\x34\x35\x36\x37\x38\x39\x31\x30\x31\x31\x31\x32\x31\x33\x31\x34\x31\x35\x09\x00\x00\x06\x01\x03\x02\x09\x00\x00\xc0\x0e\x00\x00\x07\x00\x00\x42\x00\x80\x05\x00\x00\x41\x4d\x0a\x00\x00\xe3\xe2\x42\x01\x00\x09\x00\x00\xc0\x0e\x02\x00\x05\x00\x00\x0c\x01\x94\x00\x00\x2d\xca\xc1\x0e\x80\x30\x08\x03\xd0\xc1\x60\x2e\xf3\x62\x76\x6a\xe2\x0e\xfe\xff\x57\x5a\x3b\x0f\xe4\x51\xe8\x68\xbd\x5d\x05\xe7\xf8\x34\x40\x3a\x6e\x59\xb1\x64\xe0\x91\xa9\xbf\xb1\x97\xd2\x95\x9d\x1e\xca\x55\x3a\x6d\xb4\xd2\xdd\x0b\x74\x9a\x74\xf7\x12\x39\xbd\x97\x7f\x7c\x06\xbb\xa6\x8d\x97\x17\xb4\x00\x00\xe3\x4a\xe6\x62\xe1\xe0\x0f\x60\xe0\xe2\xe3\xe0\x17\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\xe0\x57\xe2\xe0\x62\x34\x14\x62\xb4\x94\xd0\x02\x8a\xc8\x73\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\xc2\x06\x28\x26\xc4\x25\xca\xc1\x6f\xc4\xcb\xc5\x68\x20\xc4\x6c\xa0\x67\x2a\xc5\x6c\xae\x67\x0a\x14\xe6\x87\x1a\xc6\x24\xc0\x24\x21\x07\x32\x0c\x00\x4a\x01\x00\xe3\x60\x16\x58\xc3\x24\xc5\xcd\xc1\x2c\x30\x89\x51\xc2\x4b\xc1\x57\x83\x5f\x49\x83\x83\x47\x88\x95\x91\x89\x99\x85\x55\x8a\x3d\x29\x27\x3f\x39\xdb\x2f\x5f\x8a\x29\x33\x45\x8a\xa5\x2c\x31\xc7\x10\x4c\x1a\x81\x49\x63\x25\x26\x0e\x46\x20\x66\x07\x63\x36\x0e\x3e\x0d\x26\x03\x10\x9f\xd1\x80\xdf\x8a\x85\x83\x3f\x80\xc1\x8a\x8f\x83\x5f\x88\x8d\x83\x41\x80\x41\x82\x21\x80\x21\x82\xd5\x4a\x80\x83\x5f\x89\x83\x8b\xd1\x50\x88\xd1\x52\x42\x0b\x28\x22\x6f\x25\x04\x14\xe1\xe2\x62\x72\xf4\x15\x02\x62\x09\x1b\xa0\x98\x90\x95\x28\x07\xbf\x11\x2f\x17\xa3\x81\x10\xb3\x81\x9e\xa9\x14\xb3\xb9\x9e\x29\x50\x98\x1f\x6a\x18\x93\x00\x93\x84\x1c\xc8\x30\x87\x09\x7e\x1e\x0c\x00\x08\xa8\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x5d\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18",
                b"\x4f\x52\x43\x11\x00\x00\x0a\x06\x12\x04\x08\x01\x50\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x30\x00\x00\xe3\x12\xe7\x62\x65\x00\x01\x21\x3e\x0e\x46\x25\x0e\x2e\x46\x03\x21\x46\x03\x09\xa6\x00\x06\x00\x32\x00\x00\xe3\x92\xe4\x62\x65\x00\x01\x21\x01\x0e\x46\x25\x2e\x2e\x26\x47\x5f\x21\x20\x96\x60\x09\x60\x00\x00\x36\x00\x00\xe3\x92\xe1\x62\x65\x00\x01\x21\x61\x0e\x46\x23\x5e\x2e\x46\x03\x21\x66\x03\x3d\x53\x29\x10\x11\xc0\x00\x00\x2b\x00\x00\x0a\x13\x0a\x03\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x05\x00\x00\xff\x00\x03\x00\x00\x30\x07\x00\x00\x40\x00\x80\x05\x00\x00\x41\x4d\x07\x00\x00\x42\x00\x80\x03\x00\x00\x0a\x07\x00\x00\x42\x00\x80\x05\x00\x00\xff\x01\x88\x00\x00\x4d\xca\xc1\x0a\x80\x30\x0c\x03\xd0\x2e\x6b\xcb\x98\x17\xf1\x14\x50\xfc\xff\xcf\xb4\x66\x1e\x3c\x84\x47\x9a\xce\x1c\xb9\x1b\xb7\xf9\xda\x48\x09\x9e\xb2\xf3\x92\xce\x5b\x86\xf6\x56\x7f\x21\x41\x2f\x51\xa6\x7a\xd7\x1d\xe5\xea\xae\x3d\xca\xd5\x83\x71\x60\xd8\x17\xfc\x62\x0f\xa8\x00\x00\xe3\x4a\xe6\x62\xe1\x60\x0c\x60\xe0\xe2\xe3\x60\x14\x62\xe3\x60\x10\x60\x90\x60\x08\x60\x88\x60\xe5\x12\xe0\x60\x54\xe2\xe0\x62\x34\x10\x62\x34\x90\x60\x02\x8a\x70\x71\x09\x01\x45\xb8\xb8\x98\x1c\x7d\x85\x80\x58\x82\x05\x28\xc6\xcd\x25\xca\xc1\x68\xc4\x0b\x52\xc5\x6c\xa0\x67\x2a\x05\x22\xc0\x4a\x21\x86\x31\x09\x30\x81\xb5\xb2\x02\x00\x36\x01\x00\x25\x8c\xbd\x0a\xc2\x30\x14\x85\x73\x6f\x92\xf6\x92\x6a\x09\x01\x21\x64\x92\x4e\x75\x91\x58\x71\xc9\x64\x27\x5d\x2c\x1d\x5d\xfd\x59\xc4\x42\x37\x5f\xc0\x17\xe8\x23\x9b\xc6\xe1\x3b\x70\x0f\xdf\xb9\xc4\xf5\x17\x5d\x41\x5c\x4f\x60\x37\xeb\x53\x0d\x55\x4d\x0b\x23\x01\xb9\x90\x2e\xbf\x0f\xe3\xe3\xdd\x8d\x0e\x5f\x4f\x27\x3e\xb7\x61\x97\xb2\x49\xb9\xaf\x90\x20\x92\x27\x32\x2a\x6b\xf4\xf3\x0d\x1e\x82\x20\xe8\x59\x28\x09\x4c\x46\x4c\x33\xcb\x7a\x76\x95\x41\x47\x9f\x14\x78\x03\xde\x62\x6c\x54\x30\xb1\x51\x0a\xdb\x8b\x89\x58\x11\xbb\x22\xac\x08\x9a\xe5\x6c\x71\xbf\x3d\xb8\x39\x92\xfa\x7f\x86\x1a\xd3\x54\x1e\xa7\xee\xcc\x7e\x08\x9e\x01\x10\x01\x18\x80\x80\x10\x22\x02\x00\x0c\x28\x57\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18",
                # broken message
                b"\x4f\x52\x43\x0a\x0b\x0a\x03\x00\x00\x00\x12\x04\x08\x01\x50\x00\x0a\x15\x0a\x05\x00\x00\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x0a\x12\x0a\x06\x00\x00\x00\x00\x00\x00\x12\x08\x08\x01\x42\x02\x08\x06\x50\x00\x0a\x12\x0a\x06\x00\x00\x00\x00\x00\x00\x12\x08\x08\x01\x42\x02\x08\x04\x50\x00\x0a\x29\x0a\x04\x00\x00\x00\x00\x12\x21\x08\x01\x1a\x1b\x09\x00\x00\x00\x00\x00\x00\xe0\x3f\x11\x00\x00\x00\x00\x00\x00\xe0\x3f\x19\x00\x00\x00\x00\x00\x00\xe0\x3f\x50\x00\x0a\x15\x0a\x05\x00\x00\x00\x00\x00\x12\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\xff\x80\xff\x80\xff\x00\xff\x80\xff\x03\x42\x41\x44\xff\x80\xff\x02\x41\x4d\xff\x80\x00\x00\x00\x3f\xff\x80\xff\x01\x0a\x06\x08\x06\x10\x00\x18\x0d\x0a\x06\x08\x06\x10\x01\x18\x17\x0a\x06\x08\x06\x10\x02\x18\x14\x0a\x06\x08\x06\x10\x03\x18\x14\x0a\x06\x08\x06\x10\x04\x18\x2b\x0a\x06\x08\x06\x10\x05\x18\x17\x0a\x06\x08\x00\x10\x00\x18\x02\x0a\x06\x08\x00\x10\x01\x18\x02\x0a\x06\x08\x01\x10\x01\x18\x02\x0a\x06\x08\x00\x10\x02\x18\x02\x0a\x06\x08\x02\x10\x02\x18\x02\x0a\x06\x08\x01\x10\x02\x18\x03\x0a\x06\x08\x00\x10\x03\x18\x02\x0a\x06\x08\x02\x10\x03\x18\x02\x0a\x06\x08\x01\x10\x03\x18\x02\x0a\x06\x08\x00\x10\x04\x18\x02\x0a\x06\x08\x01\x10\x04\x18\x04\x0a\x06\x08\x00\x10\x05\x18\x02\x0a\x06\x08\x01\x10\x05\x18\x02\x12\x04\x08\x00\x10\x00\x12\x04\x08\x00\x10\x00\x12\x04\x08\x00\x10\x00\x12\x04\x08\x00\x10\x00\x12\x04\x08\x00\x10\x00\x12\x04\x08\x00\x10\x00\x1a\x03\x47\x4d\x54\x0a\x59\x0a\x04\x08\x01\x50\x00\x0a\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x0a\x08\x08\x01\x42\x02\x08\x06\x50\x00\x0a\x08\x08\x01\x42\x02\x08\x04\x50\x00\x0a\x21\x08\x01\x1a\x1b\x09\x00\x00\x00\x00\x00\x00\xe0\x3f\x11\x00\x00\x00\x00\x00\x00\xe0\x3f\x19\x00\x00\x00\x00\x00\x00\xe0\x3f\x50\x00\x0a\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x08\x03\x10\xec\x02\x1a\x0c\x08\x03\x10\x8e\x01\x18\x1d\x20\xc1\x01\x28\x01\x22\x2e\x08\x0c\x12\x05\x01\x02\x03\x04\x05\x1a\x02\x69\x64\x1a\x07\x62\x6c\x6f\x63\x6b\x4e\x6f\x1a\x04\x76\x61\x6c\x31\x1a\x04\x76\x61\x6c\x32\x1a\x04\x76\x61\x6c\x33\x20\x00\x28\x00\x30\x00\x22\x08\x08\x04\x20\x00\x28\x00\x30\x00\x22\x08\x08\x08\x20\x00\x28\x00\x30\x00\x22\x08\x08\x08\x20\x00\x28\x00\x30\x00\x22\x08\x08\x05\x20\x00\x28\x00\x30\x00\x22\x08\x08\x01\x20\x00\x28\x00\x30\x00\x30\x01\x3a\x04\x08\x01\x50\x00\x3a\x0c\x08\x01\x12\x06\x08\x00\x10\x00\x18\x00\x50\x00\x3a\x08\x08\x01\x42\x02\x08\x06\x50\x00\x3a\x08\x08\x01\x42\x02\x08\x04\x50\x00\x3a\x21\x08\x01\x1a\x1b\x09\x00\x00\x00\x00\x00\x00\xe0\x3f\x11\x00\x00\x00\x00\x00\x00\xe0\x3f\x19\x00\x00\x00\x00\x00\x00\xe0\x3f\x50\x00\x3a\x0c\x08\x01\x12\x06\x08\x02\x10\x02\x18\x02\x50\x00\x40\x90\x4e\x48\x01\x08\xd5\x01\x10\x00\x18\x80\x80\x04\x22\x02\x00\x0b\x28\x5b\x30\x06\x82\xf4\x03\x03\x4f\x52\x43\x18",
            ],
            "expected": {
                "raw_message": "4F52430A0B0A030000001204080150000A150A050000000000120C0801120608001000180050000A120A06000000000000120808014202080650000A120A06000000000000120808014202080450000A290A0400000000122108011A1B09000000000000E03F11000000000000E03F19000000000000E03F50000A150A050000000000120C080112060802100218025000FF80FF80FF00FF80FF03424144FF80FF02414DFF800000003FFF80FF010A0608061000180D0A060806100118170A060806100218140A060806100318140A0608061004182B0A060806100518170A060800100018020A060800100118020A060801100118020A060800100218020A060802100218020A060801100218030A060800100318020A060802100318020A060801100318020A060800100418020A060801100418040A060800100518020A060801100518021204080010001204080010001204080010001204080010001204080010001204080010001A03474D540A590A04080150000A0C0801120608001000180050000A0808014202080650000A0808014202080450000A2108011A1B09000000000000E03F11000000000000E03F19000000000000E03F50000A0C080112060802100218025000080310EC021A0C0803108E01181D20C1012801222E080C120501020304051A0269641A07626C6F636B4E6F1A0476616C311A0476616C321A0476616C33200028003000220808042000280030002208080820002800300022080808200028003000220808052000280030002208080120002800300030013A04080150003A0C0801120608001000180050003A0808014202080650003A0808014202080450003A2108011A1B09000000000000E03F11000000000000E03F19000000000000E03F50003A0C08011206080210021802500040904E480108D5011000188080042202000B285B300682F403034F524318",
                "error": "Cannot parse string 'BAD' as UInt16: syntax error at begin of string. Note: there are toUInt16OrZero and toUInt16OrNull functions, which returns zero/NULL instead of throwing exception.",
            },
            "printable": False,
        },
    }

    topic_name_prefix = "format_tests_4_stream_"
    topic_name_postfix = get_topic_postfix(create_query_generator)
    for format_name, format_opts in list(all_formats.items()):
        logging.debug(f"Set up {format_name}")
        topic_name = f"{topic_name_prefix}{format_name}{topic_name_postfix}"
        data_sample = format_opts["data_sample"]
        data_prefix = []
        raw_message = "_raw_message"
        # prepend empty value when supported
        if format_opts.get("supports_empty_value", False):
            data_prefix = data_prefix + [""]
        if format_opts.get("printable", False) == False:
            raw_message = "hex(_raw_message)"
        kafka_produce(kafka_cluster, topic_name, data_prefix + data_sample)
        create_query = create_query_generator(
            f"kafka_{format_name}",
            "id Int64, blockNo UInt16, val1 String, val2 Float32, val3 UInt8",
            topic_list=topic_name,
            consumer_group=topic_name,
            format=format_name,
            settings={
                "kafka_handle_error_mode": "stream",
                "kafka_flush_interval_ms": 1000,
            },
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka_{format_name};

            {create_query};

            DROP TABLE IF EXISTS test.kafka_data_{format_name}_mv;
            CREATE MATERIALIZED VIEW test.kafka_data_{format_name}_mv ENGINE=MergeTree ORDER BY tuple() AS
                SELECT *, _topic, _partition, _offset FROM test.kafka_{format_name}
                WHERE length(_error) = 0;

            DROP TABLE IF EXISTS test.kafka_errors_{format_name}_mv;
            CREATE MATERIALIZED VIEW test.kafka_errors_{format_name}_mv ENGINE=MergeTree ORDER BY tuple() AS
                SELECT {raw_message} as raw_message, _error as error, _topic as topic, _partition as partition, _offset as offset FROM test.kafka_{format_name}
                WHERE length(_error) > 0;
            """
        )

    raw_expected = """\
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
"""

    expected_rows_count = raw_expected.count("\n")
    result_checker = lambda res: res.count("\n") == expected_rows_count
    res = instance.query_with_retry(
        f"SELECT * FROM test.kafka_data_{list(all_formats.keys())[-1]}_mv;",
        retry_count=30,
        sleep_time=1,
        check_callback=result_checker,
    )
    assert result_checker(res)

    for format_name, format_opts in list(all_formats.items()):
        logging.debug(f"Checking {format_name}")
        topic_name = f"{topic_name_prefix}{format_name}{topic_name_postfix}"
        # shift offsets by 1 if format supports empty value
        offsets = (
            [1, 2, 3] if format_opts.get("supports_empty_value", False) else [0, 1, 2]
        )
        result = instance.query(
            "SELECT * FROM test.kafka_data_{format_name}_mv;".format(
                format_name=format_name
            )
        )
        expected = raw_expected.format(
            topic_name=topic_name,
            offset_0=offsets[0],
            offset_1=offsets[1],
            offset_2=offsets[2],
        )
        # print(('Checking result\n {result} \n expected \n {expected}\n'.format(result=str(result), expected=str(expected))))
        assert TSV(result) == TSV(expected), "Proper result for format: {}".format(
            format_name
        )
        errors_result = json.loads(
            instance.query(
                "SELECT raw_message, error FROM test.kafka_errors_{format_name}_mv format JSONEachRow".format(
                    format_name=format_name
                )
            )
        )
        # print(errors_result.strip())
        # print(errors_expected.strip())
        assert (
            errors_result["raw_message"] == format_opts["expected"]["raw_message"]
        ), "Proper raw_message for format: {}".format(format_name)
        # Errors text can change, just checking prefixes
        assert (
            format_opts["expected"]["error"] in errors_result["error"]
        ), "Proper error for format: {}".format(format_name)
        kafka_delete_topic(admin_client, topic_name)


@pytest.mark.parametrize(
    "create_query_generator",
    [
        generate_old_create_table_query,
        generate_new_create_table_query,
    ],
)
def test_kafka_consumer_failover(kafka_cluster, create_query_generator):
    topic_name = "kafka_consumer_failover" + get_topic_postfix(create_query_generator)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name, num_partitions=2):
        consumer_group = f"{topic_name}_group"
        create_queries = []
        for counter in range(3):
            create_queries.append(
                create_query_generator(
                    f"kafka{counter+1}",
                    "key UInt64, value UInt64",
                    topic_list=topic_name,
                    consumer_group=consumer_group,
                    settings={
                        "kafka_max_block_size": 1,
                        "kafka_poll_timeout_ms": 200,
                    },
                )
            )

        instance.query(
            f"""
            {create_queries[0]};
            {create_queries[1]};
            {create_queries[2]};

            CREATE TABLE test.destination (
                key UInt64,
                value UInt64,
                _consumed_by LowCardinality(String)
            )
            ENGINE = MergeTree()
            ORDER BY key;

            CREATE MATERIALIZED VIEW test.kafka1_mv TO test.destination AS
            SELECT key, value, 'kafka1' as _consumed_by
            FROM test.kafka1;

            CREATE MATERIALIZED VIEW test.kafka2_mv TO test.destination AS
            SELECT key, value, 'kafka2' as _consumed_by
            FROM test.kafka2;

            CREATE MATERIALIZED VIEW test.kafka3_mv TO test.destination AS
            SELECT key, value, 'kafka3' as _consumed_by
            FROM test.kafka3;
            """
        )

        producer = KafkaProducer(
            bootstrap_servers="localhost:{}".format(cluster.kafka_port),
            value_serializer=producer_serializer,
            key_serializer=producer_serializer,
        )

        ## all 3 attached, 2 working
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 1, "value": 1}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 1, "value": 1}),
            partition=1,
        )
        producer.flush()

        count_query = "SELECT count() FROM test.destination"
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > 0
        )

        ## 2 attached, 2 working
        instance.query("DETACH TABLE test.kafka1")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 2, "value": 2}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 2, "value": 2}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 1 attached, 1 working
        instance.query("DETACH TABLE test.kafka2")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 3, "value": 3}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 3, "value": 3}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 2 attached, 2 working
        instance.query("ATTACH TABLE test.kafka1")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 4, "value": 4}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 4, "value": 4}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 1 attached, 1 working
        instance.query("DETACH TABLE test.kafka3")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 5, "value": 5}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 5, "value": 5}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 2 attached, 2 working
        instance.query("ATTACH TABLE test.kafka2")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 6, "value": 6}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 6, "value": 6}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 3 attached, 2 working
        instance.query("ATTACH TABLE test.kafka3")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 7, "value": 7}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 7, "value": 7}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )

        ## 2 attached, same 2 working
        instance.query("DETACH TABLE test.kafka3")
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 8, "value": 8}),
            partition=0,
        )
        producer.send(
            topic=topic_name,
            value=json.dumps({"key": 8, "value": 8}),
            partition=1,
        )
        producer.flush()
        prev_count = instance.query_with_retry(
            count_query, check_callback=lambda res: int(res) > prev_count
        )


def test_kafka_predefined_configuration(kafka_cluster):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )
    topic_name = "conf"
    kafka_create_topic(admin_client, topic_name)

    messages = []
    for i in range(50):
        messages.append("{i}, {i}".format(i=i))
    kafka_produce(kafka_cluster, topic_name, messages)

    instance.query(
        f"""
        CREATE TABLE test.kafka (key UInt64, value UInt64) ENGINE = Kafka(kafka1, kafka_format='CSV');
        """
    )

    result = ""
    while True:
        result += instance.query("SELECT * FROM test.kafka", ignore_error=True)
        if kafka_check_result(result):
            break
    kafka_check_result(result, True)


# https://github.com/ClickHouse/ClickHouse/issues/26643
@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_issue26643(kafka_cluster, create_query_generator):
    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port),
        value_serializer=producer_serializer,
    )
    topic_name = "test_issue26643" + get_topic_postfix(create_query_generator)
    thread_per_consumer = must_use_thread_per_consumer(create_query_generator)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
        msg = message_with_repeated_pb2.Message(
            tnow=1629000000,
            server="server1",
            clien="host1",
            sPort=443,
            cPort=50000,
            r=[
                message_with_repeated_pb2.dd(
                    name="1", type=444, ttl=123123, data=b"adsfasd"
                ),
                message_with_repeated_pb2.dd(name="2"),
            ],
            method="GET",
        )

        data = b""
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg

        msg = message_with_repeated_pb2.Message(tnow=1629000002)

        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg

        producer.send(topic_name, value=data)

        data = _VarintBytes(len(serialized_msg)) + serialized_msg
        producer.send(topic_name, value=data)
        producer.flush()

        create_query = create_query_generator(
            "test_queue",
            """`tnow` UInt32,
               `server` String,
               `client` String,
               `sPort` UInt16,
               `cPort` UInt16,
               `r.name` Array(String),
               `r.class` Array(UInt16),
               `r.type` Array(UInt16),
               `r.ttl` Array(UInt32),
               `r.data` Array(String),
               `method` String""",
            topic_list=topic_name,
            consumer_group=f"{topic_name}_group",
            format="Protobuf",
            settings={
                "kafka_schema": "message_with_repeated.proto:Message",
                "kafka_skip_broken_messages": 10000,
                "kafka_thread_per_consumer": thread_per_consumer,
            },
        )

        instance.query(
            f"""
            {create_query};

            SET allow_suspicious_low_cardinality_types=1;

            CREATE TABLE test.log
            (
                `tnow` DateTime('Asia/Istanbul') CODEC(DoubleDelta, LZ4),
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
            """
        )

        instance.wait_for_log_line("Committed offset")
        result = instance.query("SELECT * FROM test.log")

        expected = """\
    2021-08-15 07:00:00	server1		443	50000	['1','2']	[0,0]	[444,0]	[123123,0]	['adsfasd','']	GET
    2021-08-15 07:00:02			0	0	[]	[]	[]	[]	[]
    2021-08-15 07:00:02			0	0	[]	[]	[]	[]	[]
    """
        assert TSV(result) == TSV(expected)


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_num_consumers_limit(kafka_cluster, create_query_generator):
    instance.query("DROP TABLE IF EXISTS test.kafka")

    thread_per_consumer = must_use_thread_per_consumer(create_query_generator)

    create_query = create_query_generator(
        "kafka",
        "key UInt64, value UInt64",
        settings={
            "kafka_num_consumers": 100,
            "kafka_thread_per_consumer": thread_per_consumer,
        },
    )
    error = instance.query_and_get_error(create_query)

    assert (
        "BAD_ARGUMENTS" in error
        and "The number of consumers can not be bigger than" in error
    )

    instance.query(
        f"""
        SET kafka_disable_num_consumers_limit = 1;
        {create_query};
        """
    )

    instance.query("DROP TABLE test.kafka")


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_format_with_prefix_and_suffix(kafka_cluster, create_query_generator):
    topic_name = "custom" + get_topic_postfix(create_query_generator)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
        create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="CustomSeparated",
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka;
            {create_query};
            """
        )

        instance.query(
            "INSERT INTO test.kafka select number*10 as key, number*100 as value from numbers(2) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
        )

        message_count = 2
        messages = kafka_consume_with_retry(kafka_cluster, topic_name, message_count)

        assert len(messages) == 2

        assert (
            "".join(messages)
            == "<prefix>\n0\t0\n<suffix>\n<prefix>\n10\t100\n<suffix>\n"
        )


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_max_rows_per_message(kafka_cluster, create_query_generator):
    topic_name = "custom_max_rows_per_message" + get_topic_postfix(
        create_query_generator
    )

    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
        num_rows = 5

        create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="CustomSeparated",
            settings={
                "format_custom_result_before_delimiter": "<prefix>\n",
                "format_custom_result_after_delimiter": "<suffix>\n",
                "kafka_max_rows_per_message": 3,
            },
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.kafka;
            {create_query};

            CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY (key, value) AS
                SELECT key, value FROM test.kafka;
        """
        )

        instance.query(
            f"INSERT INTO test.kafka select number*10 as key, number*100 as value from numbers({num_rows}) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
        )

        message_count = 2
        messages = kafka_consume_with_retry(kafka_cluster, topic_name, message_count)

        assert len(messages) == message_count

        assert (
            "".join(messages)
            == "<prefix>\n0\t0\n10\t100\n20\t200\n<suffix>\n<prefix>\n30\t300\n40\t400\n<suffix>\n"
        )

        instance.query_with_retry(
            "SELECT count() FROM test.view",
            check_callback=lambda res: int(res) == num_rows,
        )

        result = instance.query("SELECT * FROM test.view")
        assert result == "0\t0\n10\t100\n20\t200\n30\t300\n40\t400\n"


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_row_based_formats(kafka_cluster, create_query_generator):
    admin_client = get_admin_client(kafka_cluster)

    for format_name in [
        "TSV",
        "TSVWithNamesAndTypes",
        "TSKV",
        "CSV",
        "CSVWithNamesAndTypes",
        "CustomSeparatedWithNamesAndTypes",
        "Values",
        "JSON",
        "JSONEachRow",
        "JSONCompactEachRow",
        "JSONCompactEachRowWithNamesAndTypes",
        "JSONObjectEachRow",
        "Avro",
        "RowBinary",
        "RowBinaryWithNamesAndTypes",
        "MsgPack",
    ]:
        logging.debug("Checking {format_name}")

        topic_name = format_name + get_topic_postfix(create_query_generator)
        table_name = f"kafka_{format_name}"

        with kafka_topic(admin_client, topic_name):
            num_rows = 10
            max_rows_per_message = 5
            message_count = num_rows / max_rows_per_message

            create_query = create_query_generator(
                table_name,
                "key UInt64, value UInt64",
                topic_list=topic_name,
                consumer_group=topic_name,
                format=format_name,
                settings={"kafka_max_rows_per_message": max_rows_per_message},
            )

            instance.query(
                f"""
                DROP TABLE IF EXISTS test.view;
                DROP TABLE IF EXISTS test.{table_name};

                {create_query};

                CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY (key, value) AS
                    SELECT key, value FROM test.{table_name};

                INSERT INTO test.{table_name} SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows});
            """
            )

            messages = kafka_consume_with_retry(
                kafka_cluster, topic_name, message_count, need_decode=False
            )

            assert len(messages) == message_count

            instance.query_with_retry(
                "SELECT count() FROM test.view",
                check_callback=lambda res: int(res) == num_rows,
            )

            result = instance.query("SELECT * FROM test.view")
            expected = ""
            for i in range(num_rows):
                expected += str(i * 10) + "\t" + str(i * 100) + "\n"
            assert result == expected


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_block_based_formats_1(kafka_cluster, create_query_generator):
    topic_name = "pretty_space" + get_topic_postfix(create_query_generator)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
        create_query = create_query_generator(
            "kafka",
            "key UInt64, value UInt64",
            topic_list=topic_name,
            consumer_group=topic_name,
            format="PrettySpace",
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka;

            {create_query};

            INSERT INTO test.kafka SELECT number * 10 as key, number * 100 as value FROM numbers(5) settings max_block_size=2, optimize_trivial_insert_select=0, output_format_pretty_color=1, output_format_pretty_row_numbers=0;
        """
        )

        message_count = 3
        messages = kafka_consume_with_retry(kafka_cluster, topic_name, message_count)
        assert len(messages) == 3

        data = []
        for message in messages:
            splitted = message.split("\n")
            assert splitted[0] == " \x1b[1mkey\x1b[0m   \x1b[1mvalue\x1b[0m"
            assert splitted[1] == ""
            assert splitted[-1] == ""
            data += [line.split() for line in splitted[2:-1]]

        assert data == [
            ["0", "0"],
            ["10", "100"],
            ["20", "200"],
            ["30", "300"],
            ["40", "400"],
        ]


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_block_based_formats_2(kafka_cluster, create_query_generator):
    admin_client = get_admin_client(kafka_cluster)
    num_rows = 100
    message_count = 9

    for format_name in [
        "JSONColumns",
        "Native",
        "Arrow",
        "Parquet",
        "ORC",
        "JSONCompactColumns",
    ]:
        topic_name = format_name + get_topic_postfix(create_query_generator)
        table_name = f"kafka_{format_name}"
        logging.debug(f"Checking format {format_name}")
        with kafka_topic(admin_client, topic_name):
            create_query = create_query_generator(
                table_name,
                "key UInt64, value UInt64",
                topic_list=topic_name,
                consumer_group=topic_name,
                format=format_name,
            )

            instance.query(
                f"""
                DROP TABLE IF EXISTS test.view;
                DROP TABLE IF EXISTS test.{table_name};

                {create_query};

                CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY (key, value) AS
                    SELECT key, value FROM test.{table_name};

                INSERT INTO test.{table_name} SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows}) settings max_block_size=12, optimize_trivial_insert_select=0;
            """
            )
            messages = kafka_consume_with_retry(
                kafka_cluster, topic_name, message_count, need_decode=False
            )
            assert len(messages) == message_count

            rows = int(
                instance.query_with_retry(
                    "SELECT count() FROM test.view",
                    check_callback=lambda res: int(res) == num_rows,
                )
            )

            assert rows == num_rows

            result = instance.query("SELECT * FROM test.view ORDER by key")
            expected = ""
            for i in range(num_rows):
                expected += str(i * 10) + "\t" + str(i * 100) + "\n"
            assert result == expected


def test_system_kafka_consumers(kafka_cluster):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    topic = "system_kafka_cons"
    kafka_create_topic(admin_client, topic)

    # Check that format_csv_delimiter parameter works now - as part of all available format settings.
    kafka_produce(
        kafka_cluster,
        topic,
        ["1|foo", "2|bar", "42|answer", "100|multi\n101|row\n103|message"],
    )

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;

        CREATE TABLE test.kafka (a UInt64, b String)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}',
                     kafka_commit_on_select = 1,
                     kafka_format = 'CSV',
                     kafka_row_delimiter = '\\n',
                     format_csv_delimiter = '|';
        """
    )

    result = instance.query("SELECT * FROM test.kafka ORDER BY a;")

    result_system_kafka_consumers = instance.query(
        """
        create or replace function stable_timestamp as
          (d)->multiIf(d==toDateTime('1970-01-01 00:00:00'), 'never', abs(dateDiff('second', d, now())) < 30, 'now', toString(d));

        SELECT database, table, length(consumer_id), assignments.topic, assignments.partition_id,
          assignments.current_offset,
          if(length(exceptions.time)>0, exceptions.time[1]::String, 'never') as last_exception_time_,
          if(length(exceptions.text)>0, exceptions.text[1], 'no exception') as last_exception_,
          stable_timestamp(last_poll_time) as last_poll_time_, num_messages_read, stable_timestamp(last_commit_time) as last_commit_time_,
          num_commits, stable_timestamp(last_rebalance_time) as last_rebalance_time_,
          num_rebalance_revocations, num_rebalance_assignments, is_currently_used
          FROM system.kafka_consumers WHERE database='test' and table='kafka' format Vertical;
        """
    )
    logging.debug(f"result_system_kafka_consumers: {result_system_kafka_consumers}")
    assert (
        result_system_kafka_consumers
        == """Row 1:
──────
database:                   test
table:                      kafka
length(consumer_id):        67
assignments.topic:          ['system_kafka_cons']
assignments.partition_id:   [0]
assignments.current_offset: [4]
last_exception_time_:       never
last_exception_:            no exception
last_poll_time_:            now
num_messages_read:          4
last_commit_time_:          now
num_commits:                1
last_rebalance_time_:       never
num_rebalance_revocations:  0
num_rebalance_assignments:  1
is_currently_used:          0
"""
    )

    instance.query("DROP TABLE test.kafka")
    kafka_delete_topic(admin_client, topic)


def test_system_kafka_consumers_rebalance(kafka_cluster, max_retries=15):
    # based on test_kafka_consumer_hang2
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(cluster.kafka_port),
        value_serializer=producer_serializer,
        key_serializer=producer_serializer,
    )

    topic = "system_kafka_cons2"
    kafka_create_topic(admin_client, topic, num_partitions=2)

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka2;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}',
                     kafka_commit_on_select = 1,
                     kafka_format = 'JSONEachRow';

        CREATE TABLE test.kafka2 (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_commit_on_select = 1,
                     kafka_group_name = '{topic}',
                     kafka_format = 'JSONEachRow';
        """
    )

    producer.send(topic=topic, value=json.dumps({"key": 1, "value": 1}), partition=0)
    producer.send(topic=topic, value=json.dumps({"key": 11, "value": 11}), partition=1)
    time.sleep(3)

    # first consumer subscribe the topic, try to poll some data, and go to rest
    instance.query("SELECT * FROM test.kafka")

    # second consumer do the same leading to rebalance in the first
    # consumer, try to poll some data
    instance.query("SELECT * FROM test.kafka2")

    producer.send(topic=topic, value=json.dumps({"key": 1, "value": 1}), partition=0)
    producer.send(topic=topic, value=json.dumps({"key": 10, "value": 10}), partition=1)
    time.sleep(3)

    instance.query("SELECT * FROM test.kafka")
    instance.query("SELECT * FROM test.kafka2")
    instance.query("SELECT * FROM test.kafka")
    instance.query("SELECT * FROM test.kafka2")

    result_system_kafka_consumers = instance.query(
        """
        create or replace function stable_timestamp as
          (d)->multiIf(d==toDateTime('1970-01-01 00:00:00'), 'never', abs(dateDiff('second', d, now())) < 30, 'now', toString(d));
        SELECT database, table, length(consumer_id), assignments.topic, assignments.partition_id,
          assignments.current_offset,
          if(length(exceptions.time)>0, exceptions.time[1]::String, 'never') as last_exception_time_,
          if(length(exceptions.text)>0, exceptions.text[1], 'no exception') as last_exception_,
          stable_timestamp(last_poll_time) as last_poll_time_, stable_timestamp(last_commit_time) as last_commit_time_,
          num_commits, stable_timestamp(last_rebalance_time) as last_rebalance_time_,
          num_rebalance_revocations, num_rebalance_assignments, is_currently_used
          FROM system.kafka_consumers WHERE database='test' and table IN ('kafka', 'kafka2') format Vertical;
        """
    )
    logging.debug(f"result_system_kafka_consumers: {result_system_kafka_consumers}")
    assert (
        result_system_kafka_consumers
        == """Row 1:
──────
database:                   test
table:                      kafka
length(consumer_id):        67
assignments.topic:          ['system_kafka_cons2']
assignments.partition_id:   [0]
assignments.current_offset: [2]
last_exception_time_:       never
last_exception_:            no exception
last_poll_time_:            now
last_commit_time_:          now
num_commits:                2
last_rebalance_time_:       now
num_rebalance_revocations:  1
num_rebalance_assignments:  2
is_currently_used:          0

Row 2:
──────
database:                   test
table:                      kafka2
length(consumer_id):        68
assignments.topic:          ['system_kafka_cons2']
assignments.partition_id:   [1]
assignments.current_offset: [2]
last_exception_time_:       never
last_exception_:            no exception
last_poll_time_:            now
last_commit_time_:          now
num_commits:                1
last_rebalance_time_:       never
num_rebalance_revocations:  0
num_rebalance_assignments:  1
is_currently_used:          0
"""
    )

    instance.query("DROP TABLE test.kafka")
    instance.query("DROP TABLE test.kafka2")

    kafka_delete_topic(admin_client, topic)


def test_system_kafka_consumers_rebalance_mv(kafka_cluster, max_retries=15):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    producer = KafkaProducer(
        bootstrap_servers="localhost:{}".format(cluster.kafka_port),
        value_serializer=producer_serializer,
        key_serializer=producer_serializer,
    )

    topic = "system_kafka_cons_mv"
    kafka_create_topic(admin_client, topic, num_partitions=2)

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.kafka;
        DROP TABLE IF EXISTS test.kafka2;
        DROP TABLE IF EXISTS test.kafka_persistent;
        DROP TABLE IF EXISTS test.kafka_persistent2;

        CREATE TABLE test.kafka (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_group_name = '{topic}',
                     kafka_commit_on_select = 1,
                     kafka_format = 'JSONEachRow';

        CREATE TABLE test.kafka2 (key UInt64, value UInt64)
            ENGINE = Kafka
            SETTINGS kafka_broker_list = 'kafka1:19092',
                     kafka_topic_list = '{topic}',
                     kafka_commit_on_select = 1,
                     kafka_group_name = '{topic}',
                     kafka_format = 'JSONEachRow';

        CREATE TABLE test.kafka_persistent (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.kafka_persistent2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;

        CREATE MATERIALIZED VIEW test.persistent_kafka_mv TO test.kafka_persistent AS
        SELECT key, value
        FROM test.kafka;

        CREATE MATERIALIZED VIEW test.persistent_kafka_mv2 TO test.kafka_persistent2 AS
        SELECT key, value
        FROM test.kafka2;
        """
    )

    producer.send(topic=topic, value=json.dumps({"key": 1, "value": 1}), partition=0)
    producer.send(topic=topic, value=json.dumps({"key": 11, "value": 11}), partition=1)
    time.sleep(3)

    retries = 0
    result_rdkafka_stat = ""
    while True:
        result_rdkafka_stat = instance.query(
            """
            SELECT table, JSONExtractString(rdkafka_stat, 'type')
            FROM system.kafka_consumers WHERE database='test' and table = 'kafka' format Vertical;
            """
        )
        if result_rdkafka_stat.find("consumer") or retries > max_retries:
            break
        retries += 1
        time.sleep(1)

    assert (
        result_rdkafka_stat
        == """Row 1:
──────
table:                                   kafka
JSONExtractString(rdkafka_stat, 'type'): consumer
"""
    )

    instance.query("DROP TABLE test.kafka")
    instance.query("DROP TABLE test.kafka2")
    instance.query("DROP TABLE test.kafka_persistent")
    instance.query("DROP TABLE test.kafka_persistent2")
    instance.query("DROP TABLE test.persistent_kafka_mv")
    instance.query("DROP TABLE test.persistent_kafka_mv2")

    kafka_delete_topic(admin_client, topic)


def test_formats_errors(kafka_cluster):
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:{}".format(kafka_cluster.kafka_port)
    )

    for format_name in [
        "Template",
        "Regexp",
        "TSV",
        "TSVWithNamesAndTypes",
        "TSKV",
        "CSV",
        "CSVWithNames",
        "CSVWithNamesAndTypes",
        "CustomSeparated",
        "CustomSeparatedWithNames",
        "CustomSeparatedWithNamesAndTypes",
        "Values",
        "JSON",
        "JSONEachRow",
        "JSONStringsEachRow",
        "JSONCompactEachRow",
        "JSONCompactEachRowWithNamesAndTypes",
        "JSONObjectEachRow",
        "Avro",
        "RowBinary",
        "RowBinaryWithNamesAndTypes",
        "MsgPack",
        "JSONColumns",
        "JSONCompactColumns",
        "JSONColumnsWithMetadata",
        "BSONEachRow",
        "Native",
        "Arrow",
        "Parquet",
        "ORC",
        "JSONCompactColumns",
        "Npy",
        "ParquetMetadata",
        "CapnProto",
        "Protobuf",
        "ProtobufSingle",
        "ProtobufList",
        "DWARF",
        "HiveText",
        "MySQLDump",
    ]:
        with kafka_topic(admin_client, format_name):
            table_name = f"kafka_{format_name}"

            instance.query(
                f"""
                DROP TABLE IF EXISTS test.view;
                DROP TABLE IF EXISTS test.{table_name};

                CREATE TABLE test.{table_name} (key UInt64, value UInt64)
                    ENGINE = Kafka
                    SETTINGS kafka_broker_list = 'kafka1:19092',
                            kafka_topic_list = '{format_name}',
                            kafka_group_name = '{format_name}',
                            kafka_format = '{format_name}',
                            kafka_max_rows_per_message = 5,
                            format_template_row='template_row.format',
                            format_regexp='id: (.+?)',
                            input_format_with_names_use_header=0,
                            format_schema='key_value_message:Message';

                CREATE MATERIALIZED VIEW test.view ENGINE=MergeTree ORDER BY (key, value) AS
                    SELECT key, value FROM test.{table_name};
            """
            )

            kafka_produce(
                kafka_cluster,
                format_name,
                ["Broken message\nBroken message\nBroken message\n"],
            )

            num_errors = int(
                instance.query_with_retry(
                    f"SELECT length(exceptions.text) from system.kafka_consumers where database = 'test' and table = '{table_name}'",
                    check_callback=lambda res: int(res) > 0,
                )
            )

            assert num_errors > 0

            instance.query(f"DROP TABLE test.{table_name}")
            instance.query("DROP TABLE test.view")


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_multiple_read_in_materialized_views(kafka_cluster, create_query_generator):
    topic_name = "multiple_read_from_mv" + get_topic_postfix(create_query_generator)

    with kafka_topic(get_admin_client(kafka_cluster), topic_name):
        create_query = create_query_generator(
            "kafka_multiple_read_input",
            "id Int64",
            topic_list=topic_name,
            consumer_group=topic_name,
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS test.kafka_multiple_read_input SYNC;
            DROP TABLE IF EXISTS test.kafka_multiple_read_table;
            DROP TABLE IF EXISTS test.kafka_multiple_read_mv;

            {create_query};

            CREATE TABLE test.kafka_multiple_read_table (id Int64)
            ENGINE = MergeTree
            ORDER BY id;


            CREATE MATERIALIZED VIEW test.kafka_multiple_read_mv TO test.kafka_multiple_read_table AS
            SELECT id
            FROM test.kafka_multiple_read_input
            WHERE id NOT IN (
                SELECT id
                FROM test.kafka_multiple_read_table
                WHERE id IN (
                    SELECT id
                    FROM test.kafka_multiple_read_input
                )
            );
            """
        )

        kafka_produce(
            kafka_cluster, topic_name, [json.dumps({"id": 42}), json.dumps({"id": 43})]
        )

        expected_result = "42\n43\n"
        res = instance.query_with_retry(
            f"SELECT id FROM test.kafka_multiple_read_table ORDER BY id",
            check_callback=lambda res: res == expected_result,
        )
        assert res == expected_result

        # Verify that the query deduplicates the records as it meant to be
        messages = []
        for _ in range(0, 10):
            messages.append(json.dumps({"id": 42}))
            messages.append(json.dumps({"id": 43}))

        messages.append(json.dumps({"id": 44}))

        kafka_produce(kafka_cluster, topic_name, messages)

        expected_result = "42\n43\n44\n"
        res = instance.query_with_retry(
            f"SELECT id FROM test.kafka_multiple_read_table ORDER BY id",
            check_callback=lambda res: res == expected_result,
        )
        assert res == expected_result

        instance.query(
            f"""
            DROP TABLE test.kafka_multiple_read_input;
            DROP TABLE test.kafka_multiple_read_table;
            DROP TABLE test.kafka_multiple_read_mv;
            """
        )


@pytest.mark.parametrize(
    "create_query_generator",
    [generate_old_create_table_query, generate_new_create_table_query],
)
def test_kafka_null_message(kafka_cluster, create_query_generator):
    topic_name = "null_message"

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.null_message_view;
        DROP TABLE IF EXISTS test.null_message_consumer;
        DROP TABLE IF EXISTS test.null_message_kafka;

        {create_query_generator("null_message_kafka", "value UInt64", topic_list=topic_name, consumer_group="mv")};
        CREATE TABLE test.null_message_view (value UInt64)
            ENGINE = MergeTree()
            ORDER BY value;
        CREATE MATERIALIZED VIEW test.null_message_consumer TO test.null_message_view AS
            SELECT * FROM test.null_message_kafka;
    """
    )

    message_key_values = []
    for i in range(5):
        # Here the key is key for Kafka message
        message = json.dumps({"value": i}) if i != 3 else None
        message_key_values.append({"key": f"{i}".encode(), "message": message})

    producer = get_kafka_producer(kafka_cluster.kafka_port, producer_serializer, 15)
    for message_kv in message_key_values:
        producer.send(
            topic=topic_name, key=message_kv["key"], value=message_kv["message"]
        )
        producer.flush()

    expected = TSV(
        """
0
1
2
4
"""
    )
    with existing_kafka_topic(get_admin_client(kafka_cluster), topic_name):
        result = instance.query_with_retry(
            "SELECT * FROM test.null_message_view",
            check_callback=lambda res: TSV(res) == expected,
        )

        assert expected == TSV(result)

        instance.query(
            """
            DROP TABLE test.null_message_consumer SYNC;
            DROP TABLE test.null_message_view;
            DROP TABLE test.null_message_kafka SYNC;
        """
        )


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
