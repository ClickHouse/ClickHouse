import os
import pytest
import subprocess
import sys
import grpc
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


# Use grpcio-tools to generate *pb2.py files from *.proto.

proto_dir = os.path.join(SCRIPT_DIR, './protos')
gen_dir = os.path.join(SCRIPT_DIR, './_gen')
os.makedirs(gen_dir, exist_ok=True)
subprocess.check_call(
    'python3 -m grpc_tools.protoc -I{proto_dir} --python_out={gen_dir} --grpc_python_out={gen_dir} \
    {proto_dir}/clickhouse_grpc.proto'.format(proto_dir=proto_dir, gen_dir=gen_dir), shell=True)

sys.path.append(gen_dir)
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc


# Utilities

config_dir = os.path.join(SCRIPT_DIR, './configs')
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/grpc_port.xml'])
grpc_port = 9001
main_channel = None

def create_channel():
    node_ip_with_grpc_port = cluster.get_instance_ip('node') + ':' + str(grpc_port)
    channel = grpc.insecure_channel(node_ip_with_grpc_port)
    grpc.channel_ready_future(channel).result(timeout=10)
    global main_channel
    if not main_channel:
        main_channel = channel
    return channel

def query_common(query_text, settings={}, input_data=[], output_format='TabSeparated', query_id='123', channel=None):
    if type(input_data) == str:
        input_data = [input_data]
    if not channel:
        channel = main_channel
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    def send_query_info():
        input_data_part = input_data.pop(0) if input_data else ''
        yield clickhouse_grpc_pb2.QueryInfo(query=query_text, settings=settings, input_data=input_data_part, output_format=output_format,
                                            query_id=query_id, next_query_info=bool(input_data))
        while input_data:
            input_data_part = input_data.pop(0)
            yield clickhouse_grpc_pb2.QueryInfo(input_data=input_data_part, next_query_info=bool(input_data))
    return list(stub.ExecuteQuery(send_query_info()))

def query_no_errors(*args, **kwargs):
    results = query_common(*args, **kwargs)
    if results and results[-1].HasField('exception'):
        raise Exception(results[-1].exception.display_text)
    return results

def query(*args, **kwargs):
    output = ""
    for result in query_no_errors(*args, **kwargs):
        output += result.output
    return output

def query_and_get_error(*args, **kwargs):
    results = query_common(*args, **kwargs)
    if not results or not results[-1].HasField('exception'):
        raise Exception("Expected to be failed but succeeded!")
    return results[-1].exception

def query_and_get_totals(*args, **kwargs):
    totals = ""
    for result in query_no_errors(*args, **kwargs):
        totals += result.totals
    return totals

def query_and_get_extremes(*args, **kwargs):
    extremes = ""
    for result in query_no_errors(*args, **kwargs):
        extremes += result.extremes
    return extremes

@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.start()
    try:
        with create_channel() as channel:
            yield cluster
            
    finally:
        cluster.shutdown()

@pytest.fixture(autouse=True)
def reset_after_test():
    yield
    query("DROP TABLE IF EXISTS t")

# Actual tests

def test_select_one():
    assert query("SELECT 1") == "1\n"

def test_ordinary_query():
    assert query("SELECT count() FROM numbers(100)") == "100\n"

def test_insert_query():
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    query("INSERT INTO t VALUES (1),(2),(3)")
    query("INSERT INTO t FORMAT TabSeparated 4\n5\n6\n")
    query("INSERT INTO t VALUES", input_data="(7),(8)")
    query("INSERT INTO t FORMAT TabSeparated", input_data="9\n10\n")
    assert query("SELECT a FROM t ORDER BY a") == "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"

def test_insert_query_streaming():
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    query("INSERT INTO t VALUES", input_data=["(1),(2),(3)", "(5),(4),(6)", "(8),(7),(9)"])
    assert query("SELECT a FROM t ORDER BY a") == "1\n2\n3\n4\n5\n6\n7\n8\n9\n"

def test_output_format():
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    query("INSERT INTO t VALUES (1),(2),(3)")
    assert query("SELECT a FROM t ORDER BY a FORMAT JSONEachRow") == '{"a":1}\n{"a":2}\n{"a":3}\n'
    assert query("SELECT a FROM t ORDER BY a", output_format="JSONEachRow") == '{"a":1}\n{"a":2}\n{"a":3}\n'

def test_totals_and_extremes():
    query("CREATE TABLE t (x UInt8, y UInt8) ENGINE = Memory")
    query("INSERT INTO t VALUES (1, 2), (2, 4), (3, 2), (3, 3), (3, 4)")
    assert query("SELECT sum(x), y FROM t GROUP BY y WITH TOTALS") == "4\t2\n3\t3\n5\t4\n"
    assert query_and_get_totals("SELECT sum(x), y FROM t GROUP BY y WITH TOTALS") == "12\t0\n"
    assert query("SELECT x, y FROM t") == "1\t2\n2\t4\n3\t2\n3\t3\n3\t4\n"
    assert query_and_get_extremes("SELECT x, y FROM t", settings={"extremes": "1"}) == "1\t2\n3\t4\n"

def test_errors_handling():
    e = query_and_get_error("")
    #print(e)
    assert "Empty query" in e.display_text
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    e = query_and_get_error("CREATE TABLE t (a UInt8) ENGINE = Memory")
    assert "Table default.t already exists" in e.display_text
