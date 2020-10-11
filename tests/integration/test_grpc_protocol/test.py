import os
import pytest
import subprocess
import sys
import grpc
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Use grpcio-tools to generate *pb2.py files from *.proto.
proto_dir = os.path.join(SCRIPT_DIR, './protos')
proto_gen_dir = os.path.join(SCRIPT_DIR, './_gen')
os.makedirs(proto_gen_dir, exist_ok=True)
subprocess.check_call(
    'python3 -m grpc_tools.protoc -I{proto_dir} --python_out={proto_gen_dir} --grpc_python_out={proto_gen_dir} \
    {proto_dir}/clickhouse_grpc.proto'.format(proto_dir=proto_dir, proto_gen_dir=proto_gen_dir), shell=True)

# Import everything from the generated *pb2.py files.
sys.path.append(proto_gen_dir)
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc

config_dir = os.path.join(SCRIPT_DIR, './configs')
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/grpc_port.xml'])
server_port = 9001

@pytest.fixture(scope="module")
def server_address():
    cluster.start()
    try:
        yield cluster.get_instance_ip('node')
    finally:
        cluster.shutdown()

def Query(server_address_and_port, query, mode="output", insert_data=[]):
    output = []
    totals = []
    data_stream = (len(insert_data) != 0)
    with grpc.insecure_channel(server_address_and_port) as channel:
        grpc.channel_ready_future(channel).result()
        stub = clickhouse_grpc_pb2_grpc.GRPCStub(channel)
        def write_query():
            user_info = clickhouse_grpc_pb2.User(user="default", quota='default')
            query_info = clickhouse_grpc_pb2.QuerySettings(query=query, query_id='123', data_stream=data_stream, format='TabSeparated')
            yield clickhouse_grpc_pb2.QueryRequest(user_info=user_info, query_info=query_info)
            if data_stream:
                for data in insert_data:
                    yield clickhouse_grpc_pb2.QueryRequest(insert_data=data)
                yield clickhouse_grpc_pb2.QueryRequest(insert_data="")
        for response in stub.Query(write_query(), 10.0):
            output += response.output.split()
            totals += response.totals.split()
        if mode == "output":
            return output
        elif mode == "totals":
            return totals

def test_ordinary_query(server_address):
    server_address_and_port = server_address + ':' + str(server_port)
    assert Query(server_address_and_port, "SELECT 1") == [u'1']
    assert Query(server_address_and_port, "SELECT count() FROM numbers(100)") == [u'100']

def test_query_insert(server_address):
    server_address_and_port = server_address + ':' + str(server_port)
    assert Query(server_address_and_port, "CREATE TABLE t (a UInt8) ENGINE = Memory") == []
    assert Query(server_address_and_port, "INSERT INTO t VALUES (1),(2),(3)") == []
    assert Query(server_address_and_port, "INSERT INTO t FORMAT TabSeparated 4\n5\n6\n") == []
    assert Query(server_address_and_port, "INSERT INTO t FORMAT TabSeparated 10\n11\n12\n") == []
    assert Query(server_address_and_port, "SELECT a FROM t ORDER BY a") == [u'1', u'2', u'3', u'4', u'5', u'6', u'10', u'11', u'12']
    assert Query(server_address_and_port, "DROP TABLE t") == []

def test_handle_mistakes(server_address):
    server_address_and_port = server_address + ':' + str(server_port)
    assert Query(server_address_and_port, "") == []
    assert Query(server_address_and_port, "CREATE TABLE t (a UInt8) ENGINE = Memory") == []
    assert Query(server_address_and_port, "CREATE TABLE t (a UInt8) ENGINE = Memory") == []

def test_totals(server_address):
    server_address_and_port = server_address + ':' + str(server_port)
    assert Query(server_address_and_port, "") == []
    assert Query(server_address_and_port, "CREATE TABLE tabl (x UInt8, y UInt8) ENGINE = Memory;") == []
    assert Query(server_address_and_port, "INSERT INTO tabl VALUES (1, 2), (2, 4), (3, 2), (3, 3), (3, 4);") == []
    assert Query(server_address_and_port, "SELECT sum(x), y FROM tabl GROUP BY y WITH TOTALS") == [u'4', u'2', u'3', u'3', u'5', u'4']
    assert Query(server_address_and_port, "SELECT sum(x), y FROM tabl GROUP BY y WITH TOTALS", mode="totals") == [u'12', u'0']

def test_query_insert(server_address):
    server_address_and_port = server_address + ':' + str(server_port)
    assert Query(server_address_and_port, "CREATE TABLE t (a UInt8) ENGINE = Memory") == []
    assert Query(server_address_and_port, "INSERT INTO t VALUES", insert_data=["(1),(2),(3)", "(5),(4),(6)", "(8),(7),(9)"]) == []
    assert Query(server_address_and_port, "SELECT a FROM t ORDER BY a") == [u'1', u'2', u'3', u'4', u'5', u'6', u'7', u'8', u'9']
    assert Query(server_address_and_port, "DROP TABLE t") == []
