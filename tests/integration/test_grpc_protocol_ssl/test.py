import os
import pytest
import sys
import grpc
from helpers.cluster import ClickHouseCluster, run_and_check

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


# Use grpcio-tools to generate *pb2.py files from *.proto.

proto_dir = os.path.join(SCRIPT_DIR, './protos')
gen_dir = os.path.join(SCRIPT_DIR, './_gen')
os.makedirs(gen_dir, exist_ok=True)
run_and_check(
    'python3 -m grpc_tools.protoc -I{proto_dir} --python_out={gen_dir} --grpc_python_out={gen_dir} \
    {proto_dir}/clickhouse_grpc.proto'.format(proto_dir=proto_dir, gen_dir=gen_dir), shell=True)

sys.path.append(gen_dir)
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc


# Utilities

node_ip = '10.5.172.77' # It's important for the node to work at this IP because 'server-cert.pem' requires that (see server-ext.cnf).
grpc_port = 9100
node_ip_with_grpc_port = node_ip + ':' + str(grpc_port)
config_dir = os.path.join(SCRIPT_DIR, './configs')
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', ipv4_address=node_ip, main_configs=['configs/grpc_config.xml', 'configs/server-key.pem', 'configs/server-cert.pem', 'configs/ca-cert.pem'])

def create_secure_channel():
    ca_cert = open(os.path.join(config_dir, 'ca-cert.pem'), 'rb').read()
    client_key = open(os.path.join(config_dir, 'client-key.pem'), 'rb').read()
    client_cert = open(os.path.join(config_dir, 'client-cert.pem'), 'rb').read()
    credentials = grpc.ssl_channel_credentials(ca_cert, client_key, client_cert)
    channel = grpc.secure_channel(node_ip_with_grpc_port, credentials)
    grpc.channel_ready_future(channel).result(timeout=10)
    return channel

def create_insecure_channel():
    channel = grpc.insecure_channel(node_ip_with_grpc_port)
    grpc.channel_ready_future(channel).result(timeout=2)
    return channel

def create_secure_channel_with_wrong_client_certificate():
    ca_cert = open(os.path.join(config_dir, 'ca-cert.pem'), 'rb').read()
    client_key = open(os.path.join(config_dir, 'wrong-client-key.pem'), 'rb').read()
    client_cert = open(os.path.join(config_dir, 'wrong-client-cert.pem'), 'rb').read()
    credentials = grpc.ssl_channel_credentials(ca_cert, client_key, client_cert)
    channel = grpc.secure_channel(node_ip_with_grpc_port, credentials)
    grpc.channel_ready_future(channel).result(timeout=2)
    return channel

def query(query_text, channel):
    query_info = clickhouse_grpc_pb2.QueryInfo(query=query_text)
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    result = stub.ExecuteQuery(query_info)
    if result and result.HasField('exception'):
        raise Exception(result.exception.display_text)
    return result.output

@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.start()
    try:
        yield cluster
            
    finally:
        cluster.shutdown()

# Actual tests

def test_secure_channel():
    with create_secure_channel() as channel:
        assert query("SELECT 'ok'", channel) == "ok\n"

def test_insecure_channel():
    with pytest.raises(grpc.FutureTimeoutError):
        with create_insecure_channel() as channel:
            query("SELECT 'ok'", channel)

def test_wrong_client_certificate():
    with pytest.raises(grpc.FutureTimeoutError):
        with create_insecure_channel() as channel:
            query("SELECT 'ok'", channel)
