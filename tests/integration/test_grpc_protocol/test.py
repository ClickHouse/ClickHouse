# coding: utf-8

import docker
import datetime
import math
import os
import pytest
import subprocess
import time
import pymysql.connections
from docker.models.containers import Container

from helpers.cluster import ClickHouseCluster


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
proto_dir = os.path.join(SCRIPT_DIR, './protos')
subprocess.check_call(
    'python -m grpc_tools.protoc -I{proto_path} --python_out=. --grpc_python_out=. \
    {proto_path}/GrpcConnection.proto'.format(proto_path=proto_dir), shell=True)
import grpc

import GrpcConnection_pb2
import GrpcConnection_pb2_grpc


config_dir = os.path.join(SCRIPT_DIR, './configs')
cluster = ClickHouseCluster(__file__)


node = cluster.add_instance('node', config_dir=config_dir, env_variables={'UBSAN_OPTIONS': 'print_stacktrace=1'})

server_port = 9005

@pytest.fixture(scope="module")
def server_address():
    cluster.start()
    try:
        yield cluster.get_instance_ip('node')
    finally:
        cluster.shutdown()

def test_1(server_address):

    print(server_address)
    with grpc.insecure_channel(server_address + ':9005') as channel:
        stub = GrpcConnection_pb2_grpc.GRPCStub(channel)
        print('client connected')
        user_info = GrpcConnection_pb2.User(user="default", key='123', quota='default')
        query_info = GrpcConnection_pb2.QuerySettings(query="SELECT 1", query_id='123')
        for s in stub.Query(GrpcConnection_pb2.QueryRequest(user_info=user_info, query_info=query_info, interactive_delay=1000)):
            print(s.query)
        assert False
    # with grpc.insecure_channel(server_address) as channel:
        # stub = GrpcConnection_pb2_grpc.GreeterStub(channel)
        # response = stub.SayHello(GrpcConnection_pb2.Query(name='you'))
    # print("Greeter client received: " + response.message)



