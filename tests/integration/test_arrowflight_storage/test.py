# coding: utf-8

import os
import pyarrow.flight as fl
import pyarrow as pa
import pytest
import threading

from helpers.cluster import ClickHouseCluster, get_docker_compose_path
from helpers.test_tools import TSV

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()


cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", stay_alive=True)

SERVER_LOCATION = "grpc+tcp://0.0.0.0:5005"

class FlightServer(fl.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)

    def do_get(self, context, ticket):
        print(f"Do get query with ticket: {ticket}")
        schema = pa.schema([('column1', pa.string()), ('column2', pa.string())])
        column1_data = pa.array(["test_value_1", "abcadbc", "123456789"])
        column2_data = pa.array(["data1", "text_text_text", "data3"])
        
        table = pa.table({'column1': column1_data, 'column2': column2_data}, schema=schema)
        
        return fl.RecordBatchStream(table)

    def get_schema(self, context, descriptor):
        print(f"Get schema query with descriptor: {descriptor}")
        schema = pa.schema([('column1', pa.string()), ('column2', pa.string())])
        return fl.SchemaResult(schema)

    def do_action(self, context, action):
        print(f"Do action query: {action}")
        return fl.FlightDescriptor.for_command("Action executed")

    def do_flight_info(self, context, descriptor):
        print(f"Do flight info query: {descriptor}")
        schema = pa.schema([('column1', pa.string()), ('column2', pa.string())])
        return fl.FlightInfo(
            schema,
            descriptor,
            1,
            0,
            None,
            None
        )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

@pytest.fixture(scope="module")
def start_arrowflight_server():
    flight_server = FlightServer(SERVER_LOCATION)
    try:
        yield flight_server
        flight_server.serve()
    finally:
        flight_server.shutdown()


def arrowflight_check_result(result, reference):
    assert TSV(result) == TSV(reference)


def test_get_data():
    result = instance.query("SELECT * FROM arrowflight('127.0.0.1:5005', 'ABC');")
    arrowflight_check_result(result, [["test_value_1", "abcadbc", "123456789"], ["data1", "text_text_text", "data3"]])
