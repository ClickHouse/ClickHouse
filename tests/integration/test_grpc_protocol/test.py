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

def query_common(query_text, settings={}, input_data=[], input_data_delimiter='', output_format='TabSeparated', query_id='123', session_id='', stream_output=False, channel=None):
    if type(input_data) == str:
        input_data = [input_data]
    if not channel:
        channel = main_channel
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    def query_info():
        input_data_part = input_data.pop(0) if input_data else ''
        return clickhouse_grpc_pb2.QueryInfo(query=query_text, settings=settings, input_data=input_data_part, input_data_delimiter=input_data_delimiter,
                                             output_format=output_format, query_id=query_id, session_id=session_id, next_query_info=bool(input_data))
    def send_query_info():
        yield query_info()
        while input_data:
            input_data_part = input_data.pop(0)
            yield clickhouse_grpc_pb2.QueryInfo(input_data=input_data_part, next_query_info=bool(input_data))
    stream_input = len(input_data) > 1
    if stream_input and stream_output:
        return list(stub.ExecuteQueryWithStreamIO(send_query_info()))
    elif stream_input:
        return [stub.ExecuteQueryWithStreamInput(send_query_info())]
    elif stream_output:
        return list(stub.ExecuteQueryWithStreamOutput(query_info()))
    else:
        return [stub.ExecuteQuery(query_info())]

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

def query_and_get_logs(*args, **kwargs):
    logs = ""
    for result in query_no_errors(*args, **kwargs):
        for log_entry in result.logs:
            #print(log_entry)
            logs += log_entry.text + "\n"
    return logs

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
    query("INSERT INTO t VALUES", input_data=["(1),(2),(3),", "(5),(4),(6),", "(7),(8),(9)"])
    assert query("SELECT a FROM t ORDER BY a") == "1\n2\n3\n4\n5\n6\n7\n8\n9\n"

def test_insert_query_delimiter():
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    query("INSERT INTO t FORMAT CSV 1\n2", input_data=["3", "4\n5"], input_data_delimiter='\n')
    assert query("SELECT a FROM t ORDER BY a") == "1\n2\n3\n4\n5\n"
    query("DROP TABLE t")
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    query("INSERT INTO t FORMAT CSV 1\n2", input_data=["3", "4\n5"])
    assert query("SELECT a FROM t ORDER BY a") == "1\n5\n234\n"

def test_insert_default_column():
    query("CREATE TABLE t (a UInt8, b Int32 DEFAULT 100, c String DEFAULT 'c') ENGINE = Memory")
    query("INSERT INTO t (c, a) VALUES ('x',1),('y',2)")
    query("INSERT INTO t (a) FORMAT TabSeparated", input_data="3\n4\n")
    assert query("SELECT * FROM t ORDER BY a") == "1\t100\tx\n" \
                                                  "2\t100\ty\n" \
                                                  "3\t100\tc\n" \
                                                  "4\t100\tc\n"

def test_insert_splitted_row():
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    query("INSERT INTO t VALUES", input_data=["(1),(2),(", "3),(5),(4),(6)"])
    assert query("SELECT a FROM t ORDER BY a") == "1\n2\n3\n4\n5\n6\n"

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

def test_logs():
    logs = query_and_get_logs("SELECT 1", settings={'send_logs_level':'debug'})
    assert "SELECT 1" in logs
    assert "Read 1 rows" in logs
    assert "Peak memory usage" in logs

def test_progress():
    results = query_no_errors("SELECT number, sleep(0.31) FROM numbers(8) SETTINGS max_block_size=2, interactive_delay=100000", stream_output=True)
    #print(results)
    assert str(results) ==\
"""[progress {
  read_rows: 2
  read_bytes: 16
  total_rows_to_read: 8
}
, output: "0\\t0\\n1\\t0\\n"
, progress {
  read_rows: 2
  read_bytes: 16
}
, output: "2\\t0\\n3\\t0\\n"
, progress {
  read_rows: 2
  read_bytes: 16
}
, output: "4\\t0\\n5\\t0\\n"
, progress {
  read_rows: 2
  read_bytes: 16
}
, output: "6\\t0\\n7\\t0\\n"
, stats {
  rows: 8
  blocks: 4
  allocated_bytes: 324
  applied_limit: true
  rows_before_limit: 8
}
]"""

def test_session():
    session_a = "session A"
    session_b = "session B"
    query("SET custom_x=1", session_id=session_a)
    query("SET custom_y=2", session_id=session_a)
    query("SET custom_x=3", session_id=session_b)
    query("SET custom_y=4", session_id=session_b)
    assert query("SELECT getSetting('custom_x'), getSetting('custom_y')", session_id=session_a) == "1\t2\n"
    assert query("SELECT getSetting('custom_x'), getSetting('custom_y')", session_id=session_b) == "3\t4\n"

def test_no_session():
    e = query_and_get_error("SET custom_x=1")
    assert "There is no session" in e.display_text
