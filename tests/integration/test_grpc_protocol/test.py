import os
import pytest
import subprocess
import sys
import time
import grpc
from helpers.cluster import ClickHouseCluster
from threading import Thread

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
node = cluster.add_instance('node', main_configs=['configs/grpc_config.xml'])
grpc_port = 9100
main_channel = None

def create_channel():
    node_ip_with_grpc_port = cluster.get_instance_ip('node') + ':' + str(grpc_port)
    channel = grpc.insecure_channel(node_ip_with_grpc_port)
    grpc.channel_ready_future(channel).result(timeout=10)
    global main_channel
    if not main_channel:
        main_channel = channel
    return channel

def query_common(query_text, settings={}, input_data=[], input_data_delimiter='', output_format='TabSeparated', external_tables=[],
                 user_name='', password='', query_id='123', session_id='', stream_output=False, channel=None):
    if type(input_data) == str:
        input_data = [input_data]
    if not channel:
        channel = main_channel
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    def query_info():
        input_data_part = input_data.pop(0) if input_data else ''
        return clickhouse_grpc_pb2.QueryInfo(query=query_text, settings=settings, input_data=input_data_part, input_data_delimiter=input_data_delimiter,
                                             output_format=output_format, external_tables=external_tables, user_name=user_name, password=password,
                                             query_id=query_id, session_id=session_id, next_query_info=bool(input_data))
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

class QueryThread(Thread):
    def __init__(self, query_text, expected_output, query_id, use_separate_channel=False):
        Thread.__init__(self)
        self.query_text = query_text
        self.expected_output = expected_output
        self.use_separate_channel = use_separate_channel
        self.query_id = query_id
    
    def run(self):
        if self.use_separate_channel:
            with create_channel() as channel:
                assert query(self.query_text, query_id=self.query_id, channel=channel) == self.expected_output
        else:
            assert query(self.query_text, query_id=self.query_id) == self.expected_output

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

def test_authentication():
    query("CREATE USER john IDENTIFIED BY 'qwe123'")
    assert query("SELECT currentUser()", user_name="john", password="qwe123") == "john\n"

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

def test_input_function():
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    query("INSERT INTO t SELECT col1 * col2 FROM input('col1 UInt8, col2 UInt8') FORMAT CSV", input_data=["5,4\n", "8,11\n", "10,12\n"])
    assert query("SELECT a FROM t ORDER BY a") == "20\n88\n120\n"
    query("INSERT INTO t SELECT col1 * col2 FROM input('col1 UInt8, col2 UInt8') FORMAT CSV 11,13")
    assert query("SELECT a FROM t ORDER BY a") == "20\n88\n120\n143\n"
    query("INSERT INTO t SELECT col1 * col2 FROM input('col1 UInt8, col2 UInt8') FORMAT CSV 20,10\n", input_data="15,15\n")
    assert query("SELECT a FROM t ORDER BY a") == "20\n88\n120\n143\n200\n225\n"

def test_external_table():
    columns = [clickhouse_grpc_pb2.NameAndType(name='UserID', type='UInt64'), clickhouse_grpc_pb2.NameAndType(name='UserName', type='String')]
    ext1 = clickhouse_grpc_pb2.ExternalTable(name='ext1', columns=columns, data='1\tAlex\n2\tBen\n3\tCarl\n', format='TabSeparated')
    assert query("SELECT * FROM ext1 ORDER BY UserID", external_tables=[ext1]) == "1\tAlex\n"\
                                                                                  "2\tBen\n"\
                                                                                  "3\tCarl\n"
    ext2 = clickhouse_grpc_pb2.ExternalTable(name='ext2', columns=columns, data='4,Daniel\n5,Ethan\n', format='CSV')
    assert query("SELECT * FROM (SELECT * FROM ext1 UNION ALL SELECT * FROM ext2) ORDER BY UserID", external_tables=[ext1, ext2]) == "1\tAlex\n"\
                                                                                                                                     "2\tBen\n"\
                                                                                                                                     "3\tCarl\n"\
                                                                                                                                     "4\tDaniel\n"\
                                                                                                                                     "5\tEthan\n"
    unnamed_columns = [clickhouse_grpc_pb2.NameAndType(type='UInt64'), clickhouse_grpc_pb2.NameAndType(type='String')]
    unnamed_table = clickhouse_grpc_pb2.ExternalTable(columns=unnamed_columns, data='6\tGeorge\n7\tFred\n')
    assert query("SELECT * FROM _data ORDER BY _2", external_tables=[unnamed_table]) == "7\tFred\n"\
                                                                                        "6\tGeorge\n"

def test_external_table_streaming():
    columns = [clickhouse_grpc_pb2.NameAndType(name='UserID', type='UInt64'), clickhouse_grpc_pb2.NameAndType(name='UserName', type='String')]
    def send_query_info():
        yield clickhouse_grpc_pb2.QueryInfo(query="SELECT * FROM exts ORDER BY UserID",
                                            external_tables=[clickhouse_grpc_pb2.ExternalTable(name='exts', columns=columns, data='1\tAlex\n2\tBen\n3\tCarl\n')],
                                            next_query_info=True)
        yield clickhouse_grpc_pb2.QueryInfo(external_tables=[clickhouse_grpc_pb2.ExternalTable(name='exts', data='4\tDaniel\n5\tEthan\n')])
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(main_channel)
    result = stub.ExecuteQueryWithStreamInput(send_query_info())
    assert result.output == "1\tAlex\n"\
                            "2\tBen\n"\
                            "3\tCarl\n"\
                            "4\tDaniel\n"\
                            "5\tEthan\n"

def test_simultaneous_queries_same_channel():
    threads=[]
    try:
        for i in range(0, 100):
            thread = QueryThread("SELECT sum(number) FROM numbers(10)", expected_output="45\n", query_id='sqA'+str(i))
            threads.append(thread)
            thread.start()
    finally:
        for thread in threads:
            thread.join()

def test_simultaneous_queries_multiple_channels():
    threads=[]
    try:
        for i in range(0, 100):
            thread = QueryThread("SELECT sum(number) FROM numbers(10)", expected_output="45\n", query_id='sqB'+str(i), use_separate_channel=True)
            threads.append(thread)
            thread.start()
    finally:
        for thread in threads:
            thread.join()

def test_cancel_while_processing_input():
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    def send_query_info():
        yield clickhouse_grpc_pb2.QueryInfo(query="INSERT INTO t FORMAT TabSeparated", input_data="1\n2\n3\n", next_query_info=True)
        yield clickhouse_grpc_pb2.QueryInfo(input_data="4\n5\n6\n", next_query_info=True)
        yield clickhouse_grpc_pb2.QueryInfo(cancel=True)
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(main_channel)
    result = stub.ExecuteQueryWithStreamInput(send_query_info())
    assert result.cancelled == True
    assert result.progress.written_rows == 6
    assert query("SELECT a FROM t ORDER BY a") == "1\n2\n3\n4\n5\n6\n"

def test_cancel_while_generating_output():
    def send_query_info():
        yield clickhouse_grpc_pb2.QueryInfo(query="SELECT number, sleep(0.2) FROM numbers(10) SETTINGS max_block_size=2")
        time.sleep(0.5)
        yield clickhouse_grpc_pb2.QueryInfo(cancel=True)
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(main_channel)
    results = list(stub.ExecuteQueryWithStreamIO(send_query_info()))
    assert len(results) >= 1
    assert results[-1].cancelled == True
    output = ''
    for result in results:
        output += result.output
    assert output == '0\t0\n1\t0\n2\t0\n3\t0\n'
